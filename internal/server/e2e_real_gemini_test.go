package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── gemini 包真实上游 E2E(AI Studio;真实网络,真实 key) ───
//
// key 经环境变量注入,绝不写入仓库:
//
//	GEMINI_TEST_KEY       AI Studio key
//	GEMINI_TEST_MODEL     模型(缺省 gemini-3.6-flash;2.5 系列对新用户已下线)
//	GEMINI_TEST_TRANSPORT 命名传输实例名(缺省直连;Gemini 对部分地区返回
//	                      "User location is not supported",受限环境在此指定代理传输)
//
// 无 key 时整组跳过(CI 无凭据环境不失败)。

const (
	geminiTestModelDefault = "gemini-3.6-flash"
	geminiBaseURL          = "https://generativelanguage.googleapis.com"
	geminiEchoPrompt       = "reply with exactly: pong"
	// 网关客户端等待上限:真实上游跨区往返 + 模型思考耗时,本地假上游的上限不适用
	geminiClientTimeout = 90 * time.Second
)

// findRepoRoot 向上查找含 go.mod 的仓库根(从仓库外加载插件包时用)
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("repo root not found")
	return ""
}

// mustGeminiFiles 加载 gemini 示例包(SDK 内置;缺失则跳过)
func mustGeminiFiles(t *testing.T) (string, string) {
	t.Helper()
	root := findRepoRoot(t)
	dir := filepath.Join(root, "sdk", "examples", "gemini")
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Skip("gemini example package not found; skipping real-upstream E2E")
	}
	src, err := os.ReadFile(filepath.Join(dir, "protocol.js"))
	if err != nil {
		t.Fatalf("read gemini protocol: %v", err)
	}
	return string(manifest), string(src)
}

func geminiTestEnv(t *testing.T) (key, model string) {
	t.Helper()
	key = os.Getenv("GEMINI_TEST_KEY")
	if key == "" {
		t.Skip("GEMINI_TEST_KEY not set; skipping real-upstream E2E")
	}
	model = os.Getenv("GEMINI_TEST_MODEL")
	if model == "" {
		model = geminiTestModelDefault
	}
	return key, model
}

// geminiEgress 出口配置:代理 URL 非空则注册真实命名传输实例,否则直连
// (Gemini 对部分地区返回 "User location is not supported",受限环境必须给出出口)
type geminiEgress struct {
	transport  string
	transports []TransportCfg
}

func geminiEgressFromEnv(t *testing.T) geminiEgress {
	t.Helper()
	proxyURL := os.Getenv("GEMINI_TEST_PROXY_URL")
	if proxyURL == "" {
		return geminiEgress{}
	}
	name := os.Getenv("GEMINI_TEST_TRANSPORT")
	if name == "" {
		name = "gemini-out"
	}
	t.Logf("gemini egress via %s -> %s", name, proxyURL)
	return geminiEgress{
		transport:  name,
		transports: []TransportCfg{{Name: name, URL: proxyURL}},
	}
}

// newGeminiFixture 安装 gemini 包并实例化真实 Gemini 模型行(v2:连接=包参数,密钥=包级 keys)
func newGeminiFixture(t *testing.T, apiKey, model string, egress geminiEgress) *fourGroupsFixture {
	t.Helper()
	manifest, src := mustGeminiFiles(t)
	f := newFourGroupsWith(t, geminiClientTimeout, egress.transports...)
	if err := f.app.AdminDeps.Packages.Install(t.Context(), aapZip(t, manifest, map[string]string{"protocol.js": src})); err != nil {
		t.Fatalf("install gemini pkg: %v", err)
	}
	if egress.transport != "" {
		t.Logf("gemini egress via named transport %q", egress.transport)
	}
	if err := f.app.AdminDeps.Packages.Keys().Merge("gemini", map[string]any{"api_key": apiKey}); err != nil {
		t.Fatalf("merge gemini key: %v", err)
	}
	cfg := map[string]any{"base_url": geminiBaseURL}
	if egress.transport != "" {
		cfg["transport"] = egress.transport
	}
	if _, err := f.app.AdminDeps.Packages.Settings().Put("gemini", plugin.PutInput{Config: cfg}); err != nil {
		t.Fatalf("put gemini params: %v", err)
	}
	if err := f.app.Registry.Save(t.Context(), &upstream.Model{
		Name: model, Plugin: "gemini", Enabled: true,
	}); err != nil {
		t.Fatalf("save gemini row: %v", err)
	}
	return f
}

func geminiChatPayload(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"max_tokens":512,"messages":[{"role":"user","content":%q}],"stream":%v}`,
		model, geminiEchoPrompt, stream)
}

func geminiMessagePayload(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"max_tokens":512,"stream":%v,"messages":[{"role":"user","content":%q}]}`,
		model, stream, geminiEchoPrompt)
}

// geminiTransient 上游瞬时状态判定:免费层日配额耗尽、模型过载、连接被掐
// 这些与包无关,判 skip 而非 fail——否则 E2E 变成在测 Google 的服务可用性
func geminiTransient(t *testing.T, status int, body string) {
	t.Helper()
	transient := status == http.StatusTooManyRequests ||
		status == http.StatusServiceUnavailable ||
		(strings.Contains(body, "UNAVAILABLE") && status >= 500)
	if !transient {
		return
	}
	t.Skipf("upstream transient failure (status %d): %.200s", status, body)
}

// geminiPost 真实请求;非 2xx 先过瞬时判定,再交回调用方断言
func geminiPost(t *testing.T, f *fourGroupsFixture, path string, headers map[string]string, payload string) (int, string) {
	t.Helper()
	got, body := f.post(t, path, headers, payload)
	if got != http.StatusOK {
		geminiTransient(t, got, body)
	}
	return got, body
}

func TestRealGemini_ChatNonStream(t *testing.T) {
	// Given gemini 包 + 真实 Gemini 上游 When chat 非流式 Then 200 且 openai 信封与 content/usage 齐备
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	got, body := geminiPost(t, f, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, geminiChatPayload(model, false))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	for _, want := range []string{`"object":"chat.completion"`, "pong", `"choices"`, `"usage"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s: %s", want, body)
		}
	}
}

func TestRealGemini_ChatStream(t *testing.T) {
	// When chat 流式 Then SSE 为 openai chunk 形态(choices 装载)且以 [DONE] 收尾
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	got, body := geminiPost(t, f, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, geminiChatPayload(model, true))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	for _, want := range []string{"data: ", "[DONE]", `"object":"chat.completion.chunk"`, `"delta"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "data: [{") {
		t.Fatalf("chunk must not be array-wrapped: %s", body)
	}
	// 结构断言:逐条 data 行必须是单 JSON 对象且含 choices(形损坏类回归的鉴别力)
	sawChunk := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatalf("data line is not a single json object: %q", line)
		}
		if _, ok := chunk["choices"]; ok {
			sawChunk = true
		}
	}
	if !sawChunk {
		t.Fatalf("no choices chunk in stream: %s", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) {
		t.Fatalf("missing stop finish_reason: %s", body)
	}
	if !strings.Contains(body, "pong") {
		t.Fatalf("stream content: %s", body)
	}
}

func TestRealGemini_MessageNonStream(t *testing.T) {
	// When anthropic 入口(上游 chat 形态,态适配聚合)Then message 信封 + 文本块含 pong
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	got, body := geminiPost(t, f, "/v1/messages", map[string]string{
		"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
	}, geminiMessagePayload(model, false))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	for _, want := range []string{`"type":"message"`, `"text"`, "pong"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s: %s", want, body)
		}
	}
}

func TestRealGemini_MessageStream(t *testing.T) {
	// When anthropic 入口流式 Then 事件序列完整且不出现 openai 的 [DONE]
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	got, body := geminiPost(t, f, "/v1/messages", map[string]string{
		"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
	}, geminiMessagePayload(model, true))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	for _, want := range []string{"message_start", "content_block_delta", "message_stop"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s: %q", want, body)
		}
	}
	if strings.Contains(body, "[DONE]") {
		t.Fatal("anthropic stream must not emit [DONE]")
	}
}

func TestRealGemini_AdminTestEndpoint(t *testing.T) {
	// When 管理 API 连通测试(最小请求走完整管道)Then ok=true 且上报延迟
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	var id int64
	for _, u := range f.app.Registry.List() {
		if u.Name == model {
			id = u.ID
		}
	}
	if id == 0 {
		t.Fatal("model row not found")
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/models/%d/test", id), "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		geminiTransient(t, http.StatusTooManyRequests, body)
		geminiTransient(t, http.StatusServiceUnavailable, body)
		t.Fatalf("admin test: %d %s", status, body)
	}
}

func TestRealGemini_ErrorEnvelopePassthrough(t *testing.T) {
	// Given 上游不存在的模型 When 真实请求 Then 入口错误信封且 type 为上游真实枚举(未经翻译)
	// 上游对未知模型应回 404 NOT_FOUND;若配额耗尽可能掐连接(502),此时走瞬时判定
	key, _ := geminiTestEnv(t)
	badModel := "gemini-model-does-not-exist-xyz"
	f := newGeminiFixture(t, key, badModel, geminiEgressFromEnv(t))
	got, body := geminiPost(t, f, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, geminiChatPayload(badModel, false))
	if got >= 200 && got < 300 {
		t.Fatalf("bad model must fail: %s", body)
	}
	var envelope struct {
		Error struct {
			Type   string `json:"type"`
			Status int    `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("entry error envelope (invalid json): %d %s", got, body)
	}
	if envelope.Error.Type != "NOT_FOUND" {
		t.Fatalf("type must carry upstream enum verbatim: %d %s", got, body)
	}
	if envelope.Error.Status != got {
		t.Fatalf("error.status should mirror entry status: got=%d body=%s", got, body)
	}
}

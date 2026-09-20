package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	geminiUpstreamName     = "real-gemini"
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

// mustGeminiFiles 从插件库加载 gemini 包(与主库同级的 plugins-repo;双处均缺失则跳过)
func mustGeminiFiles(t *testing.T) (string, string) {
	t.Helper()
	root := findRepoRoot(t)
	candidates := []string{
		filepath.Join(root, "plugins", "gemini"),
		filepath.Join(root, "..", "plugins-repo", "plugins", "gemini"),
	}
	for _, dir := range candidates {
		manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			continue
		}
		src, err := os.ReadFile(filepath.Join(dir, "protocol.js"))
		if err != nil {
			t.Fatalf("read gemini protocol: %v", err)
		}
		return string(manifest), string(src)
	}
	t.Skip("gemini plugin repo not found (split repo); skipping real-upstream E2E")
	return "", ""
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
		transports: []TransportCfg{{Name: name, Type: "http_proxy", URL: proxyURL}},
	}
}

// newGeminiFixture 安装 gemini 包并实例化真实 Gemini 上游
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
	u := &upstream.Upstream{
		Name: geminiUpstreamName, Enabled: true,
		Base:   upstream.PackageRef{Package: "gemini"},
		Models: []string{model},
		Params: map[string]any{},
		Targets: []upstream.Target{{Name: "t1",
			BaseURL: geminiBaseURL, Transport: egress.transport, Enabled: true,
			Secrets: map[string]string{"api_key": apiKey}}},
	}
	if err := f.app.Registry.Save(t.Context(), u); err != nil {
		t.Fatalf("save upstream: %v", err)
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

func TestRealGemini_ChatNonStream(t *testing.T) {
	// Given gemini 包 + 真实 Gemini 上游 When chat 非流式 Then 200 且 openai 信封与 content/usage 齐备
	key, model := geminiTestEnv(t)
	f := newGeminiFixture(t, key, model, geminiEgressFromEnv(t))
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
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
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
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
	got, body := f.post(t, "/v1/messages", map[string]string{
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
	got, body := f.post(t, "/v1/messages", map[string]string{
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
		if u.Name == geminiUpstreamName {
			id = u.ID
		}
	}
	if id == 0 {
		t.Fatal("upstream not found")
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/upstreams/%d/test", id), "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("admin test: %d %s", status, body)
	}
}

func TestRealGemini_UpstreamErrorPassthrough(t *testing.T) {
	// Given 上游不存在的模型 When 真实请求 Then 4xx 且错误体为入口信封(无重试,恰一次)
	key, _ := geminiTestEnv(t)
	badModel := "gemini-model-does-not-exist-xyz"
	f := newGeminiFixture(t, key, badModel, geminiEgressFromEnv(t))
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, geminiChatPayload(badModel, false))
	if got == http.StatusOK {
		t.Fatalf("bad model must fail: %s", body)
	}
	if got < 400 || got >= 500 {
		t.Fatalf("upstream status must pass through as 4xx: %d %s", got, body)
	}
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("entry error envelope: %d %s", got, body)
	}
}

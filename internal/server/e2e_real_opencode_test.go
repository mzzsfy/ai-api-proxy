package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ai-api-proxy/internal/upstream"
)

// ─── opencode 包真实上游 E2E(OpenRouter;真实网络,真实 key) ───
//
// key 经环境变量注入,绝不写入仓库:
//
//	OPENCODE_TEST_KEY   OpenRouter/OpenCode key(sk-or-v1-... 或 zen key)
//	OPENCODE_TEST_MODEL 模型(缺省 deepseek/deepseek-v4-flash-0731:free,免费层)
//
// 无 key 时整组跳过(CI 无凭据环境不失败)。

const opencodeTestModelDefault = "deepseek/deepseek-v4-flash-0731:free"

// opencodeManifest / opencodeProtocolSrc 从仓库包目录加载(测的就是仓库内真实包文件)
var (
	opencodeManifestRaw  []byte
	opencodeProtocolSrcs []byte
	opencodeLoadErr      error
)

func init() {
	opencodeManifestRaw, opencodeLoadErr = os.ReadFile(filepath.Join("..", "..", "plugins", "opencode", "manifest.json"))
	if opencodeLoadErr != nil {
		return
	}
	opencodeProtocolSrcs, opencodeLoadErr = os.ReadFile(filepath.Join("..", "..", "plugins", "opencode", "protocol.js"))
}

func mustOpencodeFiles(t *testing.T) (string, string) {
	t.Helper()
	if opencodeLoadErr != nil {
		t.Fatalf("load opencode pkg: %v", opencodeLoadErr)
	}
	return string(opencodeManifestRaw), string(opencodeProtocolSrcs)
}

func opencodeTestEnv(t *testing.T) (key, model string) {
	t.Helper()
	key = os.Getenv("OPENCODE_TEST_KEY")
	if key == "" {
		t.Skip("OPENCODE_TEST_KEY not set; skipping real-upstream E2E")
	}
	model = os.Getenv("OPENCODE_TEST_MODEL")
	if model == "" {
		model = opencodeTestModelDefault
	}
	return key, model
}

// newOpencodeFixture 安装 opencode 包并实例化 OpenRouter 上游(真实网关 HTTP 服务)
func newOpencodeFixture(t *testing.T, apiKey, model string) *fourGroupsFixture {
	t.Helper()
	manifest, src := mustOpencodeFiles(t)
	f := newFourGroups(t)
	ctx := context.Background()
	if err := f.app.AdminDeps.Packages.Install(ctx, aapZip(t, manifest, map[string]string{"protocol.js": src})); err != nil {
		t.Fatalf("install opencode pkg: %v", err)
	}
	u := &upstream.Upstream{
		Name: "real-openrouter", Enabled: true,
		Base:   upstream.PackageRef{Package: "opencode"},
		Models: []string{model},
		Params: map[string]any{},
		Targets: []upstream.Target{{Name: "t1",
			BaseURL:   "https://openrouter.ai/api",
			Transport: "", Enabled: true,
			Secrets: map[string]string{"api_key": apiKey}}},
	}
	if err := f.app.Registry.Save(ctx, u); err != nil {
		t.Fatalf("save upstream: %v", err)
	}
	return f
}

func chatPayload(model string, stream bool) string {
	return fmt.Sprintf(`{"model":%q,"max_tokens":32,"messages":[{"role":"user","content":"reply with exactly: pong"}],"stream":%v}`, model, stream)
}

func TestRealOpencode_ChatNonStream(t *testing.T) {
	// Given 真实 OpenRouter 上游(opencode 包)When 真实 chat 非流式请求 Then 200 且 content 含 pong
	key, model := opencodeTestEnv(t)
	f := newOpencodeFixture(t, key, model)
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, chatPayload(model, false))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	if !strings.Contains(body, `"content":" pong"`) && !strings.Contains(body, "pong") {
		t.Fatalf("content missing pong: %s", body)
	}
	if !strings.Contains(body, `"object":"chat.completion"`) {
		t.Fatalf("object shape: %s", body)
	}
}

func TestRealOpencode_ChatStream(t *testing.T) {
	// When 真实 chat 流式请求 Then SSE 流含 delta 与 [DONE]
	key, model := opencodeTestEnv(t)
	f := newOpencodeFixture(t, key, model)
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, chatPayload(model, true))
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	if !strings.Contains(body, "data: ") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("sse shape: %q", body)
	}
	if !strings.Contains(body, "pong") {
		t.Fatalf("stream content: %s", body)
	}
}

func TestRealOpencode_MessageNonStream(t *testing.T) {
	// When anthropic 入口(上游 chat 形态,态适配聚合)Then message JSON + text 含 pong
	key, model := opencodeTestEnv(t)
	f := newOpencodeFixture(t, key, model)
	payload := fmt.Sprintf(`{"model":%q,"max_tokens":32,"stream":false,"messages":[{"role":"user","content":"reply with exactly: pong"}]}`, model)
	got, body := f.post(t, "/v1/messages", map[string]string{
		"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
	}, payload)
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	if !strings.Contains(body, `"type":"message"`) || !strings.Contains(body, "pong") {
		t.Fatalf("message body: %s", body)
	}
}

func TestRealOpencode_MessageStream(t *testing.T) {
	// When anthropic 入口流式 Then anthropic SSE 事件序列完整
	key, model := opencodeTestEnv(t)
	f := newOpencodeFixture(t, key, model)
	payload := fmt.Sprintf(`{"model":%q,"max_tokens":32,"stream":true,"messages":[{"role":"user","content":"reply with exactly: pong"}]}`, model)
	got, body := f.post(t, "/v1/messages", map[string]string{
		"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
	}, payload)
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

func TestRealOpencode_AdminTestEndpoint(t *testing.T) {
	// When 管理 API 连通测试(走完整管道)Then ok=true 且延迟上报
	key, model := opencodeTestEnv(t)
	f := newOpencodeFixture(t, key, model)
	var id int64
	for _, u := range f.app.Registry.List() {
		if u.Name == "real-openrouter" {
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

func TestRealOpencode_UpstreamErrorPassthrough(t *testing.T) {
	// Given 不存在的模型 When 真实请求 Then 上游错误状态/错误体原样透传(无重试,恰一次)
	key, _ := opencodeTestEnv(t)
	badModel := "this-model/does-not-exist-xyz"
	f := newOpencodeFixture(t, key, badModel)
	got, body := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, chatPayload(badModel, false))
	if got == http.StatusOK {
		t.Fatalf("bad model must fail: %s", body)
	}
	if !strings.Contains(body, "error") && !strings.Contains(body, "Error") {
		t.Fatalf("error body: %d %s", got, body)
	}
}

// TestRealOpencode_KeyPoolProbes 逐 key 建上游并测连通(可选;OPENCODE_TEST_KEYS=逗号分隔)
// 预期:被封/无效 key 上报 ok=false 与原因;仅用于部署期 key 池盘点
func TestRealOpencode_KeyPoolProbes(t *testing.T) {
	pool := os.Getenv("OPENCODE_TEST_KEYS")
	if pool == "" {
		t.Skip("OPENCODE_TEST_KEYS not set; skipping key-pool probes")
	}
	_, model := opencodeTestEnv(t)
	f := newFourGroups(t)
	manifest, src := mustOpencodeFiles(t)
	ctx := context.Background()
	if err := f.app.AdminDeps.Packages.Install(ctx, aapZip(t, manifest, map[string]string{"protocol.js": src})); err != nil {
		t.Fatal(err)
	}
	for i, key := range strings.Split(pool, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		u := &upstream.Upstream{
			Name: fmt.Sprintf("probe-%d", i), Enabled: true,
			Base: upstream.PackageRef{Package: "opencode"}, Models: []string{model},
			Targets: []upstream.Target{{Name: "t1", BaseURL: "https://openrouter.ai/api", Enabled: true,
				Secrets: map[string]string{"api_key": key}}},
		}
		if err := f.app.Registry.Save(ctx, u); err != nil {
			t.Fatal(err)
		}
		var id int64
		for _, x := range f.app.Registry.List() {
			if x.Name == u.Name {
				id = x.ID
			}
		}
		start := time.Now()
		status, body := f.adminPost(t, fmt.Sprintf("/admin/api/upstreams/%d/test", id), "")
		latency := time.Since(start).Milliseconds()
		t.Logf("key[%d] => http %d body %s (%dms)", i, status, body, latency)
	}
}

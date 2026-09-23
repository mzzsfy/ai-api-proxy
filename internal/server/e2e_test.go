package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// buildTestApp 全装配 + 假上游 + 注册一条内置协议模型行(v2:连接=包参数,密钥=包级 keys)
func buildTestApp(t *testing.T, upstreamCT, upstreamBody string) (*App, *httptest.Server, *int) {
	t.Helper()
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: "admin",
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	hits := 0
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		// 记录鉴权头(快速路径注入断言)
		lastAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", upstreamCT)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(upSrv.Close)
	// 包级密钥 + 包参数 base_url(v2 连接信息归属包)
	if _, err := app.AdminDeps.Packages.Keys().Set("openai-compatible", "api_key", map[string]any{"api_key": "sk-upstream"}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.AdminDeps.Packages.Settings().Put("openai-compatible",
		plugin.PutInput{Config: map[string]any{"base_url": upSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	if err := app.Registry.Save(context.Background(), &upstream.Model{
		Name: "gpt-e2e", Enabled: true, Plugin: "openai-compatible",
	}); err != nil {
		t.Fatal(err)
	}
	return app, upSrv, &hits
}

// lastAuth 假上游最近收到的 Authorization
var lastAuth string

func postChat(t *testing.T, url, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestE2E_NonStreamFastPath(t *testing.T) {
	// Given 全装配+内置协议 When 非流请求 Then 200 原样透传 + 上游鉴权注入
	app, _, hits := buildTestApp(t, "application/json",
		`{"id":"c1","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`)
	srv := httptest.NewServer(app.Mux)
	defer srv.Close()
	body := `{"model":"gpt-e2e","messages":[{"role":"user","content":"hello"}]}`
	resp := postChat(t, srv.URL, "sk-test", body)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	if m["id"] != "c1" {
		t.Fatalf("body: %v", m)
	}
	if *hits != 1 {
		t.Fatalf("hits: %d", *hits)
	}
	if lastAuth != "Bearer sk-upstream" {
		t.Fatalf("upstream auth: %s", lastAuth)
	}
}

func TestE2E_UnknownModel404(t *testing.T) {
	// Given 未知模型 When 请求 Then 404
	app, _, _ := buildTestApp(t, "application/json", `{}`)
	srv := httptest.NewServer(app.Mux)
	defer srv.Close()
	resp := postChat(t, srv.URL, "sk-test", `{"model":"nope","messages":[]}`)
	if resp.StatusCode != 404 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestE2E_AuthRejected(t *testing.T) {
	// Given 错误 key When 请求 Then 401
	app, _, _ := buildTestApp(t, "application/json", `{}`)
	srv := httptest.NewServer(app.Mux)
	defer srv.Close()
	resp := postChat(t, srv.URL, "sk-wrong", `{"model":"gpt-e2e","messages":[]}`)
	if resp.StatusCode != 401 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestE2E_HealthzAndAdminAuth(t *testing.T) {
	// Given 服务装配 When /healthz Then 200;未登录管理 API Then 401
	app, _, _ := buildTestApp(t, "application/json", `{}`)
	srv := httptest.NewServer(app.Mux)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	resp2, err := http.Get(srv.URL + "/admin/api/models")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != 401 {
		t.Fatalf("admin unauth: %d", resp2.StatusCode)
	}
}

func TestE2E_StreamingPassthrough(t *testing.T) {
	// Given 上游 SSE(含 [DONE]) When 流式请求(快速路径) Then 原样透传
	app, _, _ := buildTestApp(t, "text/event-stream",
		"data: {\"id\":\"c\",\"delta\":{\"content\":\"a\"}}\n\ndata: [DONE]\n\n")
	srv := httptest.NewServer(app.Mux)
	defer srv.Close()
	resp := postChat(t, srv.URL, "sk-test", `{"model":"gpt-e2e","stream":true,"messages":[]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	outBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := string(outBytes)
	if !strings.Contains(out, "[DONE]") {
		t.Fatalf("sse: %q", out)
	}
}

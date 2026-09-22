package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 真实代理矩阵 E2E:clash(mihomo 混合端口 http+socks5,vless 系节点出口)× warp-pool(Cloudflare WARP 隧道) ───
//
// 依赖本机既有服务,缺席自动 skip:
//	clash 混合端口 127.0.0.1:6654(http_proxy 与 socks5 同端口)
// 上游为真实 OpenRouter(需 OPENCODE_TEST_KEY)。

const (
	clashMixedAddr = "127.0.0.1:6654"
)

func localProxyAlive(t *testing.T, addr string) bool {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// newRealTransportFixture 指定传输实例的真实上游环境(opencode 包 + OpenRouter + 真实代理)
func newRealTransportFixture(t *testing.T, apiKey, model, transportType, proxyURL string) *fourGroupsFixture {
	t.Helper()
	manifest, src := mustOpencodeFiles(t)
	adminHash, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{{Name: "real-" + transportType, URL: proxyURL}},
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	ctx := context.Background()
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, manifest, map[string]string{"protocol.js": src})); err != nil {
		t.Fatalf("install opencode pkg: %v", err)
	}
	if err := app.AdminDeps.Packages.Keys().Merge("opencode", map[string]any{"api_key": apiKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.AdminDeps.Packages.Settings().Put("opencode", plugin.PutInput{Config: map[string]any{
		"base_url": "https://openrouter.ai/api", "transport": "real-" + transportType}}); err != nil {
		t.Fatalf("put opencode params: %v", err)
	}
	if err := app.Registry.Save(ctx, &upstream.Model{
		Name: model, Plugin: "opencode", Enabled: true,
	}); err != nil {
		t.Fatalf("save model row: %v", err)
	}
	// 独立网关(不经 fourGroups 的 mock 上游/代理)
	gw := httptest.NewServer(app.Mux)
	t.Cleanup(gw.Close)
	return &fourGroupsFixture{app: app, gateway: gw, client: &http.Client{Timeout: 90 * time.Second}}
}

func TestRealTransports_ClashHTTPProxy(t *testing.T) {
	// Given clash 混合端口(http_proxy 形态)When 真实请求经该传输 Then 200 且内容真实
	key, model := opencodeTestEnv(t)
	if !localProxyAlive(t, clashMixedAddr) {
		t.Skipf("clash mixed port %s not listening", clashMixedAddr)
	}
	f := newRealTransportFixture(t, key, model, "http_proxy", "http://"+clashMixedAddr)
	got, body := pollinationsPost(t, f, "/v1/chat/completions", chatPayload(model, false), map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	})
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	if !strings.Contains(body, "pong") {
		t.Fatalf("body: %s", body)
	}
}

func TestRealTransports_ClashSocks5(t *testing.T) {
	// Given 同一 clash 端口的 socks5 形态 When 真实请求 Then 200(socks5 传输路径独立验证)
	key, model := opencodeTestEnv(t)
	if !localProxyAlive(t, clashMixedAddr) {
		t.Skipf("clash mixed port %s not listening", clashMixedAddr)
	}
	f := newRealTransportFixture(t, key, model, "socks5", "socks5://"+clashMixedAddr)
	got, body := pollinationsPost(t, f, "/v1/chat/completions", chatPayload(model, false), map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	})
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	if !strings.Contains(body, "pong") {
		t.Fatalf("body: %s", body)
	}
}

func TestRealTransports_DeadProxyFastFail(t *testing.T) {
	// Given 死代理(127.0.0.1:1)When 真实请求 Then 快速 502 且不挂起(恰一次,失败终局)
	key, model := opencodeTestEnv(t)
	f := newRealTransportFixture(t, key, model, "http_proxy", "http://127.0.0.1:1")
	start := time.Now()
	got, body := pollinationsPost(t, f, "/v1/chat/completions", chatPayload(model, false), map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	})
	elapsed := time.Since(start)
	if elapsed > 30*time.Second {
		t.Fatalf("dead proxy must fail fast, took %v", elapsed)
	}
	if got != http.StatusBadGateway {
		t.Fatalf("status %d body: %s", got, body)
	}
}

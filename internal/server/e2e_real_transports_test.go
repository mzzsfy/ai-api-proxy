package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 真实代理矩阵 E2E:clash(mihomo 混合端口 http+socks5,vless 系节点出口)× warp-pool(Cloudflare WARP 隧道) ───
//
// 依赖本机既有服务,缺席自动 skip:
//	clash 混合端口 127.0.0.1:6654(http_proxy 与 socks5 同端口)
//	warp-pool cmd/proxy 127.0.0.1:18080(HTTP CONNECT;go run ./cmd/proxy -listen 127.0.0.1:18080)
// 上游为真实 OpenRouter(需 OPENCODE_TEST_KEY)。
//
// 出口身份断言:两代理出口 IP 必须不同(证明走的是不同隧道而非同一路径)。

const (
	clashMixedAddr = "127.0.0.1:6654"
	warpProxyAddr  = "127.0.0.1:18080"
	egressEchoURL  = "https://api.ipify.org"
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

// proxyEgressIP 经指定代理取出口 IP(证明该代理可用且标识其隧道身份)
func proxyEgressIP(t *testing.T, proxyURL string) string {
	t.Helper()
	tr := &http.Transport{Proxy: http.ProxyURL(mustURL(t, proxyURL))}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	resp, err := client.Get(egressEchoURL)
	if err != nil {
		t.Fatalf("egress probe via %s: %v", proxyURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
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
		Transports: []TransportCfg{{Name: "real-" + transportType, Type: transportType, URL: proxyURL}},
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
	u := &upstream.Upstream{
		Name: "real-proxy-up", Enabled: true,
		Base:   upstream.PackageRef{Package: "opencode"},
		Models: []string{model},
		Targets: []upstream.Target{{Name: "t1",
			BaseURL:   "https://openrouter.ai/api",
			Transport: "real-" + transportType, Enabled: true,
			Secrets: map[string]string{"api_key": apiKey}}},
	}
	if err := app.Registry.Save(ctx, u); err != nil {
		t.Fatalf("save upstream: %v", err)
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

func TestRealTransports_WarpTunnelHTTP(t *testing.T) {
	// Given warp-pool(Cloudflare WARP 隧道)HTTP CONNECT 代理 When 真实请求 Then 200
	key, model := opencodeTestEnv(t)
	if !localProxyAlive(t, warpProxyAddr) {
		t.Skipf("warp proxy %s not listening (go run ./cmd/proxy -listen %s)", warpProxyAddr, warpProxyAddr)
	}
	f := newRealTransportFixture(t, key, model, "http_proxy", "http://"+warpProxyAddr)
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

func TestRealTransports_EgressIdentity(t *testing.T) {
	// Given 两条隧道同时可用 When 分别探出口 IP Then 出口不同(证明代理真实分路径,非同一出口)
	if !localProxyAlive(t, clashMixedAddr) || !localProxyAlive(t, warpProxyAddr) {
		t.Skip("both clash and warp must be running for egress identity assertion")
	}
	clashIP := proxyEgressIP(t, "http://"+clashMixedAddr)
	warpIP := proxyEgressIP(t, "http://"+warpProxyAddr)
	t.Logf("clash(vless node) egress: %s, warp(cf tunnel) egress: %s", clashIP, warpIP)
	if clashIP == "" || warpIP == "" {
		t.Fatal("empty egress ip")
	}
	if clashIP == warpIP {
		t.Fatalf("distinct tunnels must not share egress: %s", clashIP)
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

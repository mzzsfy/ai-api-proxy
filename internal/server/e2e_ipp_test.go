package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// mustNode map → yaml.Node(TransportCfg.Options 展开形态)
func mustNode(t *testing.T, m map[string]any) yaml.Node {
	t.Helper()
	b, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(b, &node); err != nil {
		t.Fatal(err)
	}
	return node
}

// ─── 全链路多路径联调:装配根(server.Build)级别验证 ipp_* 传输端到端 ───
// 路径:chat 请求 → gateway → ipTransport → ipp_clash(假 clash CONNECT)→ mock 上游 → 响应
// 同矩阵覆盖 ipp_remote(假供给方);warp/mihomo 真链路各自库内 e2e 已覆盖(env 门控)

// fakeClash 假 clash 混合端口(http CONNECT 隧道;任意目标透明转发)
type fakeClash struct {
	URL string
}

func newFakeClash(t *testing.T) *fakeClash {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		// 隧道:先 Hijack 再手写 200(net/http 对 CONNECT 的 WriteHeader 有吞响应语义)
		hj := w.(http.Hijacker)
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			return
		}
		defer up.Close()
		go func() { _, _ = ioCopy(up, conn) }()
		_, _ = ioCopy(conn, up)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = ln.Close()
	})
	return &fakeClash{URL: "http://" + ln.Addr().String()}
}

func ioCopy(dst, src net.Conn) (int64, error) {
	buf := make([]byte, 8192)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			m, werr := dst.Write(buf[:n])
			total += int64(m)
			if werr != nil {
				return total, werr
			}
		}
		if err != nil {
			return total, err
		}
	}
}

// ippFixture 联调环境:全装配 + ipp 传输 + mock 上游注册
type ippFixture struct {
	app     *App
	gateway *httptest.Server
	up      *httptest.Server
}

func newIPPFixture(t *testing.T, tr TransportCfg) *ippFixture {
	t.Helper()
	adminHash, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{tr},
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 亲和素材(X-IPP-*)不得泄漏到上游(R1 消费后剥离断言)
		if r.Header.Get("X-IPP-Session") != "" || r.Header.Get("X-IPP-Model") != "" {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"IPP header leaked"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"ipp-e2e","choices":[{"message":{"role":"assistant","content":"via-ipp"},"finish_reason":"stop"}]}`))
	}))
	t.Cleanup(up.Close)
	ctx := context.Background()
	if err := app.Registry.Save(ctx, &upstream.Upstream{
		Name: "ipp-up", Enabled: true,
		Base:   upstream.PackageRef{Package: "openai-compatible"},
		Models: []string{"m-ipp"},
		Targets: []upstream.Target{{Name: "t1",
			BaseURL: up.URL, Transport: tr.Name, Enabled: true,
			Secrets: map[string]string{"api_key": "sk-up"}}},
	}); err != nil {
		t.Fatalf("save upstream: %v", err)
	}
	gw := httptest.NewServer(app.Mux)
	t.Cleanup(gw.Close)
	return &ippFixture{app: app, gateway: gw, up: up}
}

func (f *ippFixture) chat(t *testing.T) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, f.gateway.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m-ipp","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// 路径 1:ipp_clash 全链路 —— 请求经假 clash 隧道到 mock 上游
func TestE2E_IPP_Clash全链路(t *testing.T) {
	clash := newFakeClash(t)
	f := newIPPFixture(t, TransportCfg{
		Name: "clash-out", Type: "ipp_clash",
		URL:     clash.URL,
		Options: mustNode(t, map[string]any{"type": "http_proxy", "url": clash.URL}),
	})
	status, body := f.chat(t)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if !strings.Contains(body, "via-ipp") {
		t.Fatalf("body=%s", body)
	}
	// 探测一轮后断言出口可见性(clash 单出口不可见 → egress_ips 空)
	stop := startTransportProbeLoop(f.app, time.Hour)
	stop()
	hs := transportHealthFromKV(t, f.app, "clash-out")
	if hs == nil || !hs.OK {
		t.Fatalf("health=%+v", hs)
	}
	if len(hs.EgressIPs) != 0 {
		t.Fatalf("clash egress must be invisible: %v", hs.EgressIPs)
	}
}

// 路径 2:ipp_remote 全链路 —— acquire 返回 proxy.url 指向假代理(隧道形态)到 mock 上游
func TestE2E_IPP_Remote全链路(t *testing.T) {
	clash := newFakeClash(t)
	var proxyURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/acquire", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"lease_id":"L1","proxy":{"type":"http","url":"%s"},"egress_ip":"9.9.9.9","capabilities":{"can_rotate_ip":true,"session_affinity":true}}`, proxyURL)))
	})
	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	prov := httptest.NewServer(mux)
	defer prov.Close()
	proxyURL = clash.URL // 供给方返回的代理形态端点

	f := newIPPFixture(t, TransportCfg{
		Name: "remote-out", Type: "ipp_remote",
		Options: mustNode(t, map[string]any{"base": prov.URL, "api_key": "k", "allow_insecure": true, "can_rotate_ip": true, "session_affinity": true}),
	})

	status, body := f.chat(t)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if !strings.Contains(body, "via-ipp") {
		t.Fatalf("body=%s", body)
	}
}

// 路径 3:ipp_remote 出口可见性 —— stats egress_ips 进健康 kv
func TestE2E_IPP_出口可见性(t *testing.T) {
	clash := newFakeClash(t)
	var proxyURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/acquire", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"lease_id":"L","proxy":{"type":"http","url":"%s"},"egress_ip":"7.7.7.7","capabilities":{}}`, proxyURL)))
	})
	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total":2,"normal":2,"egress_ips":["7.7.7.7","7.7.7.8"]}`))
	})
	prov := httptest.NewServer(mux)
	defer prov.Close()
	proxyURL = clash.URL

	f := newIPPFixture(t, TransportCfg{
		Name: "remote-vis", Type: "ipp_remote",
		Options: mustNode(t, map[string]any{"base": prov.URL, "api_key": "k", "allow_insecure": true}),
	})

	stop := startTransportProbeLoop(f.app, time.Hour)
	stop()
	hs := transportHealthFromKV(t, f.app, "remote-vis")
	if hs == nil {
		t.Fatal("no health record")
	}
	if len(hs.EgressIPs) != 2 || hs.EgressIPs[0] != "7.7.7.7" {
		t.Fatalf("egress_ips=%v", hs.EgressIPs)
	}
}

// 路径 4:未注册供给方类型配置拒绝启动
func TestE2E_IPP_未注册类型拒绝(t *testing.T) {
	adminHash, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{{Name: "bad", Type: "ipp_nonexistent"}},
	}
	if _, err := Build(cfg); err == nil {
		t.Fatal("want config rejection")
	}
}

// 路径 5:内置供给方注册名齐备
func TestE2E_IPP_注册名齐备(t *testing.T) {
	kinds := map[string]bool{}
	for _, k := range ipprovider.Kinds() {
		kinds[k] = true
	}
	for _, want := range []string{"ipp_warp", "ipp_clash", "ipp_remote"} {
		if !kinds[want] {
			t.Fatalf("missing builtin provider %s", want)
		}
	}
}

// 路径 6:无可用出口 → 503(契约:ErrNoExits 映射;区别于 502)
func TestE2E_IPP_无出口503(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/acquire", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"no_exits"}`))
	})
	prov := httptest.NewServer(mux)
	defer prov.Close()
	f := newIPPFixture(t, TransportCfg{
		Name: "noexit-out", Type: "ipp_remote",
		Options: mustNode(t, map[string]any{"base": prov.URL, "api_key": "k", "allow_insecure": true}),
	})
	status, body := f.chat(t)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s, want 503", status, body)
	}
}

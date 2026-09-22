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
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
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
// 同矩阵覆盖 ipp_remote(假供给方);真实外部代理经 e2e_real_transports(env 门控)

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
	if err := app.AdminDeps.Packages.Keys().Merge("openai-compatible", map[string]any{"api_key": "sk-up"}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.AdminDeps.Packages.Settings().Put("openai-compatible",
		plugin.PutInput{Config: map[string]any{"base_url": up.URL}}); err != nil {
		t.Fatal(err)
	}
	if err := app.Registry.Save(ctx, &upstream.Model{
		Name: "m-ipp", Plugin: "openai-compatible", Enabled: true,
		Params: map[string]any{"transport": tr.Name},
	}); err != nil {
		t.Fatalf("save model row: %v", err)
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

// adminEvict 管理面 evict 调用(登录 + POST)
func (f *ippFixture) adminEvict(t *testing.T, name, body string) int {
	t.Helper()
	lr, _ := http.NewRequest(http.MethodPost, f.gateway.URL+"/admin/api/login",
		strings.NewReader(`{"user":"`+adminTestUser+`","password":"`+adminTestPass+`"}`))
	lr.Header.Set("Content-Type", "application/json")
	lresp, err := http.DefaultClient.Do(lr)
	if err != nil {
		t.Fatal(err)
	}
	defer lresp.Body.Close()
	var sess *http.Cookie
	for _, c := range lresp.Cookies() {
		if c.Name == "aap_session" {
			sess = c
		}
	}
	if sess == nil {
		t.Fatal("no session cookie")
	}
	req, _ := http.NewRequest(http.MethodPost, f.gateway.URL+"/admin/api/transports/"+name+"/evict", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(sess)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// 路径 1:ipp_clash 全链路 —— 请求经假 clash 隧道到 mock 上游
func TestE2E_IPP_Clash全链路(t *testing.T) {
	clash := newFakeClash(t)
	f := newIPPFixture(t, TransportCfg{
		Name:    "clash-out",
		URL:     "ipp+clash://" + clash.URL,
		Options: mustNode(t, map[string]any{"type": "http_proxy", "url": clash.URL}),
	})
	status, body := f.chat(t)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if !strings.Contains(body, "via-ipp") {
		t.Fatalf("body=%s", body)
	}
	// 连通测试一次,断言链路可用(clash 单出口 → egress_ips 空)
	latency, err := f.app.trMgr.Test("clash-out", transportProbeURL, transportProbeTimeout)
	if err != nil {
		t.Fatalf("clash-out test: %v (latency=%dms)", err, latency)
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
		Name:    "remote-out",
		URL:     "ipp+remote://prov",
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

// 路径 3:ipp_remote 出口可见性 —— provider stats 暴露 egress_ips
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
		Name:    "remote-vis",
		URL:     "ipp+remote://prov",
		Options: mustNode(t, map[string]any{"base": prov.URL, "api_key": "k", "allow_insecure": true}),
	})

	provider, ok := f.app.trMgr.Provider("remote-vis")
	if !ok {
		t.Fatal("provider remote-vis not found")
	}
	ips := provider.Stats().EgressIPs
	if len(ips) != 2 || ips[0] != "7.7.7.7" {
		t.Fatalf("egress_ips=%v", ips)
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
		Transports: []TransportCfg{{Name: "bad", URL: "ipp+nonexistent://x"}},
	}
	if _, err := Build(cfg); err == nil {
		t.Fatal("want config rejection")
	}
}

// 路径 5:内置供给方注册名齐备(默认构建全量集)
func TestE2E_IPP_注册名齐备(t *testing.T) {
	kinds := map[string]bool{}
	for _, k := range ipprovider.Kinds() {
		kinds[k] = true
	}
	for _, want := range []string{"ipp_clash", "ipp_remote", "aap"} {
		if !kinds[want] {
			t.Fatalf("missing builtin provider %s", want)
		}
	}
}

// 路径 7:admin evict 端点 —— 非法 scope 400;非 aap 传输 400;成功 200
func TestE2E_IPP_Evict端点(t *testing.T) {
	f := newIPPFixture(t, TransportCfg{
		Name:    "clash-out",
		URL:     "ipp+clash://clash",
		Options: mustNode(t, map[string]any{"type": "http_proxy", "url": "http://127.0.0.1:1"}),
	})
	// 非法 scope → 400
	status := f.adminEvict(t, "clash-out", `{"scope":"session","value":"x"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("bad scope: status=%d want 400", status)
	}
	// 非 aap 传输(无 Evictor)→ 400
	status = f.adminEvict(t, "clash-out", `{"scope":"egress","value":"1.2.3.4"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("non-aap transport: status=%d want 400", status)
	}
	// 全零 lease → 400
	status = f.adminEvict(t, "clash-out", `{"scope":"lease","value":"`+strings.Repeat("0", 32)+`"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("zero lease: status=%d want 400", status)
	}
	// 不存在的传输 → 400
	status = f.adminEvict(t, "nope", `{"scope":"egress","value":"1.2.3.4"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("missing transport: status=%d want 400", status)
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
		Name:    "noexit-out",
		URL:     "ipp+remote://prov",
		Options: mustNode(t, map[string]any{"base": prov.URL, "api_key": "k", "allow_insecure": true}),
	})
	status, body := f.chat(t)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s, want 503", status, body)
	}
}

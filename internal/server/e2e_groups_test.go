package server

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 4 组真实插件端到端:直连/代理 × 无/有请求修改 × chat/message × 流/非流 ───

// upstreamSpy 假上游观测(线程安全)
type upstreamSpy struct {
	hits      atomic.Int64
	mu        sync.Mutex
	lastAuth  string
	lastModel string
	lastPath  string
}

func (s *upstreamSpy) snapshot() (auth, model, path string, hits int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuth, s.lastModel, s.lastPath, s.hits.Load()
}

// openaiCompletionNonStream 非流式完成体
func openaiCompletionNonStream(model string) string {
	b, _ := json.Marshal(map[string]any{
		"id": "chatcmpl-e2e", "object": "chat.completion", "model": model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": "hi"},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
	})
	return string(b)
}

// openaiChunkStream 流式帧体(单 delta + [DONE])
const openaiChunkStream = "data: {\"id\":\"chatcmpl-e2e\",\"model\":\"%s\",\"delta\":{\"content\":\"hi\"}}\n\ndata: [DONE]\n\n"

// newMockUpstream 真实 HTTP 假上游:按请求 stream 标志分流式/非流式
func newMockUpstream(t *testing.T) (*httptest.Server, *upstreamSpy) {
	t.Helper()
	spy := &upstreamSpy{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		_ = json.Unmarshal(body, &req)
		spy.mu.Lock()
		spy.lastAuth = r.Header.Get("Authorization")
		spy.lastModel = req.Model
		spy.lastPath = r.URL.Path
		spy.mu.Unlock()
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, openaiChunkStream, req.Model)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiCompletionNonStream(req.Model)))
	}))
	t.Cleanup(srv.Close)
	return srv, spy
}

// newForwardProxy 真实 HTTP forward 代理(绝对 URI 转发;不走环境代理)
func newForwardProxy(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	out := &http.Transport{} // Proxy=nil 直连
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Host == "" {
			http.Error(w, "proxy requires absolute uri", http.StatusBadRequest)
			return
		}
		hits.Add(1)
		req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := out.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

// aapZip 构造 .aap(内存 zip)
func aapZip(t *testing.T, manifest string, files map[string]string) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mf.Write([]byte(manifest)); err != nil {
		t.Fatal(err)
	}
	for name, src := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(src)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const jsProtoManifest = `{"manifestVersion":1,"name":"js-openai","version":"1.0.0","parts":{
	"protocol":{"entry":"p.js","form":["streaming","non_streaming"],"features":["tools","vision"],"secretRefs":["api_key"]}}}`

// jsProtoSrc 真实 JS 协议部件:构造请求/帧解包/响应透传
const jsProtoSrc = `module.exports = {
	buildRequest: function (ctx, pivot) {
		return {
			url: ctx.target.baseUrl + "/v1/chat/completions",
			method: "POST",
			headers: { "Content-Type": "application/json", "Authorization": "Bearer " + util.secret("api_key") },
			body: pivot,
			stream: ctx.vars.entryStream
		};
	},
	mapEvent: function (ctx, e) {
		var f = JSON.parse(e);
		if (f.data === "[DONE]") return null;
		return f.data;
	},
	mapResponse: function (ctx, body) { return body; }
};`

const rewriteManifest = `{"manifestVersion":1,"name":"rewrite-model","version":"1.0.0","parts":{
	"filters":[{"name":"rewrite","entry":"f.js","configSchema":{"type":"object","properties":{"model":{"type":"string"}},"required":["model"]}}]}}`

// rewriteSrc 真实 JS 修改部件:改写请求模型名
const rewriteSrc = `module.exports = function (config) {
	var target = (config && config.model) || "";
	return {
		mapRequest: function (ctx, pivot) {
			if (!target) return pivot;
			var o = JSON.parse(pivot);
			o.model = target;
			return JSON.stringify(o);
		},
		mapChunk: function (ctx, c) { return c; },
		mapResponse: function (ctx, r) { return r; }
	};
};`

// fourGroupsFixture 4 组环境:真实网关服务 + 假上游 + forward 代理 + 2 真实插件包 + 4 上游实例
type fourGroupsFixture struct {
	app          *App
	gateway      *httptest.Server
	upstreamSrv  *httptest.Server
	spy          *upstreamSpy
	proxySrv     *httptest.Server
	proxyHits    *atomic.Int64
	client       *http.Client
	upstreamBase string
	adminCookie  *http.Cookie
}

const (
	upstreamAPIKey   = "sk-upstream-e2e"
	upstreamRewriteM = "rewritten-by-filter"
	adminTestUser    = "admin"
	adminTestPass    = "e2e-admin-pass"
)

func newFourGroups(t *testing.T) *fourGroupsFixture {
	t.Helper()
	upSrv, spy := newMockUpstream(t)
	proxySrv, proxyHits := newForwardProxy(t)
	adminHash, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{{Name: "px", Type: "http_proxy", URL: proxySrv.URL}},
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	// 安装 2 个真实插件包
	ctx := context.Background()
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, jsProtoManifest, map[string]string{"p.js": jsProtoSrc})); err != nil {
		t.Fatal(err)
	}
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, rewriteManifest, map[string]string{"f.js": rewriteSrc})); err != nil {
		t.Fatal(err)
	}
	// 4 组上游实例:(直连/代理)×(无/有请求修改)
	mkUpstream := func(name, model, transport string, withFilter bool) {
		u := &upstream.Upstream{
			Name: name, Enabled: true,
			Base:   upstream.PackageRef{Package: "js-openai"},
			Models: []string{model},
			Targets: []upstream.Target{{Name: "t1", BaseURL: upSrv.URL, Transport: transport, Enabled: true,
				Secrets: map[string]string{"api_key": upstreamAPIKey}}},
		}
		if withFilter {
			u.Extras = []upstream.PackageRef{{Package: "rewrite-model"}}
			u.FilterParams = map[string]map[string]any{"rewrite-model/rewrite": {"model": upstreamRewriteM}}
		}
		if err := app.Registry.Save(ctx, u); err != nil {
			t.Fatalf("save %s: %v", name, err)
		}
	}
	mkUpstream("g1-direct-nofilter", "m-direct", "", false)
	mkUpstream("g2-proxy-nofilter", "m-proxy", "px", false)
	mkUpstream("g3-direct-filter", "mf-direct", "", true)
	mkUpstream("g4-proxy-filter", "mf-proxy", "px", true)
	gateway := httptest.NewServer(app.Mux)
	t.Cleanup(gateway.Close)
	return &fourGroupsFixture{
		app: app, gateway: gateway, upstreamSrv: upSrv, spy: spy, proxySrv: proxySrv, proxyHits: proxyHits,
		client:       &http.Client{Timeout: 10 * time.Second},
		upstreamBase: upSrv.URL,
	}
}

// post 真实 HTTP 请求入口
func (f *fourGroupsFixture) post(t *testing.T, path string, headers map[string]string, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// scenario 单场景参数
type scenario struct {
	group  string // 组名(模型名即路由键)
	entry  string // chat | message
	stream bool
	proxy  bool // 断言经代理
	filter bool // 断言请求被改写
}

// fourGroupScenarios 4 组 × chat/message × 流/非流
func fourGroupScenarios() []scenario {
	return []scenario{
		{group: "m-direct", entry: "chat", stream: false, proxy: false, filter: false},
		{group: "m-direct", entry: "chat", stream: true, proxy: false, filter: false},
		{group: "m-direct", entry: "message", stream: false, proxy: false, filter: false},
		{group: "m-direct", entry: "message", stream: true, proxy: false, filter: false},

		{group: "m-proxy", entry: "chat", stream: false, proxy: true, filter: false},
		{group: "m-proxy", entry: "chat", stream: true, proxy: true, filter: false},
		{group: "m-proxy", entry: "message", stream: false, proxy: true, filter: false},
		{group: "m-proxy", entry: "message", stream: true, proxy: true, filter: false},

		{group: "mf-direct", entry: "chat", stream: false, proxy: false, filter: true},
		{group: "mf-direct", entry: "chat", stream: true, proxy: false, filter: true},
		{group: "mf-direct", entry: "message", stream: false, proxy: false, filter: true},
		{group: "mf-direct", entry: "message", stream: true, proxy: false, filter: true},

		{group: "mf-proxy", entry: "chat", stream: false, proxy: true, filter: true},
		{group: "mf-proxy", entry: "chat", stream: true, proxy: true, filter: true},
		{group: "mf-proxy", entry: "message", stream: false, proxy: true, filter: true},
		{group: "mf-proxy", entry: "message", stream: true, proxy: true, filter: true},
	}
}

// runScenarios 对 base 网关执行 4 组全部场景断言(proxyHits 代理命中读数)
func runScenarios(t *testing.T, base string, proxyHits func() int64, spy *upstreamSpy) {
	t.Helper()
	runScenariosTable(t, base, proxyHits, spy, fourGroupScenarios(), true)
}

// runScenariosTable 按给定场景表执行断言(4 组与 socks5 共用)
// perReqRelay:代理命中须逐请求递增(http_proxy handler 计数成立;socks5 隧道复用不成立)
func runScenariosTable(t *testing.T, base string, proxyHits func() int64, spy *upstreamSpy, scenarios []scenario, perReqRelay bool) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	post := func(path string, headers map[string]string, body string) (int, string) {
		req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	for _, sc := range scenarios {
		name := fmt.Sprintf("%s/%s/stream=%v", sc.group, sc.entry, sc.stream)
		t.Run(name, func(t *testing.T) {
			proxyBefore := proxyHits()
			var status int
			var body string
			if sc.entry == "chat" {
				payload := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}],"stream":%v}`, sc.group, sc.stream)
				status, body = post("/v1/chat/completions", map[string]string{
					"Authorization": "Bearer sk-test", "Content-Type": "application/json",
				}, payload)
			} else {
				payload := fmt.Sprintf(`{"model":%q,"max_tokens":10,"stream":%v,"messages":[{"role":"user","content":"hello"}]}`, sc.group, sc.stream)
				status, body = post("/v1/messages", map[string]string{
					"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
				}, payload)
			}
			if status != http.StatusOK {
				t.Fatalf("status %d body: %s", status, body)
			}
			auth, model, path, hits := spy.snapshot()
			if hits < 1 {
				t.Fatal("upstream not hit")
			}
			if auth != "Bearer "+upstreamAPIKey {
				t.Fatalf("upstream auth: %q", auth)
			}
			if path != "/v1/chat/completions" {
				t.Fatalf("upstream path: %q", path)
			}
			wantModel := sc.group
			if sc.filter {
				wantModel = upstreamRewriteM
			}
			if model != wantModel {
				t.Fatalf("upstream model: got %q want %q", model, wantModel)
			}
			proxyAfter := proxyHits()
			if sc.proxy && perReqRelay && proxyAfter <= proxyBefore {
				t.Fatal("proxy not traversed")
			}
			if !sc.proxy && proxyAfter > proxyBefore {
				t.Fatal("unexpected proxy traversal")
			}
			// 形态断言
			if sc.entry == "chat" {
				if sc.stream {
					if !strings.Contains(body, "data: ") || !strings.Contains(body, "[DONE]") {
						t.Fatalf("chat stream shape: %q", body)
					}
					if !strings.Contains(body, `"content":"hi"`) {
						t.Fatalf("chat stream content: %q", body)
					}
				} else {
					var m map[string]any
					if err := json.Unmarshal([]byte(body), &m); err != nil {
						t.Fatalf("chat json: %v body %s", err, body)
					}
					if m["object"] != "chat.completion" {
						t.Fatalf("chat object: %v", m["object"])
					}
					if !strings.Contains(body, `"content":"hi"`) {
						t.Fatalf("chat content: %s", body)
					}
				}
			} else {
				if sc.stream {
					for _, want := range []string{"message_start", "content_block_delta", "message_stop"} {
						if !strings.Contains(body, want) {
							t.Fatalf("message stream missing %s: %q", want, body)
						}
					}
					if strings.Contains(body, "[DONE]") {
						t.Fatal("anthropic stream must not emit [DONE]")
					}
					if !strings.Contains(body, "hi") {
						t.Fatalf("message stream content: %q", body)
					}
				} else {
					var m map[string]any
					if err := json.Unmarshal([]byte(body), &m); err != nil {
						t.Fatalf("message json: %v body %s", err, body)
					}
					if m["type"] != "message" {
						t.Fatalf("message type: %v body %s", m["type"], body)
					}
					if !strings.Contains(body, `"text":"hi"`) {
						t.Fatalf("message content: %s", body)
					}
				}
			}
		})
	}
}

func TestE2E_FourGroups_RealPlugins(t *testing.T) {
	// Given 真实 JS 协议插件+rewrite 修改插件+真实 forward 代理+真实网关 HTTP 服务
	// When 4 组 × chat/message × 流/非流 共 16 次真实请求
	// Then 全部 200 且入口形态/流形态/代理命中/请求改写/上游鉴权全部正确
	f := newFourGroups(t)
	runScenarios(t, f.gateway.URL, func() int64 { return f.proxyHits.Load() }, f.spy)
}

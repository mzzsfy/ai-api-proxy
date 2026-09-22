package server

import (
	"archive/zip"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
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

// openaiCompletionNonStream 非流式完成体;overrides 覆盖响应对象字段(anthropic 声明变体)
func openaiCompletionNonStream(model string, overrides map[string]any) string {
	obj := map[string]any{
		"id": "chatcmpl-e2e", "object": "chat.completion", "model": model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": "hi"},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
	}
	for k, v := range overrides {
		if k == "message_content" {
			obj["content"] = []any{map[string]any{"type": "text", "text": v}}
			continue
		}
		obj[k] = v
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

// anthropicOverrides /v1/messages 路径(anthropic 声明上游)的响应对象覆盖字段
func anthropicOverrides(path string) map[string]any {
	if path != "/v1/messages" {
		return nil
	}
	return map[string]any{"type": "message", "message_content": "hi"}
}

// openaiChunkStream 流式帧体(单 delta + [DONE];overrides 覆盖响应对象字段,用于 anthropic 声明变体)
func openaiChunkStream(model string, overrides map[string]any) string {
	obj := map[string]any{"id": "chatcmpl-e2e", "model": model, "delta": map[string]any{"content": "hi"}}
	for k, v := range overrides {
		obj[k] = v
	}
	b, _ := json.Marshal(obj)
	return "data: " + string(b) + "\n\ndata: [DONE]\n\n"
}

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
			_, _ = fmt.Fprintf(w, "%s", openaiChunkStream(req.Model, anthropicOverrides(r.URL.Path)))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiCompletionNonStream(req.Model, anthropicOverrides(r.URL.Path))))
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

const jsProtoManifest = `{"manifestVersion":1,"name":"js-openai","version":"2.0.0",
	"parts":{"protocol":{"protocol":"openai-completions","features":["tools","vision"]},
	"filters":[{"name":"rewrite"}]}}`

// jsProtoAnthropicManifest 同一部件源码的另一协议声明(anthropic-messages)
const jsProtoAnthropicManifest = `{"manifestVersion":1,"name":"js-anthropic","version":"2.0.0",
	"parts":{"protocol":{"protocol":"anthropic-messages","features":["tools","vision"]}}}`

// jsProtoSrc 真实 JS 协议部件:构造请求/帧解包(声明协议事件数组)/响应透传
// v2:连接=包参数(config.base_url/protocol/transport 槽),密钥=包级 keys(util.key);
// rewrite filter 并入本包(filters/rewrite.js),其参数槽 model 同包声明
const jsProtoSrc = `module.exports = function (config) {
	var DECLARED = (config && config.protocol) || "openai-completions";
	return {
	buildRequest: function (ctx, entry) {
		var ant = DECLARED === "anthropic-messages";
		return {
			url: config.base_url + (ant ? "/v1/messages" : "/v1/chat/completions"),
			method: "POST",
			headers: { "Content-Type": "application/json", "Authorization": "Bearer " + util.key("api_key") },
			body: entry,
			stream: ctx.vars.entryStream,
			transport: (config.transport && config.transport !== "direct") ? config.transport : null
		};
	},
	mapEvent: function (ctx, e) {
		var f = JSON.parse(e);
		if (f.data === "[DONE]") return null;
		var o = JSON.parse(f.data);
		if (DECLARED === "anthropic-messages") {
			var out = [];
			if (o.delta) {
				out.push({ type: "message_start", message: { role: "assistant" } });
				out.push({ type: "content_block_delta", delta: { type: "text_delta", text: o.delta.content } });
			}
			out.push({ type: "message_stop" });
			return JSON.stringify(out);
		}
		return JSON.stringify([o]);
	},
	mapResponse: function (ctx, body) { return body; }
	};
};
module.exports.settings = {
	base_url: setting.string({ description: "上游地址", required: true }),
	protocol: setting.string({ description: "协议名", required: true }),
	transport: setting.string({ description: "出站传输实例名(direct=内置直连;命名实例在 config.yaml transports 配置)" })
};`

const rewriteManifest = `{"manifestVersion":1,"name":"rewrite-model","version":"2.0.0","parts":{
	"filters":[{"name":"rewrite"}]}}`

// rewriteSrc 真实 JS 修改部件:改写请求模型名(参数槽由同文件 settings 片段声明)
const rewriteSrc = `module.exports = function (config) {
	var target = (config && config.model) || "";
	return {
		mapRequest: function (ctx, entry) {
			if (!target) return entry;
			var o = JSON.parse(entry);
			o.model = target;
			return JSON.stringify(o);
		},
		mapChunk: function (ctx, c) { return c; },
		mapResponse: function (ctx, r) { return r; }
	};
};
module.exports.settings = {
	model: setting.string({ description: "目标模型名" })
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

// newFourGroups 默认夹具:真实网关 + 假上游 + 本地转发代理 + 2 真实插件包 + 4 上游实例;
// 网关客户端超时按本地假上游给(真实上游 E2E 用 newFourGroupsWith 放宽)
func newFourGroups(t *testing.T) *fourGroupsFixture {
	t.Helper()
	return newFourGroupsWith(t, 10*time.Second)
}

// newFourGroupsWith 指定网关客户端超时的夹具(真实网络 E2E 需要更长等待)
// extraTransports 追加命名传输实例(真实网络 E2E 从环境注入真实出口用)
func newFourGroupsWith(t *testing.T, clientTimeout time.Duration, extraTransports ...TransportCfg) *fourGroupsFixture {
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
		Transports: append([]TransportCfg{{Name: "px", URL: proxySrv.URL}}, extraTransports...),
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	// 安装 2 个真实插件包(v2:协议包内含 rewrite filter 源,独立 rewrite 包仅用于包粒度装包验证)
	ctx := context.Background()
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, jsProtoManifest, map[string]string{
		plugin.ProtocolEntry: jsProtoSrc, "filters/rewrite.js": rewriteSrc})); err != nil {
		t.Fatal(err)
	}
	// 同源码的 anthropic 协议声明包(声明式单协议:入口 anthropic 需行绑 anthropic-messages 包)
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, jsProtoAnthropicManifest, map[string]string{
		plugin.ProtocolEntry: jsProtoSrc})); err != nil {
		t.Fatal(err)
	}
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, rewriteManifest, map[string]string{"filters/rewrite.js": rewriteSrc})); err != nil {
		t.Fatal(err)
	}
	// 包级密钥 + 包参数(v2 连接信息归属包;行 params 仅覆盖差异槽)
	for _, pkg := range []string{"js-openai", "js-anthropic"} {
		if err := app.AdminDeps.Packages.Keys().Merge(pkg, map[string]any{"api_key": upstreamAPIKey}); err != nil {
			t.Fatal(err)
		}
		proto := "openai-completions"
		if pkg == "js-anthropic" {
			proto = "anthropic-messages"
		}
		if _, err := app.AdminDeps.Packages.Settings().Put(pkg, plugin.PutInput{Config: map[string]any{
			"base_url": upSrv.URL, "protocol": proto}}); err != nil {
			t.Fatal(err)
		}
	}
	// mkRow 建模型行:行名即模型名;transport/rewrite 目标模型以行 params 覆盖
	mkRow := func(name, pkg string, params map[string]any) {
		m := &upstream.Model{Name: name, Plugin: pkg, Enabled: true, Params: params}
		if err := app.Registry.Save(ctx, m); err != nil {
			t.Fatalf("save %s: %v", name, err)
		}
	}
	mkRow("m-direct", "js-openai", nil)
	mkRow("m-proxy", "js-openai", map[string]any{"transport": "px"})
	mkRow("mf-direct", "js-openai", map[string]any{"model": upstreamRewriteM})
	mkRow("mf-proxy", "js-openai", map[string]any{"transport": "px", "model": upstreamRewriteM})
	mkRow("m-anthropic", "js-anthropic", nil)
	gateway := httptest.NewServer(app.Mux)
	t.Cleanup(gateway.Close)
	return &fourGroupsFixture{
		app: app, gateway: gateway, upstreamSrv: upSrv, spy: spy, proxySrv: proxySrv, proxyHits: proxyHits,
		client:       &http.Client{Timeout: clientTimeout},
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

// fourGroupScenarios 4 组 × chat/message × 流/非流 + anthropic 声明组 × message 流/非流
func fourGroupScenarios() []scenario {
	return []scenario{
		{group: "m-direct", entry: "chat", stream: false, proxy: false, filter: false},
		{group: "m-direct", entry: "chat", stream: true, proxy: false, filter: false},

		{group: "m-proxy", entry: "chat", stream: false, proxy: true, filter: false},
		{group: "m-proxy", entry: "chat", stream: true, proxy: true, filter: false},

		{group: "mf-direct", entry: "chat", stream: false, proxy: false, filter: true},
		{group: "mf-direct", entry: "chat", stream: true, proxy: false, filter: true},

		{group: "mf-proxy", entry: "chat", stream: false, proxy: true, filter: true},
		{group: "mf-proxy", entry: "chat", stream: true, proxy: true, filter: true},

		// anthropic 声明组:入口 anthropic 与上游声明同协议(声明式单协议)
		{group: "m-anthropic", entry: "message", stream: false, proxy: false, filter: false},
		{group: "m-anthropic", entry: "message", stream: true, proxy: false, filter: false},
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
			wantPath := "/v1/chat/completions"
			if sc.entry == "message" {
				wantPath = "/v1/messages"
			}
			if path != wantPath {
				t.Fatalf("upstream path: %q want %q", path, wantPath)
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

package gateway

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/builtin"
	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"

	_ "modernc.org/sqlite"
)

// memSecrets 内存凭据
type memSecrets struct{ m map[string]map[string]string }

func (s *memSecrets) UpsertTargetSecrets(upstream, target string, secrets map[string]string) error {
	k := upstream + "/" + target
	if s.m[k] == nil {
		s.m[k] = map[string]string{}
	}
	for kk, v := range secrets {
		s.m[k][kk] = v
	}
	return nil
}
func (s *memSecrets) GetTargetSecrets(upstream, target string) (map[string]string, bool) {
	v, ok := s.m[upstream+"/"+target]
	return v, ok
}
func (s *memSecrets) DeleteTargetSecrets(upstream, target string) {
	delete(s.m, upstream+"/"+target)
}

// fixture 完整网关环境
type fixture struct {
	g           *Gateway
	reg         *upstream.Registry
	tr          *transport.Manager
	upstreamSrv *httptest.Server
	seenBody    []byte
	seenAuth    string
	seenCount   int
}

func newFixture(t *testing.T, upstreamStatus int, upstreamCT, upstreamBody string) *fixture {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/t.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filter_params_json TEXT NOT NULL DEFAULT '{}',
			filters_enabled_json TEXT NOT NULL DEFAULT '{}', strategy_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')))`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	f := &fixture{}
	// 假上游
	f.upstreamSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r.Body)
		f.seenBody = body
		f.seenAuth = r.Header.Get("Authorization")
		f.seenCount++
		w.Header().Set("Content-Type", upstreamCT)
		w.WriteHeader(upstreamStatus)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(f.upstreamSrv.Close)
	// 包与上游(名字=内置协议名,走 Go 内置工厂注册)
	pkgs := plugin.NewRegistry(db)
	pkgs.RegisterBuiltin("openai-compatible", func(deps plugin.BuiltinDeps) (pipeline.Protocol, error) {
		return &builtin.Protocol{TargetSecrets: deps.TargetSecrets}, nil
	})
	manifest := `{"manifestVersion":1,"name":"openai-compatible","version":"1","parts":{
		"protocol":{"protocol":"openai-completions","features":["tools","vision"],"secretRefs":["api_key"]}}}`
	protoSrc := `module.exports = {};`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: protoSrc})); err != nil {
		t.Fatal(err)
	}
	secrets := &memSecrets{m: map[string]map[string]string{}}
	reg := upstream.NewRegistry(db, pkgs, secrets)
	u := &upstream.Upstream{Name: "u1", Enabled: true, Base: upstream.PackageRef{Package: "openai-compatible"},
		Models: []string{"test-model"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: f.upstreamSrv.URL, Enabled: true,
			Secrets: map[string]string{"api_key": "sk-live-key"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	trMgr, err := transport.NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	f.reg = reg
	f.tr = trMgr
	f.g = &Gateway{
		OpenAI:    openaiEntry(),
		Anthropic: anthropicEntry(),
		Executor:  &pipeline.Executor{Transports: trMgr.Get},
		Registry:  reg,
		Metrics:   metrics.NewRecorder(),
		Secrets:   secrets,
	}
	return f
}

// openaiEntry openai 入口装配
func openaiEntry() Entry {
	c := convert.NewOpenAICodec()
	return Entry{EntryInspector: c, ErrorRenderer: c, FramerFactory: c, Protocol: pipeline.ProtocolOpenAICompletions}
}

// anthropicEntry anthropic 入口装配
func anthropicEntry() Entry {
	c := convert.NewAnthropicCodec()
	return Entry{EntryInspector: c, ErrorRenderer: c, FramerFactory: c, Protocol: pipeline.ProtocolAnthropicMessages}
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, nil
		}
	}
}

// installJSProtocol 换装 JS 协议包(非内置,走完整管道;上游 SSE 直通语义)
func (f *fixture) installJSProtocol(t *testing.T) {
	t.Helper()
	f.installJSProtocolAs(t, "openai-completions")
}

// installJSProtocolAs 装 JS 协议包并换装注册表(协议可指定;形态由实现推导)
func (f *fixture) installJSProtocolAs(t *testing.T, protocol string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/js.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filter_params_json TEXT NOT NULL DEFAULT '{}',
			filters_enabled_json TEXT NOT NULL DEFAULT '{}', strategy_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	pkgs := plugin.NewRegistry(db)
	manifest := `{"manifestVersion":1,"name":"js-proto","version":"1","parts":{
		"protocol":{"protocol":"` + protocol + `","features":["tools","vision"],"secretRefs":["api_key"]}}}`
	src := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			var key = util.secret("api_key");
			return { url: ctx.target.baseUrl + "/v1/chat/completions", method: "POST",
				headers: {"Content-Type": "application/json", "Authorization": "Bearer " + key},
				body: entry, stream: ctx.vars.entryStream };
		},
		mapResponse: function (ctx, b) { return b; },
		mapEvent: function (ctx, e) { var f = JSON.parse(e); return JSON.stringify([JSON.parse(f.data)]); } };
	};`
	if protocol == "anthropic-messages" {
		src = `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			var key = util.secret("api_key");
			return { url: ctx.target.baseUrl + "/v1/messages", method: "POST",
				headers: {"Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01"},
				body: entry, stream: ctx.vars.entryStream };
		},
		mapResponse: function (ctx, b) { return b; } };
	};`
	}
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: src})); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(db, pkgs, f.g.Secrets)
	u := &upstream.Upstream{Name: "u1", Enabled: true, Base: upstream.PackageRef{Package: "js-proto"},
		Models: []string{"test-model"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: f.upstreamSrv.URL, Enabled: true,
			Secrets: map[string]string{"api_key": "sk-live-key"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	f.reg = reg
	f.g.Registry = reg
}

func buildZip(t *testing.T, manifest string, files map[string]string) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = mf.Write([]byte(manifest))
	for name, content := range files {
		w, _ := zw.Create(name)
		_, _ = w.Write([]byte(content))
	}
	_ = zw.Close()
	return buf.Bytes()
}

func TestChatCompletions_NonStream_FastPath(t *testing.T) {
	// Given 内置协议+空 filter 链 When 非流请求 Then 快速路径:body 原样达上游 + Bearer 注入
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`)
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	if w.Code != 200 {
		t.Fatalf("status: %d %s", w.Code, w.Body.String())
	}
	if string(f.seenBody) != body {
		t.Fatalf("body not passthrough: %s", f.seenBody)
	}
	if f.seenAuth != "Bearer sk-live-key" {
		t.Fatalf("auth: %s", f.seenAuth)
	}
}

func TestChatCompletions_UnknownModel404(t *testing.T) {
	// Given 未知模型 When 请求 Then 404 且错误体含可用模型
	f := newFixture(t, 200, "application/json", `{}`)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"nope","messages":[]}`))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	if w.Code != 404 {
		t.Fatalf("status: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "test-model") {
		t.Fatalf("available models listed: %s", w.Body.String())
	}
}

func TestChatCompletions_NGreaterThanOne400(t *testing.T) {
	// Given n=2 When 请求 Then 400
	f := newFixture(t, 200, "application/json", `{}`)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test-model","n":2,"messages":[]}`))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	if w.Code != 400 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestChatCompletions_UpstreamTerminal(t *testing.T) {
	// Given 上游 429 When 请求 Then 终局透传 429(快速路径直接转发状态)
	f := newFixture(t, 429, "application/json", `{"error":{"message":"rate limited"}}`)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[]}`))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	if w.Code != 429 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestExhausted_LastStatusPassthrough(t *testing.T) {
	// Given 非内置协议(JS 包)+ 上游 503 When 请求 Then 透传上游状态与原始 body
	f := newFixture(t, 503, "application/json", `{"error":{"message":"overloaded upstream"}}`)
	f.installJSProtocolAs(t, "openai-completions")
	body := `{"model":"test-model","messages":[]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	if w.Code != 503 {
		t.Fatalf("status: %d body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "overloaded upstream") {
		t.Fatalf("upstream body not passed through: %s", w.Body.String())
	}
}

func TestMessages_CapabilityMismatch400(t *testing.T) {
	// Given 上游仅声明 openai-completions When anthropic 入口请求 Then 400 且错误体含可自救原因
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[]}`)
	body := `{"model":"test-model","max_tokens":10,"messages":[{"role":"user","content":"q"}]}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	w := httptest.NewRecorder()
	f.g.Messages(w, req)
	if w.Code != 400 {
		t.Fatalf("status: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"type":"error"`) {
		t.Fatalf("anthropic error shape: %s", w.Body.String())
	}
	// 逐上游原因(槽不符)+ 模型声明方汇总,用户可据此换入口或换上游
	if !strings.Contains(w.Body.String(), "u1 declares openai-completions") {
		t.Fatalf("reason missing: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "declared by u1(openai-completions)") {
		t.Fatalf("slot summary missing: %s", w.Body.String())
	}
}

func TestMessages_DeclaredProtocolServes(t *testing.T) {
	// Given 上游声明 anthropic-messages When anthropic 入口请求 Then 响应体原样透传
	f := newFixture(t, 200, "application/json", `{"id":"m1","type":"message","content":[{"type":"text","text":"ok"}]}`)
	f.installJSProtocolAs(t, "anthropic-messages")
	body := `{"model":"test-model","max_tokens":10,"messages":[{"role":"user","content":"q"}]}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	w := httptest.NewRecorder()
	f.g.Messages(w, req)
	if w.Code != 200 {
		t.Fatalf("status: %d %s", w.Code, w.Body.String())
	}
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	if m["type"] != "message" {
		t.Fatalf("anthropic shape: %v", m)
	}
}

func TestStreaming_Output(t *testing.T) {
	// Given 内置协议+空链(快速路径)上游 SSE 含 [DONE] When 流式请求 Then 原样透传含 [DONE]
	f := newFixture(t, 200, "text/event-stream",
		"data: {\"id\":\"c\",\"delta\":{\"content\":\"a\"}}\n\ndata: [DONE]\n\n")
	body := `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"x"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	out := w.Body.String()
	if !strings.Contains(out, "data:") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("sse out: %q", out)
	}
}

func TestStreaming_NonBuiltin_GeneratedDONE(t *testing.T) {
	// Given 非内置协议(JS 包)走管道 When 流式 Then convert 生成 [DONE] 收尾,且每行 data 为单对象(非数组包裹)
	f := newFixture(t, 200, "text/event-stream", "data: {\"id\":\"c\",\"delta\":{\"content\":\"a\"}}\n\n")
	f.installJSProtocol(t)
	body := `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"x"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, req)
	out := w.Body.String()
	if !strings.Contains(out, "[DONE]") || !strings.Contains(out, `"id":"c"`) {
		t.Fatalf("sse out: %q", out)
	}
	if strings.Contains(out, "data: [{") {
		t.Fatalf("chunk must not be array-wrapped: %q", out)
	}
}

func TestModels_Aggregation(t *testing.T) {
	// Given 注册上游 When Models Then 模型聚合
	f := newFixture(t, 200, "application/json", `{}`)
	req := httptest.NewRequest("GET", "/v1/models", nil)
	w := httptest.NewRecorder()
	f.g.Models(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	data := m["data"].([]any)
	found := false
	for _, d := range data {
		if d.(map[string]any)["id"] == "test-model" {
			found = true
		}
	}
	if !found {
		t.Fatalf("models: %v", m)
	}
}

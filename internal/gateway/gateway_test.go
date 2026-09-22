package gateway

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/builtin"
	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/history"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"

	_ "modernc.org/sqlite"
)

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

// v2Ddl 新模型行表(测试库)
func v2Ddl() []string {
	return []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, base_package TEXT NOT NULL,
			params_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (name, base_package))`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
		`CREATE TABLE package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE package_settings (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
	}
}

// newFixture 完整网关环境(v2:包参数 base_url + 包级 key api_key)
func newFixture(t *testing.T, upstreamStatus int, upstreamCT, upstreamBody string) *fixture {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/t.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range v2Ddl() {
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
	// 包与模型行(名字=内置协议名,走 Go 内置工厂注册)
	pkgs := plugin.NewRegistry(db)
	pkgs.RegisterBuiltin("openai-compatible", func(deps plugin.BuiltinDeps) (pipeline.Protocol, error) {
		return &builtin.Protocol{Config: deps.Config, PackageKey: deps.PackageKey}, nil
	})
	manifest := `{"manifestVersion":1,"name":"openai-compatible","version":"1","parts":{
		"protocol":{"protocol":"openai-completions","features":["tools","vision"]}}}`
	protoSrc := `module.exports = {
		buildRequest: function(){return {url:"u"}}, mapEvent: function(ctx,e){return e;}, mapResponse: function(ctx,b){return b;} };
	module.exports.settings = { base_url: setting.string({required: true}) };`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: protoSrc})); err != nil {
		t.Fatal(err)
	}
	// 包级密钥 + 包参数(连接信息在包,不在模型行)
	if err := pkgs.Keys().Merge("openai-compatible", map[string]any{"api_key": "sk-live-key"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pkgs.Settings().Put("openai-compatible", plugin.PutInput{Config: map[string]any{"base_url": f.upstreamSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(db, pkgs)
	u := &upstream.Model{Name: "test-model", Enabled: true, Plugin: "openai-compatible"}
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

// installJSProtocolAs 装 JS 协议包并换装注册表(协议可指定;形态由实现推导;v2 连接=包参数,密钥=包级 keys)
func (f *fixture) installJSProtocolAs(t *testing.T, protocol string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/js.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range v2Ddl() {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	pkgs := plugin.NewRegistry(db)
	manifest := `{"manifestVersion":1,"name":"js-proto","version":"1","parts":{
		"protocol":{"protocol":"` + protocol + `","features":["tools","vision"]}}}`
	src := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			var key = util.key("api_key");
			return { url: config.base_url + "/v1/chat/completions", method: "POST",
				headers: {"Content-Type": "application/json", "Authorization": "Bearer " + key},
				body: entry, stream: ctx.vars.entryStream };
		},
		mapResponse: function (ctx, b) { return b; },
		mapEvent: function (ctx, e) { var f = JSON.parse(e); return JSON.stringify([JSON.parse(f.data)]); } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	if protocol == "anthropic-messages" {
		src = `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			var key = util.key("api_key");
			return { url: config.base_url + "/v1/messages", method: "POST",
				headers: {"Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01"},
				body: entry, stream: ctx.vars.entryStream };
		},
		mapResponse: function (ctx, b) { return b; } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	}
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: src})); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Keys().Merge("js-proto", map[string]any{"api_key": "sk-live-key"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pkgs.Settings().Put("js-proto", plugin.PutInput{Config: map[string]any{"base_url": f.upstreamSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(db, pkgs)
	u := &upstream.Model{Name: "test-model", Enabled: true, Plugin: "js-proto"}
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
	// 逐行原因(槽不符)+ 模型声明方汇总,用户可据此换入口或换插件
	if !strings.Contains(w.Body.String(), "test-model@openai-compatible declares openai-completions") {
		t.Fatalf("reason missing: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "declared by test-model@openai-compatible(openai-completions)") {
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

// newHistory 供网关测试挂载的独立历史库
func newHistory(t *testing.T) *history.Store {
	t.Helper()
	s, err := history.New(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// lastHistory 取最新一条历史(单行场景;经 Get 验证详情路径)
func lastHistory(t *testing.T, h *history.Store) history.Entry {
	t.Helper()
	rows, total, err := h.Query(context.Background(), history.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("history rows: %d", total)
	}
	e, err := h.Get(context.Background(), rows[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestHistory_RecordsFastPath(t *testing.T) {
	// Given 网关挂历史库 When 快速路径请求 Then 记录 model/upstream/target/status/bodies
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`)
	h := newHistory(t)
	f.g.History = h
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	f.g.ChatCompletions(httptest.NewRecorder(), req)
	e := lastHistory(t, h)
	if e.Method != "POST" || e.Path != "/v1/chat/completions" || e.Model != "test-model" {
		t.Fatalf("basic fields: %+v", e)
	}
	if e.Upstream != "test-model" || e.Target != "-" {
		t.Fatalf("routing fields: %+v", e)
	}
	if e.Status != 200 || e.DurationMS < 0 || e.Stream {
		t.Fatalf("result fields: %+v", e)
	}
	if e.RequestBody != body {
		t.Fatalf("request body: %s", e.RequestBody)
	}
	if !strings.Contains(e.ResponseBody, "hi") {
		t.Fatalf("response body: %s", e.ResponseBody)
	}
}

func TestHistory_RecordsUpstreamError(t *testing.T) {
	// Given 上游 429 When 请求 Then 历史状态 429 且响应体含错误 JSON
	f := newFixture(t, 429, "application/json", `{"error":{"message":"rate limited"}}`)
	h := newHistory(t)
	f.g.History = h
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","messages":[]}`)))
	e := lastHistory(t, h)
	if e.Status != 429 {
		t.Fatalf("status: %d", e.Status)
	}
	if !strings.Contains(e.ResponseBody, "rate limited") {
		t.Fatalf("error body: %s", e.ResponseBody)
	}
}

func TestHistory_StreamNoResponseBody(t *testing.T) {
	// Given 上游 SSE When 流式请求 Then 历史标记 stream 且响应体为空
	f := newFixture(t, 200, "text/event-stream",
		"data: {\"id\":\"c\",\"delta\":{\"content\":\"a\"}}\n\ndata: [DONE]\n\n")
	h := newHistory(t)
	f.g.History = h
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","stream":true,"messages":[]}`)))
	e := lastHistory(t, h)
	if !e.Stream {
		t.Fatalf("stream flag: %+v", e)
	}
	if e.ResponseBody != "" {
		t.Fatalf("stream response body must be empty: %s", e.ResponseBody)
	}
}

func TestHistory_TruncatesOversizedRequest(t *testing.T) {
	// Given 请求体超上限 When 请求 Then 历史 request_body 截断到上限
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[]}`)
	h := newHistory(t)
	f.g.History = h
	big := `{"model":"test-model","messages":[{"role":"user","content":"` + strings.Repeat("x", history.BodyLimit*2) + `"}]}`
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(big)))
	e := lastHistory(t, h)
	if len(e.RequestBody) != history.BodyLimit {
		t.Fatalf("request body len: %d", len(e.RequestBody))
	}
}

func TestHistory_RecordsPipelinePath(t *testing.T) {
	// Given 非内置协议(JS 包)走管道 When 请求 Then 照常记录路由字段
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[]}`)
	f.installJSProtocol(t)
	h := newHistory(t)
	f.g.History = h
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","messages":[]}`)))
	e := lastHistory(t, h)
	if e.Status != 200 || e.Upstream != "test-model" || e.Target != "-" {
		t.Fatalf("pipeline record: %+v", e)
	}
}

func TestHistory_PipelineStreamNoResponseBody(t *testing.T) {
	// Given JS 协议包走管道(writeStream)When 流式请求 Then stream=true 且响应体为空
	f := newFixture(t, 200, "text/event-stream", "data: {\"id\":\"c\",\"delta\":{\"content\":\"a\"}}\n\n")
	f.installJSProtocol(t)
	h := newHistory(t)
	f.g.History = h
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","stream":true,"messages":[]}`)))
	e := lastHistory(t, h)
	if !e.Stream || e.ResponseBody != "" {
		t.Fatalf("pipeline stream record: %+v", e)
	}
}

func TestHistory_PickFailureEmptyRouting(t *testing.T) {
	// Given 未知模型(路由失败) When 请求 Then 落行 status=404 且 upstream/target 为空,model 保留
	f := newFixture(t, 200, "application/json", `{}`)
	h := newHistory(t)
	f.g.History = h
	f.g.ChatCompletions(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"no-such","messages":[]}`)))
	e := lastHistory(t, h)
	if e.Status != 404 || e.Upstream != "" || e.Target != "-" || e.Model != "no-such" {
		t.Fatalf("pick failure record: %+v", e)
	}}

func TestHistory_AbsentNoPanic(t *testing.T) {
	// Given 未装配历史库 When 请求 Then 正常服务不 panic
	f := newFixture(t, 200, "application/json", `{"id":"c1","choices":[]}`)
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"test-model","messages":[]}`)))
	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}
}

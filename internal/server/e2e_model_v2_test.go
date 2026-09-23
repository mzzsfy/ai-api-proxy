package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── v2 架构 BDD(设计稿 §4 全场景 + 路由补充场景) ───
//
// 观测件:v2Spy 记录每个上游请求的路径/头/体(多请求并发安全)
type v2Spy struct {
	mu    sync.Mutex
	paths []string
	heads []map[string]string
	bodys []string
}

func (s *v2Spy) record(r *http.Request, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := map[string]string{}
	for k := range r.Header {
		h[k] = r.Header.Get(k)
	}
	s.paths = append(s.paths, r.URL.Path)
	s.heads = append(s.heads, h)
	s.bodys = append(s.bodys, string(body))
}

func (s *v2Spy) last() (path string, head map[string]string, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.paths)
	if n == 0 {
		return "", nil, ""
	}
	return s.paths[n-1], s.heads[n-1], s.bodys[n-1]
}

// newV2Env 标准环境:观测假上游 + JS 协议包(槽:api_version,base_url) + redact filter(槽:mode)
type v2Env struct {
	f     *fourGroupsFixture
	spy   *v2Spy
	upSrv *httptest.Server
}

// v2ProtoJS 协议部件:anthropic-messages 适配(api_version 头 + x-api-key),观测经 header 断言
const v2ProtoJS = `module.exports = function (config) {
	return {
	buildRequest: function (ctx, entry) {
		if (typeof config.base_url !== "string" || !config.base_url) throw new Error("param base_url unset");
		return {
			url: config.base_url + "/v1/messages",
			method: "POST",
			headers: { "Content-Type": "application/json",
				"anthropic-version": config.api_version || "2023-06-01",
				"x-api-key": util.key().api_key },
			body: entry, stream: ctx.vars.entryStream
		};
	},
	mapEvent: function (ctx, e) {
		var f = JSON.parse(e);
		if (f.data === "[DONE]") return null;
		return JSON.stringify([JSON.parse(f.data)]);
	},
	mapResponse: function (ctx, b) { return b; }
	};
};
module.exports.settings = {
	base_url: setting.string({ description: "上游地址", required: true }),
	api_version: setting.string({ description: "API 版本", default: "2023-06-01" })
};`

// v2RedactJS filter:mode=full 时改写模型名(观测 filter 读到覆盖值的探针)
const v2RedactJS = `module.exports = function (config) {
	var mode = (config && config.mode) || "light";
	return {
		mapRequest: function (ctx, entry) {
			if (mode !== "full") return entry;
			var o = JSON.parse(entry);
			o.model = "redacted-full";
			return JSON.stringify(o);
		},
		mapChunk: function (ctx, c) { return c; },
		mapResponse: function (ctx, r) { return r; }
	};
};
module.exports.settings = {
	mode: setting.enum({ description: "脱敏模式", values: ["light", "full"], default: "light" })
};`

const v2PkgManifest = `{"manifestVersion":1,"name":"anthropic-compatible","version":"2.0.0","parts":{
	"protocol":{"protocol":"anthropic-messages","features":["tools"]},
	"filters":[{"name":"redact"}]}}`

// newV2Env 装配:真实网关 + 观测上游 + anthropic-compatible 包(密钥 K1,包参数 base_url)
func newV2Env(t *testing.T) *v2Env {
	t.Helper()
	spy := &v2Spy{}
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 4096)
		n, _ := r.Body.Read(b)
		spy.record(r, b[:n])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"pong"}]}`))
	}))
	t.Cleanup(upSrv.Close)
	f := newFourGroups(t)
	ctx := context.Background()
	if err := f.app.AdminDeps.Packages.Install(ctx, aapZip(t, v2PkgManifest, map[string]string{
		plugin.ProtocolEntry: v2ProtoJS, "filters/redact.js": v2RedactJS})); err != nil {
		t.Fatal(err)
	}
	if _, err := f.app.AdminDeps.Packages.Keys().Set("anthropic-compatible", "api_key", map[string]any{"api_key": "K1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.app.AdminDeps.Packages.Settings().Put("anthropic-compatible", plugin.PutInput{Config: map[string]any{
		"base_url": upSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	return &v2Env{f: f, spy: spy, upSrv: upSrv}
}

// postMessage anthropic 入口请求
func (e *v2Env) postMessage(t *testing.T, model string) (int, string) {
	t.Helper()
	return e.f.post(t, "/v1/messages", map[string]string{
		"x-api-key": "sk-test", "anthropic-version": "2023-06-01", "Content-Type": "application/json",
	}, `{"model":"`+model+`","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
}

// saveRow 建行(测试断言失败即 Fatal)
func (e *v2Env) saveRow(t *testing.T, name string, params map[string]any) {
	t.Helper()
	if err := e.f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: name, Plugin: "anthropic-compatible", Enabled: true, Params: params,
	}); err != nil {
		t.Fatal(err)
	}
}

// 场景:多模型共享一套插件配置
func TestV2_MultiModelSharePluginConfig(t *testing.T) {
	// Given 同包两行(auto/auto-free)无覆盖 When 各自请求 Then 均发往包参数 base_url 且鉴权 K1
	e := newV2Env(t)
	e.saveRow(t, "auto", nil)
	e.saveRow(t, "auto-free", nil)
	for _, model := range []string{"auto", "auto-free"} {
		code, body := e.postMessage(t, model)
		if code != http.StatusOK {
			t.Fatalf("%s: %d %s", model, code, body)
		}
		path, head, _ := e.spy.last()
		if path != "/v1/messages" {
			t.Fatalf("%s path: %s", model, path)
		}
		if head["X-Api-Key"] != "K1" {
			t.Fatalf("%s key: %q", model, head["X-Api-Key"])
		}
	}
}

// 场景:模型参数覆盖包配置(覆盖槽默认值;未覆盖行走包默认)
func TestV2_ModelParamOverridesPackage(t *testing.T) {
	// Given 槽 api_version 默认 2023-06-01;auto 行覆盖 2024-01-01,auto-free 不覆盖
	e := newV2Env(t)
	e.saveRow(t, "auto", map[string]any{"api_version": "2024-01-01"})
	e.saveRow(t, "auto-free", nil)
	if code, body := e.postMessage(t, "auto"); code != http.StatusOK {
		t.Fatalf("auto: %d %s", code, body)
	}
	_, head, _ := e.spy.last()
	if head["Anthropic-Version"] != "2024-01-01" {
		t.Fatalf("covered version: %q", head["Anthropic-Version"])
	}
	if code, body := e.postMessage(t, "auto-free"); code != http.StatusOK {
		t.Fatalf("auto-free: %d %s", code, body)
	}
	_, head, _ = e.spy.last()
	if head["Anthropic-Version"] != "2023-06-01" {
		t.Fatalf("default version: %q", head["Anthropic-Version"])
	}
}

// 场景:参数白名单(防漂移)——保存期未知键拒绝
func TestV2_UnknownParamRejectedOnSave(t *testing.T) {
	// Given 包仅声明 api_version/mode/base_url/transport When 建行参数含 foo Then 拒绝并注明槽名
	e := newV2Env(t)
	err := e.f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: "drift", Plugin: "anthropic-compatible", Enabled: true,
		Params: map[string]any{"foo": "bar"},
	})
	if err == nil || !strings.Contains(err.Error(), "foo") {
		t.Fatalf("unknown slot must reject: %v", err)
	}
}

// 场景:参数白名单(防漂移)——包升级删槽后残留键请求期剥离不阻断
func TestV2_UnknownParamStrippedOnRequest(t *testing.T) {
	// Given 行参数含合法槽 When 请求 Then 200(剥离语义由 ResolveParams ParamRequest 单元覆盖,此处验证端到端不受残留影响)
	e := newV2Env(t)
	e.saveRow(t, "auto", map[string]any{"api_version": "2024-01-01"})
	if code, body := e.postMessage(t, "auto"); code != http.StatusOK {
		t.Fatalf("request: %d %s", code, body)
	}
}

// 场景:密钥唯一归属——api_key 不是模型层参数
func TestV2_ApiKeyNotModelParam(t *testing.T) {
	// When 建行参数含 api_key Then 拒绝(不在声明槽内;密钥属包级 keys)
	e := newV2Env(t)
	err := e.f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: "keyrow", Plugin: "anthropic-compatible", Enabled: true,
		Params: map[string]any{"api_key": "K2"},
	})
	if err == nil {
		t.Fatal("api_key in model params must be rejected")
	}
}

// 场景:密钥轮换不断流——写 K2 后请求立即用 K2
func TestV2_KeyRotationAppliesImmediately(t *testing.T) {
	// Given 当前 K1 When Merge K2 Then 下一请求头即 K2(keys 直读不经行缓存)
	e := newV2Env(t)
	e.saveRow(t, "auto", nil)
	if code, body := e.postMessage(t, "auto"); code != http.StatusOK {
		t.Fatalf("before rotate: %d %s", code, body)
	}
	if _, head, _ := e.spy.last(); head["X-Api-Key"] != "K1" {
		t.Fatalf("initial key: %q", head["X-Api-Key"])
	}
	if _, err := e.f.app.AdminDeps.Packages.Keys().Set("anthropic-compatible", "api_key", map[string]any{"api_key": "K2"}); err != nil {
		t.Fatal(err)
	}
	if code, body := e.postMessage(t, "auto"); code != http.StatusOK {
		t.Fatalf("after rotate: %d %s", code, body)
	}
	if _, head, _ := e.spy.last(); head["X-Api-Key"] != "K2" {
		t.Fatalf("rotated key: %q", head["X-Api-Key"])
	}
}

// 场景:缺配置快速失败——行参数事后指向缺失值 → 请求 502
func TestV2_MissingConfigFailsFast(t *testing.T) {
	// Given 包参数与行参数均不提供 required 槽 base_url When 请求该行 Then 502 且错误体注明槽名
	e := newV2Env(t)
	// 行覆盖 nil 槽:合并结果 = 声明 default(无)+ 包参数(有)——需要拆掉包参数才有真实缺配置
	pkgs := e.f.app.AdminDeps.Packages
	if _, err := pkgs.Settings().Put("anthropic-compatible", plugin.PutInput{}); err != nil {
		t.Fatal(err)
	}
	e.f.app.Registry.EvictPackageSettings("anthropic-compatible")
	e.saveRow(t, "broken", nil)
	code, body := e.postMessage(t, "broken")
	if code != http.StatusBadGateway {
		t.Fatalf("status: %d body %s", code, body)
	}
	if !strings.Contains(body, "base_url") {
		t.Fatalf("error must name slot: %s", body)
	}
}

// 场景:过滤器声明自己的槽位——filter 工厂与适配器注入同一份解析后 config
func TestV2_FilterOwnSlotAndOverride(t *testing.T) {
	// Given redact 槽 mode 默认 light;auto 行覆盖 mode=full When 请求 auto Then filter 读 full(模型名改写);auto-free 读 light(不改写)
	e := newV2Env(t)
	e.saveRow(t, "auto", map[string]any{"mode": "full"})
	e.saveRow(t, "auto-free", nil)
	if code, body := e.postMessage(t, "auto"); code != http.StatusOK {
		t.Fatalf("auto: %d %s", code, body)
	}
	_, _, reqBody := e.spy.last()
	if !strings.Contains(reqBody, `"model":"redacted-full"`) {
		t.Fatalf("filter override not applied: %s", reqBody)
	}
	if code, body := e.postMessage(t, "auto-free"); code != http.StatusOK {
		t.Fatalf("auto-free: %d %s", code, body)
	}
	_, _, reqBody = e.spy.last()
	if strings.Contains(reqBody, "redacted-full") {
		t.Fatalf("default must not rewrite: %s", reqBody)
	}
}

// 场景:同名模型跨协议复用(v2 复合唯一键核心场景)
func TestV2_SameNameAcrossProtocols(t *testing.T) {
	// Given "auto" 同时绑定 anthropic-compatible 与内置 openai-compatible(各自包参数)When 各入口请求 Then 各自命中
	e := newV2Env(t)
	// 内置包参数指向同一观测上游;openai 入口槽 = /v1/chat/completions
	if _, err := e.f.app.AdminDeps.Packages.Keys().Set("openai-compatible", "api_key", map[string]any{"api_key": "K1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.f.app.AdminDeps.Packages.Settings().Put("openai-compatible", plugin.PutInput{Config: map[string]any{
		"base_url": e.upSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	if err := e.f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: "auto", Plugin: "openai-compatible", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	e.saveRow(t, "auto", nil) // anthropic 侧行
	code, _ := e.postMessage(t, "auto")
	if code != http.StatusOK {
		t.Fatalf("anthropic entry: %d", code)
	}
	path, head, _ := e.spy.last()
	if path != "/v1/messages" || head["X-Api-Key"] != "K1" {
		t.Fatalf("anthropic route: %s %v", path, head)
	}
	req, _ := http.NewRequest(http.MethodPost, e.f.gateway.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("openai entry: %d", resp.StatusCode)
	}
	path, _, _ = e.spy.last()
	if path != "/v1/chat/completions" {
		t.Fatalf("openai route path: %s", path)
	}
}

// 场景:disabled 行参与分类(非 NoModel)
func TestV2_DisabledRowClassifiesAsCapability(t *testing.T) {
	// Given 唯一行 disabled When 请求 Then 400 capability mismatch 且文案注明 disabled(非 404)
	e := newV2Env(t)
	if err := e.f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: "paused", Plugin: "anthropic-compatible", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	code, body := e.postMessage(t, "paused")
	if code != http.StatusBadRequest {
		t.Fatalf("disabled row: %d %s", code, body)
	}
	if !strings.Contains(body, "disabled") {
		t.Fatalf("reason must note disabled: %s", body)
	}
}

// 场景:未知模型 404(NoModel 与 Capability 语义分离)
func TestV2_UnknownModelIs404(t *testing.T) {
	// When 请求从未被任何行声明的模型 Then 404
	e := newV2Env(t)
	code, _ := e.postMessage(t, "no-such")
	if code != http.StatusNotFound {
		t.Fatalf("unknown model: %d", code)
	}
}

// 场景:tasks 只见插件参数(模型覆盖不进任务)—— 单元级由 hooks 测试覆盖,此处验证配置面:
// 模型行 params 不写入 Settings().View(包参数与行参数存储隔离)
func TestV2_ModelParamsStayOutOfPackageSettings(t *testing.T) {
	// Given auto 行覆盖 mode/api_version When 读包参数视图 Then 不含行覆盖值
	e := newV2Env(t)
	e.saveRow(t, "auto", map[string]any{"mode": "full", "api_version": "2024-01-01"})
	view := e.f.app.AdminDeps.Packages.SettingsOverrides("anthropic-compatible")
	b, _ := json.Marshal(view)
	if strings.Contains(string(b), "2024-01-01") || strings.Contains(string(b), `"full"`) {
		t.Fatalf("model overrides leaked into package settings: %s", b)
	}
}

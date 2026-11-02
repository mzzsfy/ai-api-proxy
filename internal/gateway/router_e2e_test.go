package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// ─── router 钩子请求级选键(BDD 场景;js-proto 包 + 两键池)───

// installRoutedPackage 换装带 router.js 的 js-proto 包并录入两键(quota 字段驱动选号)
func installRoutedPackage(t *testing.T, f *fixture, selectJS string, keyQuotas map[string]float64) *plugin.Package {
	t.Helper()
	manifest := `{"manifestVersion":1,"name":"js-proto","version":"2","parts":{
		"protocol":{"protocol":"openai-completions"},
		"hooks":{"router":{}}}}`
	src := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			return { url: config.base_url + "/v1/chat/completions", method: "POST",
				headers: {"Content-Type": "application/json", "Authorization": "Bearer " + util.key().api_key},
				body: entry, stream: ctx.vars.entryStream };
		},
		mapResponse: function (ctx, b) { return b; } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	routerSrc := "module.exports = {select:" + selectJS + "}"
	files := map[string]string{plugin.ProtocolEntry: src, plugin.RouterEntry: routerSrc}
	// installJSProtocolAs 会整体重建注册表/网关,先换装再取新 registry
	f.installJSProtocolAs(t, "openai-completions")
	pkgs := f.regPkg
	if pkgs == nil {
		t.Fatal("package registry missing")
	}
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, files), ""); err != nil {
		t.Fatalf("install js-proto(v2): %v", err)
	}
	for id, quota := range keyQuotas {
		if _, err := pkgs.Keys().Set("js-proto", id, map[string]any{"api_key": id, "quota": quota}); err != nil {
			t.Fatalf("set key %s: %v", id, err)
		}
	}
	pkg, ok := f.reg.HookPackage("js-proto")
	if !ok {
		t.Fatal("js-proto package missing")
	}
	return pkg
}

// 场景: select 按 quota 选高积分键,多次请求稳定同选该键(round-robin 会轮转)
func TestRoutedKeysSelect_HighQuotaStable(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"ok":true}`)
	installRoutedPackage(t, f,
		`function(input){var best=null,bq=-1;for(var i=0;i<input.keys.length;i++){var k=input.keys[i];var q=(k.data&&k.data.quota)||0;if(input.inFlight[k.id]){continue;}if(q>bq){bq=q;best=k.id;}}return best;}`,
		map[string]float64{"low": 10, "high": 90})
	engines := plugin.NewRouterEngines(func(p *plugin.Package) plugin.RouterDeps { return plugin.RouterDeps{Package: p} })
	f.g.Routers = engines
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`))
		resp := httptest.NewRecorder()
		f.g.ChatCompletions(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("request %d: status=%d body=%s", i, resp.Code, resp.Body.String())
		}
		if f.seenAuth != "Bearer high" {
			t.Fatalf("request %d routed to %q; want high", i, f.seenAuth)
		}
	}
}

// 场景: select 返回 null = 回落 round-robin,请求成功(可用性不依赖 JS)
func TestRoutedKeysSelect_NullFallsBackRoundRobin(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"ok":true}`)
	installRoutedPackage(t, f, `function(input){return null}`, map[string]float64{"k1": 1, "k2": 2})
	engines := plugin.NewRouterEngines(func(p *plugin.Package) plugin.RouterDeps { return plugin.RouterDeps{Package: p} })
	f.g.Routers = engines
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`))
		resp := httptest.NewRecorder()
		f.g.ChatCompletions(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("request %d: status=%d", i, resp.Code)
		}
		seen[f.seenAuth] = true
	}
	if !seen["Bearer k1"] || !seen["Bearer k2"] {
		t.Fatalf("round-robin fallback missing: %v", seen)
	}
}

// 场景: Routers 未装配 = 默认 round-robin 不受影响(既有行为回归锁定)
func TestNoRouters_DefaultRoundRobin(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"ok":true}`)
	f.installJSProtocolAs(t, "openai-completions")
	for _, id := range []string{"k1", "k2"} {
		if _, err := f.regPkg.Keys().Set("js-proto", id, map[string]any{"api_key": id}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`))
		resp := httptest.NewRecorder()
		f.g.ChatCompletions(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("request %d: status=%d", i, resp.Code)
		}
		seen[f.seenAuth] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected round-robin across keys, got %v", seen)
	}
}

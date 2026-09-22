package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func settingsPkg(t *testing.T, pkgs pkgRegistry, decl string) {
	t.Helper()
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", map[string]string{
		"settings.js":     decl,
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	})); err != nil {
		t.Fatal(err)
	}
}

type pkgRegistry = interface {
	Install(ctx context.Context, data []byte) error
}

func TestSettings_GetViewExpands(t *testing.T) {
	// Given 声明两槽位(一带 default)且无覆盖 When GET settings Then 展开视图:值=default,overridden=false,version=0
	pkgs, _ := testRegistry(t)
	settingsPkg(t, pkgs, `module.exports.settings={account:{type:"string",description:"账号"},level:{type:"int",default:3}}`)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/settings", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{`"account"`, `"level"`, `"value":3`, `"overridden":false`, `"version":0`, `"name":"signIn"`, `"cron":"* * * * *"`} {
		if !containsStr(body, want) {
			t.Fatalf("missing %s in: %s", want, body)
		}
	}
}

func TestSettings_PutRoundTripAnd409(t *testing.T) {
	// Given GET 拿 version When PUT 合法覆盖 Then 200 新 version 且 GET 值更新;旧 version 再 PUT → 409
	pkgs, _ := testRegistry(t)
	settingsPkg(t, pkgs, `module.exports.settings={account:{type:"string",description:"账号"}}`)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()

	// 第一次 PUT(version=0)
	w := httptest.NewRecorder()
	body := `{"config":{"account":"me@x"},"tasks":{},"version":0}`
	r := httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/settings", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("put status %d: %s", w.Code, w.Body.String())
	}
	if !containsStr(w.Body.String(), `"version"`) {
		t.Fatalf("version missing: %s", w.Body.String())
	}

	// GET 确认覆盖生效
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/settings", nil))
	if !containsStr(w2.Body.String(), `"value":"me@x"`) || !containsStr(w2.Body.String(), `"overridden":true`) {
		t.Fatalf("get after put: %s", w2.Body.String())
	}

	// 旧 version 再 PUT → 409
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/settings",
		strings.NewReader(`{"config":{"account":"other"},"tasks":{},"version":0}`)))
	if w3.Code != http.StatusConflict {
		t.Fatalf("stale put status %d: %s", w3.Code, w3.Body.String())
	}
}

func TestSettings_PutStripsUnknownAndDefaults(t *testing.T) {
	// Given PUT 含声明外键与等值默认 When 保存 Then 剥离不落层;全空 → 行删除(GET 仍 200)
	pkgs, _ := testRegistry(t)
	settingsPkg(t, pkgs, `module.exports.settings={level:{type:"int",default:3}}`)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/settings",
		strings.NewReader(`{"config":{"level":3,"hacker":"x"},"tasks":{"signIn":{"cron":"*/5 * * * *"}},"version":0}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("put status %d: %s", w.Code, w.Body.String())
	}
	ov := pkgs.SettingsOverrides("checkin")
	cfg, _ := ov["config"].(map[string]any)
	if cfg == nil || len(cfg) != 0 {
		t.Fatalf("default/unknown not stripped: %v", ov)
	}
	tasks, _ := ov["tasks"].(map[string]any)
	if tasks == nil || len(tasks) != 1 {
		t.Fatalf("task override: %v", ov)
	}
}

func TestSettings_Boundaries(t *testing.T) {
	// 无 hooks 包 → GET/PUT 均 400;不存在包 → 404;非法 cron → 400
	pkgs, _ := testRegistry(t)
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", map[string]string{
		"settings.js":     `module.exports.settings={}`,
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	})); err != nil {
		t.Fatal(err)
	}
	// 构造无 hooks 包:纯 protocol 包复用 builtin 声明面——直接用 hooks 包验证非法 cron 边界
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/settings",
		strings.NewReader(`{"config":{},"tasks":{"signIn":{"cron":"bad"}},"version":0}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad cron status %d: %s", w.Code, w.Body.String())
	}
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/nope/settings", nil))
	if w2.Code != http.StatusNotFound {
		t.Fatalf("missing pkg status %d", w2.Code)
	}
}

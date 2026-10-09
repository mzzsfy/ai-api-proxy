package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// ─── login/inspect 凭据通道(BDD 场景)───

// credsFixture 装配 aap 桥端点测试环境
func credsFixture(t *testing.T, files map[string]string) (*plugin.Registry, adminMuxer, *App) {
	t.Helper()
	pkgs, st := testRegistry(t)
	app := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	app.AdminDeps.Creds = app
	// login 不计入插槽:hooks 最小任务共存(manifest 模板带 signIn 任务)
	if _, ok := files["tasks/signIn.js"]; !ok {
		files["tasks/signIn.js"] = `module.exports={handler:function(ctx){}}`
	}
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", files), ""); err != nil {
		t.Fatal(err)
	}
	return pkgs, app.AdminDeps.Mux(), app
}

// 场景: aap.login 落库——页面编排产物经 login/index.js → Keys.Set(唯一落库通道)
func TestCreds_LoginFlow(t *testing.T) {
	files := map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {id: "acct-main", token: ctx.params.token + "-x"}; }`,
		"login/ui/index.html": `<html><body>login</body></html>`,
	}
	pkgs, mux, _ := credsFixture(t, files)
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]any{"input": map[string]any{"token": "tk"}})
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/login", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("login status %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		ID   string `json:"id"`
		Data map[string]any `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out.ID != "acct-main" {
		t.Fatalf("id: %s (%s)", out.ID, w.Body.String())
	}
	if e, ok := pkgs.Keys().Get("checkin", "acct-main"); !ok || e.Data.(map[string]any)["token"] != "tk-x" {
		t.Fatalf("stored: %+v", e)
	}
}

// 场景: aap.login 宿主生成 id——返回值无 id 字段时 acct-<milli>-<seq>
func TestCreds_LoginGeneratedID(t *testing.T) {
	files := map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {token: "t"}; }`,
		"login/ui/index.html": `<html></html>`,
	}
	pkgs, mux, _ := credsFixture(t, files)
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]any{"input": map[string]any{}})
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/login", strings.NewReader(string(body))))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "acct-") {
		t.Fatalf("gen id: %d %s", w.Code, w.Body.String())
	}
	if len(pkgs.Keys().List("checkin")) != 1 {
		t.Fatal("not stored")
	}
}

// 场景: inspect 展示变换——data → 脱敏 JSON;无 inspect.js = 原样
func TestCreds_InspectTransform(t *testing.T) {
	files := map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {token: "secret"}; }`,
		"login/ui/index.html": `<html></html>`,
	}
	pkgs, mux, app := credsFixture(t, files)
	if _, err := app.Login("checkin", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	// 无 inspect:原样
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/accounts", nil))
	if !strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("plain view: %s", w.Body.String())
	}
	// 有 inspect:脱敏
	pkg, _ := pkgs.GetPackage("checkin")
	pkg.Files["inspect.js"] = []byte(`module.exports = function(data){ return {masked: true}; }`)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/accounts", nil))
	if strings.Contains(w2.Body.String(), "secret") || !strings.Contains(w2.Body.String(), "masked") {
		t.Fatalf("inspect view: %s", w2.Body.String())
	}
	// 单账号(id 取列表响应)
	var list struct {
		Accounts []struct{ ID string `json:"id"` } `json:"accounts"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &list)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/accounts/"+list.Accounts[0].ID, nil))
	if !strings.Contains(w3.Body.String(), "masked") {
		t.Fatalf("single: %s", w3.Body.String())
	}
}

// 场景: aap.storage 桥——③直写打权威标;get/set 往返
func TestCreds_StorageBridge(t *testing.T) {
	_, mux, _ := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {}; }`,
		"login/ui/index.html": `<html></html>`,
	})
	do := func(op, key, value string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		b, _ := json.Marshal(map[string]any{"op": op, "key": key, "value": value})
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/accounts/storage", strings.NewReader(string(b))))
		return w
	}
	if w := do("set", "sid", "v1"); w.Code != http.StatusOK {
		t.Fatalf("set: %d %s", w.Code, w.Body.String())
	}
	w := do("get", "sid", "")
	if !strings.Contains(w.Body.String(), "v1") {
		t.Fatalf("get: %s", w.Body.String())
	}
	// 权威标:authoritative 查询为真
	if w2 := do("bogus", "k", ""); w2.Code != http.StatusBadRequest {
		t.Fatalf("op guard: %d", w2.Code)
	}
}

// 场景: 包管理页静态资源——login/ui/ serve,index 缺省
func TestCreds_LoginUIServe(t *testing.T) {
	_, mux, _ := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {}; }`,
		"login/ui/index.html": `<html><body>ui</body></html>`,
		"login/ui/app.js":    `console.log(1)`,
	})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/login/ui/", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "ui") {
		t.Fatalf("index: %d %s", w.Code, w.Body.String())
	}
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/login/ui/app.js", nil))
	if w2.Code != http.StatusOK || w2.Header().Get("Content-Type") != "text/javascript; charset=utf-8" {
		t.Fatalf("js: %d %s", w2.Code, w2.Header().Get("Content-Type"))
	}
	// 穿越拒绝
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/login/ui/..%2Findex.js", nil))
	if w3.Code != http.StatusBadRequest && w3.Code != http.StatusNotFound {
		t.Fatalf("traversal: %d", w3.Code)
	}
}

// 场景: aap.meta——页面自适应(name/version/revision/params)
func TestCreds_Meta(t *testing.T) {
	_, mux, _ := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {}; }`,
		"login/ui/index.html": `<html></html>`,
	})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/meta", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"revision":1`) {
		t.Fatalf("meta: %d %s", w.Code, w.Body.String())
	}
}

// 场景: 无 Creds 装配 = 相关路由 501(最小 App 兼容)
func TestCreds_NotWired(t *testing.T) {
	pkgs, _ := testRegistry(t)
	// Deps 无 Creds
	d := &admin.Deps{Packages: pkgs}
	mux := d.Mux()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/login", strings.NewReader(`{}`)))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("501: %d", w.Code)
	}
}

// 场景: login ctx 无 keys/settings/cron(契约 = params/http/storage/util)
func TestCreds_LoginCtxContract(t *testing.T) {
	files := map[string]string{
		"login/index.js": `module.exports = function(ctx){
			storage.set("probe", JSON.stringify([String(ctx.keys===undefined), String(ctx.settings===undefined), String(ctx.cron===undefined), String(ctx.http.run!==undefined)]));
			return {ok: true};
		}`,
		"login/ui/index.html": `<html></html>`,
	}
	_, _, app := credsFixture(t, files)
	if _, err := app.Login("checkin", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	s, err := app.StorageGet("checkin", "probe")
	if err != nil {
		t.Fatal(err)
	}
	if s != `["true","true","true","true"]` {
		t.Fatalf("probe: %v", s)
	}
}

// 场景: login/index.js 导出非函数 = 装载报错(不落库)
func TestCreds_LoginNotFunction(t *testing.T) {
	_, mux, _ := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = 42`,
		"login/ui/index.html": `<html></html>`,
	})
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]any{"input": map[string]any{}})
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/login", strings.NewReader(string(body))))
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "must be a function") {
		t.Fatalf("non-function: %d %s", w.Code, w.Body.String())
	}
}

// 场景: inspect 单条变换失败 = 回退原 data(不整页失败)
func TestCreds_InspectFallback(t *testing.T) {
	pkgs, mux, app := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {token: "raw"}; }`,
		"login/ui/index.html": `<html></html>`,
	})
	if _, err := app.Login("checkin", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	pkg, _ := pkgs.GetPackage("checkin")
	pkg.Files["inspect.js"] = []byte(`module.exports = function(data){ throw new Error("boom"); }`)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/accounts", nil))
	if !strings.Contains(w.Body.String(), "raw") {
		t.Fatalf("fallback: %s", w.Body.String())
	}
}

// 场景: 桥 set 落权威标(IsAuthoritative 为真)
func TestCreds_StorageAuthorityMark(t *testing.T) {
	_, mux, app := credsFixture(t, map[string]string{
		"login/index.js":     `module.exports = function(ctx){ return {}; }`,
		"login/ui/index.html": `<html></html>`,
	})
	body, _ := json.Marshal(map[string]any{"op": "set", "key": "sid", "value": "v1"})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/accounts/storage", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("set: %d", w.Code)
	}
	s, err := app.credsFor("checkin")
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := s.IsAuthoritative("sid"); err != nil || !marked {
		t.Fatalf("authority mark: %v %v", marked, err)
	}
}

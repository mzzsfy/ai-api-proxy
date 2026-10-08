package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// pluginUIFixture 装配含 ui 资源的测试包
func pluginUIFixture(t *testing.T, extra map[string]string) (*plugin.Registry, adminMuxer) {
	t.Helper()
	pkgs, mux := keysFixture(t)
	files := map[string]string{"tasks/signIn.js": `module.exports={handler:function(ctx){}}`}
	for k, v := range extra {
		files[k] = v
	}
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", files)); err != nil {
		t.Fatal(err)
	}
	return pkgs, mux
}

// Given 包含 ui/index.html 的包 When GET ui 资源 Then 按类型回包;穿越与缺失被拒
func TestPluginUI_Serve(t *testing.T) {
	_, mux := pluginUIFixture(t, map[string]string{"ui/index.html": `<!doctype html><html><body>zcode-ui</body></html>`, "ui/app.js": `console.log("zcode-ui")`})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/ui/index.html", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "zcode-ui") ||
		w.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("html: %d %s %s", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/ui/app.js", nil))
	if w2.Code != http.StatusOK || w2.Header().Get("Content-Type") != "text/javascript; charset=utf-8" {
		t.Fatalf("js: %d %s", w2.Code, w2.Header().Get("Content-Type"))
	}
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/ui/..%2Fmanifest.json", nil))
	if w3.Code != http.StatusBadRequest && w3.Code != http.StatusNotFound {
		t.Fatalf("traversal: %d", w3.Code)
	}
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/ui/missing.html", nil))
	if w4.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", w4.Code)
	}
}

// Given 已有对象键 When masked=1 列表 Then 字符串叶子掩码;PUT 带 *** 合并旧值
func TestPluginUI_MaskedKeys(t *testing.T) {
	pkgs, mux := pluginUIFixture(t, map[string]string{"ui/index.html": `<!doctype html><html><body>zcode-ui</body></html>`, "ui/app.js": `console.log("zcode-ui")`})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/cred",
		strings.NewReader(`{"value":{"token":"abc","n":1},"baseUpdatedAt":0}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("put: %d %s", w.Code, w.Body.String())
	}
	var putOut struct {
		UpdatedAt int64 `json:"updatedAt"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &putOut)

	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys?masked=1", nil))
	if w2.Code != http.StatusOK || !strings.Contains(w2.Body.String(), `"token":"***"`) || strings.Contains(w2.Body.String(), `"abc"`) {
		t.Fatalf("masked list: %d %s", w2.Code, w2.Body.String())
	}

	// *** 还原 abc,明文叶子 n 正常更新
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/cred",
		strings.NewReader(`{"value":{"token":"***","n":2},"baseUpdatedAt":`+strconv.FormatInt(putOut.UpdatedAt, 10)+`}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("masked put: %d %s", w3.Code, w3.Body.String())
	}
	e, ok := pkgs.Keys().Get("checkin", "cred")
	m, _ := e.Data.(map[string]any)
	if !ok || m["token"] != "abc" || m["n"] != float64(2) {
		t.Fatalf("merged data: %+v", e.Data)
	}
}

// Given 无 ui 的包 When GET ui Then 404;listPackages 带 hasPage 标记
func TestPluginUI_Flags(t *testing.T) {
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	})); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages", nil))
	if !strings.Contains(w.Body.String(), `"hasPage":false`) || strings.Contains(w.Body.String(), `"hasPage":true`) {
		t.Fatalf("hasPage flags: %s", w.Body.String())
	}
	pkgs2, mux2 := pluginUIFixture(t, map[string]string{"ui/index.html": `<!doctype html><html><body>zcode-ui</body></html>`, "ui/app.js": `console.log("zcode-ui")`})
	_ = pkgs2
	w2 := httptest.NewRecorder()
	mux2.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages", nil))
	if !strings.Contains(w2.Body.String(), `"hasPage":true`) {
		t.Fatalf("hasPage true missing: %s", w2.Body.String())
	}
}

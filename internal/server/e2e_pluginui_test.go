package server

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", files), ""); err != nil {
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

// Given 含 ui/index.html 的包 When packages 列表 Then hasPage 标记透出(包详情页管理页渲染位判定)
func TestPluginUI_HasPageFlag(t *testing.T) {
	_, mux := pluginUIFixture(t, map[string]string{"ui/index.html": "<html><body>page</body></html>"})
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/api/packages", nil))
	if !strings.Contains(w2.Body.String(), `"hasPage":true`) {
		t.Fatalf("hasPage true missing: %s", w2.Body.String())
	}
}

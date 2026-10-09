package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// keysFixture 装配 keys 钩子通道的测试环境(真 wireKeyHooks;trMgr nil = 出站报错,非出站场景不受影响)
func keysFixture(t *testing.T) (*plugin.Registry, adminMuxer) {
	t.Helper()
	pkgs, st := testRegistry(t)
	app := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	wireKeyHooks(app)
	return pkgs, app.AdminDeps.Mux()
}

// adminMuxer 路由面最小抽象
type adminMuxer interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

func TestKeys_ApiBoundaries(t *testing.T) {
	// HW9 键名非法 400 / 无 keys.js 包 PUT 原样保存(HW1) / 键不存在 detail 404
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	}), ""); err != nil {
		t.Fatal(err)
	}
	// 键名含 /(路由无法寻址)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/a%2Fb",
		strings.NewReader(`{"value":1,"baseUpdatedAt":0}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("slash key status %d", w.Code)
	}
	// 无 keys.js:PUT 原样保存
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/plain",
		strings.NewReader(`{"value":{"a":1},"baseUpdatedAt":0}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("plain put status %d: %s", w3.Code, w3.Body.String())
	}
	if v, ok := pkgs.Keys().Get("checkin", "plain"); !ok || v.Data == nil {
		t.Fatal("plain put lost")
	}
	// detail:无 keyRead = 200 无 explain(读取不再 501;键数据明文可查)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys/plain/detail", nil))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), `"data"`) {
		t.Fatalf("no keyRead detail status %d: %s", w4.Code, w4.Body.String())
	}
}

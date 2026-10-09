package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// keysFixture 装配键管理端点测试环境
func keysFixture(t *testing.T) (*plugin.Registry, adminMuxer) {
	t.Helper()
	pkgs, _ := testRegistry(t)
	app := &App{AdminDeps: newAdminDeps(pkgs)}
	return pkgs, app.AdminDeps.Mux()
}

// adminMuxer 路由面最小抽象
type adminMuxer interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

func TestKeys_ApiBoundaries(t *testing.T) {
	// HW9 键名非法 400 / PUT 原样保存(HW1;GUI 手编通道无 JS 归一化)
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
	// PUT 原样保存
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/plain",
		strings.NewReader(`{"value":{"a":1},"baseUpdatedAt":0}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("plain put status %d: %s", w3.Code, w3.Body.String())
	}
	if v, ok := pkgs.Keys().Get("checkin", "plain"); !ok || v.Data == nil {
		t.Fatal("plain put lost")
	}
	// baseUpdatedAt 过期 = 409
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/plain",
		strings.NewReader(`{"value":{"a":2},"baseUpdatedAt":0}`)))
	if w2.Code != http.StatusConflict {
		t.Fatalf("stale put status %d: %s", w2.Code, w2.Body.String())
	}
}

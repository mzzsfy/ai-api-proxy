package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ─── keysCheckGroup 错误路径单元(盲区[中]):bad id / bad json / 501 未装配 / 404 前缀分类 ───

// testKeysCheckDeps 仅装配被测函数所需最小依赖
func testKeysCheckDeps() *Deps {
	return &Deps{}
}

func TestKeysCheckGroup_BadID(t *testing.T) {
	// Given 路径 id 非数字 When keys-check Then 400 bad id(不触依赖)
	d := testKeysCheckDeps()
	mux := d.Mux()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/groups/abc/keys-check", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "bad id") {
		t.Fatalf("bad id: %d %s", w.Code, w.Body.String())
	}
}

func TestKeysCheckGroup_BadJSON(t *testing.T) {
	// Given 请求体非法 JSON When keys-check Then 400 bad json
	d := testKeysCheckDeps()
	var called bool
	d.KeysCheckFunc = func(_ context.Context, _ int64, _, _ string, _ int) (map[string]any, string) {
		called = true
		return map[string]any{}, ""
	}
	mux := d.Mux()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/groups/1/keys-check", strings.NewReader(`{broken`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "bad json") {
		t.Fatalf("bad json: %d %s", w.Code, w.Body.String())
	}
	if called {
		t.Fatal("must reject before invoking KeysCheckFunc")
	}
}

func TestKeysCheckGroup_NotConfigured(t *testing.T) {
	// Given KeysCheckFunc 未装配 When keys-check Then 501
	d := testKeysCheckDeps()
	mux := d.Mux()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/groups/1/keys-check", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented || !strings.Contains(w.Body.String(), "not configured") {
		t.Fatalf("501: %d %s", w.Code, w.Body.String())
	}
}

func TestKeysCheckGroup_NotFoundPrefix(t *testing.T) {
	// Given KeysCheckFunc 返回 "not found" 前缀错误 When keys-check Then 404;其余前缀 400
	d := testKeysCheckDeps()
	d.KeysCheckFunc = func(_ context.Context, _ int64, _, _ string, _ int) (map[string]any, string) {
		return nil, "not found: group 7 not found"
	}
	mux := d.Mux()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/groups/7/keys-check", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("404: %d %s", w.Code, w.Body.String())
	}

	d.KeysCheckFunc = func(_ context.Context, _ int64, _, _ string, _ int) (map[string]any, string) {
		return nil, "keys-check param: key pool empty"
	}
	req2 := httptest.NewRequest(http.MethodPost, "/admin/api/groups/7/keys-check", strings.NewReader(`{}`))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("400: %d %s", w2.Code, w2.Body.String())
	}
}

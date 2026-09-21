package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── P8:包闭环(模板 → 安装 → 实例化 → 导出 → 再导入 → E2E 连通) ───

// adminLogin 管理会话 cookie
func (f *fourGroupsFixture) adminLogin(t *testing.T) *http.Cookie {
	t.Helper()
	body := `{"user":"` + adminTestUser + `","password":"` + adminTestPass + `"}`
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/admin/api/login", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin login: %d", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == "aap_session" {
			return c
		}
	}
	t.Fatal("no session cookie")
	return nil
}

// adminGet 管理端 GET(带会话;模板等免会话端点同样可用)
func (f *fourGroupsFixture) adminGet(t *testing.T, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, f.gateway.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.adminCookie == nil {
		f.adminCookie = f.adminLogin(t)
	}
	req.AddCookie(f.adminCookie)
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

// adminGetRaw 管理端 GET(string 体)
func (f *fourGroupsFixture) adminGetRaw(t *testing.T, path string) (int, string) {
	t.Helper()
	code, b := f.adminGet(t, path)
	return code, string(b)
}

// adminPost 管理端 POST(带会话)
func (f *fourGroupsFixture) adminPost(t *testing.T, path, body string) (int, string) {
	t.Helper()
	return f.adminMethod(t, http.MethodPost, path, body)
}

// adminPut 管理端 PUT(带会话)
func (f *fourGroupsFixture) adminPut(t *testing.T, path, body string) (int, string) {
	t.Helper()
	return f.adminMethod(t, http.MethodPut, path, body)
}

// adminMethod 管理端带会话请求
func (f *fourGroupsFixture) adminMethod(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	if f.adminCookie == nil {
		f.adminCookie = f.adminLogin(t)
	}
	req, err := http.NewRequest(method, f.gateway.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(f.adminCookie)
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// TestPackageLoop_TemplateInstallExportReimport 闭环
func TestPackageLoop_TemplateInstallExportReimport(t *testing.T) {
	// Given 完整装配网关 When 模板下载→安装→实例化→导出→再导入 Then revision+1 且 E2E 连通
	f := newFourGroups(t)
	ctx := context.Background()

	// ① 模板下载(免会话静态端点)
	status, tplBytes := f.adminGet(t, "/packages/template")
	if status != http.StatusOK || len(tplBytes) == 0 {
		t.Fatalf("template: %d %d bytes", status, len(tplBytes))
	}

	// ② 安装模板包
	if err := f.app.AdminDeps.Packages.Install(ctx, tplBytes); err != nil {
		t.Fatalf("install template: %v", err)
	}
	rev1 := f.app.AdminDeps.Packages.Revision("my-package")
	if rev1 != 1 {
		t.Fatalf("revision after first install: %d", rev1)
	}

	// ③ 实例化(filter 部件可 NewFilter;protocol 缺 api_key 不影响实例化)
	pkg, err := f.app.AdminDeps.Packages.GetPackage("my-package")
	if err != nil {
		t.Fatal(err)
	}
	if len(pkg.Manifest.Parts.Filters) != 1 {
		t.Fatalf("template filters: %d", len(pkg.Manifest.Parts.Filters))
	}

	// ④ 导出(zip 魔数 PK)
	status, exported := f.adminGet(t, "/admin/api/packages/my-package/export")
	if status != http.StatusOK || len(exported) < 2 || exported[0] != 'P' || exported[1] != 'K' {
		t.Fatalf("export: %d %d bytes", status, len(exported))
	}

	// ⑤ 再导入(同 name 升级 revision+1,实例配置保留)
	if err := f.app.AdminDeps.Packages.Install(ctx, exported); err != nil {
		t.Fatalf("reimport: %v", err)
	}
	rev2 := f.app.AdminDeps.Packages.Revision("my-package")
	if rev2 != rev1+1 {
		t.Fatalf("revision after reimport: %d (want %d)", rev2, rev1+1)
	}

	// ⑥ import-url:本地 httptest 服务中转(真实 HTTP 拉取;源站在回环,经 Deps.FetchPackage 覆写绕过公网校验,
	// SSRF 拦截由 TestAdmin_InstallURLPrivateBlocked 覆盖)
	f.app.AdminDeps.FetchPackage = func(r *http.Request, u string) ([]byte, error) {
		resp, err := http.Get(u)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch status %d", resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}
	urlSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(exported)
	}))
	defer urlSrv.Close()
	status, body := f.adminPost(t, "/admin/api/packages/import-url", `{"url":"`+urlSrv.URL+`/my-package.aap"}`)
	if status != http.StatusOK {
		t.Fatalf("import-url: %d %s", status, body)
	}
	if f.app.AdminDeps.Packages.Revision("my-package") != rev2+1 {
		t.Fatalf("revision after import-url: %d", f.app.AdminDeps.Packages.Revision("my-package"))
	}

	// ⑦ 用模板包的 filter 实例化一个上游(过滤=透传)+ js-openai 主包 → 真实 E2E 连通
	if err := f.app.Registry.Save(ctx, &upstream.Upstream{
		Name: "g6-loop", Enabled: true,
		Base:   upstream.PackageRef{Package: "js-openai"},
		Extras: []upstream.PackageRef{{Package: "my-package"}},
		Models: []string{"m-loop"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: f.upstreamBase, Transport: "", Enabled: true,
			Secrets: map[string]string{"api_key": upstreamAPIKey}}},
	}); err != nil {
		t.Fatalf("save loop upstream: %v", err)
	}
	payload := `{"model":"m-loop","messages":[{"role":"user","content":"hello"}],"stream":false}`
	code, respBody := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, payload)
	if code != http.StatusOK {
		t.Fatalf("loop e2e: %d %s", code, respBody)
	}
	if !strings.Contains(respBody, `"content":"hi"`) {
		t.Fatalf("loop e2e body: %s", respBody)
	}
}

// TestPackageUpload_BrowserCSRFPath 模拟浏览器文件上传(octet-stream + Sec-Fetch-Site 同源)
func TestPackageUpload_BrowserCSRFPath(t *testing.T) {
	f := newFourGroups(t)
	_, tpl := f.adminGet(t, "/packages/template")
	// 同源上传(Sec-Fetch-Site: same-origin)→ 200
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/admin/api/packages", bytes.NewReader(tpl))
	if err != nil {
		t.Fatal(err)
	}
	if f.adminCookie == nil {
		f.adminCookie = f.adminLogin(t)
	}
	req.AddCookie(f.adminCookie)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("same-origin upload: %d", resp.StatusCode)
	}
	// 跨站上传(Sec-Fetch-Site: cross-site)→ 403
	if err := f.app.AdminDeps.Packages.Delete(context.Background(), "my-package"); err != nil {
		t.Fatal(err)
	}
	req2, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/admin/api/packages", bytes.NewReader(tpl))
	if err != nil {
		t.Fatal(err)
	}
	req2.AddCookie(f.adminCookie)
	req2.Header.Set("Content-Type", "application/octet-stream")
	req2.Header.Set("Sec-Fetch-Site", "cross-site")
	resp2, err := f.client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site upload must 403: %d", resp2.StatusCode)
	}
}

// TestPackageDelete_ReferencedRejected 单独验证卸载语义(引用拒绝/无引用可删/内置拒删)
func TestPackageDelete_ReferencedRejected(t *testing.T) {
	f := newFourGroups(t)
	// ① 被 g1 引用的 js-openai:卸载 → 409 引用列表
	code, body := f.adminMethod(t, http.MethodDelete, "/admin/api/packages/js-openai", "")
	if code != http.StatusConflict || !strings.Contains(body, "referenced by") {
		t.Fatalf("delete referenced: %d %s", code, body)
	}
	// ② 无引用包:卸载 → 200;再卸载 → 404
	// rewrite-model 被 g3/g4 引用,构造一个全新无引用包(模板)
	status, tplBytes := f.adminGet(t, "/packages/template")
	if status != http.StatusOK {
		t.Fatalf("template: %d", status)
	}
	if err := f.app.AdminDeps.Packages.Install(context.Background(), tplBytes); err != nil {
		t.Fatal(err)
	}
	code, body = f.adminMethod(t, http.MethodDelete, "/admin/api/packages/my-package", "")
	if code != http.StatusOK {
		t.Fatalf("delete unreferenced: %d %s", code, body)
	}
	code, _ = f.adminMethod(t, http.MethodDelete, "/admin/api/packages/my-package", "")
	if code != http.StatusNotFound {
		t.Fatalf("delete twice: %d", code)
	}
	// ③ 内置包拒删
	code, body = f.adminMethod(t, http.MethodDelete, "/admin/api/packages/openai-compatible", "")
	if code == http.StatusOK {
		t.Fatalf("builtin deleted: %s", body)
	}
}

package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

func TestAdmin_InspectBytesReturnsSummaryNotInstall(t *testing.T) {
	// Given 合法 .aap 字节 When POST inspect Then 返回 manifest 摘要且包未安装
	pkgs, _ := testRegistry(t)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	data := hooksAAP(t, "0.1.0", map[string]string{"tasks/signIn.js": `module.exports={handler:function(ctx){}}`})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/api/packages/inspect", strings.NewReader(string(data)))
	r.Header.Set("Content-Type", "application/octet-stream")
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["name"] != "checkin" || out["version"] != "0.1.0" {
		t.Fatalf("summary: %v", out)
	}
	if _, ok := out["exists"]; !ok {
		t.Fatalf("exists field missing: %v", out)
	}
	if _, err := pkgs.GetPackage("checkin"); err == nil {
		t.Fatal("inspect must not install")
	}
}

func TestAdmin_InspectURL(t *testing.T) {
	// Given inspect JSON {"url"} 指向测试服务器 .aap When POST Then 拉取解析返回摘要且不安装
	// (httptest 源站在回环,经 Deps.FetchPackage 覆写拉取实现绕过公网校验;SSRF 拦截由 TestAdmin_InspectURLPrivateBlocked 覆盖)
	pkgs, _ := testRegistry(t)
	deps := newAdminDeps(pkgs)
	deps.FetchPackage = func(r *http.Request, url string) ([]byte, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		return io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	}
	mux := deps.Mux()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(hooksAAP(t, "0.2.0", map[string]string{"tasks/signIn.js": `module.exports={handler:function(ctx){}}`}))
	}))
	defer srv.Close()
	body, _ := json.Marshal(map[string]string{"url": srv.URL + "/x.aap"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/api/packages/inspect", strings.NewReader(string(body)))
	r.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["name"] != "checkin" || out["version"] != "0.2.0" {
		t.Fatalf("summary: %v", out)
	}
	if _, err := pkgs.GetPackage("checkin"); err == nil {
		t.Fatal("inspect must not install")
	}
}

func TestAdmin_InspectInvalidZip400(t *testing.T) {
	// Given 非法 zip When POST inspect Then 400 且不安装
	pkgs, _ := testRegistry(t)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/api/packages/inspect", strings.NewReader("not-a-zip"))
	r.Header.Set("Content-Type", "application/octet-stream")
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if _, err := pkgs.GetPackage("checkin"); err == nil {
		t.Fatal("inspect must not install")
	}
}

func TestAdmin_InstallURLPrivateBlocked(t *testing.T) {
	// Given import-url 指向环回地址 When POST Then 拒绝(与 inspect 同源 SSRF 防护)且不安装
	pkgs, _ := testRegistry(t)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	body, _ := json.Marshal(map[string]string{"url": "http://127.0.0.1:1/x.aap"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/api/packages/import-url", strings.NewReader(string(body)))
	r.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatalf("private address must be rejected: %s", w.Body.String())
	}
	if _, err := pkgs.GetPackage("checkin"); err == nil {
		t.Fatal("rejected import must not install")
	}
}

func TestAdmin_InstallBodyTooLarge413(t *testing.T) {
	// Given 上传体超 8MB When POST install Then 413(不静默截断)
	pkgs, _ := testRegistry(t)
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/api/packages", strings.NewReader(string(make([]byte, 8*1024*1024+1))))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if _, err := pkgs.GetPackage("checkin"); err == nil {
		t.Fatal("oversized upload must not install")
	}
}

func TestAdmin_ListPackagesIncludesProtocolName(t *testing.T) {
	// Given 含 protocol 部件的包 When GET packages Then 行内含协议全名;无 protocol 的包字段为空
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	_ = ctx
	mm := &plugin.Manifest{ManifestVersion: plugin.ManifestVersion, Name: "proto-pkg", Version: "1.0.0"}
	mm.Parts.Protocol = &plugin.ProtocolPart{
		Protocol: "anthropic-messages",
		Features: []string{"tools"},
	}
	data, err := plugin.BuildAAP(mm, map[string][]byte{plugin.ProtocolEntry: []byte(`module.exports={
		buildRequest:function(ctx,e){return{url:"http://x",method:"POST",headers:{},body:e};},
		mapEvent:function(ctx,ev){return "[]";}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Install(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/admin/api/packages", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row["name"] == "proto-pkg" {
			found = true
			if row["protocol"] != "anthropic-messages" {
				t.Fatalf("protocol field: %v", row["protocol"])
			}
		}
	}
	if !found {
		t.Fatal("package row missing")
	}
}

package admin

import (
	"context"
	"fmt"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
	"github.com/mzzsfy/ai-api-proxy/internal/subscribe"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
	_ "modernc.org/sqlite"
)

// newSubDeps 订阅 API 测试依赖(真实 store + engine + 本地 git fixture;URL 校验 allow-all)
func newSubDeps(t *testing.T) (*Deps, *store.Store, *plugin.Registry) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git CLI not available")
	}
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reg := plugin.NewRegistry(st.DB())
	if err := reg.LoadFromDB(ctx); err != nil {
		t.Fatal(err)
	}
	engine := &subscribe.Engine{
		Store:    st,
		Packages: reg,
		WorkDir:  t.TempDir(),
		CheckURL: func(string) error { return nil },
	}
	d := &Deps{
		Packages:           reg,
		Upstream:           upstream.NewRegistry(st.DB(), reg),
		Subscriptions:      engine,
		SubscriptionStore:  st,
		SubscriptionCheck:  func(string) error { return nil },
	}
	return d, st, reg
}

// seedRepoLocal 本地裸仓库(含一个合法包)
func seedRepoLocal(t *testing.T, pkgName string) string {
	t.Helper()
	base := t.TempDir()
	src := filepath.Join(base, "src")
	repo := filepath.Join(base, "repo.git")
	g := func(dir string, args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	g(base, "init", "-b", "main", src)
	pkgDir := filepath.Join(src, "plugins", pkgName)
	if err := os.MkdirAll(filepath.Join(pkgDir, "filters"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{"manifestVersion":1,"name":"` + pkgName + `","version":"1.0.0","parts":{"filters":[{"name":"identity"}]}}`
	if err := os.WriteFile(filepath.Join(pkgDir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	filter := `module.exports = function (config) {
	return { mapRequest: function (ctx, e) { return e; }, mapChunk: function (ctx, c) { return c; }, mapResponse: function (ctx, r) { return r; } };
};`
	if err := os.WriteFile(filepath.Join(pkgDir, "filters", "identity.js"), []byte(filter), 0o644); err != nil {
		t.Fatal(err)
	}
	g(src, "add", ".")
	g(src, "commit", "-m", "init")
	g(src, "clone", "--bare", src, repo)
	return repo
}

// mustJSON 请求体序列化(Windows 路径反斜杠需转义)
func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func doJSON(t *testing.T, mux *http.ServeMux, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {	t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w, out
}

func TestSubscriptions_CreateListTokenMasked(t *testing.T) {
	// Given 无订阅 When POST 建订阅 Then 列表返回且 token 只暴露是否已设置
	d, _, _ := newSubDeps(t)
	mux := d.Mux()
	w, resp := doJSON(t, mux, "POST", "/admin/api/subscriptions",
		`{"name":"repo-a","repo_url":"https://git.example.com/o/r","user":"u","path":"plugins/demo","token":"secret-tok"}`)
	if w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if resp["id"] == nil {
		t.Fatalf("id missing: %v", resp)
	}
	w, _ = doJSON(t, mux, "GET", "/admin/api/subscriptions", "")
	if w.Code != 200 {
		t.Fatalf("list: %d", w.Code)
	}
	var subs []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &subs); err != nil {
		t.Fatal(err)
	}
	if len(subs) != 1 {
		t.Fatalf("expect 1 subscription: %v", subs)
	}
	s := subs[0]
	if s["name"] != "repo-a" || s["token_set"] != true {
		t.Fatalf("list fields: %v", s)
	}
	if strings.Contains(w.Body.String(), "secret-tok") {
		t.Fatal("token leaked in list")
	}
}

func TestSubscriptions_CreateValidates(t *testing.T) {
	// Given 各非法输入 When POST Then 400
	d, _, _ := newSubDeps(t)
	mux := d.Mux()
	cases := []struct {
		name string
		body string
	}{
		{"no name", `{"name":"","repo_url":"https://x.example.com/r"}`},
		{"no url", `{"name":"a","repo_url":""}`},
		{"path traversal", `{"name":"a","repo_url":"https://x.example.com/r","path":"../up"}`},
		{"path backslash", `{"name":"a","repo_url":"https://x.example.com/r","path":"..\\up"}`},
		{"duplicate name", `{"name":"a","repo_url":"https://x.example.com/r"}`},
	}
	// 第 4 例需要先建一个 a
	if _, r := doJSON(t, mux, "POST", "/admin/api/subscriptions", `{"name":"a","repo_url":"https://x.example.com/r"}`); r == nil {
		t.Fatal("setup create failed")
	}
	for _, c := range cases {
		w, _ := doJSON(t, mux, "POST", "/admin/api/subscriptions", c.body)
		if w.Code != 400 {
			t.Fatalf("%s: expect 400 got %d %s", c.name, w.Code, w.Body.String())
		}
	}
}

func TestSubscriptions_SyncAndDeleteLifecycle(t *testing.T) {
	// Given 本地裸仓库订阅 When 建订阅→同步→删订阅 Then 包出现后随删除卸载
	d, st, reg := newSubDeps(t)
	mux := d.Mux()
	repo := seedRepoLocal(t, "demo")

	w, resp := doJSON(t, mux, "POST", "/admin/api/subscriptions", mustJSON(t, map[string]any{
		"name": "repo-a", "repo_url": repo, "path": "plugins/demo",
	}))
	if w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	id := int64(resp["id"].(float64))

	w, _ = doJSON(t, mux, "POST", fmt.Sprintf("/admin/api/subscriptions/%d/sync", id), "")
	if w.Code != 200 {
		t.Fatalf("sync: %d %s", w.Code, w.Body.String())
	}
	if _, err := reg.GetPackage("demo"); err != nil {
		t.Fatalf("package not installed: %v", err)
	}
	if src, _ := reg.PackageSource("demo"); src != "subscription/repo-a" {
		t.Fatalf("source: %q", src)
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if sub.LastSyncedCommit == "" {
		t.Fatal("sync state not recorded")
	}

	w, _ = doJSON(t, mux, "DELETE", fmt.Sprintf("/admin/api/subscriptions/%d", id), "")
	if w.Code != 200 {
		t.Fatalf("delete: %d %s", w.Code, w.Body.String())
	}
	if _, err := reg.GetPackage("demo"); err == nil {
		t.Fatal("package must be uninstalled after subscription delete")
	}
}

func TestSubscriptions_SyncUnknown404(t *testing.T) {
	// Given 不存在的订阅 When 同步 Then 404
	d, _, _ := newSubDeps(t)
	mux := d.Mux()
	w, _ := doJSON(t, mux, "POST", "/admin/api/subscriptions/999/sync", "")
	if w.Code != 404 {
		t.Fatalf("expect 404 got %d", w.Code)
	}
}

func TestSubscriptions_Unwired501(t *testing.T) {
	// Given 未装配订阅引擎 When 任一订阅路由 Then 501
	d, _, _ := newSubDeps(t)
	d.Subscriptions = nil
	d.SubscriptionStore = nil
	mux := d.Mux()
	for _, m := range []struct {
		method, path, body string
	}{
		{"POST", "/admin/api/subscriptions", `{"name":"a"}`},
		{"GET", "/admin/api/subscriptions", ""},
		{"DELETE", "/admin/api/subscriptions/1", ""},
		{"POST", "/admin/api/subscriptions/1/sync", ""},
	} {
		w, _ := doJSON(t, mux, m.method, m.path, m.body)
		if w.Code != 501 {
			t.Fatalf("%s %s: expect 501 got %d", m.method, m.path, w.Code)
		}
	}
}

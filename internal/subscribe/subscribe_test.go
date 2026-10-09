package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
	_ "modernc.org/sqlite"
)

// gitAvailable 测试环境须有 git CLI
func gitAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git CLI not available")
	}
}

// runGit 执行 git 命令
func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// pkgManifest 测试包 manifest(仅 filter 部件,免 JS 协议依赖)
func pkgManifest(name string) string {
	return `{"manifestVersion":1,"name":"` + name + `","version":"1.0.0","parts":{"filters":[{"name":"identity"}]}}`
}

const filterSrc = `module.exports = function (config) {
	return { mapRequest: function (ctx, e) { return e; }, mapChunk: function (ctx, c) { return c; }, mapResponse: function (ctx, r) { return r; } };
};`

// seedRepo 建裸仓库并写入一次提交(pkgPath 下含 manifest.json + filter)
func seedRepo(t *testing.T, pkgName, pkgPath string) string {
	t.Helper()
	base := t.TempDir()
	src := filepath.Join(base, "src")
	repo := filepath.Join(base, "repo.git")
	runGit(t, base, "init", "-b", "main", src)
	pkgDir := filepath.Join(src, filepath.FromSlash(pkgPath))
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writePkgFiles(t, pkgDir, pkgManifest(pkgName))
	runGit(t, src, "add", ".")
	runGit(t, src, "commit", "-m", "init")
	runGit(t, src, "clone", "--bare", src, repo)
	return repo
}

// writePkgFiles 写包目录文件
func writePkgFiles(t *testing.T, dir, manifest string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "filters"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "filters", "identity.js"), []byte(filterSrc), 0o644); err != nil {
		t.Fatal(err)
	}
}

// commitRepo 对裸仓库追加一次提交(包内容变化:manifest 版本自增)
func commitRepo(t *testing.T, repo string, pkgName, pkgPath string) {
	t.Helper()
	work := t.TempDir()
	runGit(t, work, "clone", repo, "w")
	w := filepath.Join(work, "w")
	writePkgFiles(t, filepath.Join(w, filepath.FromSlash(pkgPath)), pkgManifest(pkgName))
	// 追加变化内容避免空提交
	f := filepath.Join(w, filepath.FromSlash(pkgPath), "filters", "identity.js")
	cur, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, append(cur, []byte("\n// rev2\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, w, "add", ".")
	runGit(t, w, "commit", "-m", "update")
	runGit(t, w, "push", "origin", "HEAD:main")
}

// newEngine 测试引擎(allow-all URL 校验;真实 store + registry)
func newEngine(t *testing.T) (*Engine, *store.Store, *plugin.Registry, string) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg := plugin.NewRegistry(st.DB())
	if err := reg.LoadFromDB(context.Background()); err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	e := &Engine{
		Store:    st,
		Packages: reg,
		WorkDir:  work,
		CheckURL: func(string) error { return nil },
	}
	return e, st, reg, work
}

// createSub 建订阅
func createSub(t *testing.T, st *store.Store, name, repo, path string) int64 {
	t.Helper()
	id, err := st.CreateSubscription(context.Background(), &store.Subscription{
		Name: name, RepoURL: repo, User: "u", Path: path, Token: "tok",
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSync_FirstInstall(t *testing.T) {
	// Given 裸仓库含合法包 When 首次同步 Then 包以订阅来源安装且同步态写入
	gitAvailable(t)
	e, st, reg, work := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")

	if err := e.Sync(context.Background(), id); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if src, ok := reg.PackageSource("demo"); !ok || src != "subscription/repo-a" {
		t.Fatalf("installed source: %q ok=%v", src, ok)
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if sub.LastSyncedCommit == "" || sub.Branch != "main" || sub.LastError != "" {
		t.Fatalf("sync state: %+v", sub)
	}
	if _, err := os.Stat(filepath.Join(work, "1")); err != nil {
		t.Fatalf("workdir: %v", err)
	}
}

func TestSync_UnchangedSkips(t *testing.T) {
	// Given 已同步 When 无新提交再次同步 Then revision 不变
	gitAvailable(t)
	e, st, reg, _ := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	ctx := context.Background()
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	rev := reg.Revision("demo")
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	if reg.Revision("demo") != rev {
		t.Fatalf("revision changed on no-op sync: %d -> %d", rev, reg.Revision("demo"))
	}
}

func TestSync_UnchangedSkipClearsLastError(t *testing.T) {
	// Given 同步失败留有 last_error 且上游无新提交 When 再次同步命中跳过 Then last_error 被清除(成功语义不被旧错误遮蔽)
	gitAvailable(t)
	e, st, _, _ := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	ctx := context.Background()
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateSubscriptionError(ctx, id, "transient fetch failure"); err != nil {
		t.Fatal(err)
	}
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	sub, _ := st.GetSubscription(ctx, id)
	if sub.LastError != "" {
		t.Fatalf("last_error must be cleared on successful no-op sync: %q", sub.LastError)
	}
}

func TestSync_UpstreamUpdate(t *testing.T) {
	// Given 已同步 When 上游新提交 Then 包升级且 commit 前移
	gitAvailable(t)
	e, st, reg, _ := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	ctx := context.Background()
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	first, _ := st.GetSubscription(ctx, id)
	commitRepo(t, repo, "demo", "plugins/demo")
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	second, _ := st.GetSubscription(ctx, id)
	if second.LastSyncedCommit == first.LastSyncedCommit {
		t.Fatal("commit must advance")
	}
	if reg.Revision("demo") != 2 {
		t.Fatalf("revision after update: %d", reg.Revision("demo"))
	}
}

func TestSync_MissingManifestError(t *testing.T) {
	// Given path 下无 manifest When 同步 Then 报错且 last_error 记录
	gitAvailable(t)
	e, st, _, _ := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/absent")
	if err := e.Sync(context.Background(), id); err == nil {
		t.Fatal("expect error")
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if sub.LastError == "" {
		t.Fatal("last_error must be recorded")
	}
}

func TestSync_LocalNameConflictRejected(t *testing.T) {
	// Given local 同名包已存在 When 订阅同步 Then 拒绝且 last_error 记录
	gitAvailable(t)
	e, st, reg, _ := newEngine(t)
	// local 安装同名包
	data := buildAAPBytes(t, pkgManifest("demo"))
	if err := reg.Install(context.Background(), data, ""); err != nil {
		t.Fatal(err)
	}
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	if err := e.Sync(context.Background(), id); err == nil {
		t.Fatal("expect conflict rejection")
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if sub.LastError == "" {
		t.Fatal("last_error must be recorded")
	}
	if src, _ := reg.PackageSource("demo"); src != "local" {
		t.Fatalf("local package must be untouched: %q", src)
	}
}

func TestSync_RenamedOldPackageUninstalled(t *testing.T) {
	// Given 已同步 demo When 上游包改名 demo2 同步 Then demo 卸载、demo2 存在
	gitAvailable(t)
	e, st, reg, _ := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	ctx := context.Background()
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	commitRepo(t, repo, "demo2", "plugins/demo")
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.GetPackage("demo"); err == nil {
		t.Fatal("old-name package must be uninstalled")
	}
	if _, err := reg.GetPackage("demo2"); err != nil {
		t.Fatalf("renamed package missing: %v", err)
	}
}

func TestSync_ErrorMasksToken(t *testing.T) {
	// Given token 非空且远端不可达 When 同步 Then 错误与 last_error 均不含 token 明文
	gitAvailable(t)
	e, st, _, _ := newEngine(t)
	id, err := st.CreateSubscription(context.Background(), &store.Subscription{
		Name: "repo-a", RepoURL: "https://invalid.invalid/repo.git", User: "u", Path: "plugins/demo", Token: "secret-token-123",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = e.Sync(context.Background(), id)
	if err == nil {
		t.Fatal("expect error")
	}
	if strings.Contains(err.Error(), "secret-token-123") {
		t.Fatalf("returned error leaks token: %v", err)
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if strings.Contains(sub.LastError, "secret-token-123") {
		t.Fatalf("last_error leaks token: %q", sub.LastError)
	}
}

func TestSync_PathTraversalRejected(t *testing.T) {
	// Given path 穿越工作区 When 同步 Then 直接拒绝(不触碰 git)
	gitAvailable(t)
	e, st, _, _ := newEngine(t)
	id := createSub(t, st, "repo-a", "https://example.com/repo.git", "../escape")
	if err := e.Sync(context.Background(), id); err == nil {
		t.Fatal("expect traversal rejection")
	}
	sub, _ := st.GetSubscription(context.Background(), id)
	if sub.LastError == "" {
		t.Fatal("last_error must be recorded")
	}
}

func TestDelete_UninstallsAndCleansWorkdir(t *testing.T) {
	// Given 已同步订阅 When 删除 Then 包卸载、工作区清除、行删除
	gitAvailable(t)
	e, st, reg, work := newEngine(t)
	repo := seedRepo(t, "demo", "plugins/demo")
	id := createSub(t, st, "repo-a", repo, "plugins/demo")
	ctx := context.Background()
	if err := e.Sync(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.GetPackage("demo"); err == nil {
		t.Fatal("package must be uninstalled")
	}
	if _, err := os.Stat(filepath.Join(work, "1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workdir must be removed: %v", err)
	}
	if _, err := st.GetSubscription(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("row must be deleted: %v", err)
	}
}

func TestAuthURL_CredentialInjection(t *testing.T) {
	// Given user/token 含 URL 保留字符 When 构造认证 URL Then percent-encode 且 https 原样
	gitAvailable(t)
	sub := &store.Subscription{
		RepoURL: "https://git.example.com/org/repo",
		User:    "u@ser",
		Token:   "pa:ss@word",
	}
	u, err := authURL(sub)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(u, "https://") {
		t.Fatalf("scheme: %s", u)
	}
	if strings.Contains(u, "pa:ss") || strings.Contains(u, "u@ser:x") {
		t.Fatalf("credentials must be encoded: %s", u)
	}
	if !strings.HasSuffix(u, "@git.example.com/org/repo") {
		t.Fatalf("url tail: %s", u)
	}
	// token 为空:URL 原样
	sub.Token = ""
	u2, err := authURL(sub)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(u2, "@") && strings.Contains(u2, "u@") {
		t.Fatalf("no credential expected: %s", u2)
	}
}

func TestCheckPublicURL(t *testing.T) {
	// Given 各类 URL When 公网校验 Then 非 https 拒、私网 IP 拒、公网 IP 过
	if err := CheckPublicURL("http://example.com/repo"); err == nil {
		t.Fatal("http must be rejected")
	}
	if err := CheckPublicURL("https://127.0.0.1/repo"); err == nil {
		t.Fatal("loopback must be rejected")
	}
	if err := CheckPublicURL("https://192.168.1.1/repo"); err == nil {
		t.Fatal("private must be rejected")
	}
	if err := CheckPublicURL("https://8.8.8.8/repo"); err != nil {
		t.Fatalf("public IP must pass: %v", err)
	}
	if err := CheckPublicURL("not a url"); err == nil {
		t.Fatal("garbage must be rejected")
	}
}

// buildAAPBytes 测试包字节
func buildAAPBytes(t *testing.T, manifest string) []byte {
	t.Helper()
	files := map[string][]byte{"filters/identity.js": []byte(filterSrc)}
	data, err := plugin.BuildAAP(parseManifest(t, manifest), files)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func parseManifest(t *testing.T, raw string) *plugin.Manifest {
	t.Helper()
	m := &plugin.Manifest{}
	if err := json.Unmarshal([]byte(raw), m); err != nil {
		t.Fatal(err)
	}
	return m
}

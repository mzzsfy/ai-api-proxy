package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
)

// testRegistry 带迁移库的插件注册中心(t.TempDir 隔离)
func testRegistry(t *testing.T) (*plugin.Registry, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return plugin.NewRegistry(st.DB()), st
}

// writeAAP 构造最小可用 JS 协议包并落盘
func writeAAP(t *testing.T, dir, name, version, js string) {
	t.Helper()
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"` + version + `","parts":{
		"protocol":{"entry":"p.js","form":["streaming","non_streaming"],"features":["tools"],"secretRefs":["api_key"]}}}`
	data, err := plugin.BuildAAP(
		mustManifest(t, manifest),
		map[string][]byte{"p.js": []byte(js)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".aap"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustManifest(t *testing.T, raw string) *plugin.Manifest {
	t.Helper()
	m := &plugin.Manifest{}
	if err := jsonUnmarshal([]byte(raw), m); err != nil {
		t.Fatal(err)
	}
	return m
}

const importTestJS = `module.exports = {
  buildRequest: function (ctx, pivot) { return { url: ctx.target.baseUrl, method: "POST", headers: {}, body: pivot, stream: false }; },
  mapEvent: function (ctx, e) { return e; },
  mapResponse: function (ctx, body) { return body; }
};`

func TestImportPluginsDir_InstallsAndSkips(t *testing.T) {
	// Given 目录内一个合法 .aap When 首次导入 Then 安装 revision 1;再次导入同内容 Then revision 不变
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)

	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatalf("first import: %v", err)
	}
	pkg, err := pkgs.GetPackage("zcode")
	if err != nil {
		t.Fatalf("package missing: %v", err)
	}
	if pkg.Revision != 1 {
		t.Fatalf("revision after first import: %d", pkg.Revision)
	}

	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatalf("second import: %v", err)
	}
	if pkg2, _ := pkgs.GetPackage("zcode"); pkg2.Revision != 1 {
		t.Fatalf("revision drifted on idempotent import: %d", pkg2.Revision)
	}
}

func TestImportPluginsDir_SkipsNonAAP(t *testing.T) {
	// Given 目录含子目录与非 .aap 文件 When 导入 Then 忽略,不报错
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.aap"), []byte("not a zip"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatalf("import with mixed entries: %v", err)
	}
	if _, err := pkgs.GetPackage("zcode"); err != nil {
		t.Fatalf("valid package not installed: %v", err)
	}
}

func TestImportPluginsDir_MissingDirOK(t *testing.T) {
	// Given 目录不存在 When 导入 Then 静默成功(可选目录语义)
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	if err := importPluginsDir(ctx, pkgs, filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatalf("missing dir: %v", err)
	}
}

func TestImportPluginsDir_DisabledStatePreserved(t *testing.T) {
	// Given 包被管理面禁用 When 再导入同内容 Then 保持禁用(跳过不重装)
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Enable(ctx, "zcode", false); err != nil {
		t.Fatal(err)
	}
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatal(err)
	}
	if pkgs.IsEnabled("zcode") {
		t.Fatal("disabled package re-enabled by idempotent import")
	}
}

func TestImportPluginsDir_IdempotentAcrossReload(t *testing.T) {
	// Given 包经 DB JSON roundtrip 恢复(LoadFromDB)When 导入同内容 Then revision 不变
	// (roundtrip 后 nil/空 slice、RawMessage 字节与同进程 Install 形态可能不同,DeepEqual 最脆路径)
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatal(err)
	}
	revBefore := pkgs.Revision("zcode")

	reloaded := plugin.NewRegistry(st.DB())
	if err := reloaded.LoadFromDB(ctx); err != nil {
		t.Fatal(err)
	}
	if err := importPluginsDir(ctx, reloaded, dir); err != nil {
		t.Fatal(err)
	}
	if reloaded.Revision("zcode") != revBefore {
		t.Fatalf("revision drifted across reload: %d -> %d", revBefore, reloaded.Revision("zcode"))
	}
}

func TestImportPluginsDir_UpgradesChangedContent(t *testing.T) {
	// Given 同名包内容变化 When 再导入 Then revision+1 且新内容生效
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatal(err)
	}
	writeAAP(t, dir, "zcode", "1.1.0", importTestJS)
	if err := importPluginsDir(ctx, pkgs, dir); err != nil {
		t.Fatal(err)
	}
	pkg, err := pkgs.GetPackage("zcode")
	if err != nil {
		t.Fatal(err)
	}
	if pkg.Revision != 2 || pkg.Manifest.Version != "1.1.0" {
		t.Fatalf("upgrade: rev=%d version=%s", pkg.Revision, pkg.Manifest.Version)
	}
}

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

// testDirs 两级插件目录(打包输出目录无关用例)
func testDirs(pluginsDir, builtinDir string) *Config {
	return &Config{PluginsDir: pluginsDir, BuiltinDir: builtinDir, PackDir: filepath.Join(os.TempDir(), "aap-unused")}
}

// writeAAP 构造最小可用 JS 协议包并落盘
func writeAAP(t *testing.T, dir, name, version, js string) {
	t.Helper()
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"` + version + `","parts":{
		"protocol":{"entry":"p.js","protocol":"openai-completions","form":["streaming","non_streaming"],"features":["tools"],"secretRefs":["api_key"]}}}`
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

// writePackageDir 构造包目录(目录→.aap 打包入口的输入形态)
func writePackageDir(t *testing.T, root, name, version, protocol, js string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "test"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"` + version + `","parts":{
		"protocol":{"entry":"p.js","protocol":"` + protocol + `","form":["streaming","non_streaming"],"features":["tools"],"secretRefs":["api_key"]}}}`
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p.js"), []byte(js), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "test", "p.test.js"), []byte("// 测试资产随包分发\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
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
  buildRequest: function (ctx, entry) { return { url: ctx.target.baseUrl, method: "POST", headers: {}, body: entry, stream: false }; },
  mapEvent: function (ctx, e) { return JSON.stringify([{ delta: e }]); },
  mapResponse: function (ctx, body) { return body; }
};`

func TestImportPluginDirs_InstallsAndSkips(t *testing.T) {
	// Given 目录内一个合法 .aap When 首次导入 Then 安装 revision 1;再次导入同内容 Then revision 不变
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	cfg := testDirs(dir, filepath.Join(t.TempDir(), "builtin"))

	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatalf("first import: %v", err)
	}
	pkg, err := pkgs.GetPackage("zcode")
	if err != nil {
		t.Fatalf("package missing: %v", err)
	}
	if pkg.Revision != 1 {
		t.Fatalf("revision after first import: %d", pkg.Revision)
	}

	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatalf("second import: %v", err)
	}
	if pkg2, _ := pkgs.GetPackage("zcode"); pkg2.Revision != 1 {
		t.Fatalf("revision drifted on idempotent import: %d", pkg2.Revision)
	}
}

func TestImportPluginDirs_SkipsForeignEntries(t *testing.T) {
	// Given 目录含无 manifest 的子目录、非 .aap 文件与坏 .aap When 导入 Then 忽略,不报错
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	if err := os.MkdirAll(filepath.Join(dir, "test"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.aap"), []byte("not a zip"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importPluginDirs(ctx, pkgs, testDirs(dir, filepath.Join(t.TempDir(), "builtin"))); err != nil {
		t.Fatalf("import with mixed entries: %v", err)
	}
	if _, err := pkgs.GetPackage("zcode"); err != nil {
		t.Fatalf("valid package not installed: %v", err)
	}
}

func TestImportPluginDirs_MissingDirOK(t *testing.T) {
	// Given 两级目录均不存在 When 导入 Then 静默成功(可选目录语义)
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	absent := filepath.Join(t.TempDir(), "absent")
	if err := importPluginDirs(ctx, pkgs, testDirs(absent, absent)); err != nil {
		t.Fatalf("missing dir: %v", err)
	}
}

func TestImportPluginDirs_DisabledStatePreserved(t *testing.T) {
	// Given 包被管理面禁用 When 再导入同内容 Then 保持禁用(跳过不重装)
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	cfg := testDirs(dir, filepath.Join(t.TempDir(), "builtin"))
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Enable(ctx, "zcode", false); err != nil {
		t.Fatal(err)
	}
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatal(err)
	}
	if pkgs.IsEnabled("zcode") {
		t.Fatal("disabled package re-enabled by idempotent import")
	}
}

func TestImportPluginDirs_IdempotentAcrossReload(t *testing.T) {
	// Given 包经 DB JSON roundtrip 恢复(LoadFromDB)When 导入同内容 Then revision 不变
	// (roundtrip 后 nil/空 slice、RawMessage 字节与同进程 Install 形态可能不同,DeepEqual 最脆路径)
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	cfg := testDirs(dir, filepath.Join(t.TempDir(), "builtin"))
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatal(err)
	}
	revBefore := pkgs.Revision("zcode")

	reloaded := plugin.NewRegistry(st.DB())
	if err := reloaded.LoadFromDB(ctx); err != nil {
		t.Fatal(err)
	}
	if err := importPluginDirs(ctx, reloaded, cfg); err != nil {
		t.Fatal(err)
	}
	if reloaded.Revision("zcode") != revBefore {
		t.Fatalf("revision drifted across reload: %d -> %d", revBefore, reloaded.Revision("zcode"))
	}
}

func TestImportPluginDirs_UpgradesChangedContent(t *testing.T) {
	// Given 同名包内容变化 When 再导入 Then revision+1 且新内容生效
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writeAAP(t, dir, "zcode", "1.0.0", importTestJS)
	cfg := testDirs(dir, filepath.Join(t.TempDir(), "builtin"))
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		t.Fatal(err)
	}
	writeAAP(t, dir, "zcode", "1.1.0", importTestJS)
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
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

func TestImportPluginDirs_PackageDirPackedAndInstalled(t *testing.T) {
	// Given plugins_dir 下含 manifest.json 的包目录(含子目录资产)When 导入 Then 直接打包安装
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	dir := t.TempDir()
	writePackageDir(t, dir, "zcode", "2.0.0", "openai-completions", importTestJS)

	if err := importPluginDirs(ctx, pkgs, testDirs(dir, filepath.Join(t.TempDir(), "builtin"))); err != nil {
		t.Fatalf("import package dir: %v", err)
	}
	pkg, err := pkgs.GetPackage("zcode")
	if err != nil {
		t.Fatalf("package dir not installed: %v", err)
	}
	if pkg.Manifest.Version != "2.0.0" {
		t.Fatalf("version: %s", pkg.Manifest.Version)
	}
	if _, ok := pkg.Files["test/p.test.js"]; !ok {
		t.Fatalf("subdir asset missing from packed package: %v", pkg.Files)
	}
}

func TestImportPluginDirs_BuiltinFillsGapOnly(t *testing.T) {
	// Given builtin_dir 与 plugins_dir 同名包 When 导入 Then 用户包胜出;内置独有包补齐
	ctx := context.Background()
	pkgs, _ := testRegistry(t)
	plug := t.TempDir()
	builtin := t.TempDir()
	writeAAP(t, plug, "zcode", "1.0.0", importTestJS)
	writeAAP(t, builtin, "zcode", "9.9.9", importTestJS)
	writeAAP(t, builtin, "only-builtin", "1.0.0", importTestJS)

	if err := importPluginDirs(ctx, pkgs, testDirs(plug, builtin)); err != nil {
		t.Fatal(err)
	}
	if pkg, _ := pkgs.GetPackage("zcode"); pkg.Manifest.Version != "1.0.0" {
		t.Fatalf("builtin overwrote user package: %s", pkg.Manifest.Version)
	}
	if _, err := pkgs.GetPackage("only-builtin"); err != nil {
		t.Fatalf("builtin-only package not installed: %v", err)
	}
}

func TestPackPluginsDir(t *testing.T) {
	// Given 两级目录各有一个包目录 When 打包 Then PackDir 下产出同名 .aap,且可被 ParseAAP 解析
	root := t.TempDir()
	plug := filepath.Join(root, "plugins")
	builtin := filepath.Join(root, "builtin")
	packDir := filepath.Join(root, "packed")
	writePackageDir(t, plug, "zcode", "1.0.0", "openai-completions", importTestJS)
	writePackageDir(t, builtin, "openai-compatible", "1.0.0", "openai-completions", importTestJS)
	cfg := &Config{PluginsDir: plug, BuiltinDir: builtin, PackDir: packDir}

	written, err := packPluginsDir(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(written) != 2 {
		t.Fatalf("packed %d packages, want 2: %v", len(written), written)
	}
	for _, name := range []string{"zcode.aap", "openai-compatible.aap"} {
		data, err := os.ReadFile(filepath.Join(packDir, name))
		if err != nil {
			t.Fatalf("read packed %s: %v", name, err)
		}
		pkg, err := plugin.ParseAAP(data)
		if err != nil {
			t.Fatalf("packed %s not installable: %v", name, err)
		}
		if pkg.Manifest.Parts.Protocol.Protocol != "openai-completions" {
			t.Fatalf("%s protocol slot lost in packing: %q", name, pkg.Manifest.Parts.Protocol.Protocol)
		}
	}
}

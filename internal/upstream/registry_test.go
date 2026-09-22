package upstream

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"

	_ "modernc.org/sqlite"
)

// buildZip 内存 zip(与 plugin 包测试同构,避免跨包测试依赖)
func buildZip(t *testing.T, manifest string, files map[string]string) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mf.Write([]byte(manifest)); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// v2Ddl 模型行表(测试库;与 004 迁移后形态一致)
func v2Ddl() []string {
	return []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, base_package TEXT NOT NULL,
			params_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (name, base_package))`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
		`CREATE TABLE package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE package_settings (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
	}
}

func testEnv(t *testing.T) (*plugin.Registry, *Registry) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/t.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range v2Ddl() {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	pkgs := plugin.NewRegistry(db)
	reg := NewRegistry(db, pkgs)
	return pkgs, reg
}

func installBuiltinLikePkg(t *testing.T, pkgs *plugin.Registry, name string) {
	t.Helper()
	installBuiltinLikePkgForm(t, pkgs, name, `["streaming","non_streaming"]`, `["tools"]`)
}

// installBuiltinLikePkgForm 按给定 form/features 装包(形态把关用例;form 即实现——导出哪个 map 钩子就支持哪形态)
// v2:参数槽 base_url 由 protocol.js settings 片段声明;密钥=包级 keys
func installBuiltinLikePkgForm(t *testing.T, pkgs *plugin.Registry, name, form, features string) {
	t.Helper()
	var impl string
	if strings.Contains(form, `"streaming"`) {
		impl += `mapEvent: function (ctx, e) { return e; },`
	}
	if strings.Contains(form, `"non_streaming"`) {
		impl += `mapResponse: function (ctx, b) { return b; },`
	}
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"1.0.0","parts":{
		"protocol":{"protocol":"openai-completions","features":` + features + `}}}`
	src := fmt.Sprintf(`module.exports = function (config) {
		return {
			buildRequest: function (ctx, entry) {
				var key = util.key("api_key");
				return { url: (config.base_url || "https://d") + "/v1/chat/completions", method: "POST",
					headers: {"Authorization": "Bearer " + key}, body: entry, stream: false };
			},%s
		};
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`, impl)
	// 经 zip 装包走 Install 全流程
	data := buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: src})
	if err := pkgs.Install(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func mustSave(t *testing.T, reg *Registry, m *Model) {
	t.Helper()
	if err := reg.Save(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_NameAndPluginRequired(t *testing.T) {
	// Given 空 name / 空 plugin When Save Then 拒存
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if err := reg.Save(context.Background(), &Model{Plugin: "demo", Enabled: true}); err == nil {
		t.Fatal("empty name accepted")
	}
	if err := reg.Save(context.Background(), &Model{Name: "m", Enabled: true}); err == nil {
		t.Fatal("empty plugin accepted")
	}
}

func TestValidate_UnknownParamRejected(t *testing.T) {
	// Given params 含未声明键 When Save(保存期 strict)Then 拒存并注明键名
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	err := reg.Save(context.Background(), &Model{Name: "m", Plugin: "demo", Enabled: true,
		Params: map[string]any{"ghost": 1}})
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unknown param must be rejected: %v", err)
	}
}

func TestValidate_BaseWithoutProtocol(t *testing.T) {
	// Given 插件仅 filters 包(无 protocol 部件)When Save Then 拒建
	pkgs, reg := testEnv(t)
	manifest := `{"manifestVersion":1,"name":"onlyf","version":"1","parts":{"filters":[{"name":"f"}]}}`
	src := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{"filters/f.js": src})); err != nil {
		t.Fatal(err)
	}
	err := reg.Save(context.Background(), &Model{Name: "u2", Plugin: "onlyf", Enabled: true})
	if err == nil || !contains(err.Error(), "protocol") {
		t.Fatalf("plugin protocol validation: %v", err)
	}
}

func TestPick_ThreeWayClassification(t *testing.T) {
	// Given 一行声明模型 m When Pick Then 无模型→NoModel;能力不符→Capability;命中→候选
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true})
	_, pe, _ := reg.Pick("nope", nil, "openai-completions", false)
	if pe != PickNoModel {
		t.Fatalf("want NoModel, got %v", pe)
	}
	_, pe, _ = reg.Pick("m", []Feature{"vision"}, "openai-completions", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	cands, _, err := reg.Pick("m", []Feature{"tools"}, "openai-completions", false)
	if err != nil || len(cands) != 1 {
		t.Fatalf("hit: %v %d", err, len(cands))
	}
}

func TestPick_ProtocolMismatchIsCapability(t *testing.T) {
	// Given 行仅声明 openai-completions When anthropic 入口 Pick Then PickCapability
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true})
	_, pe, _ := reg.Pick("m", nil, "anthropic-messages", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
}

func TestPick_FormMismatchIsCapability(t *testing.T) {
	// Given 行声明不含 streaming When stream=true Pick Then PickCapability
	pkgs, reg := testEnv(t)
	installBuiltinLikePkgForm(t, pkgs, "demo", `["non_streaming"]`, `["tools"]`)
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true})
	_, pe, _ := reg.Pick("m", nil, "openai-completions", true)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if _, _, err := reg.Pick("m", nil, "openai-completions", false); err != nil {
		t.Fatalf("non-stream should hit: %v", err)
	}
}

func TestPick_DisabledRowParticipatesInClassification(t *testing.T) {
	// Given 唯一行 disabled When Pick Then 非 NoModel(404 不与真实声明矛盾),归 Capability 并注明 disabled
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: false})
	_, pe, err := reg.Pick("m", nil, "openai-completions", false)
	if pe != PickCapability {
		t.Fatalf("disabled row must classify as Capability, got %v (%v)", pe, err)
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("reason must note disabled: %v", err)
	}
}

func TestPick_SameNameAcrossProtocols(t *testing.T) {
	// Given 同名绑定 openai 与 anthropic 两个插件 When 各入口 Pick Then 各自命中(v2 复合唯一核心场景)
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "oa")
	anthManifest := `{"manifestVersion":1,"name":"an","version":"1","parts":{
		"protocol":{"protocol":"anthropic-messages","features":["tools"]}}}`
	anthSrc := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			return { url: config.base_url + "/v1/messages", method: "POST", headers: {}, body: entry, stream: false };
		}, mapResponse: function (ctx, b) { return b; } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	if err := pkgs.Install(context.Background(), buildZip(t, anthManifest, map[string]string{plugin.ProtocolEntry: anthSrc})); err != nil {
		t.Fatal(err)
	}
	mustSave(t, reg, &Model{Name: "auto", Plugin: "oa", Enabled: true})
	mustSave(t, reg, &Model{Name: "auto", Plugin: "an", Enabled: true})
	cands, _, err := reg.Pick("auto", nil, "openai-completions", false)
	if err != nil || len(cands) != 1 || cands[0].Model.Plugin != "oa" {
		t.Fatalf("openai entry: %v %+v", err, cands)
	}
	cands, _, err = reg.Pick("auto", nil, "anthropic-messages", false)
	if err != nil || len(cands) != 1 || cands[0].Model.Plugin != "an" {
		t.Fatalf("anthropic entry: %v %+v", err, cands)
	}
}

func TestPick_SameNameSamePluginDuplicateRejected(t *testing.T) {
	// Given 同 (name, plugin) 重复行 When Save Then 唯一键冲突拒绝
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true})
	if err := reg.Save(context.Background(), &Model{Name: "m", Plugin: "demo", Enabled: true}); err != nil {
		t.Fatalf("upsert same key must be allowed (update): %v", err)
	}
	if len(reg.List()) != 1 {
		t.Fatalf("upsert must not duplicate: %d", len(reg.List()))
	}
}

func TestPick_CapabilityErrorCarriesReasons(t *testing.T) {
	// Given 多行声明同一模型但各缺一样(槽/形态/feature)When Pick Then 错误文案逐行给出可自救原因
	pkgs, reg := testEnv(t)
	installBuiltinLikePkgForm(t, pkgs, "oa", `["streaming","non_streaming"]`, `["tools"]`)
	installBuiltinLikePkgForm(t, pkgs, "ns", `["non_streaming"]`, `["tools"]`)
	installBuiltinLikePkgForm(t, pkgs, "nv", `["streaming","non_streaming"]`, `["tools","vision"]`)
	for _, name := range []string{"oa", "ns", "nv"} {
		mustSave(t, reg, &Model{Name: "m", Plugin: name, Enabled: true})
	}
	// anthropic 入口:oa/ns 槽不符;nv 缺 streaming 形态下再缺 vision 无意义,单维断言
	_, pe, err := reg.Pick("m", nil, "anthropic-messages", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if !strings.Contains(err.Error(), "m@oa declares openai-completions") {
		t.Fatalf("slot reason missing: %v", err)
	}
	// openai 入口 stream + vision + thinking:ns 缺 streaming,nv 缺 thinking
	_, pe, err = reg.Pick("m", []Feature{"vision", "thinking"}, "openai-completions", true)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if !strings.Contains(err.Error(), "m@ns implements non_streaming only") {
		t.Fatalf("form reason missing: %v", err)
	}
	if !strings.Contains(err.Error(), "m@nv lacks thinking") {
		t.Fatalf("feature reason missing: %v", err)
	}
	if !strings.Contains(err.Error(), "m@oa lacks vision, thinking") {
		t.Fatalf("multi-feature reason missing: %v", err)
	}
}

func TestResolve_FilterOrderAndConfig(t *testing.T) {
	// Given 包含一 filter When Resolve Then filter 链就绪且协议闭包取包参数+包密钥
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "base")
	fManifest := `{"manifestVersion":1,"name":"onlyf","version":"1","parts":{"filters":[{"name":"f1"}]}}`
	fsrc := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	if err := pkgs.Install(context.Background(), buildZip(t, fManifest, map[string]string{"filters/f1.js": fsrc})); err != nil {
		t.Fatal(err)
	}
	// filter 挂进 base 包:base 升级带 filter
	manifest := `{"manifestVersion":1,"name":"base","version":"1.1.0","parts":{
		"protocol":{"protocol":"openai-completions","features":["tools"]},
		"filters":[{"name":"f1"}]}}`
	psrc := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			return { url: config.base_url + "/v1/chat/completions", method: "POST", headers: {}, body: entry, stream: false };
		}, mapResponse: function (ctx, b) { return b; } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{
		plugin.ProtocolEntry: psrc, "filters/f1.js": fsrc})); err != nil {
		t.Fatal(err)
	}
	mustSave(t, reg, &Model{Name: "m", Plugin: "base", Enabled: true,
		Params: map[string]any{"base_url": "https://d"}})
	m, _ := reg.Get(1)
	res, release, err := reg.Resolve(m)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(res.Filters) != 1 || res.Filters[0].Name() != "base/f1" {
		t.Fatalf("filters: %+v", res.Filters)
	}
	if res.Protocol.Name() != "base" {
		t.Fatalf("protocol: %s", res.Protocol.Name())
	}
	req, err := res.Protocol.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://d/v1/chat/completions" {
		t.Fatalf("config closure: %s", req.URL)
	}
}

func TestSettings_InvalidateModelCaches(t *testing.T) {
	// Given 行已实例化(闭包持有插件参数)When 改包参数并 EvictPackageSettings Then 新实例读到新值
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if _, err := pkgs.Settings().Put("demo", plugin.PutInput{Config: map[string]any{"base_url": "https://old"}}); err != nil {
		t.Fatal(err)
	}
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true})
	m, _ := reg.Get(1)
	res, release, err := reg.Resolve(m)
	if err != nil {
		t.Fatal(err)
	}
	req, err := res.Protocol.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://old/v1/chat/completions" {
		t.Fatalf("initial closure: %s", req.URL)
	}
	release()
	// 改包参数 → 失效 → 重新 Resolve 读新值
	if _, err := pkgs.Settings().Put("demo", plugin.PutInput{Config: map[string]any{"base_url": "https://new"}}); err != nil {
		t.Fatal(err)
	}
	reg.EvictPackageSettings("demo")
	res2, release2, err := reg.Resolve(m)
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	req2, err := res2.Protocol.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req2.URL != "https://new/v1/chat/completions" {
		t.Fatalf("cache not invalidated: %s", req2.URL)
	}
}

func TestSave_PersistAndReload(t *testing.T) {
	// Given 保存行 When 同一 DB 重建 Registry 并 LoadFromDB Then 行恢复(params/enabled 一致)
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustSave(t, reg, &Model{Name: "m", Plugin: "demo", Enabled: true, Params: map[string]any{"base_url": "https://x"}})
	// 同库重建:plugin registry 复用(包声明仍在),upstream registry 全新实例走启动恢复链路
	reg2 := NewRegistry(reg.db, pkgs)
	if err := reg2.LoadFromDB(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := reg2.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "m" || got.Plugin != "demo" || !got.Enabled || got.Params["base_url"] != "https://x" {
		t.Fatalf("roundtrip: %+v", got)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func TestMigrateV1_SecretsFromKv(t *testing.T) {
	// Given v1 遗表(内嵌 secrets 为 null,凭据在 kv upstream:<名>)When 迁移 Then 密钥并入包级 keys 且 v1 表/kv 清理
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	v1 := []string{
		`CREATE TABLE upstreams_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filters_enabled_json TEXT NOT NULL DEFAULT '{}', strategy_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT)`,
		`INSERT INTO upstreams_v1 (name, base_package, models_json, targets_json) VALUES ('inst', 'demo',
			'["m1"]', '[{"name":"t1","base_url":"https://old","secrets":null,"enabled":true}]')`,
		`INSERT INTO kv (ns, key, value) VALUES ('upstream:inst', 'secrets:inst', '{"api_key":"sk-v1"}')`,
	}
	for _, stmt := range v1 {
		if _, err := reg.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.migrateV1(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := reg.db.QueryRow(`SELECT COUNT(*) FROM upstreams WHERE name='m1' AND base_package='demo'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("model row migrated: n=%d err=%v", n, err)
	}
	val, ok := pkgs.Keys().Get("demo", "api_key")
	if !ok || val != "sk-v1" {
		t.Fatalf("package key from v1 kv: %v %v", val, ok)
	}
	if err := reg.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='upstreams_v1'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("v1 table dropped: %d", n)
	}
	if err := reg.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE ns LIKE 'upstream:%'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("v1 kv cleaned: %d", n)
	}
}

func TestMigrateV1_TasksOverridePreserved(t *testing.T) {
	// Given v1 库已有包级 tasks 覆盖(hooks 时代遗留)When 迁移写包参数 Then tasks 覆盖不被 Put 全量覆盖清掉
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if _, err := pkgs.Settings().Put("demo", plugin.PutInput{
		Tasks: map[string]map[string]any{"refresh": {"cron": "0 */6 * * *"}},
	}); err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`CREATE TABLE upstreams_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filters_enabled_json TEXT NOT NULL DEFAULT '{}', strategy_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT)`,
		`INSERT INTO upstreams_v1 (name, base_package, models_json, targets_json, params_json) VALUES ('i1', 'demo',
			'["m1"]', '[]', '{"base_url":"https://keep"}')`,
	}
	for _, stmt := range stmts {
		if _, err := reg.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.migrateV1(context.Background()); err != nil {
		t.Fatal(err)
	}
	view := pkgs.Settings().View("demo")
	if cfg, _ := view.Overrides["config"].(map[string]any); cfg["base_url"] != "https://keep" {
		t.Fatalf("config migrated: %v", view.Overrides)
	}
	tasks, ok := view.Overrides["tasks"].(map[string]any)
	if !ok {
		t.Fatalf("tasks override lost: %v", view.Overrides)
	}
	doc, _ := tasks["refresh"].(map[string]any)
	if doc == nil || doc["cron"] != "0 */6 * * *" {
		t.Fatalf("tasks content: %v", tasks)
	}
}

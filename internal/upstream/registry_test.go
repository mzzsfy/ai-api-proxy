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

// v3Ddl 模型组双表(测试库;与 004 迁移后形态一致)
func v3Ddl() []string {
	return []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}', source TEXT NOT NULL DEFAULT 'local')`,
		`CREATE TABLE model_groups (id INTEGER PRIMARY KEY AUTOINCREMENT, prefix TEXT NOT NULL UNIQUE,
			base_package TEXT NOT NULL, params_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')))`,
		`CREATE TABLE model_members (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL REFERENCES model_groups(id) ON DELETE CASCADE,
			name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (group_id, name))`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
		`CREATE TABLE package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', prev_json TEXT, updated_at INTEGER NOT NULL DEFAULT 0)`,
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
	for _, ddl := range v3Ddl() {
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
				var key = (util.key() || {"api_key":"sk-x"}).api_key;
				return { url: (config.base_url || "https://d") + "/v1/chat/completions", method: "POST",
					headers: {"Authorization": "Bearer " + key}, body: entry, stream: false };
			},%s
		};
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`, impl)
	// 经 zip 装包走 Install 全流程
	data := buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: src})
	if err := pkgs.Install(context.Background(), data, ""); err != nil {
		t.Fatal(err)
	}
}

// mustGroup 建组(前缀默认=包名)
func mustGroup(t *testing.T, reg *Registry, prefix, pluginName string, params map[string]any) *Group {
	t.Helper()
	g := &Group{Prefix: prefix, Plugin: pluginName, Enabled: true, Params: params}
	if err := reg.SaveGroup(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	return g
}

// mustMember 建成员
func mustMember(t *testing.T, reg *Registry, groupID int64, name string, enabled bool) *Member {
	t.Helper()
	m := &Member{GroupID: groupID, Name: name, Enabled: enabled}
	if err := reg.SaveMember(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestValidateGroup_PrefixAndPluginRequired(t *testing.T) {
	// Given 空 prefix / 空 plugin / 前缀含斜杠 When SaveGroup Then 拒存
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if err := reg.SaveGroup(context.Background(), &Group{Plugin: "demo", Enabled: true}); err == nil {
		t.Fatal("empty prefix accepted")
	}
	if err := reg.SaveGroup(context.Background(), &Group{Prefix: "a/b", Plugin: "demo", Enabled: true}); err == nil {
		t.Fatal("prefix with slash accepted")
	}
	if err := reg.SaveGroup(context.Background(), &Group{Prefix: "d", Enabled: true}); err == nil {
		t.Fatal("empty plugin accepted")
	}
}

func TestValidateGroup_UnknownParamRejected(t *testing.T) {
	// Given params 含未声明键 When SaveGroup(保存期 strict)Then 拒存并注明键名
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	err := reg.SaveGroup(context.Background(), &Group{Prefix: "d", Plugin: "demo", Enabled: true,
		Params: map[string]any{"ghost": 1}})
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unknown param must be rejected: %v", err)
	}
}

func TestValidateGroup_PrefixConflict(t *testing.T) {
	// Given 前缀已被他组占用 When SaveGroup(插入)Then 拒绝;不同前缀同包允许并存
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustGroup(t, reg, "demo", "demo", nil)
	err := reg.SaveGroup(context.Background(), &Group{Prefix: "demo", Plugin: "demo", Enabled: true})
	if err == nil || !strings.Contains(err.Error(), "already used by group") {
		t.Fatalf("prefix conflict must be rejected: %v", err)
	}
	mustGroup(t, reg, "demo2", "demo", map[string]any{"base_url": "https://x"})
}

func TestValidateMember_GroupMissingAndNameConflict(t *testing.T) {
	// Given 组不存在 / 组内同名成员已存在 When SaveMember Then 拒绝
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if err := reg.SaveMember(context.Background(), &Member{GroupID: 999, Name: "m", Enabled: true}); err == nil {
		t.Fatal("member without group accepted")
	}
	g := mustGroup(t, reg, "demo", "demo", nil)
	mustMember(t, reg, g.ID, "m", true)
	err := reg.SaveMember(context.Background(), &Member{GroupID: g.ID, Name: "m", Enabled: true})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate member must be rejected: %v", err)
	}
}

func TestPick_FullPathExact_R1(t *testing.T) {
	// R1 全路径精确:prefix/member 命中该组成员
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "opencode", "demo", nil)
	mustMember(t, reg, g.ID, "gpt-4o", true)
	cands, _, err := reg.Pick("opencode/gpt-4o", nil, "openai-completions", false)
	if err != nil || len(cands) != 1 {
		t.Fatalf("hit: %v %d", err, len(cands))
	}
	if cands[0].Group.Prefix != "opencode" || cands[0].Member.Name != "gpt-4o" {
		t.Fatalf("candidate: %+v", cands[0])
	}
}

func TestPick_ShortNameUnique_R2(t *testing.T) {
	// R2 短名唯一:仅一组有该成员 → 直达
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "opencode", "demo", nil)
	mustMember(t, reg, g.ID, "gpt-4o", true)
	cands, _, err := reg.Pick("gpt-4o", nil, "openai-completions", false)
	if err != nil || len(cands) != 1 || cands[0].Group.Prefix != "opencode" {
		t.Fatalf("hit: %v %+v", err, cands)
	}
}

func TestPick_ShortNamePollsAcrossGroups_R3(t *testing.T) {
	// R3 短名歧义 → 组间轮询:连续命中在两组间交替
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	installBuiltinLikePkg(t, pkgs, "demo2")
	ga := mustGroup(t, reg, "a", "demo", nil)
	gb := mustGroup(t, reg, "b", "demo2", nil)
	mustMember(t, reg, ga.ID, "m", true)
	mustMember(t, reg, gb.ID, "m", true)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		cands, _, err := reg.Pick("m", nil, "openai-completions", false)
		if err != nil || len(cands) != 1 {
			t.Fatalf("pick %d: %v %+v", i, err, cands)
		}
		seen[cands[0].Group.Prefix]++
	}
	if seen["a"] != 2 || seen["b"] != 2 {
		t.Fatalf("round robin must alternate: %v", seen)
	}
}

func TestPick_LongestPrefixWins_R4(t *testing.T) {
	// R4 最长前缀:组 "or" 与 "openrouter" 并存,openrouter/m 落 openrouter 组
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	installBuiltinLikePkg(t, pkgs, "demo2")
	gor := mustGroup(t, reg, "or", "demo", nil)
	gfull := mustGroup(t, reg, "openrouter", "demo2", nil)
	mustMember(t, reg, gor.ID, "m", true)
	mustMember(t, reg, gfull.ID, "m", true)
	cands, _, err := reg.Pick("openrouter/m", nil, "openai-completions", false)
	if err != nil || len(cands) != 1 || cands[0].Group.Prefix != "openrouter" {
		t.Fatalf("longest prefix: %v %+v", err, cands)
	}
}

func TestPick_MemberNameWithSlash_R5(t *testing.T) {
	// R5 成员名含斜杠:opencode/openai/gpt-4o → 组 opencode 成员 openai/gpt-4o(禁止按 / 切分)
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "opencode", "demo", nil)
	mustMember(t, reg, g.ID, "openai/gpt-4o", true)
	cands, _, err := reg.Pick("opencode/openai/gpt-4o", nil, "openai-completions", false)
	if err != nil || len(cands) != 1 || cands[0].Member.Name != "openai/gpt-4o" {
		t.Fatalf("slashed member: %v %+v", err, cands)
	}
}

func TestPick_ZeroHitIsNoModel_R6(t *testing.T) {
	// R6 零命中 → NoModel;全路径点名不存在的成员同样 NoModel
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "opencode", "demo", nil)
	mustMember(t, reg, g.ID, "m", true)
	if pe, _ := func() (PickError, error) {
		_, pe, err := reg.Pick("nope", nil, "openai-completions", false)
		return pe, err
	}(); pe != PickNoModel {
		t.Fatalf("unknown short: %v", pe)
	}
	if _, pe, _ := reg.Pick("opencode/ghost", nil, "openai-completions", false); pe != PickNoModel {
		t.Fatalf("unknown member under prefix: %v", pe)
	}
}

func TestPick_DisabledFiltered_R7(t *testing.T) {
	// R7 禁用组/成员不参与解析;唯一声明被禁 → Capability 注明 disabled
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	gd := mustGroup(t, reg, "d", "demo", nil)
	mustMember(t, reg, gd.ID, "m", true)
	// 组禁用 → 短名零命中
	if err := reg.SaveGroup(context.Background(), &Group{ID: gd.ID, Prefix: "d", Plugin: "demo", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, pe, _ := reg.Pick("d/m", nil, "openai-completions", false); pe != PickNoModel {
		t.Fatalf("disabled group must be invisible: %v", pe)
	}
	if _, pe, _ := reg.Pick("m", nil, "openai-completions", false); pe != PickNoModel {
		t.Fatalf("disabled group member must be invisible: %v", pe)
	}
	// 重新启用组后:成员禁用(组启用)→ 声明存在但不可服务 → Capability
	if err := reg.SaveGroup(context.Background(), &Group{ID: gd.ID, Prefix: "d", Plugin: "demo", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SaveMember(context.Background(), &Member{ID: mustMember(t, reg, gd.ID, "m2", true).ID, GroupID: gd.ID, Name: "m2", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, pe, err := reg.Pick("d/m2", nil, "openai-completions", false); pe != PickCapability {
		t.Fatalf("disabled member must classify Capability, got %v (%v)", pe, err)
	}
}

func TestPick_CapabilityReasonsCarryGroupTag(t *testing.T) {
	// Given 候选协议不符 When Pick Then 错误文案含 member@prefix 定位
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "oc", "demo", nil)
	mustMember(t, reg, g.ID, "m", true)
	_, pe, err := reg.Pick("m", nil, "anthropic-messages", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if !strings.Contains(err.Error(), "m@oc declares openai-completions") {
		t.Fatalf("tagged reason missing: %v", err)
	}
}

func TestPick_FormMismatchIsCapability(t *testing.T) {
	// Given 组实现不含 streaming When stream=true Pick Then PickCapability;非流式命中
	pkgs, reg := testEnv(t)
	installBuiltinLikePkgForm(t, pkgs, "demo", `["non_streaming"]`, `["tools"]`)
	g := mustGroup(t, reg, "d", "demo", nil)
	mustMember(t, reg, g.ID, "m", true)
	if _, pe, _ := reg.Pick("m", nil, "openai-completions", true); pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if _, _, err := reg.Pick("m", nil, "openai-completions", false); err != nil {
		t.Fatalf("non-stream should hit: %v", err)
	}
}

func TestResolve_FilterOrderAndConfig(t *testing.T) {
	// Given 组含一 filter When Resolve Then filter 链就绪且协议闭包取组参数+包密钥
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "base")
	fManifest := `{"manifestVersion":1,"name":"onlyf","version":"1","parts":{"filters":[{"name":"f1"}]}}`
	fsrc := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	if err := pkgs.Install(context.Background(), buildZip(t, fManifest, map[string]string{"filters/f1.js": fsrc}), ""); err != nil {
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
		plugin.ProtocolEntry: psrc, "filters/f1.js": fsrc}), ""); err != nil {
		t.Fatal(err)
	}
	g := mustGroup(t, reg, "base", "base", map[string]any{"base_url": "https://d"})
	res, release, err := reg.Resolve(g)
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

func TestSettings_InvalidateGroupCaches(t *testing.T) {
	// Given 组已实例化(闭包持有插件参数)When 改包参数并 EvictPackageSettings Then 新实例读到新值
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	if _, err := pkgs.Keys().Set("demo", "api_key", map[string]any{"api_key": "sk-t"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pkgs.Settings().Put("demo", plugin.PutInput{Config: map[string]any{"base_url": "https://old"}}); err != nil {
		t.Fatal(err)
	}
	g := mustGroup(t, reg, "demo", "demo", nil)
	res, release, err := reg.Resolve(g)
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
	res2, release2, err := reg.Resolve(g)
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
	// Given 保存组+成员 When 同一 DB 重建 Registry 并 LoadFromDB Then 组与成员恢复(params/enabled 一致)
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": "https://x"})
	g2 := mustGroup(t, reg, "demo2", "demo", nil)
	mustMember(t, reg, g2.ID, "m", true)
	// 同库重建:plugin registry 复用(包声明仍在),upstream registry 全新实例走启动恢复链路
	reg2 := NewRegistry(reg.db, pkgs)
	if err := reg2.LoadFromDB(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := reg2.GetGroup(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Prefix != "demo" || got.Plugin != "demo" || !got.Enabled || got.Params["base_url"] != "https://x" {
		t.Fatalf("group roundtrip: %+v", got)
	}
	mems := reg2.ListMembers(g2.ID)
	if len(mems) != 1 || mems[0].Name != "m" || !mems[0].Enabled {
		t.Fatalf("member roundtrip: %+v", mems)
	}
}

func TestDeleteGroup_CascadesMembers(t *testing.T) {
	// Given 组含成员 When DeleteGroup Then 组与成员同灭
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	g := mustGroup(t, reg, "demo", "demo", nil)
	m := mustMember(t, reg, g.ID, "m", true)
	if err := reg.DeleteGroup(context.Background(), g.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.GetGroup(g.ID); err == nil {
		t.Fatal("group survived")
	}
	if _, err := reg.GetMember(m.ID); err == nil {
		t.Fatal("member survived group delete")
	}
	if reg.PollGroupCount("m") != 0 {
		t.Fatal("poll index not cleaned")
	}
}

func TestPollGroupCount(t *testing.T) {
	// Given 短名存在于两组(一组禁用)When PollGroupCount Then 只计启用组
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	installBuiltinLikePkg(t, pkgs, "demo2")
	ga := mustGroup(t, reg, "a", "demo", nil)
	gb := mustGroup(t, reg, "b", "demo2", nil)
	mustMember(t, reg, ga.ID, "m", true)
	mustMember(t, reg, gb.ID, "m", true)
	if n := reg.PollGroupCount("m"); n != 2 {
		t.Fatalf("poll count: %d", n)
	}
	if err := reg.SaveGroup(context.Background(), &Group{ID: gb.ID, Prefix: "b", Plugin: "demo2", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if n := reg.PollGroupCount("m"); n != 1 {
		t.Fatalf("poll count after disable: %d", n)
	}
}

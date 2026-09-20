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

func testEnv(t *testing.T) (*plugin.Registry, *Registry) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/t.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')))`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filter_params_json TEXT NOT NULL DEFAULT '{}',
			filters_enabled_json TEXT NOT NULL DEFAULT '{}', strategy_json TEXT NOT NULL DEFAULT '{}',
			enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')))`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	pkgs := plugin.NewRegistry(db)
	mem := &memSecrets{m: map[string]map[string]string{}}
	reg := NewRegistry(db, pkgs, mem)
	return pkgs, reg
}

type memSecrets struct {
	m map[string]map[string]string
}

func (s *memSecrets) UpsertTargetSecrets(upstream, target string, secrets map[string]string) error {
	key := upstream + "/" + target
	if s.m[key] == nil {
		s.m[key] = map[string]string{}
	}
	for k, v := range secrets {
		s.m[key][k] = v
	}
	return nil
}
func (s *memSecrets) GetTargetSecrets(upstream, target string) (map[string]string, bool) {
	v, ok := s.m[upstream+"/"+target]
	return v, ok
}
func (s *memSecrets) DeleteTargetSecrets(upstream, target string) {
	delete(s.m, upstream+"/"+target)
}

func installBuiltinLikePkg(t *testing.T, pkgs *plugin.Registry, name string) {
	t.Helper()
	installBuiltinLikePkgForm(t, pkgs, name, `["streaming","non_streaming"]`, `["tools"]`)
}

// installBuiltinLikePkgForm 按给定 form/features 声明装包(形态把关用例)
// features 逐项生成实现,保证声明↔实现对称
func installBuiltinLikePkgForm(t *testing.T, pkgs *plugin.Registry, name, form, features string) {
	t.Helper()
	var impl string
	if strings.Contains(features, `"streaming"`) || strings.Contains(form, `"streaming"`) {
		impl += `mapEvent: function (ctx, e) { return e; },`
	}
	if strings.Contains(form, `"non_streaming"`) {
		impl += `mapResponse: function (ctx, b) { return b; },`
	}
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"1.0.0","parts":{
		"protocol":{"entry":"p.js","protocol":"openai-completions","form":` + form + `,"features":` + features + `,"secretRefs":["api_key"]}}}`
	src := fmt.Sprintf(`module.exports = function (config) {
		return {
			buildRequest: function (ctx, entry) {
				var key = util.secret("api_key");
				return { url: (ctx.target.baseUrl || "https://d") + "/v1/chat/completions", method: "POST",
					headers: {"Authorization": "Bearer " + key}, body: entry, stream: false };
			},%s
		};
	};`, impl)
	// 经 zip 装包走 Install 全流程
	data := buildZip(t, manifest, map[string]string{"p.js": src})
	if err := pkgs.Install(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_MissingSecretsKey(t *testing.T) {
	// Given 目标 secrets 缺 api_key When Save Then 拒存
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	u := &Upstream{Name: "u1", Base: PackageRef{Package: "demo"}, Models: []string{"m"}, Targets: []Target{
		{Name: "t1", BaseURL: "https://x", Enabled: true, Secrets: map[string]string{}}}}
	if err := reg.Save(context.Background(), u); err == nil || !contains(err.Error(), "api_key") {
		t.Fatalf("secrets validation: %v", err)
	}
}

func TestValidate_BaseWithoutProtocol(t *testing.T) {
	// Given base 仅 filters 包(无 protocol 部件)When Save Then 拒建
	pkgs, reg := testEnv(t)
	manifest := `{"manifestVersion":1,"name":"onlyf","version":"1","parts":{"filters":[{"name":"f","entry":"a.js"}]}}`
	src := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{"a.js": src})); err != nil {
		t.Fatal(err)
	}
	u := &Upstream{Name: "u2", Base: PackageRef{Package: "onlyf"}, Models: []string{"m"}, Targets: []Target{
		{Name: "t", Enabled: true, Secrets: map[string]string{}}}}
	err := reg.Save(context.Background(), u)
	if err == nil || !contains(err.Error(), "protocol") {
		t.Fatalf("base protocol validation: %v", err)
	}
}

func TestValidate_ModelsAndTargetsRequired(t *testing.T) {
	// Given 空 Models 或空 targets When Save Then 拒存
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	u := &Upstream{Name: "u3", Base: PackageRef{Package: "demo"}, Models: nil, Targets: []Target{
		{Name: "t", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	if err := reg.Save(context.Background(), u); err == nil {
		t.Fatal("empty models accepted")
	}
	u.Models = []string{"m"}
	u.Targets = nil
	if err := reg.Save(context.Background(), u); err == nil {
		t.Fatal("empty targets accepted")
	}
}

func TestPick_ThreeWayClassification(t *testing.T) {
	// Given 一个含模型 m 的上游 When Pick Then 无模型→NoModel;能力不符→Capability;命中→候选
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "demo"}, Models: []string{"m"},
		Targets: []Target{{Name: "t", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
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
	// Given 上游仅声明 openai-completions When anthropic 入口 Pick Then PickCapability
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "demo"}, Models: []string{"m"},
		Targets: []Target{{Name: "t", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	_, pe, _ := reg.Pick("m", nil, "anthropic-messages", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
}

func TestPick_FormMismatchIsCapability(t *testing.T) {
	// Given 上游声明不含 streaming When stream=true Pick Then PickCapability
	pkgs, reg := testEnv(t)
	installBuiltinLikePkgForm(t, pkgs, "demo", `["non_streaming"]`, `["tools"]`)
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "demo"}, Models: []string{"m"},
		Targets: []Target{{Name: "t", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	_, pe, _ := reg.Pick("m", nil, "openai-completions", true)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if _, _, err := reg.Pick("m", nil, "openai-completions", false); err != nil {
		t.Fatalf("non-stream should hit: %v", err)
	}
}

func TestPick_CapabilityErrorCarriesReasons(t *testing.T) {
	// Given 多上游声明同一模型但各缺一样(槽/形态/feature) When Pick Then 错误文案逐上游给出可自救原因
	pkgs, reg := testEnv(t)
	installBuiltinLikePkgForm(t, pkgs, "oa", `["streaming","non_streaming"]`, `["tools"]`)
	installBuiltinLikePkgForm(t, pkgs, "ns", `["non_streaming"]`, `["tools"]`)
	installBuiltinLikePkgForm(t, pkgs, "nv", `["streaming","non_streaming"]`, `["tools","vision"]`)
	for _, name := range []string{"oa", "ns", "nv"} {
		u := &Upstream{Name: name, Enabled: true, Base: PackageRef{Package: name}, Models: []string{"m"},
			Targets: []Target{{Name: "t", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
		if err := reg.Save(context.Background(), u); err != nil {
			t.Fatal(err)
		}
	}
	// anthropic 入口:oa/ns 槽不符;nv 缺 streaming 形态下再缺 vision 无意义,单维断言
	_, pe, err := reg.Pick("m", nil, "anthropic-messages", false)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if !strings.Contains(err.Error(), "oa declares openai-completions") {
		t.Fatalf("slot reason missing: %v", err)
	}
	// openai 入口 stream + vision + thinking:ns 缺 streaming,nv 缺 thinking
	_, pe, err = reg.Pick("m", []Feature{"vision", "thinking"}, "openai-completions", true)
	if pe != PickCapability {
		t.Fatalf("want Capability, got %v", pe)
	}
	if !strings.Contains(err.Error(), "ns declares non_streaming only") {
		t.Fatalf("form reason missing: %v", err)
	}
	if !strings.Contains(err.Error(), "nv lacks thinking") {
		t.Fatalf("feature reason missing: %v", err)
	}
	if !strings.Contains(err.Error(), "oa lacks vision, thinking") {
		t.Fatalf("multi-feature reason missing: %v", err)
	}
}

func TestPick_UnhealthyWhenNoEnabledTarget(t *testing.T) {
	// Given 全部目标禁用 When Pick Then PickUnhealthy
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "demo"}, Models: []string{"m"},
		Targets: []Target{{Name: "t", Enabled: false, Secrets: map[string]string{"api_key": "k"}}}}
	if err := reg.Save(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	_, pe, _ := reg.Pick("m", nil, "openai-completions", false)
	if pe != PickUnhealthy {
		t.Fatalf("want Unhealthy, got %v", pe)
	}
}

func TestResolve_FilterOrderAndSecretsRef(t *testing.T) {
	// Given extras+base 各一 filter When Resolve Then extras 先于 base;SecretsRef=upstream/target
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "base")
	fManifest := `{"manifestVersion":1,"name":"fx","version":"1","parts":{"filters":[{"name":"f1","entry":"f.js"}]}}`
	fsrc := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	_ = pkgs.Install(context.Background(), buildZip(t, fManifest, map[string]string{"f.js": fsrc}))
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "base"}, Extras: []PackageRef{{Package: "fx"}},
		Models: []string{"m"}, FilterParams: map[string]map[string]any{},
		Targets: []Target{{Name: "t1", BaseURL: "https://x", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	_ = reg.Save(context.Background(), u)
	res, release, err := reg.Resolve(u)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(res.Filters) != 1 || res.Filters[0].Name() != "fx/f1" {
		t.Fatalf("filters: %+v", res.Filters)
	}
	if len(res.Targets) != 1 || res.Targets[0].SecretsRef != "u/t1" {
		t.Fatalf("targets: %+v", res.Targets)
	}
	if res.Protocol.Name() != "base" {
		t.Fatalf("protocol: %s", res.Protocol.Name())
	}
}

func TestResolve_FiltersEnabledToggle(t *testing.T) {
	// Given filter 启停=false When Resolve Then 该 filter 被过滤
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "base")
	fManifest := `{"manifestVersion":1,"name":"fx","version":"1","parts":{"filters":[{"name":"f1","entry":"f.js"}]}}`
	fsrc := `module.exports = { mapRequest: function (ctx, p) { return p; } };`
	_ = pkgs.Install(context.Background(), buildZip(t, fManifest, map[string]string{"f.js": fsrc}))
	u := &Upstream{Name: "u", Enabled: true, Base: PackageRef{Package: "base"}, Extras: []PackageRef{{Package: "fx"}},
		Models: []string{"m"}, FiltersEnabled: map[string]bool{"fx/f1": false},
		Targets: []Target{{Name: "t1", Enabled: true, Secrets: map[string]string{"api_key": "k"}}}}
	_ = reg.Save(context.Background(), u)
	res, release, err := reg.Resolve(u)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(res.Filters) != 0 {
		t.Fatalf("disabled filter still resolved: %+v", res.Filters)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

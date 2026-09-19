package plugin

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"

	_ "modernc.org/sqlite"
)

// buildAAP 构造内存 .aap zip
func buildAAP(t *testing.T, manifest string, files map[string]string) []byte {
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
	for name, src := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(src)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// testDB 内存库(含迁移)
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS packages(
		name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL,
		revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1,
		created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')))`); err != nil {
		t.Fatal(err)
	}
	return db
}

const goodManifest = `{"manifestVersion":1,"name":"demo","version":"1.0.0","parts":{
	"protocol":{"entry":"protocol.js","form":["streaming","non_streaming"],"features":["tools"],"secretRefs":["api_key"]},
	"filters":[{"name":"identity","entry":"filters/identity.js","configSchema":{"type":"object"}}]}}`

const protoSrc = `module.exports = {
	buildRequest: function (ctx, pivot) {
		return { url: ctx.target.baseUrl + "/v1/chat/completions", method: "POST",
			headers: { "Content-Type": "application/json", "Authorization": "Bearer " + util.secret("api_key") },
			body: pivot, stream: ctx.vars.entryStream };
	},
	mapEvent: function (ctx, e) { return e; },
	mapResponse: function (ctx, b) { return b; }
};`

const filterSrc = `module.exports = function (config) {
	return {
		mapRequest: function (ctx, pivot) { return pivot; },
		mapChunk: function (ctx, c) { return c; },
		mapResponse: function (ctx, r) { return r; }
	};
};`

func mustInstall(t *testing.T, r *Registry) *Package {
	t.Helper()
	if err := r.Install(context.Background(), buildAAP(t, goodManifest, map[string]string{
		"protocol.js":         protoSrc,
		"filters/identity.js": filterSrc,
	})); err != nil {
		t.Fatal(err)
	}
	pkg, err := r.GetPackage("demo")
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

// newProtoCtx 构造带 target 的管道上下文
func newProtoCtx() *pipeline.PipelineContext {
	c := pipeline.NewContext("r1", pipeline.UpstreamInfo{Name: "u"}, pipeline.Vars{Model: "m", EntryStream: true})
	c.Target = pipeline.Target{Name: "t1", BaseURL: "https://up.test", SecretsRef: "u/t1"}
	return c
}

func TestParseAndInstantiate_FullFlow(t *testing.T) {
	// Given 合法包 When Parse+NewProtocol Then hook 可执行且 util.secret 注入 Bearer
	r := NewRegistry(testDB(t))
	pkg := mustInstall(t, r)
	proto, err := NewProtocol(pkg, nil, func(target, key string) (string, bool) {
		if key == "api_key" {
			return "sk-live", true
		}
		return "", false
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.BuildRequest(newProtoCtx(), []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Headers["Authorization"] != "Bearer sk-live" {
		t.Fatalf("auth: %s", req.Headers["Authorization"])
	}
}

func TestValidate_MissingFormRejected(t *testing.T) {
	// Given protocol 无 form When Install Then 拒绝
	m := `{"manifestVersion":1,"name":"nfm","version":"1","parts":{
		"protocol":{"entry":"p.js","features":["tools"]}}}`
	r := NewRegistry(testDB(t))
	if err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"p.js": protoSrc})); err == nil {
		t.Fatal("missing form accepted")
	}
}

func TestValidate_MissingImplementationRejected(t *testing.T) {
	// Given 声明 streaming 但缺 mapEvent When Install Then 安装期拒绝(双向绑定校验)
	m := `{"manifestVersion":1,"name":"noimpl","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["streaming"]}}}`
	src := `module.exports = { buildRequest: function(){return {url:"u"}} };`
	r := NewRegistry(testDB(t))
	err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"p.js": src}))
	if err == nil || !strings.Contains(err.Error(), "mapEvent missing") {
		t.Fatalf("install must reject missing impl: %v", err)
	}
}

func TestValidate_ExtraImplRejected(t *testing.T) {
	// Given 实现 mapResponse 但未声明 non_streaming(多实现)When Install Then 拒绝
	m := `{"manifestVersion":1,"name":"extraimpl","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["streaming"]}}}`
	src := `module.exports = { buildRequest: function(){return {url:"u"}},
		mapEvent: function(ctx,e){ return e; },
		mapResponse: function(ctx,b){ return b; } };`
	r := NewRegistry(testDB(t))
	err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"p.js": src}))
	if err == nil || !strings.Contains(err.Error(), "non_streaming not declared") {
		t.Fatalf("install must reject extra impl: %v", err)
	}
}

func TestValidate_SyntaxErrorLineReported(t *testing.T) {
	// Given 语法错误部件 When Install Then 错误带行号
	m := `{"manifestVersion":1,"name":"badline","version":"1","parts":{
		"filters":[{"name":"f","entry":"f.js"}]}}`
	src := "module.exports = function (config) {\n  return { mapRequest: function (ctx, p) {\n    return p  // 缺分号致下一行语法错\n  } };\n};\n)))"
	r := NewRegistry(testDB(t))
	err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"f.js": src}))
	if err == nil || !strings.Contains(err.Error(), "Line 6") {
		t.Fatalf("syntax error must report line: %v", err)
	}
}

func TestDrift_RequiredMissingRejectedAtInstantiate(t *testing.T) {
	// Given configSchema required=model 且实例缺该键 When NewFilter Then 错误注明待补字段
	m := `{"manifestVersion":1,"name":"drift","version":"1","parts":{
		"filters":[{"name":"rw","entry":"f.js","configSchema":{"type":"object","required":["model"],"properties":{"model":{"type":"string"}}}}]}}`
	src := `module.exports = function (config) {
		return { mapRequest: function (ctx, p) { return p; } };
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"f.js": src}))
	if err != nil {
		t.Fatal(err)
	}
	// 实例缺 required(安装期不阻塞——validateParts 不查 schema;实例化期拒绝)
	_, err = NewFilter(pkg, pkg.Manifest.Parts.Filters[0], nil, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "model") || !strings.Contains(err.Error(), "required") {
		t.Fatalf("required missing must name field: %v", err)
	}
	// 补齐后成功
	if _, err = NewFilter(pkg, pkg.Manifest.Parts.Filters[0], map[string]any{"model": "m1"}, nil, nil, nil); err != nil {
		t.Fatalf("valid config: %v", err)
	}
}

func TestDrift_UnknownKeysStripped(t *testing.T) {
	// Given 实例参数含 schema 外未知键 When NewFilter Then 注入 config 不含未知键
	m := `{"manifestVersion":1,"name":"strip","version":"1","parts":{
		"filters":[{"name":"rw","entry":"f.js","configSchema":{"type":"object","properties":{"keep":{"type":"string"}}}}]}}`
	src := `module.exports = function (config) {
		return { mapRequest: function (ctx, p) { return JSON.stringify(config); } };
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"f.js": src}))
	if err != nil {
		t.Fatal(err)
	}
	f, err := NewFilter(pkg, pkg.Manifest.Parts.Filters[0],
		map[string]any{"keep": "v", "legacyGone": "x"}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.MapRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "legacyGone") {
		t.Fatalf("unknown key not stripped: %s", out)
	}
}

func TestValidate_DuplicatedFilterNameRejected(t *testing.T) {
	// Given 同包重复 filter 名 When Install Then 拒绝
	m := `{"manifestVersion":1,"name":"dup","version":"1","parts":{
		"filters":[{"name":"same","entry":"a.js"},{"name":"same","entry":"b.js"}]}}`
	r := NewRegistry(testDB(t))
	if err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"a.js": filterSrc, "b.js": filterSrc})); err == nil {
		t.Fatal("duplicate filter accepted")
	}
}

func TestValidate_SecretMissingThrowsWithPartName(t *testing.T) {
	// Given secret 缺键 When BuildRequest Then 错误含包名与键名
	pkg := mustInstall(t, NewRegistry(testDB(t)))
	proto, err := NewProtocol(pkg, nil, func(target, key string) (string, bool) { return "", false }, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = proto.BuildRequest(newProtoCtx(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "demo") || !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("error lacks pkg/key: %v", err)
	}
}

func TestSecretRefsUnion(t *testing.T) {
	// Given 主包 protocol+filter When Union Then 非空
	pkg := mustInstall(t, NewRegistry(testDB(t)))
	if got := pkg.SecretRefsUnion(); len(got) == 0 {
		t.Fatal("empty union")
	}
}

func TestRegistry_InstallUpgradePersist(t *testing.T) {
	// Given 同名包二次安装 When Install Then revision+1 且重启恢复
	db := testDB(t)
	r := NewRegistry(db)
	_ = mustInstall(t, r)
	if err := r.Install(context.Background(), buildAAP(t, goodManifest, map[string]string{
		"protocol.js":         protoSrc,
		"filters/identity.js": filterSrc,
	})); err != nil {
		t.Fatal(err)
	}
	if r.Revision("demo") != 2 {
		t.Fatalf("revision: %d", r.Revision("demo"))
	}
	r2 := NewRegistry(db)
	if err := r2.LoadFromDB(context.Background()); err != nil {
		t.Fatal(err)
	}
	pkg, err := r2.GetPackage("demo")
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if pkg.Revision != 2 {
		t.Fatalf("restored revision: %d", pkg.Revision)
	}
}

func TestRegistry_EnableDisable(t *testing.T) {
	// Given 禁用 When IsEnabled Then false;重新启用 Then true(包保留内存)
	r := NewRegistry(testDB(t))
	_ = mustInstall(t, r)
	if err := r.Enable(context.Background(), "demo", false); err != nil {
		t.Fatal(err)
	}
	if r.IsEnabled("demo") {
		t.Fatal("disabled package still enabled")
	}
	if _, err := r.GetPackage("demo"); err != nil {
		t.Fatalf("package must remain visible: %v", err)
	}
	if err := r.Enable(context.Background(), "demo", true); err != nil {
		t.Fatal(err)
	}
	if !r.IsEnabled("demo") {
		t.Fatal("re-enabled package still disabled")
	}
}

func TestUpdatePart_CompileValidation(t *testing.T) {
	// Given 语法错误的部件代码 When UpdatePart Then 拒绝且 revision 不变;合法替换 revision+1
	r := NewRegistry(testDB(t))
	_ = mustInstall(t, r)
	before := r.Revision("demo")
	if _, err := r.UpdatePart(context.Background(), "demo", "protocol", "", []byte(`module.exports = { broken`)); err == nil {
		t.Fatal("bad code accepted")
	}
	if r.Revision("demo") != before {
		t.Fatal("revision changed on rejected update")
	}
	if _, err := r.UpdatePart(context.Background(), "demo", "protocol", "", []byte(protoSrc)); err != nil {
		t.Fatal(err)
	}
	if r.Revision("demo") != before+1 {
		t.Fatalf("revision after update: %d", r.Revision("demo"))
	}
}

func TestRuntime_SyncViolationDetected(t *testing.T) {
	// Given mapEvent 返回 Promise When MapEvent Then 同步违规错误
	m := `{"manifestVersion":1,"name":"async","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["streaming"]}}}`
	asyncSrc := `module.exports = { buildRequest: function(){return {url:"u"}}, mapEvent: function(ctx, e){ return Promise.resolve(e); } };`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"p.js": asyncSrc}))
	if err != nil {
		t.Fatal(err)
	}
	proto, err := NewProtocol(pkg, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = proto.MapEvent(nil, []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "sync violation") {
		t.Fatalf("sync violation: %v", err)
	}
}

func TestRuntime_FilterFactoryConfig(t *testing.T) {
	// Given factory 形式 filter+config When NewFilter Then config 闭包生效
	cfgSrc := `module.exports = function (config) {
		return { mapRequest: function (ctx, pivot) {
			var o = JSON.parse(pivot); o.tag = config.tag; return JSON.stringify(o);
		} };
	};`
	m := `{"manifestVersion":1,"name":"cfg","version":"1","parts":{
		"filters":[{"name":"tag","entry":"f.js","configSchema":{"type":"object"}}]}}`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"f.js": cfgSrc}))
	if err != nil {
		t.Fatal(err)
	}
	f, err := NewFilter(pkg, pkg.Manifest.Parts.Filters[0], map[string]any{"tag": "T1"}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.MapRequest(nil, []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "T1") {
		t.Fatalf("config not applied: %s", out)
	}
}

func TestUtil_TemplateAndPath(t *testing.T) {
	// Given util 注入 When template/get/set Then 点路径含数组索引可用
	m := `{"manifestVersion":1,"name":"ut","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["non_streaming"]}}}`
	src := `module.exports = {
		buildRequest: function (ctx, pivot) {
			var o = util.set(JSON.parse(pivot), "messages[0].role", "system");
			var role = util.get(o, "messages[0].role");
			return { url: util.template("https://x/{model}", { model: "m1" }) + "?" + role, method: "POST", headers: {}, body: "{}" };
		},
		mapResponse: function (ctx, b) { return b; }
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"p.js": src}))
	if err != nil {
		t.Fatal(err)
	}
	proto, err := NewProtocol(pkg, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.BuildRequest(newProtoCtx(), []byte(`{"model":"m","messages":[{}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(req.URL, "https://x/m1?system") {
		t.Fatalf("template/path broken: %s", req.URL)
	}
}

func TestInspect_MasksSecrets(t *testing.T) {
	// Given 部件 inspect 含凭据值 When 输出 Then 凭据替换 ***
	m := `{"manifestVersion":1,"name":"mask","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["non_streaming"],"secretRefs":["api_key"]}}}`
	src := `module.exports = {
		buildRequest: function (ctx, pivot) {
			var k = util.secret("api_key");
			return { url: "https://x", method: "POST", headers: {}, body: util.inspect({ key: k }) };
		},
		mapResponse: function (ctx, b) { return b; }
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"p.js": src}))
	if err != nil {
		t.Fatal(err)
	}
	proto, err := NewProtocol(pkg, nil,
		func(target, key string) (string, bool) { return "sk-secret-value", true },
		func(target string) map[string]string { return map[string]string{"api_key": "sk-secret-value"} },
		nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.BuildRequest(newProtoCtx(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(req.Body), "sk-secret-value") {
		t.Fatalf("secret leaked via inspect: %s", req.Body)
	}
	if !strings.Contains(string(req.Body), "***") {
		t.Fatalf("mask missing: %s", req.Body)
	}
}

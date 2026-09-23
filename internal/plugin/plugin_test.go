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
		declaration_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')))`); err != nil {
		t.Fatal(err)
	}
	return db
}

const goodManifest = `{"manifestVersion":1,"name":"demo","version":"1.0.0","parts":{
	"protocol":{"protocol":"openai-completions","features":["tools"]},
	"filters":[{"name":"identity"}]}}`

const protoSrc = `module.exports = function (config) {
	return {
		buildRequest: function (ctx, entry) {
			return { url: config.base_url + "/v1/chat/completions", method: "POST",
				headers: { "Content-Type": "application/json", "Authorization": "Bearer " + util.key().api_key },
				body: entry, stream: ctx.vars.entryStream };
		},
		mapEvent: function (ctx, e) { return JSON.stringify([{ chunk: e }]); },
		mapResponse: function (ctx, b) { return b; }
	};
};
module.exports.settings = { base_url: setting.string({ required: true }) };`

const filterSrc = `module.exports = function (config) {
	return {
		mapRequest: function (ctx, entry) { return entry; },
		mapChunk: function (ctx, c) { return c; },
		mapResponse: function (ctx, r) { return r; }
	};
};`

// mustInstall 构造并安装测试包(v2:连接信息在包参数,密钥走包级 keys)
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

// newProtoCtx 构造管道上下文(v2:无目标)
func newProtoCtx() *pipeline.PipelineContext {
	return pipeline.NewContext("r1", pipeline.UpstreamInfo{Name: "u"}, pipeline.Vars{Model: "m", EntryStream: true})
}

// keyValues 包级 keys 值视图(脱敏用)
func keyValues() func() []string {
	return func() []string { return []string{"sk-live", "sk-secret-value"} }
}

func TestParseAndInstantiate_FullFlow(t *testing.T) {
	// Given 合法包(参数预校验)+ pctx.Key When NewProtocol+BuildRequest Then 参数闭包与 util.key() 注入当前键 data
	r := NewRegistry(testDB(t))
	pkg := mustInstall(t, r)
	proto, err := NewProtocol(pkg, map[string]any{"base_url": "https://up.test"}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pctx := newProtoCtx()
	pctx.Key = &pipeline.KeyEntry{ID: "main", Data: map[string]any{"api_key": "sk-live"}}
	req, err := proto.BuildRequest(pctx, []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://up.test/v1/chat/completions" {
		t.Fatalf("url: %s", req.URL)
	}
	if req.Headers["Authorization"] != "Bearer sk-live" {
		t.Fatalf("auth: %s", req.Headers["Authorization"])
	}
}

func TestValidate_MissingProtocolNameRejected(t *testing.T) {
	// Given protocol 无协议槽名 When Install Then 拒绝
	m := `{"manifestVersion":1,"name":"nfm","version":"1","parts":{
		"protocol":{"features":["tools"]}}}`
	r := NewRegistry(testDB(t))
	if err := r.Install(context.Background(), buildAAP(t, m, map[string]string{ProtocolEntry: protoSrc})); err == nil {
		t.Fatal("missing protocol name accepted")
	}
}

func TestValidate_MissingImplementationRejected(t *testing.T) {
	// Given 无 mapEvent/mapResponse(两形态都不支持)When Install Then 拒绝(至少支持一形态)
	m := `{"manifestVersion":1,"name":"noimpl","version":"1","parts":{
		"protocol":{"protocol":"openai-completions"}}}`
	src := `module.exports = { buildRequest: function(){return {url:"u"}} };`
	r := NewRegistry(testDB(t))
	err := r.Install(context.Background(), buildAAP(t, m, map[string]string{ProtocolEntry: src}))
	if err == nil || !strings.Contains(err.Error(), "no forms") {
		t.Fatalf("install must reject missing impl: %v", err)
	}
}

func TestValidate_ExtraImplAccepted(t *testing.T) {
	// Given 双形态全实现 When Install Then 接受(实现即声明,多实现是合法形态覆盖)
	m := `{"manifestVersion":1,"name":"extraimpl","version":"1","parts":{
		"protocol":{"protocol":"openai-completions"}}}`
	src := `module.exports = { buildRequest: function(){return {url:"u"}},
		mapEvent: function(ctx,e){ return e; },
		mapResponse: function(ctx,b){ return b; } };`
	r := NewRegistry(testDB(t))
	if err := r.Install(context.Background(), buildAAP(t, m, map[string]string{ProtocolEntry: src})); err != nil {
		t.Fatalf("full impl must be accepted: %v", err)
	}
}

func TestValidate_SyntaxErrorLineReported(t *testing.T) {
	// Given 语法错误部件 When Install Then 错误带行号
	m := `{"manifestVersion":1,"name":"badline","version":"1","parts":{
		"filters":[{"name":"f"}]}}`
	src := "module.exports = function (config) {\n  return { mapRequest: function (ctx, p) {\n    return p  // 缺分号致下一行语法错\n  } };\n};\n)))"
	r := NewRegistry(testDB(t))
	err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"filters/f.js": src}))
	if err == nil || !strings.Contains(err.Error(), "Line 6") {
		t.Fatalf("syntax error must report line: %v", err)
	}
}

func TestValidate_DuplicatedFilterNameRejected(t *testing.T) {
	// Given 同包重复 filter 名 When Install Then 拒绝(名即路径,重复名 = 重复路径)
	m := `{"manifestVersion":1,"name":"dup","version":"1","parts":{
		"filters":[{"name":"same"},{"name":"same"}]}}`
	r := NewRegistry(testDB(t))
	if err := r.Install(context.Background(), buildAAP(t, m, map[string]string{"filters/same.js": filterSrc})); err == nil {
		t.Fatal("duplicate filter accepted")
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

func TestRuntime_SyncViolationDetected(t *testing.T) {
	// Given mapEvent 返回 Promise When MapEvent Then 同步违规错误
	m := `{"manifestVersion":1,"name":"async","version":"1","parts":{
		"protocol":{"protocol":"openai-completions"}}}`
	asyncSrc := `module.exports = { buildRequest: function(){return {url:"u"}}, mapEvent: function(ctx, e){ return Promise.resolve(e); } };`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{ProtocolEntry: asyncSrc}))
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
	// Given factory 形式 filter+解析后参数 When NewFilter Then config 闭包生效(v2 参数由调用方解析)
	cfgSrc := `module.exports = function (config) {
		return { mapRequest: function (ctx, entry) {
			var o = JSON.parse(entry); o.tag = config.tag; return JSON.stringify(o);
		} };
	};`
	m := `{"manifestVersion":1,"name":"cfg","version":"1","parts":{
		"filters":[{"name":"tag"}]}}`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{"filters/tag.js": cfgSrc}))
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
		"protocol":{"protocol":"openai-completions"}}}`
	src := `module.exports = {
		buildRequest: function (ctx, entry) {
			var o = util.set(JSON.parse(entry), "messages[0].role", "system");
			var role = util.get(o, "messages[0].role");
			return { url: util.template("https://x/{model}", { model: "m1" }) + "?" + role, method: "POST", headers: {}, body: "{}" };
		},
		mapResponse: function (ctx, b) { return b; }
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{ProtocolEntry: src}))
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

func TestInspect_MasksPackageKeys(t *testing.T) {
	// Given 部件 inspect 含当前键 data 值 When 输出 Then 值替换 ***(密钥唯一来源 = 当前键 data)
	m := `{"manifestVersion":1,"name":"mask","version":"1","parts":{
		"protocol":{"protocol":"openai-completions"}}}`
	src := `module.exports = {
		buildRequest: function (ctx, entry) {
			var k = util.key().api_key;
			return { url: "https://x", method: "POST", headers: {}, body: util.inspect({ key: k }) };
		},
		mapResponse: function (ctx, b) { return b; }
	};`
	pkg, err := ParseAAP(buildAAP(t, m, map[string]string{ProtocolEntry: src}))
	if err != nil {
		t.Fatal(err)
	}
	proto, err := NewProtocol(pkg, nil, nil, keyValues(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pctx := newProtoCtx()
	pctx.Key = &pipeline.KeyEntry{ID: "main", Data: map[string]any{"api_key": "sk-secret-value"}}
	req, err := proto.BuildRequest(pctx, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(req.Body), "sk-secret-value") {
		t.Fatalf("key leaked via inspect: %s", req.Body)
	}
	if !strings.Contains(string(req.Body), "***") {
		t.Fatalf("mask missing: %s", req.Body)
	}
}

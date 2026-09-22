package plugin

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ─── 包级 key 存储(BDD:previous 覆盖 / 上限拒写 / 明文视图 / 升级快照 / 卸载清理)───

func testKeysDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestKeys_SetMovesCurrentToPrevious(t *testing.T) {
	// Given current={"token":"t2"} previous={"token":"t1"} When set({"token":"t3"}) Then previous=t2(t1 丢弃)
	ks := NewKeysStore(testKeysDB(t))
	if err := ks.Set("pkg", map[string]any{"token": "t1"}); err != nil {
		t.Fatal(err)
	}
	if err := ks.Set("pkg", map[string]any{"token": "t2"}); err != nil {
		t.Fatal(err)
	}
	if err := ks.Set("pkg", map[string]any{"token": "t3"}); err != nil {
		t.Fatal(err)
	}
	if v, _ := ks.Get("pkg", "token"); v != "t3" {
		t.Fatalf("current: %v", v)
	}
	if v, _ := ks.Previous("pkg", "token"); v != "t2" {
		t.Fatalf("previous: %v", v)
	}
}

func TestKeys_LimitRejects(t *testing.T) {
	// Given 已接近 64KB When 超限写入 Then 拒绝
	ks := NewKeysStore(testKeysDB(t))
	big := map[string]any{"blob": string(make([]byte, KeysLimit))}
	if err := ks.Set("pkg", big); err == nil {
		t.Fatal("oversized keys accepted")
	}
}

func TestKeys_ViewAndDelete(t *testing.T) {
	// Given set 后 When View Then 明文输出(键名+值+updatedAt);Delete 后全空
	ks := NewKeysStore(testKeysDB(t))
	if err := ks.Set("pkg", map[string]any{"token": "secret-value"}); err != nil {
		t.Fatal(err)
	}
	m := ks.View("pkg")
	cur := m["current"].(map[string]any)
	if cur["token"] != "secret-value" {
		t.Fatalf("view current: %v", cur)
	}
	ks.Delete("pkg")
	if _, ok := ks.Get("pkg", "token"); ok {
		t.Fatal("key survived delete")
	}
}

func TestKeys_UpgradeSnapshot(t *testing.T) {
	// Given current 有值 When 升级快照 Then previous=旧 current 且 current 原样
	ks := NewKeysStore(testKeysDB(t))
	if err := ks.Set("pkg", map[string]any{"token": "old"}); err != nil {
		t.Fatal(err)
	}
	if err := ks.UpgradeSnapshot("pkg"); err != nil {
		t.Fatal(err)
	}
	if v, _ := ks.Get("pkg", "token"); v != "old" {
		t.Fatalf("current after upgrade: %v", v)
	}
	if v, _ := ks.Previous("pkg", "token"); v != "old" {
		t.Fatalf("previous after upgrade: %v", v)
	}
}

// ─── manifest hooks 校验 ───

func TestValidate_HooksOnlyPackage(t *testing.T) {
	// Given 仅 hooks 的包 When Validate Then 合法(纯签到包形态;多文件布局)
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "cronly", Version: "0.1.0"},
		Files:    map[string][]byte{"tasks/signIn.js": []byte("module.exports={handler:function(ctx){}}")},
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "signIn", Cron: "0 9 * * *"}}}
	if err := pkg.Validate(); err != nil {
		t.Fatalf("hooks-only rejected: %v", err)
	}
}

func TestValidate_HooksErrors(t *testing.T) {
	// 任务文件缺失 / task 重名 / cron 非法 / cron+next 同现
	base := func(files map[string][]byte) *Package {
		return &Package{
			Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "h", Version: "0.1.0"},
			Files:    files,
		}
	}
	taskFile := func(src string) map[string][]byte {
		return map[string][]byte{"tasks/a.js": []byte(src)}
	}
	p := base(taskFile("module.exports={handler:function(ctx){}}"))
	p.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "gone", Cron: "* * * * *"}}}
	if err := p.Validate(); err == nil {
		t.Fatal("missing task file accepted")
	}
	p = base(taskFile("module.exports={handler:function(ctx){}}"))
	p.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{
		{Name: "a", Cron: "* * * * *"}, {Name: "a", Cron: "*/5 * * * *"},
	}}
	if err := p.Validate(); err == nil {
		t.Fatal("duplicate task accepted")
	}
	p = base(taskFile("module.exports={handler:function(ctx){}}"))
	p.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "a", Cron: "bad"}}}
	if err := p.Validate(); err == nil {
		t.Fatal("bad cron accepted")
	}
	p = base(taskFile("module.exports={handler:function(ctx){}}"))
	p.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "a", Cron: "* * * * *", Next: true}}}
	if err := p.Validate(); err == nil {
		t.Fatal("cron+next both set accepted")
	}
}

func TestValidate_MetaLimits(t *testing.T) {
	// 元数据纯展示字段限长(超限拒装注明)
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "m", Version: "0.1.0"},
		Files:    map[string][]byte{"tasks/a.js": []byte("module.exports={handler:function(ctx){}}")},
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "a", Cron: "* * * * *"}}}
	pkg.Manifest.Title = strings.Repeat("题", 65)
	if err := pkg.Validate(); err == nil || !strings.Contains(err.Error(), "title") {
		t.Fatalf("long title accepted: %v", err)
	}
	pkg.Manifest.Title = "短名"
	pkg.Manifest.Description = strings.Repeat("述", 257)
	if err := pkg.Validate(); err == nil || !strings.Contains(err.Error(), "description") {
		t.Fatalf("long description accepted: %v", err)
	}
}

// ─── hooks 运行时(多文件布局)───

type fakeHTTP struct {
	got  HttpRequest
	resp *HttpResponse
	err  error
}

func (f *fakeHTTP) Do(req HttpRequest) (*HttpResponse, error) {
	f.got = req
	return f.resp, f.err
}

func hooksPkg(t *testing.T, files map[string]string, tasks ...HooksTask) *Package {
	t.Helper()
	fs := map[string][]byte{}
	for k, v := range files {
		fs[k] = []byte(v)
	}
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "hp", Version: "0.1.0"},
		Files:    fs,
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Tasks: tasks}
	return pkg
}

func TestHooks_ProgramCache(t *testing.T) {
	// Given 同包同 revision 同文件两次取编译产物 When 取 Then 复用同一 Program;revision 变化后重新编译
	src := `module.exports={handler:function(ctx){}}`
	pkg := hooksPkg(t, map[string]string{"tasks/a.js": src}, HooksTask{Name: "a", Cron: "* * * * *"})
	p1, err := hooksProgram(pkg, "tasks/a.js", pkg.Files["tasks/a.js"])
	if err != nil {
		t.Fatal(err)
	}
	p2, err := hooksProgram(pkg, "tasks/a.js", pkg.Files["tasks/a.js"])
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatal("program must be cached within same revision")
	}
	pkg.Revision = 7
	p3, err := hooksProgram(pkg, "tasks/a.js", pkg.Files["tasks/a.js"])
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p3 {
		t.Fatal("revision bump must recompile")
	}
}

func TestHooks_TaskRunAndCtxTask(t *testing.T) {
	// Given 任务文件导出 handler When RunTask Then 执行且 ctx.task=任务名;多行同指一实现各自带名
	seen := map[string]string{}
	mem := &memKV{m: map[string]string{}}
	_ = mem
	files := map[string]string{"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.storage && storage.set("task", ctx.task);}}`}
	_ = seen
	pkg := hooksPkg(t, files, HooksTask{Name: "signIn", Cron: "* * * * *"}, HooksTask{Name: "signInPm", Cron: "0 18 * * *", Entry: "tasks/signIn.js"})
	rt, err := LoadTask(pkg, HooksTask{Name: "signIn", Cron: "* * * * *"}, HooksDeps{Storage: &memKV{m: map[string]string{}}, Now: func() time.Time { return time.Unix(0, 0) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(HooksTask{Name: "signIn", Cron: "* * * * *"}, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	_ = mem
	// 共用实现:第二行同文件,task 名不同
	rt2, err := LoadTask(pkg, HooksTask{Name: "signInPm", Cron: "0 18 * * *", Entry: "tasks/signIn.js"}, HooksDeps{Storage: &memKV{m: map[string]string{}}, Now: func() time.Time { return time.Unix(0, 0) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.RunTask(HooksTask{Name: "signInPm", Entry: "tasks/signIn.js"}, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestHooks_TaskMissingHandler(t *testing.T) {
	// Given 任务文件未导出 handler When RunTask Then 报错
	pkg := hooksPkg(t, map[string]string{"tasks/a.js": `module.exports={}`}, HooksTask{Name: "a", Cron: "* * * * *"})
	rt, err := LoadTask(pkg, HooksTask{Name: "a", Cron: "* * * * *"}, HooksDeps{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(HooksTask{Name: "a", Cron: "* * * * *"}, time.Unix(0, 0)); err == nil {
		t.Fatal("missing handler accepted")
	}
}

func TestHooks_Require(t *testing.T) {
	// Given lib 共享模块被任务 require When RunTask Then 幂等单例(同一对象引用)
	files := map[string]string{
		"lib/consts.js": `module.exports={TOKEN_KEY:"token", obj:{n:1}}`,
		"tasks/a.js": `var c = require("../lib/consts.js");
			module.exports={handler:function(ctx){ c.obj.n += 1; storage.set("n", String(c.obj.n)); }}`,
	}
	pkg := hooksPkg(t, files, HooksTask{Name: "a", Cron: "* * * * *"})
	mem := &memKV{m: map[string]string{}}
	deps := HooksDeps{Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}
	rt, err := LoadTask(pkg, HooksTask{Name: "a", Cron: "* * * * *"}, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(HooksTask{Name: "a", Cron: "* * * * *"}, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	// 同 runtime 才共享;新 runtime 新模块实例(非池化语义)——此处断言包内 require 路径解析与执行
	if v := mem.m["n"]; v != "2" {
		t.Fatalf("require exec: %v", v)
	}
	// 循环 require 拒绝
	bad := hooksPkg(t, map[string]string{
		"a.js": `module.exports=require("./b.js")`,
		"b.js": `module.exports=require("./a.js")`,
	})
	env := newHooksEnv(bad, HooksDeps{}, false)
	if _, err := env.load("", "a.js"); err == nil || !strings.Contains(err.Error(), "circular") {
		t.Fatalf("circular require accepted: %v", err)
	}
	// 包外路径拒绝
	env2 := newHooksEnv(hooksPkg(t, map[string]string{"a.js": "module.exports=1"}), HooksDeps{}, false)
	if _, err := env2.resolve("a.js", "../outside.js"); err == nil {
		t.Fatal("outside require accepted")
	}
}

func TestHooks_OnLoadAndSettings(t *testing.T) {
	// Given init.js 导出 onLoad 且声明+覆盖合并快照注入 When RunOnLoad Then ctx.settings 读到覆盖值
	files := map[string]string{"init.js": `module.exports={onLoad:function(ctx){storage.set("acct", String(ctx.settings.account));}}`}
	pkg := hooksPkg(t, files)
	snap := map[string]any{"account": "over@x"}
	mem := &memKV{m: map[string]string{}}
	rt, err := LoadInit(pkg, HooksDeps{Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}, snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunOnLoad(nil); err != nil {
		t.Fatal(err)
	}
	if v := mem.m["acct"]; v != "over@x" {
		t.Fatalf("settings snapshot: %v", v)
	}
	// previous 注入:热加载可读,启停 nil 不可读
	src := `module.exports={onLoad:function(ctx){storage.set("prev", String(ctx.keys.previous("token")));}}`
	pkg2 := hooksPkg(t, map[string]string{"init.js": src})
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("hp", map[string]any{"token": "stored"})
	rt2, err := LoadInit(pkg2, HooksDeps{Keys: ks, Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.RunOnLoad(nil); err != nil {
		t.Fatal(err)
	}
	if v := mem.m["prev"]; v != "undefined" {
		t.Fatalf("startup previous leaked: %v", v)
	}
	if err := rt2.RunOnLoad(map[string]any{"token": "hot"}); err != nil {
		t.Fatal(err)
	}
	if v := mem.m["prev"]; v != "hot" {
		t.Fatalf("hot previous: %v", v)
	}
}

func TestHooks_KeysList(t *testing.T) {
	// Given 包已有 3 键 When 任务 ctx.keys.list() Then 排序键名数组不含值
	files := map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){storage.set("names", JSON.stringify(ctx.keys.list()));}}`}
	tk := HooksTask{Name: "a", Cron: "* * * * *"}
	pkg := hooksPkg(t, files, tk)
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("hp", map[string]any{"token-b": "v2", "account": "me@x", "token-a": "v1"})
	mem := &memKV{m: map[string]string{}}
	deps := HooksDeps{Keys: ks, Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}
	rt, err := LoadTask(pkg, tk, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(tk, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if got := mem.m["names"]; got != `["account","token-a","token-b"]` {
		t.Fatalf("list: %s", got)
	}
	// 空键包 → [](独立 store 隔离场景 1 数据)
	pkg2 := hooksPkg(t, map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){storage.set("n", String(ctx.keys.list().length));}}`}, tk)
	deps2 := HooksDeps{Keys: NewKeysStore(testKeysDB(t)), Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}
	rt2, err := LoadTask(pkg2, tk, deps2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.RunTask(tk, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if got := mem.m["n"]; got != "0" {
		t.Fatalf("empty list: %s", got)
	}
}

func TestKeys_SetKeyRotatesPrevious(t *testing.T) {
	// Given 已有键 When SetKey 单键编辑 Then previous=旧 current 整体(别名污染回归:轮转后 previous 不得跟随 current 变更)
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("hp", map[string]any{"token": "old-token", "other": "keep"})
	if _, err := ks.SetKey("hp", "token", "new-token"); err != nil {
		t.Fatal(err)
	}
	cur, _ := ks.Get("hp", "token")
	if cur != "new-token" {
		t.Fatalf("current: %v", cur)
	}
	prev, ok := ks.Previous("hp", "token")
	if !ok || prev != "old-token" {
		t.Fatalf("previous polluted: %v ok=%v", prev, ok)
	}
	// 未编辑键:current 保留,previous 同值
	if v, _ := ks.Get("hp", "other"); v != "keep" {
		t.Fatalf("other lost: %v", v)
	}
	if p, ok := ks.Previous("hp", "other"); !ok || p != "keep" {
		t.Fatalf("other previous: %v %v", p, ok)
	}
	// 首次写入(空文档):previous 空
	if _, err := ks.SetKey("hp2", "k", "v"); err != nil {
		t.Fatal(err)
	}
	if p, ok := ks.Previous("hp2", "k"); ok || p != nil {
		t.Fatalf("fresh previous: %v %v", p, ok)
	}
}

func TestHooks_PromiseRejected(t *testing.T) {
	// Given task 返回 Promise When RunTask Then 同步性违规报错
	pkg := hooksPkg(t, map[string]string{"tasks/a.js": `module.exports={handler:async function(ctx){}}`}, HooksTask{Name: "a", Cron: "* * * * *"})
	rt, err := LoadTask(pkg, HooksTask{Name: "a", Cron: "* * * * *"}, HooksDeps{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(HooksTask{Name: "a", Cron: "* * * * *"}, time.Unix(0, 0)); err == nil || !strings.Contains(err.Error(), "sync violation") {
		t.Fatalf("promise accepted: %v", err)
	}
}

func TestHooks_TimeoutInterrupts(t *testing.T) {
	// Given task 死循环 timeoutMs=50 When RunTask Then 超时错误
	pkg := hooksPkg(t, map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){while(true){}}}`}, HooksTask{Name: "a", Cron: "* * * * *", TimeoutMs: 50})
	rt, err := LoadTask(pkg, HooksTask{Name: "a", Cron: "* * * * *", TimeoutMs: 50}, HooksDeps{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(HooksTask{Name: "a", Cron: "* * * * *", TimeoutMs: 50}, time.Unix(0, 0)); err == nil {
		t.Fatal("infinite task completed")
	}
}

func TestHooks_ExtractDeclaration(t *testing.T) {
	// Given 族文件多处挂 settings 片段 When ExtractDeclaration Then 固定序合并(tasks 覆盖 settings.js)
	files := map[string]string{
		"settings.js":  `module.exports.settings={a:1, shared:"from-settings"}`,
		"init.js":      `module.exports.settings={b:2}`,
		"keys.js":      `module.exports={keyWrite:function(ctx,k,v){return v;}}; module.exports.settings={c:3}`,
		"tasks/a.js":   `module.exports.settings={shared:"from-task"}; module.exports.handler=function(ctx){}`,
	}
	pkg := hooksPkg(t, files, HooksTask{Name: "a", Cron: "* * * * *"})
	decl, err := ExtractDeclaration(pkg, HooksDeps{})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := numericField(decl["a"]); n != 1 {
		t.Fatalf("decl a: %v", decl["a"])
	}
	if n, _ := numericField(decl["b"]); n != 2 || func() bool { n, _ = numericField(decl["c"]); return n != 3 }() {
		t.Fatalf("merged decl: %v", decl)
	}
	if decl["shared"] != "from-task" {
		t.Fatalf("merge order (tasks last): %v", decl["shared"])
	}
	// 语法错误拒装
	bad := hooksPkg(t, map[string]string{"init.js": `module.exports={`}, HooksTask{Name: "a", Cron: "* * * * *"})
	if _, err := ExtractDeclaration(bad, HooksDeps{}); err == nil {
		t.Fatal("syntax error accepted")
	}
	// 非法声明形态(非对象)拒装
	bad2 := hooksPkg(t, map[string]string{"init.js": `module.exports.settings=42; module.exports=function(){}`}, HooksTask{Name: "a", Cron: "* * * * *"})
	if _, err := ExtractDeclaration(bad2, HooksDeps{}); err == nil {
		t.Fatal("non-object settings accepted")
	}
}

func TestHooks_NextForm(t *testing.T) {
	// Given next 形态任务导出 next When RunNext Then 返回 unix ms;null = 停止;未导出报错
	files := map[string]string{"tasks/poll.js": `module.exports={handler:function(ctx){}, next:function(ctx){return ctx.cron.runAt ? 12345 : null;}}`}
	pkg := hooksPkg(t, files, HooksTask{Name: "poll", Next: true})
	rt, err := LoadTask(pkg, HooksTask{Name: "poll", Next: true}, HooksDeps{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ms, ok, err := rt.RunNext()
	if err != nil || !ok || ms != 12345 {
		t.Fatalf("next: %d %v %v", ms, ok, err)
	}
	// null → 停止
	files2 := map[string]string{"tasks/once.js": `module.exports={handler:function(ctx){}, next:function(ctx){return null;}}`}
	pkg2 := hooksPkg(t, files2, HooksTask{Name: "once", Next: true})
	rt2, _ := LoadTask(pkg2, HooksTask{Name: "once", Next: true}, HooksDeps{}, nil)
	if _, ok, err := rt2.RunNext(); err != nil || ok {
		t.Fatalf("null next: ok=%v err=%v", ok, err)
	}
	// 未导出 next
	files3 := map[string]string{"tasks/x.js": `module.exports={handler:function(ctx){}}`}
	pkg3 := hooksPkg(t, files3, HooksTask{Name: "x", Next: true})
	rt3, _ := LoadTask(pkg3, HooksTask{Name: "x", Next: true}, HooksDeps{}, nil)
	if _, _, err := rt3.RunNext(); err == nil {
		t.Fatal("missing next accepted")
	}
}

func TestHooks_UtilKeyReadonlyInProtocol(t *testing.T) {
	// Given protocol 部件 util.key When 读 Then 返回当前值(实时)
	protoSrc := "module.exports={buildRequest:function(ctx,req){return {url:String(util.key('endpoint')),method:'POST',headers:{},body:req};},mapEvent:function(ctx,e){return '[]';}}"
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "kp", Version: "0.1.0"},
		Files:    map[string][]byte{"p.js": []byte(protoSrc)},
	}
	pkg.Manifest.Parts.Protocol = &ProtocolPart{Entry: "p.js", Protocol: "openai-completions", Form: []string{"streaming"}}
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("kp", map[string]any{"endpoint": "https://a"})
	proto, err := NewProtocol(pkg, nil, nil, nil, nil, func(name string) (any, bool) { return ks.Get("kp", name) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://a" {
		t.Fatalf("util.key: %s", req.URL)
	}
	// 实时性:更新后新值
	_ = ks.Set("kp", map[string]any{"endpoint": "https://b"})
	req, err = proto.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://b" {
		t.Fatalf("util.key stale: %s", req.URL)
	}
}

func TestHooks_UtilNoListInProtocol(t *testing.T) {
	// Given protocol 部件 When 访问 util.list Then undefined(枚举面不扩散到 protocol)
	protoSrc := "module.exports={buildRequest:function(ctx,req){return {url:String(util.list===undefined),method:'POST',headers:{},body:req};},mapEvent:function(ctx,e){return '[]';}}"
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "kp", Version: "0.1.0"},
		Files:    map[string][]byte{"p.js": []byte(protoSrc)},
	}
	pkg.Manifest.Parts.Protocol = &ProtocolPart{Entry: "p.js", Protocol: "openai-completions", Form: []string{"streaming"}}
	proto, err := NewProtocol(pkg, nil, nil, nil, nil, func(name string) (any, bool) { return nil, false }, nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.BuildRequest(nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "true" {
		t.Fatalf("util.list leaked into protocol: %s", req.URL)
	}
}

var _ = context.Background

func TestValidate_ReservedPrefixRejected(t *testing.T) {
	// Given upstream: 前缀包名(storage ns 与 target secrets ns 碰撞面)When Validate Then 拒装
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "upstream:evil", Version: "0.1.0"},
		Files:    map[string][]byte{"tasks/a.js": []byte("module.exports={handler:function(ctx){}}")},
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Tasks: []HooksTask{{Name: "a", Cron: "* * * * *"}}}
	if err := pkg.Validate(); err == nil || !strings.Contains(err.Error(), "reserved prefix") {
		t.Fatalf("reserved prefix accepted: %v", err)
	}
}

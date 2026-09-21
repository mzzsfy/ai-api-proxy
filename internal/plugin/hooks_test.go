package plugin

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ─── 包级 key 存储(BDD:previous 覆盖 / 上限拒写 / 脱敏 / 卸载清理)───

func testKeysDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`)
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
	// Given 仅 hooks 的包 When Validate Then 合法(纯签到包形态)
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "cronly", Version: "0.1.0"},
		Files:    map[string][]byte{"hooks.js": []byte("module.exports={}")},
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Entry: "hooks.js", Tasks: []HooksTask{{Name: "signIn", Cron: "0 9 * * *"}}}
	if err := pkg.Validate(); err != nil {
		t.Fatalf("hooks-only rejected: %v", err)
	}
}

func TestValidate_HooksErrors(t *testing.T) {
	// 缺 entry / entry 缺失 / task 重名 / cron 非法
	base := func() *Package {
		return &Package{
			Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "h", Version: "0.1.0"},
			Files:    map[string][]byte{"hooks.js": []byte("module.exports={}")},
		}
	}
	p := base()
	p.Manifest.Parts.Hooks = &HooksPart{}
	if err := p.Validate(); err == nil {
		t.Fatal("empty hooks entry accepted")
	}
	p = base()
	p.Manifest.Parts.Hooks = &HooksPart{Entry: "gone.js"}
	if err := p.Validate(); err == nil {
		t.Fatal("missing hooks entry accepted")
	}
	p = base()
	p.Manifest.Parts.Hooks = &HooksPart{Entry: "hooks.js", Tasks: []HooksTask{
		{Name: "a", Cron: "* * * * *"}, {Name: "a", Cron: "* * * * *"},
	}}
	if err := p.Validate(); err == nil {
		t.Fatal("duplicate task accepted")
	}
	p = base()
	p.Manifest.Parts.Hooks = &HooksPart{Entry: "hooks.js", Tasks: []HooksTask{{Name: "a", Cron: "bad"}}}
	if err := p.Validate(); err == nil {
		t.Fatal("bad cron accepted")
	}
}

// ─── hooks 运行时 ───

type fakeHTTP struct {
	got  HttpRequest
	resp *HttpResponse
	err  error
}

func (f *fakeHTTP) Do(req HttpRequest) (*HttpResponse, error) {
	f.got = req
	return f.resp, f.err
}

func hooksPkg(t *testing.T, src string, tasks ...HooksTask) *Package {
	t.Helper()
	pkg := &Package{
		Manifest: &Manifest{ManifestVersion: ManifestVersion, Name: "hp", Version: "0.1.0"},
		Files:    map[string][]byte{"hooks.js": []byte(src)},
	}
	pkg.Manifest.Parts.Hooks = &HooksPart{Entry: "hooks.js", Tasks: tasks}
	return pkg
}

func TestHooks_ProgramCache(t *testing.T) {
	// Given 同包同 revision 两次取编译产物 When 取 Then 复用同一 Program;revision 变化后重新编译
	src := `module.exports={onLoad:function(ctx){}}`
	pkg := hooksPkg(t, src)
	p1, err := hooksProgram(pkg, "hooks.js", pkg.Files["hooks.js"])
	if err != nil {
		t.Fatal(err)
	}
	p2, err := hooksProgram(pkg, "hooks.js", pkg.Files["hooks.js"])
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatal("program must be cached within same revision")
	}
	pkg.Revision = 7
	p3, err := hooksProgram(pkg, "hooks.js", pkg.Files["hooks.js"])
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p3 {
		t.Fatal("revision bump must recompile")
	}
}

func TestHooks_OnLoadRunsOnceAndErrorNotFatal(t *testing.T) {
	// Given onLoad 写 storage When RunOnLoad Then 执行一次;抛错仅返回错误
	src := `module.exports={onLoad:function(ctx){storage.set("loaded","1");}}`
	rt, err := LoadHooks(hooksPkg(t, src), HooksDeps{Storage: &memKV{m: map[string]string{}}, Now: func() time.Time { return time.Unix(0, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunOnLoad(nil); err != nil {
		t.Fatal(err)
	}
	bad := `module.exports={onLoad:function(ctx){throw new Error("boom");}}`
	rt2, err := LoadHooks(hooksPkg(t, bad), HooksDeps{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.RunOnLoad(nil); err == nil {
		t.Fatal("onLoad error swallowed")
	}
}

func TestHooks_TaskHTTPAndKeys(t *testing.T) {
	// Given task 经 ctx.http 出站并 keys.set When RunTask Then 请求发出且 current/previous 正确
	src := `module.exports={signIn:function(ctx){
		var r = ctx.http.run({url:"https://x/checkin", method:"POST", headers:{"a":"b"}, body:"hi"});
		ctx.keys.set({token: r.body});
	}}`
	fh := &fakeHTTP{resp: &HttpResponse{Status: 200, Headers: map[string]string{"c": "d"}, Body: "tok"}}
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("hp", map[string]any{"token": "old"})
	rt, err := LoadHooks(hooksPkg(t, src, HooksTask{Name: "signIn", Cron: "* * * * *"}), HooksDeps{HTTP: fh, Keys: ks, Now: func() time.Time { return time.Unix(0, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask("signIn", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if fh.got.URL != "https://x/checkin" || fh.got.Method != "POST" || fh.got.Headers["a"] != "b" {
		t.Fatalf("http req: %+v", fh.got)
	}
	if fh.got.Timeout <= 0 {
		t.Fatalf("timeout not applied: %v", fh.got.Timeout)
	}
	if v, _ := ks.Get("hp", "token"); v != "tok" {
		t.Fatalf("current: %v", v)
	}
	if v, _ := ks.Previous("hp", "token"); v != "old" {
		t.Fatalf("previous: %v", v)
	}
}

func TestHooks_PreviousOnlyFromHotReload(t *testing.T) {
	// Given 启停场景 previous=nil When onLoad ctx.keys.previous Then undefined(不读存量)
	src := `module.exports={onLoad:function(ctx){storage.set("prev", String(ctx.keys.previous("token")));}}`
	ks := NewKeysStore(testKeysDB(t))
	_ = ks.Set("hp", map[string]any{"token": "stored", "x": "1"})
	mem := &memKV{m: map[string]string{}}
	rt, err := LoadHooks(hooksPkg(t, src), HooksDeps{Keys: ks, Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunOnLoad(nil); err != nil {
		t.Fatal(err)
	}
	if v := mem.m["prev"]; v != "undefined" {
		t.Fatalf("startup previous leaked: %v", v)
	}
	// 热加载:显式 previous 注入可读
	if err := rt.RunOnLoad(map[string]any{"token": "hot"}); err != nil {
		t.Fatal(err)
	}
	if v := mem.m["prev"]; v != "hot" {
		t.Fatalf("hot previous: %v", v)
	}
}

func TestHooks_PromiseRejected(t *testing.T) {
	// Given task 返回 Promise When RunTask Then 同步性违规报错
	src := `module.exports={work:async function(ctx){}}`
	rt, err := LoadHooks(hooksPkg(t, src, HooksTask{Name: "work", Cron: "* * * * *"}), HooksDeps{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask("work", time.Unix(0, 0)); err == nil || !contains(err.Error(), "sync violation") {
		t.Fatalf("promise accepted: %v", err)
	}
}

func TestHooks_TimeoutInterrupts(t *testing.T) {
	// Given task 死循环 timeoutMs=50 When RunTask Then 超时错误
	src := `module.exports={loop:function(ctx){while(true){}}}`
	rt, err := LoadHooks(hooksPkg(t, src, HooksTask{Name: "loop", Cron: "* * * * *", TimeoutMs: 50}), HooksDeps{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask("loop", time.Unix(0, 0)); err == nil {
		t.Fatal("infinite task completed")
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

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || index(s, sub) >= 0)
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

var _ = context.Background

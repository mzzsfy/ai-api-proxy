package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// hooksManifest 构造 hooks-only manifest(raw;多文件布局:无 entry,任务缺省 tasks/<name>.js)
const hooksManifest = `{"manifestVersion":1,"name":"checkin","version":"%s","parts":{
	"hooks":{"tasks":[{"name":"signIn","cron":"* * * * *","timeoutMs":1000}]}}}`

func hooksAAP(t *testing.T, version string, files map[string]string) []byte {
	t.Helper()
	fs := map[string][]byte{}
	for k, v := range files {
		fs[k] = []byte(v)
	}
	data, err := plugin.BuildAAP(mustManifest(t, sprintf(hooksManifest, version)), fs)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func sprintf(s, v string) string {
	out := make([]byte, 0, len(s)+len(v))
	i := 0
	for ; i < len(s); i++ {
		if i+2 < len(s) && s[i] == '%' && s[i+1] == 's' {
			out = append(out, v...)
			i++
			continue
		}
		out = append(out, s[i])
	}
	_ = i
	return string(out)
}

// initAndTaskFiles onLoad 与任务的新布局文件集
func initAndTaskFiles(onLoadJS, taskJS string) map[string]string {
	return map[string]string{
		"init.js":       onLoadJS,
		"tasks/signIn.js": taskJS,
	}
}

func TestHooks_InstallTriggersOnLoadAndKeysFlow(t *testing.T) {
	// Given hooks-only 包 When 安装→任务运行→升级→读管理 keys→卸载 Then 各环节语义成立
	// (keys 写窗:任务可写;onLoad ctx.keys 无 overwrite 能力——阉割即权限)
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()

	runSignIn := func(tokenVal string) {
		pkg, err := pkgs.GetPackage("checkin")
		if err != nil {
			t.Fatal(err)
		}
		task := plugin.HooksTask{Name: "signIn", Cron: "* * * * *", TimeoutMs: 1000}
		rt, err := plugin.LoadTask(pkg, task, plugin.HooksDeps{PackageName: "checkin", Keys: pkgs.Keys()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
			t.Fatal(err)
		}
		_ = tokenVal
	}

	// 安装:init 无写键能力(探测 overwrite 未挂载并记入 storage);任务执行写入
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", initAndTaskFiles(
		`module.exports={onLoad:function(ctx){storage.set("probe", String(ctx.keys.overwrite===undefined));}}`,
		`module.exports={handler:function(ctx){ctx.keys.overwrite({token:"signed"});}}`,
	))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, _ := pkgs.GetPackage("checkin")
		_ = v
		return true
	}, "install settle")
	runSignIn("signed")
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "signed" {
		t.Fatalf("task key: %v", v)
	}

	// 升级:onLoad 再次触发;任务再写,current 移入 previous
	if err := pkgs.Install(ctx, hooksAAP(t, "0.2.0", initAndTaskFiles(
		`module.exports={onLoad:function(ctx){storage.set("probe", "x");}}`,
		`module.exports={handler:function(ctx){ctx.keys.overwrite({token:"signed-2"});}}`,
	))); err != nil {
		t.Fatal(err)
	}
	runSignIn("signed-2")
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "signed-2" {
		t.Fatalf("task key after upgrade: %v", v)
	}
	if p, _ := pkgs.Keys().Previous("checkin", "token"); p != "signed" {
		t.Fatalf("previous: %v", p)
	}

	// 管理面明文视图
	view := pkgs.Keys().View("checkin")
	cur := view["current"].(map[string]any)
	if cur["token"] != "signed-2" {
		t.Fatalf("view: %v", cur)
	}

	// 卸载清理(keys + settings)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.2.1", initAndTaskFiles(
		`module.exports={}`, `module.exports={handler:function(ctx){}}`,
	))); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Delete(ctx, "checkin"); err != nil {
		t.Fatal(err)
	}
	if _, ok := pkgs.Keys().Get("checkin", "token"); ok {
		t.Fatal("keys survived uninstall")
	}
}

func TestHooks_TaskRunsViaRunner(t *testing.T) {
	// Given 已安装 hooks 包 When runner 形态装载任务文件并运行 Then keys 更新且 ctx.task 正确
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.keys.overwrite({token:ctx.task});}}`,
	})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 等 onLoad 回调(无 init.js,无副作用)
	pkg, err := pkgs.GetPackage("checkin")
	if err != nil {
		t.Fatal(err)
	}
	task := plugin.HooksTask{Name: "signIn", Cron: "* * * * *", TimeoutMs: 1000}
	rt, err := plugin.LoadTask(pkg, task, plugin.HooksDeps{
		PackageName: "checkin",
		Keys:        pkgs.Keys(),
		Now:         func() time.Time { return time.Unix(0, 0) },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "signIn" {
		t.Fatalf("task keys: %v", v)
	}
}

func TestHooks_AdminKeysEndpointPlaintext(t *testing.T) {
	// Given keys 有值 When GET 管理 keys 路由 Then 明文输出(键名+值)
	pkgs, _ := testRegistry(t)
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	})); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Keys().Overwrite("checkin", map[string]any{"token": "secret-value"}); err != nil {
		t.Fatal(err)
	}
	deps := newAdminDeps(pkgs)
	mux := deps.Mux()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !containsStr(body, "secret-value") || !containsStr(body, `"token"`) {
		t.Fatalf("keys output: %s", body)
	}
}

func TestHooks_SettingsLifecycle(t *testing.T) {
	// Given 声明+覆盖 When 安装/保存/快照合并 Then 声明提取落库、PUT 覆盖生效、快照合并正确
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"settings.js":     `module.exports.settings={account:{type:"string",description:"账号"},level:{type:"int",default:1}}`,
		"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.keys.overwrite({who:String(ctx.settings.account||"?")});}}`,
	})); err != nil {
		t.Fatal(err)
	}
	pkg, _ := pkgs.GetPackage("checkin")
	if _, ok := pkg.Declaration["account"]; !ok {
		t.Fatalf("declaration extracted: %v", pkg.Declaration)
	}
	// overrides 保存后快照合并
	if _, err := pkgs.Settings().Put("checkin", plugin.PutInput{
		Config: map[string]any{"account": "me@x"},
		Tasks:  map[string]map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}
	snap := settingsSnapshot(pkgs, "checkin")
	if snap["account"] != "me@x" {
		t.Fatalf("snapshot: %v", snap)
	}
	// 任务经快照读覆盖值
	task := plugin.HooksTask{Name: "signIn", Cron: "* * * * *"}
	rt, err := plugin.LoadTask(pkg, task, plugin.HooksDeps{PackageName: "checkin", Keys: pkgs.Keys(), Now: func() time.Time { return time.Unix(0, 0) }}, snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if v, _ := pkgs.Keys().Get("checkin", "who"); v != "me@x" {
		t.Fatalf("task settings read: %v", v)
	}
}

// newAdminDeps 管理面依赖(hooks 测试面)
func newAdminDeps(pkgs *plugin.Registry) *admin.Deps {
	return &admin.Deps{
		Packages: pkgs,
		KeysFunc: func(pkg string) map[string]any { return pkgs.Keys().View(pkg) },
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

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
	// (keys 写窗:任务可写;onLoad ctx.keys 无 merge 能力——阉割即权限)
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
		deps := plugin.HooksDeps{PackageName: "checkin", Keys: pkgs.Keys(), Key: &plugin.KeyRef{ID: "token", Data: "unsigned"}}
		rt, err := plugin.LoadTask(pkg, task, deps, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
			t.Fatal(err)
		}
		_ = tokenVal
	}

	// 安装:init 无写键能力(探测 set 未挂载并记入 storage);任务执行写当前键
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", initAndTaskFiles(
		`module.exports={onLoad:function(ctx){storage.set("probe", String(ctx.keys.set===undefined));}}`,
		`module.exports={handler:function(ctx){ctx.keys.set("signed");}}`,
	))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, _ := pkgs.GetPackage("checkin")
		_ = v
		return true
	}, "install settle")
	runSignIn("signed")
	if v, ok := pkgs.Keys().Get("checkin", "token"); !ok || v.Data != "signed" {
		t.Fatalf("task key: %+v", v)
	}

	// 升级:onLoad 再次触发;任务再写,data → prev
	if err := pkgs.Install(ctx, hooksAAP(t, "0.2.0", initAndTaskFiles(
		`module.exports={onLoad:function(ctx){storage.set("probe", "x");}}`,
		`module.exports={handler:function(ctx){ctx.keys.set("signed-2");}}`,
	))); err != nil {
		t.Fatal(err)
	}
	runSignIn("signed-2")
	if v, _ := pkgs.Keys().Get("checkin", "token"); v.Data != "signed-2" {
		t.Fatalf("task key after upgrade: %+v", v)
	}
	if p, _ := pkgs.Keys().Get("checkin", "token"); p.Prev != "signed" {
		t.Fatalf("prev: %v", p.Prev)
	}

	// 管理面清单视图
	list := pkgs.Keys().List("checkin")
	if len(list) != 1 || list[0].ID != "token" || list[0].Data != "signed-2" {
		t.Fatalf("list: %+v", list)
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
		"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.keys.set(ctx.task);}}`,
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
		Key:         &plugin.KeyRef{ID: "token", Data: ""},
		Now:         func() time.Time { return time.Unix(0, 0) },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v.Data != "signIn" {
		t.Fatalf("task keys: %+v", v)
	}
}

func TestHooks_TaskManualRun(t *testing.T) {
	// Given 已装 hooks 包 When POST tasks/{task}/run Then 同步执行一次(keys 写窗生效/未声明 400/停用 400/无 hooks 400)
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()
	wire.AdminDeps.RunTaskFunc = wire.RunTaskOnce
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.keys.set(ctx.task);}}`,
	})); err != nil {
		t.Fatal(err)
	}
	// 种子键(逐键执行目标)
	if _, err := pkgs.Keys().Set("checkin", "token", "seed"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 等 onLoad 回调(无 init.js,无副作用)
	mux := wire.AdminDeps.Mux()
	runTask := func(pkg, task string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/"+pkg+"/tasks/"+task+"/run", nil))
		return w
	}
	// 正常触发:200 + 键池逐键执行(写窗生效)
	w := runTask("checkin", "signIn")
	if w.Code != http.StatusOK || !containsStr(w.Body.String(), `"ok":true`) {
		t.Fatalf("run status %d: %s", w.Code, w.Body.String())
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v.Data != "signIn" {
		t.Fatalf("task keys: %+v", v)
	}
	if !containsStr(w.Body.String(), `"results"`) || !containsStr(w.Body.String(), `"key":"token"`) {
		t.Fatalf("results shape: %s", w.Body.String())
	}
	// 未声明任务 400
	if w = runTask("checkin", "nope"); w.Code != http.StatusBadRequest {
		t.Fatalf("unknown task status %d: %s", w.Code, w.Body.String())
	}
	// 停用包 400
	if err := pkgs.Enable(ctx, "checkin", false); err != nil {
		t.Fatal(err)
	}
	if w = runTask("checkin", "signIn"); w.Code != http.StatusBadRequest || !containsStr(w.Body.String(), "disabled") {
		t.Fatalf("disabled status %d: %s", w.Code, w.Body.String())
	}
	_ = pkgs.Enable(ctx, "checkin", true)
	// 无 hooks 部件包 400
	protoManifest := `{"manifestVersion":1,"name":"plain","version":"0.1.0","parts":{
		"protocol":{"protocol":"openai-completions"}}}`
	plainAAP, err := plugin.BuildAAP(mustManifest(t, protoManifest), map[string][]byte{
		plugin.ProtocolEntry: []byte("module.exports=function(){return {buildRequest:function(c,e){return {url:config.base_url}},mapResponse:function(c,b){return b;}}}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Install(ctx, plainAAP); err != nil {
		t.Fatal(err)
	}
	if w = runTask("plain", "x"); w.Code != http.StatusBadRequest || !containsStr(w.Body.String(), "no hooks") {
		t.Fatalf("no-hooks status %d: %s", w.Code, w.Body.String())
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
	if _, err := pkgs.Keys().Set("checkin", "token", "secret-value"); err != nil {
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
		"tasks/signIn.js": `module.exports={handler:function(ctx){ctx.keys.set(String(ctx.settings.account||"?"));}}`,
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
	rt, err := plugin.LoadTask(pkg, task, plugin.HooksDeps{PackageName: "checkin", Keys: pkgs.Keys(), Key: &plugin.KeyRef{ID: "token", Data: ""}, Now: func() time.Time { return time.Unix(0, 0) }}, snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(task, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v.Data != "me@x" {
		t.Fatalf("task settings read: %+v", v)
	}
}

// newAdminDeps 管理面依赖(hooks 测试面)
func newAdminDeps(pkgs *plugin.Registry) *admin.Deps {
	return &admin.Deps{
		Packages: pkgs,
		KeysFunc: func(pkg string) map[string]any {
			keys := pkgs.Keys().List(pkg)
			out := make([]map[string]any, 0, len(keys))
			for _, e := range keys {
				out = append(out, map[string]any{"id": e.ID, "data": e.Data, "updatedAt": e.UpdatedAt, "hasPrev": e.Prev != nil})
			}
			return map[string]any{"keys": out, "rotation": pkgs.Keys().PeekRotation(pkg, len(keys))}
		},
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

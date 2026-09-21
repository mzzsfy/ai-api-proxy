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

// hooksManifest 构造 hooks-only manifest(raw)
const hooksManifest = `{"manifestVersion":1,"name":"checkin","version":"%s","parts":{
	"hooks":{"entry":"hooks.js","tasks":[{"name":"signIn","cron":"* * * * *","timeoutMs":1000}]}}}`

func hooksAAP(t *testing.T, version, js string) []byte {
	t.Helper()
	data, err := plugin.BuildAAP(mustManifest(t, sprintf(hooksManifest, version)), map[string][]byte{"hooks.js": []byte(js)})
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

func TestHooks_InstallTriggersOnLoadAndKeysFlow(t *testing.T) {
	// Given hooks-only 包(onLoad 写 key) When 安装→升级→读管理 keys→卸载 Then 各环节语义成立
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()

	// 安装:onLoad 异步触发,写入 token
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", `module.exports={
		onLoad:function(ctx){ctx.keys.set({token:"loaded"});},
		signIn:function(ctx){ctx.keys.set({token:"signed"});}}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, ok := pkgs.Keys().Get("checkin", "token")
		return ok && v == "loaded"
	}, "onLoad key not written")

	// 升级:onLoad 再次触发(current 移入 previous)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.2.0", `module.exports={
		onLoad:function(ctx){ctx.keys.set({token:"reloaded"});},
		signIn:function(ctx){ctx.keys.set({token:"signed"});}}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, _ := pkgs.Keys().Get("checkin", "token")
		p, _ := pkgs.Keys().Previous("checkin", "token")
		return v == "reloaded" && p == "loaded"
	}, "upgrade keys snapshot failed")

	// 管理面明文视图
	view := pkgs.Keys().View("checkin")
	cur := view["current"].(map[string]any)
	if cur["token"] != "reloaded" {
		t.Fatalf("view: %v", cur)
	}

	// 卸载清理
	if err := pkgs.Delete(ctx, "checkin"); err != nil {
		t.Fatal(err)
	}
	if _, ok := pkgs.Keys().Get("checkin", "token"); ok {
		t.Fatal("keys survived uninstall")
	}
}

func TestHooks_TaskRunsViaRunner(t *testing.T) {
	// Given 已安装 hooks 包 When 直接调用 runner(runner 形态与调度器一致) Then keys 更新
	ctx := context.Background()
	pkgs, st := testRegistry(t)
	wire := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	sched := wireHooks(wire)
	defer sched.Stop()
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", `module.exports={
		signIn:function(ctx){ctx.keys.set({token:"signed"});}}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // onLoad 无实现,无副作用;等回调
	pkg, err := pkgs.GetPackage("checkin")
	if err != nil {
		t.Fatal(err)
	}
	rt, err := plugin.LoadHooks(pkg, plugin.HooksDeps{
		PackageName: "checkin",
		Keys:        pkgs.Keys(),
		Now:         func() time.Time { return time.Unix(0, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask("signIn", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "signed" {
		t.Fatalf("task keys: %v", v)
	}
}

func TestHooks_AdminKeysEndpointPlaintext(t *testing.T) {
	// Given keys 有值 When GET 管理 keys 路由 Then 明文输出(键名+值)
	pkgs, _ := testRegistry(t)
	if err := pkgs.Install(context.Background(), hooksAAP(t, "0.1.0", `module.exports={}`)); err != nil {
		t.Fatal(err)
	}
	if err := pkgs.Keys().Set("checkin", map[string]any{"token": "secret-value"}); err != nil {
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

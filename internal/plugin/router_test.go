package plugin

import (
	"strings"
	"testing"
)

// ─── router.js select 钩子运行时(BDD 场景)───

func routerTestPkg(t *testing.T, selectJS string) *Package {
	t.Helper()
	pkg := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{}}}}`,
		map[string]string{"router.js": "module.exports={select:" + selectJS + "}"})
	if err := pkg.Validate(); err != nil {
		t.Fatalf("pkg: %v", err)
	}
	return pkg
}

func newTestRouterEngine(t *testing.T, selectJS string) *RouterEngine {
	t.Helper()
	e, err := newRouterEngine(RouterDeps{Package: routerTestPkg(t, selectJS)})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	return e
}

func selKeys() []KeyEntry {
	return []KeyEntry{
		{ID: "a", Data: map[string]any{"quota": float64(10)}},
		{ID: "b", Data: map[string]any{"quota": float64(90)}},
	}
}

// 场景: select 返回合法 id = 原样交回宿主
func TestRouterSelect_ValidID(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){return input.keys[1].id}`)
	defer e.Close()
	out, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}, Request: RouterRequestInfo{Model: "m", Stream: true}})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if out.Selected != "b" {
		t.Fatalf("selected=%q; want b", out.Selected)
	}
}

// 场景: select 读到 keys.data 与 inFlight 快照(路由决策输入面)
func TestRouterSelect_InputFacts(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){
		for (var i = 0; i < input.keys.length; i++) {
			var k = input.keys[i];
			if ((input.inFlight[k.id] || 0) === 0 && k.data.quota > 50) return k.id;
		}
		return null;
	}`)
	defer e.Close()
	out, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{"b": 1}, Request: RouterRequestInfo{Model: "m"}})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if out.Selected != "" {
		t.Fatalf("selected=%q; want fallback(b in-flight, a quota low)", out.Selected)
	}
	out, err = e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}, Request: RouterRequestInfo{Model: "m"}})
	if err != nil {
		t.Fatalf("select2: %v", err)
	}
	if out.Selected != "b" {
		t.Fatalf("selected2=%q; want b", out.Selected)
	}
}

// 场景: select 返回 null/undefined = 回落(out.Selected 空,不出错)
func TestRouterSelect_NullFallback(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){return null}`)
	defer e.Close()
	out, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if out.Selected != "" {
		t.Fatalf("selected=%q; want empty", out.Selected)
	}
}

// 场景: select 抛错 = error 带原异常信息(调用方回落,请求不失败)
func TestRouterSelect_ThrowFallsBack(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){throw new Error("boom")}`)
	defer e.Close()
	_, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err=%v; want boom propagated", err)
	}
}

// 场景: select 返回非字符串 = error 回落(契约显式)
func TestRouterSelect_NonString(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){return 42}`)
	defer e.Close()
	_, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if err == nil || !strings.Contains(err.Error(), "string") {
		t.Fatalf("err=%v; want type error", err)
	}
}

// 场景: select 失败实例即弃,下次调用懒重建成功(污染不永久;恢复路径可复用)
func TestRouterSelect_FailureRebuildRecovers(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){ if (input.keys.length > 1) throw new Error("too many"); return input.keys[0].id }`)
	defer e.Close()
	if _, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}}); err == nil {
		t.Fatal("want first call error")
	}
	out, err := e.Select(RouterSelectInput{Keys: []KeyEntry{{ID: "only"}}, InFlight: map[string]int{}})
	if err != nil {
		t.Fatalf("rebuild select: %v", err)
	}
	if out.Selected != "only" {
		t.Fatalf("selected=%q; want only", out.Selected)
	}
}

// 场景: select 死循环 = 预算中断转 error(请求侧回落,不挂死)
func TestRouterSelect_TimeoutRecovers(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){while(true){}}`)
	defer e.Close()
	if _, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}}); err == nil {
		t.Fatal("want timeout error")
	}
}

// 场景: select 返回未知 id = 原样透传(宿主校验回落;引擎不做白名单)
func TestRouterSelect_UnknownIDPassthrough(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){return "nope"}`)
	defer e.Close()
	out, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if err != nil || out.Selected != "nope" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

// 场景: 未声明 router 的包 = For 返回 nil(调用方走默认 round-robin)
func TestRouterEngines_NonRouterPackage(t *testing.T) {
	pkg := handlerPkgFiles(`{"manifestVersion":1,"name":"plain","version":"1.0","parts":{"protocol":{"protocol":"openai-completions"}}}`,
		map[string]string{"protocol.js": "module.exports={}"})
	engines := NewRouterEngines(func(*Package) RouterDeps { return RouterDeps{} })
	e, err := engines.For(pkg)
	if err != nil || e != nil {
		t.Fatalf("non-router pkg: %+v %v", e, err)
	}
}

// 场景: buildRouter 失败分支 = 明确错误且不 panic(悬空 Free 回归锁;构造期急建,错误在 newRouterEngine 返回)
func TestRouterBuild_FailureBranches(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		wants []string
	}{
		{"no export", "module.exports={}", []string{"select export required"}},
		{"not function", "module.exports={select:42}", []string{"select must be a function"}},
		{"getter throw", "Object.defineProperty(module.exports,'select',{get:function(){throw new Error('boom')}})", []string{"read select"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{}}}}`,
				map[string]string{"router.js": tc.src})
			if err := pkg.Validate(); err != nil {
				t.Fatalf("pkg: %v", err)
			}
			e, err := newRouterEngine(RouterDeps{Package: pkg})
			if err == nil {
				e.Close()
				t.Fatal("want error on build failure")
			}
			for _, want := range tc.wants {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err=%q; want contains %q", err.Error(), want)
				}
			}
		})
	}
}

// 场景: select 返回空串 = 回落语义入口(引擎透传空串,宿主判空回落)
func TestRouterSelect_EmptyStringPassthrough(t *testing.T) {
	e := newTestRouterEngine(t, `function(input){return ""}`)
	defer e.Close()
	out, err := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if err != nil || out.Selected != "" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

// 场景: SelectTimeoutMs 透传生效(manifest timeoutMs → engine 预算;死循环按声明预算超时,错误消息携带实际预算锁死链路)
func TestRouterSelectTimeoutMs_Wired(t *testing.T) {
	pkg := routerTestPkg(t, `function(input){while(true){}}`)
	e, err := newRouterEngine(RouterDeps{Package: pkg, SelectTimeoutMs: 30})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer e.Close()
	_, selErr := e.Select(RouterSelectInput{Keys: selKeys(), InFlight: map[string]int{}})
	if selErr == nil {
		t.Fatal("want timeout error")
	}
	// 预算值进错误消息:30ms 声明生效(回退缺省 50ms 时该断言红)
	if !strings.Contains(selErr.Error(), "(after 30ms)") {
		t.Fatalf("err=%q; want budget 30ms wired", selErr.Error())
	}
}

// 场景: inFlight 计数 inc/dec/snapshot 宿主权威(缺失 id 防御不下溢)
func TestRouterInFlight_Accounting(t *testing.T) {
	f := newRouterInFlight()
	f.inc("a")
	f.inc("a")
	f.inc("b")
	f.dec("a")
	f.dec("ghost")
	snap := f.snapshot()
	if snap["a"] != 1 || snap["b"] != 1 || len(snap) != 2 {
		t.Fatalf("snap=%v", snap)
	}
}

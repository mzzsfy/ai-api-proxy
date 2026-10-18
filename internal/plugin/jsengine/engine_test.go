package jsengine

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Scenario: EvalString 执行源码并设置全局,GetGlobal 读回
func TestEvalStringAndGlobal(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	if err := inst.EvalString(`globalThis.__x = 41 + 1;`); err != nil {
		t.Fatalf("EvalString: %v", err)
	}
	v, err := inst.GetGlobal("__x")
	if err != nil {
		t.Fatalf("GetGlobal: %v", err)
	}
	defer v.Free()
	if got := v.String(); got != "42" {
		t.Fatalf("GetGlobal(__x) = %q, want 42", got)
	}
}

// Scenario: 编译缓存跨实例共享——NewProgram 后两个实例各自 RunProgram 成功
func TestProgramSharedAcrossInstances(t *testing.T) {
	prog, err := NewProgram("p.js", `globalThis.__fromProg = (globalThis.__fromProg || 0) + 7;`)
	if err != nil {
		t.Fatalf("NewProgram: %v", err)
	}
	for round := 0; round < 2; round++ {
		inst, err := New(Option{})
		if err != nil {
			t.Fatal(err)
		}
		if err := inst.RunProgram(prog); err != nil {
			t.Fatalf("RunProgram round %d: %v", round, err)
		}
		v, err := inst.GetGlobal("__fromProg")
		if err != nil {
			t.Fatalf("GetGlobal round %d: %v", round, err)
		}
		if got := v.String(); got != "7" {
			t.Fatalf("round %d __fromProg = %q, want 7 (新实例独立全局)", round, got)
		}
		v.Free()
		_ = inst.Close()
	}
}

// Scenario: 宿主函数注册后 JS 可调;宿主 error 以 JS 异常抛出并携带 message
func TestHostFunc(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	if err := inst.RegisterFunc("__h_sha", func(args []any) (any, error) {
		if len(args) != 1 {
			return nil, errors.New("want 1 arg")
		}
		s, _ := args[0].(string)
		return "len:" + itoa(len(s)), nil
	}); err != nil {
		t.Fatalf("RegisterFunc: %v", err)
	}
	if err := inst.EvalString(`globalThis.__r = __h_sha("abcd");`); err != nil {
		t.Fatalf("call host from js: %v", err)
	}
	v, _ := inst.GetGlobal("__r")
	defer v.Free()
	if got := v.String(); got != "len:4" {
		t.Fatalf("host result = %q, want len:4", got)
	}

	// 宿主错误 → JS 异常 → EngineError
	if err := inst.EvalString(`__h_sha(1, 2, 3);`); err == nil {
		t.Fatal("host error should surface as Eval error")
	} else if !strings.Contains(err.Error(), "want 1 arg") {
		t.Fatalf("host error message lost: %v", err)
	}
}

// Scenario: async 钩子返回 Promise,宿主 Await 等 settled;reject 转为 errRejected
func TestAwaitPromise(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	// 非 Promise 直通:先 Call 再 Await
	if err := inst.EvalString(`globalThis.__sync = function () { return "plain"; };`); err != nil {
		t.Fatal(err)
	}
	fn, err := inst.GetGlobal("__sync")
	if err != nil {
		t.Fatal(err)
	}
	defer fn.Free()
	sres, err := inst.Call(fn)
	if err != nil {
		t.Fatalf("Call sync: %v", err)
	}
	out, wasP, rerr := inst.Await(sres, 250*time.Millisecond)
	if rerr != nil {
		t.Fatalf("Await sync: %v", rerr)
	}
	if wasP {
		t.Fatal("sync fn must not be detected as promise")
	}
	defer out.Free()
	if out.String() != "plain" {
		t.Fatalf("sync result = %q", out.String())
	}

	// async 钩子:宿主等 settled
	if err := inst.EvalString(`globalThis.__asyncFn = async function () { return "async-" + (await 41+1); };`); err != nil {
		t.Fatal(err)
	}
	afn, err := inst.GetGlobal("__asyncFn")
	if err != nil {
		t.Fatal(err)
	}
	defer afn.Free()
	res, err := inst.Call(afn)
	if err != nil {
		t.Fatalf("Call async: %v", err)
	}
	defer res.Free()
	settled, wasP, rerr := inst.Await(res, 250*time.Millisecond)
	if rerr != nil {
		t.Fatalf("Await promise: %v", rerr)
	}
	if !wasP {
		t.Fatal("async fn must be detected as promise")
	}
	defer settled.Free()
	if got := settled.String(); got != "async-42" {
		t.Fatalf("settled = %q, want async-42", got)
	}

	// reject 路径
	if err := inst.EvalString(`globalThis.__rejFn = async function () { throw new Error("rej-boom"); };`); err != nil {
		t.Fatal(err)
	}
	rfn, err := inst.GetGlobal("__rejFn")
	if err != nil {
		t.Fatal(err)
	}
	defer rfn.Free()
	rres, err := inst.Call(rfn)
	if err != nil {
		t.Fatalf("Call rej: %v", err)
	}
	defer rres.Free()
	rSettled, _, rerr := inst.Await(rres, 250*time.Millisecond)
	if rerr == nil {
		t.Fatal("rejection must surface")
	}
	var rej errRejected
	if !errors.As(rerr, &rej) {
		t.Fatalf("rejection must be errRejected, got %T", rerr)
	}
	defer rej.val.Free()
	_ = rSettled
	ee := inst.ExceptionOf(rej.val)
	if ee.Message != "rej-boom" {
		t.Fatalf("rejection message = %q, want rej-boom", ee.Message)
	}
}

// Scenario: 内存超限 → KindMemory 错误;实例关闭后重建可服务
func TestMemoryLimit(t *testing.T) {
	inst, err := New(Option{MemoryLimit: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	if err := inst.EvalString(`var s=""; for (var i=0;i<64;i++){ s += new Array(1024*1024).join("x"); }`); err == nil {
		t.Fatal("OOM must fail")
	} else {
		ee, ok := err.(*EngineError)
		if !ok {
			t.Fatalf("want *EngineError, got %T: %v", err, err)
		}
		if ee.Kind != KindMemory {
			t.Fatalf("kind = %v, want KindMemory (%v)", ee.Kind, ee.Message)
		}
	}
	_ = inst.Close()

	reborn, err := New(Option{MemoryLimit: 8 * 1024 * 1024})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	defer func() { _ = reborn.Close() }()
	if err := reborn.EvalString(`globalThis.__ok = 1;`); err != nil {
		t.Fatalf("reborn instance must serve: %v", err)
	}
}

// Scenario: 死循环钩子超预算 → 中断错误(KindInterrupt)
func TestInterruptOnBudget(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	if err := inst.EvalString(`globalThis.__spin = function () { for (;;) {} };`); err != nil {
		t.Fatal(err)
	}
	fn, err := inst.GetGlobal("__spin")
	if err != nil {
		t.Fatal(err)
	}
	defer fn.Free()
	if err := inst.SetBudget(120 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	_, err = inst.Call(fn)
	if err == nil {
		t.Fatal("runaway call must be interrupted")
	}
	ee, ok := err.(*EngineError)
	if !ok {
		t.Fatalf("want *EngineError, got %T: %v", err, err)
	}
	if ee.Kind != KindInterrupt {
		t.Fatalf("kind = %v, want KindInterrupt (%v)", ee.Kind, ee.Message)
	}
}

// Scenario: CJS 内 throw 的错误携带行号堆栈(小白可回贴)
func TestErrorStackHasLine(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	const src = `"use strict";
module.exports = {
  boom: function () {
    throw new Error("stack-boom");
  }
};`
	// CJS 声明由宿主注入(不进源码文本),保证错误行号=源文件行号
	exports, err := inst.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer exports.Free()
	module, err := inst.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer module.Free()
	if err := inst.SetProp(module, "exports", exports); err != nil {
		t.Fatal(err)
	}
	if err := inst.SetGlobal("module", module); err != nil {
		t.Fatal(err)
	}
	if err := inst.EvalString(src); err != nil {
		t.Fatal(err)
	}
	m, err := inst.GetGlobal("module")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Free()
	exportsV, err := inst.GetProp(m, "exports")
	if err != nil {
		t.Fatal(err)
	}
	defer exportsV.Free()
	boom, err := inst.GetProp(exportsV, "boom")
	if err != nil {
		t.Fatal(err)
	}
	defer boom.Free()
	isFn, err := inst.IsFunction(boom)
	if err != nil || !isFn {
		t.Fatalf("boom must be function (isFn=%v err=%v)", isFn, err)
	}
	_, err = inst.Call(boom)
	if err == nil {
		t.Fatal("boom must throw")
	}
	ee, ok := err.(*EngineError)
	if !ok {
		t.Fatalf("want *EngineError, got %T: %v", err, err)
	}
	t.Logf("engine error: %q stack=%q", ee.Message, ee.Stack)
	if !strings.Contains(ee.Message, "stack-boom") {
		t.Fatalf("message lost: %q", ee.Message)
	}
	// 行号:throw 位于源第 4 行(CJS 声明由宿主注入,无偏移)
	if !strings.Contains(ee.Stack, ":4:") {
		t.Fatalf("stack must carry source line 4, got: %q", ee.Stack)
	}
}

// Scenario: 钩子参数为对象/字符串混合,对象属性可在 JS 侧读取
func TestCallWithObjectArgs(t *testing.T) {
	inst, err := New(Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close() }()

	if err := inst.EvalString(`globalThis.__pick = function (ctx, body) { return ctx.vars.model + ":" + body; };`); err != nil {
		t.Fatal(err)
	}
	fn, err := inst.GetGlobal("__pick")
	if err != nil {
		t.Fatal(err)
	}
	defer fn.Free()

	ctx, err := inst.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Free()
	vars, err := inst.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer vars.Free()
	if err := inst.SetProp(vars, "model", "gpt-x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.SetProp(ctx, "vars", vars); err != nil {
		t.Fatal(err)
	}
	out, err := inst.Call(fn, ctx, "hello")
	if err != nil {
		t.Fatalf("Call: kind=%v msg=%q stack=%q", func() any {
			if ee, ok := err.(*EngineError); ok {
				return ee.Kind
			}
			return "?"
		}(), err.Error(), func() string {
			if ee, ok := err.(*EngineError); ok {
				return ee.Stack
			}
			return ""
		}())
	}
	defer out.Free()
	if got := out.String(); got != "gpt-x:hello" {
		t.Fatalf("result = %q, want gpt-x:hello", got)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

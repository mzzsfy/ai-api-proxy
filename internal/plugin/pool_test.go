package plugin

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// fastProtoSrc 快速 hook
const fastProtoSrc = `module.exports = { buildRequest: function (ctx) {
	return { url: "https://x", method: "POST", headers: {}, body: "{}" };
}, mapResponse: function (ctx, b) { return b; } };`

// busyProtoSrc 忙循环 JS(单次超 250ms 预算 → 实例污染)
const busyProtoSrc = `module.exports = { buildRequest: function (ctx) {
	var t = Date.now(); while (Date.now() - t < 400) {}
	return { url: "https://x", method: "POST", headers: {}, body: "{}" };
}, mapResponse: function (ctx, b) { return b; } };`

// mustProto 实例化池化协议(自动注册 ClosePools 清理)
func mustProto(t *testing.T, name, src, poolSize string) *jsProtocol {
	t.Helper()
	pkg, err := ParseAAP(buildAAP(t, `{"manifestVersion":1,"name":"`+name+`","version":"1","parts":{
		"protocol":{"protocol":"openai-completions"}}}`, map[string]string{ProtocolEntry: src}))
	if err != nil {
		t.Fatal(err)
	}
	if poolSize != "" {
		t.Setenv(PoolEnvVar, poolSize)
	}
	p, err := NewProtocol(pkg, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	gp := p.(*jsProtocol)
	t.Cleanup(gp.ClosePools)
	return gp
}

func TestPool_ConcurrencyNoExhaustion(t *testing.T) {
	// Given 池大小=4 快速协议 When 32 并发 BuildRequest Then 全部成功
	proto := mustProto(t, "fast", fastProtoSrc, "4")
	const n = 32
	var wg sync.WaitGroup
	var failCount atomic.Int64
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := proto.BuildRequest(newProtoCtx(), []byte(`{}`)); err != nil {
				failCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if failCount.Load() != 0 {
		t.Fatalf("failed borrows: %d", failCount.Load())
	}
}

func TestPool_BusyHookTimesOut(t *testing.T) {
	// Given 忙循环 hook(400ms > 250ms 预算)When BuildRequest Then 超时错误
	proto := mustProto(t, "slow", busyProtoSrc, "1")
	if _, err := proto.BuildRequest(newProtoCtx(), []byte(`{}`)); err == nil {
		t.Fatal("expect timeout error")
	}
}

func TestPool_PoisonedInstanceReplaced(t *testing.T) {
	// Given 归还 broken 实例 Then 下次借到弹性新建的实例(工厂再调用断言)
	var factoryCalls atomic.Int64
	p := newRuntimePool("t", 1, QueueTimeout, func() (*hookInstance, error) {
		n := factoryCalls.Add(1)
		return &hookInstance{id: n, hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	inst1, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if factoryCalls.Load() != 1 {
		t.Fatalf("first borrow must build once, got %d", factoryCalls.Load())
	}
	p.Return(inst1, true) // 污染丢弃 → 下次弹性新建
	inst2, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Return(inst2, false)
	if inst2.id == inst1.id {
		t.Fatal("broken instance not replaced")
	}
}

func TestPool_NoWarmup(t *testing.T) {
	// Given 构造池 When 未借用 Then 工厂零调用(不预热,省内存)
	var factoryCalls atomic.Int64
	p := newRuntimePool("close-leak", 4, QueueTimeout, func() (*hookInstance, error) {
		factoryCalls.Add(1)
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	if factoryCalls.Load() != 0 {
		t.Fatalf("no warmup expected, built %d", factoryCalls.Load())
	}
}

func TestPool_ElasticAfterIdleWindow(t *testing.T) {
	// Given 空池 When 首次借用(空闲窗超时)Then 弹性新建恰好一个
	var factoryCalls atomic.Int64
	p := newRuntimePool("close-leak", 4, QueueTimeout, func() (*hookInstance, error) {
		factoryCalls.Add(1)
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	inst, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Return(inst, false)
	if factoryCalls.Load() != 1 {
		t.Fatalf("elastic build once, got %d", factoryCalls.Load())
	}
}

func TestPool_ReuseWithinWindow(t *testing.T) {
	// Given 池空且 10ms 窗内有归还 When 借用 Then 复用归还实例(不新建)
	var factoryCalls atomic.Int64
	p := newRuntimePool("close-leak", 4, QueueTimeout, func() (*hookInstance, error) {
		factoryCalls.Add(1)
		return &hookInstance{id: factoryCalls.Load(), hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	inst1, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(3 * time.Millisecond)
		p.Return(inst1, false)
	}()
	inst2, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Return(inst2, false)
	if inst2.id != inst1.id {
		t.Fatalf("window reuse expected: got id %d want %d", inst2.id, inst1.id)
	}
	if factoryCalls.Load() != 1 {
		t.Fatalf("reuse must not build, built %d", factoryCalls.Load())
	}
}

func TestPool_ShrinkOnReturnFull(t *testing.T) {
	// Given 常驻上限 1、并发借出 2 When 依次归还 Then 池内只留 1(临时实例弃置缩容)
	p := newRuntimePool("t", 1, QueueTimeout, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	a, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Return(a, false)
	p.Return(b, false)
	if len(p.instances) != 1 {
		t.Fatalf("resident after shrink: %d", len(p.instances))
	}
}

func TestPool_VMGateQueues(t *testing.T) {
	// Given 单插件 VM 预算占满 When 借用 Then 轮询至排队上界报池耗尽
	pkg := "capped"
	for range PerPkgVMCap {
		if !vmBudgets.tryAcquire(pkg) {
			t.Fatal("acquire until cap")
		}
	}
	t.Cleanup(func() {
		for range PerPkgVMCap {
			vmBudgets.release(pkg)
		}
	})
	p := newRuntimePool(pkg, 1, 60*time.Millisecond, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	if _, err := p.Borrow(context.Background()); !errors.Is(err, ErrPoolTimeout) {
		t.Fatalf("vm-gated borrow: %v", err)
	}
}

func TestVMBudget(t *testing.T) {
	// Given 双闸预算 When 单包达 64 顶 Then 拒;全局 128 顶拒;归还后可再用
	b := &vmBudget{perPkg: map[string]int{}}
	for range PerPkgVMCap {
		if !b.tryAcquire("a") {
			t.Fatalf("acquire within per-pkg cap failed at %d", b.perPkg["a"])
		}
	}
	if b.tryAcquire("a") {
		t.Fatal("per-pkg cap must reject")
	}
	if !b.tryAcquire("b") {
		t.Fatal("other package unaffected")
	}
	b2 := &vmBudget{perPkg: map[string]int{}}
	// 全局顶须跨包触达(单包 64 顶先挡)
	for i := range GlobalVMCap {
		key := "x"
		if i >= PerPkgVMCap {
			key = "y"
		}
		if !b2.tryAcquire(key) {
			t.Fatalf("acquire within global cap failed at %d", i)
		}
	}
	if b2.tryAcquire("z") {
		t.Fatal("global cap must reject")
	}
	b2.release("x")
	if !b2.tryAcquire("z") {
		t.Fatal("release must free budget")
	}
}

func TestPool_CloseWaitsInFlight(t *testing.T) {
	// Given 在途借用 When Close Then 阻塞至归还后才完成;关闭后借用拒绝
	p := newRuntimePool("t", 1, QueueTimeout, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	inst, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close finished while in-flight")
	case <-time.After(80 * time.Millisecond):
	}
	p.Return(inst, false)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after return")
	}
	if _, err := p.Borrow(context.Background()); !errors.Is(err, errPoolClosed) {
		t.Fatalf("borrow after close: %v", err)
	}
}

// 编译引用(池耗尽 → pipeline.ErrPoolBusy 映射在 borrowWrap)
var _ = pipeline.ErrPoolBusy

func TestPool_CloseReleasesBudget(t *testing.T) {
	// Given 池内空闲实例与在途借用 When Close Then 排空常驻 + 预算归零(账目无泄漏)
	p := newRuntimePool("close-leak", 4, QueueTimeout, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	inst, err := p.Borrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Return(inst, false)
	p.Close()
	if _, perPkg := budgetSnapshot("close-leak"); perPkg != 0 {
		t.Fatalf("budget leaked after close: %d", perPkg)
	}
}

func TestPool_CtxCancelInterruptsQueue(t *testing.T) {
	// Given 单插件预算占满 When ctx 取消 Then 借用立即中断(不等排队上界)
	pkg := "cancel"
	for range PerPkgVMCap {
		if !vmBudgets.tryAcquire(pkg) {
			t.Fatal("acquire until cap")
		}
	}
	t.Cleanup(func() {
		for range PerPkgVMCap {
			vmBudgets.release(pkg)
		}
	})
	p := newRuntimePool(pkg, 1, time.Minute, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := p.Borrow(ctx); err == nil {
		t.Fatal("expect cancel error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancel must interrupt queue, took %v", elapsed)
	}
}

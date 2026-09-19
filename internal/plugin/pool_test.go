package plugin

import (
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
func mustProto(t *testing.T, name, src, poolSize string) *gojaProtocol {
	t.Helper()
	pkg, err := ParseAAP(buildAAP(t, `{"manifestVersion":1,"name":"`+name+`","version":"1","parts":{
		"protocol":{"entry":"p.js","form":["non_streaming"]}}}`, map[string]string{"p.js": src}))
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
	gp := p.(*gojaProtocol)
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
	// Given 池大小=1 When 归还 broken 实例 Then 下次借到工厂补建的新实例(工厂再调用断言)
	var factoryCalls atomic.Int64
	p, err := newRuntimePool(1, QueueTimeout, func() (*hookInstance, error) {
		n := factoryCalls.Add(1)
		return &hookInstance{id: n, hooks: &Hooks{}, cursor: &TargetCursor{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	inst1, err := p.Borrow()
	if err != nil {
		t.Fatal(err)
	}
	p.Return(inst1, true) // 污染丢弃 → 懒补建
	deadline := time.Now().Add(time.Second)
	var inst2 *hookInstance
	for time.Now().Before(deadline) {
		if inst2, err = p.Borrow(); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer p.Return(inst2, false)
	if inst2.id == inst1.id {
		t.Fatal("broken instance not replaced")
	}
}

func TestPool_CloseWaitsInFlight(t *testing.T) {
	// Given 在途借用 When Close Then 阻塞至归还后才完成;关闭后借用拒绝
	p, err := newRuntimePool(1, QueueTimeout, func() (*hookInstance, error) {
		return &hookInstance{hooks: &Hooks{}, cursor: &TargetCursor{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	inst, err := p.Borrow()
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
	if _, err := p.Borrow(); !errors.Is(err, errPoolClosed) {
		t.Fatalf("borrow after close: %v", err)
	}
}

// 编译引用(池耗尽 → pipeline.ErrPoolBusy 映射在 borrowWrap)
var _ = pipeline.ErrPoolBusy

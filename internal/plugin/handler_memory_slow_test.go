//go:build slow

package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestHandlerMemoryLimitUnderLoad 恶意 handler 反复逼近内存上限
// Given 96MB 实例上限 When 并发满载交替执行超限/正常调用 Then 超限调用报错、正常调用持续成功(实例弃置重建),进程堆稳定
func TestHandlerMemoryLimitUnderLoad(t *testing.T) {
	runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(runtime.NumCPU())

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	// 恶意分支按 params.mode 分流:gorge = 分配超限字符串;ok = 正常应答
	plugin := strings.Replace(`module.exports = async function(ctx) {
	if (ctx.params.mode === "gorge") {
		let junk = "";
		for (let i = 0; i < 64; i++) { junk += "0123456789abcdef".repeat(1024*1024); }
		ctx.reply.json(200, junk.length);
		return;
	}
	const r = await ctx.http.request({url: "UPURL", method: "GET"});
	ctx.reply.json(r.status, r.body);
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, plugin)
	engine, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const rounds = 40
	for i := 0; i < rounds; i++ {
		mode := "gorge"
		if i%2 == 1 {
			mode = "ok"
		}
		err := engine.Execute(context.Background(), execInMode("mem-stress", mode), sink)
		if mode == "gorge" && err == nil {
			t.Fatalf("round %d: gorge must fail", i)
		}
		if mode == "ok" && err != nil {
			t.Fatalf("round %d: ok must succeed after prior break: %v", i, err)
		}
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	// JS 堆在 quickjs 侧(malloc),Go 侧只防自身泄漏(goroutine/缓冲);有符号差防无符号下溢
	var grown int64
	if after.Sys >= before.Sys {
		grown = int64(after.Sys - before.Sys)
	}
	if grown > 512*1024*1024 {
		t.Fatalf("process sys grew %dMB across %d rounds", grown/(1024*1024), rounds)
	}
}

// 并发满载:gorge/ok 混合并发,终态无 panic、正常调用存活
func TestHandlerMemoryLimitConcurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(runtime.NumCPU())

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	plugin := strings.Replace(`module.exports = async function(ctx) {
	if (ctx.params.mode === "gorge") {
		let junk = "";
		for (let i = 0; i < 64; i++) { junk += "0123456789abcdef".repeat(1024*1024); }
		ctx.reply.json(200, junk.length);
		return;
	}
	const r = await ctx.http.request({url: "UPURL", method: "GET"});
	ctx.reply.json(r.status, r.body);
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, plugin)
	engine, _, _ := testEngine(t, pkg, testTransport())

	const workers = 4
	const perWorker = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				mode := "gorge"
				if i%2 == 1 {
					mode = "ok"
				}
				if err := engine.Execute(context.Background(), execInMode("mem-conc", mode), &fakeSink{}); err != nil && mode == "ok" {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
	}
}

// execInMode 构造携带 mode 参数的执行输入
func execInMode(model, mode string) HandlerInput {
	in := execIn(model, false, "k1")
	in.Params = map[string]any{"mode": mode}
	return in
}

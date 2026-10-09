package server

import (
	"context"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// 场景: RSS 达软限——丢全部池+拒新;回落后池重建开放
func TestWatchdog_SoftLimit(t *testing.T) {
	engines := plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps { return plugin.HandlerDeps{} })
	rss := uint64(100)
	wd := NewWatchdog(engines, 1000, 0, false, func() uint64 { return rss })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wd.Run(ctx)
	time.Sleep(watchdogPoll + 100*time.Millisecond)
	if engines.RejectNew() {
		t.Fatal("pre-trip reject")
	}
	rss = 1500 // 越软限
	time.Sleep(watchdogPoll + 100*time.Millisecond)
	if !engines.RejectNew() {
		t.Fatal("soft limit must reject new")
	}
	rss = 100 // 回落
	time.Sleep(watchdogPoll + 100*time.Millisecond)
	if engines.RejectNew() {
		t.Fatal("back under must reopen")
	}
}

// 场景: 硬限(独立进程部署)——进程退出
func TestWatchdog_HardLimit(t *testing.T) {
	engines := plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps { return plugin.HandlerDeps{} })
	exited := make(chan int, 1)
	old := exitProcess
	exitProcess = func(code int) { exited <- code }
	t.Cleanup(func() { exitProcess = old })
	rss := uint64(0)
	wd := NewWatchdog(engines, 100, 150, true, func() uint64 { return rss })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wd.Run(ctx)
	rss = 200 // 越硬限
	select {
	case code := <-exited:
		if code != 1 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(watchdogPoll*2 + time.Second):
		t.Fatal("hard limit did not exit")
	}
}

// 场景: 硬限但库形态嵌入宿主——不退出
func TestWatchdog_HardLimitEmbedded(t *testing.T) {
	engines := plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps { return plugin.HandlerDeps{} })
	exited := make(chan int, 1)
	old := exitProcess
	exitProcess = func(code int) { exited <- code }
	t.Cleanup(func() { exitProcess = old })
	rss := uint64(0)
	wd := NewWatchdog(engines, 100, 150, false, func() uint64 { return rss })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wd.Run(ctx)
	rss = 1000
	select {
	case code := <-exited:
		t.Fatalf("embedded must not exit (got %d)", code)
	case <-time.After(watchdogPoll + 300*time.Millisecond):
	}
	if !engines.RejectNew() {
		t.Fatal("soft trip still applies")
	}
}

// 场景: 软限关闭(≤0)——看门狗空转退出
func TestWatchdog_Disabled(t *testing.T) {
	engines := plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps { return plugin.HandlerDeps{} })
	wd := NewWatchdog(engines, 0, 0, false, nil)
	done := make(chan struct{})
	go func() { wd.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled watchdog must return immediately")
	}
}

package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestEvictGate_限流 令牌桶耗尽后拒绝,并发位归还后恢复
func TestEvictGate_限流(t *testing.T) {
	gate := newEvictGate()
	calls := 0
	forward := func(ctx context.Context, transport, scope, value string) error {
		calls++
		return nil
	}
	evict := gatedEvict(gate, forward)
	// 桶容量内全部放行
	for i := 0; i < pluginEvictBurst; i++ {
		if err := evict("t", "egress", "1.2.3.4"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	// 桶耗尽:拒绝
	if err := evict("t", "egress", "1.2.3.4"); err == nil {
		t.Fatal("want rate limited")
	}
	// 补一个令牌的时长后恢复
	time.Sleep(time.Duration(pluginEvictRefillSec*int(time.Second)) + 10*time.Millisecond)
	if err := evict("t", "egress", "1.2.3.4"); err != nil {
		t.Fatalf("after refill: %v", err)
	}
	if calls != pluginEvictBurst+1 {
		t.Fatalf("calls=%d", calls)
	}
}

// TestEvictGate_错误透传 节点失败错误原样返回(不吞)
func TestEvictGate_错误透传(t *testing.T) {
	gate := newEvictGate()
	want := errors.New("node unreachable")
	evict := gatedEvict(gate, func(ctx context.Context, transport, scope, value string) error {
		return want
	})
	if err := evict("t", "egress", "1.2.3.4"); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}

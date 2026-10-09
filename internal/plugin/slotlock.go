// 槽锁:进程内 per-slot 互斥,FIFO 唤醒,等待有界且可取消摘除,所有权凭证防重复释放
package plugin

import (
	"errors"
	"sync"
	"time"
)

// ErrSlotWaitTimeout 槽锁等待超预算
var ErrSlotWaitTimeout = errors.New("slot wait timeout")

// ErrSlotCancelled 等待期间执行被取消
var ErrSlotCancelled = errors.New("slot wait cancelled")

// lockTicket 锁所有权凭证;release 仅对当前所有者生效,防重复释放与终局释放竞态
type lockTicket struct {
	consumed bool
}

// slotWaiter 单个排队者;ticket 在授予时写入,granted 关闭即获锁,abandoned 标记弃权
type slotWaiter struct {
	granted   chan struct{}
	ticket    *lockTicket
	abandoned bool
}

// SlotLocks 槽锁管理器(宿主进程内,与③同生命周期)
type SlotLocks struct {
	mu     sync.Mutex
	slots  map[string][]*slotWaiter
	owners map[string]*lockTicket
}

// NewSlotLocks 构造
func NewSlotLocks() *SlotLocks {
	return &SlotLocks{slots: map[string][]*slotWaiter{}, owners: map[string]*lockTicket{}}
}

// Acquire 取锁;wait = 等待上界,done = 执行取消信号(可 nil)
// 返回 release 携带所有权凭证,重复调用/终局释放后调用均为无害 no-op
func (l *SlotLocks) Acquire(slot string, wait time.Duration, done <-chan struct{}) (func(), error) {
	ticket, released := l.lockFree(slot)
	if released {
		l.mu.Unlock()
		return func() { l.release(slot, ticket) }, nil
	}
	w := &slotWaiter{granted: make(chan struct{})}
	l.slots[slot] = append(l.slots[slot], w)
	l.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-w.granted:
		return func() { l.release(slot, w.ticket) }, nil
	case <-done:
		if l.abandon(slot, w) {
			return nil, ErrSlotCancelled
		}
		<-w.granted // 弃权登记前已被授予:按获锁处理
		return func() { l.release(slot, w.ticket) }, nil
	case <-timer.C:
		if l.abandon(slot, w) {
			return nil, ErrSlotWaitTimeout
		}
		<-w.granted
		return func() { l.release(slot, w.ticket) }, nil
	}
}

// lockFree 快速路径:槽空闲则发凭证占锁(mu 保持锁定由调用方释放),返回是否立即获得
func (l *SlotLocks) lockFree(slot string) (*lockTicket, bool) {
	l.mu.Lock()
	if _, held := l.owners[slot]; !held {
		t := &lockTicket{}
		l.owners[slot] = t
		return t, true
	}
	return nil, false
}

// abandon 弃权摘除;false = 已被授予(摘除失败)
func (l *SlotLocks) abandon(slot string, w *slotWaiter) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.slots[slot] {
		if x == w {
			if x.abandoned {
				return false
			}
			x.abandoned = true
			l.slots[slot] = append(l.slots[slot][:i], l.slots[slot][i+1:]...)
			return true
		}
	}
	return false
}

// release 凭证核对后放锁;队列非空则凭证明手转移给队首(FIFO)
func (l *SlotLocks) release(slot string, t *lockTicket) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owners[slot] != t || t.consumed {
		return
	}
	t.consumed = true
	if q := l.slots[slot]; len(q) > 0 {
		w := q[0]
		l.slots[slot] = q[1:]
		nt := &lockTicket{}
		w.ticket = nt
		l.owners[slot] = nt
		close(w.granted)
		return
	}
	delete(l.owners, slot)
	delete(l.slots, slot)
}

// Held 槽锁定判定(写封闭检查用)
func (l *SlotLocks) Held(slot string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	t, held := l.owners[slot]
	return held && !t.consumed
}

// ReleaseAll 终局全量释放:强制消费该执行全部所有权并按 FIFO 转移队列
// 前置:调用时该执行已终局,不存在同组并发 Acquire/Release
func (l *SlotLocks) ReleaseAll() {
	l.mu.Lock()
	slots := make([]string, 0, len(l.owners))
	for s, t := range l.owners {
		if !t.consumed {
			slots = append(slots, s)
		}
	}
	l.mu.Unlock()
	for _, s := range slots {
		l.releaseForced(s)
	}
}

// releaseForced 无凭证强制放锁(仅终局释放路径使用)
func (l *SlotLocks) releaseForced(slot string) {
	l.mu.Lock()
	t := l.owners[slot]
	l.mu.Unlock()
	if t != nil {
		l.release(slot, t)
	}
}

// Waiting 槽上排队者数(诊断/测试观察)
func (l *SlotLocks) Waiting(slot string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.slots[slot])
}

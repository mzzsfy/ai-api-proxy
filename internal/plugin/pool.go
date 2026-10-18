package plugin

import (
	"errors"
	"sync"
	"time"
)

// QueueTimeout 借用排队预算(超时=池耗尽)
const QueueTimeout = 5 * time.Second

// errPoolClosed 池已关闭(Reload 销毁后借用)
var errPoolClosed = errors.New("runtime pool closed")

// ErrPoolTimeout 借用排队超时(适配层映射 pipeline.ErrPoolBusy)
var ErrPoolTimeout = errors.New("runtime pool queue timeout")

// runtimePool hook 粒度借还池;poisoned 实例归还时丢弃,懒补建
type runtimePool struct {
	instances chan *hookInstance
	factory   func() (*hookInstance, error)
	queue     time.Duration
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup // 在途借用(含排队中)
}

// newRuntimePool 预热 size 个实例
func newRuntimePool(size int, queue time.Duration, factory func() (*hookInstance, error)) (*runtimePool, error) {
	p := &runtimePool{instances: make(chan *hookInstance, size), factory: factory, queue: queue}
	for range size {
		inst, err := factory()
		if err != nil {
			return nil, err
		}
		p.instances <- inst
	}
	return p, nil
}

// Borrow 取实例(排队 p.queue;关闭后拒绝)
func (p *runtimePool) Borrow() (*hookInstance, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	select {
	case inst := <-p.instances:
		return inst, nil
	case <-time.After(p.queue):
		p.wg.Done()
		return nil, ErrPoolTimeout
	}
}

// Return 归还;broken(内存超限/中断/引擎错误污染)丢弃关闭并懒补建
func (p *runtimePool) Return(inst *hookInstance, broken bool) {
	defer p.wg.Done()
	if broken {
		inst.closeInstance()
		go p.refill()
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		inst.closeInstance() // 关闭中的在途归还:直接销毁
		return
	}
	select {
	case p.instances <- inst:
	default:
		inst.closeInstance()
	}
}

// refill 补建一个实例(失败=池缩容,待下次补建)
func (p *runtimePool) refill() {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return
	}
	inst, err := p.factory()
	if err != nil {
		return
	}
	select {
	case p.instances <- inst:
	default:
	}
}

// Close 停新借出并等待在途归零(阻塞;调用方异步触发)
func (p *runtimePool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
}

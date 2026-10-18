package plugin

import (
	"context"
	"errors"
	"sync"
	"time"
)

// QueueTimeout 弹性达顶后的排队预算(超时=池耗尽)
const QueueTimeout = 5 * time.Second

// pollStep VM 预算达顶后的轮询步长(唤醒延迟上界;独立于空闲等待窗)
const pollStep = 10 * time.Millisecond

// PerPkgVMCap 单插件 VM 存活上限(所有部件合计)
const PerPkgVMCap = 64

// GlobalVMCap 进程 VM 存活上限(所有插件合计)
const GlobalVMCap = 128

// vmBudget 进程级 VM 存活预算(工厂产出前占用,销毁后归还;约束内存与宿主函数注册的进程级上限)
// 装载期探测实例(NewProtocol/NewFilter 的 first)账外:串行装载 + 即时销毁,不占不还
type vmBudget struct {
	mu     sync.Mutex
	global int
	perPkg map[string]int
}

// vmBudgets 单例(VM 上限天然进程级)
var vmBudgets = &vmBudget{perPkg: map[string]int{}}

// tryAcquire 双闸都未达顶才占用
func (b *vmBudget) tryAcquire(pkg string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.global >= GlobalVMCap || b.perPkg[pkg] >= PerPkgVMCap {
		return false
	}
	b.global++
	b.perPkg[pkg]++
	return true
}

// release 归还预算;空键回收防 map 无界;下溢 = 配对破坏,fail-fast 对齐 wg 双 Done
func (b *vmBudget) release(pkg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.global <= 0 {
		panic("vm budget underflow: release without acquire (global)")
	}
	if b.perPkg[pkg] <= 0 {
		panic("vm budget underflow: release without acquire (pkg " + pkg + ")")
	}
	b.global--
	if b.perPkg[pkg]--; b.perPkg[pkg] <= 0 {
		delete(b.perPkg, pkg)
	}
}

// budgetSnapshot 测试观测(锁内读双闸计数)
func budgetSnapshot(pkg string) (global, perPkg int) {
	vmBudgets.mu.Lock()
	defer vmBudgets.mu.Unlock()
	return vmBudgets.global, vmBudgets.perPkg[pkg]
}

// errPoolClosed 池已关闭(Reload 销毁后借用)
var errPoolClosed = errors.New("runtime pool closed")

// ErrPoolTimeout 借用排队超时(适配层映射 pipeline.ErrPoolBusy)
var ErrPoolTimeout = errors.New("runtime pool queue timeout")

// runtimePool hook 粒度借还池:不预热;空闲窗内复用归还,超窗按 VM 预算弹性新建,归还池满即弃(自动缩容)
type runtimePool struct {
	pkg       string
	instances chan *hookInstance // 常驻上限 = 容量(warm)
	factory   func() (*hookInstance, error)
	queue     time.Duration // 弹性达顶后的排队上界
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup // 在途借用(含等待窗/弹性构建/排队)
}

// newRuntimePool 构造(不预热;warm=常驻上限)
func newRuntimePool(pkg string, warm int, queue time.Duration, factory func() (*hookInstance, error)) *runtimePool {
	return &runtimePool{pkg: pkg, instances: make(chan *hookInstance, warm), factory: factory, queue: queue}
}

// Borrow 取实例:空闲窗内复用;超窗按 VM 预算弹性新建(达顶轮询至排队上界);ctx 取消即中断;关闭后拒绝
// wg 计在途借用,成功借出的 Done 转移到 Return
func (p *runtimePool) Borrow(ctx context.Context) (*hookInstance, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	fail := func(err error) (*hookInstance, error) {
		p.wg.Done()
		return nil, err
	}
	wait := time.NewTimer(poolWait())
	defer wait.Stop()
	select {
	case inst := <-p.instances:
		return inst, nil
	case <-ctx.Done():
		return fail(ctx.Err())
	case <-wait.C:
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return fail(errPoolClosed)
	}
	deadline := time.NewTimer(p.queue)
	defer deadline.Stop()
	tick := time.NewTicker(pollStep)
	defer tick.Stop()
	for {
		if vmBudgets.tryAcquire(p.pkg) {
			inst, err := p.factory()
			if err != nil {
				vmBudgets.release(p.pkg)
				return fail(err)
			}
			return inst, nil
		}
		select {
		case inst := <-p.instances:
			return inst, nil
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-tick.C:
		case <-deadline.C:
			return fail(ErrPoolTimeout)
		}
	}
}

// Return 归还;broken(内存超限/中断/引擎错误污染)销毁;常驻满 = 临时实例销毁(缩容)
func (p *runtimePool) Return(inst *hookInstance, broken bool) {
	defer p.wg.Done()
	if broken {
		p.discard(inst)
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		p.discard(inst) // 关闭中的在途归还:直接销毁
		return
	}
	select {
	case p.instances <- inst:
	default:
		p.discard(inst)
	}
}

// discard 销毁实例并归还 VM 预算
func (p *runtimePool) discard(inst *hookInstance) {
	inst.closeInstance()
	vmBudgets.release(p.pkg)
}

// Close 停新借出;在途归零后排空常驻(销毁并归还预算;Reload 旧池不留泄漏)
func (p *runtimePool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
	for {
		select {
		case inst := <-p.instances:
			p.discard(inst)
		default:
			return
		}
	}
}

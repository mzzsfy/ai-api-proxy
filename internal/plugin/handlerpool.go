// handler 池实例与池((包,revision) 维度;借出计数丢弃;与 protocol 池零复用)
package plugin

import (
	"fmt"
	"sync"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// handlerInstance 单池实例(模块加载一次;ctx 为调用参数,顶层无可变请求态)
type handlerInstance struct {
	env       *hooksEnv
	handlerFn jsengine.Value
	prepareFn jsengine.Value // 可空
	borrows   int
	// cur 当前执行(池内串行;ctx 宿主闭包经此路由到在途执行)
	cur *handlerExec
}

// release 释放实例资源(测试桩 nil 安全)
func (hi *handlerInstance) release() {
	if hi.env == nil {
		return
	}
	if hi.handlerFn.VM() != nil {
		hi.handlerFn.Free()
		hi.handlerFn = jsengine.Value{}
	}
	if hi.prepareFn.VM() != nil {
		hi.prepareFn.Free()
		hi.prepareFn = jsengine.Value{}
	}
	_ = hi.env.q.Close()
}

// handlerPool 实例池:容量从 0 按需增长(VM 预算约束总量);broken/借满即弃
type handlerPool struct {
	pkg       string
	instances chan *handlerInstance
	factory   func() (*handlerInstance, error)
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	maxBorrow int
}

func newHandlerPool(pkg string, maxBorrow int, factory func() (*handlerInstance, error)) *handlerPool {
	return &handlerPool{pkg: pkg, instances: make(chan *handlerInstance, poolSizeDefault), factory: factory, maxBorrow: maxBorrow}
}

// Borrow 取实例;10ms 空闲窗内复用归还,超窗按 VM 预算弹性新建(达顶轮询至排队上界);
// wg 计在途借用,成功借出的 Done 转移到 Return
func (p *handlerPool) Borrow() (*handlerInstance, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("handler pool closed")
	}
	p.wg.Add(1) // 与 closed 检查同临界区(Close→Wait 不允许与 Add 并发)
	p.mu.Unlock()
	fail := func(err error) (*handlerInstance, error) {
		p.wg.Done()
		return nil, err
	}
	wait := time.NewTimer(PoolIdleWait)
	select {
	case inst := <-p.instances:
		wait.Stop()
		inst.borrows++
		return inst, nil
	case <-wait.C:
	}
	deadline := time.NewTimer(handlerQueueTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(PoolIdleWait)
	defer tick.Stop()
	for {
		if vmBudgets.tryAcquire(p.pkg) {
			inst, err := p.factory()
			if err != nil {
				vmBudgets.release(p.pkg)
				return fail(err)
			}
			inst.borrows++
			return inst, nil
		}
		select {
		case inst := <-p.instances:
			inst.borrows++
			return inst, nil
		case <-tick.C:
		case <-deadline.C:
			return fail(ErrPoolTimeout)
		}
	}
}

// Return 归还;broken(执行级取消污染)/借满即弃并按 VM 预算放行新建
func (p *handlerPool) Return(inst *handlerInstance, broken bool) {
	defer p.wg.Done()
	if broken || inst.borrows >= p.maxBorrow {
		p.discard(inst)
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		p.discard(inst)
		return
	}
	select {
	case p.instances <- inst:
	default:
		p.discard(inst)
	}
}

// discard 销毁实例并归还 VM 预算
func (p *handlerPool) discard(inst *handlerInstance) {
	inst.release()
	vmBudgets.release(p.pkg)
}

// Close 停新借出并等待在途归零
func (p *handlerPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
}

// buildHandlerInstance 池构建:new runtime → util/require/ctx 原语注入 → require handler.js 复核导出 → freeze globalThis
// ctx 胶水与宿主函数必须在 freeze 前落位(冻结后不可新增全局)
func buildHandlerInstance(deps HandlerDeps) (*handlerInstance, error) {
	pkg := deps.Package
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name, Log: deps.Log}, false)
	env.strictWrap = true
	inst := &handlerInstance{env: env}
	if err := env.q.inst.EvalString(ctxGlueJS); err != nil {
		inst.release()
		return nil, fmt.Errorf("ctx glue: %w", err)
	}
	if err := inst.registerCtxHostFuncs(); err != nil {
		inst.release()
		return nil, fmt.Errorf("ctx host funcs: %w", err)
	}
	if err := inst.registerSrcHostFuncs(); err != nil {
		inst.release()
		return nil, fmt.Errorf("src host funcs: %w", err)
	}
	exp, err := env.load("", HandlerEntry)
	if err != nil {
		inst.release()
		return nil, fmt.Errorf("load %s: %w", HandlerEntry, err)
	}
	// JS 值必须先于 VM 释放(err 分支 release 关闭 VM;defer 悬空 Free 会崩)
	kept := true
	defer func() {
		if kept {
			exp.Free()
		}
	}()
	// 复核导出形态(判据已在装载期求值一次,此处池构建期复核)
	handlerFn, prepareFn, err := checkHandlerExports(HandlerEntry, exp)
	if err != nil {
		exp.Free()
		inst.release()
		kept = false
		return nil, err
	}
	inst.handlerFn = handlerFn
	inst.prepareFn = prepareFn
	// 全局冻结:运行中写全局/覆写 util/require 当场抛错(严格模块包装组合)
	if err := env.q.inst.EvalString("Object.freeze(globalThis)"); err != nil {
		exp.Free()
		inst.release()
		kept = false
		return nil, fmt.Errorf("freeze globalThis: %w", err)
	}
	return inst, nil
}

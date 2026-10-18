// handler 池实例与池((包,revision) 维度;借出计数丢弃;与 protocol 池零复用)
package plugin

import (
	"fmt"
	"sync"

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

// handlerPool 实例池:容量从 0 按需增长(全局并发闸约束总量);broken/借满即弃并懒补建
type handlerPool struct {
	instances chan *handlerInstance
	factory   func() (*handlerInstance, error)
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	maxBorrow int
}

func newHandlerPool(maxBorrow int, factory func() (*handlerInstance, error)) *handlerPool {
	return &handlerPool{instances: make(chan *handlerInstance, handlerGlobalConcurr), factory: factory, maxBorrow: maxBorrow}
}

// Borrow 取实例;池空懒建(构建失败 = 该次借出报错,不回溯拒装,下次借出重试构建)。
// 排队语义由全局并发闸承担(60s 上界);本池不排队。
func (p *handlerPool) Borrow() (*handlerInstance, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("handler pool closed")
	}
	p.wg.Add(1) // 与 closed 检查同临界区(Close→Wait 不允许与 Add 并发)
	p.mu.Unlock()
	defer p.wg.Done()
	select {
	case inst := <-p.instances:
		inst.borrows++
		return inst, nil
	default:
	}
	inst, err := p.factory()
	if err != nil {
		return nil, err
	}
	inst.borrows++
	return inst, nil
}

// Return 归还;broken(执行级取消污染)/借满即弃并懒补建
func (p *handlerPool) Return(inst *handlerInstance, broken bool) {
	if broken || inst.borrows >= p.maxBorrow {
		inst.release()
		go p.refill()
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		inst.release()
		return
	}
	select {
	case p.instances <- inst:
	default:
		inst.release()
	}
}

// refill 补建实例(失败 = 待下次补建)
func (p *handlerPool) refill() {
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
		inst.release()
	}
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

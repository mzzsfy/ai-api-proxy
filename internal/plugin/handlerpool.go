// handler 池实例与池((包,revision) 维度;借出计数丢弃;与 protocol 池零复用)
package plugin

import (
	"fmt"
	"sync"

	"github.com/dop251/goja"
)

// handlerInstance 单池实例(模块加载一次;ctx 为调用参数,顶层无可变请求态)
type handlerInstance struct {
	vm        *goja.Runtime
	handlerFn goja.Callable
	prepareFn goja.Callable // 可空
	borrows   int
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
		go p.refill()
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return
	}
	select {
	case p.instances <- inst:
	default:
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
	}
}

// Close 停新借出并等待在途归零
func (p *handlerPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
}

// buildHandlerInstance 池构建:new runtime → util/require 注入 → require handler.js 复核导出 → freeze globalThis
func buildHandlerInstance(deps HandlerDeps) (*handlerInstance, error) {
	pkg := deps.Package
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name, Log: deps.Log}, false)
	env.strictWrap = true
	exp, err := env.load("", handlerEntry)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", handlerEntry, err)
	}
	// 复核导出形态(判据已在装载期求值一次,此处池构建期复核)
	if s := exp.Get("settings"); s != nil && !goja.IsUndefined(s) && !goja.IsNull(s) {
		return nil, fmt.Errorf("%s: settings export not allowed (handler params come from ctx.params)", handlerEntry)
	}
	fnV := exp.Get("handler") // 兼容 module.exports = fn(命名导出优先)与 module.exports.handler 两种形态
	if fnV == nil || goja.IsUndefined(fnV) || goja.IsNull(fnV) {
		fnV = exp
	}
	handlerFn, ok := goja.AssertFunction(fnV)
	if !ok {
		return nil, fmt.Errorf("%s: export must be a function", handlerEntry)
	}
	var prepareFn goja.Callable
	if pv := exp.Get("prepare"); pv != nil && !goja.IsUndefined(pv) && !goja.IsNull(pv) {
		prepareFn, ok = goja.AssertFunction(pv)
		if !ok {
			return nil, fmt.Errorf("%s: prepare export must be a function", handlerEntry)
		}
	}
	// 全局冻结:运行中写全局/覆写 util/require 当场抛错(严格模块包装组合)
	if _, err := env.vm.RunString("Object.freeze(globalThis)"); err != nil {
		return nil, fmt.Errorf("freeze globalThis: %w", err)
	}
	return &handlerInstance{vm: env.vm, handlerFn: handlerFn, prepareFn: prepareFn}, nil
}

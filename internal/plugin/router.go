// 键路由钩子运行时:包声明 parts.hooks.router 时 JS 接管请求级选键;
// 默认 round-robin 恒保留(select 缺失/异常/超时/未知 id 一律回落,可用性不依赖 JS)
package plugin

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// RouterTimeoutMs select 单次调用预算缺省(热路径;超时回落 round-robin)
const RouterTimeoutMs = 50

// RouterDeps 路由钩子宿主依赖
type RouterDeps struct {
	Package *Package
	// Log 日志回调(nil = 丢弃)
	Log func(level, msg string)
	// Now 时钟注入(测试);nil = time.Now
	Now func() time.Time
	// SelectTimeoutMs select 调用预算;≤0 = 缺省 RouterTimeoutMs
	SelectTimeoutMs int64
}

// RouterRequestInfo 请求要素
type RouterRequestInfo struct {
	Model  string
	Stream bool
}

// RouterSelectInput select 钩子输入(全宿主权威事实;JS 只读,不回写)
type RouterSelectInput struct {
	// Keys 键池快照(宿主 List 序;id+data)
	Keys []KeyEntry
	// InFlight 键id → 在途请求数(宿主权威记账;缺失 = 0)
	InFlight map[string]int
	// Request 请求要素
	Request RouterRequestInfo
}

// RouterSelectOut select 结果(Selected 空 = 交回宿主 round-robin)
type RouterSelectOut struct {
	Selected string
}

// routerInFlight 包维度在途计数(宿主权威;请求级 +1/-1,select 快照只读)
type routerInFlight struct {
	mu sync.Mutex
	m  map[string]int
}

func newRouterInFlight() *routerInFlight {
	return &routerInFlight{m: map[string]int{}}
}

// inc 记入途 +1(请求开始;键存在才计数由调用方保证)
func (r *routerInFlight) inc(id string) {
	r.mu.Lock()
	r.m[id]++
	r.mu.Unlock()
}

// dec 记终局 -1(缺失/零值忽略,防御性不 下溢)
func (r *routerInFlight) dec(id string) {
	r.mu.Lock()
	if n, ok := r.m[id]; ok && n > 0 {
		r.m[id] = n - 1
	}
	r.mu.Unlock()
}

// snapshot 全量快照(select 输入用)
func (r *routerInFlight) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.m))
	for k, v := range r.m {
		out[k] = v
	}
	return out
}

// routerInstance 单路由实例(模块求值一次;调用期互斥,select 同步无状态)
type routerInstance struct {
	env     *hooksEnv
	select_ jsengine.Value
}

// release 释放实例资源(测试桩 nil 安全)
func (ri *routerInstance) release() {
	if ri.env == nil {
		return
	}
	if ri.select_.VM() != nil {
		ri.select_.Free()
		ri.select_ = jsengine.Value{}
	}
	_ = ri.env.q.Close()
}

// RouterEngines 路由钩子注册表((包,revision) 维度懒建;select 互斥借用)
type RouterEngines struct {
	mu      sync.Mutex
	engines map[string]*RouterEngine
	factory func(pkg *Package) RouterDeps
}

// NewRouterEngines 构造(factory 由宿主装配:Package/Log 按包绑定)
func NewRouterEngines(factory func(pkg *Package) RouterDeps) *RouterEngines {
	return &RouterEngines{engines: map[string]*RouterEngine{}, factory: factory}
}

// For 取或懒建路由引擎(包未声明 router = nil, nil——调用方回落 round-robin)
func (h *RouterEngines) For(pkg *Package) (*RouterEngine, error) {
	if pkg.Manifest.Parts.Hooks == nil || pkg.Manifest.Parts.Hooks.Router == nil {
		return nil, nil
	}
	key := pkg.Manifest.Name + "|" + fmt.Sprint(pkg.Revision)
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.engines[key]; ok {
		return e, nil
	}
	e, err := newRouterEngine(h.factory(pkg))
	if err != nil {
		return nil, err
	}
	h.engines[key] = e
	return e, nil
}

// CloseAll 释放全部路由引擎(热升级/关闭)
func (h *RouterEngines) CloseAll() {
	h.mu.Lock()
	engines := h.engines
	h.engines = map[string]*RouterEngine{}
	h.mu.Unlock()
	for _, e := range engines {
		e.Close()
	}
}

// RouterEngine 单包路由引擎(单实例互斥;select 失败后实例污染即弃,下次调用懒重建)
type RouterEngine struct {
	mu       sync.Mutex
	deps     RouterDeps
	entry    string
	inst     *routerInstance
	timeout  time.Duration
	inFlight *routerInFlight
}

func newRouterEngine(deps RouterDeps) (*RouterEngine, error) {
	pkg := deps.Package
	entry := pkg.Manifest.Parts.Hooks.Router.Entry
	if entry == "" {
		entry = RouterEntry
	}
	timeout := time.Duration(deps.SelectTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = RouterTimeoutMs * time.Millisecond
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Log == nil {
		deps.Log = func(string, string) {}
	}
	inst, err := buildRouterInstance(pkg, entry, deps)
	if err != nil {
		return nil, err
	}
	return &RouterEngine{deps: deps, entry: entry, inst: inst, timeout: timeout, inFlight: newRouterInFlight()}, nil
}

// Begin 记请求在途 +1,返回归还函数(请求终局必调;计数供后续 select 快照)
func (e *RouterEngine) Begin(id string) func() {
	e.inFlight.inc(id)
	return func() { e.inFlight.dec(id) }
}

// InFlightSnapshot 当前在途计数快照(select 输入)
func (e *RouterEngine) InFlightSnapshot() map[string]int {
	return e.inFlight.snapshot()
}

// buildRouterInstance 求值 router.js 并复核导出(必须导出 select 函数)
// 释放序纪律:错误分支先 Free JS 值再 inst.release(关 VM),defer 仅兜底未处理分支——
// 对已关 VM Free 是 use-after-free(handlerpool 同型 kept 翻转语义)
func buildRouterInstance(pkg *Package, entry string, deps RouterDeps) (*routerInstance, error) {
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name, Log: deps.Log}, false)
	env.strictWrap = true
	inst := &routerInstance{env: env}
	exp, err := env.load("", entry)
	if err != nil {
		inst.release()
		return nil, fmt.Errorf("router %s: %w", entry, err)
	}
	kept := false
	defer func() {
		if !kept {
			exp.Free()
		}
	}()
	fail := func(format string, args ...any) (*routerInstance, error) {
		exp.Free()
		kept = true
		inst.release()
		return nil, fmt.Errorf(format, args...)
	}
	fn, err := env.q.inst.GetProp(exp, "select")
	if err != nil {
		return fail("router %s: read select: %w", entry, err)
	}
	if fn.VM() == nil || fn.IsUndefined() {
		fn.Free()
		return fail("router %s: select export required", entry)
	}
	okFn, err := env.q.inst.IsFunction(fn)
	if err != nil || !okFn {
		fn.Free()
		return fail("router %s: select must be a function", entry)
	}
	inst.select_ = fn
	kept = true
	exp.Free()
	if err := env.q.inst.EvalString("Object.freeze(globalThis)"); err != nil {
		inst.release()
		return nil, fmt.Errorf("router freeze globalThis: %w", err)
	}
	return inst, nil
}

// errRouterClosed 引擎已关闭(热升级窗口;回落 round-robin)
var errRouterClosed = errors.New("router engine closed")

// Close 释放引擎(在途 select 结束后)
func (e *RouterEngine) Close() {
	e.mu.Lock()
	inst := e.inst
	e.inst = nil
	e.mu.Unlock()
	if inst != nil {
		inst.release()
	}
}

// discard 污染实例即弃(异常/超时后调用;下次 Select 懒重建)
func (e *RouterEngine) discard() {
	if e.inst != nil {
		e.inst.release()
		e.inst = nil
	}
}

// ensureInstance 取可用实例(nil = 懒重建;重建失败 = error 回落)
func (e *RouterEngine) ensureInstance() (*routerInstance, error) {
	if e.inst == nil {
		inst, err := buildRouterInstance(e.deps.Package, e.entry, e.deps)
		if err != nil {
			return nil, err
		}
		e.inst = inst
	}
	return e.inst, nil
}

// Select 调用 JS 选键(互斥;任何失败 = error,调用方回落 round-robin;失败实例即弃懒重建)
func (e *RouterEngine) Select(in RouterSelectInput) (RouterSelectOut, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deps.Package == nil {
		return RouterSelectOut{}, errRouterClosed
	}
	inst, err := e.ensureInstance()
	if err != nil {
		return RouterSelectOut{}, err
	}
	env := inst.env
	if err := env.q.inst.SetBudget(e.timeout); err != nil {
		return RouterSelectOut{}, err
	}
	keysArg := make([]map[string]any, 0, len(in.Keys))
	for _, k := range in.Keys {
		keysArg = append(keysArg, map[string]any{"id": k.ID, "data": k.Data})
	}
	payload := map[string]any{
		"keys":     keysArg,
		"inFlight": in.InFlight,
		"request":  map[string]any{"model": in.Request.Model, "stream": in.Request.Stream},
	}
	out, err := env.q.inst.Call(inst.select_, payload)
	if err != nil {
		e.discard()
		return RouterSelectOut{}, wrapRouterCallErr(err, e.timeout)
	}
	settled, _, rerr := env.q.inst.Await(out, e.timeout)
	out.Free()
	if rerr != nil {
		var selErr error
		if rej, ok := env.q.inst.Rejection(rerr); ok {
			ee := env.q.inst.ExceptionOf(rej)
			rej.Free()
			selErr = wrapRouterCallErr(ee, e.timeout)
		} else {
			selErr = wrapRouterCallErr(rerr, e.timeout)
		}
		e.discard()
		return RouterSelectOut{}, selErr
	}
	if settled.IsUndefined() || settled.IsNull() {
		settled.Free()
		return RouterSelectOut{}, nil
	}
	s, anyErr := settled.Any()
	settled.Free()
	if anyErr != nil {
		e.discard()
		return RouterSelectOut{}, anyErr
	}
	sel, isStr := s.(string)
	if !isStr {
		e.discard()
		return RouterSelectOut{}, fmt.Errorf("router select: string return required, got %T", s)
	}
	if sel == "" || sel == "null" || sel == "undefined" {
		return RouterSelectOut{}, nil
	}
	return RouterSelectOut{Selected: sel}, nil
}

// wrapRouterCallErr 超时中断归一(可用性语义:调用方回落,不区分错误形态)
func wrapRouterCallErr(err error, timeout time.Duration) error {
	if ee, ok := err.(*jsengine.EngineError); ok && ee.Kind == jsengine.KindInterrupt {
		return fmt.Errorf("router select: %w (after %s)", errRouterClosed, timeout)
	}
	return fmt.Errorf("router select: %w", err)
}

package plugin

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// hooksProgramCache 编译产物缓存(键=name|revision|entry|src 摘要;*goja.Program 跨 runtime 共享安全;
// 每 (包,revision) 一条,体量极小,不做事后清理)
var hooksProgramCache sync.Map

// hooksProgram 取编译产物,命中缓存免编译;失败不缓存
func hooksProgram(pkg *Package, entry string, src []byte) (*goja.Program, error) {
	sum := sha256.Sum256(src)
	key := fmt.Sprintf("%s|%d|%s|%x", pkg.Manifest.Name, pkg.Revision, entry, sum[:8])
	if v, ok := hooksProgramCache.Load(key); ok {
		return v.(*goja.Program), nil
	}
	prog, err := Compile(src, entry)
	if err != nil {
		return nil, err
	}
	hooksProgramCache.Store(key, prog)
	return prog, nil
}

// HttpRequest ctx.http 请求载体
type HttpRequest struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    string
	Timeout time.Duration // 已钳制后的最终值
}

// HttpResponse ctx.http 响应载体
type HttpResponse struct {
	Status  int
	Headers map[string]string
	Body    string
}

// HttpExecutor 出站执行器(宿主注入;复用全局出站 Transport)
type HttpExecutor interface {
	Do(req HttpRequest) (*HttpResponse, error)
}

// HooksDeps hooks 部件宿主依赖
type HooksDeps struct {
	PackageName string
	HTTP        HttpExecutor // nil = ctx.http 调用抛错
	Keys        *KeysStore   // nil = keys 调用抛错
	Storage     StorageKV
	Log         func(level, msg string)
	Now         func() time.Time // 时钟注入(测试);nil = time.Now
	// TransportEvict 主动失效上报(util.evict;nil = 调用抛错)
	TransportEvict func(transport, scope, value string) error
}

// HooksRuntime 包 hooks 部件实例(非池化:每任务运行新实例,编译缓存归调用方)
type HooksRuntime struct {
	pkg      string
	vm       *goja.Runtime
	onLoad   goja.Callable
	tasks    map[string]goja.Callable
	timeouts map[string]int64
	deps     HooksDeps
}

// LoadHooks 编译并实例化 hooks 部件(仅对象导出形态;onLoad 与各 task 可选实现)
func LoadHooks(pkg *Package, deps HooksDeps) (*HooksRuntime, error) {
	part := pkg.Manifest.Parts.Hooks
	if part == nil {
		return nil, fmt.Errorf("package %s has no hooks part", pkg.Manifest.Name)
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	src := pkg.Files[part.Entry]
	prog, err := hooksProgram(pkg, part.Entry, src)
	if err != nil {
		return nil, err
	}
	vm := goja.New()
	bindUtil(vm, HostDeps{PackageName: pkg.Manifest.Name, Storage: deps.Storage, Log: deps.Log, TransportEvict: deps.TransportEvict})
	exports := vm.NewObject()
	module := vm.NewObject()
	_ = module.Set("exports", exports)
	_ = vm.Set("module", module)
	_ = vm.Set("exports", exports)
	if _, err := vm.RunProgram(prog); err != nil {
		return nil, fmt.Errorf("run %s: %w", part.Entry, err)
	}
	get := func(name string) goja.Callable {
		var v goja.Value
		if o, ok := module.Get("exports").(*goja.Object); ok {
			v = o.Get(name)
		}
		if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
			return nil
		}
		fn, ok := goja.AssertFunction(v)
		if !ok {
			return nil // 非函数导出视同未实现
		}
		return fn
	}
	rt := &HooksRuntime{pkg: pkg.Manifest.Name, vm: vm, onLoad: get("onLoad"), tasks: map[string]goja.Callable{}, timeouts: map[string]int64{}, deps: deps}
	for _, tk := range part.Tasks {
		rt.tasks[tk.Name] = get(tk.Name)
		rt.timeouts[tk.Name] = tk.TimeoutMs
	}
	return rt, nil
}

// HasTask 任务是否可执行(未实现 = 加载期告警跳过)
func (h *HooksRuntime) HasTask(name string) bool {
	return h.tasks[name] != nil
}

// TaskNames 已实现的任务名(稳定序由调用方保证)
func (h *HooksRuntime) TaskNames() []string {
	out := make([]string, 0, len(h.tasks))
	for n := range h.tasks {
		out = append(out, n)
	}
	return out
}

// RunOnLoad 加载钩子(同步,OnLoadTimeout 毫秒预算);previous = 热加载旧包 keys(启停/首载为 nil)
func (h *HooksRuntime) RunOnLoad(previous map[string]any) error {
	if h.onLoad == nil {
		return nil
	}
	return h.call("onLoad", OnLoadTimeout, h.newCtx(OnLoadTimeout, time.Time{}, previous))
}

// RunTask 定时任务(同步,timeoutMs 预算;≤0 归缺省,上限钳制)
func (h *HooksRuntime) RunTask(name string, at time.Time) error {
	fn := h.tasks[name]
	if fn == nil {
		return fmt.Errorf("task %q not implemented in package %s", name, h.pkg)
	}
	budget := int64(TaskTimeoutDefault)
	if tms := h.timeouts[name]; tms > 0 {
		budget = tms
	}
	if budget > TaskTimeoutMax {
		budget = TaskTimeoutMax
	}
	return h.call("task "+name, budget, h.newCtx(budget, at, nil))
}

// newCtx 构造本次调用的 ctx(http/keys/cron)
func (h *HooksRuntime) newCtx(budgetMs int64, at time.Time, previous map[string]any) *goja.Object {
	ctx := h.vm.NewObject()
	httpObj := h.vm.NewObject()
	_ = httpObj.Set("run", func(opts map[string]any) (map[string]any, error) {
		return h.doHTTP(opts, budgetMs)
	})
	_ = ctx.Set("http", httpObj)
	keysObj := h.vm.NewObject()
	_ = keysObj.Set("get", func(name string) (any, error) {
		if h.deps.Keys == nil {
			return nil, fmt.Errorf("keys unavailable (package %s)", h.pkg)
		}
		v, ok := h.deps.Keys.Get(h.pkg, name)
		if !ok {
			return goja.Undefined(), nil
		}
		return v, nil
	})
	_ = keysObj.Set("set", func(values map[string]any) error {
		if h.deps.Keys == nil {
			return fmt.Errorf("keys unavailable (package %s)", h.pkg)
		}
		return h.deps.Keys.Set(h.pkg, values)
	})
	_ = keysObj.Set("previous", func(name string) (any, error) {
		if h.deps.Keys == nil {
			return nil, fmt.Errorf("keys unavailable (package %s)", h.pkg)
		}
		if previous != nil {
			if v, ok := previous[name]; ok {
				return v, nil
			}
			return goja.Undefined(), nil
		}
		v, ok := h.deps.Keys.Previous(h.pkg, name)
		if !ok {
			return goja.Undefined(), nil
		}
		return v, nil
	})
	_ = ctx.Set("keys", keysObj)
	cronObj := h.vm.NewObject()
	runAt := h.deps.Now()
	if !at.IsZero() {
		runAt = at
	}
	_ = cronObj.Set("runAt", runAt.UTC().Format(time.RFC3339))
	_ = ctx.Set("cron", cronObj)
	return ctx
}

// doHTTP 执行出站请求(超时 = min(取值, 外层剩余);body 截断)
func (h *HooksRuntime) doHTTP(opts map[string]any, budgetMs int64) (map[string]any, error) {
	if h.deps.HTTP == nil {
		return nil, fmt.Errorf("http unavailable (package %s)", h.pkg)
	}
	url, _ := opts["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("http: url required")
	}
	method, _ := opts["method"].(string)
	if method == "" {
		method = "GET"
	}
	headers := map[string]string{}
	if hm, ok := opts["headers"].(map[string]any); ok {
		for k, v := range hm {
			if sv, ok := v.(string); ok {
				headers[k] = sv
			}
		}
	}
	body, _ := opts["body"].(string)
	timeout := time.Duration(budgetMs) * time.Millisecond
	if tms, ok := numericField(opts["timeoutMs"]); ok && tms > 0 && tms < budgetMs {
		timeout = time.Duration(tms) * time.Millisecond
	}
	resp, err := h.deps.HTTP.Do(HttpRequest{URL: url, Method: method, Headers: headers, Body: body, Timeout: timeout})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": resp.Status, "headers": resp.Headers, "body": resp.Body}, nil
}

// numericField 数值字段提取(goja 导出可能为 int64/float64)
func numericField(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// call 同步执行 hook(Promise 返回 = 同步性违规;超时 Interrupt;timeout 入参为毫秒)
func (h *HooksRuntime) call(name string, timeoutMs int64, ctx *goja.Object) (err error) {
	fn := h.onLoad
	if strings.HasPrefix(name, "task ") {
		fn = h.tasks[strings.TrimPrefix(name, "task ")]
	}
	timer := time.AfterFunc(time.Duration(timeoutMs)*time.Millisecond, func() { h.vm.Interrupt("timeout") })
	defer timer.Stop()
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%s: %v", name, rec)
		}
	}()
	v, err := fn(goja.Undefined(), ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if isThenable(h.vm, v) {
		return fmt.Errorf("%s: async return (sync violation)", name)
	}
	return nil
}

// isThenable 返回值是否 Promise 形态(同步性违规判定)
func isThenable(vm *goja.Runtime, v goja.Value) bool {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return false
	}
	o, ok := v.(*goja.Object)
	if !ok {
		return false
	}
	return o.Get("then") != nil
}

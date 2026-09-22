package plugin

import (
	"crypto/sha256"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

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

// hooksEnv 单 runtime 求值环境:require 幂等单例 + 循环检测 + 模块注册表
type hooksEnv struct {
	pkg    *Package
	deps   HooksDeps
	vm     *goja.Runtime
	cache  map[string]*goja.Object // file → exports(runtime 内幂等单例)
	stack  []string                // 求值栈(循环检测)
	withSetting bool               // setting 构建器全局注入(keyForm 函数体运行期需要)
}

// newHooksEnv 构造(runtime/util/setting 注入一次)
func newHooksEnv(pkg *Package, deps HooksDeps, withSetting bool) *hooksEnv {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	vm := goja.New()
	bindUtil(vm, HostDeps{PackageName: pkg.Manifest.Name, Storage: deps.Storage, Log: deps.Log, TransportEvict: deps.TransportEvict})
	env := &hooksEnv{pkg: pkg, deps: deps, vm: vm, cache: map[string]*goja.Object{}, withSetting: withSetting}
	if withSetting {
		b := map[string]any{}
		for _, typ := range []string{"string", "int", "number", "bool", "enum"} {
			t := typ
			b[t] = func(opts map[string]any) map[string]any {
				out := map[string]any{"type": t}
				for k, v := range opts {
					out[k] = v
				}
				return out
			}
		}
		_ = vm.Set("setting", b)
	}
	_ = vm.Set("require", func(call goja.FunctionCall) goja.Value {
		// require 的引用方 = 栈顶文件
		from := ""
		if len(env.stack) > 0 {
			from = env.stack[len(env.stack)-1]
		}
		exp, err := env.load(from, call.Argument(0).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return exp
	})
	return env
}

// load 求值包内文件并返回 module.exports(幂等单例;循环 require 拒绝)。
// CommonJS 函数包装:每模块独立 module/exports/require 形参,互不串扰。
func (e *hooksEnv) load(fromFile, spec string) (*goja.Object, error) {
	target := spec
	if fromFile != "" {
		var err error
		target, err = e.resolve(fromFile, spec)
		if err != nil {
			return nil, err
		}
	} else if strings.HasPrefix(spec, "./") || strings.HasPrefix(spec, "../") {
		return nil, fmt.Errorf("require %q: relative path without base file", spec)
	}
	if exp, ok := e.cache[target]; ok {
		return exp, nil
	}
	for _, f := range e.stack {
		if f == target {
			return nil, fmt.Errorf("circular require: %s → %s", strings.Join(e.stack, " → "), target)
		}
	}
	src, ok := e.pkg.Files[target]
	if !ok {
		return nil, fmt.Errorf("file %q missing in package %s", target, e.pkg.Manifest.Name)
	}
	wrapped := "(function(module, exports, require){\n" + string(src) + "\n})"
	prog, err := hooksProgram(e.pkg, target, []byte(wrapped))
	if err != nil {
		return nil, err
	}
	fnVal, err := e.vm.RunProgram(prog)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", target, err)
	}
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return nil, fmt.Errorf("%s: not a module", target)
	}
	exports := e.vm.NewObject()
	module := e.vm.NewObject()
	_ = module.Set("exports", exports)
	require := func(call goja.FunctionCall) goja.Value {
		exp, err := e.load(target, call.Argument(0).String())
		if err != nil {
			panic(e.vm.NewGoError(err))
		}
		return exp
	}
	e.stack = append(e.stack, target)
	_, callErr := fn(goja.Undefined(), module, exports, e.vm.ToValue(require))
	e.stack = e.stack[:len(e.stack)-1]
	if callErr != nil {
		return nil, fmt.Errorf("run %s: %w", target, callErr)
	}
	final := module.Get("exports")
	exp, _ := final.(*goja.Object)
	if exp == nil {
		exp = exports
	}
	e.cache[target] = exp
	return exp, nil
}

// resolve 相对 fromFile 目录解析:Clean 后判越界;精确 → +.js → 目录索引
func (e *hooksEnv) resolve(fromFile, spec string) (string, error) {
	if spec == "" || strings.HasPrefix(spec, "/") {
		return "", fmt.Errorf("require %q: only in-package relative paths allowed", spec)
	}
	dir := ""
	if i := strings.LastIndexByte(fromFile, '/'); i >= 0 {
		dir = fromFile[:i]
	}
	joined := spec
	if dir != "" {
		joined = dir + "/" + spec
	}
	cleaned := path.Clean(joined)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("require %q: escapes package", spec)
	}
	for _, c := range []string{cleaned, cleaned + ".js", cleaned + "/index.js"} {
		if _, ok := e.pkg.Files[c]; ok {
			return c, nil
		}
	}
	return "", fmt.Errorf("require %q (from %s): not found in package", spec, fromFile)
}

// HooksRuntime 包 hooks 部件实例(非池化:每次调用新 runtime;编译缓存跨 runtime 共享)
type HooksRuntime struct {
	pkg          string
	vm           *goja.Runtime
	exports      *goja.Object
	deps         HooksDeps
	settings     map[string]any // ctx.settings 快照
	taskName     string         // ctx.task
	written      []string       // CallKeySubmit 期间 ctx.keys.set 写入键名(去重按首次序)
	writtenSeen  map[string]bool
	collectWrite bool // keys.set 拦截收集开关
}

// 任务文件缺省路径
func taskEntry(tk HooksTask) string {
	if tk.Entry != "" {
		return tk.Entry
	}
	return "tasks/" + tk.Name + ".js"
}

// 族文件固定求值序(声明提取)
func familyFiles(pkg *Package) []string {
	out := []string{}
	if _, ok := pkg.Files["settings.js"]; ok {
		out = append(out, "settings.js")
	}
	if _, ok := pkg.Files["init.js"]; ok {
		out = append(out, "init.js")
	}
	if _, ok := pkg.Files["keys.js"]; ok {
		out = append(out, "keys.js")
	}
	if h := pkg.Manifest.Parts.Hooks; h != nil {
		for _, tk := range h.Tasks {
			e := taskEntry(tk)
			dup := false
			for _, f := range out {
				if f == e {
					dup = true
					break
				}
			}
			if !dup {
				out = append(out, e)
			}
		}
	}
	return out
}

// ExtractDeclaration 安装期求值全部族文件收集 settings 片段(固定序;后者覆盖前者同名键)。
// 返回合并后的声明(settingsSchema 形态);失败 = 拒装(语法/求值/非法声明)。
func ExtractDeclaration(pkg *Package, deps HooksDeps) (map[string]any, error) {
	env := newHooksEnv(pkg, deps, true)
	merged := map[string]any{}
	for _, f := range familyFiles(pkg) {
		exp, err := env.load("", f)
		if err != nil {
			return nil, err
		}
		frag := exp.Get("settings")
		if frag == nil || goja.IsUndefined(frag) || goja.IsNull(frag) {
			continue
		}
		fm, ok := frag.Export().(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: settings export must be an object", f)
		}
		for k, v := range fm {
			merged[k] = v
		}
	}
	return merged, nil
}

// normalizeDeps deps 缺省归一(Now 时钟)
func normalizeDeps(deps HooksDeps) HooksDeps {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return deps
}

// LoadTask 装载任务文件(目标文件 + require 闭包;ctx.settings/ctx.task 注入)
func LoadTask(pkg *Package, tk HooksTask, deps HooksDeps, settings map[string]any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", taskEntry(tk))
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, settings: settings, taskName: tk.Name}, nil
}

// LoadInit 装载 init.js(onLoad;previous 注入经 ctx)
func LoadInit(pkg *Package, deps HooksDeps, settings map[string]any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", "init.js")
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, settings: settings}, nil
}

// LoadKeys 装载 keys.js(五钩子探测载体;setting 全局注入——keyForm 函数体运行期需要)
func LoadKeys(pkg *Package, deps HooksDeps) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", "keys.js")
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps}, nil
}

// handler 取导出函数(非函数 = 未实现)
func (h *HooksRuntime) handler(name string) goja.Callable {
	if h.exports == nil {
		return nil
	}
	v := h.exports.Get(name)
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}
	fn, ok := goja.AssertFunction(v)
	if !ok {
		return nil
	}
	return fn
}

// HasTask 任务处理器是否已导出(未实现 = 调用期报错)
func (h *HooksRuntime) HasTask(name string) bool { return h.handler("handler") != nil || h.handler(name) != nil }

// RunTask 定时任务(crontab 行:处理器 = exports.handler 或 exports.<taskName>;timeoutMs 钳制)
func (h *HooksRuntime) RunTask(tk HooksTask, at time.Time) error {
	fn := h.handler("handler")
	if fn == nil {
		fn = h.handler(tk.Name)
	}
	if fn == nil {
		return fmt.Errorf("task %q: handler not exported in %s", tk.Name, h.pkg)
	}
	budget := tk.TimeoutMs
	if budget <= 0 {
		budget = TaskTimeoutDefault
	}
	if budget > TaskTimeoutMax {
		budget = TaskTimeoutMax
	}
	return h.call("task "+tk.Name, fn, budget, h.newCtx(budget, at, nil, &tk))
}

// RunOnLoad 加载钩子(exports 即处理器;previous = 热加载旧包 keys 快照,启停/首载 nil)
func (h *HooksRuntime) RunOnLoad(previous map[string]any) error {
	fn := h.handler("onLoad")
	if fn == nil {
		fn = h.handler("")
	}
	if fn == nil {
		return nil
	}
	return h.call("onLoad", fn, OnLoadTimeout, h.newCtx(OnLoadTimeout, time.Time{}, previous, nil))
}

// RunNext next 形态自调度查询(exports.next(ctx) → unix ms | null)
func (h *HooksRuntime) RunNext() (int64, bool, error) {
	fn := h.handler("next")
	if fn == nil {
		return 0, false, fmt.Errorf("next form: next not exported in %s", h.pkg)
	}
	v, err := h.callValue("next", fn, NextTimeoutMs, h.newCtx(NextTimeoutMs, time.Time{}, nil, nil))
	if err != nil {
		return 0, false, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return 0, false, nil
	}
	n, ok := numericField(v.Export())
	if !ok {
		return 0, false, fmt.Errorf("next: want unix ms number or null, got %v", v.String())
	}
	return n, true, nil
}

// keyHook keys.js 钩子调用基座
func (h *HooksRuntime) keyHook(name string, budgetMs int64, args ...goja.Value) (goja.Value, error) {
	fn := h.handler(name)
	if fn == nil {
		return nil, errKeyHookMissing
	}
	v, err := h.callValue(name, fn, budgetMs, h.newCtx(budgetMs, time.Time{}, nil, nil), args...)
	return v, err
}

// CallKeyWrite 键写入归一化(undefined/null 返回 = 透传原值)
func (h *HooksRuntime) CallKeyWrite(keyName string, newValue, oldValue any) (any, error) {
	v, err := h.keyHook("keyWrite", KeyHookTimeoutMs,
		h.vm.ToValue(keyName), h.vm.ToValue(newValue), h.vm.ToValue(oldValue))
	if err != nil {
		return nil, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return newValue, nil
	}
	return v.Export(), nil
}

// CallKeyRead 键详情解释(undefined → nil;未导出 = ErrHookNotExported)
func (h *HooksRuntime) CallKeyRead(keyName string) (any, error) {
	if h.handler("keyRead") == nil {
		return nil, ErrHookNotExported
	}
	v, err := h.keyHook("keyRead", KeyHookTimeoutMs, h.vm.ToValue(keyName))
	if err != nil {
		return nil, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil, nil
	}
	return v.Export(), nil
}

// CallKeyForm 添加表单声明(未导出 = ErrHookNotExported)
func (h *HooksRuntime) CallKeyForm() (any, error) {
	if h.handler("keyForm") == nil {
		return nil, ErrHookNotExported
	}
	v, err := h.keyHook("keyForm", KeyHookTimeoutMs)
	if err != nil {
		return nil, err
	}
	return v.Export(), nil
}

// CallKeyAction 表单按钮回调
func (h *HooksRuntime) CallKeyAction(action string, values map[string]any) (any, error) {
	v, err := h.keyHook("keyAction", KeyFlowTimeoutMs, h.vm.ToValue(action), h.vm.ToValue(values))
	if err != nil {
		return nil, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil, nil
	}
	return v.Export(), nil
}

// CallKeySubmit 表单提交(写入由回调内 ctx.keys.set;written 拦截收集,同键去重按首次序;未导出 = ErrHookNotExported)
func (h *HooksRuntime) CallKeySubmit(values map[string]any) ([]string, any, error) {
	if h.handler("keySubmit") == nil {
		return nil, nil, ErrHookNotExported
	}
	h.written = nil
	h.writtenSeen = map[string]bool{}
	h.collectWrite = true
	v, err := h.keyHook("keySubmit", KeyFlowTimeoutMs, h.vm.ToValue(values))
	h.collectWrite = false
	if err != nil {
		return nil, nil, err
	}
	var message any
	if v != nil && !goja.IsUndefined(v) && !goja.IsNull(v) {
		message = v.Export()
	}
	return h.written, message, nil
}

// Handler 导出函数暴露(装配面探测/调用)
func (h *HooksRuntime) Handler(name string) goja.Callable { return h.handler(name) }

// KeyHookTimeoutMs 归一化/读取钩子预算
const KeyHookTimeoutMs = 5 * 1000

// KeyFlowTimeoutMs 表单采集流程预算(出站登录慢于归一化)
const KeyFlowTimeoutMs = 10 * 1000

var errKeyHookMissing = fmt.Errorf("hook not implemented")

// HasExport 钩子是否已导出
func (h *HooksRuntime) HasExport(name string) bool { return h.handler(name) != nil }

// newCtx 构造本次调用的 ctx(http/keys/settings/task/cron)
func (h *HooksRuntime) newCtx(budgetMs int64, at time.Time, previous map[string]any, tk *HooksTask) *goja.Object {
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
		if h.collectWrite {
			for k := range values {
				if !h.writtenSeen[k] {
					h.writtenSeen[k] = true
					h.written = append(h.written, k)
				}
			}
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
	_ = keysObj.Set("list", func() ([]string, error) {
		if h.deps.Keys == nil {
			return nil, fmt.Errorf("keys unavailable (package %s)", h.pkg)
		}
		return h.deps.Keys.List(h.pkg), nil
	})
	_ = ctx.Set("keys", keysObj)
	if h.settings != nil {
		_ = ctx.Set("settings", h.settings)
	}
	if h.taskName != "" {
		_ = ctx.Set("task", h.taskName)
	}
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

// call 同步执行(Promise = 同步性违规;超时 Interrupt)
func (h *HooksRuntime) call(name string, fn goja.Callable, timeoutMs int64, ctx *goja.Object) error {
	_, err := h.callValue(name, fn, timeoutMs, ctx)
	return err
}

// callValue 同步执行并取返回值(Interrupt 中断 = goja 异常形态,消息含 interrupted 标记)
func (h *HooksRuntime) callValue(name string, fn goja.Callable, timeoutMs int64, ctx *goja.Object, args ...goja.Value) (val goja.Value, err error) {
	allArgs := append([]goja.Value{ctx}, args...)
	h.vm.ClearInterrupt()
	timer := time.AfterFunc(time.Duration(timeoutMs)*time.Millisecond, func() { h.vm.Interrupt(errHookTimeout) })
	defer timer.Stop()
	defer func() {
		if rec := recover(); rec != nil {
			if rec == errHookTimeout {
				err = fmt.Errorf("%s: %w (after %dms)", name, errHookTimeout, timeoutMs)
				return
			}
			err = fmt.Errorf("%s: %v", name, rec)
		}
	}()
	v, callErr := fn(goja.Undefined(), allArgs...)
	if callErr != nil {
		if strings.Contains(callErr.Error(), errHookTimeout.Error()) {
			return nil, fmt.Errorf("%s: %w (after %dms)", name, errHookTimeout, timeoutMs)
		}
		return nil, fmt.Errorf("%s: %w", name, callErr)
	}
	if isThenable(h.vm, v) {
		return nil, fmt.Errorf("%s: async return (sync violation)", name)
	}
	return v, nil
}

// errHookTimeout 钩子超时哨兵(错误判别不经字符串匹配)
var errHookTimeout = ErrHookTimeoutSentinel

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

// NextTimeoutMs next 查询预算(自调度链)
const NextTimeoutMs = 5 * 1000

// hooksProgramCache 编译产物缓存(键=name|revision|file|src 摘要;*goja.Program 跨 runtime 共享安全;
// 逐文件条目,体量极小,不做事后清理)
var hooksProgramCache sync.Map

// hooksProgram 取编译产物,命中缓存免编译;失败不缓存
func hooksProgram(pkg *Package, file string, src []byte) (*goja.Program, error) {
	sum := sha256.Sum256(src)
	key := fmt.Sprintf("%s|%d|%s|%x", pkg.Manifest.Name, pkg.Revision, file, sum[:8])
	if v, ok := hooksProgramCache.Load(key); ok {
		return v.(*goja.Program), nil
	}
	prog, err := Compile(src, file)
	if err != nil {
		return nil, err
	}
	hooksProgramCache.Store(key, prog)
	return prog, nil
}

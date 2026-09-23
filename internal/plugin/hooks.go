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
	Key         *KeyRef      // 当前键(任务逐键/请求级;nil = ctx.key undefined,写窗 set/merge 抛错)
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
		injectSettingBuilders(vm)
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
	written      []string   // keySubmit 期间 set 写入的键 id(多次 set = 多条)
	collectWrite bool       // set 写 id 收集开关(keySubmit)
	stx          *StorageTx // storage 事务视图(执行期缓冲;成功归并/失败丢弃)
}

// 任务文件缺省路径
func taskEntry(tk HooksTask) string {
	if tk.Entry != "" {
		return tk.Entry
	}
	return "tasks/" + tk.Name + ".js"
}

// 族文件固定求值序(声明提取):settings.js → protocol.js → filters/<名>.js(包内序) → init.js → keys.js → tasks/<名>.js
// protocol/filter 参与声明提取:适配器与过滤器的参数槽与读值代码同址声明
func familyFiles(pkg *Package) []string {
	out := []string{}
	add := func(f string) {
		for _, e := range out {
			if e == f {
				return
			}
		}
		out = append(out, f)
	}
	if _, ok := pkg.Files["settings.js"]; ok {
		add("settings.js")
	}
	if p := pkg.Manifest.Parts.Protocol; p != nil {
		if _, ok := pkg.Files[ProtocolEntry]; ok {
			add(ProtocolEntry)
		}
	}
	for _, fp := range pkg.Manifest.Parts.Filters {
		e := ProtocolEntryFor("filter", fp.Name)
		if _, ok := pkg.Files[e]; ok {
			add(e)
		}
	}
	if _, ok := pkg.Files["init.js"]; ok {
		add("init.js")
	}
	if _, ok := pkg.Files["keys.js"]; ok {
		add("keys.js")
	}
	if h := pkg.Manifest.Parts.Hooks; h != nil {
		for _, tk := range h.Tasks {
			add(taskEntry(tk))
		}
	}
	return out
}

// ExtractDeclaration 安装期求值全部族文件收集 settings 片段(固定序;同名槽拒装,报文件+槽名)。
// 返回合并后的声明(settingsSchema 形态);失败 = 拒装(语法/求值/非法声明/槽名冲突)。
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
			if _, dup := merged[k]; dup {
				return nil, fmt.Errorf("%s: settings slot %q already declared by another family file", f, k)
			}
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
	tx := NewStorageTx(deps.Storage)
	deps.Storage = tx // storage 事务化:执行期写缓冲,成功归并/失败丢弃
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", taskEntry(tk))
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, settings: settings, taskName: tk.Name, stx: tx}, nil
}

// LoadInit 装载 init.js(onLoad;previous 注入经 ctx)
func LoadInit(pkg *Package, deps HooksDeps, settings map[string]any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	tx := NewStorageTx(deps.Storage)
	deps.Storage = tx
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", "init.js")
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, settings: settings, stx: tx}, nil
}

// LoadKeys 装载 keys.js(五钩子探测载体;setting 全局注入——keyForm 函数体运行期需要;ctx.settings 快照同族注入)
func LoadKeys(pkg *Package, deps HooksDeps, settings map[string]any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	tx := NewStorageTx(deps.Storage)
	deps.Storage = tx
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", "keys.js")
	if err != nil {
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, settings: settings, stx: tx}, nil
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
	// storage 事务:Begin→执行→成功归并/失败丢弃(超时中断走 err 分支丢弃)
	if h.stx != nil {
		h.stx.Begin()
	}
	err := h.call("task "+tk.Name, fn, budget, h.newCtx(budget, at, &tk, "task"))
	if err != nil {
		if h.stx != nil {
			h.stx.Drop()
		}
		return err
	}
	return h.commitStorage()
}

// RunOnLoad 加载钩子(exports 即处理器;ctx.key = undefined,包级生命周期)
func (h *HooksRuntime) RunOnLoad() error {
	fn := h.handler("onLoad")
	if fn == nil {
		fn = h.handler("")
	}
	if fn == nil {
		return nil
	}
	if h.stx != nil {
		h.stx.Begin()
	}
	if err := h.call("onLoad", fn, OnLoadTimeout, h.newCtx(OnLoadTimeout, time.Time{}, nil, "")); err != nil {
		if h.stx != nil {
			h.stx.Drop()
		}
		return err
	}
	return h.commitStorage()
}

// commitStorage 归并 storage 事务(stx 为空 = 无存储注入)
func (h *HooksRuntime) commitStorage() error {
	if h.stx == nil {
		return nil
	}
	return h.stx.Commit()
}

// RunNext next 形态自调度查询(exports.next(ctx) → unix ms | null)
func (h *HooksRuntime) RunNext() (int64, bool, error) {
	fn := h.handler("next")
	if fn == nil {
		return 0, false, fmt.Errorf("next form: next not exported in %s", h.pkg)
	}
	if h.stx != nil {
		h.stx.Begin()
	}
	v, err := h.callValue("next", fn, NextTimeoutMs, h.newCtx(NextTimeoutMs, time.Time{}, nil, "task"))
	if err != nil {
		if h.stx != nil {
			h.stx.Drop()
		}
		return 0, false, err
	}
	if err := h.commitStorage(); err != nil {
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

// keyHook keys.js 钩子调用基座(only keySubmit 可写 keys;其余钩子 ctx 无 merge 能力;storage 事务随钩子成败)
func (h *HooksRuntime) keyHook(name string, budgetMs int64, args ...goja.Value) (goja.Value, error) {
	fn := h.handler(name)
	if fn == nil {
		return nil, errKeyHookMissing
	}
	if h.stx != nil {
		h.stx.Begin()
	}
	window := ""
	if name == "keySubmit" {
		window = "submit"
	}
	v, err := h.callValue(name, fn, budgetMs, h.newCtx(budgetMs, time.Time{}, nil, window), args...)
	if err != nil {
		if h.stx != nil {
			h.stx.Drop()
		}
		return nil, err
	}
	if cErr := h.commitStorage(); cErr != nil {
		return nil, cErr
	}
	return v, nil
}

// CallKeyWrite 键写入归一化(仅管理台 PUT;undefined/null 返回 = 透传原值)
func (h *HooksRuntime) CallKeyWrite(id string, newData, oldData any) (any, error) {
	v, err := h.keyHook("keyWrite", KeyHookTimeoutMs,
		h.vm.ToValue(id), h.vm.ToValue(newData), h.vm.ToValue(oldData))
	if err != nil {
		return nil, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return newData, nil
	}
	return v.Export(), nil
}

// CallKeyRead 键详情解释(undefined → nil;未导出 = ErrHookNotExported)
func (h *HooksRuntime) CallKeyRead(id string) (any, error) {
	if h.handler("keyRead") == nil {
		return nil, ErrHookNotExported
	}
	v, err := h.keyHook("keyRead", KeyHookTimeoutMs, h.vm.ToValue(id))
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

// CallKeySubmit 表单提交(创建新条目由回调内 ctx.keys.set;written 收集宿主生成 id,多次 set = 多条)
// 返回契约:errs 非空 = 字段级拒绝(GUI 标红);message 字段已删
func (h *HooksRuntime) CallKeySubmit(values map[string]any) ([]string, map[string]any, error) {
	if h.handler("keySubmit") == nil {
		return nil, nil, ErrHookNotExported
	}
	h.written = nil
	h.collectWrite = true
	v, err := h.keyHook("keySubmit", KeyFlowTimeoutMs, h.vm.ToValue(values))
	h.collectWrite = false
	if err != nil {
		return nil, nil, err
	}
	var errs map[string]any
	if v != nil && !goja.IsUndefined(v) && !goja.IsNull(v) {
		if m, ok := v.Export().(map[string]any); ok {
			if e, ok := m["errors"].(map[string]any); ok && len(e) > 0 {
				errs = e
			}
		}
	}
	return h.written, errs, nil
}

// Handler 导出函数暴露(装配面探测/调用)
func (h *HooksRuntime) Handler(name string) goja.Callable { return h.handler(name) }

// Deps 运行时依赖视图(宿主在调用钩子前注入键引用等)
func (h *HooksRuntime) Deps() *HooksDeps { return &h.deps }

// KeyHookTimeoutMs 归一化/读取钩子预算
const KeyHookTimeoutMs = 5 * 1000

// KeyFlowTimeoutMs 表单采集流程预算(出站登录慢于归一化)
const KeyFlowTimeoutMs = 10 * 1000

var errKeyHookMissing = fmt.Errorf("hook not implemented")

// HasExport 钩子是否已导出
func (h *HooksRuntime) HasExport(name string) bool { return h.handler(name) != nil }

// newCtx 构造本次调用的 ctx(http/keys/settings/task/cron)
// 写窗形态:taskWindow = set(data) 替换当前键 + merge(patch) 浅合并;submitWindow = set(data) 仅创建新条目
func (h *HooksRuntime) newCtx(budgetMs int64, at time.Time, tk *HooksTask, writeWindow string) *goja.Object {
	ctx := h.vm.NewObject()
	httpObj := h.vm.NewObject()
	_ = httpObj.Set("run", func(opts map[string]any) (map[string]any, error) {
		return h.doHTTP(opts, budgetMs)
	})
	_ = ctx.Set("http", httpObj)
	// ctx.key:当前键(无键 = undefined;JS 无任何按名触达通道)
	var keyObj *goja.Object
	if h.deps.Key != nil {
		keyObj = h.vm.NewObject()
		_ = keyObj.Set("id", h.deps.Key.ID)
		_ = keyObj.Set("data", h.deps.Key.Data)
		_ = ctx.Set("key", keyObj)
	}
	keysObj := h.vm.NewObject()
	// set:任务窗 = 整体替换当前键 data;submit 窗 = 创建新条目(data 任意 JSON)
	setKey := func(data any) (string, error) {
		if h.deps.Keys == nil {
			return "", fmt.Errorf("keys unavailable (package %s)", h.pkg)
		}
		var newID string
		if writeWindow == "submit" {
			// 创建窗:宿主生成 id,多条 set = 多条目(无当前键依赖)
			newID = h.deps.Keys.keySubmitID(h.deps.Now().UnixMilli())
		} else {
			if h.deps.Key == nil {
				return "", fmt.Errorf("keys: no current key context (package %s)", h.pkg)
			}
			newID = h.deps.Key.ID
		}
		exported := data
		if v, ok := data.(goja.Value); ok {
			exported = v.Export()
		}
		if _, err := h.deps.Keys.Set(h.pkg, newID, exported); err != nil {
			return "", err
		}
		// 运行中切片可见新 data(ctx.key 与 Go 侧同步)
		if writeWindow == "task" {
			h.deps.Key.Data = exported
			_ = keyObj.Set("data", exported)
		}
		return newID, nil
	}
	switch writeWindow {
	case "task", "submit":
		_ = keysObj.Set("set", func(data goja.Value) (string, error) {
			id, err := setKey(data)
			if err != nil {
				return "", err
			}
			if h.collectWrite {
				h.written = append(h.written, id)
			}
			return id, nil
		})
	}
	if writeWindow == "task" {
		_ = keysObj.Set("merge", func(patch map[string]any) error {
			if h.deps.Key == nil {
				return fmt.Errorf("keys: no current key context (package %s)", h.pkg)
			}
			base, ok := h.deps.Key.Data.(map[string]any)
			if !ok {
				return fmt.Errorf("keys.merge: current data is not an object")
			}
			merged := make(map[string]any, len(base)+len(patch))
			for k, v := range base {
				merged[k] = v
			}
			for k, v := range patch {
				merged[k] = v
			}
			_, err := setKey(merged)
			return err
		})
	}
	_ = ctx.Set("keys", keysObj)
	// ctx.storage:包级 KV(与全局 storage 同一事务视图;get/set/delete 三方法)
	if h.deps.Storage != nil {
		st := h.vm.NewObject()
		_ = st.Set("get", func(key string) (any, error) {
			v, ok := h.deps.Storage.Get(key)
			if !ok {
				return nil, nil
			}
			return v, nil
		})
		_ = st.Set("set", func(key, value string) error {
			if len(value) > MaxStorageValue {
				return fmt.Errorf("storage value exceeds limit")
			}
			return h.deps.Storage.Set(key, value)
		})
		_ = st.Set("delete", func(key string) { h.deps.Storage.Delete(key) })
		_ = ctx.Set("storage", st)
	}
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

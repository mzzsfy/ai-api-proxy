package plugin

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// HttpRequest ctx.http 请求载体
type HttpRequest struct {
	URL        string
	Method     string
	Headers    map[string]string
	Body       string
	Timeout    time.Duration // 已钳制后的最终值
	Transport  string        // 命名传输实例(空 = 缺省回退链)
	SessionKey string        // 代理会话粘滞键(透传)
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

// hooksEnv 单 runtime 求值环境:require 幂等单例 + 循环检测 + 模块注册表(Go 侧;重入经 host 原语)
type hooksEnv struct {
	pkg         *Package
	deps        HooksDeps
	q           *qjsRuntime
	cache       map[string]jsengine.Value // file → exports(runtime 内幂等单例)
	stack       []string                  // 求值栈(循环检测)
	withSetting bool                      // setting 构建器全局注入(keyForm 函数体运行期需要)
	strictWrap  bool                      // 模块严格模式包装(handler 池构建用)
}

// newHooksEnv 构造(runtime/util 注入一次;require 原语注册)
func newHooksEnv(pkg *Package, deps HooksDeps, withSetting bool) *hooksEnv {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	q, err := newQJSRuntime(HostDeps{PackageName: pkg.Manifest.Name, Storage: deps.Storage, Log: deps.Log, TransportEvict: deps.TransportEvict}, jsengine.Option{})
	if err != nil {
		panic(fmt.Sprintf("hooks env: %v", err)) // 构造失败=引擎不可用,装载路径显式失败(不 panic 于运行期)
	}
	env := &hooksEnv{pkg: pkg, deps: deps, q: q, cache: map[string]jsengine.Value{}, withSetting: withSetting}
	// require 闭包形态:每模块独立 require(捕获来源文件;经 host 原语驱动 Go 侧 loader)
	if err := q.inst.EvalString(`
globalThis.__requireFrom = function (from, spec) {
  __hostRequire(String(from), String(spec));
  return globalThis.__modPeek;
};
globalThis.__h_glue = function (ctx, fns, win) {
  ctx.http = {
    request: function (opts) { return fns.http_req(JSON.stringify(opts === undefined ? {} : opts)); }
  };
  ctx.keys = {};
  if (win === "task") {
    ctx.keys.set = function (data) {
      var r = fns.keys_set(JSON.stringify(data === undefined ? null : data));
      if (ctx.key && r !== undefined && r !== null) { ctx.key.data = data; }
      return r;
    };
    ctx.keys.merge = function (patch) {
      var r = fns.keys_merge(JSON.stringify(patch === undefined ? {} : patch));
      if (ctx.key && typeof ctx.key.data === "object" && ctx.key.data !== null) {
        var merged = {};
        for (var k in ctx.key.data) { merged[k] = ctx.key.data[k]; }
        for (var k2 in patch) { merged[k2] = patch[k2]; }
        ctx.key.data = merged;
      }
      return r;
    };
  }
  ctx.storage = {
    get: function (k) { var v = fns.st_get(String(k)); return v === null ? undefined : v; },
    set: function (k, v) { return fns.st_set(String(k), String(v)); },
    delete: function (k) { fns.st_del(String(k)); }
  };
};
`); err != nil {
		_ = q.Close()
		panic(fmt.Sprintf("hooks env require: %v", err))
	}
	if err := q.inst.RegisterFunc("__hostRequire", func(args []any) (any, error) {
		from, _ := args[0].(string)
		spec, _ := args[1].(string)
		if _, err := env.load(from, spec); err != nil {
			return nil, err
		}
		return true, nil
	}); err != nil {
		_ = q.Close()
		panic(fmt.Sprintf("hooks env require host: %v", err))
	}
	return env
}

// load 求值包内文件并返回 module.exports(幂等单例;循环 require 拒绝)。
// CommonJS 函数包装:每模块独立 module/exports/require 形参,互不串扰。
func (e *hooksEnv) load(fromFile, spec string) (jsengine.Value, error) {
	target := spec
	if fromFile != "" {
		resolved, err := e.resolve(fromFile, spec)
		if err != nil {
			return jsengine.Value{}, err
		}
		target = resolved
	} else if strings.HasPrefix(spec, "./") || strings.HasPrefix(spec, "../") {
		return jsengine.Value{}, fmt.Errorf("require %q: relative path without base file", spec)
	}
	if exp, ok := e.cache[target]; ok {
		return exp.Dup(), nil
	}
	for _, f := range e.stack {
		if f == target {
			return jsengine.Value{}, fmt.Errorf("circular require: %s → %s", strings.Join(e.stack, " → "), target)
		}
	}
	src, ok := e.pkg.Files[target]
	if !ok {
		return jsengine.Value{}, fmt.Errorf("file %q missing in package %s", target, e.pkg.Manifest.Name)
	}
	wrapped := "(function(module, exports, require){\n"
	if e.strictWrap {
		wrapped += "\"use strict\";\n"
	}
	wrapped += string(src) + "\n})"
	prog, err := hooksProgram(e.pkg, target, []byte(wrapped))
	if err != nil {
		return jsengine.Value{}, err
	}
	fnVal, err := e.q.inst.RunProgramValue(prog)
	if err != nil {
		return jsengine.Value{}, fmt.Errorf("run %s: %w", target, err)
	}
	defer fnVal.Free()
	exports, err := e.q.inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	defer exports.Free()
	module, err := e.q.inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	defer module.Free()
	if err := e.q.inst.SetProp(module, "exports", exports); err != nil {
		return jsengine.Value{}, err
	}
	// 模块级 require:闭包捕获本文件名,经全局原语驱动 loader
	requireVal, err := e.q.inst.EvalExpression(requireClosureJS(target))
	if err != nil {
		return jsengine.Value{}, err
	}
	defer requireVal.Free()
	e.stack = append(e.stack, target)
	callErr := e.q.inst.CallExpression(`(function(fn, mod, exp, req){ fn(mod, exp, req); })`, fnVal, module, exports, requireVal)
	e.stack = e.stack[:len(e.stack)-1]
	if callErr != nil {
		return jsengine.Value{}, fmt.Errorf("run %s: %w", target, callErr)
	}
	final, err := e.q.inst.GetProp(module, "exports")
	if err != nil {
		return jsengine.Value{}, err
	}
	e.cache[target] = final
	// 经全局引用通道交付(host func 无引用返回;require 侧读 __modPeek)
	if err := e.q.inst.SetGlobal("__modPeek", final); err != nil {
		return jsengine.Value{}, err
	}
	return final.Dup(), nil
}

// requireClosureJS 模块级 require 闭包(捕获来源文件)
func requireClosureJS(target string) string {
	return `(function(){ var f = ` + strconv.Quote(target) + `; return function (s) { return globalThis.__requireFrom(f, s); }; })()`
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
	pkg         string
	env         *hooksEnv
	inst        *jsengine.Instance
	exports     jsengine.Value
	deps        HooksDeps
	settings    map[string]any // ctx.settings 快照
	taskName    string         // ctx.task
	stx         *StorageTx     // storage 事务视图(执行期缓冲;成功归并/失败丢弃)
	loginFn     jsengine.Value // login/index.js 导出函数(GUI 凭据通道)
	loginParams any
	activeMs    int64  // 当前调用预算 ms(host 闭包读取;单线程执行期安全)
	writeWindow string // 键写窗("task" | "")
	hasCtxFuncs bool   // ctx 宿主原语已注册
}

// 任务文件缺省路径
func taskEntry(tk HooksTask) string {
	if tk.Entry != "" {
		return tk.Entry
	}
	return "tasks/" + tk.Name + ".js"
}

// 族文件固定求值序(声明提取):settings.js → protocol.js → filters/<名>.js(包内序) → init.js → tasks/<名>.js
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
	defer func() { _ = env.q.Close() }()
	merged := map[string]any{}
	for _, f := range familyFiles(pkg) {
		exp, err := env.load("", f)
		if err != nil {
			return nil, err
		}
		defer exp.Free()
		frag, err := env.q.inst.GetProp(exp, "settings")
		if err != nil {
			return nil, err
		}
		if frag.IsUndefined() || frag.IsNull() {
			frag.Free()
			continue
		}
		b, err := frag.JSON()
		frag.Free()
		if err != nil {
			return nil, fmt.Errorf("%s: settings export must be an object", f)
		}
		var fm map[string]any
		if err := json.Unmarshal(b, &fm); err != nil {
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
		_ = env.q.Close()
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, env: env, inst: env.q.inst, exports: exp, deps: deps, settings: settings, taskName: tk.Name, stx: tx}, nil
}

// LoadInit 装载 init.js(onLoad;previous 注入经 ctx)
func LoadInit(pkg *Package, deps HooksDeps, settings map[string]any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	tx := NewStorageTx(deps.Storage)
	deps.Storage = tx
	env := newHooksEnv(pkg, deps, true)
	exp, err := env.load("", "init.js")
	if err != nil {
		_ = env.q.Close()
		return nil, err
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, env: env, inst: env.q.inst, exports: exp, deps: deps, settings: settings, stx: tx}, nil
}

// Close 释放 runtime(任务一次性使用;长驻由调用方管理)
func (h *HooksRuntime) Close() {
	h.exports.Free()
	h.exports = jsengine.Value{}
	if h.loginFn.VM() != nil {
		h.loginFn.Free()
		h.loginFn = jsengine.Value{}
	}
	_ = h.env.q.Close()
}

// handler 取导出函数(非函数 = 未实现;调用方负责 Free)
func (h *HooksRuntime) handler(name string) jsengine.Value {
	if h.exports.VM() == nil || name == "" {
		if h.exports.VM() == nil {
			return jsengine.Value{}
		}
	}
	if name == "" {
		return h.exports.Dup()
	}
	v, err := h.inst.GetProp(h.exports, name)
	if err != nil {
		return jsengine.Value{}
	}
	defer v.Free()
	if v.IsUndefined() || v.IsNull() {
		return jsengine.Value{}
	}
	isFn, err := h.inst.IsFunction(v)
	if err != nil || !isFn {
		return jsengine.Value{}
	}
	return v.Dup()
}

// HasTask 任务处理器是否已导出(未实现 = 调用期报错)
func (h *HooksRuntime) HasTask(name string) bool {
	fn := h.handler("handler")
	if fn.VM() != nil {
		fn.Free()
		return true
	}
	fn = h.handler(name)
	if fn.VM() != nil {
		fn.Free()
		return true
	}
	return false
}

// RunTask 定时任务(crontab 行:处理器 = exports.handler 或 exports.<taskName>;timeoutMs 钳制)
func (h *HooksRuntime) RunTask(tk HooksTask, at time.Time) error {
	fn := h.handler("handler")
	if fn.VM() == nil {
		fn = h.handler(tk.Name)
	}
	if fn.VM() == nil {
		return fmt.Errorf("task %q: handler not exported in %s", tk.Name, h.pkg)
	}
	defer fn.Free()
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
	ctxVal, err := h.newCtx(budget, at, &tk, "task")
	if err != nil {
		return err
	}
	defer ctxVal.Free()
	err = h.call("task "+tk.Name, fn, budget, ctxVal)
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
	if fn.VM() == nil {
		fn = h.handler("")
	}
	if fn.VM() == nil {
		return nil
	}
	defer fn.Free()
	if h.stx != nil {
		h.stx.Begin()
	}
	ctxVal, err := h.newCtx(OnLoadTimeout, time.Time{}, nil, "")
	if err != nil {
		return err
	}
	defer ctxVal.Free()
	if err := h.call("onLoad", fn, OnLoadTimeout, ctxVal); err != nil {
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
	if fn.VM() == nil {
		return 0, false, fmt.Errorf("next form: next not exported in %s", h.pkg)
	}
	defer fn.Free()
	if h.stx != nil {
		h.stx.Begin()
	}
	ctxVal, err := h.newCtx(NextTimeoutMs, time.Time{}, nil, "task")
	if err != nil {
		return 0, false, err
	}
	defer ctxVal.Free()
	v, err := h.callValue("next", fn, NextTimeoutMs, ctxVal)
	if err != nil {
		if h.stx != nil {
			h.stx.Drop()
		}
		return 0, false, err
	}
	defer v.Free()
	if err := h.commitStorage(); err != nil {
		return 0, false, err
	}
	if v.IsUndefined() || v.IsNull() {
		return 0, false, nil
	}
	n, ok := numericField(mustAny(v))
	if !ok {
		return 0, false, fmt.Errorf("next: want unix ms number or null, got %v", v.String())
	}
	return n, true, nil
}

// Handler 导出函数暴露(装配面探测/调用)
func (h *HooksRuntime) Handler(name string) jsengine.Value { return h.handler(name) }

// Deps 运行时依赖视图(宿主在调用钩子前注入键引用等)
func (h *HooksRuntime) Deps() *HooksDeps { return &h.deps }

// HasExport 钩子是否已导出
func (h *HooksRuntime) HasExport(name string) bool {
	fn := h.handler(name)
	if fn.VM() != nil {
		fn.Free()
		return true
	}
	return false
}

// newCtx 构造本次调用的 ctx(http/keys/settings/task/cron)
// 写窗形态:taskWindow = set(data) 替换当前键 + merge(patch) 浅合并
// 宿主函数实例级单次注册;方法经 __h_glue 装配(对象参数 JSON 文本归一)
func (h *HooksRuntime) newCtx(budgetMs int64, at time.Time, tk *HooksTask, writeWindow string) (jsengine.Value, error) {
	inst := h.inst
	h.activeMs = budgetMs
	h.writeWindow = writeWindow
	if !h.hasCtxFuncs {
		if err := h.registerCtxFuncs(); err != nil {
			return jsengine.Value{}, err
		}
		h.hasCtxFuncs = true
	}
	ctx, err := inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	// ctx.key:当前键(无键 = undefined;JS 无任何按名触达通道)
	if h.deps.Key != nil {
		keyObj, err := inst.NewObject()
		if err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		defer keyObj.Free()
		if err := inst.SetProp(keyObj, "id", h.deps.Key.ID); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		if err := inst.SetProp(keyObj, "data", h.deps.Key.Data); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		if err := inst.SetProp(ctx, "key", keyObj); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	if h.settings != nil {
		if err := inst.SetProp(ctx, "settings", h.settings); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	if h.taskName != "" {
		if err := inst.SetProp(ctx, "task", h.taskName); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	cronObj, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer cronObj.Free()
	runAt := h.deps.Now()
	if !at.IsZero() {
		runAt = at
	}
	if err := inst.SetProp(cronObj, "runAt", runAt.UTC().Format(time.RFC3339)); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(ctx, "cron", cronObj); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	// http/keys/storage 经胶水装配(参数归一 + undefined 语义)
	fns, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer fns.Free()
	for _, p := range []struct{ prop, global string }{
		{"http_req", "__h_http_request"},
		{"keys_set", "__h_keys_set"}, {"keys_merge", "__h_keys_merge"},
		{"st_get", "__h_storage_get"}, {"st_set", "__h_storage_set"}, {"st_del", "__h_storage_del"},
	} {
		fn, err := inst.GetGlobal(p.global)
		if err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		if err := inst.SetProp(fns, p.prop, fn); err != nil {
			fn.Free()
			ctx.Free()
			return jsengine.Value{}, err
		}
		fn.Free()
	}
	if err := inst.CallExpression(`(function(ctx, fns, win){ globalThis.__h_glue(ctx, fns, win); })`, ctx, fns, writeWindow); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	return ctx, nil
}

// registerCtxFuncs 注册 ctx 宿主原语(每实例一次;data 经 __hostToGo 文本通道)
func (h *HooksRuntime) registerCtxFuncs() error {
	reg := func(name string, fn func(args []any) (any, error)) error {
		return h.inst.RegisterFunc(name, fn)
	}
	argStr := func(args []any, i int) string {
		if i < len(args) {
			s, _ := args[i].(string)
			return s
		}
		return ""
	}
	unmarshal := func(args []any, i int) (map[string]any, error) {
		s := argStr(args, i)
		if s == "" {
			return map[string]any{}, nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	if err := reg("__h_http_request", func(args []any) (any, error) {
		opts, err := unmarshal(args, 0)
		if err != nil {
			return nil, err
		}
		return h.doHTTP(opts, h.activeMs)
	}); err != nil {
		return err
	}
	// keys.set:任务窗 = 整体替换当前键 data
	if err := reg("__h_keys_set", func(args []any) (any, error) {
		s := argStr(args, 0)
		if s == "" {
			return nil, fmt.Errorf("keys.set: data required")
		}
		var data any
		if err := json.Unmarshal([]byte(s), &data); err != nil {
			return nil, err
		}
		id, err := h.setKeyData(data, h.writeWindow)
		if err != nil {
			return nil, err
		}
		return id, nil
	}); err != nil {
		return err
	}
	if err := reg("__h_keys_merge", func(args []any) (any, error) {
		patch, err := unmarshal(args, 0)
		if err != nil {
			return nil, err
		}
		if h.deps.Key == nil {
			return nil, fmt.Errorf("keys.merge: no current key context (package %s)", h.pkg)
		}
		base, ok := h.deps.Key.Data.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("keys.merge: current data is not an object")
		}
		merged := make(map[string]any, len(base)+len(patch))
		for k, v := range base {
			merged[k] = v
		}
		for k, v := range patch {
			merged[k] = v
		}
		_, err = h.setKeyData(merged, h.writeWindow)
		return nil, err
	}); err != nil {
		return err
	}
	if err := reg("__h_storage_get", func(args []any) (any, error) {
		v, ok := h.deps.Storage.Get(argStr(args, 0))
		if !ok {
			return nil, nil
		}
		return v, nil
	}); err != nil {
		return err
	}
	if err := reg("__h_storage_set", func(args []any) (any, error) {
		if len(argStr(args, 1)) > MaxStorageValue {
			return nil, fmt.Errorf("storage value exceeds limit")
		}
		return nil, h.deps.Storage.Set(argStr(args, 0), argStr(args, 1))
	}); err != nil {
		return err
	}
	return reg("__h_storage_del", func(args []any) (any, error) {
		h.deps.Storage.Delete(argStr(args, 0))
		return nil, nil
	})
}

// setKeyData 键数据写通道(键写通道 = 任务窗/GUI 手编,无创建窗)
func (h *HooksRuntime) setKeyData(data any, writeWindow string) (string, error) {
	if h.deps.Keys == nil {
		return "", fmt.Errorf("keys unavailable (package %s)", h.pkg)
	}
	if h.deps.Key == nil {
		return "", fmt.Errorf("keys: no current key context (package %s)", h.pkg)
	}
	if _, err := h.deps.Keys.Set(h.pkg, h.deps.Key.ID, data); err != nil {
		return "", err
	}
	// 运行中切片可见新 data(ctx.key 与 Go 侧同步)
	if writeWindow == "task" {
		h.deps.Key.Data = data
	}
	return h.deps.Key.ID, nil
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
	transportName, _ := opts["transport"].(string)
	sessionKey, _ := opts["sessionKey"].(string)
	resp, err := h.deps.HTTP.Do(HttpRequest{URL: url, Method: method, Headers: headers, Body: body, Timeout: timeout,
		Transport: transportName, SessionKey: sessionKey})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": resp.Status, "headers": resp.Headers, "body": resp.Body}, nil
}

// numericField 数值字段提取(host 桥整数值可能为 int/float64)
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

// mustAny 值导出(错误视为 undefined 形态)
func mustAny(v jsengine.Value) any {
	a, err := v.Any()
	if err != nil {
		return nil
	}
	return a
}

// call 执行钩子(宿主等待 settled;rejection 经 ExceptionOf;超时中断)
func (h *HooksRuntime) call(name string, fn jsengine.Value, timeoutMs int64, ctxVal jsengine.Value) error {
	_, err := h.callValue(name, fn, timeoutMs, ctxVal)
	return err
}

// callValue 执行钩子并取返回值(调用方负责 Free)
func (h *HooksRuntime) callValue(name string, fn jsengine.Value, timeoutMs int64, ctxVal jsengine.Value, args ...any) (jsengine.Value, error) {
	allArgs := append([]any{ctxVal}, args...)
	h.activeMs = timeoutMs
	if err := h.inst.SetBudget(time.Duration(timeoutMs) * time.Millisecond); err != nil {
		return jsengine.Value{}, err
	}
	out, err := h.inst.Call(fn, allArgs...)
	if err != nil {
		return jsengine.Value{}, h.wrapCallErr(name, err, timeoutMs)
	}
	settled, _, rerr := h.inst.Await(out, time.Duration(timeoutMs)*time.Millisecond)
	out.Free()
	if rerr != nil {
		if rej, ok := h.inst.Rejection(rerr); ok {
			defer rej.Free()
			ee := h.inst.ExceptionOf(rej)
			return jsengine.Value{}, h.wrapCallErr(name, ee, timeoutMs)
		}
		return jsengine.Value{}, h.wrapCallErr(name, rerr, timeoutMs)
	}
	return settled, nil
}

// wrapCallErr 超时中断归一(ErrHookTimeout 哨兵);其余带名包装
func (h *HooksRuntime) wrapCallErr(name string, err error, timeoutMs int64) error {
	ee, ok := err.(*jsengine.EngineError)
	if ok && ee.Kind == jsengine.KindInterrupt {
		return fmt.Errorf("%s: %w (after %dms)", name, errHookTimeout, timeoutMs)
	}
	return fmt.Errorf("%s: %w", name, err)
}

// errHookTimeout hook 单次预算耗尽(调用方丢弃当次结果;池实例按 broken 丢弃)
var errHookTimeout = errors.New("js hook timeout")

// NextTimeoutMs next 查询预算(自调度链)
const NextTimeoutMs = 5 * 1000

// hooksProgramCache 编译产物缓存(键=name|revision|file|src 摘要;字节码跨实例共享安全;
// 逐文件条目,体量极小,不做事后清理)
var hooksProgramCache sync.Map

// hooksProgram 取编译产物,命中缓存免编译;失败不缓存
func hooksProgram(pkg *Package, file string, src []byte) (*jsengine.Program, error) {
	sum := sha256.Sum256(src)
	key := fmt.Sprintf("%s|%d|%s|%x", pkg.Manifest.Name, pkg.Revision, file, sum[:8])
	if v, ok := hooksProgramCache.Load(key); ok {
		return v.(*jsengine.Program), nil
	}
	prog, err := Compile(src, file)
	if err != nil {
		return nil, err
	}
	hooksProgramCache.Store(key, prog)
	return prog, nil
}

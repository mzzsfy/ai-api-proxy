package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// ─── JS 部件 → 管道契约适配(runtime 池化;hook 全同步;超时预算 Interrupt)───

// PoolEnvVar 池大小环境变量逃生
const PoolEnvVar = "API_PROXY_POOL_SIZE"

// poolSize 池大小:env 逃生 > GOMAXPROCS
func poolSize() int {
	if v := os.Getenv(PoolEnvVar); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.GOMAXPROCS(0)
}

// gojaProtocol JS protocol 部件适配
// 并发模型:hook 粒度借还(池内并行,实例内串行);借出期间独占实例,
// util.secret 经实例 cursor(借出时按 ctx 更新,调用全程有效)
type gojaProtocol struct {
	name    string
	support pipeline.Supports
	pool    *runtimePool
}

// ClosePools 释放池(Reload 销毁;等待在途归零)
func (g *gojaProtocol) ClosePools() {
	g.pool.Close()
}

// NewProtocol 从包实例化 protocol 部件(池预热;绑定校验用首实例)
func NewProtocol(pkg *Package, params map[string]any, targetSecrets func(target, key string) (string, bool), targetSecretValues func(target string) map[string]string, storage StorageKV) (pipeline.Protocol, error) {
	return buildProtocol(pkg, params, targetSecrets, targetSecretValues, storage, true)
}

// buildProtocol 实例化主体;enforceSchema=false 供安装期结构校验(实例配置归上游,不阻塞安装)
func buildProtocol(pkg *Package, params map[string]any, targetSecrets func(target, key string) (string, bool), targetSecretValues func(target string) map[string]string, storage StorageKV, enforceSchema bool) (pipeline.Protocol, error) {
	part := pkg.Manifest.Parts.Protocol
	src := pkg.Files[part.Entry]
	prog, err := Compile(src, part.Entry)
	if err != nil {
		return nil, err
	}
	if enforceSchema {
		// 漂移矩阵:required 缺 → 注明待补字段;未知键剥离
		params, err = enforceConfig("protocol "+pkg.Manifest.Name, pkg.Manifest.Name, pkg.Manifest.ConfigSchema, params)
		if err != nil {
			return nil, err
		}
	}
	newDeps := func() HostDeps {
		return HostDeps{
			PackageName: pkg.Manifest.Name, Cursor: &TargetCursor{},
			TargetSecrets: targetSecrets, TargetSecretValues: targetSecretValues, Storage: storage,
		}
	}
	factory := func() (*hookInstance, error) {
		return newInstance(prog, part.Entry, params, newDeps())
	}
	first, err := factory()
	if err != nil {
		return nil, fmt.Errorf("protocol %s: %w", pkg.Manifest.Name, err)
	}
	// 双向绑定校验:声明 streaming ↔ mapEvent;non_streaming ↔ mapResponse(对称)
	hooks := first.hooks
	support := pipeline.Supports{Forms: part.Form, Features: part.Features}
	if err := validateProtocolBindings(pkg.Manifest.Name, part, hooks); err != nil {
		return nil, err
	}
	pool, err := newRuntimePool(poolSize(), QueueTimeout, factory)
	if err != nil {
		return nil, err
	}
	return &gojaProtocol{name: pkg.Manifest.Name, support: support, pool: pool}, nil
}

// validateProtocolBindings 声明↔实现双向对称 + buildRequest 必备
func validateProtocolBindings(name string, part *ProtocolPart, hooks *Hooks) error {
	for _, f := range part.Form {
		if f == "streaming" && hooks.MapEvent == nil {
			return fmt.Errorf("protocol %s: declares streaming but mapEvent missing", name)
		}
		if f == "non_streaming" && hooks.MapResponse == nil {
			return fmt.Errorf("protocol %s: declares non_streaming but mapResponse missing", name)
		}
	}
	if hooks.MapEvent != nil && !formDeclared(part.Form, "streaming") {
		return fmt.Errorf("protocol %s: mapEvent implemented but streaming not declared", name)
	}
	if hooks.MapResponse != nil && !formDeclared(part.Form, "non_streaming") {
		return fmt.Errorf("protocol %s: mapResponse implemented but non_streaming not declared", name)
	}
	if hooks.BuildRequest == nil {
		return fmt.Errorf("protocol %s: buildRequest missing", name)
	}
	return nil
}

// formDeclared 形态是否已声明
func formDeclared(forms []string, form string) bool {
	for _, f := range forms {
		if f == form {
			return true
		}
	}
	return false
}

func (g *gojaProtocol) Name() string { return g.name }
func (g *gojaProtocol) Supports() pipeline.Supports {
	return g.support
}

// HookTimeout 单次 hook 执行预算(超时 Interrupt,实例污染丢弃)
const HookTimeout = 250 * time.Millisecond

// errSyncViolation hook 返回 Promise(异步)= 同步性违规
var errSyncViolation = errors.New("hook returned Promise: sync violation")

// callHook 单实例上同步调用 hook;返回 broken=实例已污染(超时 Interrupt,定时器 goroutine 置位)
func callHook(inst *hookInstance, budget time.Duration, fn goja.Callable, args ...goja.Value) (goja.Value, bool, error) {
	var brokenFlag atomic.Bool
	timer := time.AfterFunc(budget, func() {
		inst.vm.Interrupt("hook timeout")
		brokenFlag.Store(true)
	})
	defer timer.Stop()
	out, err := fn(goja.Undefined(), args...)
	if err != nil {
		return nil, brokenFlag.Load(), err
	}
	if out != nil && !goja.IsUndefined(out) && !goja.IsNull(out) {
		if p, ok := out.Export().(*goja.Promise); ok && p != nil {
			return nil, brokenFlag.Load(), errSyncViolation
		}
	}
	return out, brokenFlag.Load(), nil
}

// borrowWrap 借还包装:fn 在借出实例上执行;broken 实例归还时丢弃
func (g *gojaProtocol) borrowWrap(fn func(inst *hookInstance) (any, bool, error)) (any, error) {
	inst, err := g.pool.Borrow()
	if err != nil {
		return nil, pipeline.ErrPoolBusy
	}
	res, broken, err := fn(inst)
	g.pool.Return(inst, broken)
	return res, err
}

func (g *gojaProtocol) BuildRequest(ctx *pipeline.PipelineContext, pivot []byte) (pipeline.Request, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.BuildRequest, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, pivot))
		if err != nil {
			return nil, broken, err
		}
		m, ok := toMap(inst.vm, out)
		if !ok {
			return nil, broken, errors.New("buildRequest: non-object return")
		}
		return m, broken, nil
	})
	if err != nil {
		return pipeline.Request{}, err
	}
	m := res.(map[string]any)
	req := pipeline.Request{Method: "POST", Headers: map[string]string{}}
	if v, ok := m["url"].(string); ok {
		req.URL = v
	}
	if v, ok := m["method"].(string); ok {
		req.Method = v
	}
	if hm, ok := m["headers"].(map[string]any); ok {
		for k, v := range hm {
			if s, ok := v.(string); ok {
				req.Headers[k] = s
			}
		}
	}
	if v, ok := m["body"].(string); ok {
		req.Body = []byte(v)
	}
	if v, ok := m["stream"].(bool); ok {
		req.Stream = v
	}
	if req.URL == "" {
		return pipeline.Request{}, fmt.Errorf("buildRequest: url empty")
	}
	return req, nil
}

func (g *gojaProtocol) MapEvent(ctx *pipeline.PipelineContext, event []byte) ([]byte, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapEvent == nil {
			return string(event), false, nil // 未实现:原帧透传
		}
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapEvent, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, event))
		if err != nil {
			return nil, broken, err
		}
		if out == nil || goja.IsUndefined(out) || goja.IsNull(out) {
			return nil, broken, nil // 跳帧
		}
		s, ok := out.Export().(string)
		if !ok {
			return nil, broken, errors.New("mapEvent: non-string return")
		}
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return []byte(res.(string)), nil
}

func (g *gojaProtocol) MapResponse(ctx *pipeline.PipelineContext, body []byte) ([]byte, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapResponse == nil {
			return string(body), false, nil // 未实现:原体透传
		}
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapResponse, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, body))
		if err != nil {
			return nil, broken, err
		}
		s, ok := out.Export().(string)
		if !ok {
			return nil, broken, errors.New("mapResponse: non-string return")
		}
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	return []byte(res.(string)), nil
}

// MapError 可选(未实现返回哨兵错误,调用方走透传)
func (g *gojaProtocol) MapError(ctx *pipeline.PipelineContext, status int, body []byte) ([]byte, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapError == nil {
			return nil, false, errNotImplemented
		}
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapError,
			ctxToValue(inst.vm, ctx), inst.vm.ToValue(status), vmBytes(inst.vm, body))
		if err != nil {
			return nil, broken, err
		}
		if out == nil || goja.IsUndefined(out) {
			return nil, broken, errors.New("mapError: empty return")
		}
		if m, ok := toMap(inst.vm, out); ok {
			b, err := json.Marshal(m)
			return string(b), broken, err
		}
		s, _ := out.Export().(string)
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	return []byte(res.(string)), nil
}

// errNotImplemented 可选 hook 未实现
var errNotImplemented = errors.New("not implemented")

// targetName 上下文目标名(nil 安全)
func targetName(ctx *pipeline.PipelineContext) string {
	if ctx == nil {
		return ""
	}
	return ctx.Target.Name
}

// ─── filter 适配(池模型同 protocol)───

type gojaFilter struct {
	name string
	pool *runtimePool
}

// ClosePools 释放池
func (g *gojaFilter) ClosePools() { g.pool.Close() }

// NewFilter 从包实例化 filter 部件
func NewFilter(pkg *Package, part FilterPart, params map[string]any, targetSecrets func(target, key string) (string, bool), targetSecretValues func(target string) map[string]string, storage StorageKV) (pipeline.Filter, error) {
	return buildFilter(pkg, part, params, targetSecrets, targetSecretValues, storage, true)
}

// buildFilter 实例化主体;enforceSchema=false 供安装期结构校验
func buildFilter(pkg *Package, part FilterPart, params map[string]any, targetSecrets func(target, key string) (string, bool), targetSecretValues func(target string) map[string]string, storage StorageKV, enforceSchema bool) (pipeline.Filter, error) {
	src := pkg.Files[part.Entry]
	prog, err := Compile(src, part.Entry)
	if err != nil {
		return nil, err
	}
	if enforceSchema {
		// 漂移矩阵:required 缺 → 注明待补字段;未知键剥离
		params, err = enforceConfig("filter "+pkg.Manifest.Name+"/"+part.Name, part.Name, part.ConfigSchema, params)
		if err != nil {
			return nil, err
		}
	}
	newDeps := func() HostDeps {
		return HostDeps{
			PackageName: pkg.Manifest.Name, Cursor: &TargetCursor{},
			TargetSecrets: targetSecrets, TargetSecretValues: targetSecretValues, Storage: storage,
		}
	}
	factory := func() (*hookInstance, error) {
		return newInstance(prog, part.Entry, params, newDeps())
	}
	first, err := factory()
	if err != nil {
		return nil, fmt.Errorf("filter %s/%s: %w", pkg.Manifest.Name, part.Name, err)
	}
	if first.hooks.MapRequest == nil {
		return nil, fmt.Errorf("filter %s/%s: mapRequest missing", pkg.Manifest.Name, part.Name)
	}
	pool, err := newRuntimePool(poolSize(), QueueTimeout, factory)
	if err != nil {
		return nil, err
	}
	return &gojaFilter{name: pkg.Manifest.Name + "/" + part.Name, pool: pool}, nil
}

func (g *gojaFilter) Name() string { return g.name }

func (g *gojaFilter) borrowWrap(fn func(inst *hookInstance) (any, bool, error)) (any, error) {
	inst, err := g.pool.Borrow()
	if err != nil {
		return nil, pipeline.ErrPoolBusy
	}
	res, broken, err := fn(inst)
	g.pool.Return(inst, broken)
	return res, err
}

func (g *gojaFilter) MapRequest(ctx *pipeline.PipelineContext, pivot []byte) ([]byte, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapRequest, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, pivot))
		if err != nil {
			return nil, broken, err
		}
		s, ok := out.Export().(string)
		if !ok {
			return nil, broken, errors.New("mapRequest: non-string return")
		}
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	return []byte(res.(string)), nil
}

func (g *gojaFilter) MapChunk(ctx *pipeline.PipelineContext, chunk []byte) ([]byte, error) {
	if g.pool == nil {
		return chunk, nil
	}
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapChunk == nil {
			return nil, false, nil
		}
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapChunk, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, chunk))
		if err != nil {
			return nil, broken, err
		}
		if out == nil || goja.IsUndefined(out) || goja.IsNull(out) {
			return nil, broken, nil
		}
		s, ok := out.Export().(string)
		if !ok {
			return nil, broken, errors.New("mapChunk: non-string return")
		}
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return []byte(res.(string)), nil
}

func (g *gojaFilter) MapResponse(ctx *pipeline.PipelineContext, resp []byte) ([]byte, error) {
	res, err := g.borrowWrap(func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapResponse == nil {
			return nil, false, nil
		}
		inst.cursor.Set(targetName(ctx))
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapResponse, ctxToValue(inst.vm, ctx), vmBytes(inst.vm, resp))
		if err != nil {
			return nil, broken, err
		}
		s, ok := out.Export().(string)
		if !ok {
			return nil, broken, errors.New("mapResponse: non-string return")
		}
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return []byte(res.(string)), nil
}

// ctxToValue PipelineContext → JS 可读对象(upstream 快照/target/state/vars);nil 安全
func ctxToValue(vm *goja.Runtime, ctx *pipeline.PipelineContext) goja.Value {
	o := vm.NewObject()
	if ctx == nil {
		_ = o.Set("requestId", "")
		_ = o.Set("state", map[string]any{})
		_ = o.Set("target", vm.NewObject())
		_ = o.Set("upstream", vm.NewObject())
		_ = o.Set("vars", vm.NewObject())
		return o
	}
	_ = o.Set("requestId", ctx.RequestID)
	up := vm.NewObject()
	_ = up.Set("name", ctx.Upstream.Name)
	_ = up.Set("models", ctx.Upstream.Models)
	_ = o.Set("upstream", up)
	tg := vm.NewObject()
	_ = tg.Set("id", ctx.Target.ID)
	_ = tg.Set("name", ctx.Target.Name)
	_ = tg.Set("baseUrl", ctx.Target.BaseURL)
	_ = o.Set("target", tg)
	_ = o.Set("state", ctx.State)
	vars := vm.NewObject()
	_ = vars.Set("model", ctx.Vars.Model)
	_ = vars.Set("entryStream", ctx.Vars.EntryStream)
	_ = o.Set("vars", vars)
	return o
}

// vmBytes []byte → JS 字符串(pivot/chunk 权威形态=JSON 字节串)
func vmBytes(vm *goja.Runtime, b []byte) goja.Value { return vm.ToValue(string(b)) }

// logHookTimeout hook 超时可观测(池自动补建)
func logHookTimeout(part, hook string) {
	log.Printf("js hook timeout: %s/%s instance discarded", part, hook)
}

package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// ─── JS 部件 → 管道契约适配(runtime 池化;hook 允许 async,宿主等待 settled;超时预算中断)───

// PoolEnvVar 池大小环境变量逃生
const PoolEnvVar = "API_PROXY_POOL_SIZE"

// poolSizeDefault 池容量上限(JS 实例内存重;宿主函数注册数随实例线性,池化并行收益在 4 路后边际归零)
const poolSizeDefault = 4

// poolSize 池容量:env 逃生 > 上限
func poolSize() int {
	if v := os.Getenv(PoolEnvVar); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return poolSizeDefault
}

// jsProtocol JS protocol 部件适配
// 并发模型:hook 粒度借还(池内并行,实例内串行);借出期间独占实例
type jsProtocol struct {
	name     string
	declared string
	support  pipeline.Supports
	pool     *runtimePool
}

// ClosePools 释放池(Reload 销毁;等待在途归零)
func (g *jsProtocol) ClosePools() {
	g.pool.Close()
}

// NewProtocol 从包实例化 protocol 部件(params 已由调用方 ResolveParams 预校验;池预热;绑定校验用首实例)
func NewProtocol(pkg *Package, params map[string]any, storage StorageKV, packageKeyValues func() []string, transportEvict func(transport, scope, value string) error) (pipeline.Protocol, error) {
	return buildProtocol(pkg, params, storage, packageKeyValues, transportEvict, true)
}

// buildProtocol 实例化主体;enforceSchema=false 供安装期结构校验(参数校验归调用方,v2 声明统一)
func buildProtocol(pkg *Package, params map[string]any, storage StorageKV, packageKeyValues func() []string, transportEvict func(transport, scope, value string) error, _ bool) (pipeline.Protocol, error) {
	part := pkg.Manifest.Parts.Protocol
	src := pkg.Files[ProtocolEntry]
	prog, err := Compile(src, ProtocolEntry)
	if err != nil {
		return nil, err
	}
	newDeps := func() HostDeps {
		return HostDeps{
			PackageName: pkg.Manifest.Name, PackageKeyValues: packageKeyValues,
			Storage: storage, TransportEvict: transportEvict,
		}
	}
	// 声明的协议全名并入实例配置:部件按协议名渲染请求/事件(config.protocol)
	params = mergeProtocol(params, part.Protocol)
	factory := func() (*hookInstance, error) {
		return newInstance(prog, ProtocolEntry, params, newDeps())
	}
	first, err := factory()
	if err != nil {
		return nil, fmt.Errorf("protocol %s: %w", pkg.Manifest.Name, err)
	}
	defer first.closeInstance()
	// 形态支持由实现推导(有 mapEvent = 支持流式;有 mapResponse = 支持非流式),manifest 不声明
	hooks := first.hooks
	support := pipeline.Supports{Features: part.Features}
	if hooks.MapEvent.VM() != nil {
		support.Forms = append(support.Forms, string(FormStreaming))
	}
	if hooks.MapResponse.VM() != nil {
		support.Forms = append(support.Forms, string(FormNonStreaming))
	}
	if err := validateProtocolBindings(pkg.Manifest.Name, hooks); err != nil {
		return nil, err
	}
	pool, err := newRuntimePool(poolSize(), QueueTimeout, factory)
	if err != nil {
		return nil, err
	}
	return &jsProtocol{name: pkg.Manifest.Name, declared: part.Protocol, support: support, pool: pool}, nil
}

// mergeProtocol 声明协议名并入实例配置(不改写调用方 map;声明为准覆盖同名字段)
func mergeProtocol(params map[string]any, protocol string) map[string]any {
	out := make(map[string]any, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out["protocol"] = protocol
	return out
}

// validateProtocolBindings 实现必备校验(buildRequest 必实现;至少支持一形态)
func validateProtocolBindings(name string, hooks *Hooks) error {
	if hooks.BuildRequest.VM() == nil {
		return fmt.Errorf("protocol %s: buildRequest missing", name)
	}
	if hooks.MapEvent.VM() == nil && hooks.MapResponse.VM() == nil {
		return fmt.Errorf("protocol %s: no forms supported (implement mapEvent or mapResponse)", name)
	}
	return nil
}

func (g *jsProtocol) Name() string { return g.name }
func (g *jsProtocol) Supports() pipeline.Supports {
	return g.support
}

// Declared 主包声明的协议全名
func (g *jsProtocol) Declared() string { return g.declared }

// HookTimeout 单次 hook 执行预算(含 Promise settled 等待;超时中断,实例污染丢弃)
const HookTimeout = 250 * time.Millisecond

// callHook 单实例上调用 hook;宿主等待 settled(async 放开)。
// 返回 settled 值(调用方 Free)与 broken=实例已污染(内存超限/中断/引擎错误丢弃重建)
func callHook(inst *hookInstance, budget time.Duration, fn jsengine.Value, args ...any) (jsengine.Value, bool, error) {
	if err := inst.q.inst.SetBudget(budget); err != nil {
		return jsengine.Value{}, true, err
	}
	out, err := inst.q.inst.Call(fn, args...)
	if err != nil {
		return jsengine.Value{}, brokenFromErr(err), err
	}
	settled, _, rerr := inst.q.inst.Await(out, budget)
	out.Free()
	if rerr != nil {
		if rej, ok := inst.q.inst.Rejection(rerr); ok {
			defer rej.Free()
			ee := inst.q.inst.ExceptionOf(rej)
			return jsengine.Value{}, ee.Kind != jsengine.KindException, ee
		}
		return jsengine.Value{}, brokenFromErr(rerr), rerr
	}
	return settled, false, nil
}

// brokenFromErr 引擎级错误 = 实例污染(内存超限/中断/引擎内部);JS 逻辑异常不污染
func brokenFromErr(err error) bool {
	ee, ok := err.(*jsengine.EngineError)
	if !ok {
		return true
	}
	return ee.Kind != jsengine.KindException
}

// borrowWrap 借还包装:出池按 pctx.Key 无条件重绑键槽(含空值清槽,防池化残留);broken 实例归还时丢弃
func (g *jsProtocol) borrowWrap(ctx *pipeline.PipelineContext, fn func(inst *hookInstance) (any, bool, error)) (any, error) {
	inst, err := g.pool.Borrow()
	if err != nil {
		return nil, pipeline.ErrPoolBusy
	}
	inst.rebindKey(ctx)
	res, broken, err := fn(inst)
	g.pool.Return(inst, broken)
	return res, err
}

// rebindKey 键槽重绑(nil 安全:非池实例无槽)
func (inst *hookInstance) rebindKey(ctx *pipeline.PipelineContext) {
	if inst.keyRef == nil {
		return
	}
	if ctx != nil && ctx.Key != nil {
		*inst.keyRef = *ctx.Key
	} else {
		*inst.keyRef = KeyRef{}
	}
}

func (g *jsProtocol) BuildRequest(ctx *pipeline.PipelineContext, entry []byte) (pipeline.Request, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.BuildRequest, ctxVal, string(entry))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		m, ok := valueToMap(out)
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
	if v, ok := m["transport"].(string); ok {
		req.Transport = v
	}
	if req.URL == "" {
		return pipeline.Request{}, fmt.Errorf("buildRequest: url empty")
	}
	// 会话亲和素材由 host 注入(插件输出不携带凭据)
	if ctx != nil {
		req.Model = ctx.Vars.Model
	}
	return req, nil
}

func (g *jsProtocol) MapEvent(ctx *pipeline.PipelineContext, event []byte) ([]byte, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapEvent.VM() == nil {
			// 未实现:声明缺 streaming 时 validateProtocolBindings 已拒装,此路径仅为防御
			return nil, false, errors.New("mapEvent not implemented")
		}
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapEvent, ctxVal, string(event))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		if out.IsUndefined() || out.IsNull() {
			return nil, broken, nil // 裸值 undefined/null:跳帧
		}
		exported, err := out.Any()
		if err != nil {
			return nil, broken, err
		}
		if s, ok := exported.(string); ok {
			// 返回 JSON 文本:可以是声明协议事件对象"数组"或"单个对象",两者统一归一为数组
			if s == "" {
				return nil, broken, nil
			}
			var items []any
			if err := json.Unmarshal([]byte(s), &items); err != nil {
				var one map[string]any
				if err2 := json.Unmarshal([]byte(s), &one); err2 != nil {
					return nil, broken, fmt.Errorf("mapEvent: non-json return")
				}
				items = []any{one}
			}
			if len(items) == 0 {
				return nil, broken, nil
			}
			b, err := json.Marshal(items)
			if err != nil {
				return nil, broken, fmt.Errorf("mapEvent: marshal %w", err)
			}
			return string(b), broken, nil
		}
		// 返回裸数组:声明协议事件对象数组;数组长度为 0 亦视为跳帧
		b, err := out.JSON()
		if err != nil {
			return nil, broken, fmt.Errorf("mapEvent: marshal %w", err)
		}
		if string(b) == "[]" || string(b) == "null" {
			return nil, broken, nil
		}
		return string(b), broken, nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return []byte(res.(string)), nil
}

func (g *jsProtocol) MapResponse(ctx *pipeline.PipelineContext, body []byte) ([]byte, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapResponse.VM() == nil {
			// 未实现:声明缺 non_streaming 时 validateProtocolBindings 已拒装,此路径仅为防御
			return nil, false, errors.New("mapResponse not implemented")
		}
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapResponse, ctxVal, string(body))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		s, ok := exportedString(out)
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
func (g *jsProtocol) MapError(ctx *pipeline.PipelineContext, status int, body []byte) ([]byte, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapError.VM() == nil {
			return nil, false, errNotImplemented
		}
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapError, ctxVal, status, string(body))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		if out.IsUndefined() {
			return nil, broken, errors.New("mapError: empty return")
		}
		if m, ok := valueToMap(out); ok {
			b, err := json.Marshal(m)
			return string(b), broken, err
		}
		s, _ := exportedString(out)
		return s, broken, nil
	})
	if err != nil {
		return nil, err
	}
	return []byte(res.(string)), nil
}

// errNotImplemented 可选 hook 未实现
var errNotImplemented = errors.New("not implemented")

// exportedString 导出值为字符串(Any 通道;对齐 goja Export string 判定)
func exportedString(v jsengine.Value) (string, bool) {
	if v.VM() == nil {
		return "", false
	}
	a, err := v.Any()
	if err != nil {
		return "", false
	}
	s, ok := a.(string)
	return s, ok
}

// ─── filter 适配(池模型同 protocol)───

type jsFilter struct {
	name string
	pool *runtimePool
}

// ClosePools 释放池
func (g *jsFilter) ClosePools() { g.pool.Close() }

// NewFilter 从包实例化 filter 部件(params 已由调用方 ResolveParams 预校验)
func NewFilter(pkg *Package, part FilterPart, params map[string]any, storage StorageKV, packageKeyValues func() []string, transportEvict func(transport, scope, value string) error) (pipeline.Filter, error) {
	return buildFilter(pkg, part, params, storage, packageKeyValues, transportEvict, true)
}

// buildFilter 实例化主体;末位 bool 为历史签名占位(v2 参数校验归调用方)
func buildFilter(pkg *Package, part FilterPart, params map[string]any, storage StorageKV, packageKeyValues func() []string, transportEvict func(transport, scope, value string) error, _ bool) (pipeline.Filter, error) {
	entry := ProtocolEntryFor("filter", part.Name)
	src := pkg.Files[entry]
	prog, err := Compile(src, entry)
	if err != nil {
		return nil, err
	}
	newDeps := func() HostDeps {
		return HostDeps{
			PackageName: pkg.Manifest.Name, PackageKeyValues: packageKeyValues,
			Storage: storage, TransportEvict: transportEvict,
		}
	}
	factory := func() (*hookInstance, error) {
		return newInstance(prog, entry, params, newDeps())
	}
	first, err := factory()
	if err != nil {
		return nil, fmt.Errorf("filter %s/%s: %w", pkg.Manifest.Name, part.Name, err)
	}
	defer first.closeInstance()
	if first.hooks.MapRequest.VM() == nil {
		return nil, fmt.Errorf("filter %s/%s: mapRequest missing", pkg.Manifest.Name, part.Name)
	}
	pool, err := newRuntimePool(poolSize(), QueueTimeout, factory)
	if err != nil {
		return nil, err
	}
	return &jsFilter{name: pkg.Manifest.Name + "/" + part.Name, pool: pool}, nil
}

func (g *jsFilter) Name() string { return g.name }

func (g *jsFilter) borrowWrap(ctx *pipeline.PipelineContext, fn func(inst *hookInstance) (any, bool, error)) (any, error) {
	inst, err := g.pool.Borrow()
	if err != nil {
		return nil, pipeline.ErrPoolBusy
	}
	inst.rebindKey(ctx)
	res, broken, err := fn(inst)
	g.pool.Return(inst, broken)
	return res, err
}

func (g *jsFilter) MapRequest(ctx *pipeline.PipelineContext, entry []byte) ([]byte, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapRequest, ctxVal, string(entry))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		s, ok := exportedString(out)
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

func (g *jsFilter) MapChunk(ctx *pipeline.PipelineContext, chunk []byte) ([]byte, error) {
	if g.pool == nil {
		return chunk, nil
	}
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapChunk.VM() == nil {
			return nil, false, nil
		}
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapChunk, ctxVal, string(chunk))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		if out.IsUndefined() || out.IsNull() {
			return nil, broken, nil
		}
		s, ok := exportedString(out)
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

func (g *jsFilter) MapResponse(ctx *pipeline.PipelineContext, resp []byte) ([]byte, error) {
	res, err := g.borrowWrap(ctx, func(inst *hookInstance) (any, bool, error) {
		if inst.hooks.MapResponse.VM() == nil {
			return nil, false, nil
		}
		ctxVal, err := inst.q.ctxToValue(ctx)
		if err != nil {
			return nil, true, err
		}
		defer ctxVal.Free()
		out, broken, err := callHook(inst, HookTimeout, inst.hooks.MapResponse, ctxVal, string(resp))
		if err != nil {
			return nil, broken, err
		}
		defer out.Free()
		s, ok := exportedString(out)
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

// logHookTimeout hook 超时可观测(池自动补建)
func logHookTimeout(part, hook string) {
	slog.Warn("js hook timeout: instance discarded", "part", part, "hook", hook)
}

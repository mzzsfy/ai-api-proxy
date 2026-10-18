// login/inspect 装载:GUI 凭据通道(hooks runtime;login ctx 契约 = params/http/storage/util,无锁无 reply)
package plugin

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// LoginFlowTimeoutMs 登录流程预算(出站登录慢于普通钩子)
const LoginFlowTimeoutMs = 60 * 1000

// AuthoritativeStore ③ 直写打标视图(外部写通道:login GUI/桥 storage;实现 StorageKV)
type AuthoritativeStore struct{ S *SlotStore }

func (a AuthoritativeStore) Get(key string) (string, bool) {
	v, _, err := a.S.GetAuth(key)
	return v, err == nil
}

func (a AuthoritativeStore) Set(key, value string) error {
	return a.S.ExternalWrite(key, value)
}

func (a AuthoritativeStore) Delete(key string) {
	_ = a.S.Delete(key)
}

// LoadLogin 装载 login/index.js(导出 = 单函数;返回值经宿主 Keys.Set 落库,storage 直写③打权威标)
func LoadLogin(pkg *Package, deps HooksDeps, params any) (*HooksRuntime, error) {
	deps = normalizeDeps(deps)
	env := newHooksEnv(pkg, deps, false)
	exp, err := env.load("", LoginEntry)
	if err != nil {
		_ = env.q.Close()
		return nil, fmt.Errorf("load %s: %w", LoginEntry, err)
	}
	isFn, err := env.q.inst.IsFunction(exp)
	if err != nil || !isFn {
		exp.Free()
		_ = env.q.Close()
		return nil, fmt.Errorf("%s: export must be a function", LoginEntry)
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, env: env, inst: env.q.inst, exports: exp, deps: deps, loginFn: exp.Dup(), loginParams: params}, nil
}

// RunLogin 调 login/index.js(params 注入 ctx;返回值 = 账号 data,落库归宿主)。
// 返回值经 JSON 通道深导出(quickjs Any() 对对象只给引用壳)
func (h *HooksRuntime) RunLogin() (any, error) {
	ctxVal, err := h.newLoginCtx()
	if err != nil {
		return nil, err
	}
	defer ctxVal.Free()
	v, err := h.callValue("login", h.loginFn, LoginFlowTimeoutMs, ctxVal)
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if v.IsUndefined() || v.IsNull() {
		return nil, fmt.Errorf("%s: login returned nothing", LoginEntry)
	}
	b, err := v.JSON()
	if err != nil {
		return nil, fmt.Errorf("%s: login result: %w", LoginEntry, err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%s: login result must be an object", LoginEntry)
	}
	return out, nil
}

// newLoginCtx login ctx 契约:{params, http, storage}(util 为全局;无 key/keys/settings/cron)。
// 宿主原语复用 ctx 契约函数(每实例一次;login 实例独立 runtime,无写窗语义)
func (h *HooksRuntime) newLoginCtx() (jsengine.Value, error) {
	inst := h.inst
	h.activeMs = LoginFlowTimeoutMs
	if !h.hasCtxFuncs {
		h.writeWindow = ""
		if err := h.registerCtxFuncs(); err != nil {
			return jsengine.Value{}, err
		}
		h.hasCtxFuncs = true
	}
	ctx, err := inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	if h.loginParams != nil {
		if err := inst.SetProp(ctx, "params", h.loginParams); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	httpObj, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer httpObj.Free()
	fn, err := inst.GetGlobal("__h_http_request")
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(httpObj, "request", fn); err != nil {
		fn.Free()
		ctx.Free()
		return jsengine.Value{}, err
	}
	fn.Free()
	if err := inst.SetProp(ctx, "http", httpObj); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if h.deps.Storage != nil {
		st, err := inst.NewObject()
		if err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		defer st.Free()
		for _, p := range []struct{ prop, global string }{
			{"get", "__h_storage_get"}, {"set", "__h_storage_set"}, {"delete", "__h_storage_del"},
		} {
			gfn, err := inst.GetGlobal(p.global)
			if err != nil {
				ctx.Free()
				return jsengine.Value{}, err
			}
			if err := inst.SetProp(st, p.prop, gfn); err != nil {
				gfn.Free()
				ctx.Free()
				return jsengine.Value{}, err
			}
			gfn.Free()
		}
		if err := inst.SetProp(ctx, "storage", st); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	return ctx, nil
}

// InspectTimeoutMs 展示变换预算
const InspectTimeoutMs = 30 * 1000

// LoadInspect 装载 inspect.js(可选;纯函数 data → 展示 JSON,无 ctx)
func LoadInspect(pkg *Package) (func(data any) (any, error), error) {
	if _, ok := pkg.Files["inspect.js"]; !ok {
		return nil, nil
	}
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name}, false)
	exp, err := env.load("", "inspect.js")
	if err != nil {
		_ = env.q.Close()
		return nil, fmt.Errorf("load inspect.js: %w", err)
	}
	isFn, err := env.q.inst.IsFunction(exp)
	if err != nil || !isFn {
		exp.Free()
		_ = env.q.Close()
		return nil, fmt.Errorf("inspect.js: export must be a function")
	}
	return func(data any) (any, error) {
		if err := env.q.inst.SetBudget(InspectTimeoutMs * time.Millisecond); err != nil {
			return nil, err
		}
		out, err := env.q.inst.Call(exp, data)
		if err != nil {
			return nil, err
		}
		settled, _, rerr := env.q.inst.Await(out, InspectTimeoutMs*time.Millisecond)
		out.Free()
		if rerr != nil {
			if rej, ok := env.q.inst.Rejection(rerr); ok {
				rej.Free()
				ee := env.q.inst.ExceptionOf(rej)
				return nil, ee
			}
			return nil, rerr
		}
		defer settled.Free()
		a, err := settled.Any()
		if err != nil {
			return nil, err
		}
		return a, nil
	}, nil
}

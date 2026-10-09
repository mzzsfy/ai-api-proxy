// login/inspect 装载:GUI 凭据通道(hooks runtime;login ctx 契约 = params/http/storage/util,无锁无 reply)
package plugin

import (
	"fmt"
	"time"

	"github.com/dop251/goja"
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
		return nil, fmt.Errorf("load %s: %w", LoginEntry, err)
	}
	fn, ok := goja.AssertFunction(exp)
	if !ok {
		return nil, fmt.Errorf("%s: export must be a function", LoginEntry)
	}
	return &HooksRuntime{pkg: pkg.Manifest.Name, vm: env.vm, exports: exp, deps: deps, loginFn: fn, loginParams: params}, nil
}

// RunLogin 调 login/index.js(params 注入 ctx;返回值 = 账号 data,落库归宿主)
func (h *HooksRuntime) RunLogin() (any, error) {
	v, err := h.callValue("login", h.loginFn, LoginFlowTimeoutMs, h.newLoginCtx())
	if err != nil {
		return nil, err
	}
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil, fmt.Errorf("%s: login returned nothing", LoginEntry)
	}
	return v.Export(), nil
}

// newLoginCtx login ctx 契约:{params, http, storage}(util 为全局;无 key/keys/settings/cron)
func (h *HooksRuntime) newLoginCtx() *goja.Object {
	ctx := h.vm.NewObject()
	_ = ctx.Set("params", h.loginParams)
	httpObj := h.vm.NewObject()
	_ = httpObj.Set("run", func(opts map[string]any) (map[string]any, error) {
		return h.doHTTP(opts, LoginFlowTimeoutMs)
	})
	_ = ctx.Set("http", httpObj)
	if h.deps.Storage != nil {
		st := h.vm.NewObject()
		_ = st.Set("get", func(key string) (any, error) {
			v, ok := h.deps.Storage.Get(key)
			if !ok || v == "" {
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
	return ctx
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
		return nil, fmt.Errorf("load inspect.js: %w", err)
	}
	fn, ok := goja.AssertFunction(exp)
	if !ok {
		return nil, fmt.Errorf("inspect.js: export must be a function")
	}
	return func(data any) (any, error) {
		env.vm.ClearInterrupt()
		timer := time.AfterFunc(InspectTimeoutMs, func() { env.vm.Interrupt(errHookTimeout) })
		defer timer.Stop()
		v, err := fn(goja.Undefined(), env.vm.ToValue(data))
		if err != nil {
			return nil, err
		}
		return v.Export(), nil
	}, nil
}

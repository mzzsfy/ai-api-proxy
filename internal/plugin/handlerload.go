// handler 装载期判据:导出形态求值一次(安装/升级;池构建期仅复核)
package plugin

import (
	"fmt"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// validateHandlerEntry handler/login 导出形态校验(handler 包;装载期单独求值,复用 hooksEnv 模块机制)
func validateHandlerEntry(pkg *Package) error {
	if !pkg.HasHandler() {
		return nil
	}
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name}, false)
	env.strictWrap = true // 与池构建同环境(严格包装),拒装点不后移
	defer func() { _ = env.q.Close() }()
	exp, err := env.load("", HandlerEntry)
	if err != nil {
		return fmt.Errorf("load %s: %w", HandlerEntry, err)
	}
	defer exp.Free()
	// settings 导出 = 拒装(handler 参数来自 ctx.params,不参与声明族序)
	if _, _, err := checkHandlerExports(HandlerEntry, exp); err != nil {
		return err
	}
	// login/index.js 存在时同样禁 settings 导出(页面参数经桥传入,ctx.params = 上游参数合并)
	if _, ok := pkg.Files[LoginEntry]; ok {
		lxp, err := env.load("", LoginEntry)
		if err != nil {
			return fmt.Errorf("load %s: %w", LoginEntry, err)
		}
		defer lxp.Free()
		s, err := env.q.inst.GetProp(lxp, "settings")
		if err != nil {
			return err
		}
		defer s.Free()
		if !s.IsUndefined() && !s.IsNull() {
			return fmt.Errorf("%s: settings export not allowed (login params come from the bridge payload)", LoginEntry)
		}
	}
	return nil
}

// checkHandlerExports 导出形态判据(settings 禁/主函数/prepare;装载判据与池构建复核共用)
// 返回值调用方负责 Free
func checkHandlerExports(entry string, exp jsengine.Value) (handlerFn, prepareFn jsengine.Value, err error) {
	inst := exp.VM()
	s, err := inst.GetProp(exp, "settings")
	if err != nil {
		return jsengine.Value{}, jsengine.Value{}, err
	}
	defer s.Free()
	if !s.IsUndefined() && !s.IsNull() {
		return jsengine.Value{}, jsengine.Value{}, fmt.Errorf("%s: settings export not allowed (params come from ctx.params)", entry)
	}
	fnV, err := inst.GetProp(exp, "handler") // 命名导出优先;module.exports = fn 为回退形态
	if err != nil {
		return jsengine.Value{}, jsengine.Value{}, err
	}
	defer func() {
		if fnV.VM() != nil {
			fnV.Free()
		}
	}()
	if fnV.IsUndefined() || fnV.IsNull() {
		fnV.Free()
		fnV = exp.Dup()
	}
	isFn, err := inst.IsFunction(fnV)
	if err != nil {
		return jsengine.Value{}, jsengine.Value{}, err
	}
	if !isFn {
		return jsengine.Value{}, jsengine.Value{}, fmt.Errorf("%s: export must be a function", entry)
	}
	handlerFn = fnV.Dup()
	pv, err := inst.GetProp(exp, "prepare")
	if err != nil {
		handlerFn.Free()
		return jsengine.Value{}, jsengine.Value{}, err
	}
	defer pv.Free()
	if !pv.IsUndefined() && !pv.IsNull() {
		pIsFn, err := inst.IsFunction(pv)
		if err != nil {
			handlerFn.Free()
			return jsengine.Value{}, jsengine.Value{}, err
		}
		if !pIsFn {
			handlerFn.Free()
			return jsengine.Value{}, jsengine.Value{}, fmt.Errorf("%s: prepare export must be a function", entry)
		}
		prepareFn = pv.Dup()
	}
	return handlerFn, prepareFn, nil
}

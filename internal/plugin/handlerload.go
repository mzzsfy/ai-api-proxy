// handler 装载期判据:导出形态求值一次(安装/升级;池构建期仅复核)
package plugin

import (
	"fmt"

	"github.com/dop251/goja"
)

// validateHandlerEntry handler/login 导出形态校验(handler 包;装载期单独求值,复用 hooksEnv 模块机制)
func validateHandlerEntry(pkg *Package) error {
	if !pkg.HasHandler() {
		return nil
	}
	env := newHooksEnv(pkg, HooksDeps{PackageName: pkg.Manifest.Name}, false)
	env.strictWrap = true // 与池构建同环境(严格包装),拒装点不后移
	exp, err := env.load("", HandlerEntry)
	if err != nil {
		return fmt.Errorf("load %s: %w", HandlerEntry, err)
	}
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
		if s := lxp.Get("settings"); s != nil && !goja.IsUndefined(s) && !goja.IsNull(s) {
			return fmt.Errorf("%s: settings export not allowed (login params come from the bridge payload)", LoginEntry)
		}
	}
	return nil
}

// checkHandlerExports 导出形态判据(settings 禁/主函数/prepare;装载判据与池构建复核共用)
func checkHandlerExports(entry string, exp *goja.Object) (goja.Callable, goja.Callable, error) {
	if s := exp.Get("settings"); s != nil && !goja.IsUndefined(s) && !goja.IsNull(s) {
		return nil, nil, fmt.Errorf("%s: settings export not allowed (params come from ctx.params)", entry)
	}
	fnV := exp.Get("handler") // 命名导出优先;module.exports = fn 为回退形态
	if fnV == nil || goja.IsUndefined(fnV) || goja.IsNull(fnV) {
		fnV = exp
	}
	handlerFn, ok := goja.AssertFunction(fnV)
	if !ok {
		return nil, nil, fmt.Errorf("%s: export must be a function", entry)
	}
	var prepareFn goja.Callable
	if pv := exp.Get("prepare"); pv != nil && !goja.IsUndefined(pv) && !goja.IsNull(pv) {
		prepareFn, ok = goja.AssertFunction(pv)
		if !ok {
			return nil, nil, fmt.Errorf("%s: prepare export must be a function", entry)
		}
	}
	return handlerFn, prepareFn, nil
}

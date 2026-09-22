// keys 钩子装配:写入/详情/表单采集通道(契约见 docs/feat-keys-hooks/implementation.md)
package server

import (
	"encoding/json"
	"log"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// wireKeyHooks 装配 keys 钩子通道(探测/写入/读取/表单采集注入 AdminDeps)
func wireKeyHooks(app *App) {
	pkgs := app.AdminDeps.Packages
	exec := &httpExecutor{trMgr: app.trMgr}
	deps := func(pkgName string) plugin.HooksDeps {
		return plugin.HooksDeps{
			PackageName: pkgName,
			HTTP:        exec,
			Keys:        pkgs.Keys(),
			Storage:     &kvStorage{db: app.St.DB(), ns: pkgName},
			Log:         hooksLog(pkgName),
		}
	}
	// loadKeys 探测装载(keys.js 缺失/包无 hooks = 零钩子)
	loadKeys := func(pkg string) (*plugin.HooksRuntime, error) {
		p, err := pkgs.GetPackage(pkg)
		if err != nil {
			return nil, err
		}
		if p.Manifest.Parts.Hooks == nil {
			return nil, plugin.ErrNoHooksPart
		}
		if _, ok := p.Files["keys.js"]; !ok {
			return nil, plugin.ErrNoHooksPart
		}
		return plugin.LoadKeys(p, deps(pkg))
	}

	// KeyHooksFunc 能力探测(每次请求装载,编译缓存命中毫秒级)
	app.AdminDeps.KeyHooksFunc = func(pkg string) (write, read, form bool) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return false, false, false
		}
		return rt.Handler("keyWrite") != nil, rt.Handler("keyRead") != nil, rt.Handler("keyForm") != nil
	}

	// KeyWriteFunc 单键写入编排:updatedAt 快检 → keyWrite 归一化(无钩子原样) → 单键轮转替换
	app.AdminDeps.KeyWriteFunc = func(pkg, key string, value any, baseUpdatedAt int64) (int64, bool, error) {
		p, err := pkgs.GetPackage(pkg)
		if err != nil {
			return 0, false, err
		}
		if !pkgs.IsEnabled(pkg) {
			return 0, false, plugin.ErrPackageDisabled
		}
		view := pkgs.Keys().View(pkg)
		docUpdatedAt, _ := view["updatedAt"].(int64)
		if docUpdatedAt != baseUpdatedAt {
			return 0, false, plugin.ErrVersionConflict
		}
		effective := value
		transformed := false
		// 无 keys.js 或未导出 keyWrite = 原样保存(降级语义)
		if p.Manifest.Parts.Hooks != nil {
			if _, ok := p.Files["keys.js"]; ok {
				rt, err := plugin.LoadKeys(p, deps(pkg))
				if err != nil {
					return 0, false, err
				}
				if rt.Handler("keyWrite") != nil {
					old, _ := view["current"].(map[string]any)[key]
					effective, err = rt.CallKeyWrite(key, value, old)
					if err != nil {
						return 0, false, err
					}
					transformed = !gojaEqual(effective, value)
				}
			}
		}
		updated, err := pkgs.Keys().SetKey(pkg, key, effective)
		if err != nil {
			return 0, transformed, err
		}
		return updated, transformed, nil
	}

	// KeyReadFunc 详情解释
	app.AdminDeps.KeyReadFunc = func(pkg, key string) (any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, plugin.ErrPackageDisabled
		}
		return rt.CallKeyRead(key)
	}

	// KeyFormFunc 表单声明
	app.AdminDeps.KeyFormFunc = func(pkg string) (any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, plugin.ErrPackageDisabled
		}
		return rt.CallKeyForm()
	}

	// KeyActionFunc 表单按钮回调(可出站)
	app.AdminDeps.KeyActionFunc = func(pkg, action string, values map[string]any) (any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, plugin.ErrPackageDisabled
		}
		return rt.CallKeyAction(action, values)
	}

	// KeySubmitFunc 表单提交(写入由回调内 ctx.keys.overwrite;written 拦截收集;errors=字段级拒绝)
	app.AdminDeps.KeySubmitFunc = func(pkg string, values map[string]any) ([]string, any, map[string]any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, nil, nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, nil, nil, plugin.ErrPackageDisabled
		}
		return rt.CallKeySubmit(values)
	}
	_ = log.Println
}

// gojaEqual JSON 语义等值(归一化前后比较;数值形态归一)
func gojaEqual(a, b any) bool {
	aj, errA := json.Marshal(a)
	bj, errB := json.Marshal(b)
	if errA != nil || errB != nil {
		return false
	}
	return string(aj) == string(bj)
}

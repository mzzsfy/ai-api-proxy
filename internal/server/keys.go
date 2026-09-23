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
	// keySettings keys 钩子的 ctx.settings 快照(keyRead/keySubmit 出站查询需要包参数如 base_url)
	keySettings := func(pkgName string) map[string]any { return settingsSnapshot(pkgs, pkgName) }
	// loadKeys 探测装载(keys.js 缺失/包无 hooks 能力 = 零钩子;keys.js 为独立族文件,不要求 manifest hooks 部件)
	loadKeys := func(pkg string) (*plugin.HooksRuntime, error) {
		p, err := pkgs.GetPackage(pkg)
		if err != nil {
			return nil, err
		}
		if _, ok := p.Files["keys.js"]; !ok {
			return nil, plugin.ErrNoHooksPart
		}
		return plugin.LoadKeys(p, deps(pkg), keySettings(pkg))
	}

	// KeyHooksFunc 能力探测(每次请求装载,编译缓存命中毫秒级)
	app.AdminDeps.KeyHooksFunc = func(pkg string) (write, read, form bool) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return false, false, false
		}
		return rt.Handler("keyWrite") != nil, rt.Handler("keyRead") != nil, rt.Handler("keyForm") != nil
	}

	// KeyWriteFunc 单键写入编排(仅管理台 PUT 过归一化):per-key updatedAt 快检 → keyWrite 归一化(无钩子原样) → Set 自动备份
	app.AdminDeps.KeyWriteFunc = func(pkg, key string, value any, baseUpdatedAt int64) (int64, bool, error) {
		p, err := pkgs.GetPackage(pkg)
		if err != nil {
			return 0, false, err
		}
		if !pkgs.IsEnabled(pkg) {
			return 0, false, plugin.ErrPackageDisabled
		}
		if cur := pkgs.Keys().UpdatedAt(pkg, key); cur != baseUpdatedAt {
			return 0, false, plugin.ErrVersionConflict
		}
		effective := value
		transformed := false
		// 无 keys.js 或未导出 keyWrite = 原样保存(降级语义)
		if p.Manifest.Parts.Hooks != nil {
			if _, ok := p.Files["keys.js"]; ok {
				rt, err := plugin.LoadKeys(p, deps(pkg), keySettings(pkg))
				if err != nil {
					return 0, false, err
				}
				if rt.Handler("keyWrite") != nil {
					var old any
					if e, ok := pkgs.Keys().Get(pkg, key); ok {
						old = e.Data
					}
					effective, err = rt.CallKeyWrite(key, value, old)
					if err != nil {
						return 0, false, err
					}
					transformed = !gojaEqual(effective, value)
				}
			}
		}
		updated, err := pkgs.Keys().Set(pkg, key, effective)
		if err != nil {
			return 0, transformed, err
		}
		return updated, transformed, nil
	}

	// KeyReadFunc 详情解释(仅 GET detail 触发;ctx.key = 被查看键)
	app.AdminDeps.KeyReadFunc = func(pkg, key string) (any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, plugin.ErrPackageDisabled
		}
		if e, ok := pkgs.Keys().Get(pkg, key); ok {
			rt.Deps().Key = &plugin.KeyRef{ID: e.ID, Data: e.Data}
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

	// KeySubmitFunc 表单提交(创建条目由回调内 ctx.keys.set;ids = 宿主生成新键 id;errors=字段级拒绝)
	app.AdminDeps.KeySubmitFunc = func(pkg string, values map[string]any) ([]string, map[string]any, error) {
		rt, err := loadKeys(pkg)
		if err != nil {
			return nil, nil, err
		}
		if !pkgs.IsEnabled(pkg) {
			return nil, nil, plugin.ErrPackageDisabled
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

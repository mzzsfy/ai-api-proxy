package plugin

import (
	"strings"
	"testing"
)

// ─── manifest parts.hooks.router 键路由钩子声明(BDD 场景)───

func routerPkg(mjson string, files map[string]string) *Package {
	return handlerPkgFiles(mjson, files)
}

// 场景: hooks.router 声明 + 缺省 router.js 存在 = 可装,与 tasks 并存合法
func TestHooksRouterManifest_Declaration(t *testing.T) {
	pkg := routerPkg(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{},"tasks":[{"name":"t","cron":"0 1 * * *"}]}}}`,
		map[string]string{
			"router.js":  "module.exports={select:function(){return null}}",
			"tasks/t.js": "module.exports={handler:function(ctx){}}",
		})
	if err := pkg.Validate(); err != nil {
		t.Fatalf("router pkg rejected: %v", err)
	}
	if pkg.Manifest.Parts.Hooks.Router == nil {
		t.Fatal("router declaration lost")
	}
}

// 场景: router entry 文件缺失 = 拒装(缺省与自定义路径)
func TestHooksRouterManifest_MissingEntry(t *testing.T) {
	def := routerPkg(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{}}}}`, nil)
	err := def.Validate()
	if err == nil || !strings.Contains(err.Error(), `entry "router.js" missing`) {
		t.Fatalf("default entry: %v", err)
	}

	custom := routerPkg(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{"entry":"lib/pick.js"}}}}`, nil)
	err = custom.Validate()
	if err == nil || !strings.Contains(err.Error(), `entry "lib/pick.js" missing`) {
		t.Fatalf("custom entry: %v", err)
	}
}

// 场景: router 声明计入合法插槽(仅 hooks.router 无 tasks 也成立)
func TestHooksRouterManifest_PartAlone(t *testing.T) {
	pkg := routerPkg(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"hooks":{"router":{}}}}`,
		map[string]string{"router.js": "module.exports={select:function(){return null}}"})
	if err := pkg.Validate(); err != nil {
		t.Fatalf("router-only pkg rejected: %v", err)
	}
}

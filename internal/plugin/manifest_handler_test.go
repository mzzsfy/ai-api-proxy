package plugin

import (
	"encoding/json"
	"strings"
	"testing"
)

// ─── manifest parts.handler / 拒装(BDD 场景)───

// handlerPkgFiles 构造 manifest+files 包
func handlerPkgFiles(mjson string, files map[string]string) *Package {
	pkg := &Package{Manifest: mustManifest(mjson), Files: map[string][]byte{}, Revision: 1}
	for k, v := range files {
		pkg.Files[k] = []byte(v)
	}
	return pkg
}

func mustManifest(mjson string) *Manifest {
	m := &Manifest{}
	if err := json.Unmarshal([]byte(mjson), m); err != nil {
		panic(err)
	}
	return m
}

// 场景: parts.handler 声明 + handler.js 存在 = 可装;与 protocol 同声明 = 拒装
func TestManifestHandlerDeclaration(t *testing.T) {
	ok := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`,
		map[string]string{"handler.js": "module.exports=function(ctx){}"})
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid handler pkg rejected: %v", err)
	}
	if !ok.HasHandler() {
		t.Fatal("HasHandler false")
	}

	both := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions","protocol":{"protocol":"openai-completions"}}}`,
		map[string]string{"handler.js": "module.exports=function(ctx){}", "protocol.js": "module.exports={}"})
	err := both.Validate()
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("mutex: %v", err)
	}
}

// 场景: handler 值非法 / handler.js 缺失 = 拒装
func TestManifestHandlerBinding(t *testing.T) {
	bad := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"grpc"}}`,
		map[string]string{"handler.js": "module.exports=function(ctx){}"})
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "unknown protocol") {
		t.Fatalf("unknown protocol: %v", err)
	}
	missing := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`, nil)
	if err := missing.Validate(); err == nil || !strings.Contains(err.Error(), "handler.js missing") {
		t.Fatalf("missing entry: %v", err)
	}
}

// 场景: keys.js 出现 = 拒装(任意路径)
func TestManifestKeysJSRejected(t *testing.T) {
	for _, f := range []string{"keys.js", "lib/keys.js"} {
		files := map[string]string{"handler.js": "module.exports=function(ctx){}", f: "module.exports={}"}
		pkg := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`, files)
		err := pkg.Validate()
		if err == nil || !strings.Contains(err.Error(), "keys.js is removed") {
			t.Fatalf("keys.js %s: %v", f, err)
		}
	}
}

// 场景: login/ 目录绑定——缺 index.js 或 ui/index.html = 拒装;齐备 = HasLogin
func TestManifestLoginBinding(t *testing.T) {
	base := `{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`
	hjs := map[string]string{"handler.js": "module.exports=function(ctx){}"}

	partial := handlerPkgFiles(base, merge(hjs, map[string]string{"login/index.js": "module.exports=function(ctx){}"}))
	if err := partial.Validate(); err == nil || !strings.Contains(err.Error(), "ui/index.html") {
		t.Fatalf("missing ui: %v", err)
	}
	partial2 := handlerPkgFiles(base, merge(hjs, map[string]string{"login/ui/index.html": "<html></html>", "login/ui/app.js": ""}))
	if err := partial2.Validate(); err == nil || !strings.Contains(err.Error(), "index.js") {
		t.Fatalf("missing js: %v", err)
	}
	full := handlerPkgFiles(base, merge(hjs, map[string]string{
		"login/index.js": "module.exports=function(ctx){}",
		"login/ui/index.html": "<html></html>",
		"login/ui/app.js": "",
	}))
	if err := full.Validate(); err != nil {
		t.Fatalf("complete login rejected: %v", err)
	}
	if !full.HasLogin() {
		t.Fatal("HasLogin false")
	}
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// 场景: 装载期判据——handler.js 导出非函数 = 拒装;导出 settings = 拒装;命名导出/prepare 合法
func TestHandlerLoadExports(t *testing.T) {
	fn := map[string]string{"handler.js": "module.exports=function(ctx){}"}
	good := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`, fn)
	if err := validateHandlerEntry(good); err != nil {
		t.Fatalf("good rejected: %v", err)
	}

	notFn := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`,
		map[string]string{"handler.js": "module.exports=42"})
	if err := validateHandlerEntry(notFn); err == nil || !strings.Contains(err.Error(), "must be a function") {
		t.Fatalf("non-function: %v", err)
	}

	settings := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`,
		map[string]string{"handler.js": "module.exports={settings:{a:1},handler:function(ctx){}}"})
	if err := validateHandlerEntry(settings); err == nil || !strings.Contains(err.Error(), "settings export") {
		t.Fatalf("settings export: %v", err)
	}

	named := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`,
		map[string]string{"handler.js": "module.exports={handler:function(ctx){},prepare:function(ctx){return 1}}"})
	if err := validateHandlerEntry(named); err != nil {
		t.Fatalf("named exports rejected: %v", err)
	}

	badPrepare := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"handler":"openai-completions"}}`,
		map[string]string{"handler.js": "module.exports={handler:function(ctx){},prepare:42}"})
	if err := validateHandlerEntry(badPrepare); err == nil || !strings.Contains(err.Error(), "prepare") {
		t.Fatalf("bad prepare: %v", err)
	}
}

// 场景: 非 handler 包跳过判据(protocol 包不受影响)
func TestHandlerLoadSkipsNonHandler(t *testing.T) {
	pkg := handlerPkgFiles(`{"manifestVersion":1,"name":"acme","version":"1.0","parts":{"protocol":{"protocol":"openai-completions"}}}`,
		map[string]string{"protocol.js": "module.exports={}"})
	if err := validateHandlerEntry(pkg); err != nil {
		t.Fatalf("protocol pkg touched: %v", err)
	}
}

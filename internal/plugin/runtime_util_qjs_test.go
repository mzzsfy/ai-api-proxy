package plugin

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dop251/goja"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// ─── 双引擎对照:同一表达式 goja 与 quickjs 各执行一次,断言结果一致 ───

// normJSON 结果规范化(Go json 键序;undefined→"null")
func normJSON(t *testing.T, s string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("norm %q: %v", s, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	return string(b)
}

// runBoth 双引擎执行;返回 (goja结果, quickjs结果, goja错误, quickjs错误)
func runBoth(t *testing.T, expr string) (string, string, error, error) {
	t.Helper()
	// goja 侧
	gvm := goja.New()
	bindUtil(gvm, HostDeps{})
	gv, gerr := gvm.RunString(expr)
	var gout string
	if gerr == nil {
		b, _ := json.Marshal(gv.Export())
		gout = string(b)
	}
	// quickjs 侧
	q, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatalf("qjs runtime: %v", err)
	}
	defer func() { _ = q.Close() }()
	var qout string
	qerr := q.inst.EvalString(`globalThis.__r = __hostToGo((function(){ return (` + expr + `); })());`)
	if qerr == nil {
		v, g := q.inst.GetGlobal("__r")
		if g != nil {
			qerr = g
		} else {
			defer v.Free()
			s, ae := v.Any()
			if ae != nil {
				qerr = ae
			} else {
				qout, _ = s.(string)
			}
		}
	}
	return gout, qout, gerr, qerr
}

// assertBothMatch 断言双引擎同结果或同错
func assertBothMatch(t *testing.T, expr string) {
	t.Helper()
	gout, qout, gerr, qerr := runBoth(t, expr)
	if (gerr == nil) != (qerr == nil) {
		t.Fatalf("%s: goja err=%v, qjs err=%v", expr, gerr, qerr)
	}
	if gerr != nil {
		return
	}
	if normJSON(t, gout) != normJSON(t, qout) {
		t.Fatalf("%s:\n  goja = %s\n  qjs  = %s", expr, gout, qout)
	}
}

// TestQJS_UtilParity 与 goja 版逐表达式对照
func TestQJS_UtilParity(t *testing.T) {
	for _, expr := range []string{
		// 对象操作
		`util.deepMerge({a:{x:1,y:2},arr:[1,2]}, {a:{y:3},arr:[9]})`,
		`util.deepMerge({a:1},{a:null})`,
		`util.deepClone({a:{b:[1,2],c:null}})`,
		`util.get({a:{b:[1,2]}}, "a.b[1]")`,
		`util.get({a:1}, "x.y")`,
		`util.get("not-obj", "x")`,
		`util.set({a:1}, "b.c", 2)`,
		`util.set({a:{b:1}}, "a.b", 9)`,
		`util.pick({a:1,b:2,c:3}, ["a","c","z"])`,
		`util.omit({a:1,b:2,c:3}, ["b"])`,
		// base64
		`util.b64encode("hi")`,
		`util.b64decode("aGk=")`,
		`util.b64urlEncode("hi?hi")`,
		`util.b64urlDecode("aG8_")`,
		// 摘要
		`util.sha256hex("abc")`,
		`util.hmacSha256hex("k","data")`,
		// 模板
		`util.template("{a}-{b}!{missing}", {a:1,b:"x"})`,
		`util.template("plain", "not-obj")`,
		// json 句柄
		`(function(){ var d = util.json('{"a":{"b":[1,2]},"c":"x"}'); return {get:d.get("a.b"), getArr:d.get("a"), missing:d.get("nope")===undefined, has:d.has("a.b"), hasNo:d.has("z")}; })()`,
		`(function(){ var d = util.json('{"arr":[1]}'); var r = d.ops([{op:"set",path:"arr.-1",value:"2"},{op:"remove",path:"nope"}]); return {r:r, f:d.finalize()}; })()`,
		`(function(){ try { util.json('not-json'); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		`(function(){ var d = util.json('{}'); try { d.ops([{op:"set",path:"x",value:5}]); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		`(function(){ try { util.json('{}').ops([{op:"bogus",path:"x"}]); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		// evict(无 transport 上下文)
		`(function(){ try { util.evict("t","s","v"); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		// 64 位整数(错误路径文案)
		`(function(){ try { util.u64be(new Uint8Array(8), -1); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		`(function(){ try { util.u64be(new Uint8Array(8), 1); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		`(function(){ try { util.u64be("not bytes", 0); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		`(function(){ try { util.u64be(new Uint8Array(4), 0); return "no-throw"; } catch(e) { return String(e.message); } })()`,
		// setting
		`setting.string({default:"x",label:"L"})`,
		`setting.enum({values:["a","b"]})`,
		`setting.int()`,
	} {
		assertBothMatch(t, expr)
	}
}

// TestQJS_BigEndianValues 64 位整数值(quickjs 侧独立断言 BigInt 字符串)
func TestQJS_BigEndianValues(t *testing.T) {
	q, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	for _, tt := range []struct{ expr, want string }{
		{`util.i64be(new Uint8Array([127,255,255,255,255,255,255,255]).buffer,0).toString()`, "9223372036854775807"},
		{`util.i64be(new Uint8Array([128,0,0,0,0,0,0,0]).buffer,0).toString()`, "-9223372036854775808"},
		{`util.u64be(new Uint8Array([255,255,255,255,255,255,255,255]).buffer,0).toString()`, "18446744073709551615"},
		{`util.u64be(new Uint8Array([0,0,0,0,0,0,0,1]).buffer,0).toString()`, "1"},
		{`util.u64be(new Uint8Array([99,0,0,0,0,0,0,0,0,1,88]).subarray(1,10),1).toString()`, "1"},
	} {
		if err := q.inst.EvalString(`globalThis.__r = (` + tt.expr + `).toString();`); err != nil {
			t.Fatalf("%s: %v", tt.expr, err)
		}
		v, g := q.inst.GetGlobal("__r")
		if g != nil {
			t.Fatalf("%s: %v", tt.expr, g)
		}
		if got := v.String(); got != tt.want {
			t.Fatalf("%s = %q, want %q", tt.expr, got, tt.want)
		}
		v.Free()
	}
}

// TestQJS_KeyAndStorage 逐键凭据与存储(nil 语义对齐)
func TestQJS_KeyAndStorage(t *testing.T) {
	q, err := newQJSRuntime(HostDeps{Key: &KeyRef{Data: map[string]any{"token": "T"}}, PackageName: "p"}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	got := evalQJSJSON(t, q, `({key: util.key().token, noKey: util.key().missing === undefined})`)
	if got != `{"key":"T","noKey":true}` {
		t.Fatalf("key parity: %s", got)
	}
	// 无键实例:key() = undefined
	q2, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()
	if err := q2.inst.EvalString(`globalThis.__r = (util.key() === undefined);`); err != nil {
		t.Fatal(err)
	}
	v, _ := q2.inst.GetGlobal("__r")
	defer v.Free()
	if v.String() != "true" {
		t.Fatalf("no-key must be undefined, got %s", v.String())
	}
}

// TestQJS_InspectMasking inspect 掩码与截断(与 goja maskSecrets 语义同源)
func TestQJS_InspectMasking(t *testing.T) {
	deps := HostDeps{PackageKeyValues: func() []string { return []string{"SECRET"} }}
	gvm := goja.New()
	bindUtil(gvm, deps)
	gv, err := gvm.RunString(`util.inspect({t:"SECRET",ok:"plain"})`)
	if err != nil {
		t.Fatal(err)
	}
	q, err := newQJSRuntime(deps, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	qout := evalQJSJSON(t, q, `JSON.parse(util.inspect({t:"SECRET",ok:"plain"}))`)
	if gv.String() != qout {
		t.Fatalf("inspect:\n  goja = %s\n  qjs  = %s", gv.String(), qout)
	}
	if !strings.Contains(qout, "***") || !strings.Contains(qout, "plain") {
		t.Fatalf("mask broken: %s", qout)
	}
}

// TestQJS_LogAndStorage log 掩码输出 + storage 生命周期
func TestQJS_LogAndStorage(t *testing.T) {
	var logs []string
	store := map[string]string{}
	deps := HostDeps{
		PackageKeyValues: func() []string { return []string{"SECRET"} },
		Log:              func(level, msg string) { logs = append(logs, level+":"+msg) },
		Storage:          &mapStorage{m: store},
	}
	q, err := newQJSRuntime(deps, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()

	if err := q.inst.EvalString(`log.info("hello", {a:1}); log.warn("SECRET"); log.error(42);`); err != nil {
		t.Fatal(err)
	}
	// goja sprintArgs 与 quickjs __u_fmt 同为 JS String() 语义
	want := []string{"info:hello [object Object]", "warn:***", "error:42"}
	if len(logs) != len(want) {
		t.Fatalf("logs = %v", logs)
	}
	for i := range want {
		if logs[i] != want[i] {
			t.Fatalf("log[%d] = %q, want %q", i, logs[i], want[i])
		}
	}

	if err := q.inst.EvalString(`storage.set("k","v1");`); err != nil {
		t.Fatalf("storage.set: %v", err)
	}
	got := evalQJSJSON(t, q, `({get: storage.get("k"), miss: storage.get("nope") === undefined})`)
	if got != `{"get":"v1","miss":true}` {
		t.Fatalf("storage: %s", got)
	}
	if err := q.inst.EvalString(`storage.delete("k");`); err != nil {
		t.Fatal(err)
	}
	if err := q.inst.EvalString(`storage.set("big", "x".repeat(70000));`); err == nil {
		t.Fatal("storage limit must throw")
	}
}

// TestQJS_EventstreamProtected util.eventstream 只读且可用(与 goja 版同脚本)
func TestQJS_EventstreamProtected(t *testing.T) {
	q, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	got := evalQJSJSON(t, q, `(function(){
	util.extraHelper = 42;
	const before = typeof util.extraHelper === "number";
	try { util.eventstream = {decode:function(){}}; } catch (_) {}
	return [before, typeof util.eventstream.decode];
})()`)
	if got != `[true,"function"]` {
		t.Fatalf("eventstream protection: %s", got)
	}
}

// TestQJS_AsyncUtil async 钩子内 await util 调用(迁移放开点)
func TestQJS_AsyncUtil(t *testing.T) {
	q, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if err := q.inst.EvalString(`globalThis.__afn = async function(){ const x = await Promise.resolve({b:2}); return util.deepMerge({a:1}, x); };`); err != nil {
		t.Fatal(err)
	}
	fn, err := q.inst.GetGlobal("__afn")
	if err != nil {
		t.Fatal(err)
	}
	defer fn.Free()
	res, err := q.inst.Call(fn)
	if err != nil {
		t.Fatalf("Call async: %v", err)
	}
	defer res.Free()
	settled, wasP, rerr := q.inst.Await(res, 250*1000*1000)
	if rerr != nil {
		t.Fatalf("Await: %v", rerr)
	}
	if !wasP {
		t.Fatal("must be promise")
	}
	defer settled.Free()
	got, err := settled.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if normJSON(t, string(got)) != `{"a":1,"b":2}` {
		t.Fatalf("async util result = %s", got)
	}
}

// mapStorage 测试用 StorageKV fake
type mapStorage struct{ m map[string]string }

func (s *mapStorage) Get(key string) (string, bool) {
	v, ok := s.m[key]
	return v, ok
}

func (s *mapStorage) Set(key, value string) error {
	s.m[key] = value
	return nil
}

func (s *mapStorage) Delete(key string) { delete(s.m, key) }

// evalQJSJSON 执行表达式并返回规范化 JSON
func evalQJSJSON(t *testing.T, q *qjsRuntime, expr string) string {
	t.Helper()
	if err := q.inst.EvalString(`globalThis.__r = JSON.stringify(` + expr + `);`); err != nil {
		t.Fatalf("eval %s: %v", expr, err)
	}
	v, g := q.inst.GetGlobal("__r")
	if g != nil {
		t.Fatalf("get __r: %v", g)
	}
	defer v.Free()
	return normJSON(t, v.String())
}

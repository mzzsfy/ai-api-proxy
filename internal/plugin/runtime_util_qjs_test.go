package plugin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// ─── quickjs util 单引擎断言(期望值 = 迁移前 goja 行为冻结;见迁移文档行为差异表)───

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

// newUtilVM 一次性 util runtime
func newUtilVM(t *testing.T, deps HostDeps) *qjsRuntime {
	t.Helper()
	q, err := newQJSRuntime(deps, jsengine.Option{})
	if err != nil {
		t.Fatalf("qjs runtime: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// evalQJSON 表达式 → __hostToGo 文本(undefined→"null")
func evalQJSON(t *testing.T, q *qjsRuntime, expr string) string {
	t.Helper()
	if err := q.inst.EvalString(`globalThis.__r = __hostToGo((function(){ return (` + expr + `); })());`); err != nil {
		t.Fatalf("eval %s: %v", expr, err)
	}
	v, g := q.inst.GetGlobal("__r")
	if g != nil {
		t.Fatalf("get __r: %v", g)
	}
	defer v.Free()
	s, err := v.Any()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	out, _ := s.(string)
	return out
}

// assertQJS 断言表达式结果与期望一致
func assertQJS(t *testing.T, expr, want string) {
	t.Helper()
	q := newUtilVM(t, HostDeps{})
	got := evalQJSON(t, q, expr)
	if normJSON(t, got) != normJSON(t, want) {
		t.Fatalf("%s:\n  got  = %s\n  want = %s", expr, got, want)
	}
}

func TestQJS_UtilParity(t *testing.T) {
	mac := hmac.New(sha256.New, []byte("k"))
	mac.Write([]byte("data"))
	hmacWant := `"` + hex.EncodeToString(mac.Sum(nil)) + `"`
	for _, tt := range []struct{ expr, want string }{
		// 对象操作
		{`util.deepMerge({a:{x:1,y:2},arr:[1,2]}, {a:{y:3},arr:[9]})`, `{"a":{"x":1,"y":3},"arr":[9]}`},
		{`util.deepMerge({a:1},{a:null})`, `{"a":null}`},
		{`util.deepClone({a:{b:[1,2],c:null}})`, `{"a":{"b":[1,2],"c":null}}`},
		{`util.get({a:{b:[1,2]}}, "a.b[1]")`, `2`},
		{`util.get({a:1}, "x.y")`, `null`},
		{`util.get("not-obj", "x")`, `null`},
		{`util.set({a:1}, "b.c", 2)`, `{"a":1,"b":{"c":2}}`},
		{`util.set({a:{b:1}}, "a.b", 9)`, `{"a":{"b":9}}`},
		{`util.pick({a:1,b:2,c:3}, ["a","c","z"])`, `{"a":1,"c":3}`},
		{`util.omit({a:1,b:2,c:3}, ["b"])`, `{"a":1,"c":3}`},
		// base64
		{`util.b64encode("hi")`, `"aGk="`},
		{`util.b64decode("aGk=")`, `"hi"`},
		{`util.b64urlEncode("hi?hi")`, `"aGk_aGk="`},
		{`util.b64urlDecode("aG8_")`, `"ho?"`},
		// 摘要
		{`util.sha256hex("abc")`, `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"`},
		{`util.hmacSha256hex("k","data")`, hmacWant},
		// 模板
		{`util.template("{a}-{b}!{missing}", {a:1,b:"x"})`, `"1-x!{missing}"`},
		{`util.template("plain", "not-obj")`, `"plain"`},
		// json 句柄(get/has 返回 JSON 原文,零失真)
		{`(function(){ var d = util.json('{"a":{"b":[1,2]},"c":"x"}'); return {get:d.get("a.b"), getArr:d.get("a"), missing:d.get("nope")===undefined, has:d.has("a.b"), hasNo:d.has("z")}; })()`,
			`{"get":"[1,2]","getArr":"{\"b\":[1,2]}","missing":true,"has":true,"hasNo":false}`},
		{`(function(){ var d = util.json('{"arr":[1]}'); var r = d.ops([{op:"set",path:"arr.-1",value:"2"},{op:"remove",path:"nope"}]); return {r:r, f:d.finalize()}; })()`,
			`{"r":{"applied":["set arr.-1"],"skipped":["remove nope"]},"f":{"applied":["set arr.-1"],"body":"{\"arr\":[1,2]}","skipped":["remove nope"]}}`},
		// 错误路径(文案冻结)
		{`(function(){ try { util.json('not-json'); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"util.json: invalid JSON"`},
		{`(function(){ var d = util.json('{}'); try { d.ops([{op:"set",path:"x",value:5}]); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"util.json set x: value requires literal string"`},
		{`(function(){ try { util.json('{}').ops([{op:"bogus",path:"x"}]); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"util.json ops: unknown op \"bogus\""`},
		// evict(无 transport 上下文)
		{`(function(){ try { util.evict("t","s","v"); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"evict \"t\": no transport context (package )"`},
		// 64 位整数(错误路径文案)
		{`(function(){ try { util.u64be(new Uint8Array(8), -1); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"64-bit integer offset must be non-negative"`},
		{`(function(){ try { util.u64be(new Uint8Array(8), 1); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"64-bit integer read out of bounds"`},
		{`(function(){ try { util.u64be("not bytes", 0); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"64-bit integer input must be an ArrayBuffer or TypedArray"`},
		{`(function(){ try { util.u64be(new Uint8Array(4), 0); return "no-throw"; } catch(e) { return String(e.message); } })()`, `"64-bit integer read out of bounds"`},
		// setting
		{`setting.string({default:"x",label:"L"})`, `{"type":"string","default":"x","label":"L"}`},
		{`setting.enum({values:["a","b"]})`, `{"type":"enum","values":["a","b"]}`},
		{`setting.int()`, `{"type":"int"}`},
	} {
		assertQJS(t, tt.expr, tt.want)
	}
}

// TestQJS_BigEndianValues 64 位整数值(BigInt 字符串)
func TestQJS_BigEndianValues(t *testing.T) {
	q := newUtilVM(t, HostDeps{})
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
	q := newUtilVM(t, HostDeps{Key: &KeyRef{Data: map[string]any{"token": "T"}}, PackageName: "p"})
	got := evalQJSON(t, q, `({key: util.key().token, noKey: util.key().missing === undefined})`)
	if got != `{"key":"T","noKey":true}` {
		t.Fatalf("key parity: %s", got)
	}
	// 无键实例:key() = undefined
	q2 := newUtilVM(t, HostDeps{})
	if err := q2.inst.EvalString(`globalThis.__r = (util.key() === undefined);`); err != nil {
		t.Fatal(err)
	}
	v, _ := q2.inst.GetGlobal("__r")
	defer v.Free()
	if v.String() != "true" {
		t.Fatalf("no-key must be undefined, got %s", v.String())
	}
}

// TestQJS_InspectMasking inspect 掩码与截断
func TestQJS_InspectMasking(t *testing.T) {
	deps := HostDeps{PackageKeyValues: func() []string { return []string{"SECRET"} }}
	q := newUtilVM(t, deps)
	qout := evalQJSON(t, q, `JSON.parse(util.inspect({t:"SECRET",ok:"plain"}))`)
	if strings.Contains(qout, "SECRET") || !strings.Contains(qout, "***") || !strings.Contains(qout, "plain") {
		t.Fatalf("mask broken: %s", qout)
	}
	// 长文本截断(上限 2048;__hostToGo 通道含引号包装)
	long := strings.Repeat("a", 3000)
	got := evalQJSON(t, q, `util.inspect({pad:"`+long+`"})`)
	if strings.Count(got, "a") >= 3000 {
		t.Fatalf("not truncated: %d", len(got))
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
	q := newUtilVM(t, deps)
	if err := q.inst.EvalString(`log.info("hello", {a:1}); log.warn("SECRET"); log.error(42);`); err != nil {
		t.Fatal(err)
	}
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
	got := evalQJSON(t, q, `({get: storage.get("k"), miss: storage.get("nope") === undefined})`)
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

// TestQJS_EvictReport 上报回调收原样参数;scope 非法抛错
func TestQJS_EvictReport(t *testing.T) {
	var gotName, gotScope, gotValue string
	deps := HostDeps{
		PackageName: "p",
		TransportEvict: func(name, scope, value string) error {
			gotName, gotScope, gotValue = name, scope, value
			return nil
		},
	}
	q := newUtilVM(t, deps)
	if got := evalQJSON(t, q, `util.evict("aap-out","egress","1.2.3.4")`); got != "true" {
		t.Fatalf("evict = %s", got)
	}
	if gotName != "aap-out" || gotScope != "egress" || gotValue != "1.2.3.4" {
		t.Fatalf("args = %s/%s/%s", gotName, gotScope, gotValue)
	}
	q2 := newUtilVM(t, HostDeps{PackageName: "p", TransportEvict: func(string, string, string) error {
		return errors.New("scope/value 非法")
	}})
	if err := q2.inst.EvalString(`util.evict("aap-out","session","x")`); err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("want scope error, got %v", err)
	}
}

// TestQJS_EventstreamProtected util.eventstream 只读且可用
func TestQJS_EventstreamProtected(t *testing.T) {
	q := newUtilVM(t, HostDeps{})
	got := evalQJSON(t, q, `(function(){
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
	q := newUtilVM(t, HostDeps{})
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

// TestQJS_NowIsoNow 时钟族(util.now 毫秒;isoNow RFC3339)
func TestQJS_NowIsoNow(t *testing.T) {
	q := newUtilVM(t, HostDeps{})
	before := time.Now().UnixMilli()
	got := evalQJSON(t, q, `util.now()`)
	after := time.Now().UnixMilli()
	var n int64
	if err := json.Unmarshal([]byte(got), &n); err != nil {
		t.Fatalf("now not number: %s", got)
	}
	if n < before-1 || n > after+1 {
		t.Fatalf("now out of range: %d", n)
	}
	iso := strings.Trim(evalQJSON(t, q, `util.isoNow()`), `"`)
	if _, err := time.Parse(time.RFC3339, iso); err != nil {
		t.Fatalf("isoNow: %v (%s)", err, iso)
	}
}

// TestQJS_Uuid 形态 + 唯一性
func TestQJS_Uuid(t *testing.T) {
	q := newUtilVM(t, HostDeps{})
	got := strings.Trim(evalQJSON(t, q, `util.uuid()`), `"`)
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !re.MatchString(got) {
		t.Fatalf("uuid: %s", got)
	}
	if got == strings.Trim(evalQJSON(t, q, `util.uuid()`), `"`) {
		t.Fatal("uuid not unique")
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

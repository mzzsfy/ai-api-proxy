package plugin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dop251/goja"
)

// runUtil 在一次性 runtime 中执行 util 表达式,返回 JSON 序列化结果
func runUtil(t *testing.T, expr string) string {
	t.Helper()
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	v, err := vm.RunString(expr)
	if err != nil {
		t.Fatalf("run %s: %v", expr, err)
	}
	b, _ := json.Marshal(v.Export())
	return string(b)
}

func TestUtil_DeepMerge(t *testing.T) {
	// b 覆盖;嵌套合并;数组整体替换;返回新对象
	got := runUtil(t, `util.deepMerge({a:{x:1,y:2},arr:[1,2]}, {a:{y:3},arr:[9]})`)
	want := `{"a":{"x":1,"y":3},"arr":[9]}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestUtil_DeepClone(t *testing.T) {
	got := runUtil(t, `(function(){var o={a:{b:[1,2]}};var c=util.deepClone(o);c.a.b[0]=99;return [o.a.b[0],c.a.b[0]];})()`)
	if got != `[1,99]` {
		t.Fatalf("clone not deep: %s", got)
	}
}

func TestUtil_GetSet(t *testing.T) {
	if got := runUtil(t, `util.get({a:{b:[1,{c:"v"}]}},"a.b[1].c")`); got != `"v"` {
		t.Fatalf("get: %s", got)
	}
	got := runUtil(t, `util.set({a:{b:[{}]}}, "a.b[0].k", 7)`)
	if got != `{"a":{"b":[{"k":7}]}}` {
		t.Fatalf("set: %s", got)
	}
}

func TestUtil_PickOmit(t *testing.T) {
	if got := runUtil(t, `util.pick({a:1,b:2,c:3},["a","c","z"])`); got != `{"a":1,"c":3}` {
		t.Fatalf("pick: %s", got)
	}
	if got := runUtil(t, `util.omit({a:1,b:2,c:3},["b"])`); got != `{"a":1,"c":3}` {
		t.Fatalf("omit: %s", got)
	}
}

func TestUtil_B64(t *testing.T) {
	// 往返 + UTF-8
	if got := runUtil(t, `util.b64decode(util.b64encode("héllo"))`); got != `"héllo"` {
		t.Fatalf("b64 roundtrip: %s", got)
	}
	// 标准基线
	if got := runUtil(t, `util.b64encode("hi")`); got != `"aGk="` {
		t.Fatalf("b64encode: %s", got)
	}
	// url-safe 与标准差异(: / +)
	got64 := runUtil(t, `util.b64urlEncode(" subjects?")`)
	if got64 != `"`+base64.URLEncoding.EncodeToString([]byte(" subjects?"))+`"` {
		t.Fatalf("b64urlEncode: %s", got64)
	}
	if got := runUtil(t, `util.b64urlDecode(util.b64urlEncode("?&="))`); got != `"?\u0026="` {
		t.Fatalf("b64url roundtrip: %s", got)
	}
}

func TestUtil_Sha256Hex(t *testing.T) {
	sum := sha256.Sum256([]byte("abc"))
	want := hex.EncodeToString(sum[:])
	if got := runUtil(t, `util.sha256hex("abc")`); got != `"`+want+`"` {
		t.Fatalf("sha256hex: %s want %s", got, want)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(strings.Trim(runUtil(t, `util.sha256hex("")`), `"`)) {
		t.Fatal("sha256hex not lowercase hex64")
	}
}

func TestUtil_HmacSha256Hex(t *testing.T) {
	mac := hmac.New(sha256.New, []byte("key"))
	mac.Write([]byte("msg"))
	want := hex.EncodeToString(mac.Sum(nil))
	if got := runUtil(t, `util.hmacSha256hex("key","msg")`); got != `"`+want+`"` {
		t.Fatalf("hmacSha256hex: %s want %s", got, want)
	}
}

func TestUtil_Uuid(t *testing.T) {
	got := strings.Trim(runUtil(t, `util.uuid()`), `"`)
	// RFC 4122 v4:版本位 4,变体位 8/9/a/b
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !re.MatchString(got) {
		t.Fatalf("uuid: %s", got)
	}
	if got == strings.Trim(runUtil(t, `util.uuid()`), `"`) {
		t.Fatal("uuid not unique")
	}
}

func TestUtil_NowIsoNow(t *testing.T) {
	before := time.Now().UnixMilli()
	ms := runUtil(t, `util.now()`)
	after := time.Now().UnixMilli()
	var n int64
	if err := json.Unmarshal([]byte(ms), &n); err != nil {
		t.Fatalf("now not number: %s", ms)
	}
	if n < before-1 || n > after+1 {
		t.Fatalf("now out of range: %d", n)
	}
	iso := strings.Trim(runUtil(t, `util.isoNow()`), `"`)
	if _, err := time.Parse(time.RFC3339, iso); err != nil {
		t.Fatalf("isoNow: %v (%s)", err, iso)
	}
}

func TestUtil_Template(t *testing.T) {
	// 未匹配键保留原文
	got := runUtil(t, `util.template("https://x/{model}?k={keep}", {model:"m1"})`)
	if got != `"https://x/m1?k={keep}"` {
		t.Fatalf("template: %s", got)
	}
}

func TestUtil_KeyMissingThrowsWithPart(t *testing.T) {
	// v2:util.secret 删除;key 无 keys 上下文时抛错,错误注明包名
	vm := goja.New()
	bindUtil(vm, HostDeps{PackageName: "pkg-a"})
	_, err := vm.RunString(`util.key("nope")`)
	if err == nil || !strings.Contains(err.Error(), "pkg-a") || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("key error: %v", err)
	}
}

func TestUtil_StorageNs(t *testing.T) {
	// storage 经 deps.Storage 注入(	ns=包名由 host 决定);值=字符串
	m := map[string]string{}
	st := &memKV{m: m}
	vm := goja.New()
	bindUtil(vm, HostDeps{PackageName: "p1", Storage: st})
	if _, err := vm.RunString(`storage.set("k","v1"); storage.set("big","` + strings.Repeat("x", MaxStorageValue+1) + `")`); err == nil {
		t.Fatal("oversize value accepted")
	}
	if _, err := vm.RunString(`storage.set("k","v1")`); err != nil {
		t.Fatal(err)
	}
	got, err := vm.RunString(`storage.get("k")`)
	if err != nil || got.String() != "v1" {
		t.Fatalf("storage get: %v %v", got, err)
	}
	if _, err := vm.RunString(`storage.delete("k")`); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["k"]; ok {
		t.Fatal("delete failed")
	}
}

// memKV 测试存储
type memKV struct{ m map[string]string }

func (s *memKV) Get(key string) (string, bool) { v, ok := s.m[key]; return v, ok }
func (s *memKV) Set(key, value string) error   { s.m[key] = value; return nil }
func (s *memKV) Delete(key string)             { delete(s.m, key) }

func TestUtil_InspectMasksAndTruncates(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{
		PackageName: "p",
		PackageKeyValues: func() map[string]string {
			return map[string]string{"api_key": "sk-xyz"}
		},
	})
	got, _ := vm.RunString(`util.inspect({k:"sk-xyz", pad:"` + strings.Repeat("a", 3000) + `"})`)
	s := got.String()
	if strings.Contains(s, "sk-xyz") {
		t.Fatal("secret leaked")
	}
	if len(s) > 2048 {
		t.Fatalf("not truncated: %d", len(s))
	}
}

// Given host 注入 evict 回调 When util.evict(name,scope,value) Then 回调收到原样参数且返回 true(不触发重试,纯失效命令)
func TestUtil_Evict上报(t *testing.T) {
	var gotName, gotScope, gotValue string
	called := false
	vm := goja.New()
	bindUtil(vm, HostDeps{
		PackageName: "p",
		TransportEvict: func(name, scope, value string) error {
			called = true
			gotName, gotScope, gotValue = name, scope, value
			return nil
		},
	})
	ok, err := vm.RunString(`util.evict("aap-out","egress","1.2.3.4")`)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("evict 回调未被调用")
	}
	if gotName != "aap-out" || gotScope != "egress" || gotValue != "1.2.3.4" {
		t.Fatalf("args = %s/%s/%s", gotName, gotScope, gotValue)
	}
	if ok.String() != "true" {
		t.Fatalf("evict = %s", ok.String())
	}
}

// Given host 未注入 evict 回调 When util.evict Then 抛错且错误含 no transport context
func TestUtil_Evict未装配抛错(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{PackageName: "p"})
	_, err := vm.RunString(`util.evict("aap-out","egress","1.2.3.4")`)
	if err == nil || !strings.Contains(err.Error(), "no transport context") {
		t.Fatalf("want no transport context, got %v", err)
	}
}

// Given evict 回调返回 ErrBadScope When util.evict Then 抛错且错误含 scope
func TestUtil_EvictScope非法抛错(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{
		PackageName: "p",
		TransportEvict: func(string, string, string) error {
			return fmt.Errorf("scope/value 非法")
		},
	})
	_, err := vm.RunString(`util.evict("aap-out","session","x")`)
	if err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("want scope error, got %v", err)
	}
}

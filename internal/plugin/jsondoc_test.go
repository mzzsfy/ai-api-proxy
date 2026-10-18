package plugin

import (
	"encoding/json"
	"testing"
)

// jsStr Go 字符串 → JS 字符串字面量
func jsStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// runUtil 在一次性 quickjs runtime 执行 util 脚本,返回最后表达式完成值的 JSON 序列化
func runUtil(t *testing.T, expr string) string {
	t.Helper()
	q := newUtilVM(t, HostDeps{})
	v, err := q.inst.EvalExpression(expr)
	if err != nil {
		t.Fatalf("run %s: %v", expr, err)
	}
	defer v.Free()
	b, err := v.JSON()
	if err != nil {
		t.Fatalf("export %s: %v", expr, err)
	}
	return string(b)
}

// runUtilErr 执行 util 脚本,捕获 JS 异常
func runUtilErr(t *testing.T, expr string) (string, error) {
	t.Helper()
	q := newUtilVM(t, HostDeps{})
	v, err := q.inst.EvalExpression(expr)
	if err != nil {
		return "", err
	}
	defer v.Free()
	b, jerr := v.JSON()
	if jerr != nil {
		return "", jerr
	}
	return string(b), nil
}

// 场景1:get 大整数字面量零失真
func TestJsonDoc_GetLiteralBigInt(t *testing.T) {
	raw := `{"id":12345678901234567890,"name":"z"}`
	got := runUtil(t, `util.json(`+jsStr(raw)+`).get("id")`)
	if got != `"12345678901234567890"` {
		t.Fatalf("get id: %s", got)
	}
}

// 场景2:set 顶层键替换,未触碰文本逐字节保留
func TestJsonDoc_SetPreservesUntouched(t *testing.T) {
	raw := `{ "id" : 12345678901234567890,  "max_tokens" : 1, "note":  "keep" }`
	got := runUtil(t, `var d=util.json(`+jsStr(raw)+`);d.ops([{op:"set",path:"max_tokens",value:"4096"}]);d.finalize().body`)
	want := `{ "id" : 12345678901234567890,  "max_tokens" : 4096, "note":  "keep" }`
	if got != jsStr(want) {
		t.Fatalf("set body: %s", got)
	}
}

// 场景3:remove 对象键与数组元素
func TestJsonDoc_Remove(t *testing.T) {
	raw := `{"a":1,"b":2,"arr":[10,20,30]}`
	got := runUtil(t, `var d=util.json(`+jsStr(raw)+`);d.ops([{op:"remove",path:"b"},{op:"remove",path:"arr.1"}]);d.finalize().body`)
	want := `{"a":1,"arr":[10,30]}`
	if got != jsStr(want) {
		t.Fatalf("remove body: %s", got)
	}
}

// 场景4:set 路径 .-1 数组尾插(嵌套块注入)
func TestJsonDoc_AppendArrayTail(t *testing.T) {
	raw := `{"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`
	block := `{"type":"text","text":"x","cache_control":{"type":"ephemeral"}}`
	got := runUtil(t, `var d=util.json(`+jsStr(raw)+`);d.ops([{op:"set",path:"messages.0.content.-1",value:`+jsStr(block)+`}]);d.finalize().body`)
	want := `{"messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"text","text":"x","cache_control":{"type":"ephemeral"}}]}]}`
	if got != jsStr(want) {
		t.Fatalf("append body: %s", got)
	}
}

// 场景5:setIfAbsent 已存在跳过;回执正确
func TestJsonDoc_SetIfAbsent(t *testing.T) {
	raw := `{"metadata":{"user_id":"u1"}}`
	got := runUtil(t, `var d=util.json(`+jsStr(raw)+`);d.ops([{op:"setIfAbsent",path:"metadata",value:"{\"x\":1}"},{op:"setIfAbsent",path:"trace",value:"\"t1\""}]);var f=d.finalize();JSON.stringify({body:f.body,applied:f.applied,skipped:f.skipped})`)
	want := `{"body":"{\"metadata\":{\"user_id\":\"u1\"},\"trace\":\"t1\"}","applied":["setIfAbsent trace"],"skipped":["setIfAbsent metadata"]}`
	if got != jsStr(want) {
		t.Fatalf("setIfAbsent: %s", got)
	}
}

// 场景6:错误路径——非法 JSON 输入、缺父路径、非法 value 字面量
func TestJsonDoc_Errors(t *testing.T) {
	if _, err := runUtilErr(t, `util.json('{bad')`); err == nil {
		t.Fatal("invalid JSON should throw")
	}
	if _, err := runUtilErr(t, `util.json('{}').ops([{op:"set",path:"a.b",value:"1"}])`); err == nil {
		t.Fatal("missing parent should throw")
	}
	if _, err := runUtilErr(t, `util.json('{}').ops([{op:"set",path:"a",value:"{bad"}])`); err == nil {
		t.Fatal("invalid literal should throw")
	}
	if _, err := runUtilErr(t, `util.json('{}').ops([{op:"set",path:"a",value:1}])`); err == nil {
		t.Fatal("non-string value should throw")
	}
	if _, err := runUtilErr(t, `util.json('{}').ops([{op:"zzz",path:"a",value:"1"}])`); err == nil {
		t.Fatal("unknown op should throw")
	}
}

// 场景7:负索引 -1 读最后元素
func TestJsonDoc_GetLastElement(t *testing.T) {
	raw := `{"arr":[5,6,7]}`
	got := runUtil(t, `util.json(`+jsStr(raw)+`).get("arr.-1")`)
	if got != `"7"` {
		t.Fatalf("get last: %s", got)
	}
}

// 场景8:无 ops finalize 原文逐字节返回
func TestJsonDoc_FinalizeIdentity(t *testing.T) {
	raw := "{\n  \"a\" : 1,\n\t\"b\":[1 , 2]\n}\n"
	got := runUtil(t, `util.json(`+jsStr(raw)+`).finalize().body`)
	if got != jsStr(raw) {
		t.Fatalf("identity body: %s", got)
	}
}

// 边界:根级数组尾追加;空数组尾追加;ops 空 list
func TestJsonDoc_Edges(t *testing.T) {
	if got := runUtil(t, `var d=util.json("[1,2]");d.ops([{op:"set",path:"-1",value:"3"}]);d.finalize().body`); got != jsStr("[1,2,3]") {
		t.Fatalf("root array append: %s", got)
	}
	if got := runUtil(t, `var d=util.json('{"a":[]}');d.ops([{op:"set",path:"a.-1",value:"1"}]);d.finalize().body`); got != jsStr(`{"a":[1]}`) {
		t.Fatalf("empty array append: %s", got)
	}
	if got := runUtil(t, `util.json('{}').ops([]).applied.length`); got != "0" {
		t.Fatalf("empty ops: %s", got)
	}
}

// 边界:ops 中途失败,已执行指令生效(半应用语义)
func TestJsonDoc_PartialApply(t *testing.T) {
	_, err := runUtilErr(t, `var d=util.json('{}');d.ops([{op:"set",path:"a",value:"1"},{op:"set",path:"x.y",value:"2"}])`)
	if err == nil {
		t.Fatal("second op should throw")
	}
	if got := runUtil(t, `var d=util.json('{}');try{d.ops([{op:"set",path:"a",value:"1"},{op:"set",path:"x.y",value:"2"}])}catch(e){}d.finalize().body`); got != jsStr(`{"a":1}`) {
		t.Fatalf("partial apply body: %s", got)
	}
}

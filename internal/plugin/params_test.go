package plugin

import (
	"strings"
	"testing"
)

func declFixture() map[string]any {
	return map[string]any{
		"apiVersion": map[string]any{"type": "string", "default": "2023-06-01"},
		"mode":       map[string]any{"type": "enum", "values": []any{"light", "full"}, "default": "light"},
		"retries":    map[string]any{"type": "int", "default": float64(3)},
		"endpoint":   map[string]any{"type": "string", "required": true},
	}
}

func TestResolveParams_Layering(t *testing.T) {
	// Given 三层都有值 When 解析 Then 模型 > 插件 > default
	got, err := ResolveParams(declFixture(),
		map[string]any{"apiVersion": "2024-01-01", "mode": "full", "endpoint": "https://p.example"},
		map[string]any{"mode": "light", "retries": float64(9)}, ParamSave)
	if err != nil {
		t.Fatal(err)
	}
	if got["apiVersion"] != "2024-01-01" {
		t.Fatalf("apiVersion: %v", got["apiVersion"])
	}
	if got["mode"] != "light" {
		t.Fatalf("model override wins: %v", got["mode"])
	}
	if got["retries"] != float64(9) {
		t.Fatalf("model override int: %v", got["retries"])
	}
	if got["endpoint"] != "https://p.example" {
		t.Fatalf("plugin value: %v", got["endpoint"])
	}
	// 无 default 且未在任何层赋值的槽(apiVersion 无插件值时走 default;此断言用无 default 槽)
	noDefault := map[string]any{"token": map[string]any{"type": "string"}}
	got2, err := ResolveParams(noDefault, nil, nil, ParamSave)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := got2["token"]; present {
		t.Fatalf("unset no-default slot stays absent: %v", got2)
	}
}

func TestResolveParams_UnknownKey(t *testing.T) {
	// Given 模型覆盖含未声明键 When strict(保存期) Then 报错;When 非 strict(请求期) Then 剥离
	ov := map[string]any{"ghost": 1, "endpoint": "https://x.example"}
	if _, err := ResolveParams(declFixture(), nil, ov, ParamSave); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("strict must reject unknown key: %v", err)
	}
	got, err := ResolveParams(declFixture(), nil, ov, ParamRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["ghost"]; ok {
		t.Fatalf("request path must strip unknown key: %v", got)
	}
}

func TestResolveParams_EnumAndTypeViolations(t *testing.T) {
	// Given 覆盖值违反枚举/类型 When 解析 Then 报错注明槽名
	if _, err := ResolveParams(declFixture(), map[string]any{"mode": "ultra", "endpoint": "e"}, nil, ParamRequest); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("enum violation: %v", err)
	}
	if _, err := ResolveParams(declFixture(), map[string]any{"retries": "many", "endpoint": "e"}, nil, ParamRequest); err == nil || !strings.Contains(err.Error(), "retries") {
		t.Fatalf("type violation: %v", err)
	}
}

func TestResolveParams_Required(t *testing.T) {
	// Given required 槽三层皆缺:保存期通过(分步配置),请求期报错;补齐后两态皆通过
	if _, err := ResolveParams(declFixture(), nil, nil, ParamSave); err != nil {
		t.Fatalf("save mode must allow step-by-step config: %v", err)
	}
	if _, err := ResolveParams(declFixture(), nil, nil, ParamRequest); err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("request mode must enforce required: %v", err)
	}
	got, err := ResolveParams(declFixture(), nil, map[string]any{"endpoint": "https://m.example"}, ParamRequest)
	if err != nil {
		t.Fatal(err)
	}
	if got["endpoint"] != "https://m.example" {
		t.Fatalf("endpoint via model override: %v", got["endpoint"])
	}
}

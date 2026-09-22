// Package plugin 参数解析:声明 ⊕ 插件参数 ⊕ 模型覆盖 的唯一解析入口(v2 参数模型)
package plugin

import (
	"fmt"
	"sort"
)

// ParamMode 解析场景(决定未知键与 required 的行为)
type ParamMode int

const (
	// ParamSave 模型行保存期:未知键报错;required 跳过(允许分步配置:先建行后配包参数)
	ParamSave ParamMode = iota
	// ParamRequest 请求期:未知键剥离+日志面;required 缺失报错(502 兜底)
	ParamRequest
)

// ResolveParams 参数解析与校验:
//   - 底座 = 声明 default;插件值覆盖;模型值再覆盖(层级:模型 > 插件 > default)
//   - 未知键:ParamSave 报错(保存期白名单);ParamRequest 剥离(包升级删槽后残留键不阻断流量)
//   - 类型/枚举违例一律报错(值坏了不能静默)
//   - required:仅 ParamRequest 检查(合并后仍缺 → 报错,调用方转 502)
//
// decl 槽位形态 = settings 片段声明:{type, description?, default?, values?(enum), required?}
func ResolveParams(decl map[string]any, pluginVals map[string]any, modelOverrides map[string]any, mode ParamMode) (map[string]any, error) {
	out := map[string]any{}
	// ① default 底座
	for name, raw := range decl {
		slot, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if dv, ok := slot["default"]; ok {
			out[name] = dv
		}
	}
	// ② 插件值覆盖(声明外键剥离;包参数表单由视图约束,这里兜底)
	for k, v := range pluginVals {
		if _, known := decl[k]; !known {
			continue
		}
		out[k] = v
	}
	// ③ 模型覆盖(层级最高;ParamSave 未知键报错,ParamRequest 剥离)
	for k, v := range modelOverrides {
		if _, known := decl[k]; !known {
			if mode == ParamSave {
				return nil, fmt.Errorf("unknown param %q (not declared by package)", k)
			}
			continue
		}
		out[k] = v
	}
	// ④ 校验:类型/枚举恒查;required 仅请求期(保存期允许分步配置)
	names := make([]string, 0, len(decl))
	for name := range decl {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		slot, _ := decl[name].(map[string]any)
		if slot == nil {
			continue
		}
		v, present := out[name]
		if !present || v == nil {
			if mode == ParamRequest && requiredBool(slot["required"]) {
				return nil, fmt.Errorf("param %q required but unset", name)
			}
			continue
		}
		if err := CheckSettingType(name, slot, v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, nil
}

// requiredBool 声明槽 required 键归一(缺省 false)
func requiredBool(v any) bool {
	b, _ := v.(bool)
	return b
}

// CheckSettingType 类型粗校验(string/int/number/bool/enum;admin 视图与参数解析共用)
func CheckSettingType(name string, schema map[string]any, v any) error {
	typ, _ := schema["type"].(string)
	switch typ {
	case "string", "":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("setting %s: want string", name)
		}
	case "int", "number":
		switch v.(type) {
		case float64, int64, int:
		default:
			return fmt.Errorf("setting %s: want %s", name, typ)
		}
	case "bool":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("setting %s: want bool", name)
		}
	case "enum":
		sv, ok := v.(string)
		if !ok {
			return fmt.Errorf("setting %s: want string enum", name)
		}
		for _, e := range toStringSlice(schema["values"]) {
			if e == sv {
				return nil
			}
		}
		return fmt.Errorf("setting %s: %q not in enum values", name, sv)
	}
	return nil
}

// toStringSlice any → []string(声明 values 键)
func toStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

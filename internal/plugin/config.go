package plugin

import (
	"encoding/json"
	"fmt"
)

// configSchema 子集规格(feat/plugin.md:type/required/properties/enum/default)

// configSchema 结构化形态(仅用到的字段)
type configSchema struct {
	Type       string                     `json:"type"`
	Required   []string                   `json:"required"`
	Properties map[string]json.RawMessage `json:"properties"`
}

// enforceConfig 漂移矩阵应用:required 缺 → 错误注明字段(待补配置);
// 声明了 properties 的 schema 外未知键剥离(未声明 properties = 不校验键域)
func enforceConfig(part, paramsName string, schemaRaw json.RawMessage, params map[string]any) (map[string]any, error) {
	if len(schemaRaw) == 0 {
		return params, nil // 无 schema:不裁剪
	}
	var schema configSchema
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		return params, nil // schema 非法不阻塞(与包安装校验解耦)
	}
	if len(schema.Properties) == 0 && len(schema.Required) == 0 {
		return params, nil // 宽松 schema:不裁剪
	}
	out := make(map[string]any, len(params))
	for k, v := range params {
		if _, known := schema.Properties[k]; known {
			out[k] = v
		}
		// 未知键:忽略(漂移矩阵③④)
	}
	var missing []string
	for _, req := range schema.Required {
		if _, ok := out[req]; !ok {
			missing = append(missing, req)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: missing required config fields %v (update upstream config)", paramsName, missing)
	}
	return out, nil
}

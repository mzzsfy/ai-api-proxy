package convert

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AnthropicCodec anthropic 入口编解码
type AnthropicCodec struct{}

// NewAnthropicCodec 构造
func NewAnthropicCodec() *AnthropicCodec { return &AnthropicCodec{} }

// Parse 解析 anthropic messages 请求 → pivot(阶段 03 功能面)
func (c *AnthropicCodec) Parse(body []byte) (*PivotRequest, []Feature, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, nil, fmt.Errorf("parse anthropic request: %w", err)
	}
	pivot := anthropicToPivot(m)
	feats := anthropicFeatures(m)
	return pivot, feats, nil
}

// anthropicFeatures 检出能力
func anthropicFeatures(m map[string]any) []Feature {
	var feats []Feature
	if b, ok := m["stream"].(bool); ok && b {
		feats = append(feats, FeatureStream)
	}
	if _, ok := m["tools"]; ok {
		feats = append(feats, FeatureTools)
	}
	if th, ok := m["thinking"].(map[string]any); ok {
		if t, _ := th["type"].(string); t == "enabled" {
			feats = append(feats, FeatureThinking)
		}
	}
	if hasAnthropicImage(m) {
		feats = append(feats, FeatureVision)
	}
	return feats
}

// hasAnthropicImage 检查 messages/content 是否含 image 块(顶层或 content parts)
func hasAnthropicImage(m map[string]any) bool {
	msgs, ok := m["messages"].([]any)
	if !ok {
		return false
	}
	for _, msg := range msgs {
		mm, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		switch parts := mm["content"].(type) {
		case []any:
			for _, p := range parts {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				if t, _ := pm["type"].(string); t == "image" {
					return true
				}
			}
		}
	}
	return false
}

// anthropicToPivot 入方向:messages 块归一 + system 合并 + x_ 扩展写入
func anthropicToPivot(m map[string]any) *PivotRequest {
	pivot := map[string]any{}
	// 直通字段(语义一致)
	for _, k := range []string{"model", "temperature", "top_p", "max_tokens", "stop_sequences", "stream", "metadata"} {
		if v, ok := m[k]; ok {
			pivot[k] = v
		}
	}
	if v, ok := m["stop_sequences"]; ok {
		pivot["stop"] = v
	}
	if v, ok := m["top_k"]; ok {
		pivot["x_top_k"] = v
	}
	if v, ok := m["metadata"]; ok {
		pivot["x_metadata"] = v
	}
	if v, ok := m["thinking"]; ok {
		pivot["x_thinking"] = v
	}
	pivot["messages"] = normalizeAnthropicMessages(m)
	if sys, ok := m["system"]; ok {
		pivot["system"] = anthropicSystemToString(sys)
	}
	return &PivotRequest{Raw: pivot}
}

// anthropicSystemToString system 字符串或块数组归一为 string
func anthropicSystemToString(sys any) string {
	switch s := sys.(type) {
	case string:
		return s
	case []any:
		var b strings.Builder
		for _, blk := range s {
			if bm, ok := blk.(map[string]any); ok {
				if t, ok := bm["text"].(string); ok {
					b.WriteString(t)
				}
			}
		}
		return b.String()
	}
	return ""
}

// normalizeAnthropicMessages content 块数组归一:text 块拼接为 string,image 归一 image_url part
func normalizeAnthropicMessages(m map[string]any) []any {
	rawMsgs, ok := m["messages"].([]any)
	if !ok {
		return []any{}
	}
	out := make([]any, 0, len(rawMsgs))
	for _, msg := range rawMsgs {
		mm, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		nm := map[string]any{"role": mm["role"]}
		switch content := mm["content"].(type) {
		case string:
			nm["content"] = content
		case []any:
			nm["content"] = normalizeAnthropicContent(content)
		default:
			nm["content"] = ""
		}
		out = append(out, nm)
	}
	return out
}

// normalizeAnthropicContent 块数组 → openai content parts 形态(text 字符串化,image 转 image_url)
func normalizeAnthropicContent(parts []any) []any {
	out := make([]any, 0, len(parts))
	for _, p := range parts {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		switch t, _ := pm["type"].(string); t {
		case "text":
			if s, ok := pm["text"].(string); ok {
				out = append(out, map[string]any{"type": "text", "text": s})
			}
		case "image":
			if src, ok := pm["source"].(map[string]any); ok {
				data, _ := src["data"].(string)
				media, _ := src["media_type"].(string)
				url := "data:" + media + ";base64," + data
				if st, _ := src["type"].(string); st == "url" {
					url, _ = src["url"].(string)
				}
				out = append(out, map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}})
			}
		}
	}
	return out
}

// FromPivotResponse pivot(归一 openai 形态)→ anthropic message JSON
func (c *AnthropicCodec) FromPivotResponse(p *PivotResponse) ([]byte, error) {
	if p.Err != nil {
		return json.Marshal(map[string]any{
			"error": map[string]any{"type": p.Err.Type, "message": p.Err.Message},
		})
	}
	return pivotToAnthropic(p.JSON)
}

// pivotToAnthropic openai 形态响应 → anthropic message 形态
func pivotToAnthropic(body []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("pivot parse: %w", err)
	}
	resp := map[string]any{"type": "message", "role": "assistant"}
	if id, ok := m["id"]; ok {
		resp["id"] = id
	}
	if model, ok := m["model"]; ok {
		resp["model"] = model
	}
	text, stopReason, toolCalls := extractAssistant(m)
	reasoning := extractReasoning(m)
	content := make([]any, 0, 3)
	if reasoning != "" {
		content = append(content, map[string]any{"type": "thinking", "thinking": reasoning})
	}
	if text != "" {
		content = append(content, map[string]any{"type": "text", "text": text})
	}
	for _, tc := range toolCalls {
		content = append(content, tc)
	}
	resp["content"] = content
	resp["stop_reason"] = stopReason
	if u, ok := m["usage"]; ok {
		resp["usage"] = toAnthropicUsage(u)
	}
	return json.Marshal(resp)
}

// extractAssistant 从 openai 形态抽取文本/停止原因/tool_calls
func extractAssistant(m map[string]any) (string, string, []any) {
	choices, ok := m["choices"].([]any)
	if !ok || len(choices) == 0 {
		return "", "end_turn", nil
	}
	cm, _ := choices[0].(map[string]any)
	msg, _ := cm["message"].(map[string]any)
	text := ""
	if msg != nil {
		text, _ = msg["content"].(string)
	}
	fr, _ := cm["finish_reason"].(string)
	stop := "end_turn"
	switch fr {
	case "length":
		stop = "max_tokens"
	case "tool_calls", "function_call":
		stop = "tool_use"
	}
	var tools []any
	if msg != nil {
		if tcs, ok := msg["tool_calls"].([]any); ok {
			tools = toAnthropicToolUse(tcs)
		}
	}
	return text, stop, tools
}

// extractReasoning 从 openai 形态抽取推理文本(deepseek 形态 reasoning_content)
func extractReasoning(m map[string]any) string {
	choices, ok := m["choices"].([]any)
	if !ok || len(choices) == 0 {
		return ""
	}
	cm, _ := choices[0].(map[string]any)
	msg, _ := cm["message"].(map[string]any)
	if msg == nil {
		return ""
	}
	s, _ := msg["reasoning_content"].(string)
	return s
}

// toAnthropicToolUse openai tool_calls → anthropic tool_use 块
func toAnthropicToolUse(tcs []any) []any {
	out := make([]any, 0, len(tcs))
	for _, tc := range tcs {
		cm, ok := tc.(map[string]any)
		if !ok {
			continue
		}
		id, _ := cm["id"].(string)
		fn, _ := cm["function"].(map[string]any)
		name, _ := fn["name"].(string)
		args := fn["arguments"]
		var input any
		if s, ok := args.(string); ok {
			_ = json.Unmarshal([]byte(s), &input)
		} else if args != nil {
			input = args
		}
		if input == nil {
			input = map[string]any{}
		}
		out = append(out, map[string]any{"type": "tool_use", "id": id, "name": name, "input": input})
	}
	return out
}

// toAnthropicUsage openai usage → anthropic usage 字段名
func toAnthropicUsage(u any) map[string]any {
	b, _ := json.Marshal(u)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return map[string]any{
		"input_tokens":  m["prompt_tokens"],
		"output_tokens": m["completion_tokens"],
	}
}

package convert

import (
	"encoding/json"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// AnthropicCodec anthropic-messages 入口只读面
type AnthropicCodec struct{}

// NewAnthropicCodec 构造
func NewAnthropicCodec() *AnthropicCodec { return &AnthropicCodec{} }

// Protocol 协议全名
func (c *AnthropicCodec) Protocol() string { return pipeline.ProtocolAnthropicMessages }

// Framer 逐请求帧格式化器
func (c *AnthropicCodec) Framer() Framer { return &anthropicFramer{} }

// Inspect 入口检出:model / stream / features(轻量:仅顶层类型校验 + 能力识别,不做归一)
func (c *AnthropicCodec) Inspect(m map[string]any) (string, bool, []Feature, error) {
	model, _ := m["model"].(string)
	if model == "" {
		return "", false, nil, errModelRequired
	}
	stream, _ := m["stream"].(bool)
	return model, stream, anthropicFeatures(m), nil
}

// anthropicFeatures 检出能力
func anthropicFeatures(m map[string]any) []Feature {
	var feats []Feature
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

// hasAnthropicImage 检查 messages/content 是否含 image 块
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
		parts, ok := mm["content"].([]any)
		if !ok {
			continue
		}
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
	return false
}

// ErrorBody anthropic 错误体
func (c *AnthropicCodec) ErrorBody(status int, msg string, models []string) []byte {
	if len(models) > 0 {
		msg = msg + ": " + joinModels(models)
	}
	b, _ := json.Marshal(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
	return b
}

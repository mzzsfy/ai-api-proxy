package convert

import (
	"encoding/json"
	"errors"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// OpenAICodec openai-completions 入口只读面
type OpenAICodec struct{}

// NewOpenAICodec 构造
func NewOpenAICodec() *OpenAICodec { return &OpenAICodec{} }

// Protocol 协议全名
func (c *OpenAICodec) Protocol() string { return pipeline.ProtocolOpenAICompletions }

// Framer 逐请求帧格式化器
func (c *OpenAICodec) Framer() Framer { return &openAIFramer{} }

// Inspect 入口检出:model / stream / features(只读)
func (c *OpenAICodec) Inspect(m map[string]any) (string, bool, []Feature, error) {
	model, _ := m["model"].(string)
	if model == "" {
		return "", false, nil, errors.New("model required")
	}
	stream, _ := m["stream"].(bool)
	return model, stream, openaiFeatures(m), nil
}

// openaiFeatures 从请求对象检出能力标记
func openaiFeatures(m map[string]any) []Feature {
	var feats []Feature
	if _, ok := m["tools"]; ok {
		feats = append(feats, FeatureTools)
	}
	if hasVisionContent(m) {
		feats = append(feats, FeatureVision)
	}
	if n, ok := m["n"].(float64); ok && n > 1 {
		feats = append(feats, FeatureN)
	}
	return feats
}

// hasVisionContent 检查 messages 是否含 image_url content part
func hasVisionContent(m map[string]any) bool {
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
			if t, _ := pm["type"].(string); t == "image_url" {
				return true
			}
		}
	}
	return false
}

// ErrorBody openai 错误体
func (c *OpenAICodec) ErrorBody(status int, msg string, models []string) []byte {
	if len(models) > 0 {
		msg = msg + ": " + joinModels(models)
	}
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{"type": "api_error", "message": msg},
	})
	return b
}

// openAIFramer openai-completions 流式帧格式化器(mapEvent 产物为事件对象数组,逐元素一 data 行;收尾 [DONE])
type openAIFramer struct{}

// Frame 事件对象数组 → SSE 事件序列;元素字节原样直出(零重序列化);非数组载荷降级原样单事件
func (f *openAIFramer) Frame(payload []byte) []Event {
	var items []json.RawMessage
	if err := json.Unmarshal(payload, &items); err != nil {
		return []Event{{Data: string(payload)}}
	}
	out := make([]Event, 0, len(items))
	for _, it := range items {
		out = append(out, Event{Data: string(it)})
	}
	return out
}

// Flush [DONE] 收尾
func (f *openAIFramer) Flush() []Event {
	return []Event{{Data: "[DONE]"}}
}

// joinModels 逗号连接模型名
func joinModels(models []string) string {
	out := ""
	for i, m := range models {
		if i > 0 {
			out += ","
		}
		out += m
	}
	return out
}

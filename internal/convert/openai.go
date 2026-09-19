package convert

import (
	"encoding/json"
	"fmt"
	"strings"
)

// OpenAICodec openai 入口编解码
type OpenAICodec struct{}

// NewOpenAICodec 构造
func NewOpenAICodec() *OpenAICodec { return &OpenAICodec{} }

// Parse 解析 openai chat 请求:pivot 保留全字段,检出 features
func (c *OpenAICodec) Parse(body []byte) (*PivotRequest, []Feature, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, nil, fmt.Errorf("parse openai request: %w", err)
	}
	feats := openaiFeatures(m)
	return &PivotRequest{Raw: m}, feats, nil
}

// openaiFeatures 从请求对象检出能力标记
func openaiFeatures(m map[string]any) []Feature {
	var feats []Feature
	if b, ok := m["stream"].(bool); ok && b {
		feats = append(feats, FeatureStream)
	}
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

// FromPivotResponse pivot 响应 → openai JSON(未知字段天然保留;错误形态转换)
func (c *OpenAICodec) FromPivotResponse(p *PivotResponse) ([]byte, error) {
	if p.Err != nil {
		return json.Marshal(map[string]any{
			"error": map[string]any{"type": p.Err.Type, "message": p.Err.Message},
		})
	}
	return p.JSON, nil
}

// openAIChunker openai 流式输出:每 pivot chunk 恰 1 事件 + [DONE] 收尾
type openAIChunker struct{}

// NewFromPivotChunker 构造 per-request chunker
func (c *OpenAICodec) NewFromPivotChunker() FromPivotChunker { return &openAIChunker{} }

// sseEvent 单个 SSE data 载荷(不含 data: 前缀)
type sseEvent struct {
	Data    string
	Done    bool // [DONE] 终止标记
	RawPass bool // raw 降级帧:原样透传
}

// FromPivotChunker 流式输出契约
type FromPivotChunker interface {
	Next(chunkJSON []byte) ([]sseEvent, error)
	Flush() ([]sseEvent, error)
}

// Next 每 chunk 恰 1 事件;raw 帧 / x_error / 常规三分支
func (o *openAIChunker) Next(chunkJSON []byte) ([]sseEvent, error) {
	ch, err := ParseChunk(chunkJSON)
	if err != nil {
		return nil, err
	}
	if ch.HasRaw {
		return []sseEvent{{Data: ch.RawFrame, RawPass: true}}, nil
	}
	if ch.XError != nil {
		body, _ := json.Marshal(map[string]any{
			"error": map[string]any{"type": ch.XError.Type, "message": ch.XError.Message},
		})
		return []sseEvent{{Data: string(body)}}, nil
	}
	// pivot chunk 即 openai chunk 形态,原样输出
	return []sseEvent{{Data: string(chunkJSON)}}, nil
}

// Flush 产出 [DONE]
func (o *openAIChunker) Flush() ([]sseEvent, error) {
	return []sseEvent{{Done: true}}, nil
}

// StreamAdapter 态适配(协议无关)
type StreamAdapter struct{}

// Chunkify 整体 → 单 chunk 序列(文本全量单 chunk)
func (StreamAdapter) Chunkify(resp *PivotResponse) ([]*Chunk, error) {
	if resp.Err != nil {
		b, _ := json.Marshal(resp.Err)
		ch, err := ParseChunk(b)
		if err != nil {
			return nil, err
		}
		return []*Chunk{ch}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(resp.JSON, &m); err != nil {
		return nil, fmt.Errorf("chunkify parse: %w", err)
	}
	chunk := buildChunkifyChunk(m)
	b, err := json.Marshal(chunk)
	if err != nil {
		return nil, fmt.Errorf("chunkify marshal: %w", err)
	}
	ch, err := ParseChunk(b)
	if err != nil {
		return nil, err
	}
	return []*Chunk{ch}, nil
}

// buildChunkifyChunk 从完整响应抽取首 choice 文本与收尾字段
func buildChunkifyChunk(m map[string]any) map[string]any {
	chunk := map[string]any{}
	if id, ok := m["id"]; ok {
		chunk["id"] = id
	}
	if model, ok := m["model"]; ok {
		chunk["model"] = model
	}
	text := extractChoiceText(m)
	chunk["delta"] = map[string]any{"content": text}
	if usage, ok := m["usage"]; ok {
		chunk["usage"] = usage
	}
	if fr, ok := m["finish_reason"]; ok {
		chunk["finish_reason"] = fr
	} else if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
		if cm, ok := choices[0].(map[string]any); ok {
			if fr, ok := cm["finish_reason"]; ok {
				chunk["finish_reason"] = fr
			}
		}
	}
	return chunk
}

// extractChoiceText 取首 choice 文本(openai 响应形态)
func extractChoiceText(m map[string]any) string {
	choices, ok := m["choices"].([]any)
	if !ok || len(choices) == 0 {
		return ""
	}
	cm, ok := choices[0].(map[string]any)
	if !ok {
		return ""
	}
	if msg, ok := cm["message"].(map[string]any); ok {
		if s, ok := msg["content"].(string); ok {
			return s
		}
	}
	return ""
}

// Aggregate 流 → 整体:delta 拼接,usage/finish_reason 取末 chunk;x_error 中止
func (StreamAdapter) Aggregate(chunks <-chan *Chunk) (*PivotResponse, error) {
	var text strings.Builder
	var model, id string
	var usage json.RawMessage
	var finishReason string
	hasFinish := false
	for ch := range chunks {
		if ch.HasRaw {
			continue
		}
		if ch.XError != nil {
			return &PivotResponse{Err: ch.XError}, nil
		}
		if ch.ID != "" {
			id = ch.ID
		}
		if ch.Model != "" {
			model = ch.Model
		}
		if len(ch.Delta) > 0 {
			var d map[string]any
			if json.Unmarshal(ch.Delta, &d) == nil {
				if s, ok := d["content"].(string); ok {
					text.WriteString(s)
				}
			}
		}
		if len(ch.Usage) > 0 {
			usage = ch.Usage
		}
		if ch.FinishReason != "" {
			finishReason = ch.FinishReason
			hasFinish = true
		}
	}
	resp := map[string]any{
		"object":  "chat.completion",
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text.String()}, "finish_reason": finishReason}},
	}
	if id != "" {
		resp["id"] = id
	}
	if model != "" {
		resp["model"] = model
	}
	if usage != nil {
		var u any
		if json.Unmarshal(usage, &u) == nil {
			resp["usage"] = u
		}
	}
	_ = hasFinish
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("aggregate marshal: %w", err)
	}
	return &PivotResponse{JSON: b}, nil
}

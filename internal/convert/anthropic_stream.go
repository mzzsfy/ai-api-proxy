package convert

import (
	"encoding/json"
	"fmt"
)

// anthropicChunker anthropic 流式输出状态机(per-request)
// 事件序列:message_start → content_block_start(x_block 建块或首个文本块)
//
//	→ content_block_delta(增量)→ content_block_stop(x_block.stop)
//	→ ... → message_delta(usage/finish)→ Flush 补未闭合块 stop + message_stop
type anthropicChunker struct {
	started      bool // message_start 已发
	blockOpen    bool // 当前有未闭合 content block
	blockIndex   int  // 当前块索引
	blockType    string
	textStarted  bool // 已开启过文本块
	messageDelta bool // message_delta 已发
}

// NewFromPivotChunker 构造 per-request 状态机
func (c *AnthropicCodec) NewFromPivotChunker() FromPivotChunker {
	return &anthropicChunker{}
}

// ev 构造 SSE 事件
func ev(v any) sseEvent {
	b, _ := json.Marshal(v)
	return sseEvent{Data: string(b)}
}

// Next 输入 pivot chunk,输出 0..n 事件;raw 帧原样透传
func (a *anthropicChunker) Next(chunkJSON []byte) ([]sseEvent, error) {
	ch, err := ParseChunk(chunkJSON)
	if err != nil {
		return nil, err
	}
	if ch.HasRaw {
		return []sseEvent{{Data: ch.RawFrame, RawPass: true}}, nil
	}
	var out []sseEvent
	// x_error:按 anthropic 流内 error 事件终止
	if ch.XError != nil {
		out = append(out, ev(map[string]any{
			"type":  "error",
			"error": map[string]any{"type": ch.XError.Type, "message": ch.XError.Message},
		}))
		return out, nil
	}
	// 首 chunk:message_start(+ 可能联合 content_block_start/delta)
	if !a.started {
		a.started = true
		msgStart := map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"type": "message", "role": "assistant",
				"content": []any{}, "stop_reason": nil,
			},
		}
		if ch.ID != "" {
			msgStart["message"].(map[string]any)["id"] = ch.ID
		}
		if ch.Model != "" {
			msgStart["message"].(map[string]any)["model"] = ch.Model
		}
		out = append(out, ev(msgStart))
	}
	// usage/finish chunk(message_delta;可能同时带最后增量)
	if len(ch.Usage) > 0 || ch.FinishReason != "" {
		if len(ch.Delta) > 0 {
			out = append(out, a.deltaEvents(ch)...)
		}
		stop := anthropicStopReason(ch.FinishReason)
		md := map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": stop},
		}
		if len(ch.Usage) > 0 {
			var u map[string]any
			_ = json.Unmarshal(ch.Usage, &u)
			md["usage"] = map[string]any{"output_tokens": u["completion_tokens"]}
		}
		out = append(out, ev(md))
		a.messageDelta = true
		return out, nil
	}
	if ch.XBlock != nil {
		out = append(out, a.blockEvents(ch)...)
		return out, nil
	}
	out = append(out, a.deltaEvents(ch)...)
	return out, nil
}

// blockEvents x_block 建块/闭块事件
func (a *anthropicChunker) blockEvents(ch *Chunk) []sseEvent {
	var out []sseEvent
	xb := ch.XBlock
	if xb.Stop {
		if a.blockOpen {
			out = append(out, ev(map[string]any{
				"type": "content_block_stop", "index": a.blockIndex,
			}))
			a.blockOpen = false
			a.blockIndex++
		}
		return out
	}
	// 建块:纯文本块已隐式开启时,先闭旧文本块再开新块
	if a.blockOpen {
		out = append(out, ev(map[string]any{"type": "content_block_stop", "index": a.blockIndex}))
		a.blockIndex++
		a.blockOpen = false
	}
	a.blockType = xb.Type
	if a.blockType == "" {
		a.blockType = "text"
	}
	cbs := map[string]any{
		"type":  "content_block_start",
		"index": a.blockIndex,
		"content_block": map[string]any{
			"type": a.blockType, "text": "",
		},
	}
	out = append(out, ev(cbs))
	a.blockOpen = true
	a.textStarted = true
	// 建块可能同时带首增量
	if len(ch.Delta) > 0 {
		out = append(out, a.deltaEvents(ch)...)
	}
	return out
}

// deltaEvents 文本增量事件;首个非空增量隐式开文本块(纯 openai 语义上游无 x_block);
// 无文本增量帧(role/reasoning)不发事件不开空块
func (a *anthropicChunker) deltaEvents(ch *Chunk) []sseEvent {
	if ch.Delta == nil {
		return nil
	}
	var d map[string]any
	if json.Unmarshal(ch.Delta, &d) != nil {
		return nil
	}
	s, _ := d["content"].(string)
	if s == "" {
		return nil
	}
	var out []sseEvent
	if !a.textStarted && !a.blockOpen {
		out = append(out, ev(map[string]any{
			"type":          "content_block_start",
			"index":         a.blockIndex,
			"content_block": map[string]any{"type": "text", "text": ""},
		}))
		a.blockOpen = true
		a.textStarted = true
		a.blockType = "text"
	}
	out = append(out, ev(map[string]any{
		"type":  "content_block_delta",
		"index": a.blockIndex,
		"delta": map[string]any{"type": "text_delta", "text": s},
	}))
	return out
}

// Flush 收尾:未闭合块补发 content_block_stop(未闭合块规则)+ message_stop
func (a *anthropicChunker) Flush() ([]sseEvent, error) {
	var out []sseEvent
	if !a.started {
		// 零 chunk 流:最小合法序列
		out = append(out, ev(map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"type": "message", "role": "assistant",
				"content": []any{}, "stop_reason": nil,
			},
		}))
		a.started = true
	}
	if a.blockOpen {
		out = append(out, ev(map[string]any{"type": "content_block_stop", "index": a.blockIndex}))
		a.blockOpen = false
	}
	if !a.messageDelta {
		out = append(out, ev(map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": "end_turn"},
			"usage": map[string]any{"output_tokens": 0},
		}))
	}
	out = append(out, ev(map[string]any{"type": "message_stop"}))
	return out, nil
}

// anthropicStopReason openai finish_reason → anthropic stop_reason
func anthropicStopReason(fr string) string {
	switch fr {
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "tool_calls":
		return "tool_use"
	case "":
		return "end_turn"
	default:
		return fr
	}
}

// ParseChunkError 包装解析失败(供测试断言)
func ParseChunkError(b []byte) error {
	if _, err := ParseChunk(b); err != nil {
		return fmt.Errorf("chunk: %w", err)
	}
	return nil
}

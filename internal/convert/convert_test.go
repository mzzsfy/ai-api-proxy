package convert

import (
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// TestOpenAICodec_Protocol 协议全名与入口枚举一致
func TestOpenAICodec_Protocol(t *testing.T) {
	if got := NewOpenAICodec().Protocol(); got != pipeline.ProtocolOpenAICompletions {
		t.Fatalf("protocol: %s", got)
	}
}

// TestOpenAICodec_Inspect 入口检出:model/stream/features 只读
func TestOpenAICodec_Inspect(t *testing.T) {
	m := map[string]any{
		"model":  "m1",
		"stream": true,
		"tools":  []any{},
		"n":      float64(2),
		"messages": []any{
			map[string]any{"role": "user", "content": []any{map[string]any{"type": "image_url"}}},
		},
	}
	model, stream, feats, err := NewOpenAICodec().Inspect(m)
	if err != nil {
		t.Fatal(err)
	}
	if model != "m1" || !stream {
		t.Fatalf("model/stream: %s %v", model, stream)
	}
	if len(feats) != 3 {
		t.Fatalf("features: %v", feats)
	}
}

// TestOpenAICodec_Inspect_MissingModel 缺 model → 错误
func TestOpenAICodec_Inspect_MissingModel(t *testing.T) {
	if _, _, _, err := NewOpenAICodec().Inspect(map[string]any{}); err == nil {
		t.Fatal("want error")
	}
}

// TestOpenAICodec_Inspect_NoMutation 只读:入参 map 不被改写
func TestOpenAICodec_Inspect_NoMutation(t *testing.T) {
	m := map[string]any{"model": "m", "messages": []any{}}
	before := len(m)
	if _, _, _, err := NewOpenAICodec().Inspect(m); err != nil {
		t.Fatal(err)
	}
	if len(m) != before {
		t.Fatalf("mutated: %v", m)
	}
}

// TestOpenAICodec_Framer 逐 chunk 一事件 + [DONE] 收尾
func TestOpenAICodec_Framer(t *testing.T) {
	f := NewOpenAICodec().Framer()
	evs := f.Frame([]byte(`{"id":"c1"}`))
	if len(evs) != 1 || evs[0].Event != "" || evs[0].Data != `{"id":"c1"}` {
		t.Fatalf("frame: %+v", evs)
	}
	fin := f.Flush()
	if len(fin) != 1 || fin[0].Data != "[DONE]" {
		t.Fatalf("flush: %+v", fin)
	}
}

// TestOpenAICodec_ErrorBody openai 错误体形态
func TestOpenAICodec_ErrorBody(t *testing.T) {
	b := NewOpenAICodec().ErrorBody(404, "no upstream for model", []string{"a", "b"})
	want := `{"error":{"message":"no upstream for model: a,b","type":"api_error"}}`
	if string(b) != want {
		t.Fatalf("error body: %s", b)
	}
}

// TestAnthropicCodec_Protocol 协议全名
func TestAnthropicCodec_Protocol(t *testing.T) {
	if got := NewAnthropicCodec().Protocol(); got != pipeline.ProtocolAnthropicMessages {
		t.Fatalf("protocol: %s", got)
	}
}

// TestAnthropicCodec_Framer 事件对象数组 → SSE 事件(带 event 行)
func TestAnthropicCodec_Framer(t *testing.T) {
	f := NewAnthropicCodec().Framer()
	evs := f.Frame([]byte(`[{"type":"message_start"},{"type":"content_block_delta"}]`))
	if len(evs) != 2 {
		t.Fatalf("events: %+v", evs)
	}
	if evs[0].Event != "message_start" || evs[1].Event != "content_block_delta" {
		t.Fatalf("event names: %+v", evs)
	}
	if len(f.Flush()) != 0 {
		t.Fatal("anthropic flush must be empty")
	}
}

// TestAnthropicCodec_Framer_EmptyArray 空数组 → 零事件
func TestAnthropicCodec_Framer_EmptyArray(t *testing.T) {
	if evs := NewAnthropicCodec().Framer().Frame([]byte(`[]`)); len(evs) != 0 {
		t.Fatalf("events: %+v", evs)
	}
}

// TestAnthropicCodec_Inspect features:tools/vision/thinking
func TestAnthropicCodec_Inspect(t *testing.T) {
	m := map[string]any{
		"model":    "m",
		"tools":    []any{},
		"thinking": map[string]any{"type": "enabled"},
		"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "image"}}}},
	}
	_, stream, feats, err := NewAnthropicCodec().Inspect(m)
	if err != nil {
		t.Fatal(err)
	}
	if stream {
		t.Fatal("stream should be false")
	}
	if len(feats) != 3 {
		t.Fatalf("features: %v", feats)
	}
}

// TestAnthropicCodec_ErrorBody anthropic 错误体形态
func TestAnthropicCodec_ErrorBody(t *testing.T) {
	b := NewAnthropicCodec().ErrorBody(400, "capability missing", nil)
	want := `{"error":{"message":"capability missing","type":"api_error"},"type":"error"}`
	if string(b) != want {
		t.Fatalf("error body: %s", b)
	}
}

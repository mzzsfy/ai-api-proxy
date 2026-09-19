package convert

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// mustJSON 辅助
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOpenAIParse_Features(t *testing.T) {
	// Given 含 stream/tools/vision/n 的请求 When Parse Then features 全部检出
	body := `{"model":"gpt-x","stream":true,"n":2,"tools":[{"type":"function"}],
	  "messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"http://x"}}]}]}`
	p, feats, err := NewOpenAICodec().Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	got := map[Feature]bool{}
	for _, f := range feats {
		got[f] = true
	}
	for _, want := range []Feature{FeatureStream, FeatureTools, FeatureVision, FeatureN} {
		if !got[want] {
			t.Fatalf("missing feature %s, got %v", want, feats)
		}
	}
	if _, ok := p.GetScalar("model"); !ok {
		t.Fatal("model lost")
	}
}

func TestOpenAIParse_UnknownFieldsKept(t *testing.T) {
	// Given 请求含未知字段 user/logit_bias When Parse→Marshal Then 字段保留
	body := `{"model":"m","messages":[],"user":"u1","logit_bias":{"50256":-100}}`
	p, _, err := NewOpenAICodec().Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	_ = json.Unmarshal(b, &back)
	if back["user"] != "u1" {
		t.Fatalf("user field lost: %s", b)
	}
	if _, ok := back["logit_bias"]; !ok {
		t.Fatal("logit_bias lost")
	}
}

func TestOpenAIChunker_OneEventPerChunk(t *testing.T) {
	// Given 2 个 pivot chunk When Next Then 恰 2 事件且内容一致;Flush 产 [DONE]
	c := NewOpenAICodec().NewFromPivotChunker()
	ch1 := mustJSON(t, map[string]any{"id": "c1", "delta": map[string]any{"content": "he"}})
	ch2 := mustJSON(t, map[string]any{"delta": map[string]any{"content": "llo"}})
	ev1, err := c.Next(ch1)
	if err != nil || len(ev1) != 1 || ev1[0].Data != string(ch1) {
		t.Fatalf("ev1: %v %+v", err, ev1)
	}
	ev2, err := c.Next(ch2)
	if err != nil || len(ev2) != 1 {
		t.Fatalf("ev2: %v %+v", err, ev2)
	}
	fin, err := c.Flush()
	if err != nil || len(fin) != 1 || !fin[0].Done {
		t.Fatalf("flush: %v %+v", err, fin)
	}
}

func TestOpenAIChunker_RawPassthrough(t *testing.T) {
	// Given raw 帧 chunk When Next Then 原样透传不二次格式化
	rawFrame := `{"weird":"frame","x":1}`
	ch := mustJSON(t, map[string]any{"raw": rawFrame})
	evs, err := NewOpenAICodec().NewFromPivotChunker().Next(ch)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Data != rawFrame || !evs[0].RawPass {
		t.Fatalf("raw passthrough broken: %+v", evs)
	}
}

func TestAggregate_DeltaJoinUsageFromLast(t *testing.T) {
	// Given 3 chunk(含 usage 末 chunk)When Aggregate Then 文本拼接 usage 取末 chunk
	ch := make(chan *Chunk, 3)
	mk := func(s string, usage any) *Chunk {
		m := map[string]any{"delta": map[string]any{"content": s}}
		if usage != nil {
			m["usage"] = usage
		}
		b, _ := json.Marshal(m)
		c, _ := ParseChunk(b)
		return c
	}
	ch <- mk("he", nil)
	ch <- mk("llo", map[string]any{"prompt_tokens": 2})
	ch <- mk("", map[string]any{"completion_tokens": 5})
	close(ch)
	resp, err := StreamAdapter{}.Aggregate(ch)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(resp.JSON, &m)
	choices := m["choices"].([]any)
	c0 := choices[0].(map[string]any)
	msg := c0["message"].(map[string]any)
	if msg["content"] != "hello" {
		t.Fatalf("text: %v", msg["content"])
	}
	usage := m["usage"].(map[string]any)
	if usage["completion_tokens"] != float64(5) {
		t.Fatalf("usage not from last chunk: %v", usage)
	}
}

func TestAggregate_XErrorAborts(t *testing.T) {
	// Given 流中含 x_error chunk When Aggregate Then 产错误响应
	ch := make(chan *Chunk, 1)
	b, _ := json.Marshal(map[string]any{"x_error": map[string]any{"type": "overloaded", "message": "busy"}})
	c, _ := ParseChunk(b)
	ch <- c
	close(ch)
	resp, err := StreamAdapter{}.Aggregate(ch)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Err == nil || resp.Err.Type != "overloaded" {
		t.Fatalf("x_error not surfaced: %+v", resp)
	}
}

func TestChunkify_SingleChunkFullText(t *testing.T) {
	// Given 完整响应 When Chunkify Then 恰 1 chunk 且承载全量文本
	resp := &PivotResponse{JSON: mustJSON(t, map[string]any{
		"id": "c9", "model": "m",
		"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "full text"}, "finish_reason": "stop"}},
		"usage":   map[string]any{"completion_tokens": 7},
	})}
	chunks, err := StreamAdapter{}.Chunkify(resp)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(chunks))
	}
	var m map[string]any
	_ = json.Unmarshal(chunks[0].Bytes(), &m)
	delta := m["delta"].(map[string]any)
	if delta["content"] != "full text" {
		t.Fatalf("chunkify text: %v", delta)
	}
	if m["finish_reason"] != "stop" {
		t.Fatalf("chunkify finish_reason: %v", m["finish_reason"])
	}
}

func TestOpenAIParse_NGreaterThanOne(t *testing.T) {
	// Given n=2 When Parse Then 标记 FeatureN(gateway 层负责 400)
	_, feats, err := NewOpenAICodec().Parse([]byte(`{"model":"m","n":2,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range feats {
		if f == FeatureN {
			found = true
		}
	}
	if !found {
		t.Fatal("n>1 not flagged")
	}
}

func TestRoundTripSemanticEquivalence(t *testing.T) {
	// Given openai 请求样例 When Parse→FromPivotResponse(经 echo pivot) Then 未知字段不丢
	cases := []string{
		`{"model":"m","messages":[{"role":"system","content":"sys"},{"role":"user","content":"hi"}],"temperature":0.5,"top_p":0.9,"stop":["\n"],"seed":1,"presence_penalty":0.1,"frequency_penalty":0.2,"max_tokens":100,"user":"u"}`,
		`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"a"}]}],"stream":false}`,
	}
	for i, body := range cases {
		p, _, err := NewOpenAICodec().Parse([]byte(body))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		pb, _ := json.Marshal(p)
		resp := &PivotResponse{JSON: pb}
		out, err := NewOpenAICodec().FromPivotResponse(resp)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		var inMap, outMap map[string]any
		_ = json.Unmarshal([]byte(body), &inMap)
		_ = json.Unmarshal(out, &outMap)
		for k := range inMap {
			if _, ok := outMap[k]; !ok {
				t.Fatalf("case %d: field %s lost", i, k)
			}
		}
	}
}

func TestAnthropicStream_FullSequence(t *testing.T) {
	// Given 多 chunk 流(x_block 多块 + usage 末 chunk)When Next×n+Flush Then 官方事件序列
	c := NewAnthropicCodec().NewFromPivotChunker()
	var events []string
	appendEvents := func(evs []sseEvent, err error) {
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			var m map[string]any
			_ = json.Unmarshal([]byte(e.Data), &m)
			events = append(events, m["type"].(string))
		}
	}
	appendEvents(c.Next([]byte(`{"id":"msg_1","model":"claude-x"}`)))
	appendEvents(c.Next([]byte(`{"delta":{"content":"he"}}`)))
	appendEvents(c.Next([]byte(`{"x_block":{"index":1,"type":"text"},"delta":{"content":"block2"}}`)))
	appendEvents(c.Next([]byte(`{"x_block":{"index":1,"stop":true}}`)))
	appendEvents(c.Next([]byte(`{"usage":{"completion_tokens":9},"finish_reason":"stop"}`)))
	appendEvents(c.Flush())
	want := []string{
		"message_start",
		"content_block_start", "content_block_delta",
		"content_block_stop",
		"content_block_start", "content_block_delta",
		"content_block_stop",
		"message_delta",
		"message_stop",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("sequence:\n got %v\nwant %v", events, want)
	}
}

func TestAnthropicStream_UnclosedBlockFlush(t *testing.T) {
	// Given 流结束仍有未闭合块 When Flush Then 先补 content_block_stop 再 message_stop
	c := NewAnthropicCodec().NewFromPivotChunker()
	_, _ = c.Next([]byte(`{"id":"m","delta":{"content":"x"}}`))
	fin, err := c.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(fin) < 3 {
		t.Fatalf("flush too short: %d", len(fin))
	}
	types := make([]string, 0, len(fin))
	for _, e := range fin {
		var m map[string]any
		_ = json.Unmarshal([]byte(e.Data), &m)
		types = append(types, m["type"].(string))
	}
	if types[0] != "content_block_stop" {
		t.Fatalf("first flush event = %s, want content_block_stop", types[0])
	}
	if types[len(types)-1] != "message_stop" {
		t.Fatalf("last flush event = %s", types[len(types)-1])
	}
}

func TestAnthropicStream_XErrorTerminal(t *testing.T) {
	// Given x_error chunk When Next Then 产出 error 事件(流终止)
	c := NewAnthropicCodec().NewFromPivotChunker()
	evs, err := c.Next([]byte(`{"x_error":{"type":"overloaded_error","message":"busy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || !strings.Contains(evs[0].Data, "overloaded_error") {
		t.Fatalf("x_error event: %+v", evs)
	}
}

func TestAnthropicParse_SystemAndExtensions(t *testing.T) {
	// Given system 块数组 + top_k + metadata + stop_sequences When Parse Then pivot 归一
	body := `{"model":"claude-x","max_tokens":1024,"top_k":5,"stop_sequences":["END"],
	  "system":[{"type":"text","text":"s1"},{"type":"text","text":"s2"}],
	  "metadata":{"user_id":"u9"},
	  "messages":[{"role":"user","content":[{"type":"text","text":"q"}]}]}`
	p, _, err := NewAnthropicCodec().Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if sys, _ := p.GetScalar("system"); sys != "s1s2" {
		t.Fatalf("system: %q", sys)
	}
	if tk, ok := p.Raw["x_top_k"]; !ok || tk != float64(5) {
		t.Fatalf("x_top_k: %v", p.Raw["x_top_k"])
	}
	if _, ok := p.Raw["x_metadata"]; !ok {
		t.Fatal("x_metadata lost")
	}
	if _, ok := p.Raw["stop"]; !ok {
		t.Fatal("stop_sequences not normalized to stop")
	}
}

func TestAnthropicParse_ImageToImageURL(t *testing.T) {
	// Given image 块(base64 source)When Parse Then 归一为 image_url part
	body := `{"model":"m","max_tokens":10,
	  "messages":[{"role":"user","content":[
	    {"type":"text","text":"look"},
	    {"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}]}]}`
	p, _, err := NewAnthropicCodec().Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	msgs := p.Raw["messages"].([]any)
	m0 := msgs[0].(map[string]any)
	parts := m0["content"].([]any)
	img := parts[1].(map[string]any)
	if img["type"] != "image_url" {
		t.Fatalf("image part type: %v", img["type"])
	}
	iu := img["image_url"].(map[string]any)
	if !strings.HasPrefix(iu["url"].(string), "data:image/png;base64,") {
		t.Fatalf("data url: %v", iu["url"])
	}
}

func TestAnthropicResponse_Conversion(t *testing.T) {
	// Given openai 形态 pivot 响应 When FromPivotResponse Then anthropic message 形态
	pivot := mustJSON(t, map[string]any{
		"id": "c1", "model": "m",
		"choices": []any{map[string]any{
			"message":       map[string]any{"role": "assistant", "content": "答"},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": 3, "completion_tokens": 4},
	})
	out, err := NewAnthropicCodec().FromPivotResponse(&PivotResponse{JSON: pivot})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if m["type"] != "message" || m["stop_reason"] != "end_turn" {
		t.Fatalf("message shell: %v", m)
	}
	content := m["content"].([]any)
	if len(content) != 1 || content[0].(map[string]any)["text"] != "答" {
		t.Fatalf("content: %v", content)
	}
	usage := m["usage"].(map[string]any)
	if usage["input_tokens"] != float64(3) || usage["output_tokens"] != float64(4) {
		t.Fatalf("usage: %v", usage)
	}
}

func TestAnthropicChunker_OpenAIFormChunk(t *testing.T) {
	// Given 上游 chunk 为 openai chat 形态(choices 装载,协议 mapEvent 信封解包后即此形态)
	// When 走 anthropic chunker Then 完整事件序列且文本增量不丢;reasoning 帧不开空块
	a := NewAnthropicCodec().NewFromPivotChunker()

	// 首 chunk(role 帧):仅 message_start
	evs, err := a.Next([]byte(`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || !strings.Contains(evs[0].Data, "message_start") {
		t.Fatalf("first chunk: %v", evs)
	}

	// reasoning 帧(无 content):不发事件不开空块
	evs, err = a.Next([]byte(`{"id":"c1","choices":[{"index":0,"delta":{"reasoning":"think..."},"finish_reason":null}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("reasoning chunk must be silent: %v", evs)
	}

	// 文本帧:content_block_start + content_block_delta
	evs, err = a.Next([]byte(`{"id":"c1","choices":[{"index":0,"delta":{"content":" po"},"finish_reason":null}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 ||
		!strings.Contains(evs[0].Data, "content_block_start") ||
		!strings.Contains(evs[1].Data, `"text_delta"`) || !strings.Contains(evs[1].Data, " po") {
		t.Fatalf("text chunk: %v", evs)
	}

	// 收尾帧(choices 内 finish_reason 提升 + 顶层 usage):文本增量 + message_delta
	evs, err = a.Next([]byte(`{"id":"c1","choices":[{"index":0,"delta":{"content":"ng"},"finish_reason":"stop"}],"usage":{"completion_tokens":9}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 ||
		!strings.Contains(evs[0].Data, `"text_delta"`) || !strings.Contains(evs[0].Data, "ng") ||
		!strings.Contains(evs[1].Data, "message_delta") || !strings.Contains(evs[1].Data, "end_turn") {
		t.Fatalf("final chunk: %v", evs)
	}

	// Flush:未闭块补 stop + message_stop
	evs, err = a.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 ||
		!strings.Contains(evs[0].Data, "content_block_stop") ||
		!strings.Contains(evs[1].Data, "message_stop") {
		t.Fatalf("flush: %v", evs)
	}
}

func TestParseChunk_ChoicesFormLift(t *testing.T) {
	// Given openai chat chunk When ParseChunk Then choices[0].delta/finish_reason 提升为顶层
	ch, err := ParseChunk([]byte(`{"id":"c1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}],"usage":{"completion_tokens":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if ch.Delta == nil || !strings.Contains(string(ch.Delta), "hi") {
		t.Fatalf("delta lift: %s", ch.Delta)
	}
	if ch.FinishReason != "stop" {
		t.Fatalf("finish_reason lift: %q", ch.FinishReason)
	}
	// 顶层 delta 形态(chunkify 产物)仍直读
	ch, err = ParseChunk([]byte(`{"delta":{"content":"x"},"finish_reason":"length"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ch.FinishReason != "length" || ch.Delta == nil {
		t.Fatalf("top-level form: %q %s", ch.FinishReason, ch.Delta)
	}
}

package convert

import "encoding/json"

// anthropicFramer anthropic-messages 流式帧格式化器(直通:声明协议的事件对象原样成帧)
// 事件序列由上游部件产出([{type:...},...] 数组),本层只做 JSON → SSE 事件包装
type anthropicFramer struct{}

// Frame 事件对象数组 → SSE 事件序列;数组元素 type 非空即写 event 行
func (f *anthropicFramer) Frame(payload []byte) []Event {
	var items []map[string]any
	if err := json.Unmarshal(payload, &items); err != nil {
		return []Event{{Data: string(payload)}}
	}
	out := make([]Event, 0, len(items))
	for _, it := range items {
		b, err := json.Marshal(it)
		if err != nil {
			continue
		}
		t, _ := it["type"].(string)
		out = append(out, Event{Event: t, Data: string(b)})
	}
	return out
}

// Flush anthropic 无统一终止帧(部件以 message_stop 事件收尾)
func (f *anthropicFramer) Flush() []Event { return nil }

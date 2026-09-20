// Package convert 入口协议只读面:特征识别 / 响应帧格式化 / 本地错误体
// 声明式单协议:上游声明协议与入口协议恒等,本包不做任何格式转换
package convert

import "errors"

// Feature 能力协商标记(入口检出,路由协商用)
type Feature string

// Feature 常量(入口检出;能力/形态由声明矩阵决定,非入口字段决定)
const (
	FeatureStream    Feature = "stream"
	FeatureTools     Feature = "tools"
	FeatureVision    Feature = "vision"
	FeatureThinking  Feature = "thinking"
	FeatureN         Feature = "n>1"
	FeatureReasoning Feature = "reasoning"
)

// EntryInspector 入口请求只读解析:模型/流式意图/features;原文不改
type EntryInspector interface {
	Inspect(m map[string]any) (model string, stream bool, feats []Feature, err error)
}

// ErrorRenderer 本地错误体(协议格式;models 非空时附可用模型列表)
type ErrorRenderer interface {
	ErrorBody(status int, msg string, models []string) []byte
}

// Framer 流式帧格式化(per-request;event 非空即写 event 行)
type Framer interface {
	Frame(payload []byte) []Event
	Flush() []Event
}

// FramerFactory 逐请求构造帧格式化器(有状态:块开闭/收尾)
type FramerFactory interface {
	Framer() Framer
}

// Event 输出 SSE 事件(event 空 = 仅 data 行)
type Event struct {
	Event string
	Data  string
}

// errModelRequired 入口缺 model(400)
var errModelRequired = errors.New("model required")

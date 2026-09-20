// Package pipeline 管道契约与执行:Protocol/Filter/Target/Context + 单目标直发引擎
// 重试/熔断不归本层(下游自行处理);失败即终局,上游状态原样透传
package pipeline

import (
	"context"
	"errors"
)

// TransportRef 默认传输实例名
const TransportRef = "direct"

// ErrPoolBusy runtime 池排队超时/已关闭(本地资源问题;503)
var ErrPoolBusy = errors.New("runtime pool busy")

// ProtocolSlot 协议槽全名(协议部件声明与入口协议共用的唯一枚举源)
type ProtocolSlot = string

// 协议全名枚举(管道权威格式 = 插件声明协议格式 = 入口协议格式)
const (
	ProtocolOpenAICompletions ProtocolSlot = "openai-completions"
	ProtocolAnthropicMessages ProtocolSlot = "anthropic-messages"
)

// Form 请求形态(串行管道的形态口径)
type Form = string

// 形态枚举(与 manifest 协议槽/SDK Form 同一组字面量)
const (
	FormStreaming    Form = "streaming"
	FormNonStreaming Form = "non_streaming"
)

// Vars 入口形态路由元数据(优化参考,正确性不得依赖)
type Vars struct {
	Model       string
	EntryStream bool
}

// UpstreamInfo 上游轻量快照(hook 可读)
type UpstreamInfo struct {
	Name   string
	Models []string
}

// Target 上游目标(SecretsRef=凭据存储键)
type Target struct {
	ID         string
	Name       string
	BaseURL    string
	Transport  string
	SecretsRef string
}

// PipelineContext 管道请求上下文
type PipelineContext struct {
	RequestID string
	Upstream  UpstreamInfo
	Target    Target
	State     map[string]any
	Vars      Vars
}

// NewContext 构造(初始化 State)
func NewContext(requestID string, info UpstreamInfo, vars Vars) *PipelineContext {
	return &PipelineContext{
		RequestID: requestID,
		Upstream:  info,
		State:     make(map[string]any),
		Vars:      vars,
	}
}

// Supports 能力声明
type Supports struct {
	Forms    []string // streaming | non_streaming
	Features []string
}

// HasForm 声明中是否含指定形态
func (s Supports) HasForm(form string) bool {
	for _, f := range s.Forms {
		if f == form {
			return true
		}
	}
	return false
}

// HasFeature 声明中是否含指定能力
func (s Supports) HasFeature(feat string) bool {
	for _, f := range s.Features {
		if f == feat {
			return true
		}
	}
	return false
}

// Declarer 协议全名声明(路由把关入口协议与声明是否一致;不匹配即拒绝,无转换)
type Declarer interface {
	Declared() string
}

// Request 上游请求载体(传输由 host 执行)
type Request struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    []byte
	Stream  bool   // 双声明分发意图
	Model   string // 会话亲和素材(ipp 供给方传输消费;非 ipp 忽略)
}

// Protocol 协议适配层(恰一;声明式单协议;hook 全同步)
type Protocol interface {
	Declarer
	Name() string
	Supports() Supports
	BuildRequest(ctx *PipelineContext, entry []byte) (Request, error)
	MapEvent(ctx *PipelineContext, event []byte) ([]byte, error)
	MapResponse(ctx *PipelineContext, body []byte) ([]byte, error)
}

// ErrorMapper 可选 hook:错误映射(未实现走 host 透传兜底)
type ErrorMapper interface {
	MapError(ctx *PipelineContext, status int, body []byte) ([]byte, error)
}

// Filter 请求修改层(0..n 串行)
type Filter interface {
	Name() string
	MapRequest(ctx *PipelineContext, entry []byte) ([]byte, error)
	MapChunk(ctx *PipelineContext, chunkJSON []byte) ([]byte, error)
	MapResponse(ctx *PipelineContext, respJSON []byte) ([]byte, error)
}

// Response 管道产物(流式或非流式)
type Response struct {
	Stream      bool
	Chunks      <-chan ChunkItem // Stream=true 时有效
	Body        []byte           // Stream=false 时有效
	Status      int              // 末次上游状态(错误路径)
	ContentType string
}

// ChunkItem 流式单 chunk(携带降级标记)
type ChunkItem struct {
	JSON    []byte
	RawPass bool // raw 降级帧,convert 跳过格式化
}

// Transport 命名传输实例(host 面;实现见 transport 包)
type Transport interface {
	RoundTrip(ctx context.Context, req Request) (TransportResponse, error)
	Name() string
}

// TransportResponse 上游响应载体
type TransportResponse struct {
	Status  int
	Headers map[string]string
	Body    []byte          // 非流式:全量
	Events  <-chan SSEFrame // 流式:分帧后事件通道
}

// SSEFrame 上游 SSE 单帧(流式)
type SSEFrame struct {
	Event string
	Data  string
}

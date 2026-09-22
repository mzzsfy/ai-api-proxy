// Package builtin 内置 openai-compatible 协议(Go 实现 Protocol,随二进制预置)
package builtin

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// Name 协议名
const Name = "openai-compatible"

// DeclaredProtocol 声明的协议全名(入口路由据此把关;唯一源在 pipeline)
const DeclaredProtocol = pipeline.ProtocolOpenAICompletions

// Protocol 内置协议:入口原文 1:1 透传(声明 openai-completions),凭据注入授权头
// v2:连接信息 = 包参数 base_url/transport;密钥 = 包级 keys api_key(与 JS 包同语义)
type Protocol struct {
	// Config 解析后的包参数(base_url 必填;transport 可空 = direct)
	Config map[string]any
	// PackageKey 包级 key 只读(实时)
	PackageKey func(name string) (any, bool)
}

// New 构造
func New() *Protocol { return &Protocol{} }

// configString 参数读值(字符串形态)
func (p *Protocol) configString(name string) string {
	if p.Config == nil {
		return ""
	}
	s, _ := p.Config[name].(string)
	return s
}

// Name 实现 pipeline.Protocol
func (p *Protocol) Name() string { return Name }

// Declared 声明协议全名
func (p *Protocol) Declared() string { return DeclaredProtocol }

// Supports 阶段 01 全集;声明含 tools/vision(透传语义,真实上游兜底)
func (p *Protocol) Supports() pipeline.Supports {
	return pipeline.Supports{
		Forms:    []string{pipeline.FormStreaming, pipeline.FormNonStreaming},
		Features: []string{"tools", "vision"},
	}
}

// BearerPrefix 授权头 scheme
const BearerPrefix = "Bearer "

// BaseURLParam 连接地址参数槽名
const BaseURLParam = "base_url"

// TransportParam 传输实例参数槽名
const TransportParam = "transport"

// APIKeyParam 密钥键名
const APIKeyParam = "api_key"

// BuildRequest 组请求:URL=包参数 base_url + /v1/chat/completions;鉴权 = 包级 key api_key
// stream 标志透传入口意图;transport 槽随请求载体下传(空 = direct)
func (p *Protocol) BuildRequest(ctx *pipeline.PipelineContext, entry []byte) (pipeline.Request, error) {
	base := p.configString(BaseURLParam)
	if base == "" {
		return pipeline.Request{}, fmt.Errorf("builtin: package param %s missing", BaseURLParam)
	}
	if p.PackageKey == nil {
		return pipeline.Request{}, fmt.Errorf("builtin: keys reader not wired")
	}
	keyVal, ok := p.PackageKey(APIKeyParam)
	if !ok || keyVal == nil {
		return pipeline.Request{}, fmt.Errorf("builtin: package key %s missing", APIKeyParam)
	}
	key, _ := keyVal.(string)
	if key == "" {
		return pipeline.Request{}, fmt.Errorf("builtin: package key %s empty", APIKeyParam)
	}
	body := entry
	url := fmt.Sprintf("%s/v1/chat/completions", strings.TrimSuffix(base, "/"))
	return pipeline.Request{
		URL:    url,
		Method: "POST",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": BearerPrefix + key,
		},
		Body:      body,
		Stream:    ctx.Vars.EntryStream,
		Model:     ctx.Vars.Model,
		APIKey:    key, // 会话亲和素材
		Transport: p.configString(TransportParam),
	}, nil
}

// doneData SSE 结束帧 data 字面量
const doneData = "[DONE]"

// MapEvent 信封解包:入参 {"event","data"},返回声明协议事件对象数组;
// [DONE] 返回 nil(跳帧,收尾帧由入口 Framer 负责)
func (p *Protocol) MapEvent(ctx *pipeline.PipelineContext, event []byte) ([]byte, error) {
	var frame struct {
		Event string `json:"event"`
		Data  string `json:"data"`
	}
	if err := json.Unmarshal(event, &frame); err != nil {
		return nil, fmt.Errorf("builtin: bad sse frame: %w", err)
	}
	if frame.Data == doneData {
		return nil, nil
	}
	data := json.RawMessage(frame.Data)
	if !json.Valid(data) {
		data = mustQuote(frame.Data)
	}
	obj := map[string]any{}
	if !isJSONObject(data) {
		// 非对象载荷(如 keepalive 字面量)按帧原样装信封,由部件/上游语义决定
		out, err := json.Marshal([]any{map[string]any{"event": frame.Event, "data": frame.Data}})
		if err != nil {
			return nil, fmt.Errorf("builtin: marshal event: %w", err)
		}
		return out, nil
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("builtin: unmarshal event: %w", err)
	}
	out, err := json.Marshal([]any{obj})
	if err != nil {
		return nil, fmt.Errorf("builtin: marshal event: %w", err)
	}
	return out, nil
}

// isJSONObject 载荷是否为 JSON 对象
func isJSONObject(b []byte) bool {
	var m map[string]any
	return json.Unmarshal(b, &m) == nil
}

// mustQuote 字面量包装为 JSON 字符串
func mustQuote(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// MapResponse 透传
func (p *Protocol) MapResponse(ctx *pipeline.PipelineContext, body []byte) ([]byte, error) {
	return body, nil
}

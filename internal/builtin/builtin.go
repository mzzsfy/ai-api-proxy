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

// DeclaredProtocol 声明的协议全名(入口路由据此把关)
const DeclaredProtocol = "openai-completions"

// Protocol 内置协议:入口原文 1:1 透传(声明 openai-completions),凭据注入授权头
type Protocol struct {
	// TargetSecrets 按 (target 名, 键) 解析凭据(Resolve 时注入,与 JS 部件同机制)
	TargetSecrets func(target, key string) (string, bool)
}

// New 构造
func New() *Protocol { return &Protocol{} }

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

// BuildRequest 组请求:URL=目标 BaseURL + /v1/chat/completions;鉴权 = 目标 secrets 的 api_key
// secretsRef 读值即 key(无 scheme,前缀在此拼接);stream 标志透传入口意图
func (p *Protocol) BuildRequest(ctx *pipeline.PipelineContext, entry []byte) (pipeline.Request, error) {
	if ctx == nil || ctx.Target.BaseURL == "" {
		return pipeline.Request{}, fmt.Errorf("builtin: target base url empty")
	}
	if p.TargetSecrets == nil {
		return pipeline.Request{}, fmt.Errorf("builtin: secrets reader not wired")
	}
	key, ok := p.TargetSecrets(ctx.Target.Name, "api_key")
	if !ok || key == "" {
		return pipeline.Request{}, fmt.Errorf("builtin: api_key secret missing for target %s", ctx.Target.Name)
	}
	body := entry
	url := fmt.Sprintf("%s/v1/chat/completions", strings.TrimSuffix(ctx.Target.BaseURL, "/"))
	return pipeline.Request{
		URL:    url,
		Method: "POST",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": BearerPrefix + key,
		},
		Body:   body,
		Stream: ctx.Vars.EntryStream,
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

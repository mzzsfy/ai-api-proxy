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

// Protocol 内置协议:1:1 透传 pivot(openai 原生形态),凭据注入授权头
type Protocol struct {
	// TargetSecrets 按 (target 名, 键) 解析凭据(Resolve 时注入,与 JS 部件同机制)
	TargetSecrets func(target, key string) (string, bool)
}

// New 构造
func New() *Protocol { return &Protocol{} }

// Name 实现 pipeline.Protocol
func (p *Protocol) Name() string { return Name }

// Supports 阶段 01 全集;声明含 tools/vision(透传语义,真实上游兜底)
func (p *Protocol) Supports() pipeline.Supports {
	return pipeline.Supports{
		Forms:    []string{"streaming", "non_streaming"},
		Features: []string{"tools", "vision"},
	}
}

// BearerPrefix 授权头 scheme
const BearerPrefix = "Bearer "

// BuildRequest 组请求:URL=目标 BaseURL + /v1/chat/completions;鉴权 = 目标 secrets 的 api_key
// secretsRef 读值即 key(无 scheme,前缀在此拼接);stream 标志透传入口意图
func (p *Protocol) BuildRequest(ctx *pipeline.PipelineContext, pivot []byte) (pipeline.Request, error) {
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
	body, err := sanitizePivot(pivot)
	if err != nil {
		return pipeline.Request{}, fmt.Errorf("builtin: sanitize: %w", err)
	}
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

// sanitizePivot 清洗 anthropic 残留:system 顶层字段转 system 消息;stop_sequences→stop;x_* 剥离
func sanitizePivot(pivot []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(pivot, &m); err != nil {
		return pivot, nil // 非 JSON 不动(交给上游报错)
	}
	changed := false
	if sys, ok := m["system"].(string); ok && sys != "" {
		msgs, _ := m["messages"].([]any)
		sysMsg := map[string]any{"role": "system", "content": sys}
		m["messages"] = append([]any{sysMsg}, msgs...)
		delete(m, "system")
		changed = true
	}
	if ss, ok := m["stop_sequences"]; ok {
		if _, has := m["stop"]; !has {
			m["stop"] = ss
		}
		delete(m, "stop_sequences")
		changed = true
	}
	if tk, ok := m["x_top_k"]; ok {
		if _, has := m["top_k"]; !has {
			m["top_k"] = tk
		}
		delete(m, "x_top_k")
		changed = true
	}
	for k := range m {
		if strings.HasPrefix(k, "x_") {
			delete(m, k)
			changed = true
		}
	}
	if !changed {
		return pivot, nil
	}
	return json.Marshal(m)
}

// doneData SSE 结束帧 data 字面量
const doneData = "[DONE]"

// MapEvent 信封解包:入参 {"event","data"},返回 pivot chunk 字符串;[DONE] 返回 nil(交由 Flush 收尾)
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
	return []byte(frame.Data), nil
}

// MapResponse 透传
func (p *Protocol) MapResponse(ctx *pipeline.PipelineContext, body []byte) ([]byte, error) {
	return body, nil
}

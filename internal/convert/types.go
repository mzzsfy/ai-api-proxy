// Package convert 入口协议与 pivot 双向转换 + 形态态适配(协议无关)
package convert

import (
	"encoding/json"
	"fmt"
)

// Feature 能力协商标记(Parse 检出,路由用)
type Feature string

// Feature 常量(阶段 01/03 范围;tools/vision/thinking 阶段 07)
const (
	FeatureStream   Feature = "stream"
	FeatureTools    Feature = "tools"
	FeatureVision   Feature = "vision"
	FeatureThinking Feature = "thinking"
	FeatureN        Feature = "n>1"
)

// XError 上游错误统一结构(流内终止标记)
type XError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Status  int    `json:"status,omitempty"`
}

// XBlock anthropic 多 content block 边界
type XBlock struct {
	Index int    `json:"index"`
	Type  string `json:"type,omitempty"`
	Stop  bool   `json:"stop,omitempty"`
}

// Chunk 管道流式权威格式:openai chat chunk 语义 + x_ 扩展 + raw 逃生口
// 解析到 map 保留未知字段;结构化字段按需读取
type Chunk struct {
	Raw          json.RawMessage // 原始 JSON 字节(权威,未知字段天然保留)
	ID           string
	Model        string
	Delta        json.RawMessage // {"content": "..."} 或 {"role":"assistant"}
	Usage        json.RawMessage
	FinishReason string
	XBlock       *XBlock
	XError       *XError
	HasRaw       bool // raw 降级透传:原始帧 JSON 文本
	RawFrame     string
}

// ParseChunk 解析管道 chunk;raw 字段存在即为降级帧
func ParseChunk(b []byte) (*Chunk, error) {
	c := &Chunk{Raw: json.RawMessage(b)}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse chunk: %w", err)
	}
	if v, ok := m["raw"]; ok {
		c.HasRaw = true
		c.RawFrame = string(v)
		// raw 为字符串形式承载原始帧 JSON 文本;去掉外层引号转义由调用方按需解析
		var s string
		if json.Unmarshal(v, &s) == nil {
			c.RawFrame = s
		}
		return c, nil
	}
	if v, ok := m["id"]; ok {
		_ = json.Unmarshal(v, &c.ID)
	}
	if v, ok := m["model"]; ok {
		_ = json.Unmarshal(v, &c.Model)
	}
	if v, ok := m["delta"]; ok {
		c.Delta = v
	}
	if v, ok := m["usage"]; ok && string(v) != "null" {
		c.Usage = v
	}
	if v, ok := m["finish_reason"]; ok && string(v) != "null" {
		_ = json.Unmarshal(v, &c.FinishReason)
	}
	// openai chat chunk 形态(choices 装载):提升首 choice 的 delta/finish_reason 为顶层
	var choices []json.RawMessage
	if json.Unmarshal(m["choices"], &choices) == nil && len(choices) > 0 {
		var c0 map[string]json.RawMessage
		if json.Unmarshal(choices[0], &c0) == nil {
			if c.Delta == nil {
				if v, ok := c0["delta"]; ok {
					c.Delta = v
				}
			}
			if c.FinishReason == "" {
				if v, ok := c0["finish_reason"]; ok && string(v) != "null" {
					_ = json.Unmarshal(v, &c.FinishReason)
				}
			}
		}
	}
	if v, ok := m["x_block"]; ok && string(v) != "null" {
		xb := &XBlock{}
		if err := json.Unmarshal(v, xb); err != nil {
			return nil, fmt.Errorf("parse x_block: %w", err)
		}
		c.XBlock = xb
	}
	if v, ok := m["x_error"]; ok && string(v) != "null" {
		xe := &XError{}
		if err := json.Unmarshal(v, xe); err != nil {
			return nil, fmt.Errorf("parse x_error: %w", err)
		}
		c.XError = xe
	}
	return c, nil
}

// Bytes chunk 原始 JSON
func (c *Chunk) Bytes() []byte { return c.Raw }

// PivotRequest 非流式 pivot(解析保留未知字段)
type PivotRequest struct {
	Raw map[string]any
}

// MarshalJSON 序列化
func (p *PivotRequest) MarshalJSON() ([]byte, error) { return json.Marshal(p.Raw) }

// GetScalar 读顶层标量字段
func (p *PivotRequest) GetScalar(key string) (string, bool) {
	v, ok := p.Raw[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// GetBool 读顶层布尔字段
func (p *PivotRequest) GetBool(key string) bool {
	v, ok := p.Raw[key]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

// GetNumber 读顶层数值字段
func (p *PivotRequest) GetNumber(key string) (float64, bool) {
	v, ok := p.Raw[key]
	if !ok {
		return 0, false
	}
	n, ok := v.(float64)
	return n, ok
}

// PivotResponse 非流式 pivot 响应(含错误形态)
type PivotResponse struct {
	JSON []byte  // 完整响应 JSON
	Err  *XError // 非空 = 错误响应(aggregate 中止/协议错误)
}

// EntryCodec 入口编解码契约(openai/anthropic 两实现)
type EntryCodec interface {
	Parse(body []byte) (*PivotRequest, []Feature, error)
	FromPivotResponse(p *PivotResponse) ([]byte, error)
	NewFromPivotChunker() FromPivotChunker
}

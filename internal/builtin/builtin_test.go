package builtin

import (
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

func TestSupports(t *testing.T) {
	// Given 内置协议 When Supports Then 双形态 + tools/vision
	s := New().Supports()
	if !s.HasForm(pipeline.FormStreaming) || !s.HasForm(pipeline.FormNonStreaming) {
		t.Fatalf("forms: %+v", s)
	}
	if !s.HasFeature("tools") || !s.HasFeature("vision") {
		t.Fatalf("features: %+v", s)
	}
}

func TestBuildRequest_AuthAndURL(t *testing.T) {
	// Given target base url + api_key secret When BuildRequest Then URL 拼接 + Bearer 前缀 + stream 透传
	p := &Protocol{TargetSecrets: func(target, key string) (string, bool) {
		if target == "t1" && key == "api_key" {
			return "sk-k", true
		}
		return "", false
	}}
	ctx := pipeline.NewContext("r1", pipeline.UpstreamInfo{Name: "u"}, pipeline.Vars{Model: "m", EntryStream: true})
	ctx.Target = pipeline.Target{Name: "t1", BaseURL: "https://api.x.com/", SecretsRef: "ref"}
	req, err := p.BuildRequest(ctx, []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL != "https://api.x.com/v1/chat/completions" {
		t.Fatalf("url: %s", req.URL)
	}
	if req.Headers["Authorization"] != "Bearer sk-k" {
		t.Fatalf("auth: %s", req.Headers["Authorization"])
	}
	if !req.Stream {
		t.Fatal("stream flag lost")
	}
}

func TestBuildRequest_MissingSecretRejected(t *testing.T) {
	// Given 无凭据读取器 When BuildRequest Then 拒绝
	p := New()
	ctx := pipeline.NewContext("r", pipeline.UpstreamInfo{}, pipeline.Vars{})
	ctx.Target = pipeline.Target{Name: "t", BaseURL: "https://x"}
	_, err := p.BuildRequest(ctx, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "secret") {
		t.Fatalf("missing secret: %v", err)
	}
}

func TestPassthroughHooks(t *testing.T) {
	// Given 信封帧 When MapEvent Then 返回声明协议事件对象数组;MapResponse 原样
	p := New()
	b := []byte(`{"x":1}`)
	out, err := p.MapResponse(nil, b)
	if err != nil || string(out) != string(b) {
		t.Fatalf("mapResponse: %v", err)
	}
	out, err = p.MapEvent(nil, []byte(`{"event":"","data":"{\"x\":1}"}`))
	if err != nil || string(out) != `[{"x":1}]` {
		t.Fatalf("mapEvent: %v %s", err, out)
	}
	// [DONE] 帧 → 跳帧
	out, err = p.MapEvent(nil, []byte(`{"event":"","data":"[DONE]"}`))
	if err != nil || out != nil {
		t.Fatalf("done frame: %v %s", err, out)
	}
}

func TestPassthroughBody(t *testing.T) {
	// Given 入口原文字节 When BuildRequest Then body 逐字节不变(声明式单协议,不做任何清洗)
	in := []byte(`{"model":"m","messages":[],"top_k":3}`)
	p := &Protocol{TargetSecrets: func(target, key string) (string, bool) { return "k", true }}
	req, err := p.BuildRequest(&pipeline.PipelineContext{
		Target: pipeline.Target{Name: "t", BaseURL: "https://up.example"},
	}, in)
	if err != nil {
		t.Fatal(err)
	}
	if string(req.Body) != string(in) {
		t.Fatalf("body mutated:\n in=%s\nout=%s", in, req.Body)
	}
}

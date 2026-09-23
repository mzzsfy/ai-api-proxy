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
	// Given 包参数 base_url + 当前键 data={"api_key":...} When BuildRequest Then URL 拼接 + Bearer 前缀 + stream 透传
	p := &Protocol{
		Config:     map[string]any{BaseURLParam: "https://api.x.com/"},
		PackageKey: func() (any, bool) { return map[string]any{APIKeyParam: "sk-k"}, true },
	}
	ctx := pipeline.NewContext("r1", pipeline.UpstreamInfo{Name: "u"}, pipeline.Vars{Model: "m", EntryStream: true})
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

func TestBuildRequest_TransportSlot(t *testing.T) {
	// Given 包参数 transport=slow When BuildRequest Then 槽随请求载体下传;缺省 = 空(direct)
	newProto := func(transport any) *Protocol {
		cfg := map[string]any{BaseURLParam: "https://x"}
		if transport != nil {
			cfg[TransportParam] = transport
		}
		return &Protocol{Config: cfg, PackageKey: func() (any, bool) { return "k", true }}
	}
	req, err := newProto("slow").BuildRequest(&pipeline.PipelineContext{}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Transport != "slow" {
		t.Fatalf("transport slot: %q", req.Transport)
	}
	req, err = newProto(nil).BuildRequest(&pipeline.PipelineContext{}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Transport != "" {
		t.Fatalf("default transport: %q", req.Transport)
	}
}

func TestBuildRequest_MissingConfigRejected(t *testing.T) {
	// Given 缺 base_url / 缺键 When BuildRequest Then 拒绝并注明槽名
	p := &Protocol{Config: nil, PackageKey: func() (any, bool) { return "k", true }}
	_, err := p.BuildRequest(&pipeline.PipelineContext{}, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), BaseURLParam) {
		t.Fatalf("missing base_url: %v", err)
	}
	p2 := &Protocol{Config: map[string]any{BaseURLParam: "https://x"}, PackageKey: func() (any, bool) { return map[string]any{}, true }}
	_, err = p2.BuildRequest(&pipeline.PipelineContext{}, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), APIKeyParam) {
		t.Fatalf("missing key: %v", err)
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
	p := &Protocol{Config: map[string]any{BaseURLParam: "https://up.example"}, PackageKey: func() (any, bool) { return "k", true }}
	req, err := p.BuildRequest(&pipeline.PipelineContext{}, in)
	if err != nil {
		t.Fatal(err)
	}
	if string(req.Body) != string(in) {
		t.Fatalf("body mutated:\n in=%s\nout=%s", in, req.Body)
	}
}

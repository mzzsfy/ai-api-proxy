package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// ─── 直发引擎测试(v2:无目标概念;无重试/无熔断,失败即终局) ───

// recordingFilter 记录调用序的假 filter
type recordingFilter struct {
	name   string
	log    *[]string
	mutate func(entry []byte) []byte
}

func (f *recordingFilter) Name() string { return f.name }
func (f *recordingFilter) MapRequest(ctx *PipelineContext, entry []byte) ([]byte, error) {
	*f.log = append(*f.log, "req:"+f.name)
	if f.mutate != nil {
		return f.mutate(entry), nil
	}
	return nil, nil
}
func (f *recordingFilter) MapChunk(ctx *PipelineContext, chunk []byte) ([]byte, error) {
	*f.log = append(*f.log, "chunk:"+f.name)
	return nil, nil
}
func (f *recordingFilter) MapResponse(ctx *PipelineContext, resp []byte) ([]byte, error) {
	*f.log = append(*f.log, "resp:"+f.name)
	return nil, nil
}

// failFilter MapRequest 恒失败
type failFilter struct{ name string }

func (f *failFilter) Name() string { return f.name }
func (f *failFilter) MapRequest(ctx *PipelineContext, entry []byte) ([]byte, error) {
	return nil, fmt.Errorf("boom")
}
func (f *failFilter) MapChunk(ctx *PipelineContext, c []byte) ([]byte, error)    { return nil, nil }
func (f *failFilter) MapResponse(ctx *PipelineContext, r []byte) ([]byte, error) { return nil, nil }

// fakeProtocol 记录 BuildRequest 序列与请求载体
type fakeProtocol struct {
	mu        sync.Mutex
	builds    []string
	requests  []Request
	respBody  string
	forms     []string
	flagSSE   bool
	transport string
	roundtrip int
}

func (p *fakeProtocol) Name() string { return "fake" }
func (p *fakeProtocol) Declared() string {
	return "openai-completions"
}
func (p *fakeProtocol) Supports() Supports {
	if len(p.forms) > 0 {
		return Supports{Forms: p.forms}
	}
	return Supports{Forms: []string{FormNonStreaming, FormStreaming}}
}
func (p *fakeProtocol) BuildRequest(ctx *PipelineContext, entry []byte) (Request, error) {
	p.mu.Lock()
	p.builds = append(p.builds, ctx.Upstream.Name)
	p.roundtrip++
	req := Request{URL: "http://t/up", Method: "POST", Stream: p.flagSSE, Transport: p.transport}
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	return req, nil
}
func (p *fakeProtocol) MapEvent(ctx *PipelineContext, event []byte) ([]byte, error) {
	return event, nil
}
func (p *fakeProtocol) MapResponse(ctx *PipelineContext, body []byte) ([]byte, error) {
	return []byte(p.respBody), nil
}

// fakeTransport 可编程响应(fixed 非空恒返回;err 非空恒失败)
type fakeTransport struct {
	mu    sync.Mutex
	calls int
	fixed TransportResponse
	err   error
	name  string
}

func (t *fakeTransport) Name() string { return t.name }
func (t *fakeTransport) RoundTrip(ctx context.Context, req Request) (TransportResponse, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	if t.err != nil {
		return TransportResponse{}, t.err
	}
	r := t.fixed
	if r.Headers == nil {
		r.Headers = map[string]string{"Content-Type": "application/json"}
	}
	return r, nil
}

func (p *fakeProtocol) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.builds)
}

func resolved(proto Protocol, filters []Filter) Resolved {
	return Resolved{Filters: filters, Protocol: proto}
}

// chunkFailFilter MapChunk 恒失败的 filter
type chunkFailFilter struct{ name string }

func (f *chunkFailFilter) Name() string                                              { return f.name }
func (f *chunkFailFilter) MapRequest(ctx *PipelineContext, p []byte) ([]byte, error) { return p, nil }
func (f *chunkFailFilter) MapChunk(ctx *PipelineContext, c []byte) ([]byte, error) {
	return nil, fmt.Errorf("boom")
}
func (f *chunkFailFilter) MapResponse(ctx *PipelineContext, r []byte) ([]byte, error) { return r, nil }

func TestRun_MapChunkFailureDegradesRaw(t *testing.T) {
	// Given filter MapChunk 失败 When 流式 Then 该 chunk 以修改链前内容 RawPass 透传(不丢帧)
	proto := &fakeProtocol{}
	data := `{"id":"1","delta":{"content":"x"}}`
	events := make(chan SSEFrame, 1)
	events <- SSEFrame{Data: data}
	close(events)
	sse := TransportResponse{
		Status:  200,
		Headers: map[string]string{"Content-Type": "text/event-stream"},
		Events:  events,
	}
	tr := &fakeTransport{name: TransportRef, fixed: sse}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{EntryStream: true})
	resp, err := ex.Run(context.Background(), pctx, resolved(proto, []Filter{&chunkFailFilter{name: "bad"}}), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Stream {
		t.Fatal("expect stream")
	}
	var items []ChunkItem
	for it := range resp.Chunks {
		items = append(items, it)
	}
	if len(items) != 1 {
		t.Fatalf("chunks: %d", len(items))
	}
	if !items[0].RawPass {
		t.Fatal("expect RawPass degrade")
	}
	// pre = 修改链前的 chunk(MapEvent 直通 = 上游帧信封)
	wantEnvelope, _ := json.Marshal(map[string]string{"event": "", "data": data})
	if string(items[0].JSON) != string(wantEnvelope) {
		t.Fatalf("pre-chain chunk expected: %s got %s", wantEnvelope, items[0].JSON)
	}
}

func TestRun_FilterChainOrder(t *testing.T) {
	// Given 两个 filter When Run Then MapRequest 正序,MapResponse 逆序
	var log []string
	f1 := &recordingFilter{name: "f1", log: &log}
	f2 := &recordingFilter{name: "f2", log: &log}
	proto := &fakeProtocol{respBody: `{"ok":true}`}
	ex := &Executor{
		Transports: func(string) (Transport, bool) {
			return &fakeTransport{name: TransportRef}, true
		},
	}
	pctx := NewContext("r1", UpstreamInfo{Name: "u"}, Vars{})
	resp, err := ex.Run(context.Background(), pctx, resolved(proto, []Filter{f1, f2}), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Stream {
		t.Fatal("expect non-stream")
	}
	want := []string{"req:f1", "req:f2", "resp:f2", "resp:f1"}
	if strings.Join(log, ",") != strings.Join(want, ",") {
		t.Fatalf("order: %v", log)
	}
}

func TestRun_MapRequestFail502WithPartName(t *testing.T) {
	// Given filter 失败 When Run Then 错误含部件名
	ex := &Executor{Transports: func(string) (Transport, bool) { return nil, false }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	_, err := ex.Run(context.Background(), pctx, resolved(&fakeProtocol{}, []Filter{&failFilter{name: "bad-f"}}), []byte(`{}`))
	var be *BuildError
	if err == nil || !errors.As(err, &be) || !strings.Contains(err.Error(), "bad-f") {
		t.Fatalf("want part name in error: %v", err)
	}
}

func TestRun_SingleShot_NoFailoverOnTransportError(t *testing.T) {
	// Given 传输失败 When Run Then 直接失败(重试归下游)
	tr := &fakeTransport{name: TransportRef, err: fmt.Errorf("conn refused")}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{}
	_, err := ex.Run(context.Background(), pctx, resolved(proto, nil), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "conn refused") {
		t.Fatalf("transport error: %v", err)
	}
	if proto.count() != 1 {
		t.Fatalf("builds: %v", proto.builds)
	}
}

func TestRun_SingleShot_NoRefreshOn401(t *testing.T) {
	// Given 上游 401 When Run Then 原样透传 401,不发第二次请求(刷新归下游)
	tr := &fakeTransport{name: TransportRef, fixed: TransportResponse{Status: 401, Body: []byte(`{"error":"auth"}`)}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	resp, err := ex.Run(context.Background(), pctx, resolved(&fakeProtocol{}, nil), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 401 || string(resp.Body) != `{"error":"auth"}` {
		t.Fatalf("passthrough: %d %s", resp.Status, resp.Body)
	}
	if tr.calls != 1 {
		t.Fatalf("roundtrips: %d", tr.calls)
	}
}

func TestRun_SingleShot_5xxPassthroughNoRetry(t *testing.T) {
	// Given 上游 503 When Run Then 透传 503 且仅一次请求
	tr := &fakeTransport{name: TransportRef, fixed: TransportResponse{Status: 503, Body: []byte(`overloaded`)}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{}
	resp, err := ex.Run(context.Background(), pctx, resolved(proto, nil), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 503 || string(resp.Body) != "overloaded" {
		t.Fatalf("passthrough: %d %s", resp.Status, resp.Body)
	}
	if proto.count() != 1 {
		t.Fatalf("must be single shot: %v", proto.builds)
	}
}

func TestRun_OnDoneCallback(t *testing.T) {
	// Given OnDone 注入 When 成功/失败 Then 回调按 (upstream, failed) 触发恰一次
	type rec struct {
		name   string
		failed bool
	}
	var log []rec
	newEx := func(tr Transport) *Executor {
		return &Executor{Transports: func(string) (Transport, bool) { return tr, true },
			OnDone: func(upstream string, failed bool) { log = append(log, rec{upstream, failed}) }}
	}
	pctx := NewContext("r", UpstreamInfo{Name: "m1"}, Vars{})
	ok := newEx(&fakeTransport{name: TransportRef, fixed: TransportResponse{Status: 200, Body: []byte(`ok`)}})
	if _, err := ok.Run(context.Background(), pctx, resolved(&fakeProtocol{respBody: `ok`}, nil), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if len(log) != 1 || log[0].name != "m1" || log[0].failed {
		t.Fatalf("onDone ok: %v", log)
	}
	log = nil
	bad := newEx(&fakeTransport{name: TransportRef, err: fmt.Errorf("boom")})
	if _, err := bad.Run(context.Background(), pctx, resolved(&fakeProtocol{}, nil), []byte(`{}`)); err == nil {
		t.Fatal("expect error")
	}
	if len(log) != 1 || !log[0].failed {
		t.Fatalf("onDone failed: %v", log)
	}
}

func TestRun_TransportSlotFromRequest(t *testing.T) {
	// Given 请求载体声明 transport=slow When Run Then 按该名取传输;空 = direct
	calls := map[string]int{}
	ex := &Executor{Transports: func(name string) (Transport, bool) {
		calls[name]++
		return &fakeTransport{name: name, fixed: TransportResponse{Status: 200, Body: []byte(`ok`)}}, true
	}}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{respBody: `ok`, transport: "slow"}
	if _, err := ex.Run(context.Background(), pctx, resolved(proto, nil), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if calls["slow"] != 1 || calls[TransportRef] != 0 {
		t.Fatalf("transport routing: %v", calls)
	}
	proto2 := &fakeProtocol{respBody: `ok`}
	if _, err := ex.Run(context.Background(), pctx, resolved(proto2, nil), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if calls[TransportRef] != 1 {
		t.Fatalf("empty transport defaults to direct: %v", calls)
	}
}

func TestRun_TransportMissing(t *testing.T) {
	// Given 传输不存在 When Run Then 502 错误
	ex := &Executor{Transports: func(string) (Transport, bool) { return nil, false }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	_, err := ex.Run(context.Background(), pctx, resolved(&fakeProtocol{}, nil), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("transport missing: %v", err)
	}
}

func TestRun_MapErrorHook(t *testing.T) {
	// Given 实现 MapError 且 4xx 终局 When Run Then 产物为 mapError 输出与状态
	proto := &fakeProtocol{}
	tr := &fakeTransport{name: TransportRef, fixed: TransportResponse{Status: 400, Body: []byte(`bad`)}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	u := resolved(proto, nil)
	u.Protocol = &mapErrorProto{inner: proto}
	resp, err := ex.Run(context.Background(), pctx, u, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 400 || !strings.Contains(string(resp.Body), "api_error") {
		t.Fatalf("mapError product: %d %s", resp.Status, resp.Body)
	}
}

// mapErrorProto 包装:MapError 产声明协议错误信封
type mapErrorProto struct{ inner Protocol }

func (m *mapErrorProto) Name() string       { return m.inner.Name() }
func (m *mapErrorProto) Declared() string   { return m.inner.Declared() }
func (m *mapErrorProto) Supports() Supports { return m.inner.Supports() }
func (m *mapErrorProto) BuildRequest(ctx *PipelineContext, p []byte) (Request, error) {
	return m.inner.BuildRequest(ctx, p)
}
func (m *mapErrorProto) MapEvent(ctx *PipelineContext, e []byte) ([]byte, error) {
	return m.inner.MapEvent(ctx, e)
}
func (m *mapErrorProto) MapResponse(ctx *PipelineContext, b []byte) ([]byte, error) {
	return m.inner.MapResponse(ctx, b)
}
func (m *mapErrorProto) MapError(ctx *PipelineContext, status int, body []byte) ([]byte, error) {
	return []byte(`{"error":{"type":"api_error","status":400}}`), nil
}

func TestDecideStream_DeclarationRules(t *testing.T) {
	// 表驱动:单声明固定;双声明按 BuildRequest stream 标志,实际 CT 矛盾按实际并告警
	cases := []struct {
		name   string
		forms  []string
		ct     string
		flag   bool
		want   bool
		wanted bool
	}{
		{"streaming only", []string{FormStreaming}, "application/json", false, true, false},
		{"non-streaming only", []string{FormNonStreaming}, "text/event-stream", true, false, false},
		{"dual flag sse actual json", []string{FormStreaming, FormNonStreaming}, "application/json", true, false, true},
		{"dual flag json actual sse", []string{FormStreaming, FormNonStreaming}, "text/event-stream", false, true, true},
		{"dual flag sse actual sse", []string{FormStreaming, FormNonStreaming}, "text/event-stream; charset=utf-8", true, true, false},
		{"dual flag json no ct", []string{FormStreaming, FormNonStreaming}, "", false, false, false},
	}
	for _, tc := range cases {
		p := stubProto{forms: tc.forms}
		tresp := &TransportResponse{Headers: map[string]string{"Content-Type": tc.ct}}
		got, warned := decideStream(p, tc.flag, tresp)
		if got != tc.want || warned != tc.wanted {
			t.Fatalf("%s: got %v(warn=%v) want %v(warn=%v)", tc.name, got, warned, tc.want, tc.wanted)
		}
	}
}

type stubProto struct{ forms []string }

func (s stubProto) Name() string       { return "stub" }
func (s stubProto) Declared() string   { return "openai-completions" }
func (s stubProto) Supports() Supports { return Supports{Forms: s.forms} }
func (s stubProto) BuildRequest(ctx *PipelineContext, p []byte) (Request, error) {
	return Request{}, nil
}
func (s stubProto) MapEvent(ctx *PipelineContext, e []byte) ([]byte, error)    { return e, nil }
func (s stubProto) MapResponse(ctx *PipelineContext, b []byte) ([]byte, error) { return b, nil }

func TestRefreshSingleflight_Removed(t *testing.T) {
	// 重试/刷新语义已删除;此占位保证文件非空(删除时同步删除)
	t.Skip("retry semantics removed by design change: downstream owns retries")
}

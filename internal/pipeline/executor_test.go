package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

// ─── 单目标直发引擎测试:无重试/无切目标/无熔断,失败即终局 ───

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

// fakeProtocol 记录 BuildRequest 目标序列
type fakeProtocol struct {
	mu        sync.Mutex
	builds    []string
	respBody  string
	forms     []string
	flagSSE   bool
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
	p.builds = append(p.builds, ctx.Target.Name)
	p.roundtrip++
	p.mu.Unlock()
	return Request{URL: "http://t/" + ctx.Target.Name, Method: "POST", Stream: p.flagSSE}, nil
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

func testUpstream(proto Protocol, filters []Filter, targets []Target) Resolved {
	return Resolved{Filters: filters, Protocol: proto, Targets: targets}
}

func mkTarget(name string) Target {
	// 测试目标 Transport 引用与目标同名,便于按名分流假传输
	return Target{ID: name, Name: name, BaseURL: "http://" + name, Transport: name, SecretsRef: "ref-" + name}
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
	tr := &fakeTransport{name: "a", fixed: sse}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{EntryStream: true})
	resp, err := ex.Run(context.Background(), pctx, testUpstream(proto, []Filter{&chunkFailFilter{name: "bad"}}, []Target{mkTarget("a")}), []byte(`{}`))
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
	// Given extras→base 两个 filter When Run Then MapRequest 正序,MapResponse 逆序
	var log []string
	f1 := &recordingFilter{name: "f1", log: &log}
	f2 := &recordingFilter{name: "f2", log: &log}
	proto := &fakeProtocol{respBody: `{"ok":true}`}
	ex := &Executor{
		Transports: func(string) (Transport, bool) {
			return &fakeTransport{name: "direct"}, true
		},
	}
	pctx := NewContext("r1", UpstreamInfo{Name: "u"}, Vars{})
	resp, err := ex.Run(context.Background(), pctx, testUpstream(proto, []Filter{f1, f2}, []Target{mkTarget("a")}), []byte(`{}`))
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
	_, err := ex.Run(context.Background(), pctx, testUpstream(&fakeProtocol{}, []Filter{&failFilter{name: "bad-f"}}, []Target{mkTarget("a")}), []byte(`{}`))
	var be *BuildError
	if err == nil || !errors.As(err, &be) || !strings.Contains(err.Error(), "bad-f") {
		t.Fatalf("want part name in error: %v", err)
	}
}

func TestRun_SingleShot_NoFailoverOnTransportError(t *testing.T) {
	// Given 传输失败 When Run Then 不切目标直接失败(重试归下游)
	tr := &fakeTransport{name: "a", err: fmt.Errorf("conn refused")}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{}
	_, err := ex.Run(context.Background(), pctx, testUpstream(proto, nil, []Target{mkTarget("a"), mkTarget("b")}), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "conn refused") {
		t.Fatalf("transport error: %v", err)
	}
	if proto.count() != 1 {
		t.Fatalf("builds: %v", proto.builds)
	}
}

func TestRun_SingleShot_NoRefreshOn401(t *testing.T) {
	// Given 上游 401 When Run Then 原样透传 401,不发第二次请求(刷新归下游)
	tr := &fakeTransport{name: "a", fixed: TransportResponse{Status: 401, Body: []byte(`{"error":"auth"}`)}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	resp, err := ex.Run(context.Background(), pctx, testUpstream(&fakeProtocol{}, nil, []Target{mkTarget("a")}), []byte(`{}`))
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

func TestRun_SingleShot_5xxPassthroughNoFailover(t *testing.T) {
	// Given 首目标 503 When Run Then 透传 503 且不试第二目标
	trA := &fakeTransport{name: "a", fixed: TransportResponse{Status: 503, Body: []byte(`overloaded`)}}
	trB := &fakeTransport{name: "b", fixed: TransportResponse{Status: 200, Body: []byte(`ok`)}}
	// 注入固定随机源(seed=2 首个 Intn(2)=0 先命中 a):全局 rand 自动播种下 50% 先选 b,透传断言须确定命中 a
	ex := &Executor{Rand: rand.New(rand.NewSource(2)), Transports: func(name string) (Transport, bool) {
		if name == "a" {
			return trA, true
		}
		return trB, true
	}}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{}
	resp, err := ex.Run(context.Background(), pctx, testUpstream(proto, nil, []Target{mkTarget("a"), mkTarget("b")}), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 503 || string(resp.Body) != "overloaded" {
		t.Fatalf("passthrough: %d %s", resp.Status, resp.Body)
	}
	if proto.count() != 1 {
		t.Fatalf("must not failover: %v", proto.builds)
	}
}

func TestRun_EnabledTargetsOnly(t *testing.T) {
	// Given 多目标(disabled 已在 Resolve 过滤)When Run Then 恰调用一个目标,失败不切换
	tr := &fakeTransport{name: "first", fixed: TransportResponse{Status: 200, Body: []byte(`ok`)}}
	calls := map[string]int{}
	ex := &Executor{Rand: rand.New(rand.NewSource(1)), Transports: func(name string) (Transport, bool) {
		calls[name]++
		if name == "first" {
			return tr, true
		}
		return &fakeTransport{name: name}, true
	}}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	proto := &fakeProtocol{respBody: `ok`}
	t1 := mkTarget("first")
	_, err := ex.Run(context.Background(), pctx, testUpstream(proto, nil, []Target{t1, mkTarget("second")}), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	sum := calls["first"] + calls["second"]
	if sum != 1 {
		t.Fatalf("exactly one target call expected: %v", calls)
	}
}

func TestWeightedPick_Distribution(t *testing.T) {
	// Given 权重 1/1/2 When 按脚本随机值选择 Then 命中区间与权重边界一致
	targets := []Target{mkTarget("a"), mkTarget("b"), mkTarget("c")}
	targets[2].Weight = 2
	seq := []int{0, 1, 2, 3}
	i := 0
	rnd := func(total int) int {
		if total != 4 {
			t.Fatalf("total weight: %d", total)
		}
		v := seq[i%len(seq)]
		i++
		return v
	}
	got := []string{}
	for range seq {
		got = append(got, weightedPick(targets, rnd).Name)
	}
	want := []string{"a", "b", "c", "c"}
	for j := range want {
		if got[j] != want[j] {
			t.Fatalf("pick %d: got %v want %v", j, got, want)
		}
	}
}

func TestWeightedPick_NonPositiveWeightAsOne(t *testing.T) {
	// Given weight ≤0 与缺省混合 When 选择 Then 全部按 weight=1 参与且总数=3
	targets := []Target{mkTarget("a"), mkTarget("b"), mkTarget("c")}
	targets[0].Weight = -5
	total := 0
	weightedPick(targets, func(n int) int { total = n; return 0 })
	if total != 3 {
		t.Fatalf("total: %d", total)
	}
}

func TestRun_WeightedSelectionCallsExactlyOne(t *testing.T) {
	// Given 多候选目标 When Run Then 恰一个目标被调用且传输按选中名分流
	tr := &fakeTransport{name: "small", fixed: TransportResponse{Status: 200, Body: []byte(`ok`)}}
	calls := map[string]int{}
	ex := &Executor{Rand: rand.New(rand.NewSource(1)), Transports: func(name string) (Transport, bool) {
		calls[name]++
		if name == "small" {
			return tr, true
		}
		return &fakeTransport{name: name}, true
	}}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	big := mkTarget("disabled-big")
	big.Weight = 1000
	small := mkTarget("small")
	small.Weight = 1
	u := testUpstream(&fakeProtocol{respBody: `ok`}, nil, []Target{big, small})
	if _, err := ex.Run(context.Background(), pctx, u, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	sum := calls["disabled-big"] + calls["small"]
	if sum != 1 {
		t.Fatalf("exactly one target must be called: %v", calls)
	}
}

func TestRun_TransportMissing(t *testing.T) {
	// Given 目标传输不存在 When Run Then 502 错误
	ex := &Executor{Transports: func(string) (Transport, bool) { return nil, false }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	_, err := ex.Run(context.Background(), pctx, testUpstream(&fakeProtocol{}, nil, []Target{mkTarget("a")}), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("transport missing: %v", err)
	}
}

func TestRun_MapErrorHook(t *testing.T) {
	// Given 实现 MapError 且 4xx 终局 When Run Then 产物为 mapError 输出与状态
	proto := &fakeProtocol{}
	tr := &fakeTransport{name: "a", fixed: TransportResponse{Status: 400, Body: []byte(`bad`)}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	u := testUpstream(proto, nil, []Target{mkTarget("a")})
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

func TestRun_ZeroTargets(t *testing.T) {
	// Given 零目标 When Run Then 报错
	ex := &Executor{Transports: func(string) (Transport, bool) { return nil, false }}
	pctx := NewContext("r", UpstreamInfo{}, Vars{})
	_, err := ex.Run(context.Background(), pctx, testUpstream(&fakeProtocol{}, nil, nil), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "no enabled targets") {
		t.Fatalf("zero targets: %v", err)
	}
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

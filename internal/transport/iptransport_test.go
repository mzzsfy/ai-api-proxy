package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// ─── ipTransport 裁决点(设计 §5;契约语义在 ipprovider 包测) ───

// countingProvider 计数 mock:Acquire 次数/报告记录,可配置 Dial 结果
type countingProvider struct {
	acquires  atomic.Int64
	dialFails atomic.Int64 // 前 N 次 Dial 失败,之后成功
	reports   atomic.Value // []string
	caps      ipprovider.Capabilities
}

func (c *countingProvider) Capabilities() ipprovider.Capabilities { return c.caps }

func (c *countingProvider) Acquire(ctx context.Context, hint ipprovider.Hint) (ipprovider.Lease, error) {
	c.acquires.Add(1)
	return &countingLease{p: c}, nil
}

func (c *countingProvider) Stats() ipprovider.Stats { return ipprovider.Stats{} }
func (c *countingProvider) Close() error            { return nil }

type countingLease struct {
	p        *countingProvider
	released bool
}

func (l *countingLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if n := l.p.dialFails.Add(-1); n >= 0 {
		return nil, errors.New("injected dial fail")
	}
	// 真 listener:供 http.Client 走完整请求
	ln := dialTargetListener.Load().(net.Listener)
	conn, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (l *countingLease) EgressIP() string { return "" }

func (l *countingLease) Report(result ipprovider.ReportResult, reason string) {
	cur, _ := l.p.reports.Load().([]string)
	l.p.reports.Store(append(cur, reason))
}

func (l *countingLease) Release()                              { l.released = true }
func (l *countingLease) Capabilities() ipprovider.Capabilities { return l.p.caps }

var dialTargetListener atomic.Value

// 场景:connect_fail 且 CanRotateIP=true → 恰好换出口重试一次后成功
func TestIPTransport_connectFail换出口重试一次(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: true}}
	p.dialFails.Store(1) // 首个出口连接失败
	tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
	resp, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusOK {
		t.Fatalf("status=%d", resp.Status)
	}
	if n := p.acquires.Load(); n != 2 {
		t.Fatalf("acquires=%d, want 恰好 2(1 失败+1 成功)", n)
	}
}

// 场景:connect_fail 且 CanRotateIP=false → 不重试直接失败
func TestIPTransport_不可轮换不重试(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: false}}
	p.dialFails.Store(1)
	tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err == nil {
		t.Fatal("want error")
	}
	if n := p.acquires.Load(); n != 1 {
		t.Fatalf("acquires=%d, want 1(不可轮换)", n)
	}
}

// 场景:预算耗尽(连续 2 个出口失败)→ 503 语义错误
func TestIPTransport_预算耗尽(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: true}}
	p.dialFails.Store(2)
	tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err == nil {
		t.Fatal("want error after budget exhausted")
	}
	if n := p.acquires.Load(); n != 2 {
		t.Fatalf("acquires=%d, want 2(预算 1+首次)", n)
	}
}

// 场景:403/429 → 按映射 Report(Bad) 且响应透传(终局,不重试)
func TestIPTransport_状态码裁决透传(t *testing.T) {
	for _, tc := range []struct {
		status int
		reason string
	}{
		{http.StatusForbidden, ipprovider.ReasonTargetBlacklist},
		{http.StatusTooManyRequests, ipprovider.ReasonRateLimited},
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		dialTargetListener.Store(ts.Listener)
		p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: true}}
		tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
		resp, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status != tc.status {
			t.Fatalf("透传破坏: status=%d want %d", resp.Status, tc.status)
		}
		reports, _ := p.reports.Load().([]string)
		if len(reports) != 1 || reports[0] != tc.reason {
			t.Fatalf("reports=%v want [%s]", reports, tc.reason)
		}
		if n := p.acquires.Load(); n != 1 {
			t.Fatalf("acquires=%d, 请求已发出后绝不重试", n)
		}
		ts.Close()
	}
}

// 场景:2xx 且配置映射 → ReportOk(清除供给方既有 Bad 标记,设计 §5 步骤 5)
func TestIPTransport_成功裁决Ok(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{}
	tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err != nil {
		t.Fatal(err)
	}
	reports, _ := p.reports.Load().([]string)
	if len(reports) != 1 || reports[0] != "" { // ReportOk 记空 reason
		t.Fatalf("reports=%v, want [ReportOk]", reports)
	}
}

// 场景:SessionKey 计算(\x00 分隔防碰撞)
func TestSessionKey_防拼接碰撞(t *testing.T) {
	a := sessionKey("a", "bc")
	b := sessionKey("ab", "c")
	if a == b {
		t.Fatal("colliding session keys")
	}
	if len(a) != 16 {
		t.Fatalf("key len=%d want 16", len(a))
	}
}

// 场景:X-IPP-* 头消费后剥离(不出现在上游请求)
func TestIPTransport_亲和头剥离(t *testing.T) {
	var gotAuth, gotSession atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		gotSession.Store(r.Header.Get("X-IPP-Session"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{}
	tr := &ipTransport{name: "t", provider: p, codes: defaultTargetStatusCodes()}
	_, err := tr.RoundTrip(context.Background(), pipeline.Request{
		URL:    ts.URL,
		Method: http.MethodGet,
		Headers: map[string]string{
			"Authorization": "Bearer k",
			"X-IPP-Session": "sess",
			"X-IPP-Model":   "m",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := gotSession.Load().(string); v != "" {
		t.Fatalf("X-IPP-Session leaked to upstream: %q", v)
	}
	if v, _ := gotAuth.Load().(string); v != "Bearer k" {
		t.Fatalf("Authorization = %q", v)
	}
}

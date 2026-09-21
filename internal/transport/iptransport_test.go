package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// ─── ipTransport 裁决点(契约语义在 ipprovider 包测) ───

// countingProvider 计数 mock:Acquire 次数,可配置 Dial 结果
type countingProvider struct {
	acquires  atomic.Int64
	dialFails atomic.Int64 // 前 N 次 Dial 失败,之后成功
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
	return net.Dial(ln.Addr().Network(), ln.Addr().String())
}

func (l *countingLease) EgressIP() string                           { return "" }
func (l *countingLease) Release()                                   { l.released = true }
func (l *countingLease) Capabilities() ipprovider.Capabilities      { return l.p.caps }
func (l *countingLease) WithExclude(ids ...string) ipprovider.Lease { return l }

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
	tr := &ipTransport{name: "t", provider: p}
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
	tr := &ipTransport{name: "t", provider: p}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err == nil {
		t.Fatal("want error")
	}
	if n := p.acquires.Load(); n != 1 {
		t.Fatalf("acquires=%d, want 1(不可轮换)", n)
	}
}

// 场景:预算耗尽(连续 2 个出口失败)→ 终局错误
func TestIPTransport_预算耗尽(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: true}}
	p.dialFails.Store(2)
	tr := &ipTransport{name: "t", provider: p}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err == nil {
		t.Fatal("want error after budget exhausted")
	}
	if n := p.acquires.Load(); n != 2 {
		t.Fatalf("acquires=%d, want 2(预算 1+首次)", n)
	}
}

// 场景:403/429 → 无失效动作,响应透传(终局,不重试)
func TestIPTransport_状态码透传无裁决(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
		dialTargetListener.Store(ts.Listener)
		p := &countingProvider{caps: ipprovider.Capabilities{CanRotateIP: true}}
		tr := &ipTransport{name: "t", provider: p}
		resp, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status != status {
			t.Fatalf("透传破坏: status=%d want %d", resp.Status, status)
		}
		if n := p.acquires.Load(); n != 1 {
			t.Fatalf("acquires=%d, 请求已发出后绝不重试", n)
		}
		ts.Close()
	}
}

// 场景:SessionKey 计算(\x00 分隔防碰撞;原始 16 字节)
func TestSessionKey_防拼接碰撞(t *testing.T) {
	a := sessionKey("a", "bc")
	b := sessionKey("ab", "c")
	if a == b {
		t.Fatal("colliding session keys")
	}
	if len(a) != 16 {
		t.Fatalf("key len=%d want 16(原始字节)", len(a))
	}
}

// 场景:会话亲和素材来自 req.APIKey+req.Model;请求头不携带任何亲和专用头
func TestIPTransport_亲和素材与头(t *testing.T) {
	var gotAuth, gotSession atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		gotSession.Store(r.Header.Get("X-IPP-Session"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &countingProvider{}
	tr := &ipTransport{name: "t", provider: p}
	_, err := tr.RoundTrip(context.Background(), pipeline.Request{
		URL:    ts.URL,
		Method: http.MethodGet,
		APIKey: "sk-abc",
		Model:  "m1",
		Headers: map[string]string{
			"Authorization": "Bearer k",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := gotSession.Load().(string); v != "" {
		t.Fatalf("亲和专用头泄漏到上游: %q", v)
	}
	if v, _ := gotAuth.Load().(string); v != "Bearer k" {
		t.Fatalf("Authorization = %q", v)
	}
}

// repLease 注入 RepError 的 mock(aap 重试路径)
type repProvider struct {
	acquires    atomic.Int64
	dialFails   atomic.Int64 // 前 N 次 Dial 返回 RepError
	repOverride uint8        // 注入的 rep 码(默认 1)
	excludes    atomic.Value // []string:重试 Dial 携带的 exclude 累计
	caps        ipprovider.Capabilities
}

func (p *repProvider) Capabilities() ipprovider.Capabilities { return p.caps }
func (p *repProvider) Acquire(ctx context.Context, hint ipprovider.Hint) (ipprovider.Lease, error) {
	p.acquires.Add(1)
	return &repLease{p: p}, nil
}
func (p *repProvider) Stats() ipprovider.Stats { return ipprovider.Stats{} }
func (p *repProvider) Close() error            { return nil }

type repLease struct {
	p        *repProvider
	excludes []string
}

func (l *repLease) WithExclude(ids ...string) ipprovider.Lease {
	cp := &repLease{p: l.p, excludes: append(append([]string(nil), l.excludes...), ids...)}
	cur, _ := l.p.excludes.Load().([]string)
	l.p.excludes.Store(append(cur, ids...))
	return cp
}

func (l *repLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if n := l.p.dialFails.Add(-1); n >= 0 {
		rep := l.p.repOverride
		if rep == 0 {
			rep = ipprovider.AapRepRetry
		}
		return nil, &ipprovider.RepError{Rep: rep, LeaseID: strings.Repeat("\x01", 16)}
	}
	ln := dialTargetListener.Load().(net.Listener)
	return net.Dial(ln.Addr().Network(), ln.Addr().String())
}

func (l *repLease) EgressIP() string                      { return "" }
func (l *repLease) Release()                              {}
func (l *repLease) Capabilities() ipprovider.Capabilities { return l.p.caps }

// 场景:aap rep=1 → exclude 换出口重试一次后成功(不受 CanRotateIP 限制),exclude 透传断言
func TestIPTransport_AapRep1换出口重试(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &repProvider{caps: ipprovider.Capabilities{CanRotateIP: false}} // aap 路径不看 CanRotateIP
	p.dialFails.Store(1)
	tr := &ipTransport{name: "t", provider: p}
	resp, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusOK {
		t.Fatalf("status=%d", resp.Status)
	}
	if n := p.acquires.Load(); n != 2 {
		t.Fatalf("acquires=%d, want 2", n)
	}
	ex, _ := p.excludes.Load().([]string)
	if len(ex) != 1 || ex[0] != strings.Repeat("\x01", 16) {
		t.Fatalf("excludes=%v, want [回填 lease_id]", ex)
	}
}

// 场景:aap rep≠0 非 1 → 终局失败不重试
func TestIPTransport_AapRep2不重试(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	dialTargetListener.Store(ts.Listener)
	p := &repProvider{caps: ipprovider.Capabilities{CanRotateIP: true}, repOverride: 2}
	p.dialFails.Store(1)
	tr := &ipTransport{name: "t", provider: p}
	if _, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet}); err == nil {
		t.Fatal("want error(rep=2 终局)")
	}
	if n := p.acquires.Load(); n != 1 {
		t.Fatalf("acquires=%d, want 1(rep=2 不重试)", n)
	}
}

// 场景:rep 终局映射 —— rep=1 耗尽/rep=2 → ErrNoEgress503;rep=3 → 非 503 通道(502)
func TestIPTransport_Rep终局映射(t *testing.T) {
	for _, tc := range []struct {
		rep     uint8
		want503 bool
	}{
		{ipprovider.AapRepRetry, true},
		{2, true},
		{3, false},
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		dialTargetListener.Store(ts.Listener)
		p := &repProvider{caps: ipprovider.Capabilities{CanRotateIP: true}, repOverride: tc.rep}
		p.dialFails.Store(2) // 两次失败(首次+重试),预算耗尽
		tr := &ipTransport{name: "t", provider: p}
		_, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: ts.URL, Method: http.MethodGet})
		ts.Close()
		if err == nil {
			t.Fatalf("rep=%d want error", tc.rep)
		}
		if got := errors.Is(err, ErrNoEgress503); got != tc.want503 {
			t.Fatalf("rep=%d ErrNoEgress503=%v, want %v (err=%v)", tc.rep, got, tc.want503, err)
		}
	}
}

// repLease 注入 RepError 的 mock(aap 重试路径)

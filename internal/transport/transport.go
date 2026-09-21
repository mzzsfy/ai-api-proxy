package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/proxy"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// Manager 命名实例集合(配置期构建,运行期只读)
type Manager struct {
	instances map[string]pipeline.Transport
	defs      []TransportDef
	// providers 供给方实例(生命周期随 Manager)
	mu        sync.Mutex
	providers map[string]ipprovider.Provider
}

// NewManager 按配置构建;每实例一个 http.Client(连接池实例内共享,禁 Client.Timeout)
func NewManager(defs []TransportDef) (*Manager, error) {
	return NewManagerWithCfg(defs, "")
}

// NewManagerWithCfg 完整装配:state 根目录
func NewManagerWithCfg(defs []TransportDef, stateRoot string) (*Manager, error) {
	m := &Manager{
		instances: map[string]pipeline.Transport{},
		defs:      append([]TransportDef(nil), defs...),
		providers: map[string]ipprovider.Provider{},
	}
	for _, d := range defs {
		tr, prov, err := m.build(d, stateRoot)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("transport %s: %w", d.Name, err)
		}
		m.instances[d.Name] = tr
		if prov != nil {
			m.providers[d.Name] = prov
		}
	}
	// direct 默认实例必然存在
	if _, ok := m.instances[pipeline.TransportRef]; !ok {
		m.instances[pipeline.TransportRef] = &httpTransport{name: pipeline.TransportRef, client: &http.Client{}}
	}
	return m, nil
}

// Provider 传输实例对应的供给方(非供给方传输返回 nil)
func (m *Manager) Provider(name string) (ipprovider.Provider, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.providers[name]
	return p, ok
}

// SupplierKind 传输实例是否供给方类(ipp+/aap;true=亲和素材缺失会退化为恒定绑定)
func (m *Manager) SupplierKind(name string) (string, bool) {
	for _, d := range m.defs {
		if d.Name == name {
			return d.URL, isSupplierURL(d.URL)
		}
	}
	return "", false
}

// EvictScope 管理面失效 scope(协议 §EVICT)
const (
	EvictScopeLease  = 1
	EvictScopeEgress = 2
)

// Evict 对传输实例的供给方发失效命令;非供给方传输返回 false(映射 400)
func (m *Manager) Evict(ctx context.Context, name string, scope uint8, value []byte) (bool, error) {
	p, ok := m.Provider(name)
	if !ok {
		return false, nil
	}
	ev, ok := p.(ipprovider.Evictor)
	if !ok {
		return false, nil
	}
	return true, ev.Evict(ctx, scope, value)
}

// Close 级联关闭全部供给方(幂等;错误记日志不中断)
func (m *Manager) Close() {
	m.mu.Lock()
	providers := m.providers
	m.providers = map[string]ipprovider.Provider{}
	m.mu.Unlock()
	for name, p := range providers {
		if err := p.Close(); err != nil {
			log.Printf("transport %s: provider close: %v", name, err)
		}
	}
}

// Get 取实例
func (m *Manager) Get(name string) (pipeline.Transport, bool) {
	tr, ok := m.instances[name]
	return tr, ok
}

// Names 实例名清单(稳定序)
func (m *Manager) Names() []string {
	names := make([]string, 0, len(m.instances))
	for n := range m.instances {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Defs 实例定义(只读视图)
func (m *Manager) Defs() []TransportDef {
	out := make([]TransportDef, 0, len(m.defs))
	out = append(out, m.defs...)
	return out
}

// Test 单实例连通测试:经该传输对 probeURL 发 HEAD,验证代理链路(TCP/CONNECT/握手)
func (m *Manager) Test(name, probeURL string, timeout time.Duration) (int64, error) {
	tr, ok := m.instances[name]
	if !ok {
		return 0, fmt.Errorf("transport %q not found", name)
	}
	req, err := http.NewRequest(http.MethodHead, probeURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build probe: %w", err)
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	start := time.Now()
	// probe 会话亲和素材独立命名空间,不污染业务绑定
	tresp, err := tr.RoundTrip(ctx, pipeline.Request{URL: probeURL, Method: http.MethodHead, APIKey: "aap-probe:" + name})
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return latency, err
	}
	if tresp.Status >= 500 {
		return latency, fmt.Errorf("probe status %d", tresp.Status)
	}
	return latency, nil
}

// TransportDef 实例定义(server.Config.Transports 展开形态)
type TransportDef struct {
	Name string
	// URL 判型:scheme 推导——direct(字面值)/socks5://http://https://aap://ipp+<kind>://
	URL string
	// Options 供给方私有配置(仅 aap:affinity_ttl;其余 scheme 必须为空)
	Options map[string]any
}

// build 构造单实例;供给方类 scheme 同时返回 Provider
func (m *Manager) build(d TransportDef, stateRoot string) (pipeline.Transport, ipprovider.Provider, error) {
	switch {
	case d.URL == "direct":
		return &httpTransport{name: d.Name, client: &http.Client{}}, nil, nil
	case strings.HasPrefix(d.URL, "http://"), strings.HasPrefix(d.URL, "https://"):
		u, err := url.Parse(d.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("parse proxy url: %w", err)
		}
		client := &http.Client{
			Transport: &http.Transport{Proxy: http.ProxyURL(u)},
		}
		return &httpTransport{name: d.Name, client: client}, nil, nil
	case strings.HasPrefix(d.URL, "socks5://"):
		base, err := url.Parse(d.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("parse socks url: %w", err)
		}
		var auth *proxy.Auth
		if base.User != nil {
			pw, _ := base.User.Password()
			auth = &proxy.Auth{User: base.User.Username(), Password: pw}
		}
		dialer, err := proxy.SOCKS5("tcp", base.Host, auth, proxy.Direct)
		if err != nil {
			return nil, nil, fmt.Errorf("socks5 dialer: %w", err)
		}
		cd, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return nil, nil, fmt.Errorf("socks5 dialer not context aware")
		}
		client := &http.Client{
			Transport: &http.Transport{DialContext: cd.DialContext},
		}
		return &httpTransport{name: d.Name, client: client}, nil, nil
	case strings.HasPrefix(d.URL, "aap://"):
		prov, err := ipprovider.Create("aap", ipprovider.ProviderCfg{
			Name:     d.Name,
			URL:      d.URL,
			Options:  d.Options,
			StateDir: joinStateDir(stateRoot, d.Name),
		})
		if err != nil {
			return nil, nil, err
		}
		return &ipTransport{name: d.Name, provider: prov}, prov, nil
	}
	if kind, ok := strings.CutPrefix(d.URL, "ipp+"); ok {
		kindName, rest, found := strings.Cut(kind, "://")
		if !found || kindName == "" || rest == "" {
			return nil, nil, fmt.Errorf("ipp url 须为 ipp+<kind>://<参数>")
		}
		prov, err := ipprovider.Create("ipp_"+kindName, ipprovider.ProviderCfg{
			Name:     d.Name,
			URL:      rest,
			Options:  d.Options,
			StateDir: joinStateDir(stateRoot, d.Name),
		})
		if err != nil {
			return nil, nil, err
		}
		return &ipTransport{name: d.Name, provider: prov}, prov, nil
	}
	return nil, nil, fmt.Errorf("unknown transport url %q", d.URL)
}

// isSupplierURL 供给方类 url 判型(ipp+<kind>:// 或 aap://)
func isSupplierURL(raw string) bool {
	if strings.HasPrefix(raw, "aap://") {
		return true
	}
	kind, ok := strings.CutPrefix(raw, "ipp+")
	return ok && strings.Contains(kind, "://")
}

// joinStateDir 供给方状态目录(data 根下按传输名隔离)
func joinStateDir(root, name string) string {
	if root == "" {
		return name
	}
	return root + "/" + name
}

// sessionKey 会话亲和键:sha256(apiKey + NUL + model) 前 16 字节原始值(设计定案;\x00 分隔防拼接碰撞;model 为空退化为单素材形态);诊断渲染用 ipprovider.SessionHex
func sessionKey(apiKey, model string) string {
	if model == "" {
		return ipprovider.SessionKey(apiKey)
	}
	return ipprovider.SessionKey(apiKey, model)
}

// httpTransport 标准库实现变体
type httpTransport struct {
	name   string
	client *http.Client
}

func (t *httpTransport) Name() string { return t.name }

// RoundTrip 执行请求;流式响应转帧通道
func (t *httpTransport) RoundTrip(ctx context.Context, req pipeline.Request) (pipeline.TransportResponse, error) {
	var bodyReader io.Reader
	if len(req.Body) > 0 {
		bodyReader = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bodyReader)
	if err != nil {
		return pipeline.TransportResponse{}, fmt.Errorf("build request: %w", err)
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	hresp, err := t.client.Do(hreq)
	if err != nil {
		return pipeline.TransportResponse{}, fmt.Errorf("round trip: %w", err)
	}
	return consumeResponse(hresp), nil
}

// ipTransport IP 供给方传输:每请求 Acquire → lease 拨号;失效无错误驱动
// (403/429 直接透传);恰一次边界:连接建立失败(零字节触达)换出口重试预算 1,
// 请求已发出后终局透传
type ipTransport struct {
	name     string
	provider ipprovider.Provider
}

func (t *ipTransport) Name() string { return t.name }

// RoundTrip 供给方路径
// 恰一次边界:仅"连接未建立(零字节触达)"的失败可换出口重试,预算 1;
// 请求已写出(TLS 握手启动/请求写出)后任何错误终局透传。
// aap rep=1 走 exclude 换出口(RepError 驱动);ipp_* 走 CanRotateIP 判定;
// rep 终局映射:rep=1/2 → errNoEgress503,rep=3 → 502(gateway 转译)。
func (t *ipTransport) RoundTrip(ctx context.Context, req pipeline.Request) (pipeline.TransportResponse, error) {
	hint := ipprovider.Hint{SessionKey: sessionKey(req.APIKey, req.Model)}
	retryBudget := 1
	var excludes []string
	for {
		lease, err := t.provider.Acquire(ctx, hint)
		if err != nil {
			if ctx.Err() != nil {
				return pipeline.TransportResponse{}, fmt.Errorf("acquire lease: %w", ctx.Err())
			}
			return pipeline.TransportResponse{}, fmt.Errorf("acquire lease: %w", err)
		}
		if ex, ok := lease.(ipprovider.Excluder); ok && len(excludes) > 0 {
			lease = ex.WithExclude(excludes...)
		}
		// 契约:Release 幂等;defer 保证 panic/早退路径租约必归还
		defer lease.Release()
		resp, dialErr, unwritten := t.via(ctx, lease, req)
		if dialErr != nil {
			var repErr *ipprovider.RepError
			if errors.As(dialErr, &repErr) && unwritten && retryBudget > 0 && repErr.Rep == ipprovider.AapRepRetry {
				retryBudget--
				if id := repErr.LeaseID; !ipprovider.IsZeroLeaseID(id) {
					excludes = append(excludes, id)
				}
				continue // rep=1:请求未写出,换出口重试
			}
			if errors.As(dialErr, &repErr) {
				return pipeline.TransportResponse{}, translateRep(repErr) // aap 非 rep=1 终局
			}
			if unwritten && retryBudget > 0 && lease.Capabilities().CanRotateIP {
				retryBudget--
				continue // ipp_* 零字节触达,换出口重试
			}
			return pipeline.TransportResponse{}, dialErr
		}
		return resp, nil
	}
}

// ErrNoEgress503 rep=1/2 终局(出口不可用/节点并发上限;临时性,gateway 映射 503)
var ErrNoEgress503 = errors.New("no available egress")

// translateRep RepError → host 状态映射(rep=1/2→503 通道;rep=3→502)
func translateRep(repErr *ipprovider.RepError) error {
	if repErr.Rep == ipprovider.AapRepRetry || repErr.Rep == ipprovider.AapRepBusy {
		return fmt.Errorf("%w: aap rep=%d", ErrNoEgress503, repErr.Rep)
	}
	return fmt.Errorf("aap node rep=%d", repErr.Rep)
}

// via 经 lease 建连执行请求(Transport 不设 Proxy 字段——隧道语义在 Lease.Dial 内,设计 R1 定案)。
// 第三返回值 unwritten:错误发生时请求是否尚未写出(零字节触达上游;httptrace 边界)。
func (t *ipTransport) via(ctx context.Context, lease ipprovider.Lease, req pipeline.Request) (pipeline.TransportResponse, error, bool) {
	var bodyReader io.Reader
	if len(req.Body) > 0 {
		bodyReader = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bodyReader)
	if err != nil {
		return pipeline.TransportResponse{}, fmt.Errorf("build request: %w", err), true
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	// DisableKeepAlives:每请求新租约新连接,keep-alive 归池只会泄漏 fd/goroutine
	tr := &http.Transport{
		DialContext:       lease.Dial,
		DisableKeepAlives: true,
	}
	// 请求写出边界探测:TLS 握手启动或请求体写出后,失败不再可安全重试
	var wroteRequest, tlsStarted atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wroteRequest.Store(true)
			}
		},
		TLSHandshakeStart: func() { tlsStarted.Store(true) },
	}
	hreq = hreq.WithContext(httptrace.WithClientTrace(hreq.Context(), trace))
	client := &http.Client{Transport: tr}
	hresp, err := client.Do(hreq)
	if err != nil {
		// 关闭本 transport 的 idle 池(虽已禁 keep-alive,防御性回收)
		client.CloseIdleConnections()
		return pipeline.TransportResponse{}, fmt.Errorf("round trip via lease: %w", err), !(wroteRequest.Load() || tlsStarted.Load())
	}
	client.CloseIdleConnections()
	return consumeResponse(hresp), nil, false
}

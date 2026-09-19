package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
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
	// providers ipp_* 类型实例(生命周期随 Manager;非 ipp 传输无条目)
	mu        sync.Mutex
	providers map[string]ipprovider.Provider
	// targetStatusCodes 状态码→裁决原因映射(空=默认 403/429)
	targetStatusCodes map[int]string
}

// defaultTargetStatusCodes 裁决映射默认值(设计定案:403→blacklist,429→限速)
func defaultTargetStatusCodes() map[int]string {
	return map[int]string{
		403: ipprovider.ReasonTargetBlacklist,
		429: ipprovider.ReasonRateLimited,
	}
}

// NewManager 按配置构建;每实例一个 http.Client(连接池实例内共享,禁 Client.Timeout)
func NewManager(defs []TransportDef) (*Manager, error) {
	return NewManagerWithCfg(defs, defaultTargetStatusCodes(), "")
}

// NewManagerWithCfg 完整装配:ipp 供给方注册表 + 裁决映射 + state 根目录
func NewManagerWithCfg(defs []TransportDef, targetStatusCodes map[int]string, stateRoot string) (*Manager, error) {
	if len(targetStatusCodes) == 0 {
		targetStatusCodes = defaultTargetStatusCodes()
	}
	m := &Manager{
		instances:         map[string]pipeline.Transport{},
		defs:              append([]TransportDef(nil), defs...),
		providers:         map[string]ipprovider.Provider{},
		targetStatusCodes: targetStatusCodes,
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

// Provider 传输实例对应的供给方(非 ipp 传输返回 nil)
func (m *Manager) Provider(name string) (ipprovider.Provider, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.providers[name]
	return p, ok
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
	tresp, err := tr.RoundTrip(ctx, pipeline.Request{URL: probeURL, Method: http.MethodHead})
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
	Name    string
	Type    string
	URL     string
	Options map[string]any // ipp_* 私有配置(供给方自行解析)
}

// build 构造单实例;ipp_* 类型同时返回供给方
func (m *Manager) build(d TransportDef, stateRoot string) (pipeline.Transport, ipprovider.Provider, error) {
	switch d.Type {
	case "direct":
		return &httpTransport{name: d.Name, client: &http.Client{}}, nil, nil
	case "http_proxy":
		u, err := url.Parse(d.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("parse proxy url: %w", err)
		}
		client := &http.Client{
			Transport: &http.Transport{Proxy: http.ProxyURL(u)},
		}
		return &httpTransport{name: d.Name, client: client}, nil, nil
	case "socks5":
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
	}
	if isIPPType(d.Type) {
		prov, err := ipprovider.Create(d.Type, ipprovider.ProviderCfg{
			Name:     d.Name,
			URL:      d.URL,
			Options:  d.Options,
			StateDir: joinStateDir(stateRoot, d.Name),
		})
		if err != nil {
			return nil, nil, err
		}
		tr := &ipTransport{name: d.Name, provider: prov, codes: m.targetStatusCodes}
		return tr, prov, nil
	}
	return nil, nil, fmt.Errorf("unknown transport type %q", d.Type)
}

// isIPPType 是否 IP 供给方类型(ipp_ 前缀)
func isIPPType(t string) bool {
	const prefix = "ipp_"
	return len(t) > len(prefix) && t[:len(prefix)] == prefix
}

// joinStateDir 供给方状态目录(data 根下按传输名隔离)
func joinStateDir(root, name string) string {
	if root == "" {
		return name
	}
	return root + "/" + name
}

// sessionKey 会话亲和键:sha256hex(apiKey + NUL + model) 前 16 字符(设计定案,\x00 分隔防拼接碰撞)
func sessionKey(apiKey, model string) string {
	h := sha256.Sum256([]byte(apiKey + "\x00" + model))
	return hex.EncodeToString(h[:])[:16]
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

// ipTransport IP 供给方传输(设计 §5):每请求 Acquire → lease 拨号 → 裁决;
// 恰一次边界:连接建立失败(零字节触达)换出口重试预算 1;请求已发出后终局透传
type ipTransport struct {
	name     string
	provider ipprovider.Provider
	codes    map[int]string
}

func (t *ipTransport) Name() string { return t.name }

// RoundTrip 供给方路径
// 恰一次边界(设计 §1):仅"连接未建立(零字节触达)"的失败可换出口重试,预算 1;
// 请求已写出(TLS 握手启动/请求写出)后任何错误终局透传。
func (t *ipTransport) RoundTrip(ctx context.Context, req pipeline.Request) (pipeline.TransportResponse, error) {
	key, model := req.Headers["X-IPP-Session"], req.Model
	delete(req.Headers, "X-IPP-Session") // gateway 旧注入路径清退;素材以 req.Model 为准
	retryBudget := 1
	for {
		lease, err := t.provider.Acquire(ctx, ipprovider.Hint{SessionKey: sessionKey(key, model)})
		if err != nil {
			if ctx.Err() != nil {
				return pipeline.TransportResponse{}, fmt.Errorf("acquire lease: %w", ctx.Err())
			}
			return pipeline.TransportResponse{}, fmt.Errorf("acquire lease: %w", err)
		}
		// 契约:Release 幂等;defer 保证 panic/早退路径租约必归还
		defer lease.Release()
		resp, dialErr, unwritten := t.via(ctx, lease, req)
		if dialErr != nil {
			lease.Report(ipprovider.ReportBad, ipprovider.ReasonConnectFail)
			if unwritten && retryBudget > 0 && lease.Capabilities().CanRotateIP {
				retryBudget--
				continue // 零字节触达,换出口重试
			}
			return pipeline.TransportResponse{}, dialErr
		}
		// 请求已发出:终局透传;状态码命中映射裁决出口;2xx 清供给方既有 Bad 标记
		if reason, hit := t.codes[resp.Status]; hit {
			lease.Report(ipprovider.ReportBad, reason)
		} else if resp.Status >= 200 && resp.Status < 300 {
			lease.Report(ipprovider.ReportOk, "")
		}
		return resp, nil
	}
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

var _ = net.Dialer{} // 保持 net 引用(dial 语义经 lease.Dial;直接依赖不漂移)

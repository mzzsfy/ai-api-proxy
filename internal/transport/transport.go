// Package transport 命名传输实例:direct/http_proxy/socks5(http.RoundTripper 变体)
package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"

	"golang.org/x/net/proxy"

	"ai-api-proxy/internal/pipeline"
)

// Manager 命名实例集合(配置期构建,运行期只读)
type Manager struct {
	instances map[string]pipeline.Transport
	defs      []TransportDef
}

// NewManager 按配置构建;每实例一个 http.Client(连接池实例内共享,禁 Client.Timeout)
func NewManager(defs []TransportDef) (*Manager, error) {
	m := &Manager{instances: map[string]pipeline.Transport{}, defs: append([]TransportDef(nil), defs...)}
	for _, d := range defs {
		tr, err := build(d)
		if err != nil {
			return nil, fmt.Errorf("transport %s: %w", d.Name, err)
		}
		m.instances[d.Name] = tr
	}
	// direct 默认实例必然存在
	if _, ok := m.instances[pipeline.TransportRef]; !ok {
		m.instances[pipeline.TransportRef] = &httpTransport{name: pipeline.TransportRef, client: &http.Client{}}
	}
	return m, nil
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
	Name string
	Type string
	URL  string
}

// build 构造单实例
func build(d TransportDef) (pipeline.Transport, error) {
	switch d.Type {
	case "direct":
		return &httpTransport{name: d.Name, client: &http.Client{}}, nil
	case "http_proxy":
		u, err := url.Parse(d.URL)
		if err != nil {
			return nil, fmt.Errorf("parse proxy url: %w", err)
		}
		client := &http.Client{
			Transport: &http.Transport{Proxy: http.ProxyURL(u)},
		}
		return &httpTransport{name: d.Name, client: client}, nil
	case "socks5":
		base, err := url.Parse(d.URL)
		if err != nil {
			return nil, fmt.Errorf("parse socks url: %w", err)
		}
		var auth *proxy.Auth
		if base.User != nil {
			pw, _ := base.User.Password()
			auth = &proxy.Auth{User: base.User.Username(), Password: pw}
		}
		dialer, err := proxy.SOCKS5("tcp", base.Host, auth, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("socks5 dialer: %w", err)
		}
		cd, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("socks5 dialer not context aware")
		}
		client := &http.Client{
			Transport: &http.Transport{DialContext: cd.DialContext},
		}
		return &httpTransport{name: d.Name, client: client}, nil
	default:
		return nil, fmt.Errorf("unknown transport type %q", d.Type)
	}
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
	headers := map[string]string{}
	for k := range hresp.Header {
		headers[k] = hresp.Header.Get(k)
	}
	out := pipeline.TransportResponse{Status: hresp.StatusCode, Headers: headers}
	if isEventStream(headers["Content-Type"]) {
		out.Events = frameSSE(hresp.Body)
	} else {
		out.Body = readAllLimited(hresp.Body)
		_ = hresp.Body.Close()
	}
	return out, nil
}

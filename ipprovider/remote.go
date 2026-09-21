package ipprovider

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// remoteProvider 远程供给 API v1 适配器(设计 §4.3):
// acquire(拿 lease 级能力与代理地址)/ report(出口裁决)/ release(归还)/ stats(观测)。
// 拨号走 acquire 返回的 proxy.url(远程决策,本地拨号)。
func init() {
	Register("ipp_remote", newRemoteProvider)
}

// remoteOptions ipp_remote 私有配置
type remoteOptions struct {
	// Base 供给方 API 根(必须 https;httptest 场景允许 http,经 allowInsecure 显式放行)
	Base string `json:"base"`
	// APIKey Bearer 凭据
	APIKey string `json:"api_key"`
	// AllowInsecure 允许 http base(仅测试)
	AllowInsecure bool `json:"allow_insecure"`
	// StaticCaps 配置静态能力声明(与服务端 lease 级不一致时以服务端为准)
	CanRotateIP     bool `json:"can_rotate_ip"`
	SessionAffinity bool `json:"session_affinity"`
}

func newRemoteProvider(cfg ProviderCfg) (Provider, error) {
	var o remoteOptions
	if err := decodeOptions(cfg.Options, &o); err != nil {
		return nil, fmt.Errorf("ipp_remote %s: %w", cfg.Name, err)
	}
	if o.Base == "" {
		return nil, fmt.Errorf("ipp_remote %s: base required", cfg.Name)
	}
	if !o.AllowInsecure && !isHTTPS(o.Base) {
		return nil, fmt.Errorf("ipp_remote %s: base 必须 https(测试场景显式 allow_insecure)", cfg.Name)
	}
	if o.APIKey == "" {
		return nil, fmt.Errorf("ipp_remote %s: api_key required", cfg.Name)
	}
	return &remoteProvider{
		opts:   o,
		caps:   Capabilities{CanRotateIP: o.CanRotateIP, SessionAffinity: o.SessionAffinity},
		client: &http.Client{Timeout: remoteRPCTimeout},
	}, nil
}

// remoteRPCTimeout 单次 RPC 上限(acquire/report/release 均为短请求)
const remoteRPCTimeout = 10 * time.Second

type remoteProvider struct {
	opts   remoteOptions
	caps   Capabilities
	client *http.Client
}

func (p *remoteProvider) Capabilities() Capabilities { return p.caps }

// remoteCaps acquire 响应的 lease 级能力(服务端为准)
type remoteCaps struct {
	CanRotateIP     bool `json:"can_rotate_ip"`
	SessionAffinity bool `json:"session_affinity"`
}

type remoteAcquireResp struct {
	LeaseID      string     `json:"lease_id"`
	Proxy        remoteAddr `json:"proxy"`
	EgressIP     string     `json:"egress_ip"`
	Capabilities remoteCaps `json:"capabilities"`
}

type remoteAddr struct {
	Type string `json:"type"` // socks5 | http
	URL  string `json:"url"`
}

// remoteReportReq 远程 release 请求体
type remoteReportReq struct {
	LeaseID string `json:"lease_id"`
}

func (p *remoteProvider) Acquire(ctx context.Context, hint Hint) (Lease, error) {
	// session_key 以 hex 文本下发(JSON 文本契约;原始字节含非法 UTF-8 会被 Marshal 塌缩)
	body, _ := json.Marshal(map[string]string{"session_key": hex.EncodeToString([]byte(hint.SessionKey))})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.Base+"/acquire", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	p.auth(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err) // 网络/5xx 均按无出口处理(调用方映射 503)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var ar remoteAcquireResp
		if err := json.NewDecoder(io.LimitReader(resp.Body, remoteBodyLimit)).Decode(&ar); err != nil {
			return nil, fmt.Errorf("acquire resp: %w", err)
		}
		if ar.LeaseID == "" || ar.Proxy.URL == "" {
			return nil, fmt.Errorf("acquire resp: lease_id/proxy.url required")
		}
		caps := Capabilities{CanRotateIP: ar.Capabilities.CanRotateIP, SessionAffinity: ar.Capabilities.SessionAffinity}
		if caps != p.caps {
			logf("ipp_remote %s: 服务端能力 %v 与配置声明 %v 不一致,以服务端为准", p.opts.Base, caps, p.caps)
		}
		var dialer ProxyDialer
		switch ar.Proxy.Type {
		case "socks5":
			dialer = ProxyDialer{Type: "socks5", URL: ar.Proxy.URL}
		case "http":
			dialer = ProxyDialer{Type: "http_proxy", URL: ar.Proxy.URL}
		default:
			return nil, fmt.Errorf("acquire resp: 未知 proxy.type %q(仅支持 socks5|http)", ar.Proxy.Type)
		}
		return &remoteLease{p: p, id: ar.LeaseID, dialer: dialer, egress: ar.EgressIP, caps: caps}, nil
	case http.StatusForbidden, http.StatusUnauthorized:
		return nil, fmt.Errorf("acquire: 鉴权失败(%d),检查 api_key", resp.StatusCode)
	case http.StatusServiceUnavailable:
		return nil, ErrNoExits
	default:
		return nil, fmt.Errorf("acquire: status %d", resp.StatusCode)
	}
}

// release 实现经由 lease(带 lease_id);Provider 级不做全局通知
func (p *remoteProvider) release(ctx context.Context, leaseID string) error {
	body, _ := json.Marshal(remoteReportReq{LeaseID: leaseID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.Base+"/release", bytes.NewReader(body))
	if err != nil {
		return err
	}
	p.auth(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("release: status %d", resp.StatusCode)
	}
	return nil
}

func (p *remoteProvider) Stats() Stats {
	s := Stats{EgressIPs: []string{}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.opts.Base+"/stats", nil)
	if err != nil {
		return s
	}
	p.auth(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return s
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s
	}
	var rs struct {
		Total     int      `json:"total"`
		Normal    int      `json:"normal"`
		Probing   int      `json:"probing"`
		Draining  int      `json:"draining"`
		Disabled  int      `json:"disabled"`
		EgressIPs []string `json:"egress_ips"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, remoteBodyLimit)).Decode(&rs) != nil {
		return s
	}
	s.Total, s.Normal, s.Probing, s.Draining, s.Disabled = rs.Total, rs.Normal, rs.Probing, rs.Draining, rs.Disabled
	s.EgressIPs = rs.EgressIPs
	return s
}

func (p *remoteProvider) Close() error { return nil }

func (p *remoteProvider) auth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+p.opts.APIKey)
	req.Header.Set("Content-Type", "application/json")
}

// remoteBodyLimit RPC 响应体上限(防护失控供给方)
const remoteBodyLimit = 1 << 20

// remoteLease 单请求作用域
type remoteLease struct {
	p        *remoteProvider
	id       string
	dialer   ProxyDialer
	egress   string
	caps     Capabilities
	released bool
	mu       sync.Mutex
}

func (l *remoteLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil, ErrLeaseReleased
	}
	l.mu.Unlock()
	return l.dialer.Dial(ctx, network, addr)
}

func (l *remoteLease) EgressIP() string { return l.egress }

// Release 幂等;异步通知供给方归还(不阻塞响应路径;SSE 长流不受 10s RPC 牵连;
// 失败仅记日志,TTL 兜底)
func (l *remoteLease) Release() {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	p := l.p
	id := l.id
	l.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
		defer cancel()
		if err := p.release(ctx, id); err != nil {
			logf("ipp_remote: release %s: %v", id, err)
		}
	}()
}

func (l *remoteLease) Capabilities() Capabilities { return l.caps }

// isHTTPS base 是否 https 形态
func isHTTPS(base string) bool {
	return len(base) >= httpsSchemeLen && base[:httpsSchemeLen] == "https://"
}

// httpsSchemeLen "https://" 长度
const httpsSchemeLen = len("https://")

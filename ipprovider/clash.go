package ipprovider

import (
	"context"
	"fmt"
	"net"
)

// clashProvider 单出口适配(设计 §4.2):连接既有 clash/mihomo 混合端口,
// 无池无轮换,Report 仅记录健康标记。出口 IP 不可见(单出口无探测,本期接受)。
func init() {
	Register("ipp_clash", newClashProvider)
}

// clashOptions ipp_clash 私有配置
type clashOptions struct {
	// Type 代理形态:http_proxy(混合端口的 http 形态)| socks5
	Type string `json:"type"`
	// URL 代理地址(http://127.0.0.1:6654 / socks5://...)
	URL string `json:"url"`
}

func newClashProvider(cfg ProviderCfg) (Provider, error) {
	var o clashOptions
	if err := decodeOptions(cfg.Options, &o); err != nil {
		return nil, fmt.Errorf("ipp_clash %s: %w", cfg.Name, err)
	}
	if o.Type != "http_proxy" && o.Type != "socks5" {
		return nil, fmt.Errorf("ipp_clash %s: type 须为 http_proxy|socks5, got %q", cfg.Name, o.Type)
	}
	if o.URL == "" {
		return nil, fmt.Errorf("ipp_clash %s: url required", cfg.Name)
	}
	d := ProxyDialer{Type: o.Type, URL: o.URL}
	marks := newMarkStore()
	return &clashProvider{dialer: d, marks: marks}, nil
}

type clashProvider struct {
	dialer ProxyDialer
	marks  *markStore
}

func (p *clashProvider) Capabilities() Capabilities {
	return Capabilities{CanRotateIP: false, SessionAffinity: false}
}

func (p *clashProvider) Acquire(ctx context.Context, hint Hint) (Lease, error) {
	return &clashLease{p: p}, nil
}

func (p *clashProvider) Stats() Stats {
	s := Stats{EgressIPs: []string{}}
	if mark, ok := p.marks.get(p.dialer.URL); ok {
		_ = mark
	}
	return s
}

func (p *clashProvider) Close() error { return nil }

// clashLease 单出口租约:Dial 经代达拨号(裸 conn,对端即上游)
type clashLease struct {
	p        *clashProvider
	dialed   bool
	released bool
}

func (l *clashLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if l.released {
		return nil, ErrLeaseReleased
	}
	conn, err := l.p.dialer.Dial(ctx, network, addr)
	if err == nil {
		l.dialed = true
	}
	return conn, err
}

func (l *clashLease) EgressIP() string { return "" } // 单出口不可见(设计定案)

// Report Bad 记健康标记(出口不变;Dial 从未成功则 no-op——契约 §2);Ok 清标记
func (l *clashLease) Report(result ReportResult, reason string) {
	if result == ReportBad && !l.dialed {
		return // 无可用连接语义可标记
	}
	l.p.marks.set(l.p.dialer.URL, leaseMark{Reason: reason, At: timeNow(), OK: result == ReportOk})
}

func (l *clashLease) Release() { l.released = true }

func (l *clashLease) Capabilities() Capabilities { return l.p.Capabilities() }

//go:build withwarp

package ipprovider

import (
	"context"
	"fmt"
	"net"
	"sync"

	warppool "github.com/mzzsfy/warp-pool"
)

// warpProvider warp-pool 胶水(设计 §4.1):进程内 WARP 实例池,
// Dial 失败自动顺延候选(库内建),Report(Bad) 触发实例 Draining 重播换出口。
// 构建约束:withwarp 标签——warp-pool 为可公开的独立库,默认构建不引入
// (与 ipp_mihomo 的 withproxy 同构);未启用时 ipp_warp 未注册,配置校验拒绝。
func init() {
	Register("ipp_warp", newWarpProvider)
}

// warpOptions ipp_warp 私有配置(Options map 解析形态)
type warpOptions struct {
	Min           int    `json:"min"`
	Max           int    `json:"max"`
	ListenBase    string `json:"listen_base"`
	DialTransport string `json:"dial_transport"` // socks5(默认)| http
}

func newWarpProvider(cfg ProviderCfg) (Provider, error) {
	var o warpOptions
	if err := decodeOptions(cfg.Options, &o); err != nil {
		return nil, fmt.Errorf("ipp_warp %s: %w", cfg.Name, err)
	}
	if o.Min <= 0 || o.Max < o.Min {
		return nil, fmt.Errorf("ipp_warp %s: 非法 min/max(%d/%d)", cfg.Name, o.Min, o.Max)
	}
	opts := warppool.Options{
		Min:           o.Min,
		Max:           o.Max,
		ListenBase:    o.ListenBase,
		StateDir:      cfg.StateDir,
		DialTransport: warppool.DialTransport(o.DialTransport),
	}
	pool, err := warppool.New(opts)
	if err != nil {
		return nil, fmt.Errorf("ipp_warp %s: %w", cfg.Name, err)
	}
	return &warpProvider{pool: pool}, nil
}

type warpProvider struct {
	pool *warppool.Pool
}

func (p *warpProvider) Capabilities() Capabilities {
	return Capabilities{CanRotateIP: true, SessionAffinity: true}
}

// Acquire 轻对象:不预占不拨号,拨号在 Lease.Dial 内按亲和键落实例
func (p *warpProvider) Acquire(ctx context.Context, hint Hint) (Lease, error) {
	if p.pool == nil {
		return nil, ErrNoExits
	}
	return &warpLease{p: p, sessionKey: hint.SessionKey, egress: pickKnownEgress(p.pool)}, nil
}

// pickKnownEgress 池快照内出口已知的 Normal 实例中任取;全未知返回空串
func pickKnownEgress(pool *warppool.Pool) string {
	for _, inst := range pool.Instances() {
		if inst.Status == warppool.StatusNormal && inst.Egress.V4.IsValid() {
			return inst.Egress.V4.String()
		}
		if inst.Status == warppool.StatusNormal && inst.Egress.V6.IsValid() {
			return inst.Egress.V6.String()
		}
	}
	return ""
}

func (p *warpProvider) Stats() Stats {
	s := Stats{EgressIPs: []string{}}
	poolStats := p.pool.Stats()
	s.Total = poolStats.Total
	s.Normal = poolStats.Normal
	s.Probing = poolStats.Probing
	s.Draining = poolStats.Draining
	s.Disabled = poolStats.Disabled
	for _, inst := range p.pool.Instances() {
		switch {
		case inst.Egress.V4.IsValid():
			s.EgressIPs = append(s.EgressIPs, inst.Egress.V4.String())
		case inst.Egress.V6.IsValid():
			s.EgressIPs = append(s.EgressIPs, inst.Egress.V6.String())
		}
	}
	return s
}

func (p *warpProvider) Close() error {
	if p.pool == nil {
		return nil
	}
	return p.pool.Close()
}

// warpLease 单请求作用域;Dial 后缓存实际选中实例(Report 落点)
type warpLease struct {
	p          *warpProvider
	sessionKey string
	egress     string
	instanceID warppool.ID
	dialed     bool
	reported   bool
	released   bool
	mu         sync.Mutex
}

func (l *warpLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil, ErrLeaseReleased
	}
	l.mu.Unlock()
	if l.p.pool == nil {
		return nil, ErrNoExits
	}
	conn, err := l.p.pool.DialContextWithKey(ctx, l.sessionKey, network, addr)
	if err != nil {
		return nil, err // 顺延耗尽返回最后错误(库内建)
	}
	ic, ok := conn.(warppool.InstanceConn)
	if ok {
		l.mu.Lock()
		l.instanceID = ic.Instance().ID
		l.dialed = true
		l.mu.Unlock()
		if e := ic.Instance().Egress; e.V4.IsValid() {
			l.mu.Lock()
			l.egress = e.V4.String()
			l.mu.Unlock()
		} else if e.V6.IsValid() {
			l.mu.Lock()
			l.egress = e.V6.String()
			l.mu.Unlock()
		}
	}
	return conn, nil
}

func (l *warpLease) EgressIP() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.egress
}

// Report 幂等(首次生效):Bad 触发最近成功实例 Draining(重播换出口身份);
// connect_fail no-op(顺延内建);Ok no-op(池自管);Dial 从未成功 no-op。
// SetStatus 错误仅记日志丢弃,不影响透传。
func (l *warpLease) Report(result ReportResult, reason string) {
	l.mu.Lock()
	if l.reported {
		l.mu.Unlock()
		return
	}
	l.reported = true
	id, dialed := l.instanceID, l.dialed
	l.mu.Unlock()
	if result != ReportBad || reason == ReasonConnectFail {
		return
	}
	if !dialed {
		return
	}
	if err := l.p.pool.SetStatus(id, warppool.StatusDraining); err != nil {
		logf("ipp_warp: SetStatus(%s, Draining): %v", id, err)
	}
}

func (l *warpLease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = true
}

func (l *warpLease) Capabilities() Capabilities { return l.p.Capabilities() }

package ipprovider

// aapProvider 插件化代理节点客户端(aap 协议):SOCKS5 式定长二进制
// 握手 + TUNNEL,连接即请求,rep=0 后为裸字节双向流。协议规范见
// docs/ai-api-proxy/feat/aap-protocol.md(权威源)。
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	Register("aap", newAAPProvider)
}

// 协议魔数与 rep 码(与 aap-protocol.md 对齐)
const (
	aapMagicHandshake = 0xA1
	aapMagicTunnel    = 0xA2
	aapMagicEvict     = 0xA3

	aapStatusOK   = 0 // 握手 status:0=ok,1=auth 失败
	aapRepOK      = 0 // rep=0 目标已连通
	aapRepNoExits = 1 // 出口不可用/目标连通失败(请求零字节写出,可 exclude 重试)
	aapRepBusy    = 2 // 节点并发上限
	aapRepNoCap   = 3 // 请求含节点不支持的能力

	aapEvictScopeLease  = 1
	aapEvictScopeEgress = 2

	aapLeaseIDLen  = 16 // lease_id 128bit 原始字节
	aapSessionKLen = 16 // session_key = sha256 前 16 字节
)

// AapRepRetry rep=1(出口不可用/目标连通失败)——唯一可 exclude 重试的 rep 码
const AapRepRetry = aapRepNoExits

// AapRepBusy rep=2(节点并发上限);AapRepNoCap rep=3(请求含节点不支持的能力)
const (
	AapRepBusy  = aapRepBusy
	AapRepNoCap = aapRepNoCap
)

// IsZeroLeaseID lease_id 是否全零(节点无绑定语义)
func IsZeroLeaseID(id string) bool {
	for i := 0; i < len(id); i++ {
		if id[i] != 0 {
			return false
		}
	}
	return true
}

// aapHandshakeTimeout 握手/TUNNEL 应答时限(不含数据阶段)
const aapHandshakeTimeout = 5 * time.Second

// aapAuthFailLimit 握手连续失败上限(重连+重握手计 1 次);达到后标记不可用
const aapAuthFailLimit = 3

// aapCooldown 不可用冷却期;冷却后自动重探,握手成功即恢复
const aapCooldown = 60 * time.Second

// aapDefaultTTL 默认亲和 TTL(Options affinity_ttl 可覆盖)
const aapDefaultTTL = 24 * 60 * 60

// aapOptions aap 私有配置
type aapOptions struct {
	// AffinityTTL 亲和 TTL(秒);随 TUNNEL 下发
	AffinityTTL int `json:"affinity_ttl"`
}

// SessionKey 派生 aap session_key(sha256 前 16 字节;诊断渲染 hex)
func SessionKey(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return string(h[:aapSessionKLen])
}

// SessionKeyHex session_key 诊断渲染
func SessionHex(key string) string { return hex.EncodeToString([]byte(key)) }

type aapProvider struct {
	name  string
	url   string // aap://host:port?token=xxx
	token string
	host  string // host:port
	ttl   uint32
	// authFails 握手连续失败计数;unavailableUntil 非零 = 不可用至该时刻
	authFails         atomic.Int32
	unavailableUntil  atomic.Int64 // unix nano
	unavailableReason atomic.Pointer[string]
}

func newAAPProvider(cfg ProviderCfg) (Provider, error) {
	if !strings.HasPrefix(cfg.URL, "aap://") {
		return nil, fmt.Errorf("aap %s: url 须为 aap://host:port?token=xxx", cfg.Name)
	}
	var o aapOptions
	if err := decodeOptions(cfg.Options, &o); err != nil {
		return nil, fmt.Errorf("aap %s: %w", cfg.Name, err)
	}
	host, token, err := parseAAPURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("aap %s: %w", cfg.Name, err)
	}
	if o.AffinityTTL < 0 {
		return nil, fmt.Errorf("aap %s: affinity_ttl 须 ≥0", cfg.Name)
	}
	ttl := uint32(aapDefaultTTL)
	if o.AffinityTTL > 0 {
		ttl = uint32(o.AffinityTTL)
	}
	return &aapProvider{name: cfg.Name, url: cfg.URL, token: token, host: host, ttl: ttl}, nil
}

// parseAAPURL 拆出 host:port 与 token(token ≤255 字节,握手 u8 长度上限)
func parseAAPURL(raw string) (host, token string, err error) {
	u := raw
	u = u[len("aap://"):]
	q := strings.IndexByte(u, '?')
	if q >= 0 {
		for _, kv := range strings.Split(u[q+1:], "&") {
			if v, ok := strings.CutPrefix(kv, "token="); ok {
				token = v
			}
		}
		u = u[:q]
	}
	if u == "" || token == "" {
		return "", "", fmt.Errorf("host:port 与 token 均必填")
	}
	if len(token) > 255 {
		return "", "", fmt.Errorf("token 长度超上限 255")
	}
	return u, token, nil
}

// SanitizeURL 日志渲染用 url:aap 形态脱敏 token,其余原样
func SanitizeURL(raw string) string {
	if !strings.HasPrefix(raw, "aap://") {
		return raw
	}
	host, _, err := parseAAPURL(raw)
	if err != nil {
		return "aap://<invalid>"
	}
	return "aap://" + host + "?token=***"
}

func (p *aapProvider) Capabilities() Capabilities {
	return Capabilities{CanRotateIP: true, SessionAffinity: true}
}

// Acquire 轻对象:连接语义全部在 Lease.Dial(每请求一条连接)
func (p *aapProvider) Acquire(ctx context.Context, hint Hint) (Lease, error) {
	return &aapLease{p: p, sessionKey: hint.SessionKey}, nil
}

func (p *aapProvider) Stats() Stats { return Stats{EgressIPs: []string{}} }

func (p *aapProvider) Close() error { return nil }

// Evict 管理面失效:A3 命令,rep=0 即成功(幂等含目标不存在)
func (p *aapProvider) Evict(ctx context.Context, scope uint8, value []byte) error {
	conn, done, err := p.dialNode(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := aapWriteFrame(conn, aapMagicEvict, append([]byte{scope}, value...)); err != nil {
		return err
	}
	var resp [2]byte // magic + rep
	if _, err := readFull(conn, resp[:]); err != nil {
		return err
	}
	if resp[0] != aapMagicEvict {
		return fmt.Errorf("evict resp: 非法应答")
	}
	if resp[1] != aapRepOK {
		return &RepError{Rep: resp[1]}
	}
	return nil
}

// available 不可用状态检查;冷却期内拒绝,冷却到期重置失败计数(恢复"连续 3 次"语义)
func (p *aapProvider) available() error {
	if until := p.unavailableUntil.Load(); until != 0 {
		if timeNow().UnixNano() < until {
			reason := p.unavailableReason.Load()
			return fmt.Errorf("aap %s: 节点不可用(冷却中): %s", p.name, *reason)
		}
		p.unavailableUntil.Store(0)
		p.authFails.Store(0)
	}
	return nil
}

// markAuthFail 握手失败计数;达上限标记不可用并设冷却
func (p *aapProvider) markAuthFail() {
	n := p.authFails.Add(1)
	if n < aapAuthFailLimit {
		return
	}
	reason := fmt.Sprintf("连续 %d 次握手失败", n)
	p.unavailableReason.Store(&reason)
	p.unavailableUntil.Store(timeNow().Add(aapCooldown).UnixNano())
	logf("aap %s: %s,冷却 %s", p.name, reason, aapCooldown)
}

// dialNode 建连 + 握手;成功清失败计数,auth 失败累计
func (p *aapProvider) dialNode(ctx context.Context) (net.Conn, func(), error) {
	if err := p.available(); err != nil {
		return nil, nil, err
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.host)
	if err != nil {
		return nil, nil, fmt.Errorf("connect node: %w", err)
	}
	done := func() { _ = conn.Close() }
	deadline := timeNow().Add(aapHandshakeTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
	if err := aapWriteFrame(conn, aapMagicHandshake, append([]byte{byte(len(p.token))}, p.token...)); err != nil {
		done()
		return nil, nil, fmt.Errorf("write hello: %w", err)
	}
	magic, err := aapReadFrame(conn)
	if err != nil {
		done()
		return nil, nil, fmt.Errorf("read hello resp: %w", err)
	}
	if magic != aapMagicHandshake {
		done()
		return nil, nil, fmt.Errorf("hello resp: 非法应答")
	}
	var status [1]byte
	if _, err := readFull(conn, status[:]); err != nil {
		done()
		return nil, nil, fmt.Errorf("read hello status: %w", err)
	}
	if status[0] != aapStatusOK {
		done()
		p.markAuthFail()
		return nil, nil, fmt.Errorf("auth 失败(status=%d)", status[0])
	}
	p.authFails.Store(0)
	p.unavailableUntil.Store(0)
	return conn, done, nil
}

// aapWriteFrame 定长字段直写:魔数 + payload(协议无帧封装,握手/命令一次写全)
func aapWriteFrame(conn net.Conn, magic byte, payload []byte) error {
	buf := make([]byte, 0, 1+len(payload))
	buf = append(buf, magic)
	buf = append(buf, payload...)
	_, err := conn.Write(buf)
	return err
}

// aapReadFrame 读应答首字节(魔数);后续结构化字段由调用方 readFull
func aapReadFrame(conn net.Conn) (byte, error) {
	var head [1]byte
	if _, err := readFull(conn, head[:]); err != nil {
		return 0, err
	}
	return head[0], nil
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// aapLease 单请求作用域;每次 Dial = 新连接(握手+TUNNEL),rep=0 返回已连通字节流
type aapLease struct {
	p          *aapProvider
	sessionKey string
	excludes   []string
	egress     string
	leaseID    string
	released   bool
	mu         sync.Mutex
}

func (l *aapLease) WithExclude(ids ...string) Lease {
	cp := &aapLease{p: l.p, sessionKey: l.sessionKey}
	cp.excludes = append(append([]string(nil), l.excludes...), ids...)
	return cp
}

// Dial 连接节点:握手 + TUNNEL;rep=0 返回裸字节流(对端即目标);
// rep≠0 返回 *RepError(请求零字节写出,可安全重试)
func (l *aapLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	l.mu.Lock()
	released := l.released
	l.mu.Unlock()
	if released {
		return nil, ErrLeaseReleased
	}
	if network != "tcp" {
		return nil, fmt.Errorf("aap: unsupported network %s", network)
	}
	if len(l.sessionKey) != aapSessionKLen {
		return nil, fmt.Errorf("aap: session_key 须为 %d 字节, got %d", aapSessionKLen, len(l.sessionKey))
	}
	conn, done, err := l.p.dialNode(ctx)
	if err != nil {
		return nil, err
	}
	// TUNNEL 请求:ttl(u32) + skey(16B) + exclude + atyp/addr/port
	req := make([]byte, 0, 4+aapSessionKLen+1+aapLeaseIDLen*len(l.excludes)+1+len(addr)+2)
	req = appendBE32(req, l.p.ttl)
	req = append(req, l.sessionKey...)
	req = append(req, byte(len(l.excludes)))
	for _, id := range l.excludes {
		req = append(req, id...)
	}
	req, err = appendAddr(req, addr)
	if err != nil {
		done()
		return nil, err
	}
	if err := aapWriteFrame(conn, aapMagicTunnel, req); err != nil {
		done()
		return nil, fmt.Errorf("write tunnel: %w", err)
	}
	// 应答:magic(1) + rep(1) + lease_id(16) + expires(8) + atyp(1)
	var head [2 + aapLeaseIDLen + 8 + 1]byte
	if _, err := readFull(conn, head[:]); err != nil {
		done()
		return nil, fmt.Errorf("read tunnel resp: %w", err)
	}
	if head[0] != aapMagicTunnel {
		done()
		return nil, fmt.Errorf("tunnel resp: 非法应答")
	}
	rep := head[1]
	leaseID := string(head[2 : 2+aapLeaseIDLen])
	atyp := head[2+aapLeaseIDLen+8]
	bndLen := map[byte]int{1: 4, 4: 16}[atyp]
	if atyp == 3 { // 域名:len 前缀
		var lb [1]byte
		if _, err := readFull(conn, lb[:]); err != nil {
			done()
			return nil, fmt.Errorf("read tunnel bnd: %w", err)
		}
		bndLen = int(lb[0])
	}
	bnd := make([]byte, bndLen+2)
	if _, err := readFull(conn, bnd); err != nil {
		done()
		return nil, fmt.Errorf("read tunnel bnd: %w", err)
	}
	if rep != aapRepOK {
		done()
		return nil, &RepError{Rep: rep, LeaseID: leaseID}
	}
	_ = conn.SetDeadline(time.Time{}) // 数据阶段解除握手时限
	l.mu.Lock()
	l.leaseID, l.egress = leaseID, aapAddrString(atyp, bnd[:bndLen])
	l.mu.Unlock()
	return &aapConn{Conn: conn, done: done}, nil
}

func (l *aapLease) EgressIP() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.egress
}

func (l *aapLease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = true
}

func (l *aapLease) Capabilities() Capabilities { return l.p.Capabilities() }

// aapConn 数据面连接;关闭即归还(无独立 RELEASE,租约随 TTL 回收)
type aapConn struct {
	net.Conn
	done func()
}

func (c *aapConn) Close() error {
	c.done()
	return c.Conn.Close()
}

// appendBE32 大端 u32
func appendBE32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// appendAddr SOCKS5 地址编码(atyp + addr + port)
func appendAddr(b []byte, addr string) ([]byte, error) {
	host, port := splitHostPort(addr)
	if len(host) > 255 {
		return nil, fmt.Errorf("aap: addr host 超长(>255): %d", len(host))
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			b = append(b, 1)
			b = append(b, v4...)
		} else {
			b = append(b, 4)
			b = append(b, ip.To16()...)
		}
	} else {
		b = append(b, 3, byte(len(host)))
		b = append(b, host...)
	}
	p := 0
	for _, c := range []byte(port) {
		p = p*10 + int(c-'0')
	}
	return append(b, byte(p>>8), byte(p)), nil
}

// aapAddrString 应答 bnd 渲染(IP 文本;域名形态透传)
func aapAddrString(atyp byte, bnd []byte) string {
	switch atyp {
	case 1:
		return net.IP(bnd).String()
	case 4:
		return net.IP(bnd).String()
	default:
		return string(bnd)
	}
}

func splitHostPort(addr string) (host, port string) {
	i := strings.LastIndexByte(addr, ':')
	if i < 0 {
		return addr, "80"
	}
	host, port = addr[:i], addr[i+1:]
	host = strings.Trim(host, "[]")
	return host, port
}

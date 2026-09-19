package ipprovider

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/proxy"
)

// dialHandshakeTimeout CONNECT/socks5 握手时限(不含数据阶段;拨号超时由调用方 ctx 主导)
const dialHandshakeTimeout = 5 * time.Second

// timeNow / timeZero 间接引用(测试可注入;语义自明)
var timeNow = time.Now

func timeZero() time.Time { return time.Time{} }

// ProxyDialer 标准代理形态的拨号器:经 http_proxy(CONNECT 隧道)或 socks5 代达目标,
// 返回隧道后的裸连接(对端即目标地址)。clash 适配与 remote 适配共用。
//
// 设计要点(R1 定案):Lease.Dial 的实现持有 ProxyDialer,Transport 侧不设 Proxy 字段,
// 隧道语义全部在 Dial 内部完成。
type ProxyDialer struct {
	// Type 代理形态:http_proxy | socks5
	Type string
	// URL 代理地址(scheme://[user:pass@]host:port)
	URL string
}

// Dial 实现代达拨号
func (d ProxyDialer) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" {
		return nil, fmt.Errorf("proxy dialer: unsupported network %s", network)
	}
	base, err := url.Parse(d.URL)
	if err != nil {
		return nil, fmt.Errorf("proxy dialer: parse url: %w", err)
	}
	switch d.Type {
	case "http_proxy":
		return dialHTTPConnect(ctx, base, addr)
	case "socks5":
		return dialSocks5(ctx, base, addr)
	default:
		return nil, fmt.Errorf("proxy dialer: unknown type %q", d.Type)
	}
}

// dialHTTPConnect CONNECT 隧道:TCP 连代理 → 发 CONNECT → 2xx 即隧道建立
func dialHTTPConnect(ctx context.Context, base *url.URL, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", base.Host)
	if err != nil {
		return nil, fmt.Errorf("connect proxy: %w", err)
	}
	// 带 5s 级超时的 CONNECT 握手(经由 ctx;超时后 conn 一并回收)
	deadline, ok := ctx.Deadline()
	if ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(timeNow().Add(dialHandshakeTimeout))
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if base.User != nil {
		pw, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pw)
	}
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write connect: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read connect resp: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("connect proxy: status %d", resp.StatusCode)
	}
	_ = conn.SetDeadline(timeZero())
	// 代理可能多发字节留在 bufio:仅当无缓冲残余时裸 conn 可直用;
	// 有残余说明对端行为异常,拒绝该连接(隧道语义已破坏)
	if br.Buffered() > 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("connect proxy: %d buffered bytes after handshake", br.Buffered())
	}
	return conn, nil
}

// dialSocks5 socks5 握手代达
func dialSocks5(ctx context.Context, base *url.URL, addr string) (net.Conn, error) {
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
	conn, err := cd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

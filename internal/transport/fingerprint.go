package transport

import (
	"context"
	"net"
	"net/http"

	utls "github.com/refraction-networking/utls"
)

// clientHelloID 浏览器指纹 ClientHello(随 utls 版本跟进 Chrome 当前形态);TLS 层 JA3/JA4 对抗的单点
var clientHelloID = utls.HelloChrome_Auto

// fingerprintDialer 复用底层拨号,UTLS 握手包一层
type fingerprintDialer struct {
	inner func(ctx context.Context, network, addr string) (net.Conn, error)
}

// DialContext 直连形态(明文 http)
func (d fingerprintDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.inner(ctx, network, addr)
}

// DialTLSContext 在既有连接(直连 TCP/代理 CONNECT 隧道)上做浏览器指纹握手
// NextProtos 锁 http/1.1:标准库 http2 封装要求 *tls.Conn,utls 连接装不上;需要 h2 时另做桥
func (d fingerprintDialer) DialTLSContext(ctx context.Context, network, addr string) (net.Conn, error) {
	raw, err := d.inner(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	cfg := &utls.Config{ServerName: serverName(addr), NextProtos: []string{"http/1.1"}}
	uconn := utls.UClient(raw, cfg, clientHelloID)
	if err := uconn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return uconn, nil
}

// serverName host:port → SNI 主机名
func serverName(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// plainDial 标准拨号(缺省路由)
func plainDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// fingerprintHTTPTransport 浏览器指纹 http.Transport(direct/代理共用;TLS 站点走 utls,明文站点直拨)
func fingerprintHTTPTransport(dial func(ctx context.Context, network, addr string) (net.Conn, error)) *http.Transport {
	d := fingerprintDialer{inner: dial}
	return &http.Transport{
		DialContext:    d.DialContext,
		DialTLSContext: d.DialTLSContext,
	}
}

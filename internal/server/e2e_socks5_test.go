package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── P11:socks5 真实服务端到端(测试内最小 RFC 1928 服务,真 TCP) ───

// socks5 应答码(RFC 1928 §6)
const (
	socksReplySuccess     = 0x00
	socksReplyHostUnreach = 0x04
	socksReplyBadATYP     = 0x08
)

// socks5Server 最小无鉴权 socks5:方法协商 + CONNECT 转发(真 TCP 双向拷贝;白名单单目标)
type socks5Server struct {
	ln       net.Listener
	hits     atomic.Int64
	upstream string // 允许的转发目标(host:port,测试收紧)
}

func newSocks5Server(t *testing.T, allowTarget string) *socks5Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &socks5Server{ln: ln, upstream: allowTarget}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *socks5Server) addr() string      { return s.ln.Addr().String() }
func (s *socks5Server) relayCount() int64 { return s.hits.Load() }

func (s *socks5Server) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *socks5Server) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	// ① 方法协商
	head := make([]byte, 2)
	if _, err := io.ReadFull(br, head); err != nil || head[0] != 5 {
		return
	}
	if _, err := io.CopyN(io.Discard, br, int64(head[1])); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil { // 无鉴权
		return
	}
	// ② 请求头 + 目标地址
	req := make([]byte, 4)
	if _, err := io.ReadFull(br, req); err != nil || req[1] != 1 { // 仅 CONNECT
		_, _ = conn.Write([]byte{5, socksReplyBadATYP, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	dest, ok := readSocksTarget(br, req[3])
	if !ok {
		_, _ = conn.Write([]byte{5, socksReplyBadATYP, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	if dest != s.upstream {
		_, _ = conn.Write([]byte{5, socksReplyHostUnreach, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	up, err := net.DialTimeout("tcp", dest, 5*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{5, socksReplyHostUnreach, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer func() { _ = up.Close() }()
	// ③ 成功应答(BND 占零)+ 双向拷贝
	if _, err := conn.Write([]byte{5, socksReplySuccess, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	s.hits.Add(1)
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, up); done <- struct{}{} }()
	<-done
}

// readSocksTarget 解析 ATYP 地址 + 端口
func readSocksTarget(br *bufio.Reader, atyp byte) (string, bool) {
	var host string
	switch atyp {
	case 1: // IPv4
		raw := make([]byte, 4)
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", false
		}
		host = net.IP(raw).String()
	case 3: // 域名
		n := make([]byte, 1)
		if _, err := io.ReadFull(br, n); err != nil {
			return "", false
		}
		raw := make([]byte, n[0])
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", false
		}
		host = string(raw)
	case 4: // IPv6
		raw := make([]byte, 16)
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", false
		}
		host = net.IP(raw).String()
	default:
		return "", false
	}
	portRaw := make([]byte, 2)
	if _, err := io.ReadFull(br, portRaw); err != nil {
		return "", false
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", binary.BigEndian.Uint16(portRaw))), true
}

// trimScheme 去掉 http:// 前缀(socks5 白名单按 host:port 匹配)
func trimScheme(u string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(u) > len(p) && u[:len(p)] == p {
			return u[len(p):]
		}
	}
	return u
}

// TestE2E_Socks5_RealService 真实 socks5 服务 × chat/message × 流/非流
func TestE2E_Socks5_RealService(t *testing.T) {
	// Given 真 TCP socks5(白名单=假上游)+ socks5 传输 + 真实 JS 协议插件
	// When 4 场景真实请求 Then 全部 200 且流量真经 CONNECT 隧道
	upSrv, spy := newMockUpstream(t)
	socks := newSocks5Server(t, trimScheme(upSrv.URL))
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: "admin",
		Transports: []TransportCfg{{Name: "sx", Type: "socks5", URL: "socks5://" + socks.addr()}},
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	ctx := context.Background()
	if err := app.AdminDeps.Packages.Install(ctx, aapZip(t, jsProtoManifest, map[string]string{"p.js": jsProtoSrc})); err != nil {
		t.Fatal(err)
	}
	u := &upstream.Upstream{
		Name: "g5-socks5", Enabled: true,
		Base:   upstream.PackageRef{Package: "js-openai"},
		Models: []string{"m-socks5"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: upSrv.URL, Transport: "sx", Enabled: true,
			Secrets: map[string]string{"api_key": upstreamAPIKey}}},
	}
	if err := app.Registry.Save(ctx, u); err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(app.Mux)
	t.Cleanup(gateway.Close)

	scenarios := []scenario{
		{group: "m-socks5", entry: "chat", stream: false, proxy: true},
		{group: "m-socks5", entry: "chat", stream: true, proxy: true},
		{group: "m-socks5", entry: "message", stream: false, proxy: true},
		{group: "m-socks5", entry: "message", stream: true, proxy: true},
	}
	relayBefore := socks.relayCount()
	runScenariosTable(t, gateway.URL, func() int64 { return socks.relayCount() }, spy, scenarios, false)
	if socks.relayCount() <= relayBefore {
		t.Fatal("socks5 server never relayed")
	}
}

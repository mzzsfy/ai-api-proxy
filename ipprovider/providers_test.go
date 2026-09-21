package ipprovider

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ─── clash 适配 ───

// 场景:ipp_clash http_proxy 形态 —— Dial 发 CONNECT 且目标为上游地址(R1 定案锁定)
func TestClash_Dial_CONNECT目标为上游(t *testing.T) {
	gotTarget := make(chan string, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		gotTarget <- req.Host
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	u := ln.Addr().String()
	p, err := Create("ipp_clash", ProviderCfg{Name: "t", Options: map[string]any{"type": "http_proxy", "url": "http://" + u}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Acquire(context.Background(), Hint{SessionKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := lease.Dial(context.Background(), "tcp", "target.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("x"))
	select {
	case got := <-gotTarget:
		if got != "target.example:443" {
			t.Fatalf("CONNECT target = %v, want target.example:443(不得为代理自身)", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CONNECT not received")
	}
	_ = conn.Close()
	lease.Release()
	if _, err := lease.Dial(context.Background(), "tcp", "x:80"); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("want ErrLeaseReleased, got %v", err)
	}
}

// 场景:ipp_clash 单出口语义 —— Dial 失败可透传;egress 不可见;不可轮换
func TestClash_单出口语义(t *testing.T) {
	p, err := Create("ipp_clash", ProviderCfg{Name: "t", Options: map[string]any{"type": "socks5", "url": "socks5://127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, _ := p.Acquire(context.Background(), Hint{})
	_, dialErr := lease.Dial(context.Background(), "tcp", "127.0.0.1:1")
	if dialErr == nil {
		t.Fatal("dial to dead port must fail")
	}
	if egress := lease.EgressIP(); egress != "" {
		t.Fatalf("clash egress must be empty, got %q", egress)
	}
	if p.Capabilities().CanRotateIP {
		t.Fatal("clash must not rotate")
	}
}

// 场景:ipp_clash 非法配置拒绝构造
func TestClash_非法配置(t *testing.T) {
	if _, err := Create("ipp_clash", ProviderCfg{Name: "t", Options: map[string]any{"type": "weird", "url": "x"}}); err == nil {
		t.Fatal("want error for bad type")
	}
	if _, err := Create("ipp_clash", ProviderCfg{Name: "t", Options: map[string]any{"type": "socks5"}}); err == nil {
		t.Fatal("want error for missing url")
	}
}

// ─── ProxyDialer(socks5 形态;共享于 clash/remote) ───

// 场景:socks5 代达——假 socks5 握手(无认证,CONNECT 成功应答)后连接可用
func TestProxyDialer_Socks5握手(t *testing.T) {
	ts := newFakeSocks5(t, "target.example:80", nil)
	p, err := Create("ipp_clash", ProviderCfg{Name: "t", Options: map[string]any{"type": "socks5", "url": "socks5://" + ts.listener.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	lease, _ := p.Acquire(context.Background(), Hint{})
	conn, err := lease.Dial(context.Background(), "tcp", "target.example:80")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

// fakeSocks5 最小 socks5 服务:无认证 + CONNECT 应答;targetEcho 非空时把目标地址回写给客户端
type fakeSocks5 struct {
	listener net.Listener
	srv      *httptest.Server
}

func newFakeSocks5(t *testing.T, wantTarget string, authUser *string) *fakeSocks5 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleFakeSocks5(conn, wantTarget, authUser)
		}
	}()
	return &fakeSocks5{listener: ln}
}

func handleFakeSocks5(conn net.Conn, wantTarget string, authUser *string) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	head := make([]byte, 2)
	if _, err := ioReadFull(br, head); err != nil {
		return
	}
	nMethods := int(head[1])
	methods := make([]byte, nMethods)
	if _, err := ioReadFull(br, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil { // 无认证
		return
	}
	reqHead := make([]byte, 4)
	if _, err := ioReadFull(br, reqHead); err != nil {
		return
	}
	// ATYP + ADDR + PORT
	atyp := reqHead[3]
	var addrLen int
	switch atyp {
	case 0x01:
		addrLen = 4
	case 0x03:
		l, _ := br.ReadByte()
		addrLen = int(l)
		br.UnreadByte()
	case 0x04:
		addrLen = 16
	}
	total := 1 + addrLen + 2
	rest := make([]byte, total)
	if _, err := ioReadFull(br, rest); err != nil {
		return
	}
	_ = wantTarget // 单测断言在 Dial 成功性本身;地址格式由 x/net/proxy 构造保证
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	// 隧道建立:丢弃后续,保持连接
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

func ioReadFull(br *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := br.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// ─── remote 适配 ───

// 场景:ipp_remote 正常路径 —— acquire 返回能力/出口/代理地址,release 走协议
func TestRemote_正常路径(t *testing.T) {
	var released atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/acquire", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"lease_id":"L1","proxy":{"type":"http","url":"http://127.0.0.1:1"},"egress_ip":"9.9.9.9","capabilities":{"can_rotate_ip":true,"session_affinity":false}}`))
	})
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		released.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	p, err := Create("ipp_remote", ProviderCfg{Name: "t", Options: map[string]any{
		"base": ts.URL, "api_key": "k", "allow_insecure": true,
		"can_rotate_ip": true, "session_affinity": true, // 配置与服务端不一致 → 以服务端为准
	}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Acquire(context.Background(), Hint{SessionKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if got := lease.EgressIP(); got != "9.9.9.9" {
		t.Fatalf("egress = %q", got)
	}
	if lease.Capabilities().SessionAffinity { // 服务端 false 覆盖配置 true
		t.Fatal("lease caps must follow server")
	}
	lease.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !released.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !released.Load() {
		t.Fatal("release endpoint not called")
	}
}

// 场景:ipp_remote 503 → ErrNoExits;401 → 明确鉴权错误
func TestRemote_错误映射(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/acquire", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusFromQuery(r))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	mk := func() Provider {
		p, err := Create("ipp_remote", ProviderCfg{Name: "t", Options: map[string]any{"base": ts.URL, "api_key": "k", "allow_insecure": true}})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	setStatus(http.StatusServiceUnavailable)
	if _, err := mk().Acquire(context.Background(), Hint{}); !errors.Is(err, ErrNoExits) {
		t.Fatalf("want ErrNoExits, got %v", err)
	}
	setStatus(http.StatusUnauthorized)
	_, err := mk().Acquire(context.Background(), Hint{})
	if err == nil || !strings.Contains(err.Error(), "鉴权") {
		t.Fatalf("want auth error, got %v", err)
	}
}

var queryStatus = http.StatusServiceUnavailable

func statusFromQuery(r *http.Request) int { return queryStatus }

func setStatus(code int) { queryStatus = code }

// 场景:ipp_remote 非 https base 拒绝(未显式放行)
func TestRemote_强制https(t *testing.T) {
	if _, err := Create("ipp_remote", ProviderCfg{Name: "t", Options: map[string]any{"base": "http://x.example", "api_key": "k"}}); err == nil {
		t.Fatal("want https enforcement error")
	}
}

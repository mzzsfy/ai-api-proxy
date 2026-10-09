package transport

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"

	utls "github.com/refraction-networking/utls"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

func newReq(h map[string]string) pipeline.Request { return pipeline.Request{Headers: h} }

func newPipelineReq(url string) pipeline.Request {
	return pipeline.Request{URL: url, Method: http.MethodGet}
}

// TestBrowserDefaults_补齐与显式优先 显式头不覆盖;缺省补齐浏览器组;off 关闭
func TestBrowserDefaults_补齐与显式优先(t *testing.T) {
	h := withBrowserDefaults(newReq(map[string]string{"User-Agent": "cli/1.0"}))
	if h["User-Agent"] != "cli/1.0" {
		t.Fatalf("explicit UA overridden: %s", h["User-Agent"])
	}
	if h["Accept-Encoding"] != defaultAccEnc || h["Accept-Language"] != defaultAccLang {
		t.Fatalf("defaults missing: %+v", h)
	}
	off := withBrowserDefaults(newReq(map[string]string{"X-Aap-Browser-Defaults": "off"}))
	if _, ok := off["User-Agent"]; ok {
		t.Fatal("off must skip all defaults")
	}
}

// trustCert 测试服务器证书加入 utls 信任池
func trustCert(t *testing.T, srv *httptest.Server) *utls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return &utls.Config{RootCAs: pool, ServerName: "127.0.0.1", NextProtos: []string{"http/1.1"}}
}

// TestFingerprintTLS_直连握手 utls 指纹拨号对 TLS 站点完整请求(信任池挂测试自签)
func TestFingerprintTLS_直连握手(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	ucfg := trustCert(t, srv)
	raw, err := plainDial(context.Background(), "tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	uconn := utls.UClient(raw, ucfg, clientHelloID)
	if err := uconn.HandshakeContext(context.Background()); err != nil {
		t.Fatalf("utls handshake: %v", err)
	}
	defer uconn.Close()
	if uconn.ConnectionState().NegotiatedProtocol != "http/1.1" {
		t.Fatalf("alpn %q", uconn.ConnectionState().NegotiatedProtocol)
	}
}

// TestBrowserDefaults_经传输路径注入 httpTransport RoundTrip 实际发出缺省头
func TestBrowserDefaults_经传输路径注入(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	defer srv.Close()
	m, err := NewManager([]TransportDef{{Name: "direct", URL: "direct"}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	tr, _ := m.Get("direct")
	if _, err := tr.RoundTrip(context.Background(), newPipelineReq(srv.URL)); err != nil {
		t.Fatal(err)
	}
	if got.Get("User-Agent") != defaultUA {
		t.Fatalf("ua %q", got.Get("User-Agent"))
	}
	if got.Get("Sec-Ch-Ua") == "" {
		t.Fatal("sec-ch-ua missing")
	}
}

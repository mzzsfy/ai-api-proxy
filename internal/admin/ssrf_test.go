package admin

import (
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// TestForbiddenIP_ReservedRanges 私网/保留段全清单判定(公网地址放行)
func TestForbiddenIP_ReservedRanges(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.1.1",
		"0.0.0.1", "100.64.0.1", "198.18.0.1", "198.19.255.1", "240.0.0.1",
		"255.255.255.255", "224.0.0.1", "192.0.0.1", "::1", "fe80::1", "fc00::1",
	}
	for _, s := range blocked {
		if !forbiddenIP(net.ParseIP(s)) {
			t.Errorf("%s must be forbidden", s)
		}
	}
	allowed := []string{"8.8.8.8", "1.1.1.1", "2606:4700::1111"}
	for _, s := range allowed {
		if forbiddenIP(net.ParseIP(s)) {
			t.Errorf("%s must be allowed", s)
		}
	}
}

// TestInstallURLPrivateBlocked import-url 与 inspect 同源 SSRF 防线:环回目标拒绝且不安装
func TestInstallURLPrivateBlocked(t *testing.T) {
	// 真实拉取(不覆写):127.0.0.1 命中拨号级校验
	pkgs := plugin.NewRegistry(nil)
	deps := &Deps{Packages: pkgs}
	mux := deps.Mux()
	body := `{"url":"http://127.0.0.1:1/x.aap"}`
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/admin/api/packages/import-url", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatalf("loopback import must be rejected: %s", w.Body.String())
	}
}

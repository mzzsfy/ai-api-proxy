package server

import (
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

)

// ─── 真实二进制进程端到端:config.yaml 启动 + 管理 API 装配 + 4 组真实请求 ───

// freePort 取空闲端口(探测后关闭,占用风险可接受)
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// buildGatewayBinary 编译真实二进制
func buildGatewayBinary(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "ai-api-proxy.exe")
	cmd := exec.Command("go", "build", "-o", out, "../../cmd/ai-api-proxy")
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=go1.27.0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
	return out
}

// waitForHealth 轮询真实进程 /healthz
func waitForHealth(t *testing.T, base string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("gateway process not healthy in time")
}

// postAdmin 管理 API 调用
func postAdmin(t *testing.T, client *http.Client, base, path, contentType string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

// getAdmin 管理 API GET 调用
func getAdmin(t *testing.T, client *http.Client, base, path string) (int, []byte) {
	t.Helper()
	resp, err := client.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

// putAdmin 管理 API PUT 调用
func putAdmin(t *testing.T, client *http.Client, base, path string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

func TestE2E_BinaryProcess_FourGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("binary e2e skipped in short mode")
	}
	// Given 真实二进制进程(config.yaml 启动)+ 管理 API 装配的两个真实插件 + 4 上游
	upSrv, spy := newMockUpstream(t)
	proxySrv, proxyHits := newForwardProxy(t)

	bin := buildGatewayBinary(t)
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	dataDir := t.TempDir()
	passHash, err := bcrypt.GenerateFromPassword([]byte("e2e-admin-pass"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dataDir, "config.yaml")
	cfg := fmt.Sprintf(`listen: 127.0.0.1:%d
data_dir: %s
api_keys: [sk-test]
admin_user: admin
admin_pass_bcrypt: %q
log_level: warn
transports:
  - name: px
    type: http_proxy
    url: %s
`, port, filepath.ToSlash(dataDir), string(passHash), proxySrv.URL)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "-config", cfgPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	waitForHealth(t, base)

	// 管理 API 装配:登录(cookiejar 持会话)→ 上传 2 包 → 建 4 上游
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second, Jar: jar}
	status, body := postAdmin(t, client, base, "/admin/api/login", "application/json",
		[]byte(`{"user":"admin","password":"e2e-admin-pass"}`))
	if status != http.StatusOK {
		t.Fatalf("login %d: %s", status, body)
	}
	status, body = postAdmin(t, client, base, "/admin/api/packages", "application/zip",
		aapZip(t, jsProtoManifest, map[string]string{plugin.ProtocolEntry: jsProtoSrc, "filters/rewrite.js": rewriteSrc}))
	if status != http.StatusOK {
		t.Fatalf("install js-openai %d: %s", status, body)
	}
	status, body = postAdmin(t, client, base, "/admin/api/packages", "application/zip",
		aapZip(t, jsProtoAnthropicManifest, map[string]string{plugin.ProtocolEntry: jsProtoSrc}))
	if status != http.StatusOK {
		t.Fatalf("install js-anthropic %d: %s", status, body)
	}
	// v2 装配:包级 keys → 包参数(乐观锁 version)→ 模型行
	for _, pkg := range []string{"js-openai", "js-anthropic"} {
		status, body = putAdmin(t, client, base, "/admin/api/packages/"+pkg+"/keys/api_key",
			[]byte(`{"value":"`+upstreamAPIKey+`"}`))
		if status != http.StatusOK {
			t.Fatalf("put key %s %d: %s", pkg, status, body)
		}
		proto := "openai-completions"
		if pkg == "js-anthropic" {
			proto = "anthropic-messages"
		}
		_, vbody := getAdmin(t, client, base, "/admin/api/packages/"+pkg+"/settings")
		var view struct {
			Version int64 `json:"version"`
		}
		if err := json.Unmarshal(vbody, &view); err != nil {
			t.Fatalf("settings view %s: %v", pkg, err)
		}
		cfgBody, _ := json.Marshal(map[string]any{
			"config":  map[string]any{"base_url": upSrv.URL, "protocol": proto},
			"version": view.Version,
		})
		status, body = putAdmin(t, client, base, "/admin/api/packages/"+pkg+"/settings", cfgBody)
		if status != http.StatusOK {
			t.Fatalf("put settings %s %d: %s", pkg, status, body)
		}
	}
	mkRow := func(name, pkg string, params map[string]any) {
		b, err := json.Marshal(map[string]any{"name": name, "plugin": pkg, "enabled": true, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		st, bd := postAdmin(t, client, base, "/admin/api/models", "application/json", b)
		if st != http.StatusOK {
			t.Fatalf("save %s %d: %s", name, st, bd)
		}
	}
	mkRow("m-direct", "js-openai", nil)
	mkRow("m-proxy", "js-openai", map[string]any{"transport": "px"})
	mkRow("mf-direct", "js-openai", map[string]any{"model": upstreamRewriteM})
	mkRow("mf-proxy", "js-openai", map[string]any{"transport": "px", "model": upstreamRewriteM})
	mkRow("m-anthropic", "js-anthropic", nil)

	// When 5 组 × 入口/流形态 全部真实 HTTP 请求
	// Then 全部场景断言通过
	runScenarios(t, base, func() int64 { return proxyHits.Load() }, spy)
}

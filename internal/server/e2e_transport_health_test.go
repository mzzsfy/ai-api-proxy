package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
)

// mustAdminHash 测试管理员口令散列
func mustAdminHash(t *testing.T) string {
	t.Helper()
	h, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// ─── 传输健康周期探测 E2E:probe loop 落 kv → transports API 读回 ───
//
// Given 配置 transport_probe_interval_sec>0 且存在 http_proxy 传输
// When 探测循环跑过至少一轮(探针指向本地 mock 204 端点)
// Then GET /admin/api/transports 返回体携带最近探测结果(ok/latency_ms/checked_at)

func TestTransportProbeLoop_HealthLandsInTransportsAPI(t *testing.T) {
	// 探针指向本地 mock(替代 gstatic,无外网依赖)
	probeSrv := newProbeHandler()
	t.Cleanup(probeSrv.Close)
	origURL, origTimeout := transportProbeURL, transportProbeTimeout
	transportProbeURL = probeSrv.URL + "/generate_204"
	transportProbeTimeout = 5 * time.Second
	t.Cleanup(func() { transportProbeURL, transportProbeTimeout = origURL, origTimeout })

	adminHash := mustAdminHash(t)
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{
			{Name: "healthy", Type: "direct"},
			{Name: "dead", Type: "http_proxy", URL: "http://127.0.0.1:1"},
		},
		TransportProbeIntervalSec: 1,
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	stop := startTransportProbeLoop(app, time.Second)
	t.Cleanup(stop)

	// 轮询等待两个实例都有探测结果(dead 快败,healthy 应 204)
	deadline := time.Now().Add(10 * time.Second)
	for {
		healthy := transportHealthFromKV(t, app, "healthy")
		dead := transportHealthFromKV(t, app, "dead")
		if healthy != nil && dead != nil && healthy.OK && !dead.OK {
			// API 视图核验:health 字段落 transports 清单
			if hs, ok := transportsAPIHealth(t, app, "healthy"); ok && hs.OK {
				t.Logf("healthy: %+v | dead: %+v", healthy, dead)
				return // 两者形态均符合预期
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe results not settled: healthy=%+v dead=%+v", healthy, dead)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// transportsAPIHealth 经 admin TransportsFunc 读回 health(与 GUI 同源)
func transportsAPIHealth(t *testing.T, app *App, name string) (*transportHealthStatus, bool) {
	t.Helper()
	for _, item := range app.AdminDeps.TransportsFunc() {
		if item["name"] != name {
			continue
		}
		hs, ok := item["health"].(transportHealthStatus)
		if !ok {
			return nil, false
		}
		return &hs, true
	}
	return nil, false
}

func TestTransportProbeLoop_DisabledByDefault(t *testing.T) {
	// Given 未配置 interval When Build Then 不启动探测循环(kv 无记录),手动测试仍可用
	probeSrv := newProbeHandler()
	t.Cleanup(probeSrv.Close)
	origURL := transportProbeURL
	transportProbeURL = probeSrv.URL + "/generate_204"
	t.Cleanup(func() { transportProbeURL = origURL })

	adminHash := mustAdminHash(t)
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: adminHash,
		Transports: []TransportCfg{{Name: "px", Type: "direct"}},
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	time.Sleep(300 * time.Millisecond) // 若误启动,首轮探测应已落 kv
	if v, ok, _ := transportHealthRaw(t, app, "px"); ok {
		t.Fatalf("probe loop should be disabled by default, got %s", v)
	}
	// 手动测试不受影响
	latency, err := app.TransportTest("px", transportProbeURL, transportProbeTimeout)
	if err != nil {
		t.Fatalf("manual test: %v", err)
	}
	if latency < 0 {
		t.Fatalf("negative latency %d", latency)
	}
}

// newProbeHandler 204 端点(与 gstatic generate_204 同语义)
func newProbeHandler() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
}

// transportHealthRaw kv 原始读取
func transportHealthRaw(t *testing.T, app *App, name string) (string, bool, error) {
	t.Helper()
	return app.St.KVGet(t.Context(), "transport_health", name)
}

// transportHealthFromKV 解析后的健康态(nil=尚无记录)
func transportHealthFromKV(t *testing.T, app *App, name string) *transportHealthStatus {
	t.Helper()
	v, ok, err := transportHealthRaw(t, app, name)
	if err != nil || !ok {
		return nil
	}
	var hs transportHealthStatus
	if err := json.Unmarshal([]byte(v), &hs); err != nil {
		t.Fatalf("decode health %s: %v", name, err)
	}
	return &hs
}

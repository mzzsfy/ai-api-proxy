package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// TestManager_Scheme判型_构建 校验 url scheme → 实例类型的判定与错误面
// Given 各 scheme 形态的传输定义 When NewManager 构建 Then 正确 scheme 成功且类型正确,非法 scheme 报错
func TestManager_Scheme判型_构建(t *testing.T) {
	direct, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	if _, ok := direct.Get("direct"); !ok {
		t.Fatal("空定义应存在默认 direct 实例")
	}

	tests := []struct {
		name    string
		url     string
		wantErr string // 空 = 期望成功
	}{
		{"direct 字面值", "direct", ""},
		{"http 代理", "http://127.0.0.1:1", ""},
		{"https 代理", "https://127.0.0.1:1", ""},
		{"socks5 代理", "socks5://127.0.0.1:1", ""},
		{"未知 scheme", "unknown://x", `unknown transport url "unknown://x"`},
		{"ipp+ 缺参数", "ipp+clash://", "ipp url 须为 ipp+<kind>://<参数>"},
		{"ipp+ 未知 kind", "ipp+nosuchkind://x", "未知供给方类型"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewManagerWithCfg([]TransportDef{{Name: "t", URL: tt.url}}, t.TempDir())
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("构建失败: %v", err)
				}
				inst, ok := m.Get("t")
				if !ok {
					t.Fatal("实例应已注册")
				}
				if _, isHTTP := inst.(*httpTransport); !isHTTP {
					t.Fatal("代理类 scheme 应构建为 httpTransport")
				}
				m.Close()
				return
			}
			if err == nil {
				m.Close()
				t.Fatalf("期望错误含 %q,得到成功", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("错误 %q 不含 %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestManager_Aap构建_供给方注册校验 aap:// 构建成功且 Provider 可取,Close 幂等
func TestManager_Aap构建_供给方注册(t *testing.T) {
	m, err := NewManagerWithCfg([]TransportDef{{Name: "aap-out", URL: "aap://node.example:7000?token=x"}, {Name: "d", URL: "direct"}}, t.TempDir())
	if err != nil {
		t.Fatalf("aap 构建失败: %v", err)
	}
	defer m.Close()
	if _, ok := m.Provider("aap-out"); !ok {
		t.Fatal("aap 传输应注册供给方")
	}
	if _, ok := m.Provider("direct"); ok {
		t.Fatal("direct 不应有供给方")
	}
	if _, isSupplier := m.SupplierKind("aap-out"); !isSupplier {
		t.Fatal("aap:// 应判为供给方")
	}
	if url, isSupplier := m.SupplierKind("d"); isSupplier || url != "direct" {
		t.Fatalf("direct 不应判为供给方,得到 url=%q supplier=%v", url, isSupplier)
	}
	if _, ok := m.SupplierKind("nosuch"); ok {
		t.Fatal("未知实例名不应判为供给方")
	}
	m.Close() // 幂等,不得 panic
}

// TestManager_Names_Defs_只读视图 Given 多实例定义 When 查询清单与定义 Then 稳定序返回且 Defs 为副本
func TestManager_Names_Defs_只读视图(t *testing.T) {
	m, err := NewManagerWithCfg([]TransportDef{
		{Name: "b", URL: "direct"},
		{Name: "a", URL: "direct"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	names := m.Names()
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "direct" {
		t.Fatalf("Names 应稳定序 [a b direct],得到 %v", names)
	}
	defs := m.Defs()
	if len(defs) != 2 {
		t.Fatalf("Defs 应含 2 个定义,得到 %d", len(defs))
	}
	defs[0].Name = "mutated"
	if m.Defs()[0].Name == "mutated" {
		t.Fatal("Defs 应返回副本,外部修改不得影响内部状态")
	}
}

// TestManager_Evict_非供给方返回false Given 非供给方传输 When Evict Then 返回 false 无错误(映射 400)
func TestManager_Evict_非供给方返回false(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	handled, err := m.Evict(context.Background(), "direct", EvictScopeLease, nil)
	if err != nil {
		t.Fatalf("非供给方 Evict 不应报错: %v", err)
	}
	if handled {
		t.Fatal("非供给方 Evict 应返回 false")
	}
}

// TestManager_Test_连通探测 Given 直连实例 When 对探测地址发 HEAD Then 成功计延迟/上游 5xx 报错/未知实例报错
func TestManager_Test_连通探测(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("探测应为 HEAD,得到 %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.Test("direct", okSrv.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("正常探测不应报错: %v", err)
	}

	if _, err := m.Test("direct", badSrv.URL, 5*time.Second); err == nil || !strings.Contains(err.Error(), "probe status 500") {
		t.Fatalf("5xx 探测应报 probe status 500,得到 %v", err)
	}

	if _, err := m.Test("nosuch", okSrv.URL, time.Second); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("未知实例应报 not found,得到 %v", err)
	}
}

// TestHTTPTransport_RoundTripRaw_透传 Given httptest 上游 When RoundTripRaw Then 状态与 body 原样交出且缺省浏览器头注入
func TestHTTPTransport_RoundTripRaw_透传(t *testing.T) {
	var seenUA, seenBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seenUA = r.Header.Get("User-Agent")
		seenBody = string(b)
		w.Header().Set("X-Echo", "1")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"ok":false}`))
	}))
	defer srv.Close()

	tr := &httpTransport{name: "t", client: &http.Client{Transport: fingerprintHTTPTransport(plainDial)}}
	resp, err := tr.RoundTripRaw(context.Background(), pipeline.Request{
		URL:    srv.URL,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type":          "application/json",
			"X-Aap-Browser-Defaults": "off", // 关闭浏览器头组,UA 仍必选注入
		},
		Body: []byte(`{"ping":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("状态应原样透传 418,得到 %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Echo") != "1" {
		t.Fatal("响应头应原样交出")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":false}` {
		t.Fatalf("body 应原样交出,得到 %q", string(body))
	}
	if seenBody != `{"ping":1}` {
		t.Fatalf("请求体应原样发出,得到 %q", seenBody)
	}
	if seenUA == "" {
		t.Fatal("UA 缺省注入不可关(爬虫特征单点对抗)")
	}
}

// TestHTTPTransport_RoundTripRaw_连接失败报错 Given 不可达地址 When RoundTripRaw Then 返回错误且响应为 nil
func TestHTTPTransport_RoundTripRaw_连接失败报错(t *testing.T) {
	tr := &httpTransport{name: "t", client: &http.Client{Transport: fingerprintHTTPTransport(plainDial)}}
	// 端口 1 上无服务;直连本机回环立即拒绝
	resp, err := tr.RoundTripRaw(context.Background(), pipeline.Request{URL: "http://127.0.0.1:1/x", Method: http.MethodGet})
	if err == nil {
		resp.Body.Close()
		t.Fatal("连接失败应返回错误")
	}
	if resp != nil {
		t.Fatal("失败路径响应应为 nil")
	}
}

// TestJoinStateDir Given 状态根目录 When 拼接 Then 空根返回名字本身,非空根按名隔离
func TestJoinStateDir(t *testing.T) {
	if got := joinStateDir("", "t1"); got != "t1" {
		t.Fatalf("空根应返回名字本身,得到 %q", got)
	}
	if got := joinStateDir("/data", "t1"); got != "/data/t1" {
		t.Fatalf("应拼接为 /data/t1,得到 %q", got)
	}
}

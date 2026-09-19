package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 平台探测记录(2026-03 实测;免 key 真实推理平台稀缺) ───
//
// 平台            无 key 状态           结论
// openrouter      /models 公开,推理 401  需 key(已有 TestRealOpencode*)
// pollinations    200 匿名可推理          已接入(TestRealPollinations*)
// nvidia integrate /models 公开,推理 401  需 key(免费 key 需注册 NGC)
// deepseek/groq/cerebras/together/mistral/xai/hf  401/403/000  需 key 或被墙
// opencode zen public  403 FreeTierError  出口 IP 门禁,见 e2e_real_opencode_test.go 注释
//
// 本文件:对可接入平台补齐 streaming 场景与重复请求幂等性(限流恢复路径)。

const (
	pollinationsBase = "https://text.pollinations.ai"
	pollinationsPath = "/openai"
	// 匿名层全局队列限并发 1:测试间节流 + 长超时(推理模型首 token 慢)
	pollinationsGap     = 3 * time.Second
	platformHTTPTimeout = 90 * time.Second
)

var platformLastReq time.Time

// platformPost 长超时 POST + 平台限流节流;cookie 可空
func platformPost(t *testing.T, f *fourGroupsFixture, path, body string, headers map[string]string, cookie *http.Cookie) (int, string) {
	t.Helper()
	if elapsed := time.Since(platformLastReq); elapsed < pollinationsGap {
		time.Sleep(pollinationsGap - elapsed)
	}
	platformLastReq = time.Now()
	client := &http.Client{Timeout: platformHTTPTimeout}
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func pollinationsPost(t *testing.T, f *fourGroupsFixture, path, body string, headers map[string]string) (int, string) {
	t.Helper()
	return platformPost(t, f, path, body, headers, nil)
}

// adminCookieOr 惰性登录取会话
func (f *fourGroupsFixture) adminCookieOr(t *testing.T) *http.Cookie {
	t.Helper()
	if f.adminCookie == nil {
		f.adminCookie = f.adminLogin(t)
	}
	return f.adminCookie
}

const pollinationsModel = "openai-fast"

// newPlatformFixture 指定平台+模型的标准 opencode 包实例(Pollinations 类 chat-completions 兼容网关)
func newPlatformFixture(t *testing.T, name, baseUrl, model string) *fourGroupsFixture {
	t.Helper()
	manifest, src := mustOpencodeFiles(t)
	f := newFourGroups(t)
	if err := f.app.AdminDeps.Packages.Install(context.Background(), aapZip(t, manifest, map[string]string{"protocol.js": src})); err != nil {
		t.Fatalf("install opencode pkg: %v", err)
	}
	u := &upstream.Upstream{
		Name: name, Enabled: true,
		Base:   upstream.PackageRef{Package: "opencode"},
		Models: []string{model},
		Targets: []upstream.Target{{Name: "t1", BaseURL: baseUrl, Transport: "", Enabled: true,
			Secrets: map[string]string{"api_key": ""}}},
	}
	if err := f.app.Registry.Save(context.Background(), u); err != nil {
		t.Fatalf("save upstream: %v", err)
	}
	return f
}

func platformsGate(t *testing.T) {
	t.Helper()
	if os.Getenv("OPENCODE_TEST_PLATFORMS") != "1" {
		t.Skip("OPENCODE_TEST_PLATFORMS != 1; skipping platform E2E")
	}
}

func TestPlatform_Pollinations_ModelCatalog(t *testing.T) {
	// Given Pollinations 匿名层 When 拉模型目录 Then 仅 openai-fast 匿名可用(openai 为其别名)
	platformsGate(t)
	m := pollinationsModels(t)
	var found bool
	for _, it := range m {
		if it.Name == pollinationsModel {
			found = true
			if !it.Tools {
				t.Errorf("%s should declare tools support", pollinationsModel)
			}
		}
	}
	if !found {
		t.Fatalf("anonymous model %s missing: %v", pollinationsModel, m)
	}
}

func TestPlatform_Pollinations_StreamingWithTools(t *testing.T) {
	// Given tools:true 声明 When 带工具定义的流式请求 Then 网关透传 tool_calls 形态(链路不破坏结构化输出)
	platformsGate(t)
	f := newPlatformFixture(t, "real-pollinations", pollinationsBase, "openai-fast")
	payload := `{"model":"openai-fast","max_tokens":128,"stream":true,
	  "messages":[{"role":"user","content":"What is the weather in Paris? Use the tool."}],
	  "tools":[{"type":"function","function":{"name":"get_weather","description":"Get current weather",
	    "parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]}`
	got, body := pollinationsPost(t, f, "/v1/chat/completions", payload, map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	})
	if got != http.StatusOK {
		t.Fatalf("status %d body: %s", got, body)
	}
	// 上游与参数决定是否真出 tool_calls;此处断言流未破坏(有增量帧且 [DONE] 收尾)
	if !strings.Contains(body, "data: ") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("sse shape: %q", body)
	}
}

func TestPlatform_Pollinations_RepeatedAfter429(t *testing.T) {
	// Given 匿名层按 IP 队列限流 When 连续两请求 Then 均返回终局(200 或 429),网关自身不重试不挂起
	platformsGate(t)
	f := newPlatformFixture(t, "real-pollinations", pollinationsBase, pollinationsModel)
	for i := 0; i < 2; i++ {
		start := time.Now()
		got, body := pollinationsPost(t, f, "/v1/chat/completions", chatPayload(pollinationsModel, false), map[string]string{
			"Authorization": "Bearer sk-test", "Content-Type": "application/json",
		})
		elapsed := time.Since(start)
		if elapsed > 60*time.Second {
			t.Fatalf("round %d took %v (gateway must not hang)", i, elapsed)
		}
		if got != http.StatusOK && got != http.StatusTooManyRequests {
			t.Fatalf("round %d: unexpected status %d body %s", i, got, body)
		}
		t.Logf("round %d => %d (%v)", i, got, elapsed)
	}
}

// pollinationsModelInfo 匿名层模型元数据
type pollinationsModelInfo struct {
	Name  string `json:"name"`
	Tier  string `json:"tier"`
	Tools bool   `json:"tools"`
}

// pollinationsModels 拉取 Pollinations 模型目录
func pollinationsModels(t *testing.T) []pollinationsModelInfo {
	t.Helper()
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get("https://text.pollinations.ai/models")
	if err != nil {
		t.Fatalf("models: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out []pollinationsModelInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode models: %v", err)
	}
	return out
}

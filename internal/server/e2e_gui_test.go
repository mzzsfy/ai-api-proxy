package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── P9:管理 GUI 冒烟(单文件内嵌页 + 部件 API 闭环) ───

func TestGUI_AdminPageServed(t *testing.T) {
	// Given 完整装配 When GET /admin Then 200 HTML(登录页公开可达)
	f := newFourGroups(t)
	resp, err := f.client.Get(f.gateway.URL + "/admin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("content type: %s", ct)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	// 监控页指标标记元素(RPM 三项 + 错误率 + 并发 + 页签)
	for _, mark := range []string{`id="rpm-now"`, `id="rpm-avg"`, `id="rpm-peak"`, `id="err-rate"`, `id="conc"`, `data-p="mon"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing %s", mark)
		}
	}
	// 导航四页与分组(模型管理/插件管理/插件配置/监控;旧"包/上游"命名移除)
	for _, mark := range []string{`data-p="ups"`, `data-p="pkgs"`, `data-p="plugconf"`, `>模型管理<`, `>插件管理<`, `>插件配置<`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing nav mark %s", mark)
		}
	}
	// S6 插件管理页纯化:配置/keys 弹层与内嵌编辑区移除(功能迁插件配置页)
	for _, gone := range []string{`id="cfgbox"`, `id="keysbox"`} {
		if strings.Contains(body, gone) {
			t.Fatalf("admin page must not contain %s (插件管理页已纯化)", gone)
		}
	}
	// S7 插件配置页五 tabs + 元信息条 + 包监控图
	for _, mark := range []string{`data-tab="config"`, `data-tab="keys"`, `data-tab="tasks"`, `data-tab="code"`, `data-tab="mon"`,
		`id="plugcfg-meta"`, `id="pkg-chart"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing plugconf mark %s", mark)
		}
	}
	// S8 监控页增强:包维度表 + 双 sparkline;对话测试面板
	for _, mark := range []string{`id="bypkg"`, `id="rpm-spark"`, `id="conc-spark"`, `id="chatpanel"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing monitor mark %s", mark)
		}
	}
	// 双主题:浅色默认 token + 暗色覆盖组 + 切换按钮
	for _, mark := range []string{`:root`, `html[data-theme="dark"]`, `id="themetoggle"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing theme mark %s", mark)
		}
	}
	// 零外部依赖守卫:内嵌页不得引用外链资源(架构约束:离线可用、无构建链)
	for _, bad := range []string{"http://", "https://"} {
		for _, line := range strings.Split(body, "\n") {
			trim := strings.TrimSpace(line)
			lower := strings.ToLower(trim)
			if strings.Contains(lower, bad) &&
				(strings.Contains(lower, "<script") || strings.Contains(lower, "<link") || strings.Contains(lower, "<img") ||
					strings.Contains(lower, "<iframe") || strings.Contains(lower, "@import")) {
				t.Fatalf("admin page references external resource: %s", trim)
			}
		}
	}
}

func TestGUI_PartCodeEditHotReload(t *testing.T) {
	// Given 载入 js-openai 的 protocol 源码 When 改写 mapResponse 并 PUT Then 新请求用新代码(热生效)
	f := newFourGroups(t)
	// ① 未认证 GET code → 401
	resp, _ := f.client.Get(f.gateway.URL + "/admin/api/packages/js-openai/code?kind=protocol")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code read: %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	// ② 登录读取
	status, src := f.adminGet(t, "/admin/api/packages/js-openai/code?kind=protocol")
	if status != http.StatusOK || !strings.Contains(string(src), "buildRequest") {
		t.Fatalf("read code: %d %s", status, string(src[:min(80, len(src))]))
	}
	// ③ 修改:mapResponse 注入标记字段
	mutated := strings.Replace(string(src), "mapResponse: function (ctx, body) { return body; }",
		`mapResponse: function (ctx, body) { var o = JSON.parse(body); o.x_gui_edit = 1; return JSON.stringify(o); }`, 1)
	if mutated == string(src) {
		t.Fatal("mutation did not apply (source anchor drifted)")
	}
	code, body := f.adminPut(t, "/admin/api/packages/js-openai/code?kind=protocol", mutated)
	if code != http.StatusOK {
		t.Fatalf("put code: %d %s", code, body)
	}
	// ④ 新请求走新代码(热生效断言:x_gui_edit 出现在响应)
	payload := `{"model":"m-direct","messages":[{"role":"user","content":"hello"}],"stream":false}`
	got, respBody := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, payload)
	if got != http.StatusOK {
		t.Fatalf("request after edit: %d %s", got, respBody)
	}
	if !strings.Contains(respBody, `"x_gui_edit":1`) {
		t.Fatalf("hot reload failed: %s", respBody)
	}
}

func TestGUI_UpstreamTestEndpoint(t *testing.T) {
	// Given 已装配上游 When POST /upstreams/{id}/test Then ok=true 且延迟>0;坏上游 ok=false 带原因
	f := newFourGroups(t)
	// ① 好上游:直连组(m-direct 的上游 id 查列表)
	ups := f.app.Registry.List()
	var goodID, badID int64
	for _, u := range ups {
		if u.Name == "g1-direct-nofilter" {
			goodID = u.ID
		}
	}
	if goodID == 0 {
		t.Fatal("good upstream not found")
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/upstreams/%d/test", goodID), "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("test good: %d %s", status, body)
	}
	if !strings.Contains(body, `"latency_ms":`) {
		t.Fatalf("latency missing: %s", body)
	}
	// ② 坏上游:BaseURL 指向不可达端口
	if err := f.app.Registry.Save(context.Background(), &upstream.Upstream{
		Name: "g7-dead", Enabled: true,
		Base:   upstream.PackageRef{Package: "js-openai"},
		Models: []string{"m-dead"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Transport: "", Enabled: true,
			Secrets: map[string]string{"api_key": upstreamAPIKey}}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, u := range f.app.Registry.List() {
		if u.Name == "g7-dead" {
			badID = u.ID
		}
	}
	status, body = f.adminPost(t, fmt.Sprintf("/admin/api/upstreams/%d/test", badID), "")
	if status != http.StatusOK {
		t.Fatalf("test bad status: %d %s", status, body)
	}
	if !strings.Contains(body, `"ok":false`) {
		t.Fatalf("bad upstream must report failure: %s", body)
	}
}

func TestGUI_TransportsListAndTest(t *testing.T) {
	// Given 配置了 http_proxy 传输 When 列表+连通测试 Then 清单正确且 direct/代理探测结果正确
	// probe 覆写为本地 mock(不打外网)
	f := newFourGroups(t)
	origProbe := transportProbeURL
	transportProbeURL = f.upstreamSrv.URL
	t.Cleanup(func() { transportProbeURL = origProbe })

	status, body := f.adminGetRaw(t, "/admin/api/transports")
	if status != http.StatusOK || !strings.Contains(body, `"name":"px"`) || !strings.Contains(body, `"url":"http://`) {
		t.Fatalf("transports list: %d %s", status, body)
	}
	// 代理实例探测:经 forward 代理打 mock 上游(HEAD 非流式路径返回 200/405 均视为链路通)
	status, body = f.adminPost(t, "/admin/api/transports/px/test", "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("transport px test: %d %s", status, body)
	}
	// 不存在实例
	status, body = f.adminPost(t, "/admin/api/transports/nope/test", "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":false`) {
		t.Fatalf("transport nope test: %d %s", status, body)
	}
}

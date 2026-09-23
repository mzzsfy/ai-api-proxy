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
	// S7 插件配置页三 tabs(配置/keys/任务) + 元信息条(仅上下文标识,无操作按钮)
	for _, mark := range []string{`data-tab="config"`, `data-tab="keys"`, `data-tab="tasks"`, `id="plugcfg-meta"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing plugconf mark %s", mark)
		}
	}
	// S7 插件配置页纯化:包操作/代码/监控入口不得回流
	for _, gone := range []string{`id="pd-toggle"`, `id="pd-del"`, `data-tab="code"`, `data-tab="mon"`,
		`id="pane-code"`, `id="pane-mon"`, `id="pkg-chart"`} {
		if strings.Contains(body, gone) {
			t.Fatalf("admin page must not contain %s (插件配置页已纯化)", gone)
		}
	}
	// S8 监控页增强:包维度表 + 双 sparkline;对话测试面板
	for _, mark := range []string{`id="bypkg"`, `id="rpm-spark"`, `id="conc-spark"`, `id="chatpanel"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing monitor mark %s", mark)
		}
	}
	// 模型行批量输入:列表添加/删除,替代旧逗号单输入
	for _, mark := range []string{`id="model-list"`, `id="model-add"`, `data-model-name data-i=`, `data-model-remove data-i=`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing model batch mark %s", mark)
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

func TestGUI_PartCodeEditRemoved(t *testing.T) {
	// Given 代码编辑能力已整体移除 When 已登录读/写包代码 Then 均返回 404,不再提供伪需求入口
	f := newFourGroups(t)
	status, _ := f.adminGet(t, "/admin/api/packages/js-openai/code?kind=protocol")
	if status != http.StatusNotFound {
		t.Fatalf("code read must be removed: %d", status)
	}
	status, body := f.adminPut(t, "/admin/api/packages/js-openai/code?kind=protocol", "module.exports = {}")
	if status != http.StatusNotFound {
		t.Fatalf("code write must be removed: %d %s", status, body)
	}
}

func TestGUI_ModelTestEndpoint(t *testing.T) {
	// Given 已装配模型行 When POST /models/{id}/test Then ok=true 且延迟>0;坏行 ok=false 带原因
	f := newFourGroups(t)
	// ① 好行:直连组(m-direct 的行 id 查列表)
	rows := f.app.Registry.List()
	var goodID int64
	for _, u := range rows {
		if u.Name == "m-direct" {
			goodID = u.ID
		}
	}
	if goodID == 0 {
		t.Fatal("good model row not found")
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/models/%d/test", goodID), "")
	if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("test good: %d %s", status, body)
	}
	if !strings.Contains(body, `"latency_ms":`) {
		t.Fatalf("latency missing: %s", body)
	}
	// ② 坏行:params 覆盖 base_url 指向不可达端口(v2 覆盖层级:模型 > 插件)
	if err := f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: "m-dead", Plugin: "js-openai", Enabled: true,
		Params: map[string]any{"base_url": "http://127.0.0.1:1"},
	}); err != nil {
		t.Fatal(err)
	}
	var badID int64
	for _, u := range f.app.Registry.List() {
		if u.Name == "m-dead" {
			badID = u.ID
		}
	}
	status, body = f.adminPost(t, fmt.Sprintf("/admin/api/models/%d/test", badID), "")
	if status != http.StatusOK {
		t.Fatalf("test bad status: %d %s", status, body)
	}
	if !strings.Contains(body, `"ok":false`) {
		t.Fatalf("bad model row must report failure: %s", body)
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

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
	// Given 完整装配 When GET /admin Then 200 HTML(Vue SPA 入口 + 相对资源引用)
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
	for _, mark := range []string{`id="app"`, `<script type="module"`} {
		if !strings.Contains(body, mark) {
			t.Fatalf("admin page missing %s", mark)
		}
	}
	// 内嵌页资源必须相对引用(离线可用)
	for _, bad := range []string{"http://", "https://"} {
		if strings.Contains(body, bad) {
			t.Fatalf("admin page references external resource: %s", bad)
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

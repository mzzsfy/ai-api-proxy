package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// chatTestUp 查模型行 id
func chatTestUp(t *testing.T, f *fourGroupsFixture, name string) int64 {
	t.Helper()
	for _, u := range f.app.Registry.List() {
		if u.Name == name {
			return u.ID
		}
	}
	t.Fatalf("model row %s not found", name)
	return 0
}

// liveOnce 查询 live 指标
func liveOnce(t *testing.T, f *fourGroupsFixture) map[string]any {
	t.Helper()
	code, body := f.adminGet(t, "/admin/api/metrics/live")
	if code != http.StatusOK {
		t.Fatalf("live: %d", code)
	}
	var live map[string]any
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	return live
}

// saveRow 测试内快速建行(v2:行名即模型名,连接差异走行 params 覆盖)
func saveRow(t *testing.T, f *fourGroupsFixture, name, pkg string, params map[string]any) {
	t.Helper()
	if err := f.app.Registry.Save(context.Background(), &upstream.Model{
		Name: name, Plugin: pkg, Enabled: true, Params: params,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestChatTest_Success(t *testing.T) {
	// Given 可用模型行 When POST chat-test(消息) Then 200 ok:true 返回 latency/status/body 且 模型行/包双维度计数+1
	f := newFourGroups(t)
	id := chatTestUp(t, f, "m-direct")
	code, body := f.adminPost(t, "/admin/api/models/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"message":"你好"}`)
	if code != http.StatusOK {
		t.Fatalf("chat-test: %d %s", code, body)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true {
		t.Fatalf("ok: %v (%s)", out["ok"], body)
	}
	if _, ok := out["latency_ms"]; !ok {
		t.Fatal("latency_ms missing")
	}
	if _, ok := out["body"]; !ok {
		t.Fatal("body missing")
	}
	if out["status"] != float64(200) {
		t.Fatalf("status: %v", out["status"])
	}
	pkgReqs, _ := dimCount(t, liveOnce(t, f)["by_package"], "js-openai")
	rowReqs, _ := dimCount(t, liveOnce(t, f)["by_upstream"], "m-direct")
	if pkgReqs != 1 || rowReqs != 1 {
		t.Fatalf("counters pkg=%d row=%d, want 1/1", pkgReqs, rowReqs)
	}
}

func TestChatTest_UpstreamErrorCounts(t *testing.T) {
	// Given 行包参数覆盖 base_url 指向 500 上游 When chat-test Then 200 ok:false status:500 且 by_package errors +1
	f := newFourGroups(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	// 行 params 覆盖包参数 base_url(v2 覆盖层级:模型 > 插件)
	saveRow(t, f, "m-e500", "js-openai", map[string]any{"base_url": srv.URL})
	id := chatTestUp(t, f, "m-e500")
	code, body := f.adminPost(t, "/admin/api/models/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"message":"ping"}`)
	if code != http.StatusOK {
		t.Fatalf("chat-test: %d %s", code, body)
	}
	var out struct {
		Ok     bool   `json:"ok"`
		Status int    `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out.Ok || out.Status != 500 {
		t.Fatalf("want ok:false status:500, got %s", body)
	}
	_, errMsg := dimCount(t, liveOnce(t, f)["by_package"], "js-openai")
	if errMsg != 1 {
		t.Fatalf("by_package[js-openai].errors = %d, want 1", errMsg)
	}
}

func TestChatTest_Unreachable(t *testing.T) {
	// Given 行 params 指向不可达地址 When chat-test Then 200 ok:false 且 by_package errors +1
	f := newFourGroups(t)
	saveRow(t, f, "m-dead", "js-openai", map[string]any{"base_url": "http://127.0.0.1:1"})
	id := chatTestUp(t, f, "m-dead")
	code, body := f.adminPost(t, "/admin/api/models/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"message":"ping"}`)
	if code != http.StatusOK {
		t.Fatalf("chat-test: %d %s", code, body)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != false {
		t.Fatalf("unreachable must be ok:false: %s", body)
	}
}

func TestChatTest_NotFoundAndValidation(t *testing.T) {
	// Given 不存在的行 id When chat-test Then 404(行名即模型名,请求体仅消息)
	f := newFourGroups(t)
	code, _ := f.adminPost(t, "/admin/api/models/99999/chat-test", `{"message":"hi"}`)
	if code != http.StatusNotFound {
		t.Fatalf("not found: %d", code)
	}
	id := chatTestUp(t, f, "m-direct")
	code, _ = f.adminPost(t, "/admin/api/models/"+strconv.FormatInt(id, 10)+"/chat-test", `{}`)
	if code != http.StatusOK {
		t.Fatalf("default message ping: %d", code)
	}
}

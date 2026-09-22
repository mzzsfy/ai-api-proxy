package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// chatTestUp 查上游 id
func chatTestUp(t *testing.T, f *fourGroupsFixture, name string) int64 {
	t.Helper()
	for _, u := range f.app.Registry.List() {
		if u.Name == name {
			return u.ID
		}
	}
	t.Fatalf("upstream %s not found", name)
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

func TestChatTest_Success(t *testing.T) {
	// Given 上游有可用目标 When POST chat-test(合法模型+消息) Then 200 ok:true 返回 latency/status/body 且 by_package 计数+1
	f := newFourGroups(t)
	id := chatTestUp(t, f, "g1-direct-nofilter")
	code, body := f.adminPost(t, "/admin/api/upstreams/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"model":"m-direct","message":"你好"}`)
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
	// 目标维度计数(executor OnTargetExit 照常)
	tgReqs, _ := dimCount(t, liveOnce(t, f)["by_target"], "g1-direct-nofilter/t1")
	if tgReqs != 1 {
		t.Fatalf("by_target requests = %d, want 1", tgReqs)
	}
	pkgReqs, _ := dimCount(t, liveOnce(t, f)["by_package"], "js-openai")
	upReqs, _ := dimCount(t, liveOnce(t, f)["by_upstream"], "g1-direct-nofilter")
	if pkgReqs != 1 || upReqs != 1 {
		t.Fatalf("counters pkg=%d up=%d, want 1/1", pkgReqs, upReqs)
	}
}

func TestChatTest_UpstreamErrorCounts(t *testing.T) {
	// Given 上游返回 500 When chat-test Then 200 ok:false status:500 且 by_package errors +1(>=400 计错误口径,body 透传)
	f := newFourGroups(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "g8-e500", Enabled: true,
		Base:   upstream.PackageRef{Package: "js-openai"},
		Models: []string{"m-e500"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: srv.URL, Enabled: true,
			Secrets: map[string]string{"api_key": upstreamAPIKey}}},
	}); err != nil {
		t.Fatal(err)
	}
	id := chatTestUp(t, f, "g8-e500")
	code, body := f.adminPost(t, "/admin/api/upstreams/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"model":"m-e500","message":"ping"}`)
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

func TestChatTest_UpstreamUnreachable(t *testing.T) {
	// Given 目标连接被拒(执行错误) When chat-test Then 200 ok:false 且 by_package errors +1
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "g8-dead", Enabled: true,
		Base:   upstream.PackageRef{Package: "js-openai"},
		Models: []string{"m-dead"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Enabled: true,
			Secrets: map[string]string{"api_key": upstreamAPIKey}}},
	}); err != nil {
		t.Fatal(err)
	}
	id := chatTestUp(t, f, "g8-dead")
	code, body := f.adminPost(t, "/admin/api/upstreams/"+strconv.FormatInt(id, 10)+"/chat-test",
		`{"model":"m-dead","message":"ping"}`)
	if code != http.StatusOK {
		t.Fatalf("chat-test: %d %s", code, body)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != false {
		t.Fatalf("dead upstream must be ok:false: %s", body)
	}
}

func TestChatTest_NoModels(t *testing.T) {
	// Given 上游模型声明为空 When Registry.Save Then 拒绝(models required)
	// 无模型上游在保存期即被拒,chat-test 的 400 无模型分支不可达(handler 保留为防御检查)
	f := newFourGroups(t)
	err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "g9-nomodels", Enabled: true,
		Base:    upstream.PackageRef{Package: "js-openai"},
		Models:  []string{},
		Targets: []upstream.Target{{Name: "t1", BaseURL: f.upstreamBase, Enabled: true}},
	})
	if err == nil || err.Error() != "models required" {
		t.Fatalf("empty models must be rejected at save: %v", err)
	}
}

func TestChatTest_NotFoundAndValidation(t *testing.T) {
	// Given 不存在的上游 id / 空 model When chat-test Then 404 / 400
	f := newFourGroups(t)
	code, _ := f.adminPost(t, "/admin/api/upstreams/99999/chat-test", `{"model":"m-direct"}`)
	if code != http.StatusNotFound {
		t.Fatalf("not found: %d", code)
	}
	id := chatTestUp(t, f, "g1-direct-nofilter")
	code, body := f.adminPost(t, "/admin/api/upstreams/"+strconv.FormatInt(id, 10)+"/chat-test", `{"message":"hi"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("empty model: %d %s", code, body)
	}
}

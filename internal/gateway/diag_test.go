package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// fixtureGroup 取 fixture 中唯一模型组
func fixtureGroup(t *testing.T, f *fixture) *upstream.Group {
	t.Helper()
	grps := f.reg.ListGroups()
	if len(grps) != 1 {
		t.Fatalf("fixture 应恰有 1 个模型组,得到 %d", len(grps))
	}
	return grps[0]
}

// TestTestUpstream_成功 Given 上游 200 When TestUpstream Then 延迟非负且无错误说明
func TestTestUpstream_成功(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"choices":[]}`)
	latency, errMsg := f.g.TestUpstream(context.Background(), fixtureGroup(t, f), "test-model")
	if errMsg != "" {
		t.Fatalf("上游 200 应无错误说明,得到 %q", errMsg)
	}
	if latency < 0 {
		t.Fatalf("延迟不应为负: %d", latency)
	}
}

// TestTestUpstream_上游错误状态 Given 上游 500 When TestUpstream Then 测试失败但计入延迟,错误说明含状态码
func TestTestUpstream_上游错误状态(t *testing.T) {
	f := newFixture(t, http.StatusInternalServerError, "application/json", `{"error":"boom"}`)
	latency, errMsg := f.g.TestUpstream(context.Background(), fixtureGroup(t, f), "test-model")
	if !strings.Contains(errMsg, "upstream status 500") {
		t.Fatalf("错误说明应含 upstream status 500,得到 %q", errMsg)
	}
	if latency < 0 {
		t.Fatalf("失败也计入延迟,得到 %d", latency)
	}
}

// TestChatTest_成功_非流式 Given 上游 200 When ChatTest Then 状态 200 与响应体原样返回
func TestChatTest_成功_非流式(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"choices":[{"message":{"role":"assistant"}}]}`)
	latency, status, body, errMsg := f.g.ChatTest(context.Background(), fixtureGroup(t, f), "test-model", "hello")
	if errMsg != "" {
		t.Fatalf("上游 200 应无错误说明,得到 %q", errMsg)
	}
	if status != http.StatusOK {
		t.Fatalf("状态应为 200,得到 %d", status)
	}
	if !strings.Contains(string(body), "assistant") {
		t.Fatalf("响应体应原样返回,得到 %q", string(body))
	}
	if latency < 0 {
		t.Fatalf("延迟不应为负: %d", latency)
	}
}

// TestChatTest_上游4xx错误口径 Given 上游 404 When ChatTest Then 返回上游状态与错误说明(响应失败口径 >=300)
func TestChatTest_上游4xx错误口径(t *testing.T) {
	f := newFixture(t, http.StatusNotFound, "application/json", `{"error":"no model"}`)
	_, status, body, errMsg := f.g.ChatTest(context.Background(), fixtureGroup(t, f), "test-model", "hello")
	if status != http.StatusNotFound {
		t.Fatalf("状态应透传 404,得到 %d", status)
	}
	if !strings.Contains(errMsg, "upstream status 404") {
		t.Fatalf("错误说明应含 upstream status 404,得到 %q", errMsg)
	}
	if !strings.Contains(string(body), "no model") {
		t.Fatalf("错误体应原样返回,得到 %q", string(body))
	}
}

// TestChatTest_流式拼接 Given 上游 SSE When ChatTest Then 各帧 JSON 数组包裹返回
func TestChatTest_流式拼接(t *testing.T) {
	sse := "data: {\"delta\":\"a\"}\n\ndata: [DONE]\n\n"
	f := newFixture(t, http.StatusOK, "text/event-stream", sse)
	_, status, body, errMsg := f.g.ChatTest(context.Background(), fixtureGroup(t, f), "test-model", "hello")
	if errMsg != "" {
		t.Fatalf("SSE 200 应无错误说明,得到 %q", errMsg)
	}
	if status != http.StatusOK {
		t.Fatalf("状态应为 200,得到 %d", status)
	}
	got := string(body)
	var frames []any
	if err := json.Unmarshal([]byte(got), &frames); err != nil {
		t.Fatalf("流式结果应为合法 JSON 数组,得到 %q: %v", got, err)
	}
	if len(frames) != 1 {
		t.Fatalf("应恰含 1 个数据帧([DONE] 不入数组),得到 %d: %q", len(frames), got)
	}
	if !strings.Contains(got, `"delta":"a"`) {
		t.Fatalf("数组应含数据帧,得到 %q", got)
	}
}

// TestRunErrorStatus映射 Run 错误 → HTTP 状态单一映射表
// Given 各错误类型 When runErrorStatus Then 能力 400/池忙与无出口 503/其余 502
func TestRunErrorStatus映射(t *testing.T) {
	capErr := &pipeline.CapabilityError{Reason: "bad form"}
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"能力不符", capErr, http.StatusBadRequest},
		{"池忙", pipeline.ErrPoolBusy, http.StatusServiceUnavailable},
		{"无出口", ipprovider.ErrNoExits, http.StatusServiceUnavailable},
		{"其他错误", errors.New("dial refused"), http.StatusBadGateway},
		{"包装的池忙", joinErr{pipeline.ErrPoolBusy}, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runErrorStatus(tt.err); got != tt.want {
				t.Fatalf("runErrorStatus = %d,期望 %d", got, tt.want)
			}
		})
	}
}

type joinErr struct{ target error }

func (e joinErr) Error() string { return "wrap: " + e.target.Error() }
func (e joinErr) Unwrap() error { return e.target }

// TestWriteRunError输出 Given 各错误 When writeRunError Then 状态码与错误文案按分支写出
func TestWriteRunError输出(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{}`)
	entry := openaiEntry()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{"能力不符", &pipeline.CapabilityError{Reason: "form mismatch"}, http.StatusBadRequest, "form mismatch"},
		{"池忙", pipeline.ErrPoolBusy, http.StatusServiceUnavailable, "runtime pool busy"},
		{"无出口", ipprovider.ErrNoExits, http.StatusServiceUnavailable, "no available egress"},
		{"其他", errors.New("boom"), http.StatusBadGateway, "boom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			f.g.writeRunError(w, entry, nil, pipeline.Resolved{}, tt.err)
			if w.Code != tt.wantStatus {
				t.Fatalf("状态 = %d,期望 %d", w.Code, tt.wantStatus)
			}
			if !strings.Contains(w.Body.String(), tt.wantBody) {
				t.Fatalf("响应体应含 %q,得到 %q", tt.wantBody, w.Body.String())
			}
		})
	}
}

// TestModelByID Given 已注册模型行 When ModelByID Then 命中返回对象形态,未命中 404
func TestModelByID(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{}`)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models/gw/test-model", nil)
	f.g.ModelByID(w, req, "gw/test-model")
	if w.Code != http.StatusOK {
		t.Fatalf("已注册模型应 200,得到 %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"id":"gw/test-model"`) {
		t.Fatalf("应返回模型对象,得到 %q", w.Body.String())
	}

	w2 := httptest.NewRecorder()
	f.g.ModelByID(w2, req, "gw/absent")
	if w2.Code != http.StatusNotFound {
		t.Fatalf("未注册模型应 404,得到 %d", w2.Code)
	}
	if !strings.Contains(w2.Body.String(), "not_found") {
		t.Fatalf("404 体应含 not_found,得到 %q", w2.Body.String())
	}
}

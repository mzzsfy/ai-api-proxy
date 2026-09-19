package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-api-proxy/internal/pipeline"
)

func TestRoundTrip_NonStream(t *testing.T) {
	// Given 上游返回 JSON When RoundTrip Then 状态与体透传,请求头到达上游
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	tr, _ := m.Get("direct")
	req := pipeline.Request{URL: srv.URL + "/v1/x", Method: "POST", Headers: map[string]string{"Authorization": "Bearer k"}, Body: []byte(`{}`)}
	resp, err := tr.RoundTrip(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 || string(resp.Body) != `{"ok":true}` {
		t.Fatalf("resp: %d %s", resp.Status, resp.Body)
	}
	if gotAuth != "Bearer k" || gotPath != "/v1/x" {
		t.Fatalf("upstream saw: %s %s", gotAuth, gotPath)
	}
}

func TestRoundTrip_SSEFraming(t *testing.T) {
	// Given SSE 流(含 event 字段/多 data 行/空行分帧) When RoundTrip Then 帧序列正确
	sse := "event: a\ndata: {\"i\":1}\n\ndata: line1\ndata: line2\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(sse))
	}))
	defer srv.Close()
	m, _ := NewManager(nil)
	tr, _ := m.Get("direct")
	resp, err := tr.RoundTrip(context.Background(), pipeline.Request{URL: srv.URL, Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	var frames []pipeline.SSEFrame
	for f := range resp.Events {
		frames = append(frames, f)
	}
	if len(frames) != 2 {
		t.Fatalf("frames: %+v", frames)
	}
	if frames[0].Event != "a" || frames[0].Data != `{"i":1}` {
		t.Fatalf("frame0: %+v", frames[0])
	}
	if frames[1].Event != "" || frames[1].Data != "line1\nline2" {
		t.Fatalf("frame1: %+v", frames[1])
	}
}

func TestManager_DirectDefault(t *testing.T) {
	// Given 空配置 When Get("direct") Then 默认实例存在
	m, err := NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("direct"); !ok {
		t.Fatal("direct missing")
	}
	if _, ok := m.Get("nope"); ok {
		t.Fatal("unknown instance returned")
	}
}

func TestManager_HTTPProxyInvalidURL(t *testing.T) {
	// Given 非法代理 URL When NewManager Then 报错含实例名
	_, err := NewManager([]TransportDef{{Name: "bad", Type: "http_proxy", URL: "://x"}})
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("proxy url: %v", err)
	}
}

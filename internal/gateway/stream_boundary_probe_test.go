//go:build audit

// 已知生产缺陷的复现探针,默认测试套件不加载。
// 运行: go test -race -tags=audit ./internal/gateway -run '^TestStreamBoundaryProbe_' -count=1
// 当前全部预期失败;对应缺陷修复后应将探针迁入默认回归套件并删除本文件,
// 不得仅删除失败断言。缺陷明细见 docs/progress/test-stream-boundaries.md。

package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

type probeBrokenWriter struct {
	header http.Header
	err    error
	writes int
}

func (w *probeBrokenWriter) Header() http.Header       { return w.header }
func (*probeBrokenWriter) WriteHeader(int)             {}
func (w *probeBrokenWriter) Write([]byte) (int, error) { w.writes++; return 0, w.err }
func (*probeBrokenWriter) Flush()                      {}

func TestStreamBoundaryProbe_HandlerPropagatesWriteFailure(t *testing.T) {
	// Given 下游写入失败 When handler 发事件或尾帧 Then 错误返回给脚本执行器
	for _, operation := range []string{"event", "end", "error"} {
		t.Run(operation, func(t *testing.T) {
			errProbeWrite := errors.New("synthetic downstream failure")
			writer := &probeBrokenWriter{header: make(http.Header), err: errProbeWrite}
			sink := &handlerSink{w: writer, framer: openaiEntry().Framer()}
			if err := sink.SSEStart(); err != nil {
				t.Fatal(err)
			}
			var err error
			switch operation {
			case "event":
				err = sink.Event(`[{"choices":[{"delta":{"content":"x"}}]}]`)
			case "end":
				err = sink.End()
			case "error":
				err = sink.ErrorFrame(`{"error":"upstream failure"}`)
			}
			if writer.writes == 0 {
				t.Fatal("fixture did not attempt a write")
			}
			if !errors.Is(err, errProbeWrite) {
				t.Fatalf("write failure lost: %v", err)
			}
		})
	}
}

func TestStreamBoundaryProbe_FastPathPreservesSSEErrorStatus(t *testing.T) {
	// Given 上游以 SSE 类型返回限流 When 快速路径转发 Then 保留原错误状态
	f := newFixture(t, http.StatusTooManyRequests, "text/event-stream", "data: rate limited\n\n")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test-model","stream":true,"messages":[]}`))
	response := httptest.NewRecorder()
	f.g.ChatCompletions(response, req)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("upstream status %d changed to %d; body=%q", http.StatusTooManyRequests, response.Code, response.Body.String())
	}
}

type probeCanceledTransport struct {
	cancel context.CancelFunc
	calls  int
}

func (*probeCanceledTransport) Name() string { return "direct" }
func (tr *probeCanceledTransport) RoundTrip(ctx context.Context, _ pipeline.Request) (pipeline.TransportResponse, error) {
	tr.calls++
	tr.cancel()
	return pipeline.TransportResponse{}, ctx.Err()
}

func TestStreamBoundaryProbe_CanceledKeysDoNotDispatchQueuedWork(t *testing.T) {
	// Given 单并发下多个键排队 When 首个请求取消 Then 队列键不再调用传输
	f := newFixture(t, http.StatusOK, "application/json", `{}`)
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if _, err := f.g.Keys.Set("openai-compatible", id, map[string]any{"api_key": id}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &probeCanceledTransport{cancel: cancel}
	f.g.Executor = &pipeline.Executor{Transports: func(string) (pipeline.Transport, bool) { return tr, true }}
	report, err := f.g.KeysCheck(ctx, groupByID(f.reg), KeysCheckRequest{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tr.calls != 1 {
		t.Fatalf("transport dispatches=%d; want only first of %d keys", tr.calls, report.Total)
	}
}

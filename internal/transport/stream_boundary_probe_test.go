//go:build audit

// 已知生产缺陷的复现探针,默认测试套件不加载。
// 运行: go test -race -tags=audit ./internal/transport -run '^TestStreamBoundaryProbe_' -count=1
// 当前全部预期失败;对应缺陷修复后应将探针迁入默认回归套件并删除本文件,
// 不得仅删除失败断言。缺陷明细见 docs/progress/test-stream-boundaries.md。

package transport

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

type probeErrorSSEProtocol struct{}

func (probeErrorSSEProtocol) Name() string     { return "probe" }
func (probeErrorSSEProtocol) Declared() string { return pipeline.ProtocolOpenAICompletions }
func (probeErrorSSEProtocol) Supports() pipeline.Supports {
	return pipeline.Supports{Forms: []string{pipeline.FormNonStreaming}}
}
func (probeErrorSSEProtocol) BuildRequest(*pipeline.PipelineContext, []byte) (pipeline.Request, error) {
	return pipeline.Request{Method: http.MethodPost, URL: "http://synthetic.invalid"}, nil
}
func (probeErrorSSEProtocol) MapResponse(_ *pipeline.PipelineContext, body []byte) ([]byte, error) {
	return body, nil
}
func (probeErrorSSEProtocol) MapEvent(_ *pipeline.PipelineContext, event []byte) ([]byte, error) {
	return event, nil
}

type probeErrorSSETransport struct {
	response *http.Response
	frames   <-chan pipeline.SSEFrame
}

func (*probeErrorSSETransport) Name() string { return "direct" }
func (tr *probeErrorSSETransport) RoundTrip(context.Context, pipeline.Request) (pipeline.TransportResponse, error) {
	result := consumeResponse(tr.response)
	tr.frames = result.Events
	return result, nil
}

func TestStreamBoundaryProbe_TerminalSSEDoesNotLeaveBlockedProducer(t *testing.T) {
	// Given 错误状态SSE超过缓冲区 When 管道终局拒绝且body关闭 Then 生产者不得永久阻塞在无人接收的通道
	synctest.Test(t, func(t *testing.T) {
		body, writer := io.Pipe()
		defer body.Close()
		defer writer.Close()
		tracked := &contractSSEBody{ReadCloser: body}
		tr := &probeErrorSSETransport{response: &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: tracked}}
		ex := &pipeline.Executor{Transports: func(string) (pipeline.Transport, bool) { return tr, true }}
		response, err := ex.Run(context.Background(), pipeline.NewContext("probe", pipeline.UpstreamInfo{}, pipeline.Vars{}), pipeline.Resolved{Protocol: probeErrorSSEProtocol{}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if response.Status != http.StatusTooManyRequests {
			t.Fatalf("status=%d", response.Status)
		}
		writeDone := make(chan error, 1)
		go func() { _, err := io.WriteString(writer, strings.Repeat("data: rejected\n\n", 64)); writeDone <- err }()
		synctest.Wait()
		_ = body.Close()
		_ = writer.Close()
		<-writeDone
		synctest.Wait()
		producerExited := tracked.closes != 0
		// 主动排空仅用于探针清理,生产终局路径没有该消费者。
		for range tr.frames {
		}
		synctest.Wait()
		if !producerExited {
			t.Error("terminal response abandoned SSE producer; body close did not release blocked channel send")
		}
	})
}

func TestStreamBoundaryProbe_StopExitsSSEProducer(t *testing.T) {
	// Given 完成帧发送后等待下一次上游读取 When stop 关闭 Then 生产者退出并关闭通道
	// 豁免依据:出站生产路径handlerhttp的source close同时关闭stop与body,生产路径未泄漏;
	// 本探针锁定helper的stop-only契约缺口。
	synctest.Test(t, func(t *testing.T) {
		body, upstream := io.Pipe()
		defer body.Close()
		defer upstream.Close()
		stop := make(chan struct{})
		items := FrameSSEResult(body, stop)
		writeDone := make(chan error, 1)
		go func() { _, err := io.WriteString(upstream, "data: first\n\n"); writeDone <- err }()
		if err := <-writeDone; err != nil {
			t.Fatal(err)
		}
		first := <-items
		if first.Frame.Data != "first" || first.Err != nil {
			t.Fatalf("first=%+v", first)
		}
		close(stop)
		synctest.Wait()
		stopped := false
		select {
		case _, ok := <-items:
			stopped = !ok
		default:
		}
		// 失败路径也关闭上游,保证探针不遗留阻塞读。
		_ = upstream.Close()
		_ = body.Close()
		for range items {
		}
		synctest.Wait()
		if !stopped {
			t.Error("stop did not terminate producer blocked on next read")
		}
	})
}

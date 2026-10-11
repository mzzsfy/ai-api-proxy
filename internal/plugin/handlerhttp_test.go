package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
)

func TestOutboundBodyBytesPreservesRawBytesWithoutContentTypeGate(t *testing.T) {
	want := []byte{0x00, 0x80, 0xff, 0xc3, 0x28}
	body := &countingCloseBody{Reader: bytes.NewReader(want)}
	src := &outboundSource{
		status:    http.StatusOK,
		headers:   map[string]string{"Content-Type": "application/octet-stream"},
		body:      body,
		totalAt:   time.Now().Add(time.Minute),
		stop:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}

	got, err := src.BodyBytes(context.Background())
	if err != nil {
		t.Fatalf("BodyBytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("BodyBytes = %v, want %v", got, want)
	}
	if body.closeCount != 1 {
		t.Fatalf("body Close count = %d, want 1", body.closeCount)
	}
	if _, err := src.Body(context.Background()); err == nil {
		t.Fatal("Body after BodyBytes must fail single-consume guard")
	}
	if _, err := src.BodyBytes(context.Background()); err == nil {
		t.Fatal("second BodyBytes must fail single-consume guard")
	}
	if _, err := src.Chunks(); err == nil {
		t.Fatal("Chunks after BodyBytes must fail single-consume guard")
	}
	if _, err := src.Frames(); err == nil {
		t.Fatal("Frames after BodyBytes must fail single-consume guard")
	}
}

type countingCloseBody struct {
	io.Reader
	closeCount int
}

func (b *countingCloseBody) Close() error {
	b.closeCount++
	return nil
}
func TestOutboundChunkIteratorReturnCloseError(t *testing.T) {
	closeErr := errors.New("body close failed")
	src := &outboundSource{headers: map[string]string{"Content-Type": "text/event-stream"}, body: &closeErrorBody{closeErr: closeErr}, stop: make(chan struct{}), closeDone: make(chan struct{})}
	vm := goja.New()
	var ex handlerExec
	source := ex.bindSource(vm, src)
	if err := vm.Set("source", source); err != nil {
		t.Fatal(err)
	}
	got, err := vm.RunString(`(function(){
	const it=source.chunks();
	let first=false, second=false;
	try { it.return(); } catch (e) { first=String(e).includes("body close failed"); }
	try { it.return(); } catch (e) { second=String(e).includes("body close failed"); }
	return first && second;
})()`)
	if err != nil || !got.ToBoolean() {
		t.Fatalf("both return calls should preserve close error: %v, %v", got, err)
	}
}

type blockingReadBody struct {
	started chan struct{}
	closed  chan struct{}
}

func (b *blockingReadBody) Read([]byte) (int, error) {
	select {
	case <-b.started:
	default:
		close(b.started)
	}
	<-b.closed
	return 0, errors.New("body closed")
}
func (b *blockingReadBody) Close() error { close(b.closed); return nil }

func TestOutboundChunkIteratorReturnUnblocksNext(t *testing.T) {
	body := &blockingReadBody{started: make(chan struct{}), closed: make(chan struct{})}
	cli := &outboundClient{to: outboundTimeouts{Connect: time.Second, Response: time.Second, FrameIdle: time.Minute, Total: time.Minute}}
	src := &outboundSource{cli: cli, status: 200, headers: map[string]string{"Content-Type": "text/event-stream"}, body: body,
		totalAt: time.Now().Add(time.Minute), chunks: transport.RawChunks(body, make(chan struct{})),
		stop: make(chan struct{}), closeDone: make(chan struct{})}
	iter := &chunkIter{s: src}
	nextDone := make(chan error, 1)
	go func() {
		_, _, err := iter.Next(context.Background())
		nextDone <- err
	}()
	<-body.started
	if err := src.close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	if err := <-nextDone; !errors.Is(err, errSourceTerminated) {
		t.Fatalf("next error after source close = %v, want errSourceTerminated", err)
	}
	if _, ok, err := iter.Next(context.Background()); !errors.Is(err, errSourceTerminated) || ok {
		t.Fatalf("next after source close: ok=%v err=%v, want errSourceTerminated", ok, err)
	}
}

func TestOutboundSourceCloseErrorIsStable(t *testing.T) {
	closeErr := errors.New("body close failed")
	body := &closeErrorBody{closeErr: closeErr}
	src := &outboundSource{body: body, stop: make(chan struct{}), closeDone: make(chan struct{})}
	if err := src.close(); !errors.Is(err, closeErr) {
		t.Fatalf("first close error = %v", err)
	}
	if err := src.close(); !errors.Is(err, closeErr) {
		t.Fatalf("repeated close error = %v", err)
	}
	const waiters = 8
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.close(); !errors.Is(err, closeErr) {
				t.Errorf("concurrent close error = %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestStreamDeadlineErrorBeatsReadyDataAtCommit(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		at   time.Time
		want error
	}{
		{name: "total timeout", ctx: context.Background(), at: now.Add(-time.Second), want: errOutboundTimeout},
		{name: "canceled context", ctx: canceledContext(), at: now.Add(time.Minute), want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := streamDeadlineError(tc.ctx, tc.at, now.Add(time.Minute), now); !errors.Is(err, tc.want) {
				t.Fatalf("deadline error=%v, want %v", err, tc.want)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestOutboundChunkNextAfterTotalDeadlineRejectsBufferedChunk(t *testing.T) {
	body := &countingCloseBody{Reader: bytes.NewReader(nil)}
	stop := make(chan struct{})
	chunks := make(chan transport.ChunkResult, 1)
	chunks <- transport.ChunkResult{Data: []byte("late")}
	cli := &outboundClient{to: outboundTimeouts{FrameIdle: time.Minute, Total: time.Minute}}
	src := &outboundSource{cli: cli, body: body, status: http.StatusOK, headers: map[string]string{"Content-Type": "text/event-stream"}, totalAt: time.Now().Add(-time.Second), stop: stop, closeDone: make(chan struct{}), chunks: chunks}
	_, ok, err := (&chunkIter{s: src}).Next(context.Background())
	if ok || !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("Next ok=%v err=%v, want total timeout", ok, err)
	}
}

func TestOutboundFrameNextAfterTotalDeadlineRejectsBufferedFrame(t *testing.T) {
	body := &countingCloseBody{Reader: bytes.NewReader(nil)}
	frames := make(chan transport.SSEFrameResult, 1)
	frames <- transport.SSEFrameResult{Frame: pipeline.SSEFrame{Data: "late"}}
	cli := &outboundClient{to: outboundTimeouts{FrameIdle: time.Minute, Total: time.Minute}}
	src := &outboundSource{cli: cli, body: body, status: http.StatusOK, headers: map[string]string{"Content-Type": "text/event-stream"}, totalAt: time.Now().Add(-time.Second), stop: make(chan struct{}), closeDone: make(chan struct{}), events: frames}
	_, ok, err := (&frameIter{s: src}).Next(context.Background())
	if ok || !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("Next ok=%v err=%v, want total timeout", ok, err)
	}
}

func TestOutboundChunkIteratorReturnsDoneAfterTerminal(t *testing.T) {
	for _, terminal := range []error{nil, errors.New("upstream stream broken")} {
		src := &outboundSource{dead: true, terminal: terminal}
		iter := &chunkIter{s: src}
		chunk, ok, err := iter.Next(context.Background())
		if err != nil || ok || chunk != nil {
			t.Fatalf("after terminal: chunk=%v ok=%v err=%v", chunk, ok, err)
		}
	}
}

func TestOutboundSourceTerminateUsesContextIfCloseWon(t *testing.T) {
	committed := errors.New("previous timeout")
	for _, tc := range []struct {
		name   string
		cancel bool
		want   error
	}{
		{name: "canceled context wins", cancel: true, want: context.Canceled},
		{name: "committed terminal with live context", want: committed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancel {
				cancel()
			} else {
				defer cancel()
			}
			src := &outboundSource{closed: true, dead: true, terminal: committed}
			if err := src.terminateSource(ctx, errors.New("idle timeout")); !errors.Is(err, tc.want) {
				t.Fatalf("terminal error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestOutboundChunkNextAfterClosePrefersContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := &outboundSource{cli: &outboundClient{}, closed: true, terminal: errors.New("read failed")}
	iter := &chunkIter{s: src}
	_, ok, err := iter.Next(ctx)
	if ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = (ok=%v, err=%v), want canceled context", ok, err)
	}
}

func TestOutboundNextAfterCommittedTerminalReturnsDone(t *testing.T) {
	for _, name := range []string{"chunk", "frame"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			src := &outboundSource{closed: true, dead: true, terminal: errors.New("previous read error")}
			if name == "chunk" {
				_, ok, err := (&chunkIter{s: src}).Next(ctx)
				if ok || err != nil {
					t.Fatalf("chunk Next = (ok=%v, err=%v), want done", ok, err)
				}
				return
			}
			_, ok, err := (&frameIter{s: src}).Next(ctx)
			if ok || err != nil {
				t.Fatalf("frame Next = (ok=%v, err=%v), want done", ok, err)
			}
		})
	}
}

func TestOutboundFrameNextAfterClosePrefersContextError(t *testing.T) {
	for _, name := range []string{"chunk", "frame"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			src := &outboundSource{cli: &outboundClient{}, closed: true, terminal: errors.New("read failed")}
			if name == "chunk" {
				_, ok, err := (&chunkIter{s: src}).Next(ctx)
				if ok || !errors.Is(err, context.Canceled) {
					t.Fatalf("chunk Next = (ok=%v, err=%v)", ok, err)
				}
				return
			}
			_, ok, err := (&frameIter{s: src}).Next(ctx)
			if ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("frame Next = (ok=%v, err=%v)", ok, err)
			}
		})
	}
}

func TestOutboundNextAfterCloseWithLiveContextIsTerminated(t *testing.T) {
	for _, name := range []string{"chunk", "frame"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			src := &outboundSource{cli: &outboundClient{}, closed: true, terminal: errors.New("read failed")}
			if name == "chunk" {
				_, ok, err := (&chunkIter{s: src}).Next(ctx)
				if ok || !errors.Is(err, errSourceTerminated) {
					t.Fatalf("chunk Next = (ok=%v, err=%v)", ok, err)
				}
				return
			}
			_, ok, err := (&frameIter{s: src}).Next(ctx)
			if ok || !errors.Is(err, errSourceTerminated) {
				t.Fatalf("frame Next = (ok=%v, err=%v)", ok, err)
			}
		})
	}
}

type closeErrorBody struct{ closeErr error }

func (b *closeErrorBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *closeErrorBody) Close() error             { return b.closeErr }

type rawTestTransport struct{ client *http.Client }

func (t *rawTestTransport) Name() string { return "test" }

func (t *rawTestTransport) RoundTrip(ctx context.Context, req pipeline.Request) (pipeline.TransportResponse, error) {
	hresp, err := t.RoundTripRaw(ctx, req)
	if err != nil {
		return pipeline.TransportResponse{}, err
	}
	headers := map[string]string{}
	for k := range hresp.Header {
		headers[k] = hresp.Header.Get(k)
	}
	if transport.IsSSECT(headers["Content-Type"]) {
		return pipeline.TransportResponse{Status: hresp.StatusCode, Headers: headers, Events: transport.FrameSSE(hresp.Body)}, nil
	}
	return pipeline.TransportResponse{Status: hresp.StatusCode, Headers: headers}, nil
}

func (t *rawTestTransport) RoundTripRaw(ctx context.Context, req pipeline.Request) (*http.Response, error) {
	var rd io.Reader
	if len(req.Body) > 0 {
		rd = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, rd)
	if err != nil {
		return nil, err
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	return t.client.Do(hreq)
}

// ─── 出站执行接口:四级超时/source 生命周期 ───

func testTransport() pipeline.Transport {
	return &rawTestTransport{client: &http.Client{}}
}

func newOutbound(tr pipeline.Transport) *outboundClient {
	return newOutboundClient(tr, defaultOutboundTimeouts())
}

// 场景: 上游断流 = 迭代抛错(非自然 EOF)
func TestOutboundStreamBroken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close() // 中途强断
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it, err := src.Frames()
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("frame1: %v", err)
	}
	for {
		_, ok, err := it.Next(context.Background())
		if err != nil {
			if !strings.Contains(err.Error(), "broken") {
				t.Fatalf("broken stream must error with cause: %v", err)
			}
			return
		}
		if !ok {
			t.Fatal("truncated stream must not report clean EOF")
		}
	}
}

// 场景: 显式弃置 source 即释放(无 Frames/Body 消费路径)
func TestOutboundSourceExplicitClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		time.Sleep(2 * time.Second) // 持续挂流;弃置后由连接关闭终结
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	src.Close()
	// 弃置后消费 = 终态错
	if _, err := src.Body(context.Background()); err == nil {
		t.Fatal("body after close must error")
	}
}

// 场景: 缓冲出站——返回 status/headers/body
func TestOutboundRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Probe", "yes")
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	st, hd, body, err := c.Request(context.Background(), outboundReq{
		URL: srv.URL, Method: "POST", Headers: map[string]string{"A": "b"},
	})
	if err != nil || st != 200 || string(body) != `{"ok":true}` || hd["X-Probe"] != "yes" {
		t.Fatalf("request: st=%d body=%q hd=%v err=%v", st, body, hd, err)
	}
}

// 场景: 出站四级超时——单次出站超 totalTimeoutMs 可 catch,不污染 client
func TestOutboundTotalTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, "late")
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: 30 * time.Second, Response: 60 * time.Second, FrameIdle: 180 * time.Second, Total: 50 * time.Millisecond})
	_, _, _, err := c.Request(context.Background(), outboundReq{URL: srv.URL, Method: "GET"})
	if err == nil || !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("total timeout: %v", err)
	}
	// 实例未污染:恢复正常设置后续接成功
	c.overwrite(defaultOutboundTimeouts())
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	defer srv2.Close()
	if _, _, _, err := c.Request(context.Background(), outboundReq{URL: srv2.URL, Method: "GET"}); err != nil {
		t.Fatalf("after timeout reuse: %v", err)
	}
}

// 场景: responseTimeoutMs——对方迟迟无任何返回
func TestOutboundResponseTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		fmt.Fprint(w, "late")
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: 30 * time.Second, Response: 50 * time.Millisecond, FrameIdle: time.Second, Total: time.Minute})
	if _, _, _, err := c.Request(context.Background(), outboundReq{URL: srv.URL, Method: "GET"}); !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("response timeout: %v", err)
	}
}

// 场景: connect/response 调用时定死——中途 overwrite 不影响已计时调用
func TestOutboundTimeoutsFixedAtCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		fmt.Fprint(w, "late")
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: 50 * time.Millisecond, FrameIdle: time.Second, Total: time.Minute})
	done := make(chan error, 1)
	go func() {
		_, _, _, err := c.Request(context.Background(), outboundReq{URL: srv.URL, Method: "GET"})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Minute, FrameIdle: time.Second, Total: time.Minute})
	if err := <-done; !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("mid-call overwrite must not extend: %v", err)
	}
}

// 场景: open 流式出站 + frames EOF 自然结束
func TestOutboundOpenFramesEOF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\ndata: b\n\n")
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it, err := src.Frames()
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	var got []string
	for {
		f, ok, err := it.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, f.Data)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("frames: %v", got)
	}
}

// 场景: 非 event-stream 上游 frames() 抛错,改走 body()
func TestOutboundFramesOnNonSSE(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"json":"not-sse"}`)
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := src.Frames(); err == nil || !strings.Contains(err.Error(), "event-stream") {
		t.Fatalf("frames on non-sse must error: %v", err)
	}
	body, err := src.Body(context.Background())
	if err != nil || string(body) != `{"json":"not-sse"}` {
		t.Fatalf("body fallback: %q %v", body, err)
	}
}

// 场景: frames 混用——重复 frames/body 后 frames/超时后迭代器终态且 source 立即关闭
func TestOutboundSourceSingleConsume(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-block
	}))
	defer srv.Close()
	defer close(block)

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it, err := src.Frames()
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	// 重复消费 = 抛错(frames 后 body)
	if _, err := src.Body(context.Background()); err == nil {
		t.Fatal("body after frames must error")
	}
	// 帧间超时:迭代器终态 + 连接立即关闭
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Second, FrameIdle: 30 * time.Millisecond, Total: time.Minute})
	_, _, err = it.Next(context.Background())
	if err == nil || !errors.Is(err, errOutboundTimeout) {
		t.Fatalf("frame idle timeout: %v", err)
	}
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("next after committed terminal should be done: %v", err)
	}
}

// 场景: frameIdle 逐帧隙重武装读当前设置
func TestOutboundFrameIdleReArm(t *testing.T) {
	var release chan struct{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-release
		fmt.Fprint(w, "data: b\n\n")
	}))
	defer srv.Close()
	var releaseOnce sync.Once
	release = make(chan struct{})
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo() // 兜底:失败路径解救 handler goroutine

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Second, FrameIdle: 80 * time.Millisecond, Total: time.Minute})
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it, err := src.Frames()
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("frame1: %v", err)
	}
	// 帧隙中调大:下一帧隙按新值计时(旧值 80ms 内第二帧不会到,调大后等待成功)
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Second, FrameIdle: 3 * time.Second, Total: time.Minute})
	letGo() // 放行第二帧
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("frame2 with re-armed idle: %v", err)
	}
}

// 场景: frameIdle 与 total 同时到期优先报 total
func TestOutboundTotalPriority(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-block
	}))
	defer srv.Close()
	defer close(block)

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Second, FrameIdle: 30 * time.Millisecond, Total: 60 * time.Millisecond})
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	it, err := src.Frames()
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	if _, _, err := it.Next(context.Background()); err != nil {
		t.Fatalf("frame1: %v", err)
	}
	// 越过 total 终界后帧隙到期:idle 计时先满但按定序报 total
	time.Sleep(80 * time.Millisecond)
	_, _, err = it.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "total") {
		t.Fatalf("total priority: %v", err)
	}
}

// 场景: status ≥ 300 —— body() 一次取回并关闭,frames() 抛错
func TestOutboundNon2xxStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":"rate"}`)
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	src, err := c.Open(context.Background(), outboundReq{URL: srv.URL, Method: "GET", Stream: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if src.status != 429 {
		t.Fatalf("status: %d", src.status)
	}
	body, err := src.Body(context.Background())
	if err != nil || string(body) != `{"error":"rate"}` {
		t.Fatalf("body: %q %v", body, err)
	}
	if _, err := src.Frames(); err == nil {
		t.Fatal("frames after body must error")
	}
}

// 场景: body() 无大小上限(超 32MB 限量仍完整)
func TestOutboundBodyUnbounded(t *testing.T) {
	big := strings.Repeat("x", 33*1024*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, big)
	}))
	defer srv.Close()

	c := newOutbound(testTransport())
	c.overwrite(outboundTimeouts{Connect: time.Second, Response: time.Minute, FrameIdle: time.Second, Total: time.Minute})
	_, _, body, err := c.Request(context.Background(), outboundReq{URL: srv.URL, Method: "GET"})
	if err != nil || len(body) != len(big) {
		t.Fatalf("unbounded body: len=%d err=%v", len(body), err)
	}
}

// handler 出站执行接口:直消费 pipeline.Transport 原始响应,四级超时双通道(单次出站可 catch,不污染执行)
package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/streamct"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
)

// errOutboundTimeout 单次出站超时(可 catch;执行级取消另走 ctx)
var errOutboundTimeout = errors.New("outbound timeout")

// errSourceTerminated 迭代器终态错
var errSourceTerminated = errors.New("source terminated")

// outboundTimeouts 四级出站超时(执行级可覆写,合并语义)
type outboundTimeouts struct {
	Connect   time.Duration
	Response  time.Duration
	FrameIdle time.Duration
	Total     time.Duration
}

// defaultOutboundTimeouts 缺省面
func defaultOutboundTimeouts() outboundTimeouts {
	return outboundTimeouts{
		Connect:   30 * time.Second,
		Response:  60 * time.Second,
		FrameIdle: 3 * time.Minute,
		Total:     10 * time.Minute,
	}
}

// overwrite 合并覆写(零值成员保持原值)
func (t *outboundTimeouts) overwrite(nv outboundTimeouts) {
	if nv.Connect > 0 {
		t.Connect = nv.Connect
	}
	if nv.Response > 0 {
		t.Response = nv.Response
	}
	if nv.FrameIdle > 0 {
		t.FrameIdle = nv.FrameIdle
	}
	if nv.Total > 0 {
		t.Total = nv.Total
	}
}

// outboundReq 单次出站请求(JS 对象导出面在 handler.go)
type outboundReq struct {
	URL        string
	Method     string
	Headers    map[string]string
	Body       []byte
	SessionKey string
	TimeoutMs  int64 // 本次调用覆写 totalTimeoutMs
	Stream     bool
}

// outboundClient 单执行出站通道(settings 状态归一次执行,实例随执行生灭)
type outboundClient struct {
	tr pipeline.Transport

	mu sync.Mutex
	to outboundTimeouts
}

// newOutboundClient 构造(初始超时面 = 执行级设置)
func newOutboundClient(tr pipeline.Transport, to outboundTimeouts) *outboundClient {
	return &outboundClient{tr: tr, to: to}
}

// overwrite 执行级超时覆写(JS settings.overwrite 通道)
func (c *outboundClient) overwrite(nv outboundTimeouts) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.to.overwrite(nv)
}

// callTimeouts 调用时定死快照(中途覆写不影响已计时调用)
func (c *outboundClient) callTimeouts() outboundTimeouts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.to
}

// Request 缓冲出站(status, headers, body)
func (c *outboundClient) Request(ctx context.Context, r outboundReq) (int, map[string]string, []byte, error) {
	src, err := c.roundTrip(ctx, r)
	if err != nil {
		return 0, nil, nil, err
	}
	defer src.close()
	body, err := readUnbounded(ctx, src)
	if err != nil {
		return 0, nil, nil, err
	}
	return src.status, src.headers, body, nil
}

// Open 流式出站(stream 必须显式 true;缓冲出站走 Request)
func (c *outboundClient) Open(ctx context.Context, r outboundReq) (*outboundSource, error) {
	if !r.Stream {
		return nil, fmt.Errorf("http.open requires stream:true (buffered outbound uses http.request)")
	}
	return c.roundTrip(ctx, r)
}

// roundTrip 建连+首字节阶段按调用时设置计时;per-call ctx 按 total 界,存活至 source 关闭
func (c *outboundClient) roundTrip(ctx context.Context, r outboundReq) (*outboundSource, error) {
	raw, ok := c.tr.(transport.RawTransport)
	if !ok {
		return nil, fmt.Errorf("transport %q does not support raw outbound", c.tr.Name())
	}
	to := c.callTimeouts()
	total := to.Total
	if r.TimeoutMs > 0 {
		total = time.Duration(r.TimeoutMs) * time.Millisecond
	}
	start := time.Now()
	reqCtx, cancel := context.WithTimeout(ctx, total)
	// 建连与响应在 Transport 抽象下不可分,首字节前阶段取更紧者界定(竞争计时,到期掐断 reqCtx)
	stage1 := to.Response
	if to.Connect < stage1 {
		stage1 = to.Connect
	}
	stageTimer := time.NewTimer(stage1)
	defer stageTimer.Stop()

	type rawResult struct {
		resp *http.Response
		err  error
	}
	resCh := make(chan rawResult, 1)
	go func() {
		resp, err := raw.RoundTripRaw(reqCtx, pipeline.Request{URL: r.URL, Method: r.Method, Headers: r.Headers, Body: r.Body, APIKey: r.SessionKey})
		resCh <- rawResult{resp, err}
	}()
	var hresp *http.Response
	var err error
	stageFired := false
	select {
	case res := <-resCh:
		hresp, err = res.resp, res.err
	case <-stageTimer.C:
		stageFired = true
		cancel()
		// 回收 RoundTrip goroutine;实现不响应 ctx 时以执行 ctx 兜底放弃
		select {
		case <-resCh:
		case <-ctx.Done():
		}
		hresp, err = nil, fmt.Errorf("%w: connect/responseTimeoutMs exceeded", errOutboundTimeout)
	}
	if err != nil {
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !stageFired && reqCtx.Err() != nil && errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout)
		}
		return nil, err
	}
	headers := map[string]string{}
	for k := range hresp.Header {
		headers[k] = hresp.Header.Get(k)
	}
	return &outboundSource{
		cli:       c,
		status:    hresp.StatusCode,
		headers:   headers,
		body:      hresp.Body,
		req:       r,
		totalAt:   start.Add(total),
		cancel:    cancel,
		stop:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}, nil
}

// readUnbounded 全量无界读(total 界)
func readUnbounded(ctx context.Context, src *outboundSource) ([]byte, error) {
	rctx, cancel := context.WithDeadline(ctx, src.totalAt)
	defer cancel()
	data, err := io.ReadAll(&ctxReader{r: src.body, ctx: rctx})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if rctx.Err() != nil {
			return nil, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout)
		}
		return nil, err
	}
	return data, nil
}

// ctxReader 读取随 ctx 中止
type ctxReader struct {
	r   io.Reader
	ctx context.Context
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// outboundSource 流式/非流式上游 source(单次消费语义)
type outboundSource struct {
	cli     *outboundClient
	status  int
	headers map[string]string
	body    io.ReadCloser
	req     outboundReq
	totalAt time.Time
	events  <-chan transport.SSEFrameResult
	chunks  <-chan transport.ChunkResult
	cancel  context.CancelFunc
	stop    chan struct{}

	mu        sync.Mutex
	consumed  bool
	dead      bool
	closed    bool
	terminal  error
	closeDone chan struct{}
	closeErr  error
}

// markConsume 单次消费仲裁
func (s *outboundSource) markConsume() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimConsume()
}

// claimConsume 消费仲裁(调用方已持 mu)
func (s *outboundSource) claimConsume() error {
	if s.dead || s.closed {
		return errSourceTerminated
	}
	if s.consumed {
		return fmt.Errorf("source already consumed (frames/body are single-use)")
	}
	s.consumed = true
	return nil
}

func (s *outboundSource) close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	close(s.stop)
	s.mu.Unlock()

	defer close(s.closeDone)
	defer func() {
		if s.cancel != nil {
			s.cancel()
		}
	}()
	closeErr := s.body.Close()
	s.mu.Lock()
	s.closeErr = closeErr
	s.mu.Unlock()
	return closeErr
}

// Close 显式弃置(消费者既不 Frames 也不 Body 的丢弃路径;终局兜底也走此出口)
func (s *outboundSource) Close() { _ = s.close() }

// Frames 领取帧迭代器(status ≥ 300 或非 event-stream 上游抛错;单次)
func (s *outboundSource) Frames() (*frameIter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status >= 300 {
		return nil, fmt.Errorf("frames() unavailable on status %d (consume via body())", s.status)
	}
	if !isEventStreamHeader(s.headers["Content-Type"]) {
		return nil, fmt.Errorf("frames() requires event-stream upstream (use body(), got Content-Type %q)", s.headers["Content-Type"])
	}
	if err := s.claimConsume(); err != nil {
		return nil, err
	}
	return &frameIter{s: s}, nil
}

// Body 缓冲读全量并关闭(无大小上限);非 event-stream 上游与 status ≥ 300 的规范出口
func (s *outboundSource) Body(ctx context.Context) ([]byte, error) {
	if err := s.markConsume(); err != nil {
		return nil, err
	}
	defer s.close()
	return readUnbounded(ctx, s)
}

// Chunks 领取原始字节迭代器(status ≥ 300 或非流式 Content-Type 拒绝;单次)
func (s *outboundSource) Chunks() (*chunkIter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status >= 300 {
		return nil, fmt.Errorf("chunks() unavailable on status %d", s.status)
	}
	if !streamct.IsStream(s.headers["Content-Type"]) {
		return nil, fmt.Errorf("chunks() requires SSE or EventStream Content-Type, got %q", s.headers["Content-Type"])
	}
	if err := s.claimConsume(); err != nil {
		return nil, err
	}
	s.chunks = transport.RawChunks(s.body, s.stop)
	return &chunkIter{s: s}, nil
}

// BodyBytes 缓冲读取原始响应字节并关闭 source。
func (s *outboundSource) BodyBytes(ctx context.Context) ([]byte, error) {
	if err := s.markConsume(); err != nil {
		return nil, err
	}
	defer s.close()
	return readUnbounded(ctx, s)
}

// chunkIter 逐次等待无损字节块。
type chunkIter struct{ s *outboundSource }

func (it *chunkIter) Next(ctx context.Context) ([]byte, bool, error) {
	s := it.s
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return nil, false, nil
	}
	if s.closed {
		ctxErr := ctx.Err()
		s.mu.Unlock()
		if ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, false, errSourceTerminated
	}
	chunks := s.chunks
	s.mu.Unlock()
	to := s.cli.callTimeouts()
	idleAt := time.Now().Add(to.FrameIdle)
	idle := time.NewTimer(time.Until(idleAt))
	total := time.NewTimer(time.Until(s.totalAt))
	defer idle.Stop()
	defer total.Stop()
	if timeoutErr := streamDeadlineError(ctx, s.totalAt, idleAt, time.Now()); timeoutErr != nil {
		return nil, false, s.terminateSource(ctx, timeoutErr)
	}
	select {
	case item, ok := <-chunks:
		s.mu.Lock()
		if s.closed {
			ctxErr := ctx.Err()
			terminal := s.terminal
			s.mu.Unlock()
			if ctxErr != nil {
				return nil, false, ctxErr
			}
			if terminal != nil {
				return nil, false, terminal
			}
			return nil, false, errSourceTerminated
		}
		if s.dead {
			s.mu.Unlock()
			return nil, false, nil
		}
		if timeoutErr := streamDeadlineError(ctx, s.totalAt, idleAt, time.Now()); timeoutErr != nil {
			s.dead = true
			s.terminal = timeoutErr
			s.mu.Unlock()
			_ = s.close()
			return nil, false, timeoutErr
		}
		if !ok {
			s.dead = true
			s.mu.Unlock()
			_ = s.close()
			return nil, false, nil
		}
		if item.Err != nil {
			mapped := mapSourceReadError(ctx, item.Err)
			s.dead = true
			s.terminal = mapped
			s.mu.Unlock()
			_ = s.close()
			return nil, false, mapped
		}
		data := item.Data
		s.mu.Unlock()
		return data, true, nil
	case <-ctx.Done():
		return nil, false, s.terminateSource(ctx, ctx.Err())
	case <-total.C:
		return nil, false, s.terminateSource(ctx, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout))
	case <-idle.C:
		if !time.Now().Before(s.totalAt) {
			return nil, false, s.terminateSource(ctx, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout))
		}
		return nil, false, s.terminateSource(ctx, fmt.Errorf("%w: frameIdleTimeoutMs exceeded", errOutboundTimeout))
	}
}

func streamDeadlineError(ctx context.Context, totalAt, idleAt, now time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !now.Before(totalAt) {
		return fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout)
	}
	if !now.Before(idleAt) {
		return fmt.Errorf("%w: frameIdleTimeoutMs exceeded", errOutboundTimeout)
	}
	return nil
}

func mapSourceReadError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout)
	}
	return fmt.Errorf("upstream stream broken: %w", err)
}

func (s *outboundSource) terminateSource(ctx context.Context, err error) error {
	s.mu.Lock()
	if s.dead || s.closed {
		ctxErr := ctx.Err()
		committed := s.terminal
		s.mu.Unlock()
		if ctxErr != nil {
			return ctxErr
		}
		if committed != nil {
			return committed
		}
		return errSourceTerminated
	}
	s.dead = true
	s.terminal = err
	s.mu.Unlock()
	_ = s.close()
	return err
}

// frameIter 帧迭代器(Frames() 一次性领取消费权;迭代内多次 Next 合法)
type frameIter struct{ s *outboundSource }

// 返回 (帧, 是否仍有帧, 错误)
func (it *frameIter) Next(ctx context.Context) (pipeline.SSEFrame, bool, error) {
	s := it.s
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return pipeline.SSEFrame{}, false, nil
	}
	if s.closed {
		ctxErr := ctx.Err()
		s.mu.Unlock()
		if ctxErr != nil {
			return pipeline.SSEFrame{}, false, ctxErr
		}
		return pipeline.SSEFrame{}, false, errSourceTerminated
	}
	if s.events == nil {
		s.events = transport.FrameSSEResult(s.body, s.stop)
	}
	s.mu.Unlock()

	to := s.cli.callTimeouts()
	idleAt := time.Now().Add(to.FrameIdle)
	idle := time.NewTimer(time.Until(idleAt))
	total := time.NewTimer(time.Until(s.totalAt))
	defer total.Stop()
	defer idle.Stop()
	if timeoutErr := streamDeadlineError(ctx, s.totalAt, idleAt, time.Now()); timeoutErr != nil {
		return pipeline.SSEFrame{}, false, s.terminateSource(ctx, timeoutErr)
	}
	select {
	case res, ok := <-s.events:
		s.mu.Lock()
		if s.closed {
			ctxErr := ctx.Err()
			terminal := s.terminal
			s.mu.Unlock()
			if ctxErr != nil {
				return pipeline.SSEFrame{}, false, ctxErr
			}
			if terminal != nil {
				return pipeline.SSEFrame{}, false, terminal
			}
			return pipeline.SSEFrame{}, false, errSourceTerminated
		}
		if s.dead {
			s.mu.Unlock()
			return pipeline.SSEFrame{}, false, nil
		}
		if timeoutErr := streamDeadlineError(ctx, s.totalAt, idleAt, time.Now()); timeoutErr != nil {
			s.dead = true
			s.terminal = timeoutErr
			s.mu.Unlock()
			_ = s.close()
			return pipeline.SSEFrame{}, false, timeoutErr
		}
		if !ok {
			s.dead = true
			s.mu.Unlock()
			_ = s.close()
			return pipeline.SSEFrame{}, false, nil
		}
		if res.Err != nil {
			mapped := mapSourceReadError(ctx, res.Err)
			s.dead = true
			s.terminal = mapped
			s.mu.Unlock()
			_ = s.close()
			return pipeline.SSEFrame{}, false, mapped
		}
		if s.closed {
			ctxErr := ctx.Err()
			terminal := s.terminal
			s.mu.Unlock()
			if ctxErr != nil {
				return pipeline.SSEFrame{}, false, ctxErr
			}
			if terminal != nil {
				return pipeline.SSEFrame{}, false, terminal
			}
			return pipeline.SSEFrame{}, false, errSourceTerminated
		}
		if s.dead {
			s.mu.Unlock()
			return pipeline.SSEFrame{}, false, nil
		}
		if timeoutErr := streamDeadlineError(ctx, s.totalAt, idleAt, time.Now()); timeoutErr != nil {
			s.dead = true
			s.terminal = timeoutErr
			s.mu.Unlock()
			_ = s.close()
			return pipeline.SSEFrame{}, false, timeoutErr
		}
		s.mu.Unlock()
		return res.Frame, true, nil
	case <-ctx.Done():
		return pipeline.SSEFrame{}, false, s.terminateSource(ctx, ctx.Err())
	case <-total.C:
		return pipeline.SSEFrame{}, false, s.terminateSource(ctx, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout))
	case <-idle.C:
		if !time.Now().Before(s.totalAt) {
			return pipeline.SSEFrame{}, false, s.terminateSource(ctx, fmt.Errorf("%w: totalTimeoutMs exceeded", errOutboundTimeout))
		}
		return pipeline.SSEFrame{}, false, s.terminateSource(ctx, fmt.Errorf("%w: frameIdleTimeoutMs exceeded", errOutboundTimeout))
	}
}

// terminate 终态:标记 dead + 立即关闭连接(不等终局兜底)
func (s *outboundSource) terminate() {
	s.mu.Lock()
	s.dead = true
	s.mu.Unlock()
	s.close()
}

func isEventStreamHeader(ct string) bool {
	return transport.IsSSECT(ct)
}

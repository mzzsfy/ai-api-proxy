// gateway handler 分流:parts.handler 包请求不经 pipeline,直入 handler 执行器
package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// handlerSink HandlerReplySink 实现(直写 http.ResponseWriter;流式边执行边写)
type handlerSink struct {
	w           http.ResponseWriter
	flusher     http.Flusher
	framer      convert.Framer
	status      int
	wroteHeader bool // 响应已提交(header 写出;与"流已声明"分离)
}

func (s *handlerSink) JSON(status int, body string) error {
	s.status = status
	s.w.Header().Set("Content-Type", "application/json")
	s.w.WriteHeader(status)
	_, err := s.w.Write([]byte(body))
	return err
}

func (s *handlerSink) SSEStart() error {
	flusher, ok := s.w.(http.Flusher)
	if !ok {
		return errors.New("stream unsupported")
	}
	s.flusher = flusher
	s.w.Header().Set("Content-Type", "text/event-stream")
	s.w.Header().Set("Cache-Control", "no-cache")
	s.w.WriteHeader(http.StatusOK)
	s.wroteHeader = true
	flusher.Flush()
	return nil
}

// Event 入口 Framer 编码分帧(handler event payload = 入口协议 chunk JSON)
func (s *handlerSink) Event(evtJSON string) error {
	if !s.wroteHeader || s.flusher == nil {
		return errors.New("http.event before sse start")
	}
	for _, e := range s.framer.Frame([]byte(evtJSON)) {
		writeSSE(s.w, sseEventOut{event: e.Event, data: e.Data})
	}
	s.flusher.Flush()
	return nil
}

func (s *handlerSink) End() error {
	if !s.wroteHeader || s.flusher == nil {
		return nil
	}
	for _, e := range s.framer.Flush() {
		writeSSE(s.w, sseEventOut{event: e.Event, data: e.Data})
	}
	s.flusher.Flush()
	return nil
}

// ErrorFrame 规范错误帧:原样 data 行(不经入口 Framer,与协议错误路径分叉;已定案)
func (s *handlerSink) ErrorFrame(evtJSON string) error {
	if s.wroteHeader && s.flusher != nil {
		writeSSE(s.w, sseEventOut{data: evtJSON})
		s.flusher.Flush()
		return nil
	}
	// 流未开始:防御式 JSON 502
	return s.JSON(http.StatusBadGateway, evtJSON)
}

// handlerInputFor 合成 HandlerInput(键/参数/传输槽;无键 = 502 不进 handler)
func (g *Gateway) handlerInputFor(grp *upstream.Group, model string, entryStream bool, body []byte, r *http.Request) (plugin.HandlerInput, error) {
	key := g.pickKey(grp.Plugin)
	if key == nil {
		return plugin.HandlerInput{}, fmt.Errorf("no key available for package %s", grp.Plugin)
	}
	params, err := g.Registry.MergedParams(grp)
	if err != nil {
		return plugin.HandlerInput{}, err
	}
	transportSlot := ""
	if s, ok := params["transport"].(string); ok {
		transportSlot = s
	}
	return plugin.HandlerInput{
		Model: model, Stream: entryStream,
		Headers: headersFromBody(r.Header),
		Body: string(body), Key: *key, Params: params,
		Transport: transportSlot, SessionKey: model,
	}, nil
}

// hopHeaders 逐跳头(handler ctx.request.headers 只读视图剔除)
var hopHeaders = map[string]bool{"Host": true, "Content-Length": true, "Connection": true,
	"Accept-Encoding": true, "Transfer-Encoding": true, "Keep-Alive": true, "Proxy-Connection": true,
	"Upgrade": true, "Te": true, "Trailer": true}

// headersFromBody 请求头透传(handler ctx.request.headers 只读视图)
func headersFromBody(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) > 0 && !hopHeaders[k] {
			out[k] = vs[0]
		}
	}
	return out
}

// serveHandler 请求路径分流入口(Execute 后回复已写完;历史由 serve defer 记录)
func (g *Gateway) serveHandler(w http.ResponseWriter, r *http.Request, entry Entry, cand *upstream.Candidate, hp *plugin.Package, body []byte, model string, entryStream bool) {
	eng, err := g.Handlers.For(hp)
	if err != nil {
		writeError(w, entry, http.StatusInternalServerError, err.Error())
		return
	}
	in, err := g.handlerInputFor(cand.Group, model, entryStream, body, r)
	if err != nil {
		writeError(w, entry, http.StatusBadGateway, err.Error())
		return
	}
	sink := &handlerSink{w: w, framer: entry.Framer()}
	if err := eng.Execute(r.Context(), in, sink); err != nil {
		// 响应未提交才补错误(已提交 = 错误已分叉,零补偿)
		if !sink.wroteHeader && sink.status == 0 {
			writeError(w, entry, http.StatusBadGateway, err.Error())
		}
	}
}

// runViaHandler TestUpstream/ChatTest 分流:合成最小非流式请求跑一次 handler(空壳必败,不能照旧)
func (g *Gateway) runViaHandler(ctx context.Context, grp *upstream.Group, hp *plugin.Package, memberName, message string) (int64, int, []byte, string) {
	eng, err := g.Handlers.For(hp)
	if err != nil {
		return 0, 0, nil, err.Error()
	}
	raw, err := chatBody(hp.Manifest.Parts.Handler, memberName, message)
	if err != nil {
		return 0, 0, nil, "marshal test body: " + err.Error()
	}
	in, err := g.handlerInputFor(grp, memberName, false, raw, &http.Request{Header: http.Header{}})
	if err != nil {
		return 0, 0, nil, err.Error()
	}
	sink := &testSink{}
	start := time.Now()
	execErr := eng.Execute(ctx, in, sink)
	latency := time.Since(start).Milliseconds()
	if execErr != nil {
		return latency, sink.status, []byte(sink.body), execErr.Error()
	}
	if sink.status >= 300 {
		return latency, sink.status, []byte(sink.body), fmt.Sprintf("upstream status %d", sink.status)
	}
	return latency, sink.status, []byte(sink.body), ""
}

// testSink 缓冲应答(诊断路径)
type testSink struct {
	status int
	body   string
}

func (s *testSink) JSON(status int, body string) error {
	s.status, s.body = status, body
	return nil
}
func (s *testSink) SSEStart() error { return errors.New("test path is non-streaming") }
func (s *testSink) Event(string) error {
	return errors.New("test path is non-streaming")
}
func (s *testSink) End() error   { return nil }
func (s *testSink) ErrorFrame(e string) error {
	s.status, s.body = http.StatusBadGateway, e
	return nil
}

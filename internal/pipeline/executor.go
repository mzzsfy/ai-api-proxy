package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// Resolved 上游解析产物(Resolve 组装:extras 引用序 → base 包内序)
type Resolved struct {
	Filters  []Filter
	Protocol Protocol
	Targets  []Target
}

// BuildError 请求构造/传输失败(本地或部件原因;无上游状态,502)
type BuildError struct{ Err error }

func (e *BuildError) Error() string { return e.Err.Error() }
func (e *BuildError) Unwrap() error { return e.Err }

// Executor 单目标直发引擎:修改链 → BuildRequest → 一次 RoundTrip → 响应映射。
// 不重试、不切目标、不熔断——失败即终局,上游状态原样透传(重试归下游)
type Executor struct {
	Transports func(name string) (Transport, bool)
	// OnTargetExit 目标尝试结束回调(failed=非 2xx/3xx 或内部错误;metrics 用,可空)
	OnTargetExit func(upstream, target string, failed bool)
}

// Run 执行模型:①修改链 ②单目标直发 ③响应映射
func (e *Executor) Run(ctx context.Context, pctx *PipelineContext, u Resolved, pivotReq []byte) (*Response, error) {
	// ① 修改链:extras(引用序)→ base(包内序)已由 Resolve 排好
	pivot := pivotReq
	for _, f := range u.Filters {
		out, err := f.MapRequest(pctx, pivot)
		if err != nil {
			return nil, &BuildError{Err: &MapRequestError{Part: f.Name(), Err: err}}
		}
		if out != nil {
			pivot = out
		}
	}
	// ② 单目标:首个目标(Routing 多目标次序留给未来策略;失败不切换)
	if len(u.Targets) == 0 {
		return nil, &BuildError{Err: errors.New("no enabled targets")}
	}
	target := u.Targets[0]
	pctx.Target = target
	resp, req, failed, err := e.tryTarget(ctx, pctx, u, target, pivot)
	if e.OnTargetExit != nil {
		e.OnTargetExit(pctx.Upstream.Name, target.Name, failed)
	}
	if err != nil {
		return nil, err
	}
	// ③ 响应映射(状态≥300 = 错误终局:mapError 一次或透传)
	if resp.Status >= 300 {
		return terminalResponse(pctx, u, resp.Status, resp.Body), nil
	}
	return e.mapResponse(pctx, u, req.Stream, resp)
}

// tryTarget 单次请求:BuildRequest → RoundTrip;无重试语义
func (e *Executor) tryTarget(ctx context.Context, pctx *PipelineContext, u Resolved, target Target, pivot []byte) (tresp *TransportResponse, req Request, failed bool, err error) {
	req, err = u.Protocol.BuildRequest(pctx, pivot)
	if err != nil {
		if errors.Is(err, ErrPoolBusy) {
			return nil, req, true, err // 池耗尽:原样上抛(503)
		}
		return nil, req, true, &BuildError{Err: fmt.Errorf("buildRequest: %w", err)}
	}
	tr, ok := e.Transports(targetTransport(target))
	if !ok {
		return nil, req, true, &BuildError{Err: fmt.Errorf("transport %q not found", targetTransport(target))}
	}
	out, terr := tr.RoundTrip(ctx, req)
	if terr != nil {
		if errors.Is(terr, ErrPoolBusy) || errors.Is(terr, ipprovider.ErrNoExits) {
			return nil, req, true, terr // 池忙 503 / 无出口 503(host 语义层)
		}
		return nil, req, true, &BuildError{Err: fmt.Errorf("round trip: %w", terr)}
	}
	return &out, req, out.Status >= 400, nil
}

// MapRequestError 修改链失败(502,带部件名)
type MapRequestError struct {
	Part string
	Err  error
}

func (e *MapRequestError) Error() string {
	return fmt.Sprintf("filter %s: %v", e.Part, e.Err)
}

// terminalResponse 错误终局:mapError(一次)或原样透传
func terminalResponse(pctx *PipelineContext, u Resolved, status int, body []byte) *Response {
	if em, ok := u.Protocol.(ErrorMapper); ok {
		if out, err := em.MapError(pctx, status, body); err == nil && out != nil {
			return &Response{Stream: false, Body: out, Status: status}
		}
	}
	return &Response{Stream: false, Body: body, Status: status}
}

// mapResponse 响应映射:流式逐帧 MapEvent → filters 逆序 MapChunk;非流式 MapResponse → filters 逆序
func (e *Executor) mapResponse(pctx *PipelineContext, u Resolved, intentStream bool, tresp *TransportResponse) (*Response, error) {
	stream, warn := decideStream(u.Protocol, intentStream, tresp)
	if warn {
		log.Printf("stream dispatch: flag/ct mismatch, processing as actual (%s)", tresp.Headers["Content-Type"])
	}
	if !stream {
		body, err := u.Protocol.MapResponse(pctx, tresp.Body)
		if err != nil {
			return nil, &BuildError{Err: fmt.Errorf("protocol mapResponse: %w", err)}
		}
		for i := len(u.Filters) - 1; i >= 0; i-- {
			out, err := u.Filters[i].MapResponse(pctx, body)
			if err != nil {
				return nil, &BuildError{Err: fmt.Errorf("filter %s mapResponse: %w", u.Filters[i].Name(), err)}
			}
			if out != nil {
				body = out
			}
		}
		return &Response{Stream: false, Body: body, Status: tresp.Status}, nil
	}
	// 流式:MapEvent 逐帧(null=跳帧);失败降级 raw 帧(不丢帧,流不可回滚)
	out := make(chan ChunkItem)
	go func() {
		defer close(out)
		for frame := range tresp.Events {
			payload, err := u.Protocol.MapEvent(pctx, sseFrameJSON(frame))
			if err != nil {
				if errors.Is(err, ErrPoolBusy) {
					out <- ChunkItem{JSON: sseFrameJSON(frame), RawPass: true}
					continue
				}
				out <- ChunkItem{JSON: sseFrameJSON(frame), RawPass: true}
				continue
			}
			if payload == nil {
				continue // 跳帧
			}
			chunk := payload
			pre := payload
			for i := len(u.Filters) - 1; i >= 0; i-- {
				mc, err := u.Filters[i].MapChunk(pctx, chunk)
				if err != nil {
					// 降级:进入修改链前的 chunk 原样透传(不丢帧)
					log.Printf("filter %s mapChunk failed: %v", u.Filters[i].Name(), err)
					out <- ChunkItem{JSON: pre, RawPass: true}
					chunk = nil
					break
				}
				if mc != nil {
					chunk = mc
				}
			}
			if chunk != nil {
				out <- ChunkItem{JSON: chunk}
			}
		}
	}()
	return &Response{Stream: true, Chunks: out, Status: tresp.Status}, nil
}

// decideStream 分发:单声明按 form 固定;双声明按请求 stream 意图(BuildRequest 标志),
// 实际响应 Content-Type 兜底校验,不符按实际处理并告警
func decideStream(p Protocol, intentStream bool, tresp *TransportResponse) (stream, warn bool) {
	s := p.Supports()
	streaming := s.HasForm("streaming")
	nonStreaming := s.HasForm("non_streaming")
	if streaming && !nonStreaming {
		return true, false
	}
	if nonStreaming && !streaming {
		return false, false
	}
	ct := tresp.Headers["Content-Type"]
	actual := containsStreamCT(ct)
	if ct != "" && actual != intentStream {
		return actual, true // 标志与实际不符:按实际
	}
	return intentStream, false
}

// containsStreamCT Content-Type 是否事件流
func containsStreamCT(ct string) bool {
	for _, part := range splitSemi(ct) {
		if trimSpace(part) == "text/event-stream" {
			return true
		}
	}
	return false
}

func splitSemi(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ';' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// targetTransport 目标传输名(默认 direct)
func targetTransport(t Target) string {
	if t.Transport == "" {
		return TransportRef
	}
	return t.Transport
}

// sseFrameJSON 帧 → mapEvent 入参形态 {"event": string, "data": string}
func sseFrameJSON(f SSEFrame) []byte {
	b, _ := json.Marshal(map[string]string{"event": f.Event, "data": f.Data})
	return b
}

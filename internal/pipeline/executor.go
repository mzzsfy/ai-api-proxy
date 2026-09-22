package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// Resolved 模型行解析产物(过滤器串行 + 协议适配;v2 无目标概念)
type Resolved struct {
	Filters  []Filter
	Protocol Protocol
}

// BuildError 请求构造/传输失败(本地或部件原因;无上游状态,502)
type BuildError struct{ Err error }

func (e *BuildError) Error() string { return e.Err.Error() }
func (e *BuildError) Unwrap() error { return e.Err }

// Executor 单路直发引擎:修改链 → BuildRequest → 一次 RoundTrip → 响应映射。
// 不重试、不熔断——失败即终局,上游状态原样透传(重试归下游)
type Executor struct {
	Transports func(name string) (Transport, bool)
	// OnDone 模型请求结束回调(failed=非 2xx/3xx 或内部错误;metrics 用,可空)
	OnDone func(upstream string, failed bool)
}

// Run 执行模型:①修改链 ②直发 ③响应映射。entryReq = 入口协议请求体(声明协议与之恒等)
func (e *Executor) Run(ctx context.Context, pctx *PipelineContext, u Resolved, entryReq []byte) (*Response, error) {
	// 入口形态把关:声明未覆盖 → 拒绝,不调用部件(不做任何转换)
	if err := guardForm(u.Protocol, pctx.Vars.EntryStream); err != nil {
		return nil, err
	}
	// ① 修改链:包内序已由 Resolve 排好
	entry := entryReq
	for _, f := range u.Filters {
		out, err := f.MapRequest(pctx, entry)
		if err != nil {
			return nil, &BuildError{Err: &MapRequestError{Part: f.Name(), Err: err}}
		}
		if out != nil {
			entry = out
		}
	}
	// ② 直发:传输由请求载体的 transport 槽决定(空 = direct)
	resp, req, failed, err := e.dispatch(ctx, pctx, u, entry)
	if e.OnDone != nil {
		e.OnDone(pctx.Upstream.Name, failed)
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

// dispatch 单次请求:BuildRequest → RoundTrip;无重试语义
func (e *Executor) dispatch(ctx context.Context, pctx *PipelineContext, u Resolved, entry []byte) (tresp *TransportResponse, req Request, failed bool, err error) {
	req, err = u.Protocol.BuildRequest(pctx, entry)
	if err != nil {
		if errors.Is(err, ErrPoolBusy) {
			return nil, req, true, err // 池耗尽:原样上抛(503)
		}
		return nil, req, true, &BuildError{Err: fmt.Errorf("buildRequest: %w", err)}
	}
	name := req.Transport
	if name == "" {
		name = TransportRef
	}
	tr, ok := e.Transports(name)
	if !ok {
		return nil, req, true, &BuildError{Err: fmt.Errorf("transport %q not found", name)}
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

// terminalResponse 错误终局:mapError(一次)或原样透传(mapError 失败不掩盖上游状态,记日志后回退)
func terminalResponse(pctx *PipelineContext, u Resolved, status int, body []byte) *Response {
	if em, ok := u.Protocol.(ErrorMapper); ok {
		out, err := em.MapError(pctx, status, body)
		if err != nil {
			log.Printf("mapError %s failed (pass through): %v", u.Protocol.Name(), err)
		} else if out != nil {
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

// CapabilityError 入口请求与上游声明不符(协议/形态/features);host 出 400,不做转换
type CapabilityError struct {
	Reason string
}

func (e *CapabilityError) Error() string { return e.Reason }

// guardForm 形态把关:入口形态须被声明,否则拒绝(不调用部件、不转换)
func guardForm(p Protocol, entryStream bool) error {
	s := p.Supports()
	want := FormNonStreaming
	if entryStream {
		want = FormStreaming
	}
	if !s.HasForm(want) {
		return &CapabilityError{Reason: fmt.Sprintf("upstream declares no %s form for entry %s", want, p.Declared())}
	}
	return nil
}

// decideStream 响应形态:单声明按声明固定;双声明按请求 stream 意图(BuildRequest 标志);
// 实际响应 Content-Type 兜底校验,不符按实际处理并告警
func decideStream(p Protocol, intentStream bool, tresp *TransportResponse) (stream, warn bool) {
	s := p.Supports()
	streaming := s.HasForm(FormStreaming)
	nonStreaming := s.HasForm(FormNonStreaming)
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

// sseFrameJSON 帧 → mapEvent 入参形态 {"event": string, "data": string}
func sseFrameJSON(f SSEFrame) []byte {
	b, _ := json.Marshal(map[string]string{"event": f.Event, "data": f.Data})
	return b
}

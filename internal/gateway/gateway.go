// Package gateway 对外 API 入口:鉴权/路由/管道调用/格式化输出/快速路径
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"ai-api-proxy/internal/builtin"
	"ai-api-proxy/internal/convert"
	"ai-api-proxy/internal/metrics"
	"ai-api-proxy/internal/pipeline"
	"ai-api-proxy/internal/upstream"
)

// Gateway HTTP 入口
type Gateway struct {
	OpenAI    convert.EntryCodec
	Anthropic convert.EntryCodec
	Executor  *pipeline.Executor
	Registry  *upstream.Registry
	Metrics   *metrics.Recorder
	Secrets   upstream.SecretsStore
}

// requestID 请求标识(header 键)
const requestIDHeader = "X-Request-Id"

// ChatCompletions POST /v1/chat/completions(openai 入口)
func (g *Gateway) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, g.OpenAI, false)
}

// Messages POST /v1/messages(anthropic 入口)
func (g *Gateway) Messages(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, g.Anthropic, true)
}

// serve 主流程
func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, codec convert.EntryCodec, anthropicEntry bool) {
	exit := g.Metrics.EnterRequest()
	defer exit()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		writeError(w, codec, http.StatusBadRequest, "read body")
		return
	}
	pivot, feats, err := codec.Parse(body)
	if err != nil {
		writeError(w, codec, http.StatusBadRequest, fmt.Sprintf("parse: %v", err))
		return
	}
	// n>1 → 400
	for _, f := range feats {
		if f == convert.FeatureN {
			writeError(w, codec, http.StatusBadRequest, "n>1 unsupported")
			return
		}
	}
	model, _ := pivot.GetScalar("model")
	entryStream := pivot.GetBool("stream")
	candidates, pickErr, err := g.Registry.Pick(model, featsToStrings(feats))
	if err != nil {
		switch pickErr {
		case upstream.PickNoModel:
			writeErrorWithModels(w, codec, http.StatusNotFound, "no upstream for model", g.availableModels())
		case upstream.PickCapability:
			writeError(w, codec, http.StatusBadRequest, "capability missing")
		default:
			writeError(w, codec, http.StatusServiceUnavailable, "no healthy target")
		}
		return
	}
	u := candidates[0].Upstream
	resolved, release, err := g.Registry.Resolve(u)
	if err != nil {
		log.Printf("resolve %s: %v", u.Name, err)
		writeError(w, codec, http.StatusBadGateway, "resolve failed")
		return
	}
	defer release() // 响应完全写完后释放部件池持有(流式含排空)
	// 快速路径:openai 入口 ∧ 内置协议 ∧ 有效 filter 链空
	if !anthropicEntry && isBuiltin(resolved.Protocol) && len(resolved.Filters) == 0 {
		g.fastPath(w, r, u, resolved, body)
		return
	}
	pctx := pipeline.NewContext(r.Header.Get(requestIDHeader), pipeline.UpstreamInfo{Name: u.Name, Models: u.Models},
		pipeline.Vars{Model: model, EntryStream: entryStream})
	resp, err := g.Executor.Run(r.Context(), pctx, resolved, mustJSON(pivot))
	if err != nil {
		g.Metrics.IncUpstream(u.Name, true)
		if r.Context().Err() != nil {
			return // 客户端断开:Cancelled 不计 errors
		}
		g.writeRunError(w, codec, pctx, resolved, err)
		return
	}
	if resp.Status >= 400 {
		// 终局错误(mapError 已在 executor 应用;未实现则原样透传)
		g.Metrics.IncUpstream(u.Name, true)
		g.Metrics.IncError()
		writeRaw(w, resp.Status, resp.Body)
		return
	}
	g.Metrics.IncUpstream(u.Name, false)
	// 态适配:入口形态 × 上游形态 失配时协议无关转换
	if entryStream != resp.Stream {
		g.adapted(w, r, codec, entryStream, resp)
		return
	}
	if resp.Stream {
		g.writeStream(w, codec, resp)
		return
	}
	out, err := codec.FromPivotResponse(&convert.PivotResponse{JSON: resp.Body})
	if err != nil {
		g.Metrics.IncError()
		writeError(w, codec, http.StatusBadGateway, "format response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(pipelineStatus(resp.Status))
	_, _ = w.Write(out)
}

// writeRunError Run 错误输出:池耗尽 503;BuildError(部件/传输/本地原因)502 协议格式
// (上游错误状态不走此路径:executor 已作终局响应返回)
func (g *Gateway) writeRunError(w http.ResponseWriter, codec convert.EntryCodec, pctx *pipeline.PipelineContext, resolved pipeline.Resolved, err error) {
	if errors.Is(err, pipeline.ErrPoolBusy) {
		writeError(w, codec, http.StatusServiceUnavailable, "runtime pool busy")
		return
	}
	writeError(w, codec, http.StatusBadGateway, err.Error())
}

// TestUpstream 管理连通性测试:首个模型发最小请求走完整管道(非流式;经 executor,OnTargetExit 照常计数)
// 返回延迟毫秒与错误说明;上游错误状态视为测试失败但计入延迟
func (g *Gateway) TestUpstream(ctx context.Context, u *upstream.Upstream) (int64, string) {
	if len(u.Models) == 0 {
		return 0, "upstream has no models"
	}
	resolved, release, err := g.Registry.Resolve(u)
	if err != nil {
		return 0, "resolve: " + err.Error()
	}
	defer release()
	pivot := map[string]any{
		"model":      u.Models[0],
		"max_tokens": 1,
		"messages":   []any{map[string]any{"role": "user", "content": "ping"}},
	}
	raw, err := json.Marshal(pivot)
	if err != nil {
		return 0, "marshal pivot: " + err.Error()
	}
	pctx := pipeline.NewContext("admin-test-"+u.Name, pipeline.UpstreamInfo{Name: u.Name, Models: u.Models},
		pipeline.Vars{Model: u.Models[0], EntryStream: false})
	start := time.Now()
	resp, err := g.Executor.Run(ctx, pctx, resolved, raw)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return latency, err.Error()
	}
	if resp.Status >= 300 {
		return latency, fmt.Sprintf("upstream status %d", resp.Status)
	}
	if resp.Stream {
		for range resp.Chunks { // 排空
		}
	}
	return latency, ""
}

// writeRaw 原样状态与 body(错误透传语义)
func writeRaw(w http.ResponseWriter, status int, body []byte) {
	if status < 100 || status > 599 {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// adapted 态适配分派:入口流×上游非流 → Chunkify 后按流输出;入口非流×上游流 → Aggregate 后按整体输出
func (g *Gateway) adapted(w http.ResponseWriter, r *http.Request, codec convert.EntryCodec, entryStream bool, resp *pipeline.Response) {
	ad := convert.StreamAdapter{}
	if entryStream {
		chunks, err := ad.Chunkify(&convert.PivotResponse{JSON: resp.Body})
		if err != nil {
			g.Metrics.IncError()
			writeError(w, codec, http.StatusBadGateway, "chunkify failed")
			return
		}
		ch := make(chan *convert.Chunk, len(chunks))
		for _, c := range chunks {
			ch <- c
		}
		close(ch)
		g.writeStream(w, codec, &pipeline.Response{Stream: true, Chunks: pivotChunkChan(ch), Status: resp.Status})
		return
	}
	pivotCh := make(chan *convert.Chunk)
	go func() {
		defer close(pivotCh)
		for item := range resp.Chunks {
			if item.RawPass {
				continue
			}
			ch, err := convert.ParseChunk(item.JSON)
			if err != nil {
				continue
			}
			pivotCh <- ch
		}
	}()
	agg, err := ad.Aggregate(pivotCh)
	// Aggregate 提前返回(x_error 中止)时泵 goroutine 可能阻塞在 send:排空防泄漏
	for range pivotCh {
	}
	if err != nil {
		g.Metrics.IncError()
		writeError(w, codec, http.StatusBadGateway, "aggregate failed")
		return
	}
	if agg.Err != nil {
		g.Metrics.IncError()
		status := http.StatusBadGateway
		if agg.Err.Status > 0 {
			status = agg.Err.Status
		}
		writeError(w, codec, status, agg.Err.Message)
		return
	}
	out, err := codec.FromPivotResponse(agg)
	if err != nil {
		g.Metrics.IncError()
		writeError(w, codec, http.StatusBadGateway, "format response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(pipelineStatus(resp.Status))
	_, _ = w.Write(out)
}

// pivotChunkChan convert.Chunk 通道适配为 pipeline chunk 项通道(仅用于态适配输出)
func pivotChunkChan(in <-chan *convert.Chunk) <-chan pipeline.ChunkItem {
	out := make(chan pipeline.ChunkItem)
	go func() {
		defer close(out)
		for c := range in {
			b, err := json.Marshal(c)
			if err != nil {
				continue
			}
			out <- pipeline.ChunkItem{JSON: b}
		}
	}()
	return out
}

// fastPath 透传:body 原样,目标 secrets 注入 Authorization,不切换不刷新
func (g *Gateway) fastPath(w http.ResponseWriter, r *http.Request, u *upstream.Upstream, resolved pipeline.Resolved, body []byte) {
	var target *upstream.Target
	for i := range u.Targets {
		if u.Targets[i].Enabled {
			target = &u.Targets[i]
			break
		}
	}
	if target == nil {
		writeError(w, g.OpenAI, http.StatusServiceUnavailable, "no enabled target")
		return
	}
	exitTarget := g.Metrics.EnterTarget(u.Name, target.Name)
	failed := false
	defer func() { exitTarget(failed) }()
	secrets, _ := g.Secrets.GetTargetSecrets(u.Name, target.Name)
	key := secrets["api_key"]
	if key == "" {
		failed = true
		g.Metrics.IncUpstream(u.Name, true)
		g.Metrics.IncError()
		writeError(w, g.OpenAI, http.StatusBadGateway, "target api_key missing")
		return
	}
	trName := target.Transport
	if trName == "" {
		trName = pipeline.TransportRef
	}
	tr, ok := g.Executor.Transports(trName)
	if !ok {
		failed = true
		g.Metrics.IncUpstream(u.Name, true)
		g.Metrics.IncError()
		writeError(w, g.OpenAI, http.StatusBadGateway, "transport missing")
		return
	}
	url := strings.TrimSuffix(target.BaseURL, "/") + "/v1/chat/completions"
	preq := pipeline.Request{
		URL:    url,
		Method: "POST",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": builtin.BearerPrefix + key,
		},
		Body: body, // 原样透传,不重序列化
	}
	tresp, err := tr.RoundTrip(r.Context(), preq)
	if err != nil {
		failed = true
		g.Metrics.IncUpstream(u.Name, true)
		if r.Context().Err() == nil {
			g.Metrics.IncError()
			writeError(w, g.OpenAI, http.StatusBadGateway, "upstream unreachable")
			return
		}
		return // 客户端断开:Cancelled 不计 errors
	}
	if tresp.Status >= 400 {
		failed = true
		g.Metrics.IncUpstream(u.Name, true)
		g.Metrics.IncError()
	} else {
		g.Metrics.IncUpstream(u.Name, false)
	}
	if isEventStreamCT(tresp.Headers["Content-Type"]) {
		g.passthroughStream(w, tresp)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(tresp.Status)
	_, _ = w.Write(tresp.Body)
}

// writeStream 流式输出:逐 chunk Flush;raw 帧原样;[DONE] 收尾
func (g *Gateway) writeStream(w http.ResponseWriter, codec convert.EntryCodec, resp *pipeline.Response) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, codec, http.StatusInternalServerError, "stream unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	chunker := codec.NewFromPivotChunker()
	for item := range resp.Chunks {
		var events []sseEventOut
		if item.RawPass {
			events = []sseEventOut{{data: string(item.JSON)}}
		} else {
			evs, err := chunker.Next(item.JSON)
			if err != nil {
				log.Printf("chunker next: %v", err)
				continue
			}
			for _, e := range evs {
				events = append(events, sseEventOut{data: e.Data, done: e.Done})
			}
		}
		for _, e := range events {
			writeSSE(w, e)
		}
		flusher.Flush()
	}
	fin, err := chunker.Flush()
	if err == nil {
		for _, e := range fin {
			writeSSE(w, sseEventOut{data: e.Data, done: e.Done})
		}
		flusher.Flush()
	}
}

// passthroughStream 快速路径流式:上游帧原样转发
func (g *Gateway) passthroughStream(w http.ResponseWriter, tresp pipeline.TransportResponse) {
	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	for frame := range tresp.Events {
		if frame.Event != "" {
			_, _ = fmt.Fprintf(w, "event: %s\n", frame.Event)
		}
		_, _ = fmt.Fprintf(w, "data: %s\n\n", frame.Data)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// Models GET /v1/models(双形态)+ /v1/models/{id}
func (g *Gateway) Models(w http.ResponseWriter, r *http.Request) {
	models := g.availableModels()
	anthropicForm := r.Header.Get("x-api-key") != "" || r.Header.Get("anthropic-version") != ""
	if anthropicForm {
		data := make([]map[string]any, 0, len(models))
		for _, m := range models {
			data = append(data, map[string]any{"type": "model", "id": m, "display_name": m})
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": data})
		return
	}
	data := make([]map[string]any, 0, len(models))
	for _, m := range models {
		data = append(data, map[string]any{"id": m, "object": "model", "owned_by": "ai-api-proxy"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// ModelByID GET /v1/models/{id}(openai 形态)
func (g *Gateway) ModelByID(w http.ResponseWriter, r *http.Request, id string) {
	for _, m := range g.availableModels() {
		if m == id {
			writeJSON(w, http.StatusOK, map[string]any{"id": id, "object": "model", "owned_by": "ai-api-proxy"})
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]any{"type": "not_found", "message": "model not found"}})
}

// availableModels 聚合启用上游模型声明去重(稳定序)
func (g *Gateway) availableModels() []string {
	seen := map[string]bool{}
	var out []string
	for _, u := range g.Registry.List() {
		if !u.Enabled {
			continue
		}
		for _, m := range u.Models {
			if !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	return out
}

// sseEventOut 输出事件
type sseEventOut struct {
	data string
	done bool
}

// writeSSE 写单事件
func writeSSE(w io.Writer, e sseEventOut) {
	if e.done {
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		return
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", e.data)
}

// isBuiltin 是否内置协议
func isBuiltin(p pipeline.Protocol) bool { return p.Name() == builtin.Name }

// featsToStrings 能力转字符串;stream 属形态协商(分发层),不进路由能力
func featsToStrings(feats []convert.Feature) []string {
	out := make([]string, 0, len(feats))
	for _, f := range feats {
		if f == convert.FeatureStream {
			continue
		}
		out = append(out, string(f))
	}
	return out
}

// mustJSON pivot 序列化
func mustJSON(p *convert.PivotRequest) []byte {
	b, _ := json.Marshal(p)
	return b
}

// pipelineStatus 非流式响应状态(0 → 200)
func pipelineStatus(status int) int {
	if status == 0 {
		return http.StatusOK
	}
	return status
}

// writeError 协议格式错误体
func writeError(w http.ResponseWriter, codec convert.EntryCodec, status int, msg string) {
	out, _ := codec.FromPivotResponse(&convert.PivotResponse{Err: &convert.XError{Type: "api_error", Message: msg}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

// writeErrorWithModels 404 附可用模型
func writeErrorWithModels(w http.ResponseWriter, codec convert.EntryCodec, status int, msg string, models []string) {
	out, _ := codec.FromPivotResponse(&convert.PivotResponse{Err: &convert.XError{
		Type: "api_error", Message: msg + ": " + strings.Join(models, ","),
	}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

// writeJSON 直接 JSON 输出
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// isEventStreamCT SSE Content-Type 判定
func isEventStreamCT(ct string) bool {
	return strings.HasPrefix(strings.ToLower(ct), "text/event-stream")
}

// maxBodySize 请求体上限
const maxBodySize = 32 * 1024 * 1024

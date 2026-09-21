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

	"github.com/mzzsfy/ai-api-proxy/internal/builtin"
	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// Entry 入口协议(codec + 协议全名)
type Entry struct {
	convert.EntryInspector
	convert.ErrorRenderer
	convert.FramerFactory
	// Protocol 入口协议全名(与插件协议槽同枚举)
	Protocol string
}

// Inspect 入口解析:只读,原文不改
func (e Entry) Inspect(body []byte) (model string, stream bool, feats []convert.Feature, err error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return "", false, nil, fmt.Errorf("parse request: %w", err)
	}
	return e.EntryInspector.Inspect(m)
}

// Gateway HTTP 入口
type Gateway struct {
	OpenAI    Entry
	Anthropic Entry
	Executor  *pipeline.Executor
	Registry  *upstream.Registry
	Metrics   *metrics.Recorder
	Secrets   upstream.SecretsStore
}

// requestIDHeader 请求标识(header 键)
const requestIDHeader = "X-Request-Id"

// testBody 管理连通性测试体:按上游声明协议构造最小请求
func testBody(declared, model string) ([]byte, error) {
	if declared == pipeline.ProtocolAnthropicMessages {
		return json.Marshal(map[string]any{
			"model":      model,
			"max_tokens": 1,
			"messages":   []any{map[string]any{"role": "user", "content": "ping"}},
		})
	}
	return json.Marshal(map[string]any{
		"model":    model,
		"stream":   false,
		"messages": []any{map[string]any{"role": "user", "content": "ping"}},
	})
}

// ChatCompletions POST /v1/chat/completions(openai 入口)
func (g *Gateway) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, g.OpenAI, false)
}

// Messages POST /v1/messages(anthropic 入口)
func (g *Gateway) Messages(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, g.Anthropic, true)
}

// serve 主流程:入口只读解析 → 按声明协议协商路由 → 管道执行 → 入口格式输出
func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, entry Entry, anthropicEntry bool) {
	exit := g.Metrics.EnterRequest()
	defer exit()
	start := time.Now()
	status := http.StatusOK
	detail := "" // 模型/上游/插件路径等上下文(请求日志)
	defer func() {
		log.Printf("request %s %s %s status=%d duration=%s%s",
			r.Method, r.URL.Path, detail, status, time.Since(start).Round(time.Millisecond), levelMark(status))
	}()
	w = &statusWriter{ResponseWriter: w, code: &status}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		status = http.StatusBadRequest
		writeError(w, entry, http.StatusBadRequest, "read body")
		return
	}
	model, entryStream, feats, err := entry.Inspect(body)
	if err != nil {
		status = http.StatusBadRequest
		writeError(w, entry, http.StatusBadRequest, err.Error())
		return
	}
	detail = "model=" + model
	// n>1 → 400
	for _, f := range feats {
		if f == convert.FeatureN {
			status = http.StatusBadRequest
			writeError(w, entry, http.StatusBadRequest, "n>1 unsupported")
			return
		}
	}
	candidates, pickErr, err := g.Registry.Pick(model, featsToStrings(feats), entry.Protocol, entryStream)
	if err != nil {
		status = pickStatus(pickErr)
		detail += " reason=" + pickErr.Error() + " plugins=-"
		switch pickErr {
		case upstream.PickNoModel:
			writeErrorWithModels(w, entry, status, "no upstream for model", g.availableModels(entry.Protocol))
		case upstream.PickCapability:
			writeError(w, entry, status, err.Error()+"; declared by "+g.slotSummary(model))
		default:
			writeError(w, entry, status, "no healthy target")
		}
		return
	}
	u := candidates[0].Upstream
	resolved, release, err := g.Registry.Resolve(u)
	if err != nil {
		log.Printf("resolve %s: %v", u.Name, err)
		status = http.StatusBadGateway
		detail += " upstream=" + u.Name
		writeError(w, entry, http.StatusBadGateway, "resolve failed")
		return
	}
	defer release() // 响应完全写完后释放部件池持有(流式含排空)
	detail += " upstream=" + u.Name + " target=" + firstTarget(resolved) +
		" plugins=" + pluginPath(u.Name, resolved)
	// 快速路径:openai 入口 ∧ 内置协议 ∧ 有效 filter 链空
	if !anthropicEntry && isBuiltin(resolved.Protocol) && len(resolved.Filters) == 0 {
		g.fastPath(w, r, u, resolved, body, model)
		return
	}
	pctx := pipeline.NewContext(r.Header.Get(requestIDHeader), pipeline.UpstreamInfo{Name: u.Name, Models: u.Models},
		pipeline.Vars{Model: model, EntryStream: entryStream})
	resp, err := g.Executor.Run(r.Context(), pctx, resolved, body)
	if err != nil {
		g.Metrics.IncUpstream(u.Name, true)
		if r.Context().Err() != nil {
			return // 客户端断开:Cancelled 不计 errors
		}
		status = runErrorStatus(err)
		detail += " error=" + err.Error()
		g.writeRunError(w, entry, pctx, resolved, err)
		return
	}
	if resp.Status >= 400 {
		// 终局错误(mapError 已在 executor 应用;未实现则原样透传)
		g.Metrics.IncUpstream(u.Name, true)
		g.Metrics.IncError()
		status = resp.Status
		writeRaw(w, resp.Status, resp.Body)
		return
	}
	g.Metrics.IncUpstream(u.Name, false)
	detail += fmt.Sprintf(" stream=%t", resp.Stream)
	if resp.Stream {
		g.writeStream(w, entry, resp)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(pipelineStatus(resp.Status))
	_, _ = w.Write(resp.Body)
}

// levelMark 日志分级标记:5xx=ERROR、4xx=WARN、其余空(grep 定位用)
func levelMark(status int) string {
	switch {
	case status >= 500:
		return " level=ERROR"
	case status >= 400:
		return " level=WARN"
	default:
		return ""
	}
}

// statusWriter 捕获 WriteHeader 状态码(请求日志用)
type statusWriter struct {
	http.ResponseWriter
	code *int
}

func (s *statusWriter) WriteHeader(code int) {
	*s.code = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush 透传流式冲刷(保持原 writer 的 Flusher 能力)
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// pluginPath 插件路径描述:协议包 + filter 链(与请求路由/处理顺序一致)
func pluginPath(upstreamName string, resolved pipeline.Resolved) string {
	parts := make([]string, 0, 1+len(resolved.Filters))
	parts = append(parts, resolved.Protocol.Name())
	for _, f := range resolved.Filters {
		parts = append(parts, f.Name())
	}
	return strings.Join(parts, "->")
}
// firstTarget 首个启用目标名(无目标时为 -)
func firstTarget(resolved pipeline.Resolved) string {
	if len(resolved.Targets) == 0 {
		return "-"
	}
	return resolved.Targets[0].Name
}

// pickStatus 路由失败状态(与 serve 内错误分支一致)
func pickStatus(pickErr upstream.PickError) int {
	switch pickErr {
	case upstream.PickNoModel:
		return http.StatusNotFound
	case upstream.PickCapability:
		return http.StatusBadRequest
	default:
		return http.StatusServiceUnavailable
	}
}

// runErrorStatus Run 错误状态(与 writeRunError 分支一致)
func runErrorStatus(err error) int {
	var capErr *pipeline.CapabilityError
	if errors.As(err, &capErr) {
		return http.StatusBadRequest
	}
	if errors.Is(err, pipeline.ErrPoolBusy) || errors.Is(err, ipprovider.ErrNoExits) {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

// writeRunError Run 错误输出:形态不符 400;池耗尽/无出口 503;其余 502 协议格式
// (上游错误状态不走此路径:executor 已作终局响应返回)
func (g *Gateway) writeRunError(w http.ResponseWriter, entry Entry, pctx *pipeline.PipelineContext, resolved pipeline.Resolved, err error) {
	var capErr *pipeline.CapabilityError
	if errors.As(err, &capErr) {
		writeError(w, entry, http.StatusBadRequest, capErr.Reason)
		return
	}
	if errors.Is(err, pipeline.ErrPoolBusy) {
		writeError(w, entry, http.StatusServiceUnavailable, "runtime pool busy")
		return
	}
	if errors.Is(err, ipprovider.ErrNoExits) {
		writeError(w, entry, http.StatusServiceUnavailable, "no available egress")
		return
	}
	writeError(w, entry, http.StatusBadGateway, err.Error())
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
	raw, err := testBody(g.Registry.DeclaredProtocol(u), u.Models[0])
	if err != nil {
		return 0, "marshal test body: " + err.Error()
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

// 声明式单协议下无 pivot 中转:入口原文经 filters 直达 buildRequest,回程帧直接来自 mapEvent

// fastPath 透传:body 原样,目标 secrets 注入 Authorization,不切换不刷新
func (g *Gateway) fastPath(w http.ResponseWriter, r *http.Request, u *upstream.Upstream, resolved pipeline.Resolved, body []byte, model string) {
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
		Body:  body,  // 原样透传,不重序列化
		Model: model, // ipp 供给方会话亲和素材(非 ipp 传输忽略)
	}
	tresp, err := tr.RoundTrip(r.Context(), preq)
	if err != nil {
		failed = true
		g.Metrics.IncUpstream(u.Name, true)
		if r.Context().Err() == nil {
			g.Metrics.IncError()
			// 供给方无可用出口:503(临时性,调用方可换 upstream 重试);其余上游不可达:502
			if errors.Is(err, ipprovider.ErrNoExits) {
				writeError(w, g.OpenAI, http.StatusServiceUnavailable, "no available egress")
				return
			}
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

// writeStream 流式输出:逐帧经入口 Framer 归一;降级帧原样;尾帧由 Flush 收尾
func (g *Gateway) writeStream(w http.ResponseWriter, entry Entry, resp *pipeline.Response) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, entry, http.StatusInternalServerError, "stream unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	framer := entry.Framer()
	for item := range resp.Chunks {
		var events []sseEventOut
		if item.RawPass {
			events = []sseEventOut{{data: string(item.JSON)}}
		} else {
			for _, e := range framer.Frame(item.JSON) {
				events = append(events, sseEventOut{event: e.Event, data: e.Data})
			}
		}
		for _, e := range events {
			writeSSE(w, e)
		}
		flusher.Flush()
	}
	for _, e := range framer.Flush() {
		writeSSE(w, sseEventOut{event: e.Event, data: e.Data})
	}
	flusher.Flush()
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
	anthropicForm := r.Header.Get("x-api-key") != "" || r.Header.Get("anthropic-version") != ""
	protocol := pipeline.ProtocolOpenAICompletions
	if anthropicForm {
		protocol = pipeline.ProtocolAnthropicMessages
	}
	models := g.availableModels(protocol)
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
	for _, m := range g.availableModels(pipeline.ProtocolOpenAICompletions) {
		if m == id {
			writeJSON(w, http.StatusOK, map[string]any{"id": id, "object": "model", "owned_by": "ai-api-proxy"})
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]any{"type": "not_found", "message": "model not found"}})
}

// availableModels 聚合启用上游模型声明去重(稳定序;协议非空时仅计声明该协议的上游)
func (g *Gateway) availableModels(protocol string) []string {
	seen := map[string]bool{}
	var out []string
	for _, u := range g.Registry.List() {
		if !u.Enabled {
			continue
		}
		if protocol != "" && g.Registry.DeclaredProtocol(u) != protocol {
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

// allModels 聚合启用上游模型声明去重(不分协议)
func (g *Gateway) allModels() []string { return g.availableModels("") }

// sseEventOut 输出事件(event 非空即写 event 行;done 仅 openai 收尾用)
type sseEventOut struct {
	event string
	data  string
	done  bool
}

// writeSSE 写单事件
func writeSSE(w io.Writer, e sseEventOut) {
	if e.done {
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		return
	}
	if e.event != "" {
		_, _ = fmt.Fprintf(w, "event: %s\n", e.event)
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

// slotSummary 声明该模型的启用上游所持协议槽(去重稳定序;诊断用)
func (g *Gateway) slotSummary(model string) string {
	seen := map[string]bool{}
	out := ""
	for _, u := range g.Registry.List() {
		if !u.Enabled || !containsModel(u.Models, model) {
			continue
		}
		slot := u.Name + "(" + g.Registry.DeclaredProtocol(u) + ")"
		if seen[slot] {
			continue
		}
		seen[slot] = true
		if out != "" {
			out += ","
		}
		out += slot
	}
	if out == "" {
		return "none"
	}
	return out
}

// containsModel 模型是否在该上游声明内
func containsModel(models []string, model string) bool {
	for _, m := range models {
		if m == model {
			return true
		}
	}
	return false
}

// pipelineStatus 非流式响应状态(0 → 200)
func pipelineStatus(status int) int {
	if status == 0 {
		return http.StatusOK
	}
	return status
}

// writeError 协议格式错误体
func writeError(w http.ResponseWriter, entry Entry, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(entry.ErrorBody(status, msg, nil))
}

// writeErrorWithModels 404 附可用模型
func writeErrorWithModels(w http.ResponseWriter, entry Entry, status int, msg string, models []string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(entry.ErrorBody(status, msg, models))
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

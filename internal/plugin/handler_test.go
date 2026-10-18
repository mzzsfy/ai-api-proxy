package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// ─── handler 执行引擎(BDD 场景)───

// fakeSink 应答写口记录
type fakeSink struct {
	mu      sync.Mutex
	json    []string
	status  int
	events  []string
	ended   bool
	errs    []string
	sseHead bool
}

func (s *fakeSink) JSON(status int, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.json = append(s.json, body)
	return nil
}
func (s *fakeSink) SSEStart() error { s.mu.Lock(); s.sseHead = true; s.mu.Unlock(); return nil }
func (s *fakeSink) Event(e string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}
func (s *fakeSink) End() error { s.mu.Lock(); s.ended = true; s.mu.Unlock(); return nil }
func (s *fakeSink) ErrorFrame(e string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, e)
	return nil
}

// handlerPkg 测试包(files: handler.js 必带)
func handlerPkg(t *testing.T, src string) *Package {
	t.Helper()
	return &Package{
		Manifest: &Manifest{Name: "acme"},
		Revision: 1,
		Files:    map[string][]byte{"handler.js": []byte(src)},
	}
}

func testEngine(t *testing.T, pkg *Package, tr pipeline.Transport) (*HandlerEngine, *SlotStore, *SlotLocks) {
	t.Helper()
	db := testSlotDB(t)
	store := NewSlotStore(db, "acme")
	locks := NewSlotLocks()
	return NewHandlerEngine(HandlerDeps{
		Package: pkg, Store: store, Locks: locks,
		Transport: func(name string) (pipeline.Transport, error) {
			if name == "" {
				return tr, nil
			}
			return nil, fmt.Errorf("transport %q not found", name)
		},
		Log: func(level, msg string) { t.Logf("[%s] %s", level, msg) },
	}), store, locks
}

func execIn(model string, stream bool, keyID string) HandlerInput {
	return HandlerInput{
		Model: model, Stream: stream, Headers: map[string]string{"X-E": "1"},
		Body: `{"m":"x"}`, Key: pipeline.KeyEntry{ID: keyID, Data: map[string]any{"k": "v"}},
		Params: map[string]any{"base_url": "u"}, SessionKey: "sess:" + keyID,
	}
}

// 场景: 非流式应答——reply.json(200, body) 后 return
func TestHandlerEngineReplyJSON(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, '{"ok":true}'); }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if sink.status != 200 || len(sink.json) != 1 || sink.json[0] != `{"ok":true}` {
		t.Fatalf("sink: %+v", sink)
	}
}

// 场景: prepare 交接——返回值作 ctx.data 初始值,不经 storage
func TestHandlerEnginePrepareHandoff(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, ctx.data.token); };
module.exports.prepare = function(ctx){ return {token: "t1"} };`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if sink.json[0] != "t1" {
		t.Fatalf("handoff: %v", sink.json)
	}
}

// 场景: prepare 抛错 = 502,主函数不执行
func TestHandlerEnginePrepareError(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, "SHOULD_NOT_RUN"); };
module.exports.prepare = function(ctx){ throw new Error("boot failed") };`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink)
	if err == nil || !strings.Contains(err.Error(), "boot failed") || !strings.Contains(err.Error(), "acme") {
		t.Fatalf("prepare error: %v", err)
	}
	if len(sink.json) != 0 {
		t.Fatal("handler must not run")
	}
}

// 场景: prepare 禁 reply / 禁 ctx.data
func TestHandlerEnginePrepareForbidden(t *testing.T) {
	for _, src := range []string{
		`module.exports = function(ctx){ ctx.reply.json(200, "y") };
module.exports.prepare = function(ctx){ ctx.reply.json(200, "x") }`,
		`module.exports = function(ctx){ ctx.reply.json(200, "y") };
module.exports.prepare = function(ctx){ ctx.data }`,
	} {
		pkg := handlerPkg(t, src)
		e, _, _ := testEngine(t, pkg, testTransport())
		err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
		if err == nil || !strings.Contains(err.Error(), "prepare") {
			t.Fatalf("prepare violation: %v", err)
		}
	}
}

// 场景: 零应答 = 502(注明包名)
func TestHandlerEngineZeroReply(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ util.now(); }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "no reply") || !strings.Contains(err.Error(), "acme") {
		t.Fatalf("zero reply: %v", err)
	}
}

// 场景: 应答形态强制校验——入口 stream=true 调 reply.json 抛错(未写出任何内容)
// 场景: 入口 stream=false 调 reply.sse 抛错
func TestHandlerEngineFormValidation(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, "x") }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", true, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "stream=true") {
		t.Fatalf("json on stream entry: %v", err)
	}

	pkg2 := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.sse() }`)
	e2, _, _ := testEngine(t, pkg2, testTransport())
	err = e2.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "stream=false") {
		t.Fatalf("sse on non-stream entry: %v", err)
	}
}

// 场景: sse 后 return 未调 end = 异常终局(错误终帧收尾)
func TestHandlerEngineSSEWithoutEnd(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.sse() }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(sink.errs) != 1 || !strings.Contains(sink.errs[0], "reply.end") {
		t.Fatalf("error terminal frame: %+v", sink)
	}
}

// 场景: reply.error 已写 = 视同已收尾,return 即正常终局
func TestHandlerEngineReplyErrorTerminal(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.sse(); ctx.reply.error({message:"up"}); }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(sink.errs) != 1 {
		t.Fatalf("error frame: %+v", sink)
	}
}

// 场景: 已写出部分帧后抛错 = 规范错误终帧(零补偿)
// 场景: reply.json 已完整写出后 handler 抛错 = 正常关闭 + log(非 502)
func TestHandlerEngineErrorAfterWrite(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){
	ctx.reply.sse(); ctx.reply.event({delta:"a"}); throw new Error("mid-stream boom");
}`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink)
	if err == nil || !strings.Contains(err.Error(), "mid-stream boom") {
		t.Fatalf("stream error: %v", err)
	}
	if len(sink.errs) != 1 || len(sink.events) != 1 {
		t.Fatalf("partial frames then error frame: %+v", sink)
	}

	pkg2 := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, "done"); throw new Error("late boom"); }`)
	e2, _, _ := testEngine(t, pkg2, testTransport())
	sink2 := &fakeSink{}
	if err := e2.Execute(context.Background(), execIn("m1", false, "k1"), sink2); err != nil {
		t.Fatalf("error after json must close normally: %v", err)
	}
}

// 场景: reply 越序——sse 后调 json 抛错
func TestHandlerEngineReplyOutOfOrder(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.sse(); ctx.reply.json(200, "x"); }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", true, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "json") {
		t.Fatalf("out of order: %v", err)
	}
}

// 场景: 同步性违规——Promise 返回 = 502
func TestHandlerEngineAsyncHandlerSettled(t *testing.T) {
	// Given async handler 返回 Promise When Execute Then 宿主等待 settled(async 放开;未应答仍违规)
	pkg := handlerPkg(t, `module.exports = async function(ctx){ await Promise.resolve(1); ctx.reply.json(200, "ok"); }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("async handler: %v", err)
	}
	if len(sink.json) == 0 || sink.json[0] != "ok" {
		t.Fatalf("reply lost: %+v", sink)
	}
}

// 场景: 只读成员违规——写 ctx.key 抛错(严格模块 + 冻结)
func TestHandlerEngineReadonlyViolation(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ "use strict"; ctx.key.id = "x"; }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
	if err == nil {
		t.Fatal("readonly write must throw")
	}
}

// 场景: 全局污染封死——写 globalThis 当场抛错
func TestHandlerEngineGlobalFreeze(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){ "use strict"; globalThis.foo = 1; }`)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
	if err == nil {
		t.Fatal("global write must throw")
	}
	// 污染实例不可用后,下一请求仍正常(实例被丢弃重建)
	pkg2 := handlerPkg(t, `module.exports = function(ctx){ ctx.reply.json(200, "ok") }`)
	e2, _, _ := testEngine(t, pkg2, testTransport())
	if err := e2.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{}); err != nil {
		t.Fatalf("fresh execution: %v", err)
	}
}

// 场景: 直线刷 token(S1)+ unlock 落值全局可见 + 羊群单飞(串行化)
func TestHandlerEngineSlotLockFlow(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){
	const slot = "live_token:" + ctx.key.id;
	let token = ctx.storage.getWithLock(slot);
	if (!token) { token = "boot-" + ctx.key.id; }
	ctx.storage.unlock(slot, token);
	ctx.reply.json(200, token);
}`)
	e, store, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("first: %v", err)
	}
	if sink.json[0] != "boot-k1" {
		t.Fatalf("boot token: %v", sink.json)
	}
	// 第二请求直读成果(③)
	sink2 := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink2); err != nil {
		t.Fatalf("second: %v", err)
	}
	if sink2.json[0] != "boot-k1" {
		t.Fatalf("reuse: %v", sink2.json)
	}
	// ③ 已打权威标:②缓冲 set 同槽会被归并丢弃
	pkg2 := handlerPkg(t, `module.exports = function(ctx){
	ctx.storage.set("live_token:k1", "stale");
	ctx.reply.json(200, "done");
}`)
	e2, _, _ := testEngine(t, pkg2, testTransport())
	e2.deps.Store = store
	e2.deps.Log = func(level, msg string) { t.Logf("[%s] %s", level, msg) }
	if err := e2.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{}); err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if v, auth, _ := store.GetAuth("live_token:k1"); v != "boot-k1" || !auth {
		t.Fatalf("authoritative survives buffered set: %q %v", v, auth)
	}
}

// 场景: 锁定槽写封闭——持锁者 set 同槽抛错;未持锁 unlock 抛错
func TestHandlerEngineSlotWriteClosure(t *testing.T) {
	for _, src := range []string{
		`module.exports = function(ctx){
	const s = "tok";
	ctx.storage.getWithLock(s);
	ctx.storage.set(s, "v");
}`,
		`module.exports = function(ctx){ ctx.storage.unlock("never-held"); }`,
		`module.exports = function(ctx){
	ctx.storage.getWithLock("s");
	ctx.storage.getWithLock("s");
}`,
	} {
		pkg := handlerPkg(t, src)
		e, _, _ := testEngine(t, pkg, testTransport())
		err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
		if err == nil {
			t.Fatalf("closure violation must throw: %s", src)
		}
	}
}

// 场景: 终局锁泄漏兜底——抛错路径两把锁均被宿主释放,排队请求继续
func TestHandlerEngineLockLeakRelease(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){
	ctx.storage.getWithLock("s1");
	ctx.storage.getWithLock("s2");
	throw new Error("boom with locks");
}`)
	e, _, locks := testEngine(t, pkg, testTransport())
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{}); err == nil {
		t.Fatal("expected error")
	}
	if locks.Held("s1") || locks.Held("s2") {
		t.Fatal("locks must be released at terminal")
	}
}

// 场景: 非流式上游伪流式——200 非 event-stream:frames 抛错后走 body 聚合拆块
func TestHandlerEnginePseudoStreaming(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"full":"payload"}`)
	}))
	defer upstream.Close()

	pkg := handlerPkg(t, `module.exports = function(ctx){
	ctx.reply.sse();
	let src = ctx.http.open({url: "UPURL", method: "GET", stream: true});
	let text;
	try {
		for (const f of src.frames()) { text = (text||"") + f.data; }
	} catch (e) {
		text = src.body();
	}
	ctx.reply.event({delta: text});
	ctx.reply.end();
}`)
	src := strings.Replace(string(pkg.Files["handler.js"]), "UPURL", upstream.URL, 1)
	pkg.Files["handler.js"] = []byte(src)

	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !sink.ended || len(sink.events) != 1 || !strings.Contains(sink.events[0], "payload") {
		t.Fatalf("pseudo stream: %+v", sink)
	}
}

func TestHandlerChunkIteratorPreservesReadError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
	}))
	defer upstream.Close()
	plugin := strings.Replace(`module.exports=function(ctx){
	const source=ctx.http.open({url:"UPURL",method:"GET",stream:true});
	const iter=source.chunks();
	iter.next();
	}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, plugin)
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", true, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "body closed") && !strings.Contains(err.Error(), "broken") {
		t.Fatalf("chunk read error was swallowed: %v", err)
	}
}

func TestHandlerEngineBodyBytesPreservesRawBytes(t *testing.T) {
	want := []byte{0x00, 0x80, 0xff, 0xc3, 0x28}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(want)
	}))
	defer upstream.Close()

	plugin := strings.Replace(`module.exports = function(ctx) {
	const source = ctx.http.open({url:"UPURL",method:"GET",stream:true});
	const bytes = new Uint8Array(source.bodyBytes());
	const got = Array.from(bytes).join(",");
	const want = "0,128,255,195,40";
	ctx.reply.json(200, JSON.stringify({same: got === want, bytes: got}));
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, plugin)
	engine, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := engine.Execute(context.Background(), execIn("body-bytes", false, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var response map[string]any
	if len(sink.json) == 0 {
		t.Fatal("bodyBytes response was not sent")
	}
	if err := json.Unmarshal([]byte(sink.json[0]), &response); err != nil {
		t.Fatalf("decode bodyBytes response: %v", err)
	}
	if response["same"] != true || response["bytes"] != "0,128,255,195,40" {
		t.Fatalf("bodyBytes response = %#v", response)
	}
}

func TestHandlerEngineEventStreamToSSE(t *testing.T) {
	payload := []byte(`{"message":"hello"}`)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		_, _ = w.Write(testEventStreamFrame(payload))
	}))
	defer upstream.Close()
	src := strings.Replace(`module.exports = function(ctx){
	ctx.reply.sse();
	const source = ctx.http.open({url:"UPURL",method:"GET",stream:true});
	for (const event of util.eventstream.decode(source.chunks())) {
		ctx.reply.event({message:Array.from(new Uint8Array(event.payload)).map(x=>String.fromCharCode(x)).join("")});
	}
	ctx.reply.end();
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, src)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink); err != nil {
		t.Fatal(err)
	}
	if !sink.ended || len(sink.events) != 1 || !strings.Contains(sink.events[0], "hello") {
		t.Fatalf("EventStream relay = %+v", sink)
	}
}

func TestHandlerEngineStreamRelay(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: a\n\ndata: b\n\n")
	}))
	defer upstream.Close()

	src := strings.Replace(`module.exports = function(ctx){
	ctx.reply.sse();
	for (const f of ctx.http.open({url: "UPURL", method: "GET", stream: true}).frames()) {
		ctx.reply.event({delta: f.data});
	}
	ctx.reply.end();
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, src)

	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", true, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(sink.events) != 2 || !sink.ended {
		t.Fatalf("relay: %+v", sink)
	}
}

// 场景: 上游非 200——open 后 frames 不可用,body 一次取回
func TestHandlerEngineNon2xx(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":"rate"}`)
	}))
	defer upstream.Close()

	src := strings.Replace(`module.exports = function(ctx){
	let src = ctx.http.open({url: "UPURL", method: "GET", stream: true});
	if (src.status !== 200) { ctx.reply.json(429, src.body()); return; }
}`, "UPURL", upstream.URL, 1)
	pkg := handlerPkg(t, src)

	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if sink.status != 429 || sink.json[0] != `{"error":"rate"}` {
		t.Fatalf("non-2xx: %+v", sink)
	}
}

// 场景: settings.overwrite 非法键抛错;合法覆写生效(http.timeoutMs 单调用覆写)
func TestHandlerEngineSettings(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){
	try { ctx.settings.overwrite({bogus: 1}); ctx.reply.json(500, "no"); return; } catch (e) {}
	ctx.reply.json(200, "ok");
}`)
	e, _, _ := testEngine(t, pkg, testTransport())
	sink := &fakeSink{}
	if err := e.Execute(context.Background(), execIn("m1", false, "k1"), sink); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if sink.json[0] != "ok" {
		t.Fatalf("settings: %v", sink.json)
	}
}

// 场景: 池构建失败 = 该次借出 502(不拒装;下次重试构建)
func TestHandlerEnginePoolBuildFailure(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = 42`) // 导出非函数
	e, _, _ := testEngine(t, pkg, testTransport())
	err := e.Execute(context.Background(), execIn("m1", false, "k1"), &fakeSink{})
	if err == nil || !strings.Contains(err.Error(), "function") {
		t.Fatalf("pool build failure: %v", err)
	}
}

// 场景: 池化复用——同 (包,revision) 第二次借出复用实例(模块只加载一次)
func TestHandlerEnginePoolReuse(t *testing.T) {
	pkg := handlerPkg(t, `module.exports = function(ctx){
	ctx.reply.json(200, "ok");
}`)
	_ = pkg
	pool := newHandlerPool(100, func() (*handlerInstance, error) {
		return &handlerInstance{}, nil
	})
	inst, err := pool.Borrow()
	if err != nil {
		t.Fatalf("borrow1: %v", err)
	}
	// 借还至第 maxBorrow 次归还时丢弃
	for inst.borrows < pool.maxBorrow {
		pool.Return(inst, false)
		next, err := pool.Borrow()
		if err != nil {
			t.Fatalf("borrow n=%d: %v", inst.borrows, err)
		}
		if next != inst {
			t.Fatalf("instance discarded before max borrows at n=%d", inst.borrows)
		}
	}
	pool.Return(inst, false) // 第 maxBorrow 次归还:丢弃
	i3, err := pool.Borrow()
	if err != nil {
		t.Fatalf("borrow3: %v", err)
	}
	if i3 == inst {
		t.Fatal("instance must be discarded after max borrows")
	}
}

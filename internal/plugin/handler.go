// handler 执行引擎:每执行独立 goroutine + 池化 runtime(参数化注入)+ 两级 storage + 槽锁登记表
package plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// 执行预算(定案常量)
const (
	handlerOverallBudget  = 60 * time.Minute // 整体预算(含 prepare/池排队/锁排队)
	handlerStatementCap   = 10 * time.Second // 纯计算上限(无原语活动即超限)
	handlerGlobalConcurr  = 64               // 全局 handler 总并发(所有包合计)
	handlerQueueTimeout   = 60 * time.Second // 池排队上界(写死,不做配置)
	handlerVMMaxBorrowDft = 100              // 借出次数缺省(≤0 视为缺省)
)

// ErrHandlerTimeout 执行级超时哨兵(整体/纯计算/池排队/锁等待)
var ErrHandlerTimeout = errors.New("handler budget exceeded")

// HandlerReplySink 下游应答写口(gateway 提供;宿主以入口 Framer 编码分帧)
type HandlerReplySink interface {
	JSON(status int, body string) error
	SSEStart() error
	Event(evtJSON string) error
	End() error
	ErrorFrame(evtJSON string) error
}

// HandlerInput 单次 handler 请求输入
type HandlerInput struct {
	Model      string
	Stream     bool
	Headers    map[string]string
	Body       string // 入口协议 JSON 文本(经 filters)
	Key        pipeline.KeyEntry
	Params     map[string]any // 与 protocol 路径同源的合并参数
	Transport  string         // 组参数解析传输槽(空名解析走 Transport(""))
	SessionKey string         // 会话亲和缺省素材(模型)
}

// HandlerDeps handler 引擎宿主依赖
type HandlerDeps struct {
	Package *Package
	Store   *SlotStore // ③ 权威槽
	Locks   *SlotLocks // 包级槽锁
	// Transport 传输名解析;不存在 = error("" = 组参数解析缺省槽/全局出站)
	Transport func(name string) (pipeline.Transport, error)
	Log       func(level, msg string)
	MaxBorrow int // handler_vm_max_borrow;≤0 = 缺省
}

// handlerExec 单次执行状态(登记表/应答状态机/source 账目)
type handlerExec struct {
	engine *HandlerEngine
	deps   HandlerDeps
	in     HandlerInput
	sink   HandlerReplySink

	ctx      context.Context
	cancel   context.CancelFunc
	deadline time.Time
	out      *outboundClient
	tx       *StorageTx        // ② 隔离缓冲
	held     map[string]func() // 持锁登记表(slot → release)
	heldMu   sync.Mutex
	sources  []*outboundSource
	srcMu    sync.Mutex
	vm       *goja.Runtime
	ctxObj   *goja.Object

	broken     atomic.Bool  // 执行级取消污染(实例丢弃)
	lastAct    atomic.Int64 // 最近原语活动(unixnano;纯计算超限判定锚点)
	prim       atomic.Int64 // 原语内计数(>0 = 阻塞在 Go 原语,不计纯计算)
	replyMode  string       // "" | "json" | "sse"
	sseStarted bool
	replyDone  bool
}

// HandlerEngine handler 池引擎((包,revision) 维度池 + 全局并发闸)
type HandlerEngine struct {
	deps HandlerDeps

	sem chan struct{}

	mu     sync.Mutex
	pools  map[string]*handlerPool
	closed bool
}

// NewHandlerEngine 构造
func NewHandlerEngine(deps HandlerDeps) *HandlerEngine {
	if deps.MaxBorrow <= 0 {
		deps.MaxBorrow = handlerVMMaxBorrowDft
	}
	return &HandlerEngine{
		deps:  deps,
		sem:   make(chan struct{}, handlerGlobalConcurr),
		pools: map[string]*handlerPool{},
	}
}

// Close 丢弃全部池(升级/热加载/看门狗软限;在借实例随归还丢弃)
func (e *HandlerEngine) Close() {
	e.mu.Lock()
	pools := e.pools
	e.pools = map[string]*handlerPool{}
	e.closed = true
	e.mu.Unlock()
	for _, p := range pools {
		p.Close()
	}
}

// poolFor 取或懒建 (包,revision) 池
func (e *HandlerEngine) poolFor() (*handlerPool, error) {
	key := e.deps.Package.Manifest.Name + "|" + fmt.Sprint(e.deps.Package.Revision)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errors.New("handler engine closed")
	}
	if p, ok := e.pools[key]; ok {
		return p, nil
	}
	p := newHandlerPool(e.deps.MaxBorrow, func() (*handlerInstance, error) {
		return buildHandlerInstance(e.deps)
	})
	e.pools[key] = p
	return p, nil
}

// Execute 直线执行:prepare → handler → 终局(锁兜底释放/②归并/source 回收)
// 返回错误 = 请求 502(注明包名由调用方);出站失败零补偿
func (e *HandlerEngine) Execute(parent context.Context, in HandlerInput, sink HandlerReplySink) error {
	ex := &handlerExec{
		engine: e, deps: e.deps, in: in, sink: sink,
		held: map[string]func(){},
	}
	ex.ctx, ex.cancel = context.WithTimeout(parent, handlerOverallBudget)
	ex.deadline = time.Now().Add(handlerOverallBudget)
	defer ex.cancel()
	startWatchdog(ex)

	// 池排队(全局并发闸)上界 60s,计入整体预算
	queueDeadline := time.Now().Add(handlerQueueTimeout)
	if ex.deadline.Before(queueDeadline) {
		queueDeadline = ex.deadline
	}
	timer := time.NewTimer(time.Until(queueDeadline))
	defer timer.Stop()
	select {
	case e.sem <- struct{}{}:
	case <-timer.C:
		return fmt.Errorf("handler %s: %w (global concurrency queue)", e.deps.Package.Manifest.Name, ErrHandlerTimeout)
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-e.sem }()

	// 缺省传输解析(组参数解析槽;失败 = 502)
	defTr, err := e.deps.Transport(in.Transport)
	if err != nil {
		return fmt.Errorf("handler %s: transport: %w", e.deps.Package.Manifest.Name, err)
	}
	ex.out = newOutboundClient(defTr, defaultOutboundTimeouts())
	ex.tx = NewStorageTx(nil) // ② 缓冲;③ 经 deps.Store

	pool, err := e.poolFor()
	if err != nil {
		return fmt.Errorf("handler %s: %w", e.deps.Package.Manifest.Name, err)
	}
	inst, err := pool.Borrow()
	if err != nil {
		return fmt.Errorf("handler %s: %w", e.deps.Package.Manifest.Name, err)
	}
	// panic 逃逸也不丢归还(WG 配对);broken 终值在归还时读
	defer func() { pool.Return(inst, ex.broken.Load()) }()
	res := ex.run(inst)
	return res
}

// startWatchdog 看门狗:纯计算超限(无原语活动且不在原语内)→ cancel(执行级取消,callJS 监听双发 Interrupt)
func startWatchdog(ex *handlerExec) {
	ex.lastAct.Store(time.Now().UnixNano())
	go func() {
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ex.ctx.Done():
				return
			case <-tick.C:
			}
			if ex.prim.Load() == 0 && time.Since(time.Unix(0, ex.lastAct.Load())) > handlerStatementCap {
				ex.broken.Store(true)
				ex.cancel()
				return
			}
		}
	}()
}

// touch 原语活动登记
func (ex *handlerExec) touch() { ex.lastAct.Store(time.Now().UnixNano()) }

// enter 原语进入登记(阻塞期豁免纯计算判定);返回退出函数(touch 先于计数归零,消除退出瞬间误杀窗口)
func (ex *handlerExec) enter() func() {
	ex.touch()
	ex.prim.Add(1)
	return func() {
		ex.touch()
		ex.prim.Add(-1)
	}
}

// run 池内执行体:构建 ctx → prepare → handler → 终局账目
func (ex *handlerExec) run(inst *handlerInstance) error {
	ex.vm = inst.vm
	ex.vm.ClearInterrupt() // 封死上一执行迟到中断对本次的误伤
	defer ex.finalize()

	ctxObj := ex.buildCtx(inst.prepareFn != nil)
	var data goja.Value = goja.Undefined()
	if inst.prepareFn != nil {
		v, err := ex.callJS("prepare", inst.prepareFn, ctxObj)
		if err != nil {
			return fmt.Errorf("handler %s: prepare: %w", ex.deps.Package.Manifest.Name, err)
		}
		data = v
		// handler 期 ctx:reply 解禁,data 重绑(prepare 持有的锁经登记表自然延续)
		ctxObj = ex.buildCtx(false)
	}
	if err := ex.bindData(data); err != nil {
		return fmt.Errorf("handler %s: %w", ex.deps.Package.Manifest.Name, err)
	}
	if _, err := ex.callJS("handler", inst.handlerFn, ctxObj); err != nil {
		if terr, ok := err.(*handlerError); ok {
			return ex.terminalError(terr)
		}
		return fmt.Errorf("handler %s: %w", ex.deps.Package.Manifest.Name, err)
	}
	return ex.normalTerminal()
}

// handlerError JS 层抛错(已写出应答的分叉判定需要原始错误)
type handlerError struct{ err error }

func (e *handlerError) Error() string { return e.err.Error() }
func (e *handlerError) Unwrap() error { return e.err }

// callJS 同步调用(执行级取消 = Interrupt 双发;同步性违规判定)
func (ex *handlerExec) callJS(name string, fn goja.Callable, ctxObj *goja.Object) (val goja.Value, err error) {
	vm := ex.vm
	vm.ClearInterrupt()
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ex.ctx.Done():
			vm.Interrupt(ErrHandlerTimeout)
		}
	}()
	defer close(done)
	defer func() {
		if rec := recover(); rec != nil {
			if errors.Is(asError(rec), ErrHandlerTimeout) {
				ex.broken.Store(true)
				err = &handlerError{fmt.Errorf("%s: %w", name, ErrHandlerTimeout)}
				return
			}
			err = &handlerError{fmt.Errorf("%s: %v", name, rec)}
		}
	}()
	v, callErr := fn(goja.Undefined(), ctxObj)
	if callErr != nil {
		if errors.Is(asError(callErr), ErrHandlerTimeout) || strings.Contains(callErr.Error(), ErrHandlerTimeout.Error()) {
			ex.broken.Store(true)
			return nil, &handlerError{fmt.Errorf("%s: %w", name, ErrHandlerTimeout)}
		}
		return nil, &handlerError{fmt.Errorf("%s: %w", name, callErr)}
	}
	if isThenable(vm, v) {
		return nil, &handlerError{fmt.Errorf("%s: async return (sync violation; no async execution environment)", name)}
	}
	return v, nil
}

// asError 恢复值归一
func asError(rec any) error {
	if e, ok := rec.(error); ok {
		return e
	}
	return fmt.Errorf("%v", rec)
}

// terminalError 终局错误分叉(零补偿)
func (ex *handlerExec) terminalError(terr *handlerError) error {
	if ex.replyMode == "json" && ex.replyDone {
		// 非流式已完整写出:下游已收应答,宿主正常关闭并 log
		ex.log("warn", fmt.Sprintf("handler %s: error after reply.json (downstream already complete): %v", ex.deps.Package.Manifest.Name, terr.err))
		return nil
	}
	if ex.sseStarted && ex.replyDone {
		// 流式已正常收尾(end 或 error 已写 = 视同收尾):同 json 分支语义
		ex.log("warn", fmt.Sprintf("handler %s: error after stream terminal (downstream already complete): %v", ex.deps.Package.Manifest.Name, terr.err))
		return nil
	}
	if ex.sseStarted && !ex.replyDone {
		// 流式已写出部分帧:规范错误帧收尾
		_ = ex.sink.ErrorFrame(mustJSON(map[string]any{"error": map[string]any{"message": terr.Error(), "package": ex.deps.Package.Manifest.Name}}))
	}
	return fmt.Errorf("handler %s: %w", ex.deps.Package.Manifest.Name, terr.err)
}

// normalTerminal 正常 return 终局:零应答 = 502;sse 未 end = 异常终局
func (ex *handlerExec) normalTerminal() error {
	if ex.replyMode == "" {
		return fmt.Errorf("handler %s: no reply called (empty response is a protocol violation)", ex.deps.Package.Manifest.Name)
	}
	if ex.replyMode == "sse" && !ex.replyDone {
		_ = ex.sink.ErrorFrame(mustJSON(map[string]any{"error": map[string]any{"message": "handler returned without reply.end", "package": ex.deps.Package.Manifest.Name}}))
	}
	return nil
}

// finalize 终局序:source 回收 → ②归并(整批单事务,权威槽丢弃 log warn)→ 登记表全量放锁
func (ex *handlerExec) finalize() {
	ex.srcMu.Lock()
	for _, src := range ex.sources {
		if src.consumed {
			_ = src.close()
			continue
		}
		// 情形 a:开了没读(既未 body 也未迭代)
		ex.log("warn", fmt.Sprintf("handler %s: outbound source not consumed, closed (url %s)", ex.deps.Package.Manifest.Name, src.req.URL))
		_ = src.close()
	}
	ex.sources = nil
	ex.srcMu.Unlock()
	// 终局序:归并先于放锁(排队者读③已含归并值);抛错/超时路径同序,超时即丢弃是有意语义的例外——
	// 超时执行 ctx 已取消,归并改用独立短界保证排队者仍能读到本执行落过的 unlock 值
	mctx, mcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mcancel()
	if dropped, err := ex.deps.Store.Merge(mctx, ex.tx.Snapshot()); err == nil {
		for _, k := range dropped {
			ex.log("warn", fmt.Sprintf("storage slot %q is authoritative; buffered write dropped (package %s)", k, ex.deps.Package.Manifest.Name))
		}
	} else {
		ex.log("warn", fmt.Sprintf("storage merge failed (package %s): %v", ex.deps.Package.Manifest.Name, err))
	}
	ex.heldMu.Lock()
	releases := ex.held
	ex.held = map[string]func(){}
	ex.heldMu.Unlock()
	for _, rel := range releases {
		rel()
	}
}

func (ex *handlerExec) log(level, msg string) {
	if ex.deps.Log != nil {
		ex.deps.Log(level, msg)
	}
}

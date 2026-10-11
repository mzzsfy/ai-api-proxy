// handler ctx 绑定:全部宿主原语挂调用参数对象(参数化注入,无全局写入面)
package plugin

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// settingsOverwriteKeys 出站超时设置面(显式可枚举;非法键抛错)
var settingsOverwriteKeys = map[string]bool{
	"connectTimeoutMs": true, "responseTimeoutMs": true,
	"frameIdleTimeoutMs": true, "totalTimeoutMs": true,
}

// jsCall 求值单参函数表达式并调用(避免冻结后的全局写入面)
func jsCall(vm *goja.Runtime, expr string, args ...goja.Value) error {
	v, err := vm.RunString(expr)
	if err != nil {
		return err
	}
	fn, ok := goja.AssertFunction(v)
	if !ok {
		return fmt.Errorf("js helper not a function")
	}
	_, err = fn(goja.Undefined(), args...)
	return err
}

// freezeDeep 深冻结(递归;严格模块下写任意层抛 TypeError)
func freezeDeep(vm *goja.Runtime, o goja.Value) {
	_ = jsCall(vm, `(function deep(v){
		if (v === null || typeof v !== "object") { return; }
		Object.freeze(v);
		for (const k of Object.getOwnPropertyNames(v)) { deep(v[k]); }
	})`, o)
}

// buildCtx 构造本次执行 ctx(prepare 期 data 为访问即抛错属性)
func (ex *handlerExec) buildCtx(preparePhase bool) *goja.Object {
	vm := ex.vm
	ctx := vm.NewObject()
	ex.ctxObj = ctx
	// request(只读)
	req := vm.NewObject()
	_ = req.Set("model", ex.in.Model)
	_ = req.Set("stream", ex.in.Stream)
	_ = req.Set("body", ex.in.Body)
	hd := vm.NewObject()
	for k, v := range ex.in.Headers {
		_ = hd.Set(k, v)
	}
	_ = req.Set("headers", hd)
	freezeDeep(vm, req)
	_ = ctx.Set("request", req)
	// params(只读)
	pmv := vm.ToValue(ex.in.Params)
	if o, ok := pmv.(*goja.Object); ok {
		freezeDeep(vm, o)
	}
	_ = ctx.Set("params", pmv)
	// key(只读)
	keyObj := vm.NewObject()
	_ = keyObj.Set("id", ex.in.Key.ID)
	_ = keyObj.Set("data", ex.in.Key.Data)
	freezeDeep(vm, keyObj)
	_ = ctx.Set("key", keyObj)

	_ = ctx.Set("storage", ex.bindStorage(vm))
	_ = ctx.Set("http", ex.bindHTTP(vm))
	_ = ctx.Set("reply", ex.bindReply(vm, preparePhase))
	_ = ctx.Set("settings", ex.bindSettings(vm))

	if preparePhase {
		// prepare 期 ctx.data 访问即抛错
		_ = jsCall(vm, `(function(ctx){
			Object.defineProperty(ctx, "data", {configurable: true, get: function(){
				throw new Error("ctx.data unavailable in prepare (prepare returns the handoff value)");
			}});
		})`, ctx)
	}
	return ctx
}

// bindData prepare 返回值 → ctx.data(执行局部数据袋,随意读写)
func (ex *handlerExec) bindData(data goja.Value) error {
	ex.vm.ClearInterrupt() // watcher 迟到中断不得误杀 bindData
	return jsCall(ex.vm, `(function(ctx, v){
		Object.defineProperty(ctx, "data", {value: v, writable: true, configurable: true});
	})`, ex.ctxObj, data)
}

// bindStorage ctx.storage:两级视图(②缓冲 + ③权威)+ getWithLock/unlock
func (ex *handlerExec) bindStorage(vm *goja.Runtime) *goja.Object {
	st := vm.NewObject()
	_ = st.Set("get", func(key string) (any, error) {
		ex.touch()
		// 读序含权威判定:权威槽直读③(判定与③值同只读事务快照);非权威 ② → ③
		v, auth, err := ex.deps.Store.GetAuth(key)
		if err != nil {
			return nil, err
		}
		if !auth {
			if bv, ok := ex.tx.Get(key); ok {
				return bv, nil
			}
		}
		if v == "" {
			return nil, nil
		}
		return v, nil
	})
	_ = st.Set("set", func(key, value string) error {
		ex.touch()
		if len(value) > MaxStorageValue {
			return fmt.Errorf("storage.set(%q): value exceeds limit", key)
		}
		if ex.deps.Locks.Held(key) {
			// 锁定槽写封闭(含持锁者自己;唯一写通道 = unlock)
			return fmt.Errorf("storage.set(%q): slot is locked (write channel is unlock(slot, value))", key)
		}
		return ex.tx.Set(key, value)
	})
	_ = st.Set("delete", func(key string) error {
		ex.touch()
		if ex.deps.Locks.Held(key) {
			return fmt.Errorf("storage.delete(%q): slot is locked (write channel is unlock(slot, value))", key)
		}
		ex.tx.Delete(key)
		return nil
	})
	_ = st.Set("getWithLock", func(slot string) (any, error) {
		defer ex.enter()()
		ex.heldMu.Lock()
		_, reentrant := ex.held[slot]
		ex.heldMu.Unlock()
		if reentrant {
			return nil, fmt.Errorf("storage.getWithLock(%q): slot already held by this execution (locks are non-reentrant)", slot)
		}
		rel, err := ex.deps.Locks.Acquire(slot, time.Until(ex.deadline), ex.ctx.Done())
		if err != nil {
			if err == ErrSlotWaitTimeout {
				return nil, fmt.Errorf("storage.getWithLock(%q): %w", slot, ErrHandlerTimeout)
			}
			return nil, fmt.Errorf("storage.getWithLock(%q): %w", slot, err)
		}
		if !ex.registerHeld(slot, rel) {
			rel()
			return nil, fmt.Errorf("storage.getWithLock(%q): execution cancelled", slot)
		}
		// 读源 = ③ 跳②(pre-lock 遗留缓冲随归并丢弃,不作决策依据)
		v, _, err := ex.deps.Store.GetAuth(slot)
		if err != nil {
			ex.releaseHeld(slot)
			return nil, fmt.Errorf("storage.getWithLock(%q): %w", slot, err)
		}
		if v == "" {
			return nil, nil
		}
		return v, nil
	})
	_ = st.Set("unlock", func(slot string, args ...goja.Value) (any, error) {
		defer ex.enter()()
		ex.heldMu.Lock()
		_, held := ex.held[slot]
		ex.heldMu.Unlock()
		if !held {
			return nil, fmt.Errorf("storage.unlock(%q): slot not held by this execution", slot)
		}
		if len(args) > 0 && !goja.IsUndefined(args[0]) && !goja.IsNull(args[0]) {
			sv, isStr := args[0].Export().(string)
			if !isStr {
				return nil, fmt.Errorf("storage.unlock(%q): value must be a string", slot)
			}
			if len(sv) > MaxStorageValue {
				// 白放语义:锁必释放 + 抛错
				ex.releaseHeld(slot)
				return nil, fmt.Errorf("storage.unlock(%q): value exceeds limit (released without write)", slot)
			}
			// 同步写③ + 打权威标(写超时独立于执行 ctx;失败 = 白放 + 抛错 + 锁必释放)
			if err := ex.deps.Store.UnlockWrite(slot, sv); err != nil {
				ex.releaseHeld(slot)
				return nil, fmt.Errorf("storage.unlock(%q): %w (released without write)", slot, err)
			}
		}
		ex.releaseHeld(slot)
		return goja.Undefined(), nil
	})
	return st
}

// registerHeld 登记持锁(cancel 竞态 = 未登记即放弃)
func (ex *handlerExec) registerHeld(slot string, rel func()) bool {
	ex.heldMu.Lock()
	defer ex.heldMu.Unlock()
	select {
	case <-ex.ctx.Done():
		return false
	default:
	}
	ex.held[slot] = rel
	return true
}

func (ex *handlerExec) releaseHeld(slot string) {
	ex.heldMu.Lock()
	rel, ok := ex.held[slot]
	delete(ex.held, slot)
	ex.heldMu.Unlock()
	if ok {
		rel()
	}
}

// bindHTTP ctx.http:request(缓冲)/ open(流式)
func (ex *handlerExec) bindHTTP(vm *goja.Runtime) *goja.Object {
	h := vm.NewObject()
	_ = h.Set("request", func(opts map[string]any) (map[string]any, error) {
		defer ex.enter()()
		r, client, err := ex.buildOutbound(opts, false)
		if err != nil {
			return nil, err
		}
		status, headers, body, err := client.Request(ex.ctx, r)
		if err != nil {
			return nil, err
		}
		return map[string]any{"status": status, "headers": headers, "body": string(body)}, nil
	})
	_ = h.Set("open", func(opts map[string]any) (*goja.Object, error) {
		defer ex.enter()()
		if s, ok := opts["stream"]; !ok || s != true {
			return nil, fmt.Errorf("http.open requires stream:true (buffered outbound uses http.request)")
		}
		r, client, err := ex.buildOutbound(opts, true)
		if err != nil {
			return nil, err
		}
		src, err := client.Open(ex.ctx, r)
		if err != nil {
			return nil, err
		}
		ex.srcMu.Lock()
		ex.sources = append(ex.sources, src)
		ex.srcMu.Unlock()
		return ex.bindSource(vm, src), nil
	})
	return h
}

// buildOutbound 出站参数解析(transport 名解析;timeoutMs = 本次覆写 total)
func (ex *handlerExec) buildOutbound(opts map[string]any, stream bool) (outboundReq, *outboundClient, error) {
	url, _ := opts["url"].(string)
	if url == "" {
		return outboundReq{}, nil, fmt.Errorf("http: url required")
	}
	method, _ := opts["method"].(string)
	if method == "" {
		method = "GET"
	}
	headers := map[string]string{}
	if hm, ok := opts["headers"].(map[string]any); ok {
		for k, v := range hm {
			if sv, ok := v.(string); ok {
				headers[k] = sv
			}
		}
	}
	body, _ := opts["body"].(string)
	sessionKey, _ := opts["sessionKey"].(string)
	if sessionKey == "" {
		sessionKey = ex.in.SessionKey
	}
	var timeoutMs int64
	if n, ok := numericField(opts["timeoutMs"]); ok {
		timeoutMs = n
	}
	r := outboundReq{
		URL: url, Method: method, Headers: headers, Body: []byte(body),
		SessionKey: sessionKey, TimeoutMs: timeoutMs, Stream: stream,
	}
	tname, _ := opts["transport"].(string)
	if tname == "" {
		return r, ex.out, nil
	}
	tr, err := ex.deps.Transport(tname)
	if err != nil {
		return outboundReq{}, nil, fmt.Errorf("http: transport %q not found", tname)
	}
	return r, newOutboundClient(tr, ex.out.callTimeouts()), nil
}

// bindSource 上游 source 导出:{status, headers, frames(), chunks(), body(), bodyBytes()}
func (ex *handlerExec) bindSource(vm *goja.Runtime, src *outboundSource) *goja.Object {
	o := vm.NewObject()
	_ = o.Set("status", src.status)
	hd := vm.NewObject()
	for k, v := range src.headers {
		_ = hd.Set(k, v)
	}
	_ = o.Set("headers", hd)
	_ = o.Set("frames", func() (*goja.Object, error) {
		ex.touch()
		it, err := src.Frames()
		if err != nil {
			return nil, err
		}
		iter := vm.NewObject()
		_ = iter.Set("next", func() (map[string]any, error) {
			defer ex.enter()()
			f, ok, err := it.Next(ex.ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				return map[string]any{"done": true}, nil
			}
			return map[string]any{"value": map[string]any{"event": f.Event, "data": f.Data}, "done": false}, nil
		})
		// Symbol.iterator 胶水(for-of 同步迭代器)
		if err := jsCall(vm, "(function(o){o[Symbol.iterator]=function(){return o}; return o})", iter); err != nil {
			return nil, err
		}
		return iter, nil
	})
	_ = o.Set("chunks", func() (*goja.Object, error) {
		ex.touch()
		it, err := src.Chunks()
		if err != nil {
			return nil, err
		}
		iter := vm.NewObject()
		var iterClosed bool
		var iterMu sync.Mutex
		var returnMu sync.Mutex
		var returnErr error
		_ = iter.Set("next", func() (map[string]any, error) {
			defer ex.enter()()
			iterMu.Lock()
			closed := iterClosed
			iterMu.Unlock()
			if closed {
				return map[string]any{"done": true}, nil
			}
			data, ok, err := it.Next(ex.ctx)
			iterMu.Lock()
			closedAfterNext := iterClosed
			if !closedAfterNext && (!ok || err != nil) {
				iterClosed = true
			}
			iterMu.Unlock()
			if closedAfterNext {
				return map[string]any{"done": true}, nil
			}
			if err != nil {
				return nil, err
			}
			if !ok {
				iterMu.Lock()
				iterClosed = true
				iterMu.Unlock()
				return map[string]any{"done": true}, nil
			}
			return map[string]any{"value": vm.NewArrayBuffer(data), "done": false}, nil
		})
		_ = iter.Set("return", func() (map[string]any, error) {
			returnMu.Lock()
			defer returnMu.Unlock()
			iterMu.Lock()
			alreadyClosed := iterClosed
			iterClosed = true
			priorErr := returnErr
			iterMu.Unlock()
			if alreadyClosed {
				if priorErr != nil {
					return nil, priorErr
				}
				return map[string]any{"done": true}, nil
			}
			err := src.close()
			iterMu.Lock()
			returnErr = err
			iterMu.Unlock()
			if err != nil {
				return nil, err
			}
			return map[string]any{"done": true}, nil
		})
		if err := jsCall(vm, "(function(o){o[Symbol.iterator]=function(){return o}; return o})", iter); err != nil {
			return nil, err
		}
		return iter, nil
	})
	_ = o.Set("body", func() (string, error) {
		defer ex.enter()()
		b, err := src.Body(ex.ctx)
		if err != nil {
			return "", err
		}
		return string(b), nil
	})
	_ = o.Set("bodyBytes", func() (goja.ArrayBuffer, error) {
		defer ex.enter()()
		b, err := src.BodyBytes(ex.ctx)
		if err != nil {
			return goja.ArrayBuffer{}, err
		}
		return vm.NewArrayBuffer(b), nil
	})
	return o
}

// bindReply ctx.reply 状态机(越序/形态违规全抛错;prepare 期全禁)
func (ex *handlerExec) bindReply(vm *goja.Runtime, preparePhase bool) *goja.Object {
	r := vm.NewObject()
	fail := func(format string, a ...any) {
		panic(vm.NewGoError(fmt.Errorf(format, a...)))
	}
	if preparePhase {
		// prepare 禁 reply:应答未开始
		for _, m := range []string{"json", "sse", "event", "end", "error"} {
			name := m
			_ = r.Set(name, func(...goja.Value) {
				fail("reply.%s: unavailable in prepare (reply starts in handler)", name)
			})
		}
		return r
	}
	claim := func(mode, what string) {
		ex.touch()
		if ex.replyDone {
			fail("reply.%s: reply already finished", what)
		}
		if ex.replyMode == "" {
			ex.replyMode = mode
		} else if ex.replyMode != mode {
			fail("reply.%s: %s form already used", what, ex.replyMode)
		}
	}
	_ = r.Set("json", func(status int, body string) {
		claim("json", "json")
		if ex.in.Stream {
			fail("reply.json: entry stream=true (reply.sse is the only streaming answer form)")
		}
		if status == 0 {
			status = 200
		}
		if err := ex.sink.JSON(status, body); err != nil {
			fail("reply.json: %v", err)
		}
		ex.replyDone = true
	})
	_ = r.Set("sse", func() {
		claim("sse", "sse")
		if !ex.in.Stream {
			fail("reply.sse: entry stream=false (use reply.json; pseudo-streaming aggregates then sse)")
		}
		if err := ex.sink.SSEStart(); err != nil {
			fail("reply.sse: %v", err)
		}
		ex.sseStarted = true
	})
	_ = r.Set("event", func(evt map[string]any) {
		ex.touch()
		if ex.replyMode != "sse" || ex.replyDone {
			fail("reply.event: requires reply.sse() first and no terminal yet")
		}
		b, err := json.Marshal(evt)
		if err != nil {
			fail("reply.event: marshal: %v", err)
		}
		if err := ex.sink.Event(string(b)); err != nil {
			fail("reply.event: %v", err)
		}
	})
	_ = r.Set("end", func() {
		ex.touch()
		if ex.replyMode != "sse" || ex.replyDone {
			fail("reply.end: requires reply.sse() first and no terminal yet")
		}
		if err := ex.sink.End(); err != nil {
			fail("reply.end: %v", err)
		}
		ex.replyDone = true
	})
	_ = r.Set("error", func(evt map[string]any) {
		ex.touch()
		if ex.replyMode != "sse" {
			fail("reply.error: requires reply.sse() first (non-streaming errors throw)")
		}
		if ex.replyDone {
			fail("reply.error: reply already finished")
		}
		b, err := json.Marshal(evt)
		if err != nil {
			fail("reply.error: marshal: %v", err)
		}
		if err := ex.sink.ErrorFrame(string(b)); err != nil {
			fail("reply.error: %v", err)
		}
		ex.replyDone = true
	})
	return r
}

// bindSettings ctx.settings:执行级出站超时覆写(合并语义;非法键抛错)
func (ex *handlerExec) bindSettings(vm *goja.Runtime) *goja.Object {
	s := vm.NewObject()
	_ = s.Set("overwrite", func(patch map[string]any) error {
		ex.touch()
		nv := outboundTimeouts{}
		for k, v := range patch {
			if !settingsOverwriteKeys[k] {
				return fmt.Errorf("settings.overwrite(%q): unknown key (allowed: connectTimeoutMs responseTimeoutMs frameIdleTimeoutMs totalTimeoutMs)", k)
			}
			ms, ok := numericField(v)
			if !ok || ms <= 0 {
				return fmt.Errorf("settings.overwrite(%q): positive milliseconds required", k)
			}
			d := time.Duration(ms) * time.Millisecond
			switch k {
			case "connectTimeoutMs":
				nv.Connect = d
			case "responseTimeoutMs":
				nv.Response = d
			case "frameIdleTimeoutMs":
				nv.FrameIdle = d
			case "totalTimeoutMs":
				nv.Total = d
			}
		}
		ex.out.overwrite(nv)
		return nil
	})
	return s
}

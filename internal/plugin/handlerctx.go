// handler ctx 绑定:全部宿主原语挂调用参数对象(参数化注入,无全局写入面)
package plugin

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// settingsOverwriteKeys 出站超时设置面(显式可枚举;非法键抛错)
var settingsOverwriteKeys = map[string]bool{
	"connectTimeoutMs": true, "responseTimeoutMs": true,
	"frameIdleTimeoutMs": true, "totalTimeoutMs": true,
}

// ctxGlueJS ctx 对象装配胶水:宿主原语(全局 __h2_*)包装为方法(参数归一/undefined 语义/迭代器形态)
// 迭代器 chunk 载荷经 JSON 数组文本 → Uint8Array(quickjs 桥无 ArrayBuffer 通道)
const ctxGlueJS = `
globalThis.__bytesFromJSON = function (t) { return Uint8Array.from(JSON.parse(t)); };
globalThis.__srcSlot = {v: null};
globalThis.__h2_glue = function (ctx, fns, preparePhase) {
  ctx.storage = {
    get: function (k) { var v = fns.st_get(String(k)); return v === null ? undefined : v; },
    set: function (k, v) { return fns.st_set(String(k), String(v)); },
    delete: function (k) { fns.st_del(String(k)); },
    getWithLock: function (slot) { var v = fns.st_lock(String(slot)); return v === null ? undefined : v; },
    unlock: function (slot, value) {
      if (value === undefined || value === null) { fns.st_unlock(String(slot), false, ""); return undefined; }
      if (typeof value !== "string") { fns.st_unlock(String(slot), false, ""); throw new Error("storage.unlock(" + JSON.stringify(String(slot)) + "): value must be a string"); }
      fns.st_unlock(String(slot), true, value);
      return undefined;
    }
  };
  ctx.http = {
    request: function (opts) { return fns.http_req(JSON.stringify(opts === undefined ? {} : opts)); },
    open: function (opts) { fns.http_open(JSON.stringify(opts === undefined ? {} : opts)); return globalThis.__srcSlot.v; }
  };
  ctx.reply = {
    json: function (status, body) { fns.reply_json(status | 0, String(body)); },
    sse: function () { fns.reply_sse(); },
    event: function (evt) { fns.reply_event(JSON.stringify(evt === undefined ? {} : evt)); },
    end: function () { fns.reply_end(); },
    error: function (evt) { fns.reply_error(JSON.stringify(evt === undefined ? {} : evt)); }
  };
  ctx.settings = { overwrite: function (patch) { fns.settings_overwrite(JSON.stringify(patch === undefined ? {} : patch)); } };
  if (preparePhase) {
    Object.defineProperty(ctx, "data", {configurable: true, get: function(){
      throw new Error("ctx.data unavailable in prepare (prepare returns the handoff value)");
    }});
  }
};
globalThis.__h2_iter = function (nextFn, returnFn) {
  var o = {next: nextFn};
  if (returnFn) { o.return = returnFn; }
  o[Symbol.iterator] = function () { return o; };
  return o;
};
globalThis.__h2_bindSource = function (o, base, fns) {
  o.frames = function () {
    fns.frames_open(base);
    return globalThis.__h2_iter(function () {
      var r = fns.frames_next(base);
      if (r === null) { return {done: true}; }
      return JSON.parse(r);
    });
  };
  o.chunks = function () {
    var closed = false;
    fns.chunks_open(base);
    return globalThis.__h2_iter(function () {
      if (closed) { return {done: true}; }
      var r = fns.chunks_next(base);
      if (r === null) { closed = true; return {done: true}; }
      return {value: globalThis.__bytesFromJSON(r), done: false};
    }, function () {
      if (closed) { return {done: true}; }
      fns.chunks_return(base);
      closed = true;
      return {done: true};
    });
  };
  o.body = function () { return fns.src_body(base); };
  o.bodyBytes = function () { return globalThis.__bytesFromJSON(fns.src_body_bytes(base)); };
};
`

// buildCtx 构造本次执行 ctx(prepare 期 data 为访问即抛错属性)
func (ex *handlerExec) buildCtx(preparePhase bool) (jsengine.Value, error) {
	inst := ex.inst
	ctx, err := inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	ex.ctxObj = ctx
	fns, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer fns.Free()
	// ctx 原语经实例级注册的宿主函数(闭包读 ex.cur;池内串行安全)
	for _, p := range []struct{ prop, global string }{
		{"st_get", "__h2_st_get"}, {"st_set", "__h2_st_set"}, {"st_del", "__h2_st_del"},
		{"st_lock", "__h2_st_lock"}, {"st_unlock", "__h2_st_unlock"},
		{"http_req", "__h2_http_req"}, {"http_open", "__h2_http_open"},
		{"reply_json", "__h2_reply_json"}, {"reply_sse", "__h2_reply_sse"},
		{"reply_event", "__h2_reply_event"}, {"reply_end", "__h2_reply_end"}, {"reply_error", "__h2_reply_error"},
		{"settings_overwrite", "__h2_settings_overwrite"},
	} {
		fn, err := inst.GetGlobal(p.global)
		if err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
		if err := inst.SetProp(fns, p.prop, fn); err != nil {
			fn.Free()
			ctx.Free()
			return jsengine.Value{}, err
		}
		fn.Free()
	}
	// request(只读)
	req, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer req.Free()
	if err := inst.SetProp(req, "model", ex.in.Model); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(req, "stream", ex.in.Stream); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(req, "body", ex.in.Body); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	hd, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer hd.Free()
	for k, v := range ex.in.Headers {
		if err := inst.SetProp(hd, k, v); err != nil {
			ctx.Free()
			return jsengine.Value{}, err
		}
	}
	if err := inst.SetProp(req, "headers", hd); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.CallExpression("(function(v){(function deep(o){if(o===null||typeof o!==\"object\"){return;}Object.freeze(o);for(const k of Object.getOwnPropertyNames(o)){deep(o[k]);}})(v)})", req); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(ctx, "request", req); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	// params(只读;JSON 通道值拷贝)
	if err := inst.SetProp(ctx, "params", ex.in.Params); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	// key(只读)
	keyObj, err := inst.NewObject()
	if err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	defer keyObj.Free()
	if err := inst.SetProp(keyObj, "id", ex.in.Key.ID); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(keyObj, "data", ex.in.Key.Data); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(ctx, "key", keyObj); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	// 装配(ctx 持有 fns 引用;fns 局部引用即释)
	pp := 0
	if preparePhase {
		pp = 1
	}
	if err := inst.CallExpression(`(function(ctx, fns, pp){ globalThis.__h2_glue(ctx, fns, pp === 1); })`, ctx, fns, pp); err != nil {
		ctx.Free()
		return jsengine.Value{}, err
	}
	return ctx, nil
}

// bindData prepare 返回值 → ctx.data(执行局部数据袋,随意读写)
func (ex *handlerExec) bindData(data jsengine.Value) error {
	if data.VM() == nil {
		return nil
	}
	return ex.inst.CallExpression(`(function(ctx, v){
		Object.defineProperty(ctx, "data", {value: v, writable: true, configurable: true});
	})`, ex.ctxObj, data)
}

// ctxHostArgs 宿主原语当前执行路由(池内串行:借出期 ex.cur 恒有效)
func (hi *handlerInstance) exec() *handlerExec {
	return hi.cur
}

// registerCtxHostFuncs 注册 handler ctx 宿主原语(实例级单次;名称不随执行增长,随 VM.Close 释放)
func (hi *handlerInstance) registerCtxHostFuncs() error {
	reg := func(name string, fn func(args []any) (any, error)) error {
		return hi.env.q.inst.RegisterFunc(name, fn)
	}
	argStr := func(args []any, i int) string {
		if i < len(args) {
			s, _ := args[i].(string)
			return s
		}
		return ""
	}
	unmarshalObj := func(args []any, i int) (map[string]any, error) {
		s := argStr(args, i)
		if s == "" {
			return map[string]any{}, nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	regBool := func(args []any, i int) bool {
		if i < len(args) {
			b, _ := args[i].(bool)
			return b
		}
		return false
	}
	if err := reg("__h2_st_get", func(args []any) (any, error) {
		ex := hi.exec()
		ex.touch()
		v, auth, err := ex.deps.Store.GetAuth(argStr(args, 0))
		if err != nil {
			return nil, err
		}
		if !auth {
			if bv, ok := ex.tx.Get(argStr(args, 0)); ok {
				return bv, nil
			}
		}
		if v == "" {
			return nil, nil
		}
		return v, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_st_set", func(args []any) (any, error) {
		ex := hi.exec()
		key, value := argStr(args, 0), argStr(args, 1)
		ex.touch()
		if len(value) > MaxStorageValue {
			return nil, fmt.Errorf("storage.set(%q): value exceeds limit", key)
		}
		if ex.deps.Locks.Held(key) {
			// 锁定槽写封闭(含持锁者自己;唯一写通道 = unlock)
			return nil, fmt.Errorf("storage.set(%q): slot is locked (write channel is unlock(slot, value))", key)
		}
		return nil, ex.tx.Set(key, value)
	}); err != nil {
		return err
	}
	if err := reg("__h2_st_del", func(args []any) (any, error) {
		ex := hi.exec()
		key := argStr(args, 0)
		ex.touch()
		if ex.deps.Locks.Held(key) {
			return nil, fmt.Errorf("storage.delete(%q): slot is locked (write channel is unlock(slot, value))", key)
		}
		ex.tx.Delete(key)
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_st_lock", func(args []any) (any, error) {
		ex := hi.exec()
		slot := argStr(args, 0)
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
	}); err != nil {
		return err
	}
	if err := reg("__h2_st_unlock", func(args []any) (any, error) {
		ex := hi.exec()
		slot := argStr(args, 0)
		hasValue := regBool(args, 1)
		sv := argStr(args, 2)
		defer ex.enter()()
		ex.heldMu.Lock()
		_, held := ex.held[slot]
		ex.heldMu.Unlock()
		if !held {
			return nil, fmt.Errorf("storage.unlock(%q): slot not held by this execution", slot)
		}
		if hasValue {
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
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_http_req", func(args []any) (any, error) {
		ex := hi.exec()
		defer ex.enter()()
		opts, err := unmarshalObj(args, 0)
		if err != nil {
			return nil, err
		}
		r, client, err := ex.buildOutbound(opts, false)
		if err != nil {
			return nil, err
		}
		status, headers, body, err := client.Request(ex.ctx, r)
		if err != nil {
			return nil, err
		}
		return map[string]any{"status": status, "headers": headers, "body": string(body)}, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_http_open", func(args []any) (any, error) {
		ex := hi.exec()
		defer ex.enter()()
		opts, err := unmarshalObj(args, 0)
		if err != nil {
			return nil, err
		}
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
		// source 对象挂预分配容器(freeze 只保护 globalThis 顶层;嵌套属性可写,host func 无引用返回)
		srcVal, err := ex.bindSource(src)
		if err != nil {
			_ = src.close()
			return nil, err
		}
		slot, err := ex.inst.GetGlobal("__srcSlot")
		if err != nil {
			srcVal.Free()
			_ = src.close()
			return nil, err
		}
		defer slot.Free()
		if err := ex.inst.SetProp(slot, "v", srcVal); err != nil {
			srcVal.Free()
			_ = src.close()
			return nil, err
		}
		srcVal.Free()
		return true, nil
	}); err != nil {
		return err
	}
	for _, rp := range []struct{ name, mode string }{
		{"__h2_reply_json", "json"}, {"__h2_reply_sse", "sse"},
	} {
		rp := rp
		if err := reg(rp.name, func(args []any) (any, error) {
			ex := hi.exec()
			if ex.preparePhase {
				return nil, fmt.Errorf("reply.%s: unavailable in prepare (reply starts in handler)", rp.mode)
			}
			if rp.mode == "json" {
				status := 200
				if len(args) > 0 {
					if n, ok := args[0].(int); ok && n != 0 {
						status = n
					}
				}
				if err := ex.claimReply("json", "json"); err != nil {
					return nil, err
				}
				if ex.in.Stream {
					return nil, fmt.Errorf("reply.json: entry stream=true (reply.sse is the only streaming answer form)")
				}
				if err := ex.sink.JSON(status, argStr(args, 1)); err != nil {
					return nil, fmt.Errorf("reply.json: %w", err)
				}
				ex.replyDone = true
				return nil, nil
			}
			if err := ex.claimReply("sse", "sse"); err != nil {
				return nil, err
			}
			if !ex.in.Stream {
				return nil, fmt.Errorf("reply.sse: entry stream=false (use reply.json; pseudo-streaming aggregates then sse)")
			}
			if err := ex.sink.SSEStart(); err != nil {
				return nil, fmt.Errorf("reply.sse: %w", err)
			}
			ex.sseStarted = true
			return nil, nil
		}); err != nil {
			return err
		}
	}
	if err := reg("__h2_reply_event", func(args []any) (any, error) {
		ex := hi.exec()
		ex.touch()
		evt, err := unmarshalObj(args, 0)
		if err != nil {
			return nil, err
		}
		if ex.replyMode != "sse" || ex.replyDone {
			return nil, fmt.Errorf("reply.event: requires reply.sse() first and no terminal yet")
		}
		b, err := json.Marshal(evt)
		if err != nil {
			return nil, fmt.Errorf("reply.event: marshal: %w", err)
		}
		if err := ex.sink.Event(string(b)); err != nil {
			return nil, fmt.Errorf("reply.event: %w", err)
		}
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_reply_end", func(args []any) (any, error) {
		ex := hi.exec()
		ex.touch()
		if ex.preparePhase {
			return nil, fmt.Errorf("reply.end: unavailable in prepare (reply starts in handler)")
		}
		if ex.replyMode != "sse" || ex.replyDone {
			return nil, fmt.Errorf("reply.end: requires reply.sse() first and no terminal yet")
		}
		if err := ex.sink.End(); err != nil {
			return nil, fmt.Errorf("reply.end: %w", err)
		}
		ex.replyDone = true
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_reply_error", func(args []any) (any, error) {
		ex := hi.exec()
		ex.touch()
		evt, err := unmarshalObj(args, 0)
		if err != nil {
			return nil, err
		}
		if ex.replyMode != "sse" {
			return nil, fmt.Errorf("reply.error: requires reply.sse() first (non-streaming errors throw)")
		}
		if ex.replyDone {
			return nil, fmt.Errorf("reply.error: reply already finished")
		}
		b, err := json.Marshal(evt)
		if err != nil {
			return nil, fmt.Errorf("reply.error: marshal: %w", err)
		}
		if err := ex.sink.ErrorFrame(string(b)); err != nil {
			return nil, fmt.Errorf("reply.error: %w", err)
		}
		ex.replyDone = true
		return nil, nil
	}); err != nil {
		return err
	}
	return reg("__h2_settings_overwrite", func(args []any) (any, error) {
		ex := hi.exec()
		ex.touch()
		patch, err := unmarshalObj(args, 0)
		if err != nil {
			return nil, err
		}
		nv := outboundTimeouts{}
		for k, v := range patch {
			if !settingsOverwriteKeys[k] {
				return nil, fmt.Errorf("settings.overwrite(%q): unknown key (allowed: connectTimeoutMs responseTimeoutMs frameIdleTimeoutMs totalTimeoutMs)", k)
			}
			ms, ok := numericField(v)
			if !ok || ms <= 0 {
				return nil, fmt.Errorf("settings.overwrite(%q): positive milliseconds required", k)
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
		return nil, nil
	})
}

// srcOf base → 迭代状态(宿主原语定位;未知 base = 程序缺陷)
func (hi *handlerInstance) srcOf(args []any) *srcState {
	base, _ := args[0].(string)
	ex := hi.exec()
	ex.srcMu.Lock()
	defer ex.srcMu.Unlock()
	return ex.srcStates[base]
}

// bytesJSONText 字节 → JSON 数字数组文本(json.Marshal 对 []byte 走 base64,桥通道不可用)
func bytesJSONText(b []byte) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, c := range b {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(int(c)))
	}
	sb.WriteByte(']')
	return sb.String()
}

// registerSrcHostFuncs 注册 source 迭代原语(实例级单次;base 首参定位)
func (hi *handlerInstance) registerSrcHostFuncs() error {
	reg := func(name string, fn func(args []any) (any, error)) error {
		return hi.env.q.inst.RegisterFunc(name, fn)
	}
	if err := reg("__h2_srcFramesOpen", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		it, err := st.src.Frames()
		if err != nil {
			return nil, err
		}
		st.framesIt = it
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_srcFramesNext", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		f, ok, err := st.framesIt.Next(st.ex.ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return `{"done":true}`, nil
		}
		b, err := json.Marshal(map[string]any{"value": map[string]any{"event": f.Event, "data": f.Data}, "done": false})
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_srcChunksOpen", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		it, err := st.src.Chunks()
		if err != nil {
			return nil, err
		}
		st.chunksIt = it
		return nil, nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_srcChunksNext", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		data, ok, err := st.chunksIt.Next(st.ex.ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return bytesJSONText(data), nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_srcChunksReturn", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		if err := st.src.close(); err != nil {
			return nil, err
		}
		return "1", nil
	}); err != nil {
		return err
	}
	if err := reg("__h2_srcBody", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		b, err := st.src.Body(st.ex.ctx)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}); err != nil {
		return err
	}
	return reg("__h2_srcBodyBytes", func(args []any) (any, error) {
		st := hi.srcOf(args)
		defer st.ex.enter()()
		b, err := st.src.BodyBytes(st.ex.ctx)
		if err != nil {
			return nil, err
		}
		return bytesJSONText(b), nil
	})
}

// claimReply reply 形态认领(越序/形态违规抛错)
func (ex *handlerExec) claimReply(mode, what string) error {
	ex.touch()
	if ex.replyDone {
		return fmt.Errorf("reply.%s: reply already finished", what)
	}
	if ex.replyMode == "" {
		ex.replyMode = mode
		return nil
	}
	if ex.replyMode != mode {
		return fmt.Errorf("reply.%s: %s form already used", what, ex.replyMode)
	}
	return nil
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

// srcState 上游 source 迭代状态(base 定位;迭代器在宿主侧推进)
type srcState struct {
	ex       *handlerExec
	src      *outboundSource
	framesIt *frameIter
	chunksIt *chunkIter
}

// bindSource 上游 source 导出:{status, headers, frames(), chunks(), body(), bodyBytes()}
// 二进制通道 = JSON 数组文本 → Uint8Array(quickjs 桥无 ArrayBuffer;eventstream.decode 兼容 view)
func (ex *handlerExec) bindSource(src *outboundSource) (jsengine.Value, error) {
	inst := ex.inst
	ex.srcSeq++
	base := fmt.Sprintf("s%d", ex.srcSeq)
	ex.srcMu.Lock()
	ex.srcStates[base] = &srcState{ex: ex, src: src}
	ex.srcMu.Unlock()
	o, err := inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	if err := inst.SetProp(o, "status", src.status); err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	hd, err := inst.NewObject()
	if err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	defer hd.Free()
	for k, v := range src.headers {
		if err := inst.SetProp(hd, k, v); err != nil {
			o.Free()
			return jsengine.Value{}, err
		}
	}
	if err := inst.SetProp(o, "headers", hd); err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	fns, err := inst.NewObject()
	if err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	defer fns.Free()
	if err := inst.SetProp(fns, "base", base); err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	for _, p := range []struct{ prop, global string }{
		{"frames_open", "__h2_srcFramesOpen"}, {"frames_next", "__h2_srcFramesNext"},
		{"chunks_open", "__h2_srcChunksOpen"}, {"chunks_next", "__h2_srcChunksNext"},
		{"chunks_return", "__h2_srcChunksReturn"},
		{"src_body", "__h2_srcBody"}, {"src_body_bytes", "__h2_srcBodyBytes"},
	} {
		fn, err := inst.GetGlobal(p.global)
		if err != nil {
			o.Free()
			return jsengine.Value{}, err
		}
		if err := inst.SetProp(fns, p.prop, fn); err != nil {
			fn.Free()
			o.Free()
			return jsengine.Value{}, err
		}
		fn.Free()
	}
	if err := inst.CallExpression("(function(o, fns){ globalThis.__h2_bindSource(o, fns.base, fns); })", o, fns); err != nil {
		o.Free()
		return jsengine.Value{}, err
	}
	return o, nil
}

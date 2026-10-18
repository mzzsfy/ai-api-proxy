package jsengine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/quickjs"
)

// idleSpinLimit drain 空转阈值(超过后休眠让出核,等待异步宿主推进)
const idleSpinLimit = 1024

var errNoVM = errors.New("js engine: instance closed")

// Call 经异常包装调用 JS 函数;返回值(调用方负责 Free)或 EngineError
func (i *Instance) Call(fn Value, args ...any) (Value, error) {
	if i.vm == nil {
		return Value{}, errNoVM
	}
	full := make([]any, 0, len(args)+1)
	full = append(full, fn.raw())
	for _, a := range args {
		if jv, ok := a.(Value); ok {
			full = append(full, jv.raw())
			continue
		}
		full = append(full, a)
	}
	out, err := i.hostCall.CallValue(i.undef, full...)
	if err != nil {
		return Value{}, i.wrapEngineErr(err)
	}
	defer out.Free()
	if ok := i.propBool(out, "ok"); ok {
		return i.GetProp(Value{i.vm, out}, "v")
	}
	msg := i.propStr(out, "msg")
	stack := i.propStr(out, "stack")
	return Value{}, classifyEngineErr(msg, stack)
}

// Await 等待钩子结果 settled:
// 非 Promise 原样返回(wasPromise=false);Promise 则 drain 微任务直至 settled 或预算耗尽。
// rejection 原值经 errors.As(errRejected) 取出,由调用方用 ExceptionOf 转换。
func (i *Instance) Await(v Value, budget time.Duration) (result Value, wasPromise bool, rejected error) {
	if i.vm == nil {
		return Value{}, false, errNoVM
	}
	isP, err := i.isPromise(v)
	if err != nil {
		return Value{}, false, err
	}
	if !isP {
		return v.Dup(), false, nil
	}
	type settled struct {
		val      Value
		failVal  Value
		failed   bool
	}
	ch := make(chan settled, 1)
	if _, err := v.raw().Then(
		func(okVal quickjs.Value) { ch <- settled{val: Value{i.vm, okVal.Dup()}} },
		func(failVal quickjs.Value) { ch <- settled{failVal: Value{i.vm, failVal.Dup()}, failed: true} },
	); err != nil {
		return Value{}, true, i.wrapEngineErr(err)
	}
	if err := i.SetBudget(budget); err != nil {
		return Value{}, true, err
	}
	deadline := time.Now().Add(budget)
	idle := 0
	for {
		select {
		case s := <-ch:
			if s.failed {
				return s.failVal, true, errRejected{val: s.failVal}
			}
			return s.val, true, nil
		default:
		}
		if _, err := i.vm.ExecutePendingJobs(); err != nil {
			return Value{}, true, i.wrapEngineErr(err)
		}
		if time.Now().After(deadline) {
			i.vm.Interrupt()
			return Value{}, true, &EngineError{Kind: KindInterrupt, Message: "await budget exceeded"}
		}
		idle++
		if idle > idleSpinLimit {
			time.Sleep(time.Microsecond)
		}
	}
}

// errRejected Promise rejection 原值载体(errors.As 取出)
type errRejected struct{ val Value }

func (e errRejected) Error() string { return "js promise rejected" }

// ExceptionOf rejection 原值 → EngineError(Error 对象取 message/stack;原始值 String 化)
func (i *Instance) ExceptionOf(v Value) *EngineError {
	if v.IsUndefined() || v.IsNull() {
		return &EngineError{Kind: KindException, Message: "rejected"}
	}
	stack := i.propStr(v.raw(), "stack")
	e := &EngineError{Kind: KindException, Stack: stack}
	if msg, ok := i.propString(v, "message"); ok {
		e.Message = msg
	} else {
		e.Message = v.String()
	}
	return classifyEngineErr(e.Message, stack)
}

// classifyEngineErr 按 message 分型(quickjs 以 message 区分 OOM 与中断;catch 包装后仍可辨)
func classifyEngineErr(msg, stack string) *EngineError {
	kind := KindException
	switch {
	case strings.Contains(msg, "out of memory"):
		kind = KindMemory
	case strings.Contains(msg, "interrupted"):
		kind = KindInterrupt
	}
	return &EngineError{Kind: kind, Message: msg, Stack: stack}
}

// propStr 读对象字符串属性(即取即释;非字符串返回空)
func (i *Instance) propStr(obj quickjs.Value, name string) string {
	s, _ := i.propString(Value{i.vm, obj}, name)
	return s
}

// propString 读对象字符串属性
func (i *Instance) propString(obj Value, name string) (string, bool) {
	atom, err := i.vm.NewAtom(name)
	if err != nil {
		return "", false
	}
	pv, err := i.vm.GetPropertyValue(obj.raw(), atom)
	if err != nil {
		return "", false
	}
	defer pv.Free()
	a, err := pv.Any()
	if err != nil {
		return "", false
	}
	s, ok := a.(string)
	return s, ok
}

// propBool 读对象布尔属性
func (i *Instance) propBool(obj quickjs.Value, name string) bool {
	atom, err := i.vm.NewAtom(name)
	if err != nil {
		return false
	}
	pv, err := i.vm.GetPropertyValue(obj, atom)
	if err != nil {
		return false
	}
	defer pv.Free()
	a, err := pv.Any()
	if err != nil {
		return false
	}
	b, _ := a.(bool)
	return b
}

// isPromise 类型探测
func (i *Instance) isPromise(v Value) (bool, error) {
	r, err := i.isPromiseFn.CallValue(i.undef, v.raw())
	if err != nil {
		return false, i.wrapEngineErr(err)
	}
	defer r.Free()
	a, err := r.Any()
	if err != nil {
		return false, err
	}
	b, ok := a.(bool)
	if !ok {
		return false, fmt.Errorf("js engine: promise probe returned %T", a)
	}
	return b, nil
}

// IsFunction 导出值是否函数
func (i *Instance) IsFunction(v Value) (bool, error) {
	r, err := i.isFunctionFn.CallValue(i.undef, v.raw())
	if err != nil {
		return false, i.wrapEngineErr(err)
	}
	defer r.Free()
	a, err := r.Any()
	if err != nil {
		return false, err
	}
	b, _ := a.(bool)
	return b, nil
}

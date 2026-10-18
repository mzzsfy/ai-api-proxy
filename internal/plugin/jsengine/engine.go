// Package jsengine 封装 modernc.org/quickjs 为业务引擎实例:
// 单 goroutine 独占、内存硬上限、超时中断、错误带 JS 堆栈、宿主等待 Promise settled。
package jsengine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/quickjs"
)

// DefaultMemoryLimit 单实例内存上限默认值
const DefaultMemoryLimit = 96 * 1024 * 1024

// ErrKind 错误分类(池 broken 判定与请求侧错误分型依据)
type ErrKind int

const (
	// KindException JS 异常(插件代码抛出)
	KindException ErrKind = iota
	// KindMemory 内存超限(实例必须丢弃重建)
	KindMemory
	// KindInterrupt 超时中断(实例可能污染,按 broken 处理)
	KindInterrupt
	// KindEngine 引擎级错误(非 JS 异常,实例不可信)
	KindEngine
)

// EngineError 引擎错误;Stack 为 QuickJS 堆栈(函数名+行号+列号)
type EngineError struct {
	Kind    ErrKind
	Message string
	Stack   string
}

// Error 日志形态:message 首行 + 堆栈缩进行(小白可原样回贴)
func (e *EngineError) Error() string {
	if e.Stack == "" {
		return e.Message
	}
	return e.Message + "\n" + e.Stack
}

// newEngineError 按异常 message 分型(quickjs 以 message 区分 OOM 与中断)
func newEngineError(msg, stack string) *EngineError {
	kind := KindException
	switch {
	case strings.Contains(msg, "out of memory"):
		kind = KindMemory
	case strings.Contains(msg, "interrupted"):
		kind = KindInterrupt
	}
	return &EngineError{Kind: kind, Message: msg, Stack: stack}
}

// Program 编译缓存(字节码自包含,跨实例共享)
type Program struct {
	Name string
	bc   []byte
}

// NewProgram 编译源码为可跨实例执行的字节码
// NewProgram 编译源码为字节码缓存(跨实例共享;语法错误带行号)
func NewProgram(name, src string) (*Program, error) {
	vm, err := quickjs.NewVM()
	if err != nil {
		return nil, fmt.Errorf("compile vm: %w", err)
	}
	defer func() { _ = vm.Close() }()
	bc, err := vm.Compile(src, quickjs.EvalGlobal)
	if err != nil {
		var qe *quickjs.Error
		if errors.As(err, &qe) && qe.LineNumber > 0 {
			return nil, fmt.Errorf("compile %s: Line %d: %s", name, qe.LineNumber, qe.Message)
		}
		return nil, fmt.Errorf("compile %s: %w", name, err)
	}
	return &Program{Name: name, bc: bc}, nil
}

// Option 实例构造参数;零值字段取默认
type Option struct {
	// MemoryLimit 单实例内存硬上限;≤0 取 DefaultMemoryLimit
	MemoryLimit int64
	// MaxStackSize JS 栈上限;≤0 用引擎默认
	MaxStackSize int
}

// Instance 单 JS 引擎实例;同一时刻仅允许一个 goroutine 使用(与池借用语义一致)
type Instance struct {
	vm     *quickjs.VM
	global quickjs.Value
	undef  quickjs.Value
	// hostCall JS 侧调用包装:统一异常捕获与堆栈采集
	hostCall quickjs.Value
	// hostProbe 类型探测 helper
	isPromiseFn  quickjs.Value
	isFunctionFn quickjs.Value
}

// hostHelpers 异常包装与类型探测;异常捕获把 e.message/e.stack 固化为宿主可读对象
const hostHelpers = `
globalThis.__hostCall = function (fn) {
  var args = Array.prototype.slice.call(arguments, 1);
  try {
    return { ok: true, v: fn.apply(undefined, args) };
  } catch (e) {
    return {
      ok: false,
      msg: (e && e.message !== undefined) ? String(e.message) : String(e),
      stack: (e && typeof e.stack === "string") ? e.stack : ""
    };
  }
};
globalThis.__hostIsPromise = function (v) { return !!(v && typeof v.then === "function"); };
globalThis.__hostIsFunction = function (v) { return typeof v === "function"; };
globalThis.__hostToGo = function (v) {
  var s = JSON.stringify(v, function (k, x) { return x === undefined ? null : x; });
  return s === undefined ? "null" : s;
};
`

// New 构造实例(内存上限默认生效;helper 注入失败=构造失败)
func New(opt Option) (*Instance, error) {
	limit := opt.MemoryLimit
	if limit <= 0 {
		limit = DefaultMemoryLimit
	}
	vm, err := quickjs.NewVM()
	if err != nil {
		return nil, fmt.Errorf("js engine: %w", err)
	}
	inst := &Instance{vm: vm}
	if opt.MaxStackSize > 0 {
		vm.SetMaxStackSize(uintptr(opt.MaxStackSize))
	}
	vm.SetMemoryLimit(uintptr(limit))
	if _, err := vm.Eval(hostHelpers, quickjs.EvalGlobal); err != nil {
		_ = vm.Close()
		return nil, fmt.Errorf("js engine helpers: %w", err)
	}
	for _, p := range []struct {
		name string
		dst  *quickjs.Value
	}{
		{"__hostCall", &inst.hostCall},
		{"__hostIsPromise", &inst.isPromiseFn},
		{"__hostIsFunction", &inst.isFunctionFn},
	} {
		v, err := vm.EvalValue(p.name, quickjs.EvalGlobal)
		if err != nil {
			_ = inst.Close()
			return nil, fmt.Errorf("js engine helper %s: %w", p.name, err)
		}
		*p.dst = v
	}
	inst.global = vm.GlobalObject()
	if inst.undef, err = vm.EvalValue("undefined", quickjs.EvalGlobal); err != nil {
		_ = inst.Close()
		return nil, fmt.Errorf("js engine undefined: %w", err)
	}
	return inst, nil
}

// Close 释放实例;调用后实例不可再用
func (i *Instance) Close() error {
	for _, v := range []*quickjs.Value{&i.hostCall, &i.isPromiseFn, &i.isFunctionFn, &i.undef, &i.global} {
		if v.VM() != nil {
			v.Free()
			*v = quickjs.Value{}
		}
	}
	if i.vm == nil {
		return nil
	}
	vm := i.vm
	i.vm = nil
	return vm.Close()
}

// EvalString 执行源码片段(工具注入等);错误带堆栈
func (i *Instance) EvalString(src string) error {
	_, err := i.vm.Eval(src, quickjs.EvalGlobal)
	if err != nil {
		return i.wrapEngineErr(err)
	}
	return nil
}

// EvalExpression 求值表达式并返回值(调用方负责 Free)
func (i *Instance) EvalExpression(src string) (Value, error) {
	v, err := i.vm.EvalValue(src, quickjs.EvalGlobal)
	if err != nil {
		return Value{}, i.wrapEngineErr(err)
	}
	return Value{i, v}, nil
}

// CallExpression 求值单参函数表达式并以 args 调用(冻结/属性定义等 JS 胶水)
func (i *Instance) CallExpression(expr string, args ...any) error {
	fn, err := i.EvalExpression(expr)
	if err != nil {
		return err
	}
	defer fn.Free()
	out, err := i.Call(fn, args...)
	if err != nil {
		return err
	}
	out.Free()
	return nil
}

// RunProgram 执行编译缓存(实例构造阶段)
func (i *Instance) RunProgram(p *Program) error {
	if _, err := i.vm.EvalBytecode(p.bc); err != nil {
		return i.wrapEngineErr(err)
	}
	return nil
}

// RunProgramValue 执行编译缓存并返回求值结果(模块装载;调用方负责 Free)
func (i *Instance) RunProgramValue(p *Program) (Value, error) {
	v, err := i.vm.EvalBytecodeValue(p.bc)
	if err != nil {
		return Value{}, i.wrapEngineErr(err)
	}
	return Value{i, v}, nil
}

// RegisterFunc 注册宿主函数为全局名;fn 返回非 nil error 时以 JS 异常抛出
func (i *Instance) RegisterFunc(name string, fn func(args []any) (any, error)) error {
	return i.vm.RegisterHostFunc(name, fn)
}

// SetGlobal 设置全局值;包装 Value 走引用路径(quickjs SetPropertyValue 内部 Dup,调用方保留原引用;map 等未知类型会被引擎 JSON 深拷贝)
func (i *Instance) SetGlobal(name string, v any) error {
	atom, err := i.vm.NewAtom(name)
	if err != nil {
		return err
	}
	if jv, ok := v.(Value); ok {
		return i.vm.SetPropertyValue(i.global, atom, jv.raw())
	}
	return i.vm.SetProperty(i.global, atom, v)
}

// NewObject 构造空 JS 对象(调用方负责 Free)
func (i *Instance) NewObject() (Value, error) {
	v, err := i.vm.NewObjectValue()
	if err != nil {
		return Value{}, err
	}
	return Value{i, v}, nil
}

// SetProp 设置对象属性(标量/Value;quickjs SetPropertyValue 内部 Dup,调用方保留原引用)
func (i *Instance) SetProp(obj Value, name string, v any) error {
	atom, err := i.vm.NewAtom(name)
	if err != nil {
		return err
	}
	if jv, ok := v.(Value); ok {
		return i.vm.SetPropertyValue(obj.v, atom, jv.raw())
	}
	return i.vm.SetProperty(obj.v, atom, v)
}

// GetProp 读对象属性(调用方负责 Free)
func (i *Instance) GetProp(obj Value, name string) (Value, error) {
	atom, err := i.vm.NewAtom(name)
	if err != nil {
		return Value{}, err
	}
	v, err := i.vm.GetPropertyValue(obj.v, atom)
	if err != nil {
		return Value{}, i.wrapEngineErr(err)
	}
	return Value{i, v}, nil
}

// GetGlobal 读全局(调用方负责 Free)
func (i *Instance) GetGlobal(name string) (Value, error) {
	return i.GetProp(Value{i, i.global}, name)
}

// Undef this 参数用 undefined(实例持有,禁止 Free)
func (i *Instance) Undef() Value { return Value{i, i.undef} }

// SetBudget 设置下一次 JS 入口的墙钟预算(Call/Await 自动布防)
func (i *Instance) SetBudget(d time.Duration) error {
	if d <= 0 {
		return errors.New("js engine: budget must be positive")
	}
	return i.vm.SetEvalTimeout(d)
}

// Interrupt 中断当前 JS 执行(超时预算到点;实例按 broken 处理)
func (i *Instance) Interrupt() { i.vm.Interrupt() }

// wrapEngineErr quickjs 原生 error → EngineError(无法再取异常对象,栈信息已丢失,仅限引擎级路径)
func (i *Instance) wrapEngineErr(err error) error {
	var qe *quickjs.Error
	if errors.As(err, &qe) {
		return newEngineError(qe.Error(), "")
	}
	return &EngineError{Kind: KindEngine, Message: err.Error()}
}

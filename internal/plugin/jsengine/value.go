package jsengine

import (
	"fmt"

	"modernc.org/quickjs"
)

// Value JS 值引用;引用计数语义:取得后必须恰一次 Free,禁止拷贝结构体后重复 Free
type Value struct {
	vmI *Instance
	v   quickjs.Value
}

// Free 释放引用;调用后不可再用(零值/未关联为安全空操作)
func (v Value) Free() {
	if v.v.VM() == nil {
		return
	}
	v.v.Free()
}

// VM 关联实例;nil = 零值/已释放
func (v Value) VM() *Instance {
	if v.v.VM() == nil {
		return nil
	}
	return v.vmI
}

// Dup 增引用副本(与原引用各自独立释放)
func (v Value) Dup() Value {
	return Value{v.vmI, v.v.Dup()}
}

// Any 导出为 Go 值(string/int/float64/bool/*Object/nil=null/Undefined{}=undefined);不释放自身
func (v Value) Any() (any, error) {
	return v.v.Any()
}

// JSON 序列化为 JSON 文本
func (v Value) JSON() ([]byte, error) {
	return v.v.MarshalJSON()
}

// IsUndefined 是否 JS undefined
func (v Value) IsUndefined() bool { return v.v.IsUndefined() }

// IsNull 是否 JS null(Any() 导出 nil)
func (v Value) IsNull() bool {
	a, err := v.v.Any()
	if err != nil {
		return false
	}
	return a == nil
}

// String 调试形态
func (v Value) String() string {
	a, err := v.v.Any()
	if err != nil {
		return fmt.Sprintf("<err %v>", err)
	}
	return fmt.Sprint(a)
}

// raw 内部透传(包内)
func (v Value) raw() quickjs.Value { return v.v }

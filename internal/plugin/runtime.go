package plugin

import (
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

//go:embed jshelper/eventstream.js
var eventStreamHelper string

const MaxStorageValue = 64 * 1024

// KeyRef 当前键引用(请求级/逐键注入;JS 经 ctx.key 或 util.key() 读取,无按名通道)
type KeyRef = pipeline.KeyEntry

// HostDeps 部件宿主依赖(注入 util;密钥唯一出口 = ctx.key / util.key() 当前键)
type HostDeps struct {
	PackageName string
	// Key 当前键(请求级选键/逐键执行注入;可空=util.key 返回 undefined)
	Key *KeyRef
	// PackageKeyValues 当前键数据字符串叶子(inspect/log 脱敏用;可空)
	PackageKeyValues func() []string
	// TransportEvict 主动失效上报(管理命令转发;仅失效,不触发重试;可空=util.evict 报错)
	TransportEvict func(transport, scope, value string) error
	Storage        StorageKV
	Log            func(level, msg string)
}

// StorageKV 部件存储(ns=包名,由 host 固定)
type StorageKV interface {
	Get(key string) (string, bool)
	Set(key, value string) error
	Delete(key string)
}

// Compile 编译部件源码(字节码跨实例共享;语法错误带位置)
func Compile(src []byte, entry string) (*jsengine.Program, error) {
	pr, err := jsengine.NewProgram(entry, string(src))
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", entry, err)
	}
	return pr, nil
}

// instantiate 运行部件:CJS 声明由宿主注入(不进源码文本,保持错误行号=源文件行号)
func instantiate(prog *jsengine.Program, entry string, config any, deps HostDeps) (*qjsRuntime, jsengine.Value, error) {
	q, err := newQJSRuntime(deps, jsengine.Option{})
	if err != nil {
		return nil, jsengine.Value{}, err
	}
	exports, err := q.inst.NewObject()
	if err != nil {
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	module, err := q.inst.NewObject()
	if err != nil {
		exports.Free()
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	if err := q.inst.SetProp(module, "exports", exports); err != nil {
		exports.Free()
		module.Free()
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	if config != nil {
		for _, step := range []func() error{
			func() error { return q.inst.SetGlobal("module", module) },
			func() error { return q.inst.SetGlobal("exports", exports) },
			func() error { return q.inst.SetGlobal("__config", config) },
		} {
			if err := step(); err != nil {
				exports.Free()
				module.Free()
				_ = q.Close()
				return nil, jsengine.Value{}, fmt.Errorf("%s: globals: %w", entry, err)
			}
		}
	} else {
		for _, step := range []func() error{
			func() error { return q.inst.SetGlobal("module", module) },
			func() error { return q.inst.SetGlobal("exports", exports) },
			func() error { return q.inst.EvalString("globalThis.__config = undefined;") },
		} {
			if err := step(); err != nil {
				exports.Free()
				module.Free()
				_ = q.Close()
				return nil, jsengine.Value{}, fmt.Errorf("%s: globals: %w", entry, err)
			}
		}
	}
	module.Free() // 全局引用已持;局部引用释放
	if err := q.inst.RunProgram(prog); err != nil {
		_ = q.Close()
		return nil, jsengine.Value{}, fmt.Errorf("run %s: %w", entry, err)
	}
	mv, err := q.inst.GetGlobal("module")
	if err != nil {
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	out, err := q.inst.GetProp(mv, "exports")
	mv.Free()
	if err != nil {
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	if out.IsUndefined() || out.IsNull() {
		out.Free()
		_ = q.Close()
		return nil, jsengine.Value{}, fmt.Errorf("%s: module.exports empty", entry)
	}
	// factory 形态:exports(config) → hooks 对象(v2 参数经 __config 闭包)
	isFn, err := q.inst.IsFunction(out)
	if err != nil {
		out.Free()
		_ = q.Close()
		return nil, jsengine.Value{}, err
	}
	if isFn {
		cfg, cerr := q.inst.GetGlobal("__config")
		if cerr != nil {
			out.Free()
			_ = q.Close()
			return nil, jsengine.Value{}, cerr
		}
		defer cfg.Free()
		hooksObj, cerr := q.inst.Call(out, cfg)
		out.Free()
		if cerr != nil {
			_ = q.Close()
			return nil, jsengine.Value{}, fmt.Errorf("%s: factory: %w", entry, cerr)
		}
		return q, hooksObj, nil
	}
	return q, out, nil
}

// hookInstance 单个 JS 实例(池内并行,实例内串行)
type hookInstance struct {
	id     int64 // 工厂序号(测试观测补建)
	q      *qjsRuntime
	hooks  *Hooks
	keyRef *KeyRef // 键槽(deps.Key 指向;出池时按 pctx.Key 无条件重绑,含空值清槽)
}

// closeInstance 释放实例(nil q = 测试桩)
func (inst *hookInstance) closeInstance() {
	if inst == nil {
		return
	}
	if inst.hooks != nil {
		inst.hooks.release()
	}
	if inst.q != nil {
		_ = inst.q.Close()
	}
}

// newInstance 池工厂:实例化 + hook 抽取(keyRef 槽 = deps.Key;适配器出池重绑)
func newInstance(prog *jsengine.Program, entry string, config any, deps HostDeps) (*hookInstance, error) {
	keyRef := &KeyRef{}
	if deps.Key != nil {
		*keyRef = *deps.Key
	}
	deps.Key = keyRef // 闭包持槽指针,运行期读值随重绑变化
	q, obj, err := instantiate(prog, entry, config, deps)
	if err != nil {
		return nil, err
	}
	hooks, err := extractHooks(q.inst, obj)
	obj.Free()
	if err != nil {
		_ = q.Close()
		return nil, err
	}
	return &hookInstance{q: q, hooks: hooks, keyRef: keyRef}, nil
}

// ProbeHooks 结构探测:求值一次取钩子存在性(不进池;探测实例即毁,不外借 JS 引用)
func ProbeHooks(prog *jsengine.Program) (hasMapEvent, hasMapResponse bool, err error) {
	inst, err := newInstance(prog, "", map[string]any{}, HostDeps{})
	if err != nil {
		return false, false, err
	}
	defer inst.closeInstance()
	return inst.hooks.MapEvent.VM() != nil, inst.hooks.MapResponse.VM() != nil, nil
}

// Hooks 部件导出的 hook 函数集;恰一次直发语义下无 refresh
type Hooks struct {
	BuildRequest jsengine.Value
	MapEvent     jsengine.Value
	MapResponse  jsengine.Value
	MapError     jsengine.Value
	MapRequest   jsengine.Value
	MapChunk     jsengine.Value
}

// release 释放持有的导出函数引用
func (h *Hooks) release() {
	for _, v := range []*jsengine.Value{&h.BuildRequest, &h.MapEvent, &h.MapResponse, &h.MapError, &h.MapRequest, &h.MapChunk} {
		if v.VM() != nil {
			v.Free()
			*v = jsengine.Value{}
		}
	}
}

// hasAny 至少一个主钩子已导出
func (h *Hooks) hasAny() bool {
	return h.BuildRequest.VM() != nil || h.MapEvent.VM() != nil || h.MapResponse.VM() != nil ||
		h.MapRequest.VM() != nil || h.MapChunk.VM() != nil
}

// extractHooks 从导出对象抽取 hook;双向绑定校验由调用方按 Supports 执行
func extractHooks(inst *jsengine.Instance, obj jsengine.Value) (*Hooks, error) {
	get := func(name string) jsengine.Value {
		v, err := inst.GetProp(obj, name)
		if err != nil {
			return jsengine.Value{}
		}
		defer v.Free()
		if v.IsUndefined() || v.IsNull() {
			return jsengine.Value{}
		}
		isFn, err := inst.IsFunction(v)
		if err != nil || !isFn {
			return jsengine.Value{}
		}
		return v.Dup()
	}
	h := &Hooks{
		BuildRequest: get("buildRequest"),
		MapEvent:     get("mapEvent"),
		MapResponse:  get("mapResponse"),
		MapError:     get("mapError"),
		MapRequest:   get("mapRequest"),
		MapChunk:     get("mapChunk"),
	}
	if !h.hasAny() {
		h.release()
		return nil, fmt.Errorf("no hooks exported")
	}
	return h, nil
}

// valueToMap JS 导出值 → Go map(JSON 通道;非对象 = false)
func valueToMap(v jsengine.Value) (map[string]any, bool) {
	if v.VM() == nil || v.IsUndefined() || v.IsNull() {
		return nil, false
	}
	b, err := v.JSON()
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false
	}
	return m, true
}

// mergeMaps 深合并;b 覆盖,数组整体替换
func mergeMaps(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if am, ok := out[k].(map[string]any); ok {
			if bm, ok := v.(map[string]any); ok {
				out[k] = mergeMaps(am, bm)
				continue
			}
		}
		out[k] = v
	}
	return out
}

// pathSeg 路径段:键或数组索引
type pathSeg struct {
	key   string
	idx   int
	isIdx bool
}

// getPath 点路径取值("a.b[0].c")
func getPath(m map[string]any, path string) any {
	cur := any(m)
	for _, seg := range splitPath(path) {
		if seg.isIdx {
			c, ok := cur.([]any)
			if !ok || seg.idx < 0 || seg.idx >= len(c) {
				return nil
			}
			cur = c[seg.idx]
			continue
		}
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = cm[seg.key]
	}
	return cur
}

// setPath 点路径设值(副本语义;嵌套 map/slice 自动创建扩容)
func setPath(m map[string]any, path string, v any) map[string]any {
	segs := splitPath(path)
	if len(segs) == 0 {
		return m
	}
	out, ok := setIn(any(m), segs, v).(map[string]any)
	if !ok {
		return m
	}
	return out
}

// setIn 在容器副本上按段写值,返回新容器
func setIn(container any, segs []pathSeg, v any) any {
	seg := segs[0]
	if seg.isIdx {
		if seg.idx < 0 {
			return container
		}
		arr, _ := container.([]any)
		cp := make([]any, len(arr), maxLen(len(arr), seg.idx+1))
		copy(cp, arr)
		for len(cp) <= seg.idx {
			cp = append(cp, nil)
		}
		if len(segs) == 1 {
			cp[seg.idx] = v
		} else {
			cp[seg.idx] = setIn(cp[seg.idx], segs[1:], v)
		}
		return cp
	}
	m, _ := container.(map[string]any)
	cp := make(map[string]any, len(m)+1)
	for k, val := range m {
		cp[k] = val
	}
	if len(segs) == 1 {
		cp[seg.key] = v
	} else {
		cp[seg.key] = setIn(m[seg.key], segs[1:], v)
	}
	return cp
}

// maxLen 较大值
func maxLen(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// splitPath 点路径解析:支持 a.b[0].c 与 [0].x 形态;索引段 isIdx
func splitPath(path string) []pathSeg {
	var out []pathSeg
	for _, part := range strings.Split(path, ".") {
		for part != "" {
			if strings.HasPrefix(part, "[") {
				end := strings.Index(part, "]")
				if end < 0 {
					out = append(out, pathSeg{key: part})
					break
				}
				if idx, err := strconv.Atoi(part[1:end]); err == nil {
					out = append(out, pathSeg{idx: idx, isIdx: true})
				} else {
					out = append(out, pathSeg{key: part[1:end]})
				}
				part = part[end+1:]
				continue
			}
			key := part
			if bracket := strings.Index(part, "["); bracket >= 0 {
				key = part[:bracket]
				part = part[bracket:]
			} else {
				part = ""
			}
			if key != "" {
				out = append(out, pathSeg{key: key})
			}
		}
	}
	return out
}

// renderTemplate {key} 占位替换;未匹配保留原文
func renderTemplate(s string, vars map[string]any) string {
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// 由长到短避免前缀吞噬
	for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
		keys[i], keys[j] = keys[j], keys[i]
	}
	for _, k := range keys {
		token := "{" + k + "}"
		if strings.Contains(s, token) {
			v := vars[k]
			switch tv := v.(type) {
			case string:
				s = strings.ReplaceAll(s, token, tv)
			default:
				b, _ := json.Marshal(tv)
				s = strings.ReplaceAll(s, token, string(b))
			}
		}
	}
	return s
}

// secretMaskOutput 凭据值在输出中的替换串
const secretMaskOutput = "***"

// maskSecrets inspect/log 输出统一处理:当前键字符串叶子替换 + 截断
func maskSecrets(deps HostDeps, s string) string {
	if deps.PackageKeyValues != nil {
		for _, val := range deps.PackageKeyValues() {
			if val != "" {
				s = strings.ReplaceAll(s, val, secretMaskOutput)
			}
		}
	}
	const limit = 2048
	if len(s) > limit {
		return s[:limit]
	}
	return s
}

// readBigEndian64Bytes 大端 8 字节读(quickjs __u_bigEndian 路径)
func readBigEndian64Bytes(view []byte, offset int, signed bool) (*big.Int, error) {
	if offset > len(view) || len(view)-offset < 8 {
		return nil, fmt.Errorf("64-bit integer read out of bounds")
	}
	value := new(big.Int).SetUint64(binary.BigEndian.Uint64(view[offset : offset+8]))
	if signed && value.Bit(63) != 0 {
		value.Sub(value, new(big.Int).Lsh(big.NewInt(1), 64))
	}
	return value, nil
}

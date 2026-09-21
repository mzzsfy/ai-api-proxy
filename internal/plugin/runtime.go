package plugin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"
)

// MaxStorageValue storage 单键上限
const MaxStorageValue = 64 * 1024

// TargetCursor 当前 target 跟踪(每部件实例一份,hook 调用时更新)
type TargetCursor struct {
	mu   sync.Mutex
	name string
}

// Set 更新当前 target
func (t *TargetCursor) Set(name string) { t.mu.Lock(); t.name = name; t.mu.Unlock() }

// Get 读取当前 target
func (t *TargetCursor) Get() string { t.mu.Lock(); defer t.mu.Unlock(); return t.name }

// HostDeps 部件宿主依赖(注入 util)
type HostDeps struct {
	PackageName string
	Cursor      *TargetCursor
	// TargetSecrets 按 (target 名, 键) 解析凭据;"当前 target" 由 Cursor 承载
	TargetSecrets func(target, key string) (string, bool)
	// TargetSecretValues 当前 target 全量凭据值(inspect/log 脱敏用;可空)
	TargetSecretValues func(target string) map[string]string
	// PackageKey 包级 key 只读(实时;hooks 任务写入;可空=util.key 报错)
	PackageKey func(name string) (any, bool)
	// TransportEvict 主动失效上报(管理命令转发;仅失效,不触发重试;可空=util.evict 报错)
	TransportEvict func(transport, scope, value string) error
	Storage        StorageKV
	Log            func(level, msg string)
}

// currentTarget 当前 target 名
func (d *HostDeps) currentTarget() string {
	if d.Cursor == nil {
		return ""
	}
	return d.Cursor.Get()
}

// StorageKV 部件存储(ns=包名,由 host 固定)
type StorageKV interface {
	Get(key string) (string, bool)
	Set(key, value string) error
	Delete(key string)
}

// Compile 编译部件源码(语法错误带位置)
func Compile(src []byte, entry string) (*goja.Program, error) {
	pr, err := goja.Compile(entry, string(src), true)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", entry, err)
	}
	return pr, nil
}

// bindUtil 构造 util 对象注入 runtime
func bindUtil(vm *goja.Runtime, deps HostDeps) {
	util := vm.NewObject()
	_ = util.Set("deepMerge", func(a, b goja.Value) goja.Value {
		am, aok := toMap(vm, a)
		bm, bok := toMap(vm, b)
		if !aok || !bok {
			panic(vm.NewGoError(fmt.Errorf("deepMerge requires two objects")))
		}
		return vm.ToValue(mergeMaps(am, bm))
	})
	_ = util.Set("deepClone", func(v goja.Value) goja.Value {
		var tmp any
		b, _ := json.Marshal(v.Export())
		_ = json.Unmarshal(b, &tmp)
		return vm.ToValue(tmp)
	})
	_ = util.Set("get", func(v goja.Value, path string) goja.Value {
		m, ok := toMap(vm, v)
		if !ok {
			return goja.Undefined()
		}
		return vm.ToValue(getPath(m, path))
	})
	_ = util.Set("set", func(v goja.Value, path string, nv goja.Value) goja.Value {
		m, ok := toMap(vm, v)
		if !ok {
			panic(vm.NewGoError(fmt.Errorf("set requires object")))
		}
		return vm.ToValue(setPath(m, path, nv.Export()))
	})
	_ = util.Set("pick", func(v goja.Value, keys []string) goja.Value {
		m, ok := toMap(vm, v)
		if !ok {
			panic(vm.NewGoError(fmt.Errorf("pick requires object")))
		}
		out := map[string]any{}
		for _, k := range keys {
			if val, ok := m[k]; ok {
				out[k] = val
			}
		}
		return vm.ToValue(out)
	})
	_ = util.Set("omit", func(v goja.Value, keys []string) goja.Value {
		m, ok := toMap(vm, v)
		if !ok {
			panic(vm.NewGoError(fmt.Errorf("omit requires object")))
		}
		drop := map[string]bool{}
		for _, k := range keys {
			drop[k] = true
		}
		out := map[string]any{}
		for k, val := range m {
			if !drop[k] {
				out[k] = val
			}
		}
		return vm.ToValue(out)
	})
	_ = util.Set("b64encode", func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) })
	_ = util.Set("b64decode", func(s string) (string, error) {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return "", fmt.Errorf("b64decode: %w", err)
		}
		return string(b), nil
	})
	_ = util.Set("b64urlEncode", func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) })
	_ = util.Set("b64urlDecode", func(s string) (string, error) {
		b, err := base64.URLEncoding.DecodeString(s)
		if err != nil {
			return "", fmt.Errorf("b64urlDecode: %w", err)
		}
		return string(b), nil
	})
	_ = util.Set("uuid", func() string {
		return uuid.NewString()
	})
	_ = util.Set("now", func() int64 {
		return time.Now().UnixMilli()
	})
	_ = util.Set("isoNow", func() string {
		return time.Now().UTC().Format(time.RFC3339)
	})
	_ = util.Set("sha256hex", func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	})
	_ = util.Set("hmacSha256hex", func(key, s string) string {
		mac := hmac.New(sha256.New, []byte(key))
		mac.Write([]byte(s))
		return hex.EncodeToString(mac.Sum(nil))
	})
	_ = util.Set("template", func(str string, vars goja.Value) string {
		vm2 := vm
		m, ok := toMap(vm2, vars)
		if !ok {
			return str
		}
		return renderTemplate(str, m)
	})
	_ = util.Set("inspect", func(v goja.Value) string {
		b, _ := json.Marshal(v.Export())
		return maskSecrets(deps, string(b))
	})
	// secret:唯一凭据出口;键存在返回值(可为空串=显式匿名),键缺失抛错;"当前 target" 由 adapter 维护
	_ = util.Set("secret", func(ref string) (string, error) {
		if deps.TargetSecrets == nil {
			return "", fmt.Errorf("secret %q: no target context (package %s)", ref, deps.PackageName)
		}
		v, ok := deps.TargetSecrets(deps.currentTarget(), ref)
		if !ok {
			return "", fmt.Errorf("secret %q missing in target secrets (package %s)", ref, deps.PackageName)
		}
		return v, nil
	})
	// key:包级 key 只读出口(hooks 任务写入)
	_ = util.Set("key", func(name string) (any, error) {
		if deps.PackageKey == nil {
			return nil, fmt.Errorf("key %q: no keys context (package %s)", name, deps.PackageName)
		}
		v, ok := deps.PackageKey(name)
		if !ok {
			return nil, nil
		}
		return v, nil
	})
	// evict:主动失效上报出口(管理命令转发;仅失效当前绑定,不改当前请求重试行为)
	_ = util.Set("evict", func(transport, scope, value string) (bool, error) {
		if deps.TransportEvict == nil {
			return false, fmt.Errorf("evict %q: no transport context (package %s)", transport, deps.PackageName)
		}
		if err := deps.TransportEvict(transport, scope, value); err != nil {
			return false, err
		}
		return true, nil
	})
	_ = vm.Set("util", util)
	// log(输出经 secrets 掩码)
	logObj := vm.NewObject()
	for _, lvl := range []string{"info", "warn", "error"} {
		level := lvl
		_ = logObj.Set(level, func(args ...goja.Value) {
			if deps.Log != nil {
				deps.Log(level, maskSecrets(deps, sprintArgs(vm, args)))
			}
		})
	}
	logWrapper := vm.NewObject()
	_ = logWrapper.Set("info", logObj.Get("info"))
	_ = logWrapper.Set("warn", logObj.Get("warn"))
	_ = logWrapper.Set("error", logObj.Get("error"))
	_ = vm.Set("log", logWrapper)
	// storage(ns=包名)
	st := vm.NewObject()
	if deps.Storage != nil {
		_ = st.Set("get", func(key string) (any, error) {
			v, ok := deps.Storage.Get(key)
			if !ok {
				return nil, nil
			}
			return v, nil
		})
		_ = st.Set("set", func(key, value string) error {
			if len(value) > MaxStorageValue {
				return fmt.Errorf("storage value exceeds limit")
			}
			return deps.Storage.Set(key, value)
		})
		_ = st.Set("delete", func(key string) { deps.Storage.Delete(key) })
	} else {
		_ = st.Set("get", func(string) (any, error) { return nil, nil })
		_ = st.Set("set", func(string, string) error { return fmt.Errorf("storage unavailable") })
		_ = st.Set("delete", func(string) {})
	}
	_ = vm.Set("storage", st)
}

// instantiate 运行部件:CommonJS 包装 + factory 检测(config 闭包注入)
func instantiate(prog *goja.Program, entry string, config any, deps HostDeps) (*goja.Runtime, goja.Value, error) {
	vm := goja.New()
	bindUtil(vm, deps)
	var exports, module *goja.Object
	exports = vm.NewObject()
	module = vm.NewObject()
	_ = module.Set("exports", exports)
	_ = vm.Set("module", module)
	_ = vm.Set("exports", exports)
	if config != nil {
		_ = vm.Set("__config", config)
	} else {
		_ = vm.Set("__config", goja.Undefined())
	}
	if _, err := vm.RunProgram(prog); err != nil {
		return nil, nil, fmt.Errorf("run %s: %w", entry, err)
	}
	mv := module.Get("exports")
	if mv == nil || goja.IsUndefined(mv) || goja.IsNull(mv) {
		return nil, nil, fmt.Errorf("%s: module.exports empty", entry)
	}
	fn, ok := goja.AssertFunction(mv)
	if ok {
		// factory 形态:(config) => hooks
		out, err := fn(goja.Undefined(), vm.ToValue(config))
		if err != nil {
			return nil, nil, fmt.Errorf("%s: factory error: %w", entry, err)
		}
		return vm, out, nil
	}
	return vm, mv, nil
}

// newInstance 池工厂:实例化 + hook 抽取
func newInstance(prog *goja.Program, entry string, config any, deps HostDeps) (*hookInstance, error) {
	vm, obj, err := instantiate(prog, entry, config, deps)
	if err != nil {
		return nil, err
	}
	hooks, err := ExtractHooks(vm, obj)
	if err != nil {
		return nil, err
	}
	return &hookInstance{vm: vm, hooks: hooks, cursor: deps.Cursor}, nil
}

// Hooks 部件导出的 hook 函数集(同步调用);恰一次直发语义下无 refresh
type Hooks struct {
	VM           *goja.Runtime
	Obj          goja.Value
	BuildRequest goja.Callable
	MapEvent     goja.Callable
	MapResponse  goja.Callable
	MapError     goja.Callable
	MapRequest   goja.Callable
	MapChunk     goja.Callable
}

// ExtractHooks 从导出对象抽取 hook;双向绑定校验由调用方按 Supports 执行
func ExtractHooks(vm *goja.Runtime, obj goja.Value) (*Hooks, error) {
	get := func(name string) goja.Callable {
		var v goja.Value
		if o, ok := obj.(*goja.Object); ok {
			v = o.Get(name)
		}
		if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
			return nil
		}
		fn, ok := goja.AssertFunction(v)
		if !ok {
			return nil
		}
		return fn
	}
	h := &Hooks{
		VM:           vm,
		Obj:          obj,
		BuildRequest: get("buildRequest"),
		MapEvent:     get("mapEvent"),
		MapResponse:  get("mapResponse"),
		MapError:     get("mapError"),
		MapRequest:   get("mapRequest"),
		MapChunk:     get("mapChunk"),
	}
	if h.BuildRequest == nil && h.MapEvent == nil && h.MapResponse == nil &&
		h.MapRequest == nil && h.MapChunk == nil {
		return nil, fmt.Errorf("no hooks exported")
	}
	return h, nil
}

// toMap JS 值 → Go map
func toMap(vm *goja.Runtime, v goja.Value) (map[string]any, bool) {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil, false
	}
	m, ok := v.Export().(map[string]any)
	return m, ok
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

// maskSecrets inspect/log 输出统一处理:当前 target 凭据值替换 + 截断
func maskSecrets(deps HostDeps, s string) string {
	if deps.TargetSecretValues != nil {
		for _, val := range deps.TargetSecretValues(deps.currentTarget()) {
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

// sprintArgs 参数拼接(JS 值序列化)
func sprintArgs(vm *goja.Runtime, args []goja.Value) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		parts = append(parts, a.String())
	}
	return strings.Join(parts, " ")
}

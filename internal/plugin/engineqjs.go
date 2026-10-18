package plugin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// ─── quickjs 引擎 util 桥接(bindUtil 等价物;mergeMaps/getPath/setPath/renderTemplate/maskSecrets 复用 runtime.go)───

// maxJSONDocs 单实例 jsonDoc 句柄注册表上限(Go 侧状态不受 JS 内存上限约束,显式封顶)
const maxJSONDocs = 1024

// qjsRuntime quickjs 实例 + util 桥接状态
type qjsRuntime struct {
	inst  *jsengine.Instance
	deps  HostDeps
	docs  map[int64]*jsonDoc
	docID int64
}

// newQJSRuntime 构造实例并注入 util/log/storage/setting/eventstream
func newQJSRuntime(deps HostDeps, opt jsengine.Option) (*qjsRuntime, error) {
	inst, err := jsengine.New(opt)
	if err != nil {
		return nil, err
	}
	q := &qjsRuntime{inst: inst, deps: deps, docs: map[int64]*jsonDoc{}}
	if err := q.registerUtilFuncs(); err != nil {
		_ = inst.Close()
		return nil, err
	}
	if err := inst.EvalString(bytesOfJS); err != nil {
		_ = inst.Close()
		return nil, fmt.Errorf("bytes helper: %w", err)
	}
	if err := inst.EvalString(utilAssemblyJS); err != nil {
		_ = inst.Close()
		return nil, fmt.Errorf("util assembly: %w", err)
	}
	if err := inst.EvalString(eventStreamHelper); err != nil {
		_ = inst.Close()
		return nil, fmt.Errorf("eventstream helper: %w", err)
	}
	return q, nil
}

// Close 释放实例
func (q *qjsRuntime) Close() error { return q.inst.Close() }

// ctxToValue PipelineContext → JS 可读对象(upstream 快照/state/vars;nil 安全)。
// state 经 JSON 通道为值拷贝(quickjs host 桥无引用通道;与 goja 引用语义差异见迁移文档)
func (q *qjsRuntime) ctxToValue(ctx *pipeline.PipelineContext) (jsengine.Value, error) {
	inst := q.inst
	ctxObj, err := inst.NewObject()
	if err != nil {
		return jsengine.Value{}, err
	}
	set := func(obj jsengine.Value, name string, v any) {
		if err := inst.SetProp(obj, name, v); err != nil {
			_ = err
		}
	}
	if ctx == nil {
		set(ctxObj, "requestId", "")
		state, _ := inst.NewObject()
		defer state.Free()
		set(ctxObj, "state", state)
		up, _ := inst.NewObject()
		defer up.Free()
		set(ctxObj, "upstream", up)
		vars, _ := inst.NewObject()
		defer vars.Free()
		set(ctxObj, "vars", vars)
		return ctxObj, nil
	}
	set(ctxObj, "requestId", ctx.RequestID)
	up, err := inst.NewObject()
	if err != nil {
		ctxObj.Free()
		return jsengine.Value{}, err
	}
	defer up.Free()
	set(up, "name", ctx.Upstream.Name)
	set(ctxObj, "upstream", up)
	if err := inst.SetProp(ctxObj, "state", ctx.State); err != nil {
		ctxObj.Free()
		return jsengine.Value{}, err
	}
	vars, err := inst.NewObject()
	if err != nil {
		ctxObj.Free()
		return jsengine.Value{}, err
	}
	defer vars.Free()
	set(vars, "model", ctx.Vars.Model)
	set(vars, "entryStream", ctx.Vars.EntryStream)
	set(ctxObj, "vars", vars)
	return ctxObj, nil
}

// bytesOfJS __u_bytes 实现:类型与越界校验,返回字节数组或错误哨兵(对齐 goja 版错误文案)
const bytesOfJS = `
globalThis.__bytesOf = function (v, offset, need) {
  var u8 = null;
  if (v instanceof ArrayBuffer) u8 = new Uint8Array(v);
  else if (ArrayBuffer.isView(v)) u8 = new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
  if (u8 === null) throw new Error("64-bit integer input must be an ArrayBuffer or TypedArray");
  if (typeof offset !== "number" || (offset | 0) !== offset || offset < 0) throw new Error("64-bit integer offset must be non-negative");
  if (offset + need > u8.length) throw new Error("64-bit integer read out of bounds");
  return JSON.stringify(Array.prototype.slice.call(u8.subarray(offset, offset + need)));
};
`

// utilAssemblyJS util/log/storage/setting 装配(宿主函数为全局 __u_*/__st_*;null→undefined 语义在此层对齐 goja)
const utilAssemblyJS = `
var util = {
  deepMerge: function (a, b) { return __u_deepMerge(__hostToGo(a), __hostToGo(b)); },
  deepClone: function (v) { return __u_deepClone(__hostToGo(v)); },
  get: function (v, path) { var r = __u_get(__hostToGo(v), String(path)); return r === null ? undefined : r; },
  set: function (v, path, nv) { return __u_set(__hostToGo(v), String(path), __hostToGo(nv)); },
  pick: function (v, keys) { return __u_pick(__hostToGo(v), __hostToGo(keys)); },
  omit: function (v, keys) { return __u_omit(__hostToGo(v), __hostToGo(keys)); },
  b64encode: __u_b64encode,
  b64decode: __u_b64decode,
  b64urlEncode: __u_b64urlEncode,
  b64urlDecode: __u_b64urlDecode,
  u64be: function (buf, offset) { return __u_bigEndian(__bytesOf(buf, offset, 8), false); },
  i64be: function (buf, offset) { return __u_bigEndian(__bytesOf(buf, offset, 8), true); },
  uuid: __u_uuid,
  now: __u_now,
  isoNow: __u_isoNow,
  sha256hex: __u_sha256hex,
  hmacSha256hex: __u_hmacSha256hex,
  template: function (str, vars) { return __u_template(String(str), __hostToGo(vars)); },
  inspect: function (v) { return __u_inspect(__hostToGo(v)); },
  json: function (raw) {
    var d = __u_jsonDoc(String(raw));
    return {
      get: function (path) { var v = __jd_get(d.id, String(path)); return v === null ? undefined : v; },
      has: function (path) { return __jd_has(d.id, String(path)); },
      ops: function (list) { return __jd_ops(d.id, JSON.stringify(list)); },
      finalize: function () { return __jd_finalize(d.id); }
    };
  },
  key: function () { var v = __u_key(); return v === null ? undefined : v; },
  evict: function (transport, scope, value) { return __u_evict(String(transport), String(scope), String(value)); }
};
var setting = {
  string: function (o) { return __u_setting("string", o === undefined ? null : __hostToGo(o)); },
  int: function (o) { return __u_setting("int", o === undefined ? null : __hostToGo(o)); },
  number: function (o) { return __u_setting("number", o === undefined ? null : __hostToGo(o)); },
  bool: function (o) { return __u_setting("bool", o === undefined ? null : __hostToGo(o)); },
  enum: function (o) { return __u_setting("enum", o === undefined ? null : __hostToGo(o)); }
};
var log = {
  info: function () { __u_log("info", __u_fmt(arguments)); },
  warn: function () { __u_log("warn", __u_fmt(arguments)); },
  error: function () { __u_log("error", __u_fmt(arguments)); }
};
var storage = {
  get: function (k) { var v = __st_get(String(k)); return v === null ? undefined : v; },
  set: function (k, v) { __st_set(String(k), String(v)); },
  delete: function (k) { __st_del(String(k)); }
};
function __u_fmt(args) {
  var s = "";
  for (var i = 0; i < args.length; i++) { s += (i ? " " : "") + String(args[i]); }
  return s;
}
`

// registerUtilFuncs 注册全局宿主函数(对象属性由 utilAssemblyJS 装配)
func (q *qjsRuntime) registerUtilFuncs() error {
	inst := q.inst
	deps := q.deps
	reg := func(name string, fn func(args []any) (any, error)) error {
		return inst.RegisterFunc(name, fn)
	}
	argJSON := func(args []any, i int) any {
		if i < len(args) {
			return args[i]
		}
		return nil
	}
	argStr := func(args []any, i int) string {
		if i < len(args) {
			s, _ := args[i].(string)
			return s
		}
		return ""
	}
	unmarshal := func(v any) (any, error) {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("internal: expected json text, got %T", v)
		}
		var out any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	objArg := func(args []any, idx int, name string) (map[string]any, error) {
		v, err := unmarshal(argJSON(args, idx))
		if err != nil {
			return nil, err
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New(name)
		}
		return m, nil
	}
	return errors.Join(
		reg("__u_deepMerge", func(args []any) (any, error) {
			am, err := objArg(args, 0, "deepMerge requires two objects")
			if err != nil {
				return nil, err
			}
			bm, err := objArg(args, 1, "deepMerge requires two objects")
			if err != nil {
				return nil, err
			}
			return mergeMaps(am, bm), nil
		}),
		reg("__u_deepClone", func(args []any) (any, error) {
			v, err := unmarshal(argJSON(args, 0))
			if err != nil {
				return nil, err
			}
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			var out any
			_ = json.Unmarshal(b, &out)
			return out, nil
		}),
		reg("__u_get", func(args []any) (any, error) {
			m, err := objArg(args, 0, "")
			if err != nil {
				return nil, nil // 对齐 goja:非对象输入返回 undefined
			}
			return getPath(m, argStr(args, 1)), nil
		}),
		reg("__u_set", func(args []any) (any, error) {
			m, err := objArg(args, 0, "set requires object")
			if err != nil {
				return nil, err
			}
			nv, err := unmarshal(argJSON(args, 2))
			if err != nil {
				return nil, err
			}
			return setPath(m, argStr(args, 1), nv), nil
		}),
		reg("__u_pick", func(args []any) (any, error) {
			m, err := objArg(args, 0, "pick requires object")
			if err != nil {
				return nil, err
			}
			kv, err := unmarshal(argJSON(args, 1))
			if err != nil {
				return nil, err
			}
			out := map[string]any{}
			for _, k := range toStringSlice(kv) {
				if val, ok := m[k]; ok {
					out[k] = val
				}
			}
			return out, nil
		}),
		reg("__u_omit", func(args []any) (any, error) {
			m, err := objArg(args, 0, "omit requires object")
			if err != nil {
				return nil, err
			}
			kv, err := unmarshal(argJSON(args, 1))
			if err != nil {
				return nil, err
			}
			drop := map[string]bool{}
			for _, k := range toStringSlice(kv) {
				drop[k] = true
			}
			out := map[string]any{}
			for k, val := range m {
				if !drop[k] {
					out[k] = val
				}
			}
			return out, nil
		}),
		reg("__u_b64encode", func(args []any) (any, error) {
			return base64.StdEncoding.EncodeToString([]byte(argStr(args, 0))), nil
		}),
		reg("__u_b64decode", func(args []any) (any, error) {
			b, err := base64.StdEncoding.DecodeString(argStr(args, 0))
			if err != nil {
				return nil, fmt.Errorf("b64decode: %w", err)
			}
			return string(b), nil
		}),
		reg("__u_b64urlEncode", func(args []any) (any, error) {
			return base64.URLEncoding.EncodeToString([]byte(argStr(args, 0))), nil
		}),
		reg("__u_b64urlDecode", func(args []any) (any, error) {
			b, err := base64.URLEncoding.DecodeString(argStr(args, 0))
			if err != nil {
				return nil, fmt.Errorf("b64urlDecode: %w", err)
			}
			return string(b), nil
		}),
		reg("__u_bytes", func(args []any) (any, error) { return args, nil }),
		reg("__u_bigEndian", func(args []any) (any, error) {
			raw, err := unmarshal(argJSON(args, 0))
			if err != nil {
				return nil, errors.New("64-bit integer read out of bounds")
			}
			arr, ok := raw.([]any)
			if !ok || len(arr) != 8 {
				return nil, errors.New("64-bit integer read out of bounds")
			}
			buf := make([]byte, 8)
			for j, x := range arr {
				f, _ := x.(float64)
				buf[j] = byte(int(f) & 0xff)
			}
			signed := false
			if len(args) > 1 {
				signed, _ = args[1].(bool)
			}
			return readBigEndian64Bytes(buf, 0, signed)
		}),
		reg("__u_uuid", func([]any) (any, error) { return uuid.NewString(), nil }),
		reg("__u_now", func([]any) (any, error) { return time.Now().UnixMilli(), nil }),
		reg("__u_isoNow", func([]any) (any, error) { return time.Now().UTC().Format(time.RFC3339), nil }),
		reg("__u_sha256hex", func(args []any) (any, error) {
			sum := sha256.Sum256([]byte(argStr(args, 0)))
			return hex.EncodeToString(sum[:]), nil
		}),
		reg("__u_hmacSha256hex", func(args []any) (any, error) {
			mac := hmac.New(sha256.New, []byte(argStr(args, 0)))
			mac.Write([]byte(argStr(args, 1)))
			return hex.EncodeToString(mac.Sum(nil)), nil
		}),
		reg("__u_template", func(args []any) (any, error) {
			vars, err := unmarshal(argJSON(args, 1))
			if err != nil {
				return argStr(args, 0), nil
			}
			m, ok := vars.(map[string]any)
			if !ok {
				return argStr(args, 0), nil
			}
			return renderTemplate(argStr(args, 0), m), nil
		}),
		reg("__u_inspect", func(args []any) (any, error) {
			v, err := unmarshal(argJSON(args, 0))
			if err != nil {
				return nil, err
			}
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			return maskSecrets(deps, string(b)), nil
		}),
		reg("__u_jsonDoc", func(args []any) (any, error) {
			return q.newJSONDocHandle(argStr(args, 0))
		}),
		reg("__jd_get", func(args []any) (any, error) {
			d, err := q.docByID(args)
			if err != nil {
				return nil, err
			}
			r := gjson.Get(d.raw, resolvePath(d.raw, argStr(args, 1), false))
			if !r.Exists() {
				return nil, nil
			}
			return r.Raw, nil
		}),
		reg("__jd_has", func(args []any) (any, error) {
			d, err := q.docByID(args)
			if err != nil {
				return nil, err
			}
			return gjson.Get(d.raw, resolvePath(d.raw, argStr(args, 1), false)).Exists(), nil
		}),
		reg("__jd_ops", func(args []any) (any, error) {
			d, err := q.docByID(args)
			if err != nil {
				return nil, err
			}
			listJSON, _ := args[1].(string)
			var list []any
			if err := json.Unmarshal([]byte(listJSON), &list); err != nil {
				return nil, errors.New("util.json ops: requires array")
			}
			return d.applyList(list)
		}),
		reg("__jd_finalize", func(args []any) (any, error) {
			d, err := q.docByID(args)
			if err != nil {
				return nil, err
			}
			if !gjson.Valid(d.raw) {
				return nil, errors.New("util.json: finalize produced invalid JSON")
			}
			return map[string]any{"body": d.raw, "applied": d.applied, "skipped": d.skipped}, nil
		}),
		reg("__u_key", func([]any) (any, error) {
			if deps.Key == nil || deps.Key.Data == nil {
				return nil, nil
			}
			return deps.Key.Data, nil
		}),
		reg("__u_evict", func(args []any) (any, error) {
			if deps.TransportEvict == nil {
				return nil, fmt.Errorf("evict %q: no transport context (package %s)", argStr(args, 0), deps.PackageName)
			}
			if err := deps.TransportEvict(argStr(args, 0), argStr(args, 1), argStr(args, 2)); err != nil {
				return nil, err
			}
			return true, nil
		}),
		reg("__u_setting", func(args []any) (any, error) {
			typ, _ := args[0].(string)
			out := map[string]any{"type": typ}
			if len(args) > 1 {
				if s, ok := args[1].(string); ok {
					var m map[string]any
					if err := json.Unmarshal([]byte(s), &m); err == nil {
						for k, v := range m {
							out[k] = v
						}
					}
				}
			}
			return out, nil
		}),
		reg("__u_log", func(args []any) (any, error) {
			if deps.Log != nil && len(args) >= 2 {
				level, _ := args[0].(string)
				msg, _ := args[1].(string)
				deps.Log(level, maskSecrets(deps, msg))
			}
			return nil, nil
		}),
		reg("__st_get", func(args []any) (any, error) {
			if deps.Storage == nil {
				return nil, nil
			}
			v, ok := deps.Storage.Get(argStr(args, 0))
			if !ok {
				return nil, nil
			}
			return v, nil
		}),
		reg("__st_set", func(args []any) (any, error) {
			if deps.Storage == nil {
				return nil, errors.New("storage unavailable")
			}
			if len(argStr(args, 1)) > MaxStorageValue {
				return nil, errors.New("storage value exceeds limit")
			}
			return nil, deps.Storage.Set(argStr(args, 0), argStr(args, 1))
		}),
		reg("__st_del", func(args []any) (any, error) {
			if deps.Storage != nil {
				deps.Storage.Delete(argStr(args, 0))
			}
			return nil, nil
		}),
	)
}

// docByID 参数首位的句柄 id → 注册表条目(整数值 JS number 经 host 桥可能为 int 或 float64)
func (q *qjsRuntime) docByID(args []any) (*jsonDoc, error) {
	if len(args) == 0 {
		return nil, errors.New("util.json: handle required")
	}
	var id int64
	switch x := args[0].(type) {
	case int:
		id = int64(x)
	case int64:
		id = x
	case float64:
		id = int64(x)
	default:
		return nil, errors.New("util.json: handle required")
	}
	d, ok := q.docs[id]
	if !ok {
		return nil, errors.New("util.json: unknown handle")
	}
	return d, nil
}

// newJSONDocHandle 校验并注册句柄,返回 {id}
func (q *qjsRuntime) newJSONDocHandle(raw string) (any, error) {
	if !gjson.Valid(raw) {
		return nil, errors.New("util.json: invalid JSON")
	}
	if len(q.docs) >= maxJSONDocs {
		return nil, errors.New("util.json: handle limit exceeded")
	}
	q.docID++
	id := q.docID
	q.docs[id] = &jsonDoc{raw: raw}
	return map[string]any{"id": id}, nil
}

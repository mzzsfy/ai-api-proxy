package plugin

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// jsonDoc 原始 JSON 文档句柄:读字面量、定位编辑,全程不经 JS Number/stringify,未触碰文本逐字节保留
type jsonDoc struct {
	raw     string
	applied []string
	skipped []string
}

// resolvePath -1 解析为实际索引;keepTailAppend 时尾段 -1 保留(set 追加语义),get/has 全解析
func resolvePath(doc string, path string, keepTailAppend bool) string {
	segs := strings.Split(path, ".")
	cur := ""
	last := len(segs) - 1
	for i, s := range segs {
		if s == "-1" && (i < last || !keepTailAppend) {
			segs[i] = strconv.FormatInt(gjson.Get(doc, joinPath(cur, "#")).Int()-1, 10)
		}
		cur = joinPath(cur, segs[i])
	}
	return strings.Join(segs, ".")
}

// joinPath 拼接父子路径
func joinPath(parent, seg string) string {
	if parent == "" {
		return seg
	}
	return parent + "." + seg
}

// requireParent 校验父路径存在(根/数组尾追加除外)
func requireParent(doc, path string) error {
	idx := strings.LastIndex(path, ".")
	parent := ""
	if idx >= 0 {
		parent = path[:idx]
	}
	if parent != "" && !gjson.Get(doc, parent).Exists() {
		return fmt.Errorf("parent %q not found", parent)
	}
	return nil
}

// applyList 顺序执行编辑指令(util.json 的 ops 通道;value 一律为 JSON 字面量字符串)
func (d *jsonDoc) applyList(items []any) (map[string]any, error) {
	for _, it := range items {
		im, ok := it.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("util.json ops: item requires object")
		}
		op, _ := im["op"].(string)
		path, _ := im["path"].(string)
		if op == "" || path == "" {
			return nil, fmt.Errorf("util.json ops: op/path required")
		}
		stat := op + " " + path
		switch op {
		case "remove":
			if !gjson.Get(d.raw, resolvePath(d.raw, path, false)).Exists() {
				d.skipped = append(d.skipped, stat)
				continue
			}
			raw, err := sjson.Delete(d.raw, resolvePath(d.raw, path, false))
			if err != nil {
				return nil, fmt.Errorf("util.json remove %s: %w", path, err)
			}
			d.raw = raw
		case "set", "setIfAbsent":
			if op == "setIfAbsent" && gjson.Get(d.raw, resolvePath(d.raw, path, true)).Exists() {
				d.skipped = append(d.skipped, stat)
				continue
			}
			v, ok := im["value"].(string)
			if !ok {
				return nil, fmt.Errorf("util.json %s %s: value requires literal string", op, path)
			}
			if !json.Valid([]byte(v)) {
				return nil, fmt.Errorf("util.json %s %s: value not valid JSON literal", op, path)
			}
			target := resolvePath(d.raw, path, true)
			if err := requireParent(d.raw, target); err != nil {
				return nil, fmt.Errorf("util.json %s %s: %w", op, path, err)
			}
			raw, err := sjson.SetRaw(d.raw, target, v)
			if err != nil {
				return nil, fmt.Errorf("util.json %s %s: %w", op, path, err)
			}
			d.raw = raw
		default:
			return nil, fmt.Errorf("util.json ops: unknown op %q", op)
		}
		d.applied = append(d.applied, stat)
	}
	return map[string]any{"applied": d.applied, "skipped": d.skipped}, nil
}

package ipprovider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// bytesReader JSON 桥解码辅助
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// logf 供给方日志出口(server 装配时可重定向;默认静默防依赖 internal/log)
var logf = func(format string, args ...any) {}

// decodeOptions 把供给方私有 Options(map,来源 yaml.RawMessage 展开)解码到目标结构。
// map 键为 yaml 字段名;经 JSON 桥转(yaml 键为小写下划线形态,与 json tag 对齐)。
func decodeOptions(opts map[string]any, target any) error {
	if len(opts) == 0 {
		return nil
	}
	b, err := json.Marshal(opts)
	if err != nil {
		return fmt.Errorf("encode options: %w", err)
	}
	dec := json.NewDecoder(bytesReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("decode options: %w", err)
	}
	return nil
}

// leaseMark 供给方健康标记(kv 落地形态;clash/remote 用)
type leaseMark struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
	OK     bool      `json:"ok"`
}

// markStore 进程内标记存储(生命周期同 Manager;server 可经 Stats 出口读)
type markStore struct {
	mu    sync.Mutex
	marks map[string]leaseMark
}

func newMarkStore() *markStore { return &markStore{marks: map[string]leaseMark{}} }

func (m *markStore) set(name string, mark leaseMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.marks[name] = mark
}

func (m *markStore) get(name string) (leaseMark, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, ok := m.marks[name]
	return mark, ok
}

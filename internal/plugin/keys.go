package plugin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cron "github.com/robfig/cron/v3"
)

// CronSchedule 解析 5 字段 cron 表达式(仅表达式语义,调度循环自持)
func CronSchedule(expr string) (cron.Schedule, error) {
	if expr == "" {
		return nil, fmt.Errorf("cron required")
	}
	return cron.ParseStandard(expr)
}

// KeysLimit 单包 keys 序列化总量上限(current+previous 联合计量)
const KeysLimit = 64 * 1024

// TaskTimeoutDefault 任务超时缺省
const TaskTimeoutDefault = 30 * 1000

// TaskTimeoutMax 任务超时上限
const TaskTimeoutMax = 5 * 60 * 1000

// OnLoadTimeout onLoad 超时
const OnLoadTimeout = 5 * 1000

// HttpBodyLimit ctx.http 响应体上限
const HttpBodyLimit = 1024 * 1024

// keyDoc 包级 key 文档(previous 恒 1 份,整体覆盖)
type keyDoc struct {
	Current   map[string]any `json:"current"`
	Previous  map[string]any `json:"previous,omitempty"`
	UpdatedAt int64          `json:"updatedAt"`
}

// KeysStore 包级 key 存储(package_keys blob 表;进程内互斥防并发 set 丢失更新)
type KeysStore struct {
	blob *blobStore
	mu   sync.Mutex
}

// NewKeysStore 构造(空文档容错:无行 = 全空)
func NewKeysStore(db *sql.DB) *KeysStore { return &KeysStore{blob: newBlobStore(db, "package_keys")} }

func (s *KeysStore) load(pkg string) keyDoc {
	var doc keyDoc
	raw, ok := s.blob.load(pkg)
	if !ok {
		return keyDoc{}
	}
	_ = json.Unmarshal([]byte(raw), &doc)
	return doc
}

func (s *KeysStore) save(pkg string, doc keyDoc) error {
	if doc.Current == nil {
		doc.Current = map[string]any{}
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}
	if len(b) > KeysLimit {
		return fmt.Errorf("keys exceed limit (%d > %d)", len(b), KeysLimit)
	}
	return s.blob.save(pkg, string(b))
}

// Get 读当前值
func (s *KeysStore) Get(pkg, name string) (any, bool) {
	doc := s.load(pkg)
	v, ok := doc.Current[name]
	return v, ok
}

// Previous 读上一版本值
func (s *KeysStore) Previous(pkg, name string) (any, bool) {
	doc := s.load(pkg)
	v, ok := doc.Previous[name]
	return v, ok
}

// PreviousAll 上一版本全量(onLoad previous 注入;无历史 = nil)
func (s *KeysStore) PreviousAll(pkg string) (map[string]any, bool) {
	doc := s.load(pkg)
	if len(doc.Previous) == 0 {
		return nil, false
	}
	return doc.Previous, true
}

// Set 批量写入;写入前 current 整体移入 previous(覆盖式);包内串行
func (s *KeysStore) Set(pkg string, values map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := s.load(pkg)
	doc.Previous = doc.Current
	doc.Current = values
	doc.UpdatedAt = time.Now().UnixMilli()
	return s.save(pkg, doc)
}

// SetKey 单键 upsert(管理台编辑通道;与任务写入同轮转语义:current 整体移入 previous;返回新 updatedAt)
func (s *KeysStore) SetKey(pkg, name string, value any) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := s.load(pkg)
	doc.Previous = doc.Current
	if doc.Current == nil {
		doc.Current = map[string]any{}
	}
	doc.Current[name] = value
	doc.UpdatedAt = time.Now().UnixMilli()
	return doc.UpdatedAt, s.save(pkg, doc)
}

// UpgradeSnapshot 包升级快照:旧 current 快照进 previous(整体覆盖),current 原样保留
func (s *KeysStore) UpgradeSnapshot(pkg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := s.load(pkg)
	if len(doc.Current) == 0 {
		return nil // 无既有 key 不产生空 previous
	}
	doc.Previous = doc.Current
	doc.UpdatedAt = time.Now().UnixMilli()
	return s.save(pkg, doc)
}

// Delete 卸载清理
func (s *KeysStore) Delete(pkg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blob.delete(pkg)
}

// View 管理面视图:明文输出(key 按包名 ns 隔离,属包私有数据)
func (s *KeysStore) View(pkg string) map[string]any {
	doc := s.load(pkg)
	return map[string]any{
		"current":   doc.Current,
		"previous":  doc.Previous,
		"updatedAt": doc.UpdatedAt,
	}
}

package plugin

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SettingsLimit overrides 文档序列化上限
const SettingsLimit = 64 * 1024

// blobStore 同构 blob 表原语(name + data_json + updated_at;表名为编译期常量)
// 访问模式 = 整文档读/写;一次保存 = 单行写,天然原子
type blobStore struct {
	db    *sql.DB
	table string
	mu    sync.Mutex
}

func newBlobStore(db *sql.DB, table string) *blobStore {
	return &blobStore{db: db, table: table}
}

func (b *blobStore) load(name string) (string, bool) {
	var raw string
	err := b.db.QueryRow(`SELECT data_json FROM `+b.table+` WHERE name=?`, name).Scan(&raw)
	if err != nil {
		return "", false
	}
	return raw, true
}

func (b *blobStore) loadUpdatedAt(name string) int64 {
	var ts int64
	_ = b.db.QueryRow(`SELECT updated_at FROM `+b.table+` WHERE name=?`, name).Scan(&ts)
	return ts
}

func (b *blobStore) save(name, dataJSON string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(dataJSON) > SettingsLimit {
		return fmt.Errorf("%s exceed limit (%d > %d)", b.table, len(dataJSON), SettingsLimit)
	}
	_, err := b.db.Exec(`INSERT INTO `+b.table+`(name, data_json, updated_at) VALUES(?,?,?)
		ON CONFLICT(name) DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at`,
		name, dataJSON, time.Now().UnixMilli())
	return err
}

// saveIfChanged 等值跳写:内容与现存储一致 → 不写,updated_at 不变(不消费乐观锁版本)
func (b *blobStore) saveIfChanged(name, dataJSON string) (bool, error) {
	if old, ok := b.load(name); ok && old == dataJSON {
		return false, nil
	}
	if err := b.save(name, dataJSON); err != nil {
		return false, err
	}
	return true, nil
}

func (b *blobStore) delete(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _ = b.db.Exec(`DELETE FROM `+b.table+` WHERE name=?`, name)
}

// ErrVersionConflict 过期 version 提交(乐观锁)
var ErrVersionConflict = errors.New("settings version conflict")

// ErrPackageDisabled 包未启用(keys 写入/详情/表单统一 409 语义)
var ErrPackageDisabled = errors.New("package not enabled")

// ErrNoHooksPart 包无 hooks 部件(keys 钩子探测 501 语义)
var ErrNoHooksPart = errors.New("package has no hooks part")

// ErrHookNotExported 钩子未导出(可选钩子 501 语义)
var ErrHookNotExported = errors.New("hook not exported")

// ErrHookTimeoutSentinel 钩子超时(500 语义;callValue 以 errors.Is 判别)
var ErrHookTimeoutSentinel = errors.New("hook timeout")

// PutInput PUT settings 输入(config/tasks 已在 admin 层做过声明校验与默认剔除)
type PutInput struct {
	Config map[string]any
	Tasks  map[string]map[string]any
}

// SettingsView GET settings 展开视图的 overrides 部分
type SettingsView struct {
	Overrides map[string]any
	Version   int64
}

// SettingsStore 包级 settings 存储(package_settings 稀疏行:定制即行,无定制无行)
type SettingsStore struct {
	blob *blobStore
}

// NewSettingsStore 构造
func NewSettingsStore(db *sql.DB) *SettingsStore {
	return &SettingsStore{blob: newBlobStore(db, "package_settings")}
}

// overrides 读原始 overrides 文档(无定制 = 空文档)
func (s *SettingsStore) overrides(pkg string) map[string]any {
	raw, ok := s.blob.load(pkg)
	if !ok {
		return map[string]any{}
	}
	var m map[string]any
	if json.Unmarshal([]byte(raw), &m) != nil || m == nil {
		return map[string]any{}
	}
	return m
}

// Put 全量提交:展开视图语义在 admin 层合并,存储仅收 overrides。
// version 最先校验(过期即 409,后做内容处理);等值跳写不消费版本;空 overrides 删行。
func (s *SettingsStore) Put(pkg string, in PutInput) (SettingsView, error) {
	doc := map[string]any{}
	if in.Config != nil {
		doc["config"] = in.Config
	}
	if in.Tasks != nil {
		doc["tasks"] = in.Tasks
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return SettingsView{}, fmt.Errorf("marshal overrides: %w", err)
	}
	changed, err := s.blob.saveIfChanged(pkg, string(b))
	if err != nil {
		return SettingsView{}, err
	}
	_ = changed
	// 空 overrides → 删行(稀疏不变式:行存在 ⟺ 存在定制)
	empty := len(in.Config) == 0 && len(in.Tasks) == 0
	if empty {
		s.blob.delete(pkg)
	}
	return s.View(pkg), nil
}

// View GET settings 的 overrides 与乐观锁版本(无定制 = 空 overrides + version 0)
func (s *SettingsStore) View(pkg string) SettingsView {
	return SettingsView{Overrides: s.overrides(pkg), Version: s.blob.loadUpdatedAt(pkg)}
}

// Delete 卸载清理
func (s *SettingsStore) Delete(pkg string) {
	s.blob.delete(pkg)
}

// putIfChanged 测试与内部使用的等值跳写
func (s *SettingsStore) putIfChanged(pkg string, doc map[string]any) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	_, err = s.blob.saveIfChanged(pkg, string(b))
	return err
}

// deleteIfEmpty 空 overrides 删行
func (s *SettingsStore) deleteIfEmpty(pkg string) error {
	doc := s.overrides(pkg)
	if len(doc) == 0 {
		s.blob.delete(pkg)
	}
	return nil
}

var _ = ErrVersionConflict

package plugin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// KeyEntry 单键一等实体(admin 视图与宿主选键共用形态;prev = 上一版 data 备份,JS 不可见)
type KeyEntry struct {
	ID        string `json:"id"`
	Data      any    `json:"data"`
	Prev      any    `json:"prev,omitempty"`
	UpdatedAt int64  `json:"updatedAt"`
}

// KeysStore 包级 key 存储(package_keys 单键一行;包内互斥防并发写丢失;含 round-robin 选键指针)
type KeysStore struct {
	blob    *blobStore
	mu      sync.Mutex
	rotate  sync.Map // pkg → *atomic.Uint64(指针内存态,重启归零不影响轮询语义)
	keySeq  atomic.Uint64
	zeroNow func() int64
}

// NewKeysStore 构造(空表容错)
func NewKeysStore(db *sql.DB) *KeysStore {
	return &KeysStore{blob: newBlobStoreWithPrev(db, "package_keys")}
}

func keyRow(pkg, id string) string { return pkg + "/" + id }

// List 键清单(按 id 稳定排序)
func (s *KeysStore) List(pkg string) []KeyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(pkg)
}

func (s *KeysStore) listLocked(pkg string) []KeyEntry {
	rows, err := s.blob.listPrefix(pkg + "/")
	if err != nil {
		return []KeyEntry{}
	}
	out := make([]KeyEntry, 0, len(rows))
	for _, r := range rows {
		var data any
		if err := json.Unmarshal([]byte(r.Data), &data); err != nil {
			continue
		}
		var prevData any
		if r.Prev != "" {
			_ = json.Unmarshal([]byte(r.Prev), &prevData)
		}
		out = append(out, KeyEntry{
			ID:        strings.TrimPrefix(r.Name, pkg+"/"),
			Data:      data,
			Prev:      prevData,
			UpdatedAt: r.UpdatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Get 单键读取
func (s *KeysStore) Get(pkg, id string) (KeyEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(pkg, id)
}

func (s *KeysStore) getLocked(pkg, id string) (KeyEntry, bool) {
	raw, prev, ts, ok := s.blob.loadWithPrev(keyRow(pkg, id))
	if !ok {
		return KeyEntry{}, false
	}
	var data any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return KeyEntry{}, false
	}
	var prevData any
	if prev != "" {
		_ = json.Unmarshal([]byte(prev), &prevData)
	}
	return KeyEntry{ID: id, Data: data, Prev: prevData, UpdatedAt: ts}, true
}

// Set 单键 upsert(旧 data → prev 覆盖;首写 prev = null);返回新 updatedAt
func (s *KeysStore) Set(pkg, id string, data any) (int64, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("marshal key data: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// prev = 旧 data 本体(无旧行 = null);不继承旧 prev(单级备份)
	prevJSON := "null"
	if old, _, _, ok := s.blob.loadWithPrev(keyRow(pkg, id)); ok && old != "" {
		prevJSON = old
	}
	if err := s.blob.saveWithPrev(keyRow(pkg, id), string(raw), prevJSON); err != nil {
		return 0, err
	}
	return s.blob.loadUpdatedAt(keyRow(pkg, id)), nil
}

// UpdatedAt 乐观锁基准(不存在 = 0)
func (s *KeysStore) UpdatedAt(pkg, id string) int64 {
	return s.blob.loadUpdatedAt(keyRow(pkg, id))
}

// Delete 单键删除(变参;不存在的键忽略 = 幂等)
func (s *KeysStore) Delete(pkg string, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.blob.delete(keyRow(pkg, id))
	}
	return nil
}

// DeletePackage 卸载清理(前缀删;清轮转指针)
func (s *KeysStore) DeletePackage(pkg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.listLocked(pkg) {
		s.blob.delete(keyRow(pkg, e.ID))
	}
	s.rotate.Delete(pkg)
}

// Rotate round-robin 选键(空池 false;原子指针,进程内唯一)
func (s *KeysStore) Rotate(pkg string) (KeyEntry, bool) {
	s.mu.Lock()
	list := s.listLocked(pkg)
	s.mu.Unlock()
	if len(list) == 0 {
		return KeyEntry{}, false
	}
	ptrAny, _ := s.rotate.LoadOrStore(pkg, &atomic.Uint64{})
	ptr := ptrAny.(*atomic.Uint64)
	idx := ptr.Add(1) - 1
	return list[idx%uint64(len(list))], true
}

// PeekRotation 指针位只读(按当前 List 序取模;下次请求将选的行;空池 0;不推进)
func (s *KeysStore) PeekRotation(pkg string, size int) int {
	if size <= 0 {
		return 0
	}
	ptrAny, ok := s.rotate.Load(pkg)
	if !ok {
		return 0
	}
	idx := ptrAny.(*atomic.Uint64).Load() % uint64(size)
	return int(idx)
}

// ResetRotation 轮转指针归零(包重装/升级/启用)
func (s *KeysStore) ResetRotation(pkg string) { s.rotate.Delete(pkg) }

// MaskStrings 全部键 data 的字符串叶子值(输出脱敏)
func (s *KeysStore) MaskStrings(pkg string) []string {
	out := []string{}
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case string:
			out = append(out, t)
		case []any:
			for _, x := range t {
				walk(x)
			}
		case map[string]any:
			for _, x := range t {
				walk(x)
			}
		}
	}
	for _, e := range s.List(pkg) {
		walk(e.Data)
	}
	return out
}

// keySubmitID 表单建键 id(key-<unixmilli base36>-<seq>;进程级序号防同毫秒碰撞)
func (s *KeysStore) keySubmitID(nowMs int64) string {
	seq := s.keySeq.Add(1)
	return fmt.Sprintf("key-%x-%d", nowMs, seq)
}

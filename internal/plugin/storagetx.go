// storage 事务视图:执行期写进私有缓冲,成功归并持久层,失败/超时丢弃
package plugin

import "sync"

// StorageTx StorageKV 事务包装:Get 读穿透(缓冲命中优先);Set/Delete 只进缓冲;
// Begin 开新事务;Commit 按序归并落库;Drop 丢弃(回滚)。
// 实例绑定单个执行单元(池实例借出期/非池化 runtime 一次钩子),无跨单元并发。
type StorageTx struct {
	mu     sync.Mutex
	base   StorageKV
	writes map[string]*string // nil 值 = 删除标记
}

// NewStorageTx 包装持久层;base 为 nil 时退化为无存储(全空读/写拒绝)
func NewStorageTx(base StorageKV) *StorageTx {
	return &StorageTx{base: base, writes: map[string]*string{}}
}

// Begin 开新事务(丢弃未归并缓冲)
func (t *StorageTx) Begin() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = map[string]*string{}
}

// Commit 归并缓冲到持久层(逐键;键序不保证跨键原子)
func (t *StorageTx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, vp := range t.writes {
		if vp == nil {
			t.base.Delete(k)
			continue
		}
		if err := t.base.Set(k, *vp); err != nil {
			t.writes = map[string]*string{}
			return err
		}
	}
	t.writes = map[string]*string{}
	return nil
}

// Drop 丢弃缓冲(回滚)
func (t *StorageTx) Drop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = map[string]*string{}
}

func (t *StorageTx) Get(key string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if vp, ok := t.writes[key]; ok {
		if vp == nil {
			return "", false // 事务内已删
		}
		return *vp, true
	}
	if t.base == nil {
		return "", false
	}
	return t.base.Get(key)
}

func (t *StorageTx) Set(key, value string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := value
	t.writes[key] = &v
	return nil
}

func (t *StorageTx) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes[key] = nil
}

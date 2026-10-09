// per-pkg 槽资源注册表:③ 权威槽与包级槽锁(桥/handler 引擎/login 共用同实例)
package server

import (
	"sync"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
)

type handlerShared struct {
	st *store.Store
	mu sync.Mutex
	// slots per-pkg ③ 权威槽;locks 包级槽锁
	slots map[string]*plugin.SlotStore
	locks map[string]*plugin.SlotLocks
}

func newHandlerShared(st *store.Store) *handlerShared {
	return &handlerShared{st: st, slots: map[string]*plugin.SlotStore{}, locks: map[string]*plugin.SlotLocks{}}
}

func (h *handlerShared) storeFor(ns string) *plugin.SlotStore {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.slots[ns]; ok {
		return s
	}
	s := plugin.NewSlotStore(h.st.DB(), ns)
	h.slots[ns] = s
	return s
}

func (h *handlerShared) locksFor(ns string) *plugin.SlotLocks {
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.locks[ns]; ok {
		return l
	}
	l := plugin.NewSlotLocks()
	h.locks[ns] = l
	return l
}

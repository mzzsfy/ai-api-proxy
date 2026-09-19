package ipprovider

import (
	"fmt"
	"sort"
	"sync"
)

// ProviderCfg 供给方构造配置(传输定义展开形态;具体字段由各实现解析)
type ProviderCfg struct {
	// Name 传输实例名(kv 记录/日志定位用)
	Name string
	// URL 形如 kind:参数串(如 ipp_clash:http://127.0.0.1:6654)
	URL string
	// Options 供给方私有配置(各实现自行解析;yaml 展开后的 map)
	Options map[string]any
	// StateDir 状态目录(data_dir 下;warp 实例 state/mihomo 配置缓存等)
	StateDir string
}

// Factory 供给方构造工厂;返回的 Provider 由调用方负责 Close
type Factory func(cfg ProviderCfg) (Provider, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register 注册供给方类型;重复注册同名 panic(装配期错误,立即暴露)
func Register(kind string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[kind]; dup {
		panic(fmt.Sprintf("ipprovider: 重复注册 %s", kind))
	}
	registry[kind] = f
}

// Create 按类型构造供给方;未注册返回 ErrUnknownKind
func Create(kind string, cfg ProviderCfg) (Provider, error) {
	registryMu.RLock()
	f, ok := registry[kind]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKind, kind)
	}
	return f(cfg)
}

// Kinds 已注册类型清单(稳定序;诊断用)
func Kinds() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	kinds := make([]string, 0, len(registry))
	for k := range registry {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

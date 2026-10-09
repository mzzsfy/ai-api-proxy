// handler 包 pipeline.Protocol 兼容壳:ValidateGroup/Resolve/协商照旧;空 BuildRequest(请求分流在 gateway)
package plugin

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// ErrShellRoute 兼容壳占位错误(请求必须经 handler 执行器分流)
var ErrShellRoute = errors.New("handler package: request must be routed via the handler executor")

// HandlerShell 协议壳(声明 = parts.handler 值;两种形态均支持,协商放行)
type HandlerShell struct{ declared string }

// NewHandlerShell 构造
func NewHandlerShell(declared string) *HandlerShell { return &HandlerShell{declared: declared} }

func (h *HandlerShell) Declared() string { return h.declared }
func (h *HandlerShell) Name() string     { return h.declared }

func (h *HandlerShell) Supports() pipeline.Supports {
	return pipeline.Supports{Forms: []string{pipeline.FormStreaming, pipeline.FormNonStreaming}}
}

func (h *HandlerShell) BuildRequest(*pipeline.PipelineContext, []byte) (pipeline.Request, error) {
	return pipeline.Request{}, ErrShellRoute
}

func (h *HandlerShell) MapEvent(*pipeline.PipelineContext, []byte) ([]byte, error) {
	return nil, ErrShellRoute
}

func (h *HandlerShell) MapResponse(*pipeline.PipelineContext, []byte) ([]byte, error) {
	return nil, ErrShellRoute
}

// HandlerEngines 引擎注册表((包,revision) 维度懒建;CloseAll = RSS 看门狗软限动作)
type HandlerEngines struct {
	mu      sync.Mutex
	engines map[string]*HandlerEngine
	factory func(pkg *Package) HandlerDeps
	reject  atomic.Bool // RSS 看门狗软限拒新闸(true = 新请求 502)
}

// ErrRejectNew RSS 软限拒新(502 语义)
var ErrRejectNew = errors.New("server overloaded (rss soft limit), retry later")

// RejectNew 看门狗拒新闸查询
func (h *HandlerEngines) RejectNew() bool { return h.reject.Load() }

// SetRejectNew 看门狗闸(软限触发 true/回落 false)
func (h *HandlerEngines) SetRejectNew(v bool) { h.reject.Store(v) }

// NewHandlerEngines 构造(factory 由宿主装配:Store/Locks/Transport/Log/MaxBorrow 按包绑定)
func NewHandlerEngines(factory func(pkg *Package) HandlerDeps) *HandlerEngines {
	return &HandlerEngines{engines: map[string]*HandlerEngine{}, factory: factory}
}

// For 取或懒建引擎(包 revision 变更 = 新键新引擎,旧引擎随 CloseAll/闲置不回收语义由宿主管理)
func (h *HandlerEngines) For(pkg *Package) (*HandlerEngine, error) {
	if h.reject.Load() {
		return nil, ErrRejectNew
	}
	key := pkg.Manifest.Name + "|" + fmt.Sprint(pkg.Revision)
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.engines[key]; ok {
		return e, nil
	}
	e := NewHandlerEngine(h.factory(pkg))
	h.engines[key] = e
	return e, nil
}

// CloseAll 丢弃全部引擎池(RSS 软限;在借实例随归还即弃)
func (h *HandlerEngines) CloseAll() {
	h.mu.Lock()
	engines := h.engines
	h.engines = map[string]*HandlerEngine{}
	h.mu.Unlock()
	for _, e := range engines {
		e.Close()
	}
}

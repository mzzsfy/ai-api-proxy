// hooks 装配:hooks 部件运行通道(onLoad 回调 + 定时任务调度器 + 出站执行器)
package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/scheduler"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
)

// hooksTransportName 出站优先使用的传输实例名
const hooksTransportName = "direct"

// httpExecutor ctx.http 出站执行器(复用全局出站 Transport;无实例时报错)
type httpExecutor struct {
	trMgr *transport.Manager
}

// Do 按 (优先 direct → 任一实例) 出站;供给方类传输(scheme ipp+/aap)不参与回退——空亲和素材会退化为恒定绑定
func (h *httpExecutor) Do(req plugin.HttpRequest) (*plugin.HttpResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), req.Timeout)
	defer cancel()
	preq := pipeline.Request{URL: req.URL, Method: req.Method, Headers: req.Headers, Body: []byte(req.Body)}
	if h.trMgr == nil {
		return nil, fmt.Errorf("no outbound transport available")
	}
	tr, ok := h.trMgr.Get(hooksTransportName)
	if !ok {
		for _, name := range h.trMgr.Names() {
			if _, isSupplier := h.trMgr.SupplierKind(name); isSupplier {
				continue
			}
			if tr, ok = h.trMgr.Get(name); ok {
				break
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("no outbound transport available")
	}
	tresp, err := tr.RoundTrip(ctx, preq)
	if err != nil {
		return nil, err
	}
	body := tresp.Body
	if len(body) > plugin.HttpBodyLimit {
		body = body[:plugin.HttpBodyLimit]
	}
	return &plugin.HttpResponse{Status: tresp.Status, Headers: tresp.Headers, Body: string(body)}, nil
}

// wireHooks 装配 hooks 通道:注册 onLoad/onChange 回调并启动调度器
func wireHooks(app *App) *scheduler.Scheduler {
	exec := &httpExecutor{trMgr: app.trMgr}
	pkgs := app.AdminDeps.Packages
	// 闸门在装配期定档(Build 注入;测试装配路径惰性兜底为进程级单例,避免"每次新 gate 恒满桶"限流失效)
	gate := app.pluginEvictGate
	if gate == nil {
		gate = sharedEvictGate()
	}
	evict := gatedEvict(gate, evictForwarder(app.trMgr))
	deps := func(pkgName string) plugin.HooksDeps {
		return plugin.HooksDeps{
			PackageName: pkgName,
			HTTP:        exec,
			Keys:        pkgs.Keys(),
			Storage:     &kvStorage{db: app.St.DB(), ns: pkgName},
			Log:         hooksLog(pkgName),
			// 插件定时任务主动失效上报:同一命令转发(限流闸门;仅失效不触发重试)
			TransportEvict: evict,
		}
	}
	// onLoad 回调(安装/升级/启用;异步,失败不阻断加载)
	pkgs.OnLoad = func(pkg *plugin.Package, previous map[string]any) {
		if !pkgs.IsEnabled(pkg.Manifest.Name) {
			return
		}
		rt, err := plugin.LoadHooks(pkg, deps(pkg.Manifest.Name))
		if err != nil {
			log.Printf("hooks: %s: load: %v", pkg.Manifest.Name, err)
			return
		}
		if err := rt.RunOnLoad(previous); err != nil {
			log.Printf("hooks: %s: onLoad: %v", pkg.Manifest.Name, err)
		}
	}
	run := func(pkgName string, task plugin.HooksTask, at time.Time) error {
		pkg, err := pkgs.GetPackage(pkgName)
		if err != nil {
			return err
		}
		if !pkgs.IsEnabled(pkgName) {
			return nil
		}
		rt, err := plugin.LoadHooks(pkg, deps(pkgName))
		if err != nil {
			return err
		}
		return rt.RunTask(task.Name, at)
	}
	sched := scheduler.New(pkgs, run, nil)
	pkgs.OnChange = func() { sched.Refresh() }
	go sched.Run()
	return sched
}

// hooksLog 部件日志汇主日志(包名前缀)
func hooksLog(pkg string) func(level, msg string) {
	return func(level, msg string) {
		switch level {
		case "error":
			log.Printf("plugin %s: %s", pkg, msg)
		case "warn":
			log.Printf("plugin %s: warn: %s", pkg, msg)
		default:
			log.Printf("plugin %s: info: %s", pkg, msg)
		}
	}
}

// kvStorage kv 表存储(与 upstream.dbStorage 同表同形;卸载不清理)
type kvStorage struct {
	db *sql.DB
	ns string
}

func (s *kvStorage) Get(key string) (string, bool) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM kv WHERE ns=? AND key=?`, s.ns, key).Scan(&v)
	return v, err == nil
}

func (s *kvStorage) Set(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO kv(ns, key, value) VALUES(?,?,?)
		ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`, s.ns, key, value)
	return err
}

func (s *kvStorage) Delete(key string) {
	_, _ = s.db.Exec(`DELETE FROM kv WHERE ns=? AND key=?`, s.ns, key)
}

// hooks 装配:hooks 部件运行通道(onLoad 回调 + 定时任务调度器 + 出站执行器)
package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
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

// effectiveSettings 合并声明 ⊕ overrides 为运行时快照(校验由 admin 保存面负责,此处宽松)
func effectiveSettings(decl map[string]any, overrides map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range decl {
		out[k] = v
	}
	// overrides 形态 {"config": {...}};浅层键覆盖
	if cfg, ok := overrides["config"].(map[string]any); ok {
		for k, v := range cfg {
			out[k] = v
		}
	}
	return out
}

// settingsSnapshot 包的运行时 settings 快照(声明 + 存储覆盖;加载失败 = 空快照)
func settingsSnapshot(pkgs *plugin.Registry, pkgName string) map[string]any {
	pkg, err := pkgs.GetPackage(pkgName)
	if err != nil {
		return map[string]any{}
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	return effectiveSettings(decl, pkgs.SettingsOverrides(pkgName))
}

// hooksDeps 包运行时依赖(调度器与手动触发共用同一通道)
func (a *App) hooksDeps(pkgName string) plugin.HooksDeps {
	pkgs := a.AdminDeps.Packages
	// 闸门在装配期定档(Build 注入;测试装配路径惰性兜底为进程级单例,避免"每次新 gate 恒满桶"限流失效)
	gate := a.pluginEvictGate
	if gate == nil {
		gate = sharedEvictGate()
	}
	return plugin.HooksDeps{
		PackageName: pkgName,
		HTTP:        &httpExecutor{trMgr: a.trMgr},
		Keys:        pkgs.Keys(),
		Storage:     &kvStorage{db: a.St.DB(), ns: pkgName},
		Log:         hooksLog(pkgName),
		// 插件定时任务主动失效上报:同一命令转发(限流闸门;仅失效不触发重试)
		TransportEvict: gatedEvict(gate, evictForwarder(a.trMgr)),
	}
}

// RunTaskOnce 手动触发任务一次(同步;keyID 空 = 键池全键跑,非空 = 单键跑;与调度器共用逐键通道)
func (a *App) RunTaskOnce(pkgName, taskName, keyID string) ([]admin.TaskRunResult, error) {
	pkgs := a.AdminDeps.Packages
	pkg, err := pkgs.GetPackage(pkgName)
	if err != nil {
		return nil, err
	}
	if !pkgs.IsEnabled(pkgName) {
		return nil, fmt.Errorf("package %s disabled", pkgName)
	}
	h := pkg.Manifest.Parts.Hooks
	if h == nil {
		return nil, fmt.Errorf("package %s has no hooks", pkgName)
	}
	var tk plugin.HooksTask
	found := false
	for _, t := range h.Tasks {
		if t.Name == taskName {
			tk, found = t, true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("task %q not declared", taskName)
	}
	// 目标键集合:单键 or 全键(0 键 = 空结果,不报错)
	var targets []plugin.KeyEntry
	if keyID != "" {
		e, ok := pkgs.Keys().Get(pkgName, keyID)
		if !ok {
			return nil, fmt.Errorf("key %q not found", keyID)
		}
		targets = []plugin.KeyEntry{e}
	} else {
		targets = pkgs.Keys().List(pkgName)
	}
	results := make([]admin.TaskRunResult, 0, len(targets))
	for _, e := range targets {
		start := time.Now()
		deps := a.hooksDeps(pkgName)
		deps.Key = &plugin.KeyRef{ID: e.ID, Data: e.Data}
		rt, err := plugin.LoadTask(pkg, tk, deps, settingsSnapshot(pkgs, pkgName))
		if err == nil {
			err = rt.RunTask(tk, time.Now())
		}
		item := admin.TaskRunResult{Key: e.ID, DurationMS: time.Since(start).Milliseconds()}
		if err != nil {
			item.Error = err.Error()
		} else {
			item.OK = true
		}
		results = append(results, item)
	}
	return results, nil
}

// wireHooks 装配 hooks 通道:注册 onLoad/onChange 回调并启动调度器
func wireHooks(app *App) *scheduler.Scheduler {
	pkgs := app.AdminDeps.Packages
	// onLoad 回调(安装/升级/启用;异步,失败不阻断加载;ctx.key = undefined)
	pkgs.OnLoad = func(pkg *plugin.Package) {
		if !pkgs.IsEnabled(pkg.Manifest.Name) || pkg.Manifest.Parts.Hooks == nil {
			return
		}
		if _, ok := pkg.Files["init.js"]; !ok {
			return
		}
		rt, err := plugin.LoadInit(pkg, app.hooksDeps(pkg.Manifest.Name), settingsSnapshot(pkgs, pkg.Manifest.Name))
		if err != nil {
			log.Printf("hooks: %s: load: %v", pkg.Manifest.Name, err)
			return
		}
		if err := rt.RunOnLoad(); err != nil {
			log.Printf("hooks: %s: onLoad: %v", pkg.Manifest.Name, err)
		}
	}
	// run 单次任务执行;keyID 非空 = 单键(逐键调度的最小单元),空 = 键池逐键循环(0 键 = 跳过)
	run := func(pkgName string, task plugin.HooksTask, at time.Time, keyID string) error {
		pkg, err := pkgs.GetPackage(pkgName)
		if err != nil {
			return err
		}
		if !pkgs.IsEnabled(pkgName) {
			return nil
		}
		runOne := func(key *plugin.KeyRef) error {
			deps := app.hooksDeps(pkgName)
			deps.Key = key
			rt, err := plugin.LoadTask(pkg, task, deps, settingsSnapshot(pkgs, pkgName))
			if err != nil {
				return err
			}
			return rt.RunTask(task, at)
		}
		if keyID != "" {
			e, ok := pkgs.Keys().Get(pkgName, keyID)
			if !ok {
				return fmt.Errorf("key %q not found", keyID)
			}
			return runOne(&plugin.KeyRef{ID: e.ID, Data: e.Data})
		}
		keys := pkgs.Keys().List(pkgName)
		if len(keys) == 0 {
			return nil
		}
		var firstErr error
		for _, e := range keys {
			if err := runOne(&plugin.KeyRef{ID: e.ID, Data: e.Data}); err != nil {
				log.Printf("hooks: %s: task %s key %s: %v", pkgName, task.Name, e.ID, err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		return firstErr
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

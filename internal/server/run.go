package server

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/adminweb"
	"github.com/mzzsfy/ai-api-proxy/internal/builtin"
	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/gateway"
	"github.com/mzzsfy/ai-api-proxy/internal/history"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/scheduler"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// pluginSrc 插件来源(裸 .aap 或含 manifest.json 的包目录)
type pluginSrc struct {
	path    string
	builtin bool
}

// packAll 打包两级目录内的包并打印产出路径
func packAll(cfg *Config) error {
	out, err := packPluginsDir(cfg)
	if err != nil {
		return err
	}
	for _, p := range out {
		log.Printf("packed %s", p)
	}
	return nil
}

// searchPluginDirs 两级目录搜索:plugins_dir(普通,多候选)→ builtin_dir(内置,唯一);
// 内置目录只补缺,不覆盖用户已装的同名包(内置 = 预置默认值,去中心化覆盖语义)
func searchPluginDirs(cfg *Config) ([]pluginSrc, error) {
	paths := []struct {
		dir     string
		builtin bool
	}{{cfg.PluginsDir, false}, {cfg.BuiltinDir, true}}
	var out []pluginSrc
	for _, p := range paths {
		if p.dir == "" {
			continue
		}
		entries, err := os.ReadDir(p.dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", p.dir, err)
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			full := filepath.Join(p.dir, name)
			fi, err := os.Stat(full)
			if err != nil {
				return nil, fmt.Errorf("stat %s: %w", full, err)
			}
			switch {
			// 裸 .aap:直接安装
			case fi.Mode().IsRegular() && strings.EqualFold(filepath.Ext(name), ".aap"):
				out = append(out, pluginSrc{path: full, builtin: p.builtin})
			// 目录:含 manifest.json 即视为包目录
			case fi.IsDir():
				if _, err := os.Stat(filepath.Join(full, "manifest.json")); err != nil {
					continue
				}
				out = append(out, pluginSrc{path: full, builtin: p.builtin})
			}
		}
	}
	return out, nil
}

// packPluginsDir 打包两级目录内的所有包到 cfg.PackDir(唯一 .aap 产出路径;供分发与部署)
func packPluginsDir(cfg *Config) ([]string, error) {
	srcs, err := searchPluginDirs(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.PackDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", cfg.PackDir, err)
	}
	written := make([]string, 0, len(srcs))
	for _, s := range srcs {
		data, err := plugin.PackDir(s.path)
		if err != nil {
			return nil, fmt.Errorf("pack %s: %w", s.path, err)
		}
		dst := filepath.Join(cfg.PackDir, filepath.Base(s.path)+".aap")
		if err := os.WriteFile(dst, data, 0o600); err != nil {
			return nil, fmt.Errorf("write %s: %w", dst, err)
		}
		written = append(written, dst)
	}
	return written, nil
}

// App 装配后的应用(server 是唯一知具体类型的装配根)
type App struct {
	Cfg       *Config
	St        *store.Store
	Gateway   *gateway.Gateway
	AdminSvc  *admin.Service
	AdminDeps *admin.Deps
	Recorder  *metrics.Recorder
	Registry  *upstream.Registry
	Mux       *http.ServeMux
	History   *history.Store
	trMgr     *transport.Manager
	Scheduler *scheduler.Scheduler
	// pluginEvictGate 插件 evict 限流闸门(hooks 通道与 Registry 通道共用;Build 装配)
	pluginEvictGate *evictGate
}

// transportNames 传输实例名清单(稳定序)
func (a *App) transportNames() []string { return a.trMgr.Names() }

// Build 装配全部模块(仅运行态;打包路径见 Run)
func Build(cfg *Config) (*App, error) {
	if err := cfg.EnsureDirs(); err != nil {
		return nil, err
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	ctx := context.Background()
	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	// 传输实例
	defs := make([]transport.TransportDef, 0, len(cfg.Transports))
	for _, t := range cfg.Transports {
		opts := map[string]any{}
		if t.Options.Kind != 0 {
			if err := yamlUnmarshal(&t.Options, &opts); err != nil {
				_ = st.Close()
				return nil, fmt.Errorf("transports[%s].options: %w", t.Name, err)
			}
		}
		defs = append(defs, transport.TransportDef{Name: t.Name, URL: t.URL, Options: opts})
	}
	trMgr, err := transport.NewManagerWithCfg(defs, cfg.DataDir+"/ipp")
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("transports: %w", err)
	}
	// 插件:注册中心 + 内置协议 + 目录导入
	pkgs := plugin.NewRegistry(st.DB())
	pkgs.RegisterBuiltin(builtin.Name, func(deps plugin.BuiltinDeps) (pipeline.Protocol, error) {
		return &builtin.Protocol{Config: deps.Config, PackageKey: deps.PackageKey}, nil
	})
	if err := pkgs.LoadFromDB(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load packages: %w", err)
	}
	// 内置协议实体包缺失时补装(置于 LoadFromDB 后,避免每次启动重置 revision)
	if err := ensureBuiltinPackage(ctx, pkgs); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("builtin package: %w", err)
	}
	// 目录热载:插件目录自动导入(同内容幂等;坏包警告跳过不阻塞启动)
	if err := importPluginDirs(ctx, pkgs, cfg); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("import plugins dir: %w", err)
	}
	recorder := metrics.NewRecorder()
	// 请求历史:独立库;打开失败降级为不记录(旁路诊断,不阻塞主服务)
	hist, histErr := history.New(cfg.DataDir + "/history.db")
	if histErr != nil {
		log.Printf("history open: %v (recording disabled)", histErr)
	}
	reg := upstream.NewRegistry(st.DB(), pkgs)
	if err := reg.LoadFromDB(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load models: %w", err)
	}
	gw := &gateway.Gateway{
		OpenAI: gateway.Entry{
			EntryInspector: convert.NewOpenAICodec(),
			ErrorRenderer:  convert.NewOpenAICodec(),
			FramerFactory:  convert.NewOpenAICodec(),
			Protocol:       string(plugin.ProtocolOpenAICompletions),
		},
		Anthropic: gateway.Entry{
			EntryInspector: convert.NewAnthropicCodec(),
			ErrorRenderer:  convert.NewAnthropicCodec(),
			FramerFactory:  convert.NewAnthropicCodec(),
			Protocol:       string(plugin.ProtocolAnthropicMessages),
		},
		Executor: &pipeline.Executor{
			Transports: trMgr.Get,
		},
		Registry: reg,
		Metrics:  recorder,
		History:  hist,
	}
	// 管理服务
	adminSvc, randomPass, err := admin.New(cfg.AdminUser, cfg.AdminPassBcrypt)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("admin: %w", err)
	}
	if randomPass != "" {
		// 无配置口令:打印一次,重启重新生成
		log.Printf("generated admin password (print once): %s", randomPass)
	}
	adminDeps := &admin.Deps{
		Packages: pkgs,
		Upstream: reg,
		Metrics:  recorder,
		History:  hist,
		KeysFunc: func(pkg string) map[string]any { return pkgs.Keys().View(pkg) },
	}
	mux := http.NewServeMux()
	// 反代入口(鉴权 + panic recover)
	mux.Handle("/v1/chat/completions", keyAuth(cfg.APIKeys, recoverMW(http.HandlerFunc(gw.ChatCompletions))))
	mux.Handle("/v1/messages", keyAuth(cfg.APIKeys, recoverMW(http.HandlerFunc(gw.Messages))))
	mux.Handle("GET /v1/models", keyAuth(cfg.APIKeys, recoverMW(http.HandlerFunc(gw.Models))))
	mux.Handle("GET /v1/models/{id}", keyAuth(cfg.APIKeys, recoverMW(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gw.ModelByID(w, r, r.PathValue("id"))
	}))))
	// 管理面(登录豁免 + 会话中间件 + panic recover)
	mux.HandleFunc("POST /admin/api/login", adminSvc.Login)
	mux.HandleFunc("POST /admin/api/logout", adminSvc.Logout)
	mux.Handle("/admin/api/", recoverMW(adminSvc.Middleware(adminDeps.Mux())))
	// 管理 GUI(单文件内嵌;登录页公开可达)
	mux.Handle("/admin", adminweb.Handler())
	mux.Handle("/admin/", adminweb.Handler())
	// 包骨架模板(静态无敏感信息,免会话;GUI "新建包"下载入口)
	mux.HandleFunc("GET /packages/template", adminDeps.PackageTemplate)
	// 连通性测试:最小请求走完整管道(经 executor,指标照常计数;上游挂起不阻塞管理面——上限对齐传输探测)
	adminDeps.TestFunc = func(id int64) (int64, string) {
		u, err := reg.Get(id)
		if err != nil {
			return 0, err.Error()
		}
		ctx, cancel := context.WithTimeout(context.Background(), transportProbeTimeout)
		defer cancel()
		return gw.TestUpstream(ctx, u)
	}
	// 对话测试:行名即模型名,走完整管道(非流式;模型行+包双维度计数)
	adminDeps.ChatTestFunc = func(ctx context.Context, id int64, message string) (int64, int, []byte, string) {
		u, err := reg.Get(id)
		if err != nil {
			return 0, 0, nil, err.Error()
		}
		return gw.ChatTest(ctx, u, message)
	}
	// 监控时序:metrics_minutely 最近 n 分钟(老到新)
	adminDeps.SeriesFunc = func(minutes int) ([]map[string]any, error) {
		rows, err := st.DB().Query(`SELECT minute, requests, errors, max_concurrent, by_upstream_json, by_target_json, by_package_json
			FROM (SELECT * FROM metrics_minutely ORDER BY minute DESC LIMIT ?) ORDER BY minute ASC`, minutes)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var minute string
			var reqs, errs, conc int64
			var upJSON, tgJSON, pkgJSON string
			if err := rows.Scan(&minute, &reqs, &errs, &conc, &upJSON, &tgJSON, &pkgJSON); err != nil {
				return nil, err
			}
			var byUp, byTg, byPkg any
			_ = json.Unmarshal([]byte(upJSON), &byUp)
			_ = json.Unmarshal([]byte(tgJSON), &byTg)
			_ = json.Unmarshal([]byte(pkgJSON), &byPkg)
			out = append(out, map[string]any{
				"minute": minute, "requests": reqs, "errors": errs,
				"max_concurrent": conc, "by_upstream": byUp, "by_target": byTg, "by_package": byPkg,
			})
		}
		return out, rows.Err()
	}
	// 传输实例:只读清单 + 连通测试 + 失效命令(管理台手动;默认 probe=gstatic 204)
	adminDeps.TransportsFunc = func() []map[string]any {
		defs := trMgr.Defs()
		out := make([]map[string]any, 0, len(defs))
		for _, d := range defs {
			out = append(out, map[string]any{"name": d.Name, "url": ipprovider.SanitizeURL(d.URL)})
		}
		return out
	}
	adminDeps.TransportTestFunc = func(name string) (int64, string) {
		latency, err := trMgr.Test(name, transportProbeURL, transportProbeTimeout)
		if err != nil {
			return latency, err.Error()
		}
		return latency, ""
	}
	adminDeps.TransportEvictFunc = evictForwarder(trMgr)
	// 插件 util.evict 出口:同一失效命令转发(限流闸门;仅失效,不触发重试)
	gate := newEvictGate()
	reg.SetTransportEvict(gatedEvict(gate, evictForwarder(trMgr)))
	// 健康检查
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &App{
		Cfg: cfg, St: st, Gateway: gw, AdminSvc: adminSvc, AdminDeps: adminDeps,
		Recorder: recorder, Registry: reg, Mux: mux, trMgr: trMgr,
		History: hist, pluginEvictGate: gate,
	}, nil
}

// hooks 启动:装配回调、启动调度器、对已加载包补发 onLoad(启动恢复路径)
func (a *App) startHooks() {
	a.Scheduler = wireHooks(a)
	wireKeyHooks(a)
	pkgs := a.AdminDeps.Packages
	for _, name := range pkgs.ListPackages() {
		if !pkgs.IsEnabled(name) {
			continue
		}
		pkg, err := pkgs.GetPackage(name)
		if err != nil || pkg.Manifest.Parts.Hooks == nil {
			continue
		}
		previous, _ := pkgs.Keys().PreviousAll(name)
		pkgs.OnLoad(pkg, previous)
	}
}

// Close 释放资源(传输供给方级联在先——实例拨号依赖存储后端无关,先关无序约束)
func (a *App) Close() error {
	if a.Scheduler != nil {
		a.Scheduler.Stop()
		a.Scheduler.Wait()
	}
	a.trMgr.Close()
	if a.History != nil {
		_ = a.History.Close()
	}
	return a.St.Close()
}

// recoverMW panic → 500(不带栈;handler 内 panic 兜底)
func recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic in %s %s: %v", r.Method, r.URL.Path, rec)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"type":"api_error","message":"internal error"}}`))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// keyAuth 反代入口固定 key 校验(双头:openai Bearer / anthropic x-api-key)
func keyAuth(keys []string, next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, k := range keys {
		allowed[k] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("x-api-key")
		if key == "" {
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				key = strings.TrimPrefix(auth, "Bearer ")
			}
		}
		if !allowed[key] {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"invalid api key"}}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// 传输连通测试目标与超时(管理台手动"测试连通"用;204 端点,无副作用;变量供测试覆写指向本地 mock)
var (
	transportProbeURL     = "https://www.gstatic.com/generate_204"
	transportProbeTimeout = 10 * time.Second
)

// 请求历史保留清理:执行间隔与单次超时
const (
	historyCleanupInterval = time.Hour
	historyCleanupTimeout  = 10 * time.Second
)

// aapLeaseIDHexLen aap lease_id hex 形态长度(128bit → 32 字符)
const aapLeaseIDHexLen = 16

// pluginEvictTimeout 插件 util.evict 命令超时(独立于请求生命周期)
const pluginEvictTimeout = 10 * time.Second

// 插件 evict 闸门参数:令牌桶容量与并发上限(劣质/失控插件防连接洪水;每次 evict = 一条节点连接)
const (
	pluginEvictBurst     = 5
	pluginEvictInflight  = 2
	pluginEvictRefillSec = 1
)

// evictGate 插件 evict 限流:令牌桶(按秒补齐)+ 并发上限;不通过时拒绝报错
type evictGate struct {
	mu       sync.Mutex
	tokens   float64
	last     time.Time
	inflight int
}

func newEvictGate() *evictGate {
	return &evictGate{tokens: pluginEvictBurst, last: time.Now()}
}

// allow 取一个令牌并占一个并发位;超限返回 false
func (g *evictGate) allow() bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tokens += now.Sub(g.last).Seconds() / pluginEvictRefillSec
	if g.tokens > pluginEvictBurst {
		g.tokens = pluginEvictBurst
	}
	g.last = now
	if g.inflight >= pluginEvictInflight || g.tokens < 1 {
		return false
	}
	g.tokens--
	g.inflight++
	return true
}

// done 归还并发位
func (g *evictGate) done() {
	g.mu.Lock()
	g.inflight--
	g.mu.Unlock()
}

// gatedEvict 限流包裹的插件 evict 出口
func gatedEvict(gate *evictGate, forward func(ctx context.Context, transport, scope, value string) error) func(transport, scope, value string) error {
	return func(transport, scope, value string) error {
		if !gate.allow() {
			return fmt.Errorf("evict %q/%s: 频率超限,稍后重试", transport, scope)
		}
		defer gate.done()
		ctx, cancel := context.WithTimeout(context.Background(), pluginEvictTimeout)
		defer cancel()
		return forward(ctx, transport, scope, value)
	}
}

// hooks 测试装配路径(Build 未跑)的进程级兜底闸门(sync.Once 惰性初始化,限流不因新 gate 失效)
var (
	sharedGateOnce sync.Once
	sharedGate     *evictGate
)

func sharedEvictGate() *evictGate {
	sharedGateOnce.Do(func() { sharedGate = newEvictGate() })
	return sharedGate
}

// evictForwarder scope/value 文本 → aap EVICT 命令转发(admin API 与插件 util.evict 共用)
func evictForwarder(trMgr *transport.Manager) func(ctx context.Context, name, scope, value string) error {
	return func(ctx context.Context, name, scope, value string) error {
		var scopeID uint8
		var payload []byte
		switch scope {
		case "lease": // lease_id 32 字符 hex → 16B 原始字节;全零拒绝
			id, err := hex.DecodeString(value)
			if err != nil || len(id) != aapLeaseIDHexLen {
				return admin.ErrBadScope
			}
			if bytes.Equal(id, make([]byte, aapLeaseIDHexLen)) {
				return admin.ErrBadScope
			}
			scopeID, payload = transport.EvictScopeLease, id
		case "egress": // IP 字符串 → SOCKS5 地址编码
			ip := net.ParseIP(value)
			if ip == nil {
				return admin.ErrBadScope
			}
			scopeID = transport.EvictScopeEgress
			if v4 := ip.To4(); v4 != nil {
				payload = append([]byte{1}, v4...)
			} else {
				payload = append([]byte{4}, ip.To16()...)
			}
		default:
			return admin.ErrBadScope
		}
		found, err := trMgr.Evict(ctx, name, scopeID, payload)
		if !found {
			return admin.ErrBadScope // 非 aap 传输
		}
		return err
	}
}


// Run 启动 HTTP 服务,阻塞至退出信号;优雅排空(30s 上限);PackDir 非空时只打包
func Run(cfg *Config) error {
	if err := cfg.EnsureDirs(); err != nil {
		return err
	}
	if cfg.PackDir != "" {
		return packAll(cfg)
	}
	app, err := Build(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = app.Close() }()
	app.startHooks()
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           app.Mux,
		ReadHeaderTimeout: 10 * time.Second,
		// 无 WriteTimeout(SSE 长流)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	// 退出信号(server.md:阻塞至退出信号;main 不重复监听)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	// metrics 分钟快照落库(对齐自然分钟边界)
	stopSnap := make(chan struct{})
	snapDone := make(chan struct{})
	go func() {
		defer close(snapDone)
		now := time.Now()
		next := now.Truncate(time.Minute).Add(time.Minute)
		timer := time.NewTimer(next.Sub(now))
		defer timer.Stop()
		for {
			select {
			case <-stopSnap:
				return
			case tick := <-timer.C:
				persistSnapshot(app, tick)
				timer.Reset(time.Minute)
			}
		}
	}()
	// 请求历史保留清理:启动清一次,此后按小时
	stopClean := make(chan struct{})
	cleanDone := make(chan struct{})
	go func() {
		defer close(cleanDone)
		cleanupHistory(app)
		timer := time.NewTimer(historyCleanupInterval)
		defer timer.Stop()
		for {
			select {
			case <-stopClean:
				return
			case <-timer.C:
				cleanupHistory(app)
				timer.Reset(historyCleanupInterval)
			}
		}
	}()
	log.Printf("ai-api-proxy listening on %s | packages: %d | upstreams: %d | entries: POST /v1/chat/completions, POST /v1/messages, GET /v1/models | admin: /admin",
		cfg.Listen, len(app.AdminDeps.Packages.ListPackages()), len(app.Registry.List()))
	select {
	case <-sigCh:
	case err = <-errCh:
		if err == http.ErrServerClosed {
			err = nil
		}
		if err != nil {
			close(stopSnap)
			<-snapDone
			close(stopClean)
			<-cleanDone
			return err
		}
	}
	// 优雅退出:停新连接 → 停快照 → 排空(30s 上限)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	close(stopSnap)
	<-snapDone
	close(stopClean)
	<-cleanDone
	return nil
}

// cleanupHistory 请求历史保留期清理(retention<=0 = 永久保留,不清理)
func cleanupHistory(app *App) {
	if app.History == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), historyCleanupTimeout)
	defer cancel()
	n, err := app.History.Cleanup(ctx, app.Cfg.HistoryRetentionDays)
	if err != nil {
		log.Printf("history cleanup: %v", err)
		return
	}
	if n > 0 {
		log.Printf("history cleanup: removed %d entries", n)
	}
}

// builtinProtocolJS 内置包哑 protocol.js:实现走 Go 工厂;settings 片段声明参数槽(声明统一入口)
// transport = 命名传输实例名(direct = 内置直连;命名实例在 config.yaml transports 配置),自由串非枚举
const builtinProtocolJS = `// builtin protocol implemented in Go
module.exports.settings = {
  base_url: setting.string({ description: "上游地址(如 https://api.example.com)", required: true }),
  transport: setting.string({ description: "传输实例名(direct=内置直连;命名实例在 config.yaml transports 配置)" }),
};
`

// ensureBuiltinPackage 内置协议实体包缺失或版本不符时自动安装(哑 manifest,实现走 Go 工厂;v2:版本不符强制原位升级)
func ensureBuiltinPackage(ctx context.Context, pkgs *plugin.Registry) error {
	builtinVersion := "2.0.0"
	if pkg, err := pkgs.GetPackage(builtin.Name); err == nil && pkg.Manifest.Version == builtinVersion {
		return nil
	}
	manifest := `{"manifestVersion":1,"name":"` + builtin.Name + `","version":"` + builtinVersion + `","parts":{
		"protocol":{"protocol":"` + builtin.DeclaredProtocol + `","features":["tools","vision"]}}}`
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		return err
	}
	if _, err := mf.Write([]byte(manifest)); err != nil {
		return err
	}
	// 哑 entry(约定路径 protocol.js):实现走 Go 工厂,但包校验要求文件存在;settings 片段声明参数槽
	ef, err := zw.Create(plugin.ProtocolEntry)
	if err != nil {
		return err
	}
	if _, err := ef.Write([]byte(builtinProtocolJS)); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return pkgs.Install(ctx, buf.Bytes())
}

// importPluginDirs 装配期导入:普通目录按文件序全量导入(同名后者胜),内置目录只补缺(不覆盖用户包)
func importPluginDirs(ctx context.Context, pkgs *plugin.Registry, cfg *Config) error {
	srcs, err := searchPluginDirs(cfg)
	if err != nil {
		return err
	}
	seen := map[string]string{} // 包名 → 源路径(同轮同名冲突告警)
	for _, s := range srcs {
		if s.builtin {
			if data, err := loadSource(s.path); err == nil {
				if pkg, err := plugin.ParseAAP(data); err == nil {
					if _, err := pkgs.GetPackage(pkg.Manifest.Name); err == nil {
						continue // 用户已有同名包:内置不覆盖
					}
				}
			}
		}
		if err := installSource(ctx, pkgs, s.path, seen); err != nil {
			// 可选目录语义:坏包/冲突告警跳过,不阻塞网关启动
			log.Printf("plugins dir: install %s: %v (skipped)", s.path, err)
		}
	}
	return nil
}

// loadSource 读来源为 .aap 字节(目录现打包,单 .aap 直接读)
func loadSource(src string) ([]byte, error) {
	fi, err := os.Stat(src)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return plugin.PackDir(src)
	}
	return os.ReadFile(src)
}

// installSource 同内容跳过;变化才 Install(升级 revision 并重新启用)。
// 同轮两个来源声明同名包:按来源序后者胜,并告警提示去重
func installSource(ctx context.Context, pkgs *plugin.Registry, src string, seen map[string]string) error {
	data, err := loadSource(src)
	if err != nil {
		return err
	}
	pkg, err := plugin.ParseAAP(data)
	if err != nil {
		return err
	}
	if prev, dup := seen[pkg.Manifest.Name]; dup {
		log.Printf("plugins dir: duplicate package name %q in %s and %s (later source wins)", pkg.Manifest.Name, prev, src)
	}
	seen[pkg.Manifest.Name] = src
	if existing, err := pkgs.GetPackage(pkg.Manifest.Name); err == nil {
		if reflect.DeepEqual(existing.Manifest, pkg.Manifest) && reflect.DeepEqual(existing.Files, pkg.Files) {
			return nil
		}
	}
	if err := pkgs.Install(ctx, data); err != nil {
		return err
	}
	if existing, err := pkgs.GetPackage(pkg.Manifest.Name); err == nil && existing.Revision > 1 {
		log.Printf("plugins dir: upgraded %s from %s to revision %d", pkg.Manifest.Name, src, existing.Revision)
	}
	return nil
}

// persistSnapshot 计数快照落 metrics_minutely;now 为覆盖分钟右边界,计数属上一分钟
func persistSnapshot(app *App, now time.Time) {
	row := app.Recorder.Take(now.Add(-time.Minute))
	upJSON, err := row.UpstreamJSON()
	if err != nil {
		log.Printf("snapshot by_upstream: %v", err)
		return
	}
	tgJSON, err := row.TargetJSON()
	if err != nil {
		log.Printf("snapshot by_target: %v", err)
		return
	}
	pkgJSON, err := row.PackageJSON()
	if err != nil {
		log.Printf("snapshot by_package: %v", err)
		return
	}
	_, err = app.St.DB().Exec(`INSERT OR REPLACE INTO metrics_minutely
		(minute, requests, errors, max_concurrent, by_upstream_json, by_target_json, by_package_json) VALUES(?,?,?,?,?,?,?)`,
		row.Minute, row.Requests, row.Errors, row.MaxConc, upJSON, tgJSON, pkgJSON)
	if err != nil {
		log.Printf("snapshot persist: %v", err)
	}
}


package server

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/adminweb"
	"github.com/mzzsfy/ai-api-proxy/internal/builtin"
	"github.com/mzzsfy/ai-api-proxy/internal/convert"
	"github.com/mzzsfy/ai-api-proxy/internal/gateway"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
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
	Secrets   upstream.SecretsStore
	Registry  *upstream.Registry
	Mux       *http.ServeMux
	trMgr     *transport.Manager
}

// transportNames 传输实例名清单(稳定序)
func (a *App) transportNames() []string { return a.trMgr.Names() }

// TransportTest 单传输连通测试
func (a *App) TransportTest(name, probeURL string, timeout time.Duration) (int64, error) {
	return a.trMgr.Test(name, probeURL, timeout)
}

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
		defs = append(defs, transport.TransportDef{Name: t.Name, Type: t.Type, URL: t.URL, Options: opts})
	}
	trMgr, err := transport.NewManagerWithCfg(defs, nil, cfg.DataDir+"/ipp")
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("transports: %w", err)
	}
	// 插件:注册中心 + 内置协议 + 目录导入
	pkgs := plugin.NewRegistry(st.DB())
	pkgs.RegisterBuiltin(builtin.Name, func(deps plugin.BuiltinDeps) (pipeline.Protocol, error) {
		return &builtin.Protocol{TargetSecrets: deps.TargetSecrets}, nil
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
	secrets := &storeSecrets{st: st}
	reg := upstream.NewRegistry(st.DB(), pkgs, secrets)
	if err := reg.LoadFromDB(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load upstreams: %w", err)
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
			OnTargetExit: func(up, tg string, failed bool) {
				recorder.EnterTarget(up, tg)(failed)
			},
		},
		Registry: reg,
		Metrics:  recorder,
		Secrets:  secrets,
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
		Secrets:  secrets,
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
	// 连通性测试:最小请求走完整管道(经 executor,指标照常计数)
	adminDeps.TestFunc = func(id int64) (int64, string) {
		u, err := reg.Get(id)
		if err != nil {
			return 0, err.Error()
		}
		return gw.TestUpstream(context.Background(), u)
	}
	// 监控时序:metrics_minutely 最近 n 分钟(老到新)
	adminDeps.SeriesFunc = func(minutes int) ([]map[string]any, error) {
		rows, err := st.DB().Query(`SELECT minute, requests, errors, max_concurrent, by_upstream_json, by_target_json
			FROM (SELECT * FROM metrics_minutely ORDER BY minute DESC LIMIT ?) ORDER BY minute ASC`, minutes)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var minute string
			var reqs, errs, conc int64
			var upJSON, tgJSON string
			if err := rows.Scan(&minute, &reqs, &errs, &conc, &upJSON, &tgJSON); err != nil {
				return nil, err
			}
			var byUp, byTg any
			_ = json.Unmarshal([]byte(upJSON), &byUp)
			_ = json.Unmarshal([]byte(tgJSON), &byTg)
			out = append(out, map[string]any{
				"minute": minute, "requests": reqs, "errors": errs,
				"max_concurrent": conc, "by_upstream": byUp, "by_target": byTg,
			})
		}
		return out, rows.Err()
	}
	// 传输实例:只读清单(含最近健康探测结果) + 连通测试(代理链路探测;默认 probe=gstatic 204)
	adminDeps.TransportsFunc = func() []map[string]any {
		defs := trMgr.Defs()
		out := make([]map[string]any, 0, len(defs))
		for _, d := range defs {
			item := map[string]any{"name": d.Name, "type": d.Type, "url": d.URL}
			if v, ok, _ := st.KVGet(context.Background(), "transport_health", d.Name); ok {
				var hs transportHealthStatus
				if jsonUnmarshal([]byte(v), &hs) == nil {
					item["health"] = hs
				}
			}
			out = append(out, item)
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
	// 健康检查
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &App{
		Cfg: cfg, St: st, Gateway: gw, AdminSvc: adminSvc, AdminDeps: adminDeps,
		Recorder: recorder, Secrets: secrets, Registry: reg, Mux: mux, trMgr: trMgr,
	}, nil
}

// Close 释放资源(传输供给方级联在先——实例拨号依赖存储后端无关,先关无序约束)
func (a *App) Close() error {
	a.trMgr.Close()
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

// 传输连通探测目标与超时(204 端点,无副作用;变量供测试覆写指向本地 mock)
var (
	transportProbeURL     = "https://www.gstatic.com/generate_204"
	transportProbeTimeout = 10 * time.Second
)

// transportHealthStatus 单传输最近探测结果(序列化进 kv,ns=transport_health)
type transportHealthStatus struct {
	OK        bool     `json:"ok"`
	LatencyMS int64    `json:"latency_ms"`
	Error     string   `json:"error,omitempty"`
	CheckedAt string   `json:"checked_at"`
	EgressIPs []string `json:"egress_ips,omitempty"` // ipp_* 供给方出口(非 ipp 无此字段)
}

// startTransportProbeLoop 周期探测全部传输实例,结果落 kv 供 GUI 读取;返回停止函数(同步等待退出)
func startTransportProbeLoop(app *App, interval time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProbe := func() {
			for _, name := range app.transportNames() {
				latency, err := app.TransportTest(name, transportProbeURL, transportProbeTimeout)
				st := transportHealthStatus{OK: err == nil, LatencyMS: latency, CheckedAt: utilNowRFC3339()}
				if err != nil {
					st.Error = err.Error()
				}
				if prov, ok := app.trMgr.Provider(name); ok {
					st.EgressIPs = prov.Stats().EgressIPs
				}
				b, mErr := jsonMarshal(st)
				if mErr != nil {
					continue
				}
				_ = app.St.KVSet(context.Background(), "transport_health", name, string(b))
			}
		}
		runProbe() // 启动即探一轮
		timer := time.NewTicker(interval)
		defer timer.Stop()
		for {
			select {
			case <-stop:
				return
			case <-timer.C:
				runProbe()
			}
		}
	}()
	return func() { close(stop); <-done }
}

// utilNowRFC3339 当前时间(测试确定性无关,直接格式化)
func utilNowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// storeSecrets 凭据存储实现(kv 表 ns=upstream/target 键)
type storeSecrets struct{ st *store.Store }

// secretsKey kv 键
func secretsKey(upstream, target string) string { return "secrets:" + upstream + "/" + target }

func (s *storeSecrets) UpsertTargetSecrets(upstream, target string, secrets map[string]string) error {
	b, err := jsonMarshal(secrets)
	if err != nil {
		return err
	}
	return s.st.KVSet(context.Background(), "upstream:"+upstream, secretsKey(upstream, target), string(b))
}

func (s *storeSecrets) GetTargetSecrets(upstream, target string) (map[string]string, bool) {
	v, ok, err := s.st.KVGet(context.Background(), "upstream:"+upstream, secretsKey(upstream, target))
	if err != nil || !ok {
		return nil, false
	}
	var m map[string]string
	if jsonUnmarshal([]byte(v), &m) != nil {
		return nil, false
	}
	return m, true
}

func (s *storeSecrets) DeleteTargetSecrets(upstream, target string) {
	_ = s.st.KVDelete(context.Background(), "upstream:"+upstream, secretsKey(upstream, target))
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
	// 传输健康周期探测(interval<=0 关闭)
	var stopProbe func()
	if probeInterval := time.Duration(app.Cfg.TransportProbeIntervalSec) * time.Second; probeInterval > 0 {
		stopProbe = startTransportProbeLoop(app, probeInterval)
	}
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
			return err
		}
	}
	// 优雅退出:停新连接 → 停快照 → 停探测 → 排空(30s 上限)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	close(stopSnap)
	<-snapDone
	if stopProbe != nil {
		stopProbe()
	}
	return nil
}

// ensureBuiltinPackage 内置协议实体包缺失时自动安装(哑 manifest,实现走 Go 工厂)
func ensureBuiltinPackage(ctx context.Context, pkgs *plugin.Registry) error {
	if _, err := pkgs.GetPackage(builtin.Name); err == nil {
		return nil
	}
	manifest := `{"manifestVersion":1,"name":"` + builtin.Name + `","version":"1.0.0","parts":{
		"protocol":{"entry":"builtin.go","protocol":"` + builtin.DeclaredProtocol + `","form":["` +
		string(plugin.FormStreaming) + `","` + string(plugin.FormNonStreaming) +
		`"],"features":["tools","vision"],"secretRefs":["api_key"]}}}`
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		return err
	}
	if _, err := mf.Write([]byte(manifest)); err != nil {
		return err
	}
	// 哑 entry:实现走 Go 工厂,但包校验要求 entry 文件存在
	ef, err := zw.Create("builtin.go")
	if err != nil {
		return err
	}
	if _, err := ef.Write([]byte("// builtin protocol implemented in Go\n")); err != nil {
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

// persistSnapshot 计数快照落 metrics_minutely
func persistSnapshot(app *App, now time.Time) {
	row := app.Recorder.Take(now)
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
	_, err = app.St.DB().Exec(`INSERT OR REPLACE INTO metrics_minutely
		(minute, requests, errors, max_concurrent, by_upstream_json, by_target_json) VALUES(?,?,?,?,?,?)`,
		row.Minute, row.Requests, row.Errors, row.MaxConc, upJSON, tgJSON)
	if err != nil {
		log.Printf("snapshot persist: %v", err)
	}
}

// jsonMarshal / jsonUnmarshal 局部引用
func jsonMarshal(v any) ([]byte, error)   { return json.Marshal(v) }
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

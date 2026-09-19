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
	"strings"
	"syscall"
	"time"

	"ai-api-proxy/internal/admin"
	"ai-api-proxy/internal/adminweb"
	"ai-api-proxy/internal/builtin"
	"ai-api-proxy/internal/convert"
	"ai-api-proxy/internal/gateway"
	"ai-api-proxy/internal/metrics"
	"ai-api-proxy/internal/pipeline"
	"ai-api-proxy/internal/plugin"
	"ai-api-proxy/internal/store"
	"ai-api-proxy/internal/transport"
	"ai-api-proxy/internal/upstream"
)

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

// Build 装配全部模块
func Build(cfg *Config) (*App, error) {
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
		defs = append(defs, transport.TransportDef{Name: t.Name, Type: t.Type, URL: t.URL})
	}
	trMgr, err := transport.NewManager(defs)
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
	recorder := metrics.NewRecorder()
	secrets := &storeSecrets{st: st}
	reg := upstream.NewRegistry(st.DB(), pkgs, secrets)
	if err := reg.LoadFromDB(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load upstreams: %w", err)
	}
	gw := &gateway.Gateway{
		OpenAI:    convert.NewOpenAICodec(),
		Anthropic: convert.NewAnthropicCodec(),
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
	adminDeps.SeriesFunc = func(minutes int) ([]map[string]any, error) {		rows, err := st.DB().Query(`SELECT minute, requests, errors, max_concurrent, by_upstream_json, by_target_json
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

// Close 释放资源
func (a *App) Close() error { return a.St.Close() }

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
	OK        bool   `json:"ok"`
	LatencyMS int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
	CheckedAt string `json:"checked_at"`
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

// Run 启动 HTTP 服务,阻塞至退出信号;优雅排空(30s 上限)
func Run(cfg *Config) error {
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
		"protocol":{"entry":"builtin.go","form":["streaming","non_streaming"],"features":["tools","vision"],"secretRefs":["api_key"]}}}`
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

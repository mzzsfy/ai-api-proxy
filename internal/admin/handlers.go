package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/history"
	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// Deps 管理 API 业务依赖
type Deps struct {
	Packages *plugin.Registry
	Upstream *upstream.Registry
	Metrics  *metrics.Recorder
	History  *history.Store                                          // 请求历史(nil = 未装配,对应路由 501)
	TestFunc func(modelID int64) (latencyMS int64, errMsg string)    // 连通性测试(走完整管道)
	// ChatTestFunc 对话测试(走完整管道,非流式;行名即模型名;返回延迟/状态码/响应体/错误说明)
	ChatTestFunc func(ctx context.Context, modelID int64, message string) (latencyMS int64, status int, body []byte, errMsg string)
	// SeriesFunc 最近 n 个分钟点(老到新;空切片=无数据)
	SeriesFunc func(minutes int) ([]map[string]any, error)
	// TransportsFunc 命名传输实例清单(只读;名称+URL)
	TransportsFunc func() []map[string]any
	// TransportTestFunc 单传输实例连通测试(发一次真实 HEAD;返回延迟与错误)
	TransportTestFunc func(name string) (int64, string)
	// TransportEvictFunc 失效命令(scope:"lease"|"egress";非法 scope/value 返回 errBadScope 语义错误)
	TransportEvictFunc func(ctx context.Context, name, scope, value string) error
	// FetchPackage URL 拉取实现(nil=fetchPackage;测试覆写注入:httptest 源站在回环,生产路径强制公网校验)
	FetchPackage func(r *http.Request, url string) ([]byte, error)
	// KeysFunc 包级 keys 明文视图(按包名隔离)
	KeysFunc func(pkg string) map[string]any
	// KeyHooksFunc keys 钩子能力探测(write/read/form)
	KeyHooksFunc func(pkg string) (write, read, form bool)
	// KeyWriteFunc 单键写入(baseUpdatedAt 快检 + keyWrite 归一化 + 轮转落库)
	KeyWriteFunc func(pkg, key string, value any, baseUpdatedAt int64) (updated int64, transformed bool, err error)
	// KeyReadFunc 键详情解释
	KeyReadFunc func(pkg, key string) (any, error)
	// KeyFormFunc 添加表单声明
	KeyFormFunc func(pkg string) (any, error)
	// KeyActionFunc 表单按钮回调(可出站)
	KeyActionFunc func(pkg, action string, values map[string]any) (any, error)
	// KeySubmitFunc 表单提交(回调内 ctx.keys.merge 写入;written 收集;errors=字段级拒绝)
	KeySubmitFunc func(pkg string, values map[string]any) ([]string, any, map[string]any, error)
}

// fetch 拉取实现取依赖覆写,缺省内置实现
func (d *Deps) fetch(r *http.Request, url string) ([]byte, error) {
	if d.FetchPackage != nil {
		return d.FetchPackage(r, url)
	}
	return fetchPackage(r, url)
}

// Mux 构建管理 API 路由(挂在 /admin/api 前缀,已过会话中间件)
func (d *Deps) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/api/me", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, map[string]any{"ok": true}) })
	mux.HandleFunc("GET /admin/api/packages", d.listPackages)
	mux.HandleFunc("POST /admin/api/packages", d.installPackage)
	mux.HandleFunc("POST /admin/api/packages/inspect", d.inspectPackage)
	mux.HandleFunc("POST /admin/api/packages/import-url", d.installPackageFromURL)
	mux.HandleFunc("GET /admin/api/packages/{name}/export", d.exportPackage)
	mux.HandleFunc("DELETE /admin/api/packages/{name}", d.deletePackage)
	mux.HandleFunc("POST /admin/api/packages/{name}/enable", d.enablePackage)
	mux.HandleFunc("GET /admin/api/packages/{name}/keys", d.packageKeys)
	mux.HandleFunc("PUT /admin/api/packages/{name}/keys/{key}", d.putPackageKey)
	mux.HandleFunc("GET /admin/api/packages/{name}/keys/{key}/detail", d.packageKeyDetail)
	mux.HandleFunc("POST /admin/api/packages/{name}/keys/form", d.packageKeyForm)
	mux.HandleFunc("POST /admin/api/packages/{name}/keys/form-action", d.packageKeyFormAction)
	mux.HandleFunc("POST /admin/api/packages/{name}/keys/form-submit", d.packageKeyFormSubmit)
	mux.HandleFunc("GET /admin/api/packages/{name}/settings", d.packageSettings)
	mux.HandleFunc("PUT /admin/api/packages/{name}/settings", d.savePackageSettings)
	mux.HandleFunc("GET /admin/api/models", d.listUpstreams)
	mux.HandleFunc("POST /admin/api/models", d.saveUpstream)
	mux.HandleFunc("GET /admin/api/models/{id}", d.getUpstream)
	mux.HandleFunc("PUT /admin/api/models/{id}", d.saveUpstreamByID)
	mux.HandleFunc("DELETE /admin/api/models/{id}", d.deleteUpstream)
	mux.HandleFunc("POST /admin/api/models/{id}/test", d.testUpstream)
	mux.HandleFunc("POST /admin/api/models/{id}/chat-test", d.chatTestUpstream)
	mux.HandleFunc("GET /admin/api/metrics/live", d.metricsLive)
	mux.HandleFunc("GET /admin/api/metrics/series", d.metricsSeries)
	mux.HandleFunc("GET /admin/api/history", d.listHistory)
	mux.HandleFunc("GET /admin/api/history/{id}", d.getHistory)
	mux.HandleFunc("GET /admin/api/transports", d.listTransports)
	mux.HandleFunc("POST /admin/api/transports/{name}/test", d.testTransport)
	mux.HandleFunc("POST /admin/api/transports/{name}/evict", d.evictTransport)
	return mux
}

func (d *Deps) listPackages(w http.ResponseWriter, r *http.Request) {
	names := d.Packages.ListPackages()
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		p, err := d.Packages.GetPackage(n)
		if err != nil {
			continue
		}
		filterNames := make([]string, 0, len(p.Manifest.Parts.Filters))
		for _, fp := range p.Manifest.Parts.Filters {
			filterNames = append(filterNames, fp.Name)
		}
		out = append(out, map[string]any{
			"name": n, "version": p.Manifest.Version, "revision": p.Revision,
			"hasProtocol": p.HasProtocol(), "filters": filterNames,
			"protocol":    protocolName(p),
			"hasHooks":    p.Manifest.Parts.Hooks != nil,
			"enabled":     d.Packages.IsEnabled(n),
			"title":       p.MetaTitle(),
			"description": p.Manifest.Description,
			"author":      p.Manifest.Author,
			"homepage":    p.Manifest.Homepage,
			"license":     p.Manifest.License,
		})
	}
	writeJSON(w, out)
}

// protocolName 主包声明的协议全名(无 protocol 部件为空串)
func protocolName(p *plugin.Package) string {
	if p.Manifest.Parts.Protocol == nil {
		return ""
	}
	return p.Manifest.Parts.Protocol.Protocol
}

// inspectPackage 解析包摘要但不安装(octet-stream=字节;application/json {"url"}=服务端拉取)
func (d *Deps) inspectPackage(w http.ResponseWriter, r *http.Request) {
	var data []byte
	switch ct := r.Header.Get("Content-Type"); {
	case strings.HasPrefix(ct, "application/json"):
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil || req.URL == "" {
			httpError(w, http.StatusBadRequest, "url required")
			return
		}
		fetched, err := d.fetch(r, req.URL)
		if errors.Is(err, errTooLarge) {
			httpError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if err != nil {
			httpError(w, http.StatusBadGateway, err.Error())
			return
		}
		data = fetched
	default:
		b, err := readLimited(r.Body)
		if errors.Is(err, errTooLarge) {
			httpError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if err != nil {
			httpError(w, http.StatusBadRequest, "read body")
			return
		}
		data = b
	}
	summary, err := d.Packages.Inspect(data)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, summary)
}

// fetchPackage 从 http(s) URL 拉取 .aap 字节(仅 http(s);限 8MB;拨号级公网校验防 SSRF,校验与连接同一次解析免疫 DNS rebinding)
func fetchPackage(r *http.Request, url string) ([]byte, error) {
	u, err := neturl.Parse(url)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("only http(s) url allowed")
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
		// 每次重定向后的新拨号同样经 publicDial 复检,无需在此重复域名校验
		Transport: &http.Transport{DialContext: publicDial},
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch status %d", resp.StatusCode)
	}
	if resp.ContentLength > maxUpload {
		return nil, errTooLarge
	}
	return readLimited(resp.Body)
}

// publicDial 拨号级 SSRF 防线:解析结果逐一公网校验后直连该 IP(TLS ServerName 仍取 URL 域名,不影响证书校验)
func publicDial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("dial addr: %w", err)
	}
	addrs, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("resolve host: %w", err)
	}
	if len(addrs) == 0 {
		return nil, errors.New("resolve host: no addresses")
	}
	var dialIP net.IP
	for _, ip := range addrs {
		if forbiddenIP(ip) {
			return nil, fmt.Errorf("address %s is not allowed", ip)
		}
		// 地址族与拨号网络对齐(tcp4 不选 v6,反之亦然);默认 tcp 双栈均可
		if network == "tcp4" && ip.To4() == nil {
			continue
		}
		if network == "tcp6" && ip.To4() != nil {
			continue
		}
		if dialIP == nil {
			dialIP = ip
		}
	}
	if dialIP == nil {
		return nil, fmt.Errorf("no eligible %s address for host %q", network, host)
	}
	d := net.Dialer{}
	return d.DialContext(ctx, network, net.JoinHostPort(dialIP.String(), port))
}

// forbiddenIP 非公网地址判定(环回/私网/链路本地/组播/未指定/CGNAT/基准测试/保留段/IPv6 文档段)
func forbiddenIP(ip net.IP) bool {
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		// 0.0.0.0/8"本网络"段、240/4 保留段(含 255.255.255.255 广播)
		if v4[0] == 0 || v4[0] >= 240 {
			return true
		}
		// 100.64/10 CGNAT 共享地址段
		if v4[0] == 100 && v4[1] >= 64 && v4[1] < 128 {
			return true
		}
		// 198.18/15 基准测试段;192.0.0/24 IETF 协议分配段
		if v4[0] == 198 && (v4[1] == 18 || v4[1] == 19) {
			return true
		}
		if v4[0] == 192 && v4[1] == 0 && v4[2] == 0 {
			return true
		}
		return false
	}
	// IPv6:2001:db8::/32 文档段(ULA fc00::/7 已由 IsPrivate 覆盖)
	return len(ip) == 16 && ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8
}

// maxUpload 上传/拉取包大小上限;读取按上限+1 判定超限(拒绝而非静默截断)
const maxUpload = 8 * 1024 * 1024

// errTooLarge 超限信号(调用方映射 413)
var errTooLarge = errors.New("package exceeds size limit")

// readLimited 读至多 maxUpload+1 字节;超出返回 errTooLarge
func readLimited(rd io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(rd, maxUpload+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxUpload {
		return nil, errTooLarge
	}
	return b, nil
}

func (d *Deps) installPackage(w http.ResponseWriter, r *http.Request) {
	data, err := readLimited(r.Body)
	if errors.Is(err, errTooLarge) {
		httpError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if err != nil {
		httpError(w, http.StatusBadRequest, "read body")
		return
	}
	if err := d.Packages.Install(r.Context(), data); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// installPackageFromURL 从 URL 拉取 .aap 安装(body: {"url": "https://.../x.aap"})
// 管理面已过会话鉴权;经 fetchPackage(仅 http(s)/公网校验/重定向复检/8MB 上限)
func (d *Deps) installPackageFromURL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil || req.URL == "" {
		httpError(w, http.StatusBadRequest, "url required")
		return
	}
	data, err := d.fetch(r, req.URL)
	if errors.Is(err, errTooLarge) {
		httpError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if err != nil {
		httpError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := d.Packages.Install(r.Context(), data); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// deletePackage 卸载包(被模型行引用拒绝;内置包拒绝)
func (d *Deps) deletePackage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var refs []string
	for _, u := range d.Upstream.List() {
		if u.Plugin == name {
			refs = append(refs, u.Name)
		}
	}
	if len(refs) > 0 {
		httpError(w, http.StatusConflict, "package referenced by: "+strings.Join(refs, ","))
		return
	}
	if err := d.Packages.Delete(r.Context(), name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// exportPackage 导出 .aap(attachment 下载)
func (d *Deps) exportPackage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	data, err := d.Packages.Export(name)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`.aap"`)
	_, _ = w.Write(data)
}

// PackageTemplate 最小骨架包下载(protocol+filter+hooks 任务各一,开发者起步模板;静态无敏感信息,免会话)
func (d *Deps) PackageTemplate(w http.ResponseWriter, r *http.Request) {
	tpl := &plugin.Manifest{
		ManifestVersion: plugin.ManifestVersion,
		Name:            "my-package",
		Version:         "0.1.0",
		Title:           "示例包",
		Description:     "protocol + filter + 定时任务起步模板",
	}
	tpl.Parts.Protocol = &plugin.ProtocolPart{
		Protocol: string(plugin.ProtocolOpenAICompletions),
		Features: []string{"tools"},
	}
	tpl.Parts.Filters = []plugin.FilterPart{{Name: "log-request"}}
	tpl.Parts.Hooks = &plugin.HooksPart{Tasks: []plugin.HooksTask{
		{Name: "heartbeat", Cron: "0 * * * *", TimeoutMs: 10 * 1000},
	}}
	files := map[string][]byte{
		"protocol.js": []byte(`// openai-completions 起步模板:按需改造(路径约定 protocol.js;流式=导出 mapEvent,非流式=导出 mapResponse)
// v2:连接信息=包参数(config.base_url,本文件末尾声明);密钥=包级 keys(util.key)
module.exports = function (config) {
  return {
    buildRequest: function (ctx, entry) {
      return { url: config.base_url + "/v1/chat/completions", method: "POST",
        headers: { "Content-Type": "application/json", Authorization: "Bearer " + util.key("api_key") },
        body: entry, stream: ctx.vars.entryStream };
    },
    mapEvent: function (ctx, e) {
      var f = JSON.parse(e);
      if (f.data === "[DONE]") return null;
      return JSON.stringify([JSON.parse(f.data)]);
    },
    mapResponse: function (ctx, body) { return body; }
  };
};
module.exports.settings = {
  base_url: setting.string({ description: "上游地址", required: true }),
};`),
		"filters/log-request.js": []byte(`// filter 起步模板:透传(路径约定 filters/<name>.js;参数槽=本文件 settings 片段声明)
module.exports = {
  mapRequest: function (ctx, p) { return p; },
  mapChunk: function (ctx, c) { return c; },
  mapResponse: function (ctx, r) { return r; }
};`),
		"settings.js": []byte(`// 共享声明文件:包级配置槽位(任意族文件可挂 module.exports.settings,固定序合并)
module.exports.settings = {
  greeting: setting.string({ description: "问候语", default: "hello" }),
};`),
		"tasks/heartbeat.js": []byte(`// 定时任务起步模板:crontab 每行一时刻(manifest parts.hooks.tasks);handler 必导出
var greeting = require("../settings.js");
module.exports = {
  handler: function (ctx) {
    log.info("heartbeat", ctx.task, util.template("{g}", { g: "ok" }));
  },
};`),
	}
	data, err := plugin.BuildAAP(tpl, files)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="my-package.aap"`)
	_, _ = w.Write(data)
}

// packageSettings GET 展开视图:声明 ⊕ overrides + 乐观锁 version(禁用包放行;参数表全包开放,v2 声明统一)
func (d *Deps) packageSettings(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pkg, err := d.Packages.GetPackage(name)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	view := d.Packages.Settings().View(name)
	overrides := view.Overrides
	if overrides == nil {
		overrides = map[string]any{}
	}
	// 展开视图:每声明槽位 {schema, value(覆盖值;无则 default 缺省), overridden}
	cfgOv, _ := overrides["config"].(map[string]any)
	config := buildSettingsView(decl, cfgOv)
	tasksOv, _ := overrides["tasks"].(map[string]any)
	writeJSON(w, map[string]any{
		"config":   config,
		"tasks":    buildTasksView(pkg, tasksOv),
		"version":  view.Version,
		"revision": pkg.Revision,
	})
}

// buildSettingsView 声明槽位展开(值 = 覆盖 > default;overridden 标记)
func buildSettingsView(decl map[string]any, cfgOv map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(decl))
	for _, name := range sortedKeys(decl) {
		slot, _ := decl[name].(map[string]any)
		if slot == nil {
			continue
		}
		item := map[string]any{"name": name, "schema": slot}
		if v, ok := cfgOv[name]; ok {
			item["value"] = v
			item["overridden"] = true
		} else {
			item["value"] = slot["default"]
			item["overridden"] = false
		}
		out = append(out, item)
	}
	return out
}

// buildTasksView 任务行展开(cron/next/覆盖状态;nextRun 查询归前端经 effectiveTasks)
func buildTasksView(pkg *plugin.Package, tasksOv map[string]any) []map[string]any {
	h := pkg.Manifest.Parts.Hooks
	if h == nil {
		return nil
	}
	out := make([]map[string]any, 0, len(h.Tasks))
	for _, tk := range h.Tasks {
		item := map[string]any{
			"name":       tk.Name,
			"cron":       tk.Cron,
			"next":       tk.Next,
			"timeoutMs":  tk.TimeoutMs,
			"overridden": false,
		}
		if ov, ok := tasksOv[tk.Name].(map[string]any); ok {
			if c, ok := ov["cron"].(string); ok && c != "" {
				item["cron"] = c
				item["overridden"] = true
			}
			if dis, ok := ov["disabled"].(bool); ok {
				item["enabled"] = !dis
			} else {
				item["enabled"] = true
			}
		} else {
			item["enabled"] = true
		}
		out = append(out, item)
	}
	return out
}

// sortedKeys 稳定序键列表
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// savePackageSettings PUT:version 最先校验(不匹配 409)→ 槽位校验(声明外剥离/类型校验)→ 剔除默认 → 等值跳写(参数表全包开放)
func (d *Deps) savePackageSettings(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pkg, err := d.Packages.GetPackage(name)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	var req struct {
		Config      map[string]any            `json:"config"`
		Tasks       map[string]map[string]any `json:"tasks"`
		BaseVersion int64                     `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	// 乐观锁最先:过期即 409(提示可能由任务或升级写入)
	cur := d.Packages.Settings().View(name).Version
	if req.BaseVersion != cur {
		httpError(w, http.StatusConflict, "settings modified (version mismatch); reload and retry")
		return
	}
	// 槽位校验:声明外键剥离;类型粗校验
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	cfgOut := map[string]any{}
	for k, v := range req.Config {
		schema, ok := decl[k].(map[string]any)
		if !ok {
			continue // 声明外键剥离
		}
		if err := plugin.CheckSettingType(k, schema, v); err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		// 剔除默认值(等值于 default 不落层)
		if dv, ok := schema["default"]; ok && settingValueEqual(dv, v) {
			continue
		}
		cfgOut[k] = v
	}
	tasksOut := map[string]map[string]any{}
	declared := map[string]bool{}
	if h := pkg.Manifest.Parts.Hooks; h != nil {
		for _, tk := range h.Tasks {
			declared[tk.Name] = true
		}
	}
	for tn, ov := range req.Tasks {
		if !declared[tn] {
			continue // 未声明任务剥离
		}
		clean := map[string]any{}
		if c, ok := ov["cron"].(string); ok && c != "" {
			if _, err := plugin.CronSchedule(c); err != nil {
				httpError(w, http.StatusBadRequest, "task "+tn+": "+err.Error())
				return
			}
			clean["cron"] = c
		}
		if dis, ok := ov["disabled"].(bool); ok && dis {
			clean["disabled"] = true
		}
		if len(clean) > 0 {
			tasksOut[tn] = clean
		}
	}
	view, err := d.Packages.Settings().Put(name, plugin.PutInput{Config: cfgOut, Tasks: tasksOut})
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 插件参数烘焙进部件闭包:改值后失效引用该包的模型行缓存(Upstream 未装配时无行可失效)
	if d.Upstream != nil {
		d.Upstream.EvictPackageSettings(name)
	}
	writeJSON(w, map[string]any{"ok": true, "version": view.Version})
}

// 类型校验统一下沉 plugin.CheckSettingType(v2 参数解析唯一入口)

// settingValueEqual JSON 语义等值(数值跨 int/float 形态)
func settingValueEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func (d *Deps) enablePackage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// 空体视为启用
		req.Enabled = true
	}
	if !req.Enabled {
		// 被引用禁用 = 拒绝并提示引用列表(运行期兜底:Pick 报 package disabled)
		var refs []string
		for _, u := range d.Upstream.List() {
			if u.Plugin == name {
				refs = append(refs, u.Name)
			}
		}
		if len(refs) > 0 {
			httpError(w, http.StatusConflict, "package referenced by: "+strings.Join(refs, ","))
			return
		}
	}
	if err := d.Packages.Enable(r.Context(), name, req.Enabled); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 启停影响引用行的路由可用性;部件缓存无需失效(包 revision 未变,路由层 IsEnabled 实时判定)
	writeJSON(w, map[string]any{"ok": true})
}

// packageKeys 包级 keys 视图(明文;按包名隔离,属包私有数据;响应并 hooks 能力标记)
func (d *Deps) packageKeys(w http.ResponseWriter, r *http.Request) {
	if d.KeysFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys unavailable")
		return
	}
	name := r.PathValue("name")
	out := d.KeysFunc(name)
	hooks := map[string]any{"keyWrite": false, "keyRead": false, "keyForm": false}
	if d.KeyHooksFunc != nil {
		write, read, form := d.KeyHooksFunc(name)
		hooks["keyWrite"], hooks["keyRead"], hooks["keyForm"] = write, read, form
	}
	out["hooks"] = hooks
	writeJSON(w, out)
}

// validKeyName 键名约束:非空,≤64 字符,无控制字符,不含 /(单路径段寻址)
func validKeyName(name string) bool {
	if name == "" || len(name) > 64 || strings.Contains(name, "/") {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// putPackageKey 单键写入(HW 面:键名校验 400/未启用 409/过期 409/钩子错 400/超时 500)
func (d *Deps) putPackageKey(w http.ResponseWriter, r *http.Request) {
	if d.KeyWriteFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys write unavailable")
		return
	}
	pkg := r.PathValue("name")
	key := r.PathValue("key")
	if !validKeyName(key) {
		httpError(w, http.StatusBadRequest, "invalid key name")
		return
	}
	var req struct {
		Value         any   `json:"value"`
		BaseUpdatedAt int64 `json:"baseUpdatedAt"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024*1024)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	updated, transformed, err := d.KeyWriteFunc(pkg, key, req.Value, req.BaseUpdatedAt)
	if err != nil {
		writeKeyErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"updatedAt": updated, "transformed": transformed})
}

// packageKeyDetail 键详情(keyRead 解释;501 = 未实现)
func (d *Deps) packageKeyDetail(w http.ResponseWriter, r *http.Request) {
	if d.KeyReadFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys read unavailable")
		return
	}
	pkg, key := r.PathValue("name"), r.PathValue("key")
	if !validKeyName(key) {
		httpError(w, http.StatusBadRequest, "invalid key name")
		return
	}
	detail, err := d.KeyReadFunc(pkg, key)
	if err != nil {
		if errors.Is(err, plugin.ErrPackageDisabled) {
			httpError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, plugin.ErrNoHooksPart) || errors.Is(err, plugin.ErrHookNotExported) {
			httpError(w, http.StatusNotImplemented, "keyRead not implemented")
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"detail": detail})
}

// packageKeyForm 添加表单声明
func (d *Deps) packageKeyForm(w http.ResponseWriter, r *http.Request) {
	if d.KeyFormFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys form unavailable")
		return
	}
	decl, err := d.KeyFormFunc(r.PathValue("name"))
	if err != nil {
		if errors.Is(err, plugin.ErrPackageDisabled) {
			httpError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, plugin.ErrNoHooksPart) || errors.Is(err, plugin.ErrHookNotExported) {
			httpError(w, http.StatusNotImplemented, "keyForm not implemented")
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	form, ok := decl.(map[string]any)
	if !ok || form == nil {
		httpError(w, http.StatusNotImplemented, "keyForm not implemented")
		return
	}
	writeJSON(w, map[string]any{"fields": form["fields"], "actions": form["actions"]})
}

// packageKeyFormAction 表单按钮回调
func (d *Deps) packageKeyFormAction(w http.ResponseWriter, r *http.Request) {
	if d.KeyActionFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys form unavailable")
		return
	}
	var req struct {
		Action string         `json:"action"`
		Values map[string]any `json:"values"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&req); err != nil || req.Action == "" {
		httpError(w, http.StatusBadRequest, "action required")
		return
	}
	msg, err := d.KeyActionFunc(r.PathValue("name"), req.Action, req.Values)
	if err != nil {
		writeKeyErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"message": msg})
}

// packageKeyFormSubmit 表单提交(写入在钩子内完成;written 收集)
func (d *Deps) packageKeyFormSubmit(w http.ResponseWriter, r *http.Request) {
	if d.KeySubmitFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys form unavailable")
		return
	}
	var req struct {
		Values map[string]any `json:"values"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	written, msg, errs, err := d.KeySubmitFunc(r.PathValue("name"), req.Values)
	if err != nil {
		if errors.Is(err, plugin.ErrNoHooksPart) || errors.Is(err, plugin.ErrHookNotExported) {
			httpError(w, http.StatusNotImplemented, "keySubmit not implemented")
			return
		}
		writeKeyErr(w, err)
		return
	}
	if written == nil {
		written = []string{}
	}
	out := map[string]any{"ok": true, "written": written, "message": msg}
	if len(errs) > 0 {
		// 字段级拒绝:处理成功(200)但提交未通过,GUI 据此定位输入框
		out["ok"] = false
		out["errors"] = errs
	}
	writeJSON(w, out)
}

// writeKeyErr 错误映射:禁用 409/冲突 409/无部件 501/其余(钩子抛错/超限/超时)400|500 判别
func writeKeyErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, plugin.ErrPackageDisabled):
		httpError(w, http.StatusConflict, err.Error())
	case errors.Is(err, plugin.ErrVersionConflict):
		httpError(w, http.StatusConflict, "keys modified (by task or upgrade); reload")
	case errors.Is(err, plugin.ErrNoHooksPart):
		httpError(w, http.StatusNotImplemented, "hooks not implemented")
	case errors.Is(err, plugin.ErrHookTimeoutSentinel):
		httpError(w, http.StatusInternalServerError, err.Error())
	case strings.Contains(err.Error(), "exceed limit"):
		httpError(w, http.StatusBadRequest, err.Error())
	default:
		httpError(w, http.StatusBadRequest, err.Error())
	}
}

func (d *Deps) listUpstreams(w http.ResponseWriter, r *http.Request) {
	// 模型行列表(v2:无 secrets 概念;行 params 与包 keys 分层)
	writeJSON(w, d.Upstream.List())
}

func (d *Deps) getUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	u, err := d.Upstream.Get(id)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, u)
}

func (d *Deps) saveUpstream(w http.ResponseWriter, r *http.Request) {
	d.saveUpstreamImpl(w, r, 0)
}

func (d *Deps) saveUpstreamByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	d.saveUpstreamImpl(w, r, id)
}

func (d *Deps) saveUpstreamImpl(w http.ResponseWriter, r *http.Request, id int64) {
	u := &upstream.Model{ID: id}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		httpError(w, http.StatusBadRequest, "bad json")
		return
	}
	// (name, plugin) 是存储唯一键:改键 = 删除重建,by-id 保存拒绝改键(防路由键漂移)
	if id != 0 {
		old, err := d.Upstream.Get(id)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		if old.Name != u.Name || old.Plugin != u.Plugin {
			httpError(w, http.StatusBadRequest, "name/plugin 不可修改:删除后重建")
			return
		}
	}
	if err := d.Upstream.Save(r.Context(), u); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "id": u.ID})
}

func (d *Deps) deleteUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	if err := d.Upstream.Delete(r.Context(), id); err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (d *Deps) testUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	if d.TestFunc == nil {
		httpError(w, http.StatusNotImplemented, "test not configured")
		return
	}
	latency, errMsg := d.TestFunc(id)
	out := map[string]any{"ok": errMsg == "", "latency_ms": latency}
	if errMsg != "" {
		out["error"] = errMsg
	}
	writeJSON(w, out)
}

// chatTestUpstream 对话测试:行名即模型名,消息缺省 "ping";错误路径 501 未装配/404 id 不存在/400 参数
func (d *Deps) chatTestUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	if d.ChatTestFunc == nil {
		httpError(w, http.StatusNotImplemented, "chat test not configured")
		return
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil && err != io.EOF {
		httpError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.Message == "" {
		req.Message = "ping"
	}
	latency, status, body, errMsg := d.ChatTestFunc(r.Context(), id, req.Message)
	// 装配层 reg.Get 失败即 id 不存在 → 404(与其余模型路由的 404 语义一致)
	if strings.Contains(errMsg, "not found") {
		httpError(w, http.StatusNotFound, errMsg)
		return
	}
	out := map[string]any{"ok": errMsg == "", "latency_ms": latency}
	if status != 0 {
		out["status"] = status
	}
	if errMsg != "" {
		out["error"] = errMsg
	}
	// 非 JSON 响应体(上游错误页/纯文本/流式拼接)降级为字符串,避免整个响应序列化失败成空体
	if len(body) > 0 {
		if json.Valid(body) {
			out["body"] = json.RawMessage(body)
		} else {
			out["body"] = string(body)
		}
	}
	writeJSON(w, out)
}

func (d *Deps) metricsLive(w http.ResponseWriter, r *http.Request) {
	reqs, errs, conc, byUp, byTarget, byPkg := d.Metrics.SnapshotLive()
	writeJSON(w, map[string]any{
		"active_concurrent": conc, "total_requests": reqs, "total_errors": errs,
		"by_upstream": byUp, "by_target": byTarget, "by_package": byPkg,
	})
}

// metricsSeries 最近 minutes 个已落库分钟点(老到新)
func (d *Deps) metricsSeries(w http.ResponseWriter, r *http.Request) {
	minutes := 60
	if v := r.URL.Query().Get("minutes"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 24*60 {
			minutes = n
		}
	}
	rows, err := d.SeriesFunc(minutes)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, rows)
}

// listHistory 请求历史分页查询(?model=&upstream=&error=1&start=&end=&limit=&offset=;非法时间参数忽略)
func (d *Deps) listHistory(w http.ResponseWriter, r *http.Request) {
	if d.History == nil {
		httpError(w, http.StatusNotImplemented, "history not configured")
		return
	}
	q := r.URL.Query()
	f := history.Filter{
		Model:    q.Get("model"),
		Upstream: q.Get("upstream"),
		Error:    q.Get("error") == "1",
		Start:    parseMS(q.Get("start")),
		End:      parseMS(q.Get("end")),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Offset = n
		}
	}
	rows, total, err := d.History.Query(r.Context(), f)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"items": rows, "total": total})
}

// getHistory 单条详情(含 bodies;不存在 404)
func (d *Deps) getHistory(w http.ResponseWriter, r *http.Request) {
	if d.History == nil {
		httpError(w, http.StatusNotImplemented, "history not configured")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	e, err := d.History.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, history.ErrNotFound) {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, e)
}

// parseMS 解析 epoch 毫秒查询参数(非数字或非正按未传)
func parseMS(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// listTransports 传输实例清单(只读)
func (d *Deps) listTransports(w http.ResponseWriter, r *http.Request) {
	if d.TransportsFunc == nil {
		writeJSON(w, []any{})
		return
	}
	writeJSON(w, d.TransportsFunc())
}

// testTransport 单传输实例连通测试
func (d *Deps) testTransport(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if d.TransportTestFunc == nil {
		httpError(w, http.StatusNotImplemented, "transport test not configured")
		return
	}
	latency, errMsg := d.TransportTestFunc(name)
	out := map[string]any{"ok": errMsg == "", "latency_ms": latency}
	if errMsg != "" {
		out["error"] = errMsg
	}
	writeJSON(w, out)
}

// ErrBadScope evict scope/value 非法(调用方返回此值映射 400)
var ErrBadScope = errors.New("scope/value 非法")

// evictTransport 失效命令:body {scope,value};非法 400,节点失败 502
func (d *Deps) evictTransport(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if d.TransportEvictFunc == nil {
		httpError(w, http.StatusNotImplemented, "transport evict not configured")
		return
	}
	var body struct {
		Scope string `json:"scope"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, "decode body: "+err.Error())
		return
	}
	if err := d.TransportEvictFunc(r.Context(), name, body.Scope, body.Value); err != nil {
		if errors.Is(err, ErrBadScope) {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// secretMask 凭据回读掩码
const secretMask = "***"

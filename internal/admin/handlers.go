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
	"strconv"
	"strings"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/metrics"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// Deps 管理 API 业务依赖
type Deps struct {
	Packages *plugin.Registry
	Upstream *upstream.Registry
	Metrics  *metrics.Recorder
	Secrets  upstream.SecretsStore                                   // "***" 回读合并的旧值来源(kv 唯一存储)
	TestFunc func(upstreamID int64) (latencyMS int64, errMsg string) // 连通性测试(走完整管道)
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
}

// fetch 拉取实现取依赖覆写,缺省内置实现
func (d *Deps) fetch(r *http.Request, url string) ([]byte, error) {
	if d.FetchPackage != nil {
		return d.FetchPackage(r, url)
	}
	return fetchPackage(r, url)
}
func (d *Deps) fetchPackageFunc() func(r *http.Request, url string) ([]byte, error) {
	if d.FetchPackage != nil {
		return d.FetchPackage
	}
	return fetchPackage
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
	mux.HandleFunc("PUT /admin/api/packages/{name}/code", d.updateCode)
	mux.HandleFunc("GET /admin/api/packages/{name}/code", d.getPartCode)
	mux.HandleFunc("GET /admin/api/upstreams", d.listUpstreams)
	mux.HandleFunc("POST /admin/api/upstreams", d.saveUpstream)
	mux.HandleFunc("GET /admin/api/upstreams/{id}", d.getUpstream)
	mux.HandleFunc("PUT /admin/api/upstreams/{id}", d.saveUpstreamByID)
	mux.HandleFunc("DELETE /admin/api/upstreams/{id}", d.deleteUpstream)
	mux.HandleFunc("POST /admin/api/upstreams/{id}/test", d.testUpstream)
	mux.HandleFunc("GET /admin/api/metrics/live", d.metricsLive)
	mux.HandleFunc("GET /admin/api/metrics/series", d.metricsSeries)
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
			"secretRefs": p.SecretRefsUnion(),
			"protocol":   protocolName(p),
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
		if dialIP == nil {
			dialIP = ip
		}
	}
	d := net.Dialer{}
	return d.DialContext(ctx, network, net.JoinHostPort(dialIP.String(), port))
}

// assertPublicHost 主机名解析结果须全为公网地址(SSRF 预检;拨号级校验为权威防线)
func assertPublicHost(host string) error {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve host: %w", err)
	}
	if len(addrs) == 0 {
		return errors.New("resolve host: no addresses")
	}
	for _, ip := range addrs {
		if forbiddenIP(ip) {
			return fmt.Errorf("address %s is not allowed", ip)
		}
	}
	return nil
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

// deletePackage 卸载包(被上游引用拒绝;内置包拒绝)
func (d *Deps) deletePackage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var refs []string
	for _, u := range d.Upstream.List() {
		if u.Base.Package == name {
			refs = append(refs, u.Name+"(base)")
			continue
		}
		for _, ex := range u.Extras {
			if ex.Package == name {
				refs = append(refs, u.Name+"(extra)")
			}
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

// PackageTemplate 最小骨架包下载(protocol+filter 各一,开发者起步模板;静态无敏感信息,免会话)
func (d *Deps) PackageTemplate(w http.ResponseWriter, r *http.Request) {
	tpl := &plugin.Manifest{
		ManifestVersion: plugin.ManifestVersion,
		Name:            "my-package",
		Version:         "0.1.0",
	}
	tpl.Parts.Protocol = &plugin.ProtocolPart{
		Entry: "protocol.js", Protocol: string(plugin.ProtocolOpenAICompletions),
		Form:     []string{string(plugin.FormStreaming), string(plugin.FormNonStreaming)},
		Features: []string{"tools"}, SecretRefs: []string{"api_key"},
	}
	tpl.Parts.Filters = []plugin.FilterPart{{Name: "log-request", Entry: "filter.js"}}
	files := map[string][]byte{
		"protocol.js": []byte(`// openai-completions 起步模板:按需改造
module.exports = {
  buildRequest: function (ctx, entry) {
    return { url: ctx.target.baseUrl + "/v1/chat/completions", method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + util.secret("api_key") },
      body: entry, stream: ctx.vars.entryStream };
  },
  mapEvent: function (ctx, e) {
    var f = JSON.parse(e);
    if (f.data === "[DONE]") return null;
    return JSON.stringify([JSON.parse(f.data)]);
  },
  mapResponse: function (ctx, body) { return body; }
};`),
		"filter.js": []byte(`// filter 起步模板:透传
module.exports = {
  mapRequest: function (ctx, p) { return p; },
  mapChunk: function (ctx, c) { return c; },
  mapResponse: function (ctx, r) { return r; }
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
		// 被引用禁用 = 拒绝并提示引用列表
		var refs []string
		for _, u := range d.Upstream.List() {
			if u.Base.Package == name {
				refs = append(refs, u.Name+"(base)")
				continue
			}
			for _, ex := range u.Extras {
				if ex.Package == name {
					refs = append(refs, u.Name+"(extra)")
				}
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
	writeJSON(w, map[string]any{"ok": true})
}

func (d *Deps) updateCode(w http.ResponseWriter, r *http.Request) {
	// 在线编辑:?kind=protocol 或 ?kind=filter&name=<filter 名>;body=部件代码
	// 语义:替换部件源码 → 整包升级(revision+1)→ 实例缓存经 revision 失效
	name := r.PathValue("name")
	kind := r.URL.Query().Get("kind")
	partName := r.URL.Query().Get("name")
	if kind != "protocol" && kind != "filter" {
		httpError(w, http.StatusBadRequest, "kind must be protocol|filter")
		return
	}
	if kind == "filter" && partName == "" {
		httpError(w, http.StatusBadRequest, "filter name required")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		httpError(w, http.StatusBadRequest, "read body")
		return
	}
	revision, err := d.Packages.UpdatePart(r.Context(), name, kind, partName, body)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "revision": revision})
}

// getPartCode 读部件源码(?kind=protocol|filter&name=<filter 名>;text/plain)
func (d *Deps) getPartCode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	kind := r.URL.Query().Get("kind")
	partName := r.URL.Query().Get("name")
	src, err := d.Packages.GetPart(name, kind, partName)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(src)
}

// packageKeys 包级 keys 视图(明文;按包名隔离,属包私有数据)
func (d *Deps) packageKeys(w http.ResponseWriter, r *http.Request) {
	if d.KeysFunc == nil {
		httpError(w, http.StatusNotImplemented, "keys unavailable")
		return
	}
	writeJSON(w, d.KeysFunc(r.PathValue("name")))
}

func (d *Deps) listUpstreams(w http.ResponseWriter, r *http.Request) {
	// List 返回深拷贝且 Secrets 剥离(输出即脱敏)
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
	u := &upstream.Upstream{ID: id}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		httpError(w, http.StatusBadRequest, "bad json")
		return
	}
	// "***" 回读值从 kv 唯一存储合并旧凭据(实例快照不持明文)
	for i, t := range u.Targets {
		for k, v := range t.Secrets {
			if v != secretMask {
				continue
			}
			if old, ok := d.Secrets.GetTargetSecrets(u.Name, t.Name); ok {
				if val, has := old[k]; has {
					u.Targets[i].Secrets[k] = val
					continue
				}
			}
			delete(u.Targets[i].Secrets, k)
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

func (d *Deps) metricsLive(w http.ResponseWriter, r *http.Request) {
	reqs, errs, conc, byUp, byTarget := d.Metrics.SnapshotLive()
	writeJSON(w, map[string]any{
		"active_concurrent": conc, "total_requests": reqs, "total_errors": errs,
		"by_upstream": byUp, "by_target": byTarget,
	})
}

// metricsSeries 最近 minutes 个分钟点(含当前未落库分钟;老到新)
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

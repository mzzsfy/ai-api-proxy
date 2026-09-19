package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ai-api-proxy/internal/metrics"
	"ai-api-proxy/internal/plugin"
	"ai-api-proxy/internal/upstream"
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
	// TransportsFunc 命名传输实例清单(只读;名称+类型+URL)
	TransportsFunc func() []map[string]any
	// TransportTestFunc 单传输实例连通测试(发一次真实 HEAD;返回延迟与错误)
	TransportTestFunc func(name string) (int64, string)
}

// Mux 构建管理 API 路由(挂在 /admin/api 前缀,已过会话中间件)
func (d *Deps) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/api/me", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, map[string]any{"ok": true}) })
	mux.HandleFunc("GET /admin/api/packages", d.listPackages)
	mux.HandleFunc("POST /admin/api/packages", d.installPackage)
	mux.HandleFunc("POST /admin/api/packages/import-url", d.installPackageFromURL)
	mux.HandleFunc("GET /admin/api/packages/{name}/export", d.exportPackage)
	mux.HandleFunc("DELETE /admin/api/packages/{name}", d.deletePackage)
	mux.HandleFunc("POST /admin/api/packages/{name}/enable", d.enablePackage)
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
		})
	}
	writeJSON(w, out)
}

func (d *Deps) installPackage(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(io.LimitReader(r.Body, 8*1024*1024))
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
// 管理面已过会话鉴权;仅 http(s),限 8MB
func (d *Deps) installPackageFromURL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil || req.URL == "" {
		httpError(w, http.StatusBadRequest, "url required")
		return
	}
	if !strings.HasPrefix(req.URL, "http://") && !strings.HasPrefix(req.URL, "https://") {
		httpError(w, http.StatusBadRequest, "only http(s) url allowed")
		return
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(req.URL)
	if err != nil {
		httpError(w, http.StatusBadGateway, "fetch: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		httpError(w, http.StatusBadGateway, fmt.Sprintf("fetch status %d", resp.StatusCode))
		return
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		httpError(w, http.StatusBadGateway, "read body")
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
		Entry: "protocol.js", Form: []string{"streaming", "non_streaming"},
		Features: []string{"tools"}, SecretRefs: []string{"api_key"},
	}
	tpl.Parts.Filters = []plugin.FilterPart{{Name: "log-request", Entry: "filter.js"}}
	files := map[string][]byte{
		"protocol.js": []byte(`// openai-compatible 起步模板:按需改造
module.exports = {
  buildRequest: function (ctx, pivot) {
    return { url: ctx.target.baseUrl + "/v1/chat/completions", method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + util.secret("api_key") },
      body: pivot, stream: ctx.vars.entryStream };
  },
  mapEvent: function (ctx, e) { var f = JSON.parse(e); return f.data === "[DONE]" ? null : f.data; },
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

// secretMask 凭据回读掩码
const secretMask = "***"

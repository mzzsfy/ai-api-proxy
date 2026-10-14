// creds 装配:login/inspect 凭据通道(aap 桥后端;per-pkg ③ 权威槽注册表)
package server

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

var loginSeq atomic.Uint64

// loginID 宿主生成账号 id(acct-<unixmilli hex>-<seq>;login 返回值含非空 string id 时用之)
func loginID(nowMs int64) string {
	return fmt.Sprintf("acct-%x-%d", nowMs, loginSeq.Add(1))
}

// credsFor per-pkg ③ 权威槽(懒建;桥与 handler 引擎共用同实例)
func (a *App) credsFor(pkg string) (*plugin.SlotStore, error) {
	if _, err := a.AdminDeps.Packages.GetPackage(pkg); err != nil {
		return nil, err
	}
	return a.sharedStoreFor(pkg), nil
}

// slotView 包命名空间 ③ 权威槽视图(打标直写)
func (a *App) slotView(pkg string) plugin.StorageKV {
	return plugin.AuthoritativeStore{S: a.sharedStoreFor(pkg)}
}

// sharedStoreFor 命名 ns 槽实例(懒建;桥/handler 引擎/任务/login 全通道共用)
func (a *App) sharedStoreFor(ns string) *plugin.SlotStore {
	a.credsMu.Lock()
	defer a.credsMu.Unlock()
	if a.credsShared == nil {
		a.credsShared = newHandlerShared(a.St)
	}
	return a.credsShared.storeFor(ns)
}

// Login login/index.js → Keys.Set(GUI 登录路径唯一落库通道)
func (a *App) Login(pkg string, input any) (map[string]any, error) {
	p, err := a.AdminDeps.Packages.GetPackage(pkg)
	if err != nil {
		return nil, err
	}
	if !a.AdminDeps.Packages.IsEnabled(pkg) {
		return nil, fmt.Errorf("package %s disabled", pkg)
	}
	if _, ok := p.Files[plugin.LoginEntry]; !ok {
		return nil, fmt.Errorf("package %s has no login", pkg)
	}
	deps := a.hooksDeps(pkg)
	s, err := a.credsFor(pkg)
	if err != nil {
		return nil, err
	}
	deps.Storage = plugin.AuthoritativeStore{S: s} // login storage = ③直写打权威标(失败不降级)
	rt, err := plugin.LoadLogin(p, deps, input)
	if err != nil {
		return nil, err
	}
	data, err := rt.RunLogin()
	if err != nil {
		return nil, err
	}
	id := ""
	if m, ok := data.(map[string]any); ok {
		if s, ok := m["id"].(string); ok && s != "" {
			id = s
			delete(m, "id")
		}
	}
	if id == "" {
		id = loginID(time.Now().UnixMilli())
	}
	if _, err := a.AdminDeps.Packages.Keys().Set(pkg, id, data); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "data": data}, nil
}

// Accounts 账号清单(inspect 变换;单条变换失败 = 回退原 data,不整页失败)
func (a *App) Accounts(pkg string) ([]map[string]any, error) {
	p, err := a.AdminDeps.Packages.GetPackage(pkg)
	if err != nil {
		return nil, err
	}
	inspect, err := plugin.LoadInspect(p)
	if err != nil {
		return nil, err
	}
	entries := a.AdminDeps.Packages.Keys().List(pkg)
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		data := any(e.Data)
		if inspect != nil {
			if v, err := inspect(e.Data); err == nil {
				data = v
			} else {
				// 变换失败回退原 data(BDD 契约);静默吞错无法排障,必须留痕
				slog.Warn("inspect transform failed, fallback to raw", "pkg", pkg, "account", e.ID, "err", err)
			}
		}
		out = append(out, map[string]any{"id": e.ID, "data": data, "updatedAt": e.UpdatedAt})
	}
	return out, nil
}

// Account 单账号(inspect 变换后)
func (a *App) Account(pkg, id string) (map[string]any, error) {
	e, ok := a.AdminDeps.Packages.Keys().Get(pkg, id)
	if !ok {
		return nil, fmt.Errorf("account %q not found", id)
	}
	p, err := a.AdminDeps.Packages.GetPackage(pkg)
	if err != nil {
		return nil, err
	}
	inspect, err := plugin.LoadInspect(p)
	if err != nil {
		return nil, err
	}
	data := any(e.Data)
	if inspect != nil {
		if v, err := inspect(e.Data); err == nil {
			data = v
		} else {
			// 变换失败回退原 data(BDD 契约);静默吞错无法排障,必须留痕
			slog.Warn("inspect transform failed, fallback to raw", "pkg", pkg, "account", e.ID, "err", err)
		}
	}
	return map[string]any{"id": e.ID, "data": data, "updatedAt": e.UpdatedAt}, nil
}

// StorageGet 包级 storage 桥读(③)
func (a *App) StorageGet(pkg, key string) (any, error) {
	s, err := a.credsFor(pkg)
	if err != nil {
		return nil, err
	}
	v, _, err := s.GetAuth(key)
	if err != nil {
		return nil, err
	}
	if v == "" {
		return nil, nil
	}
	return v, nil
}

// StorageSet 包级 storage 桥写(③直写打权威标)
func (a *App) StorageSet(pkg, key, value string) error {
	if len(value) > plugin.MaxStorageValue {
		return fmt.Errorf("storage value exceeds limit")
	}
	s, err := a.credsFor(pkg)
	if err != nil {
		return err
	}
	return s.ExternalWrite(key, value)
}

// HTTPRequest 页面出站(经代理包装;transport/sessionKey 契约同任务)
func (a *App) HTTPRequest(pkg string, opts map[string]any) (map[string]any, error) {
	if a.trMgr == nil {
		return nil, fmt.Errorf("no outbound transport available")
	}
	url, _ := opts["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("http: url required")
	}
	method, _ := opts["method"].(string)
	if method == "" {
		method = "GET"
	}
	headers := map[string]string{}
	if hm, ok := opts["headers"].(map[string]any); ok {
		for k, v := range hm {
			if sv, ok := v.(string); ok {
				headers[k] = sv
			}
		}
	}
	body, _ := opts["body"].(string)
	transportName, _ := opts["transport"].(string)
	sessionKey, _ := opts["sessionKey"].(string)
	timeout := 30 * time.Second
	if tms, ok := numericVal(opts["timeoutMs"]); ok && tms > 0 {
		timeout = time.Duration(tms) * time.Millisecond
	}
	resp, err := (&httpExecutor{trMgr: a.trMgr}).Do(plugin.HttpRequest{URL: url, Method: method, Headers: headers,
		Body: body, Timeout: timeout, Transport: transportName, SessionKey: sessionKey})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": resp.Status, "headers": resp.Headers, "body": resp.Body}, nil
}

// Meta 页面自适应({name, version, revision, params};params = settings 声明键面)
func (a *App) Meta(pkg string) (map[string]any, error) {
	p, err := a.AdminDeps.Packages.GetPackage(pkg)
	if err != nil {
		return nil, err
	}
	params := map[string]any{}
	for k, v := range p.Declaration {
		params[k] = v
	}
	return map[string]any{
		"name":     p.Manifest.Name,
		"version":  p.Manifest.Version,
		"revision": p.Revision,
		"params":   params,
	}, nil
}

// numericVal 数值字段提取
func numericVal(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

var _ admin.CredsEngine = (interface {
	Login(pkg string, input any) (map[string]any, error)
	Accounts(pkg string) ([]map[string]any, error)
	Account(pkg, id string) (map[string]any, error)
	StorageGet(pkg, key string) (any, error)
	StorageSet(pkg, key, value string) error
	HTTPRequest(pkg string, opts map[string]any) (map[string]any, error)
	Meta(pkg string) (map[string]any, error)
})((*App)(nil))

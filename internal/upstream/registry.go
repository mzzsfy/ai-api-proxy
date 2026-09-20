// Package upstream 上游实例(包实例化)/校验/Pick/Resolve
package upstream

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// Feature 能力协商(与 convert.Feature 字符串语义一致,避免包依赖)
type Feature = string

// PackageRef 浮动跟随包 latest revision
type PackageRef struct {
	Package string `json:"package"`
}

// Target 实例目标
type Target struct {
	Name      string            `json:"name"`
	BaseURL   string            `json:"base_url"`
	Transport string            `json:"transport"`
	Secrets   map[string]string `json:"secrets"`
	Weight    int               `json:"weight,omitempty"`
	Enabled   bool              `json:"enabled"`
}

// Strategy 目标遍历策略
type Strategy struct {
	Mode    string   `json:"mode"`
	RetryOn []string `json:"retry_on"`
}

// Upstream 上游实例
type Upstream struct {
	ID             int64                     `json:"id"`
	Name           string                    `json:"name"`
	Base           PackageRef                `json:"base"`
	Extras         []PackageRef              `json:"extras"`
	Models         []string                  `json:"models"`
	Targets        []Target                  `json:"targets"`
	Params         map[string]any            `json:"params"`
	FilterParams   map[string]map[string]any `json:"filter_params"`
	FiltersEnabled map[string]bool           `json:"filters_enabled"`
	Strategy       Strategy                  `json:"strategy"`
	Enabled        bool                      `json:"enabled"`
}

// Candidate Pick 候选
type Candidate struct {
	Upstream *Upstream
}

// PickError 失败原因分类
type PickError int

// Pick 分类常量
const (
	PickNoModel PickError = iota
	PickCapability
	PickUnhealthy
)

func (e PickError) Error() string {
	switch e {
	case PickNoModel:
		return "no upstream declares model"
	case PickCapability:
		return "capability mismatch"
	default:
		return "all targets unhealthy"
	}
}

// Registry 上游注册中心
type Registry struct {
	mu      sync.RWMutex
	db      *sql.DB
	pkgs    *plugin.Registry
	secrets SecretsStore
	byID    map[int64]*Upstream
	// instCache 部件实例缓存(键=上游名;包 revision 变化或 Save 时失效)
	instCache map[string]*resolvedParts
}

// resolvedParts 可缓存的部件实例(JS runtime 池实例化成本高)
// refs=持有计数(缓存 1 + 每在途请求 1);evict 后归零销毁池(Reload 在途保护)
type resolvedParts struct {
	baseRev     int64
	extraRev    []int64
	fingerprint string
	Filters     []pipeline.Filter
	Protocol    pipeline.Protocol
	// Declared 主包协议全名(路由把关用;实例缓存命中时免查包注册表)
	Declared string
	refs     atomic.Int64
	dead     atomic.Bool
}

// acquire 请求持有(死亡后拒绝)
func (c *resolvedParts) acquire() bool {
	for {
		if c.dead.Load() {
			return false
		}
		n := c.refs.Load()
		if c.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// release 释放一份持有;死亡后归零 → 销毁池
func (c *resolvedParts) release() {
	if c.refs.Add(-1) == 0 && c.dead.Load() {
		c.closePools()
	}
}

// evict 缓存摘除(释放缓存持有份)
func (c *resolvedParts) evict() {
	c.dead.Store(true)
	c.release()
}

// closePools 关闭协议与全部 filter 池
func (c *resolvedParts) closePools() {
	if p, ok := c.Protocol.(interface{ ClosePools() }); ok {
		p.ClosePools()
	}
	for _, f := range c.Filters {
		if p, ok := f.(interface{ ClosePools() }); ok {
			p.ClosePools()
		}
	}
}

// SecretsStore 目标凭据存储(唯一存储=targets.secrets)
type SecretsStore interface {
	UpsertTargetSecrets(upstream, target string, secrets map[string]string) error
	GetTargetSecrets(upstream, target string) (map[string]string, bool)
	DeleteTargetSecrets(upstream, target string)
}

// NewRegistry 构造
func NewRegistry(db *sql.DB, pkgs *plugin.Registry, secrets SecretsStore) *Registry {
	return &Registry{db: db, pkgs: pkgs, secrets: secrets, byID: map[int64]*Upstream{}, instCache: map[string]*resolvedParts{}}
}

// LoadFromDB 启动恢复
func (r *Registry) LoadFromDB(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx, `SELECT id, name, base_package, extras_json, models_json, targets_json,
		params_json, filter_params_json, filters_enabled_json, strategy_json, enabled FROM upstreams`)
	if err != nil {
		return fmt.Errorf("query upstreams: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		u := &Upstream{}
		var extrasRaw, modelsRaw, targetsRaw, paramsRaw, filterParamsRaw, filtersEnabledRaw, strategyRaw string
		if err := rows.Scan(&u.ID, &u.Name, &u.Base.Package, &extrasRaw, &modelsRaw, &targetsRaw,
			&paramsRaw, &filterParamsRaw, &filtersEnabledRaw, &strategyRaw, &u.Enabled); err != nil {
			return fmt.Errorf("scan upstream: %w", err)
		}
		_ = json.Unmarshal([]byte(extrasRaw), &u.Extras)
		_ = json.Unmarshal([]byte(modelsRaw), &u.Models)
		_ = json.Unmarshal([]byte(targetsRaw), &u.Targets)
		_ = json.Unmarshal([]byte(paramsRaw), &u.Params)
		_ = json.Unmarshal([]byte(filterParamsRaw), &u.FilterParams)
		_ = json.Unmarshal([]byte(filtersEnabledRaw), &u.FiltersEnabled)
		_ = json.Unmarshal([]byte(strategyRaw), &u.Strategy)
		r.byID[u.ID] = u
	}
	return rows.Err()
}

// Validate 保存期校验
func (r *Registry) Validate(u *Upstream) error {
	if u.Name == "" {
		return fmt.Errorf("name required")
	}
	if len(u.Models) == 0 {
		return fmt.Errorf("models required")
	}
	if len(u.Targets) == 0 {
		return fmt.Errorf("targets required")
	}
	base, err := r.pkgs.GetPackage(u.Base.Package)
	if err != nil {
		return fmt.Errorf("base package: %w", err)
	}
	if !base.HasProtocol() {
		return fmt.Errorf("base package %q has no protocol part", u.Base.Package)
	}
	for _, ex := range u.Extras {
		if _, err := r.pkgs.GetPackage(ex.Package); err != nil {
			return fmt.Errorf("extra package: %w", err)
		}
	}
	// secrets 键覆盖 secretRefs 并集(全部 enabled 目标)
	need := base.SecretRefsUnion()
	for _, ex := range u.Extras {
		p, _ := r.pkgs.GetPackage(ex.Package)
		need = append(need, p.SecretRefsUnion()...)
	}
	for _, t := range u.Targets {
		if !t.Enabled {
			continue
		}
		missing := missingKeys(t.Secrets, need)
		if len(missing) > 0 {
			return fmt.Errorf("target %s missing secret keys: %s", t.Name, strings.Join(missing, ","))
		}
	}
	return nil
}

// missingKeys 缺失键列表(去重保序)
func missingKeys(have map[string]string, need []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, k := range need {
		if seen[k] {
			continue
		}
		seen[k] = true
		if _, ok := have[k]; !ok {
			out = append(out, k)
		}
	}
	return out
}

// Save 保存(校验+持久化+热生效;凭据唯一存储=kv,targets_json 持脱敏副本)
func (r *Registry) Save(ctx context.Context, u *Upstream) error {
	if err := r.Validate(u); err != nil {
		return err
	}
	// 凭据先入唯一存储
	for _, t := range u.Targets {
		if err := r.secrets.UpsertTargetSecrets(u.Name, t.Name, t.Secrets); err != nil {
			return fmt.Errorf("store secrets: %w", err)
		}
	}
	// 被移除/改名目标的 kv 凭据级联清理(凭据最小化)
	r.mu.RLock()
	for _, old := range r.byID {
		if old.Name != u.Name {
			continue
		}
		for _, ot := range old.Targets {
			if !containsTarget(u.Targets, ot.Name) {
				r.secrets.DeleteTargetSecrets(u.Name, ot.Name)
			}
		}
	}
	r.mu.RUnlock()
	sanitized := *u
	sanitized.Targets = sanitizeTargets(u.Targets)
	extrasRaw, _ := json.Marshal(sanitized.Extras)
	modelsRaw, _ := json.Marshal(sanitized.Models)
	targetsRaw, _ := json.Marshal(sanitized.Targets)
	paramsRaw, _ := json.Marshal(sanitized.Params)
	filterParamsRaw, _ := json.Marshal(sanitized.FilterParams)
	filtersEnabledRaw, _ := json.Marshal(sanitized.FiltersEnabled)
	strategyRaw, _ := json.Marshal(sanitized.Strategy)
	res := r.db.QueryRowContext(ctx, `INSERT INTO upstreams(name, base_package, extras_json, models_json, targets_json,
		params_json, filter_params_json, filters_enabled_json, strategy_json, enabled)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET base_package=excluded.base_package, extras_json=excluded.extras_json,
		models_json=excluded.models_json, targets_json=excluded.targets_json, params_json=excluded.params_json,
		filter_params_json=excluded.filter_params_json, filters_enabled_json=excluded.filters_enabled_json,
		strategy_json=excluded.strategy_json, enabled=excluded.enabled, updated_at=datetime('now')
		RETURNING id`,
		sanitized.Name, sanitized.Base.Package, string(extrasRaw), string(modelsRaw), string(targetsRaw),
		string(paramsRaw), string(filterParamsRaw), string(filtersEnabledRaw), string(strategyRaw), sanitized.Enabled)
	if err := res.Scan(&sanitized.ID); err != nil {
		return fmt.Errorf("persist upstream: %w", err)
	}
	u.ID = sanitized.ID
	r.mu.Lock()
	r.byID[sanitized.ID] = &sanitized
	old := r.instCache[sanitized.Name]
	delete(r.instCache, sanitized.Name)
	r.mu.Unlock()
	if old != nil {
		go old.evict() // 旧配置池:在途归零后销毁
	}
	return nil
}

// sanitizeTargets 深拷贝并剥离凭据
func sanitizeTargets(targets []Target) []Target {
	out := make([]Target, len(targets))
	for i, t := range targets {
		t.Secrets = nil
		out[i] = t
	}
	return out
}

// containsTarget 目标名是否在列
func containsTarget(targets []Target, name string) bool {
	for _, t := range targets {
		if t.Name == name {
			return true
		}
	}
	return false
}

// copyUpstream 深拷贝(管理读取用,防共享指针污染)
func copyUpstream(u *Upstream) *Upstream {
	c := *u
	c.Models = append([]string(nil), u.Models...)
	c.Extras = append([]PackageRef(nil), u.Extras...)
	c.Targets = make([]Target, len(u.Targets))
	for i, t := range u.Targets {
		t.Secrets = nil // secrets 永不回读原文
		c.Targets[i] = t
	}
	c.Params = cloneParams(u.Params)
	if u.FilterParams != nil {
		c.FilterParams = make(map[string]map[string]any, len(u.FilterParams))
		for k, v := range u.FilterParams {
			c.FilterParams[k] = cloneParams(v)
		}
	}
	if u.FiltersEnabled != nil {
		c.FiltersEnabled = make(map[string]bool, len(u.FiltersEnabled))
		for k, v := range u.FiltersEnabled {
			c.FiltersEnabled[k] = v
		}
	}
	return &c
}

// cloneParams 参数表拷贝
func cloneParams(p map[string]any) map[string]any {
	if p == nil {
		return nil
	}
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

// Delete 删除实例(DB 行先行,失败不动内存;成功后内存+部件缓存+kv 凭据级联)
func (r *Registry) Delete(ctx context.Context, id int64) error {
	r.mu.RLock()
	u, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("upstream %d not found", id)
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM upstreams WHERE id=?`, id); err != nil {
		return fmt.Errorf("delete upstream row: %w", err)
	}
	r.mu.Lock()
	delete(r.byID, id)
	old := r.instCache[u.Name]
	delete(r.instCache, u.Name)
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	for _, t := range u.Targets {
		r.secrets.DeleteTargetSecrets(u.Name, t.Name)
	}
	return nil
}

// Get 取实例(深拷贝;secrets 不回读)
func (r *Registry) Get(id int64) (*Upstream, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("upstream %d not found", id)
	}
	return copyUpstream(u), nil
}

// List 全部实例(深拷贝,稳定 ID 序)
func (r *Registry) List() []*Upstream {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Upstream, 0, len(r.byID))
	ids := make([]int64, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		out = append(out, copyUpstream(r.byID[id]))
	}
	return out
}

// DeclaredProtocol 上游主包声明的协议全名(实例缓存命中即免查包表;缓存未命中回退包表)
func (r *Registry) DeclaredProtocol(u *Upstream) string {
	r.mu.RLock()
	if c := r.instCache[u.Name]; c != nil {
		d := c.Declared
		r.mu.RUnlock()
		return d
	}
	r.mu.RUnlock()
	return r.pkgs.DeclaredProtocol(u.Base.Package)
}

// Pick 路由:模型声明发现 → 协议/能力/形态过滤 → 目标可用性。
// 分类优先级:① 无任何上游声明该模型 → PickNoModel;② 有声明但因协议/能力/形态被滤 → PickCapability;
// ③ 能力一致但零可用目标 → PickUnhealthy。禁用上游仍参与 ①(避免 404 与真实声明矛盾)。
func (r *Registry) Pick(model string, features []Feature, entry string, stream bool) ([]Candidate, PickError, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var withModel []*Upstream
	for _, u := range r.byID {
		if !containsString(u.Models, model) {
			continue
		}
		if !u.Enabled {
			continue
		}
		withModel = append(withModel, u)
	}
	if len(withModel) == 0 {
		return nil, PickNoModel, PickNoModel
	}
	// ID 序稳定候选
	sort.Slice(withModel, func(i, j int) bool { return withModel[i].ID < withModel[j].ID })
	var candidates []Candidate
	capMissing := false
	// 逐上游被滤原因(capability 错误文案;用户据此自救:换入口/关流/补能力声明或换上游)
	var reasons []string
	for _, u := range withModel {
		if reason, ok := r.mismatchReason(u, features, entry, stream); !ok {
			capMissing = true
			reasons = append(reasons, reason)
			continue
		}
		if !hasEnabledTarget(u) {
			continue
		}
		candidates = append(candidates, Candidate{Upstream: copyUpstream(u)})
	}
	if len(candidates) == 0 {
		if capMissing {
			// 上限防御:上游多时长串;声明该模型的上游数量级小,5 条足够定位
			if len(reasons) > 5 {
				reasons = reasons[:5]
			}
			return nil, PickCapability, fmt.Errorf("%w: model %s: %s", PickCapability, model, strings.Join(reasons, "; "))
		}
		return nil, PickUnhealthy, PickUnhealthy
	}
	return candidates, 0, nil
}

// mismatchReason 上游被滤原因(可服务=false 时返回 reason);可服务=true 时 reason 空
func (r *Registry) mismatchReason(u *Upstream, features []Feature, entry string, stream bool) (string, bool) {
	if !r.pkgs.IsEnabled(u.Base.Package) {
		return u.Name + " package disabled", false
	}
	pkg, err := r.pkgs.GetPackage(u.Base.Package)
	if err != nil || pkg.Manifest.Parts.Protocol == nil {
		return u.Name + " package not loadable", false
	}
	d := pkg.Manifest.Parts.Protocol
	if d.Protocol != entry {
		return u.Name + " declares " + d.Protocol, false
	}
	if !hasForm(d.Form, stream) {
		if stream {
			return u.Name + " declares non_streaming only", false
		}
		return u.Name + " declares streaming only", false
	}
	var missing []string
	for _, f := range features {
		if !containsString(d.Features, f) {
			missing = append(missing, string(f))
		}
	}
	if len(missing) > 0 {
		return u.Name + " lacks " + strings.Join(missing, ", "), false
	}
	return "", true
}

// hasForm 入口形态是否被声明
func hasForm(forms []string, stream bool) bool {
	want := pipeline.FormNonStreaming
	if stream {
		want = pipeline.FormStreaming
	}
	for _, f := range forms {
		if f == want {
			return true
		}
	}
	return false
}

// hasEnabledTarget 存在 enabled 目标(MVP:目标恒视为 healthy)
func hasEnabledTarget(u *Upstream) bool {
	for _, t := range u.Targets {
		if t.Enabled {
			return true
		}
	}
	return false
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Resolve 实例 → 管道产物(部件池请求持有;release 必须在响应完全写完后调用)
func (r *Registry) Resolve(u *Upstream) (pipeline.Resolved, func(), error) {
	parts, release, err := r.cachedParts(u)
	if err != nil {
		return pipeline.Resolved{}, nil, err
	}
	out := pipeline.Resolved{
		Filters:  parts.Filters,
		Protocol: parts.Protocol,
	}
	// Targets:enabled 过滤;Secrets 不进产物,SecretsRef=上游/目标定位键
	for _, t := range u.Targets {
		if !t.Enabled {
			continue
		}
		out.Targets = append(out.Targets, pipeline.Target{
			ID:         fmt.Sprintf("%d/%s", u.ID, t.Name),
			Name:       t.Name,
			BaseURL:    t.BaseURL,
			Transport:  t.Transport,
			SecretsRef: u.Name + "/" + t.Name,
		})
	}
	return out, release, nil
}

// cachedParts 取部件实例(请求持有 +1;缓存键=上游名,revision/fingerprint 失效)
// 旧实例 evict 后引用计数归零销毁池(在途请求不受影响)
func (r *Registry) cachedParts(u *Upstream) (*resolvedParts, func(), error) {
	r.mu.RLock()
	cached := r.instCache[u.Name]
	r.mu.RUnlock()
	if cached != nil && cached.acquire() {
		if cached.matches(r.pkgs, u) {
			return cached, cached.release, nil
		}
		cached.release() // 有效实例但配置漂移:释放走重建
	}
	parts, err := r.instantiateParts(u)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	old := r.instCache[u.Name]
	r.instCache[u.Name] = parts
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	if !parts.acquire() { // 刚构建不可能死亡;防御式
		return nil, nil, fmt.Errorf("parts evicted during instantiate")
	}
	return parts, parts.release, nil
}

// matches 缓存是否仍然有效(包 revision 一致 + 实例参数指纹一致)
func (c *resolvedParts) matches(pkgs *plugin.Registry, u *Upstream) bool {
	baseRev := pkgs.Revision(u.Base.Package)
	if baseRev != c.baseRev || len(u.Extras) != len(c.extraRev) {
		return false
	}
	for i, ex := range u.Extras {
		if pkgs.Revision(ex.Package) != c.extraRev[i] {
			return false
		}
	}
	return c.fingerprint == fingerprint(u)
}

// instantiateParts 实例化:Filters(extras→base,启停过滤)+ Protocol(内置工厂优先)
func (r *Registry) instantiateParts(u *Upstream) (*resolvedParts, error) {
	parts := &resolvedParts{fingerprint: fingerprint(u)}
	parts.refs.Store(1) // 缓存持有份
	parts.baseRev = r.pkgs.Revision(u.Base.Package)
	base, err := r.pkgs.GetPackage(u.Base.Package)
	if err != nil {
		return nil, err
	}
	var filters []pipeline.Filter
	for _, ex := range u.Extras {
		parts.extraRev = append(parts.extraRev, r.pkgs.Revision(ex.Package))
		pkg, err := r.pkgs.GetPackage(ex.Package)
		if err != nil {
			return nil, err
		}
		for _, fp := range pkg.Manifest.Parts.Filters {
			key := ex.Package + "/" + fp.Name
			if enabled, ok := u.FiltersEnabled[key]; ok && !enabled {
				continue
			}
			f, err := plugin.NewFilter(pkg, fp, u.FilterParams[key], r.secretReader(u), r.secretValues(u), r.pkgStorage(ex.Package))
			if err != nil {
				return nil, err
			}
			filters = append(filters, f)
		}
	}
	for _, fp := range base.Manifest.Parts.Filters {
		key := u.Base.Package + "/" + fp.Name
		if enabled, ok := u.FiltersEnabled[key]; ok && !enabled {
			continue
		}
		f, err := plugin.NewFilter(base, fp, u.FilterParams[key], r.secretReader(u), r.secretValues(u), r.pkgStorage(u.Base.Package))
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	parts.Filters = filters
	// Protocol:同名内置工厂优先(server 装配注册),否则编译主包 JS 部件
	if base.Manifest.Parts.Protocol != nil {
		parts.Declared = base.Manifest.Parts.Protocol.Protocol
	}
	var proto pipeline.Protocol
	if factory, ok := r.pkgs.BuiltinFactory(u.Base.Package); ok {
		proto, err = factory(plugin.BuiltinDeps{TargetSecrets: r.secretReader(u)})
		if err != nil {
			return nil, err
		}
	} else {
		proto, err = plugin.NewProtocol(base, u.Params, r.secretReader(u), r.secretValues(u), r.pkgStorage(u.Base.Package))
		if err != nil {
			return nil, err
		}
	}
	parts.Protocol = proto
	return parts, nil
}

// fingerprint 实例参数指纹(启停/参数/模型/目标名参与;Save 会清缓存,此处为兜底)
func fingerprint(u *Upstream) string {
	b, _ := json.Marshal(struct {
		Params    map[string]any            `json:"p"`
		FilterPrm map[string]map[string]any `json:"fp"`
		FEn       map[string]bool           `json:"fe"`
	}{u.Params, u.FilterParams, u.FiltersEnabled})
	return hex.EncodeToString(hashing(b))
}

// hashing sha256 摘要
func hashing(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// secretReader 按 (target 名, 键) 解析凭据("当前 target" 由部件 cursor 承载)
func (r *Registry) secretReader(u *Upstream) func(target, key string) (string, bool) {
	return func(target, key string) (string, bool) {
		secrets, ok := r.secrets.GetTargetSecrets(u.Name, target)
		if !ok {
			return "", false
		}
		v, ok := secrets[key]
		return v, ok
	}
}

// secretValues 当前 target 全量凭据值(部件输出脱敏用)
func (r *Registry) secretValues(u *Upstream) func(target string) map[string]string {
	return func(target string) map[string]string {
		m, _ := r.secrets.GetTargetSecrets(u.Name, target)
		return m
	}
}

// pkgStorage 部件存储(ns=包名,包间隔离,同包多实例共享)
func (r *Registry) pkgStorage(pkgName string) plugin.StorageKV {
	return &dbStorage{db: r.db, ns: pkgName}
}

// dbStorage kv 表存储实现
type dbStorage struct {
	db *sql.DB
	ns string
}

func (s *dbStorage) Get(key string) (string, bool) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM kv WHERE ns=? AND key=?`, s.ns, key).Scan(&v)
	return v, err == nil
}

func (s *dbStorage) Set(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO kv(ns, key, value) VALUES(?,?,?)
		ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`, s.ns, key, value)
	return err
}

func (s *dbStorage) Delete(key string) {
	_, _ = s.db.Exec(`DELETE FROM kv WHERE ns=? AND key=?`, s.ns, key)
}

// Package upstream 模型行(包实例化)/校验/Pick/Resolve(v2:行即模型)
package upstream

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// Feature 能力协商(与 convert.Feature 字符串语义一致,避免包依赖)
type Feature = string

// Model 模型行(路由单元;唯一键 (name, plugin))
type Model struct {
	ID      int64          `json:"id"`
	Name    string         `json:"name"`   // 客户端请求的模型名(路由输入,非唯一键)
	Plugin  string         `json:"plugin"` // 包名引用(浮动跟随 latest revision)
	Params  map[string]any `json:"params"` // 模型级参数覆盖(声明槽白名单;密钥禁入)
	Enabled bool           `json:"enabled"`
}

// Candidate Pick 候选
type Candidate struct {
	Model *Model
}

// PickError 失败原因分类(v2:无目标概念,健康度分类删除)
// 非零枚举:Pick 成功路径返回 pickOK(0),误用零值判定"无模型"的隐患消除
type PickError int

// Pick 分类常量(pickOK 非导出:仅占位成功零值,导出面只含失败分类)
const (
	pickOK PickError = iota
	PickNoModel
	PickCapability
)

func (e PickError) Error() string {
	switch e {
	case PickNoModel:
		return "no model row declares model"
	case PickCapability:
		return "capability mismatch"
	default:
		return "pick ok"
	}
}

// Registry 模型行注册中心
type Registry struct {
	mu      sync.RWMutex
	db      *sql.DB
	pkgs    *plugin.Registry
	byID    map[int64]*Model
	// instCache 部件实例缓存(键=行 ID;包 revision/插件参数版本/Save 失效)
	instCache map[int64]*resolvedParts
	// transportEvict 插件 util.evict 出口(宿主注入;nil=插件上报报错)
	transportEvict func(transport, scope, value string) error
}

// SetTransportEvict 注入插件 util.evict 出口(装配期调用;mu 保护读写,防运行期注入数据竞争)
func (r *Registry) SetTransportEvict(f func(transport, scope, value string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transportEvict = f
}

// resolvedParts 可缓存的部件实例(JS runtime 池实例化成本高)
// refs=持有计数(缓存 1 + 每在途请求 1);evict 后归零销毁池(Reload 在途保护)
type resolvedParts struct {
	baseRev     int64
	settingsVer int64
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

// NewRegistry 构造
func NewRegistry(db *sql.DB, pkgs *plugin.Registry) *Registry {
	return &Registry{db: db, pkgs: pkgs, byID: map[int64]*Model{}, instCache: map[int64]*resolvedParts{}}
}

// LoadFromDB 启动恢复;检测 v1 遗表则先执行数据迁移
func (r *Registry) LoadFromDB(ctx context.Context) error {
	if err := r.migrateV1(ctx); err != nil {
		return err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, name, base_package, params_json, enabled FROM upstreams`)
	if err != nil {
		return fmt.Errorf("query models: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		m := &Model{}
		var paramsRaw string
		if err := rows.Scan(&m.ID, &m.Name, &m.Plugin, &paramsRaw, &m.Enabled); err != nil {
			return fmt.Errorf("scan model: %w", err)
		}
		if err := json.Unmarshal([]byte(paramsRaw), &m.Params); err != nil {
			log.Printf("load model %d %s: bad params_json (treated as empty): %v", m.ID, m.Name, err)
		}
		r.byID[m.ID] = m
	}
	return rows.Err()
}

// v1Row v1 遗表行(仅迁移期形态)
type v1Row struct {
	ID       int64
	Name     string
	Base     string
	Extras   []string
	Models   []string
	Targets  []v1Target
	Params   map[string]any
	FiltersE map[string]bool
	Enabled  bool
}

// v1Target v1 目标形态(仅迁移期)
type v1Target struct {
	Name      string            `json:"name"`
	BaseURL   string            `json:"base_url"`
	Transport string            `json:"transport"`
	Secrets   map[string]string `json:"secrets"`
	Enabled   bool              `json:"enabled"`
}

// migrateV1 v1→v2 数据迁移(幂等:仅当 upstreams_v1 存在时执行一次)
// 规则:models 拆行;同 (name,plugin) 重复最小 ID 胜出;同包多实例参数逐槽一致→包参数,
// 分歧→模型行 params 保真;base_url/api_key 分歧→最小 ID 实例胜出;密钥并入包级 keys。
func (r *Registry) migrateV1(ctx context.Context) error {
	var exist int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='upstreams_v1'`).Scan(&exist); err != nil {
		return fmt.Errorf("probe v1 table: %w", err)
	}
	if exist == 0 {
		return nil
	}
	log.Printf("[migrate-v2] v1 upstreams detected; converting to model rows")
	rows, err := r.db.QueryContext(ctx, `SELECT id, name, base_package, extras_json, models_json, targets_json,
		params_json, filters_enabled_json, enabled FROM upstreams_v1 ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query v1 rows: %w", err)
	}
	var v1 []v1Row
	for rows.Next() {
		var row v1Row
		var extrasRaw, modelsRaw, targetsRaw, paramsRaw, feRaw string
		if err := rows.Scan(&row.ID, &row.Name, &row.Base, &extrasRaw, &modelsRaw, &targetsRaw,
			&paramsRaw, &feRaw, &row.Enabled); err != nil {
			rows.Close()
			return fmt.Errorf("scan v1 row: %w", err)
		}
		_ = json.Unmarshal([]byte(extrasRaw), &row.Extras)
		_ = json.Unmarshal([]byte(modelsRaw), &row.Models)
		_ = json.Unmarshal([]byte(targetsRaw), &row.Targets)
		_ = json.Unmarshal([]byte(paramsRaw), &row.Params)
		_ = json.Unmarshal([]byte(feRaw), &row.FiltersE)
		v1 = append(v1, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate v1 rows: %w", err)
	}
	// 同包分组(保持 ID 升序)
	byPkg := map[string][]v1Row{}
	pkgOrder := []string{}
	for _, row := range v1 {
		if _, seen := byPkg[row.Base]; !seen {
			pkgOrder = append(pkgOrder, row.Base)
		}
		byPkg[row.Base] = append(byPkg[row.Base], row)
	}
	for _, pkgName := range pkgOrder {
		group := byPkg[pkgName]
		pkg, pkgErr := r.pkgs.GetPackage(pkgName)
		if pkgErr != nil {
			// 包缺失:实例参数无处收敛,DROP v1 表即永久丢失 → 中止迁移(v1 表保留,装包后重启重试)
			return fmt.Errorf("migrate package %s (v1 table kept for retry): %w", pkgName, pkgErr)
		}
		decl := map[string]any{}
		if pkg.Declaration != nil {
			decl = pkg.Declaration
		}
		// ① 参数逐槽收敛:一致→包级;分歧→模型行保真
		pluginVals := map[string]any{}
		divergent := map[string]bool{}
		for _, row := range group {
			for slot, v := range row.Params {
				if _, declared := decl[slot]; !declared {
					// 未声明槽:不收敛也不丢弃——保真进各行 params(请求期白名单剥离,数据不丢)
					log.Printf("[migrate-v2] package %s: param %q not declared; kept at model level", pkgName, slot)
					continue
				}
				prev, seen := pluginVals[slot]
				if !seen {
					pluginVals[slot] = v
					continue
				}
				// JSON 形态比较:类型分歧(string "1" vs 数字 1)不算一致
				pb, _ := json.Marshal(prev)
				vb, _ := json.Marshal(v)
				if string(pb) != string(vb) {
					divergent[slot] = true
				}
			}
		}
		for slot := range divergent {
			log.Printf("[migrate-v2] package %s: param %q diverges across instances; kept at model level", pkgName, slot)
			delete(pluginVals, slot)
		}
		if len(pluginVals) > 0 {
			view := r.pkgs.Settings().View(pkgName)
			merged := map[string]any{}
			if cfg, ok := view.Overrides["config"].(map[string]any); ok {
				for k, v := range cfg {
					merged[k] = v
				}
			}
			for k, v := range pluginVals {
				merged[k] = v
			}
			// Put 是全量文档覆盖:回传已有 tasks 覆盖,避免清掉 v1 时代任务配置
			tasks := tasksOverride(view.Overrides)
			if _, err := r.pkgs.Settings().Put(pkgName, plugin.PutInput{Config: merged, Tasks: tasks}); err != nil {
				return fmt.Errorf("migrate package params %s: %w", pkgName, err)
			}
		}
		// ② 连接信息收敛:base_url → 包参数;secrets → 包级 keys(分歧最小 ID 胜出)
		// v1 凭据双源:kv ns=upstream:<实例>(键 secrets:<实例>)∪ targets_json 内嵌 secrets;
		// 数据保全优先:全部实例的凭据无条件并入(disabled 行同样),读错误中止迁移(v1 表保留可重试)
		baseURL := ""
		connSet := false
		keys := map[string]any{}
		for _, row := range group {
			secrets, secretsErr := r.v1Secrets(ctx, row.Name)
			if secretsErr != nil {
				return fmt.Errorf("migrate v1 secrets %s (v1 table kept for retry): %w", row.Name, secretsErr)
			}
			for k, v := range secrets {
				if _, dup := keys[k]; !dup {
					keys[k] = v
				}
			}
			for _, t := range row.Targets {
				if !connSet {
					if _, declared := decl["base_url"]; declared && t.BaseURL != "" {
						baseURL = t.BaseURL
						connSet = true
					} else if t.BaseURL != "" {
						log.Printf("[migrate-v2] package %s: target base_url %s not declared as param; dropped", pkgName, t.BaseURL)
					}
				} else if t.BaseURL != "" && t.BaseURL != baseURL {
					log.Printf("[migrate-v2] package %s: base_url diverges; keeping first instance value %s", pkgName, baseURL)
				}
				for k, v := range t.Secrets {
					if _, dup := keys[k]; !dup {
						keys[k] = v
					}
				}
			}
		}
		if len(keys) > 0 {
			for k, v := range keys {
				if _, err := r.pkgs.Keys().Set(pkgName, k, v); err != nil {
					return fmt.Errorf("migrate package keys %s: %w", pkgName, err)
				}
			}
		}
		if baseURL != "" {
			view := r.pkgs.Settings().View(pkgName)
			cfg := map[string]any{}
			if c, ok := view.Overrides["config"].(map[string]any); ok {
				for k, v := range c {
					cfg[k] = v
				}
			}
			if _, dup := cfg["base_url"]; !dup {
				cfg["base_url"] = baseURL
				tasks := tasksOverride(view.Overrides)
				if _, err := r.pkgs.Settings().Put(pkgName, plugin.PutInput{Config: cfg, Tasks: tasks}); err != nil {
					return fmt.Errorf("migrate package base_url %s: %w", pkgName, err)
				}
			}
		}
		// ③ models 拆行((name, plugin) 重复最小 ID 胜出;分歧参数与未声明槽入模型行保真)
		for _, row := range group {
			rowDiv := map[string]any{}
			for slot, v := range row.Params {
				if divergent[slot] {
					rowDiv[slot] = v
					continue
				}
				if _, declared := decl[slot]; !declared {
					rowDiv[slot] = v
				}
			}
			for _, modelName := range row.Models {
				var dupID int64
				err := r.db.QueryRowContext(ctx, `SELECT id FROM upstreams WHERE name=? AND base_package=?`,
					modelName, row.Base).Scan(&dupID)
				if err == nil {
					log.Printf("[migrate-v2] model %s on %s duplicated (existing id %d wins); skipped row %d",
						modelName, row.Base, dupID, row.ID)
					continue
				}
				paramsRaw, _ := json.Marshal(rowDiv)
				if _, err := r.db.ExecContext(ctx,
					`INSERT INTO upstreams(name, base_package, params_json, enabled) VALUES(?,?,?,?)`,
					modelName, row.Base, string(paramsRaw), row.Enabled); err != nil {
					return fmt.Errorf("insert migrated model %s: %w", modelName, err)
				}
			}
		}
	}
	if _, err := r.db.ExecContext(ctx, `DROP TABLE upstreams_v1`); err != nil {
		return fmt.Errorf("drop v1 table: %w", err)
	}
	// v1 凭据 kv 残留清理(ns=upstream:<name>)
	if _, err := r.db.ExecContext(ctx, `DELETE FROM kv WHERE ns LIKE 'upstream:%'`); err != nil {
		log.Printf("[migrate-v2] v1 secrets kv cleanup failed (non-fatal): %v", err)
	}
	log.Printf("[migrate-v2] conversion complete")
	return nil
}

// tasksOverride overrides 文档的 tasks 键收窄(JSON 反序列化内层恒 map[string]any,须逐项转换)
func tasksOverride(overrides map[string]any) map[string]map[string]any {
	raw, ok := overrides["tasks"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]map[string]any, len(raw))
	for name, doc := range raw {
		if m, ok := doc.(map[string]any); ok {
			out[name] = m
		}
	}
	return out
}

// v1Secrets 读 v1 实例凭据(kv ns=upstream:<name>,单行 JSON {"key": "value"})
func (r *Registry) v1Secrets(ctx context.Context, name string) (map[string]string, error) {
	var raw string
	err := r.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE ns=? AND key=?`,
		"upstream:"+name, "secrets:"+name).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	out := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("decode secrets: %w", err)
	}
	return out, nil
}

// Validate 保存期校验(name/plugin 必填;插件存在且含协议;params 声明槽白名单+合并结果校验)
func (r *Registry) Validate(m *Model) error {
	if m.Name == "" {
		return fmt.Errorf("name required")
	}
	if m.Plugin == "" {
		return fmt.Errorf("plugin required")
	}
	pkg, err := r.pkgs.GetPackage(m.Plugin)
	if err != nil {
		return fmt.Errorf("plugin package: %w", err)
	}
	if !pkg.HasProtocol() {
		return fmt.Errorf("package %q has no protocol part", m.Plugin)
	}
	if _, err := r.resolveParams(m); err != nil {
		return err
	}
	return nil
}

// resolveParams 模型行参数解析(声明 ⊕ 插件参数 ⊕ 模型覆盖;strict=保存期)
func (r *Registry) resolveParams(m *Model) (map[string]any, error) {
	pkg, err := r.pkgs.GetPackage(m.Plugin)
	if err != nil {
		return nil, err
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	pluginVals := map[string]any{}
	if cfg, ok := r.pkgs.SettingsOverrides(m.Plugin)["config"].(map[string]any); ok {
		pluginVals = cfg
	}
	merged, err := plugin.ResolveParams(decl, pluginVals, m.Params, plugin.ParamSave)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	return merged, nil
}

// Save 保存(校验+持久化+热生效;唯一键 (name,plugin),改键=删重建)
func (r *Registry) Save(ctx context.Context, m *Model) error {
	if err := r.Validate(m); err != nil {
		return err
	}
	paramsRaw, _ := json.Marshal(m.Params)
	res := r.db.QueryRowContext(ctx, `INSERT INTO upstreams(name, base_package, params_json, enabled)
		VALUES(?,?,?,?)
		ON CONFLICT(name, base_package) DO UPDATE SET params_json=excluded.params_json,
		enabled=excluded.enabled, updated_at=datetime('now')
		RETURNING id`,
		m.Name, m.Plugin, string(paramsRaw), m.Enabled)
	if err := res.Scan(&m.ID); err != nil {
		return fmt.Errorf("persist model: %w", err)
	}
	saved := *m
	saved.Params = cloneParams(m.Params)
	r.mu.Lock()
	r.byID[saved.ID] = &saved
	old := r.instCache[saved.ID]
	delete(r.instCache, saved.ID)
	r.mu.Unlock()
	if old != nil {
		go old.evict() // 旧配置池:在途归零后销毁
	}
	// 同名其他行共享插件参数闭包差异不受影响;本行重建即可
	return nil
}

// copyModel 深拷贝(管理读取用,防共享指针污染)
func copyModel(m *Model) *Model {
	c := *m
	c.Params = cloneParams(m.Params)
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

// Delete 删除行(DB 行先行,失败不动内存;成功后内存+部件缓存级联)
func (r *Registry) Delete(ctx context.Context, id int64) error {
	r.mu.RLock()
	_, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("model %d not found", id)
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM upstreams WHERE id=?`, id); err != nil {
		return fmt.Errorf("delete model row: %w", err)
	}
	r.mu.Lock()
	delete(r.byID, id)
	old := r.instCache[id]
	delete(r.instCache, id)
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	return nil
}

// Get 取行(深拷贝)
func (r *Registry) Get(id int64) (*Model, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("model %d not found", id)
	}
	return copyModel(m), nil
}

// List 全部行(深拷贝,稳定 ID 序)
func (r *Registry) List() []*Model {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Model, 0, len(r.byID))
	ids := make([]int64, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		out = append(out, copyModel(r.byID[id]))
	}
	return out
}

// DeclaredProtocol 行主包声明的协议全名(实例缓存命中即免查包表;缓存未命中回退包表)
func (r *Registry) DeclaredProtocol(m *Model) string {
	r.mu.RLock()
	if c := r.instCache[m.ID]; c != nil {
		d := c.Declared
		r.mu.RUnlock()
		return d
	}
	r.mu.RUnlock()
	return r.pkgs.DeclaredProtocol(m.Plugin)
}

// Pick 路由:模型名声明发现 → 协议/能力/形态过滤(候选链语义见架构稿 §2.2)。
// 分类:① 无任何行声明该模型名 → PickNoModel;② 有声明但全部因 disabled/协议/能力/形态被滤 → PickCapability。
// disabled 行参与 ①(避免 404 与真实声明矛盾);同协议多行按最小 ID 取首。
func (r *Registry) Pick(model string, features []Feature, entry string, stream bool) ([]Candidate, PickError, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var withModel []*Model
	for _, m := range r.byID {
		if m.Name != model {
			continue
		}
		withModel = append(withModel, m)
	}
	if len(withModel) == 0 {
		return nil, PickNoModel, PickNoModel
	}
	// ID 序稳定候选
	sort.Slice(withModel, func(i, j int) bool { return withModel[i].ID < withModel[j].ID })
	var candidates []Candidate
	capMissing := false
	// 逐行被滤原因(capability 错误文案;用户据此自救:换入口/关流/补能力声明或换插件)
	var reasons []string
	for _, m := range withModel {
		if !m.Enabled {
			capMissing = true
			reasons = append(reasons, m.Name+"@"+m.Plugin+" row disabled")
			continue
		}
		if reason, ok := r.mismatchReason(m, features, entry, stream); !ok {
			capMissing = true
			reasons = append(reasons, reason)
			continue
		}
		candidates = append(candidates, Candidate{Model: copyModel(m)})
	}
	if len(candidates) == 0 {
		if capMissing {
			// 上限防御:行多时长串;声明该模型的行数量级小,5 条足够定位
			if len(reasons) > 5 {
				reasons = reasons[:5]
			}
			return nil, PickCapability, fmt.Errorf("%w: model %s: %s", PickCapability, model, strings.Join(reasons, "; "))
		}
	}
	return candidates, pickOK, nil
}

// mismatchReason 行被滤原因(可服务=false 时返回 reason;文案含 行名@插件 定位);可服务=true 时 reason 空
func (r *Registry) mismatchReason(m *Model, features []Feature, entry string, stream bool) (string, bool) {
	tag := m.Name + "@" + m.Plugin
	if !r.pkgs.IsEnabled(m.Plugin) {
		return tag + " package disabled", false
	}
	pkg, err := r.pkgs.GetPackage(m.Plugin)
	if err != nil || pkg.Manifest.Parts.Protocol == nil {
		return tag + " package not loadable", false
	}
	d := pkg.Manifest.Parts.Protocol
	if d.Protocol != entry {
		return tag + " declares " + d.Protocol, false
	}
	// 形态由实现推导(内置走工厂实例 Supports;不可判定如实说明,不虚构"only")
	supports := r.pkgs.ProtocolSupports(m.Plugin)
	if supports == nil {
		return tag + " form support undeterminable", false
	}
	if !hasForm(supports.Forms, stream) {
		if stream {
			return tag + " implements non_streaming only", false
		}
		return tag + " implements streaming only", false
	}
	var missing []string
	for _, f := range features {
		if !containsString(d.Features, f) {
			missing = append(missing, string(f))
		}
	}
	if len(missing) > 0 {
		return tag + " lacks " + strings.Join(missing, ", "), false
	}
	return "", true
}

// hasForm 入口形态是否被支持
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

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Resolve 行 → 管道产物(部件池请求持有;release 必须在响应完全写完后调用)
func (r *Registry) Resolve(m *Model) (pipeline.Resolved, func(), error) {
	parts, release, err := r.cachedParts(m)
	if err != nil {
		return pipeline.Resolved{}, nil, err
	}
	return pipeline.Resolved{
		Filters:  parts.Filters,
		Protocol: parts.Protocol,
	}, release, nil
}

// cachedParts 取部件实例(请求持有 +1;缓存键=行 ID,revision/插件参数版本/fingerprint 失效)
// 旧实例 evict 后引用计数归零销毁池(在途请求不受影响)
func (r *Registry) cachedParts(m *Model) (*resolvedParts, func(), error) {
	r.mu.RLock()
	cached := r.instCache[m.ID]
	r.mu.RUnlock()
	if cached != nil && cached.acquire() {
		if cached.matches(r.pkgs, m) {
			return cached, cached.release, nil
		}
		cached.release() // 有效实例但配置漂移:释放走重建
	}
	parts, err := r.instantiateParts(m)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	old := r.instCache[m.ID]
	r.instCache[m.ID] = parts
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	if !parts.acquire() { // 刚构建不可能死亡;防御式
		return nil, nil, fmt.Errorf("parts evicted during instantiate")
	}
	return parts, parts.release, nil
}

// matches 缓存是否仍然有效(包 revision + 插件参数版本 + 模型参数指纹一致)
func (c *resolvedParts) matches(pkgs *plugin.Registry, m *Model) bool {
	if pkgs.Revision(m.Plugin) != c.baseRev || pkgs.Settings().View(m.Plugin).Version != c.settingsVer {
		return false
	}
	return c.fingerprint == fingerprint(m)
}

// instantiateParts 实例化:Filters(包内序)+ Protocol(内置工厂优先);
// 参数闭包 = 声明 ⊕ 插件参数 ⊕ 模型覆盖(请求期语义:未知键剥离)
func (r *Registry) instantiateParts(m *Model) (*resolvedParts, error) {
	pkg, err := r.pkgs.GetPackage(m.Plugin)
	if err != nil {
		return nil, err
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	pluginVals := map[string]any{}
	if cfg, ok := r.pkgs.SettingsOverrides(m.Plugin)["config"].(map[string]any); ok {
		pluginVals = cfg
	}
	// 请求期:未知键剥离不阻断流量(剥离记日志,残留键来自包升级删槽)
	for k := range m.Params {
		if _, known := decl[k]; !known {
			log.Printf("params: model %s: unknown param %q stripped (not declared by package %s)", m.Name, k, m.Plugin)
		}
	}
	merged, err := plugin.ResolveParams(decl, pluginVals, m.Params, plugin.ParamRequest)
	if err != nil {
		return nil, err
	}
	parts := &resolvedParts{fingerprint: fingerprint(m), settingsVer: r.pkgs.Settings().View(m.Plugin).Version}
	parts.refs.Store(1) // 缓存持有份
	parts.baseRev = r.pkgs.Revision(m.Plugin)
	r.mu.RLock()
	transportEvict := r.transportEvict
	r.mu.RUnlock()
	var filters []pipeline.Filter
	for _, fp := range pkg.Manifest.Parts.Filters {
		f, err := plugin.NewFilter(pkg, fp, cloneParams(merged), r.pkgStorage(m.Plugin), r.maskValues(m.Plugin), transportEvict)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	parts.Filters = filters
	// Protocol:同名内置工厂优先(server 装配注册),否则编译主包 JS 部件
	if pkg.Manifest.Parts.Protocol != nil {
		parts.Declared = pkg.Manifest.Parts.Protocol.Protocol
	}
	var proto pipeline.Protocol
	if factory, ok := r.pkgs.BuiltinFactory(m.Plugin); ok {
		proto, err = factory(plugin.BuiltinDeps{Config: cloneParams(merged), PackageKey: r.currentKeyData(m.Plugin)})
		if err != nil {
			return nil, err
		}
	} else {
		proto, err = plugin.NewProtocol(pkg, cloneParams(merged), r.pkgStorage(m.Plugin), r.maskValues(m.Plugin), transportEvict)
		if err != nil {
			return nil, err
		}
	}
	parts.Protocol = proto
	return parts, nil
}

// maskValues 包全部键 data 的字符串叶子(inspect/log 脱敏源)
func (r *Registry) maskValues(pkgName string) func() []string {
	return func() []string { return r.pkgs.Keys().MaskStrings(pkgName) }
}

// currentKeyData 当前键 data 读取闭包(内置协议注入;请求级选键经 ctx,内置单例读轮询指针)
func (r *Registry) currentKeyData(pkgName string) func() (any, bool) {
	return func() (any, bool) {
		e, ok := r.pkgs.Keys().Rotate(pkgName)
		if !ok {
			return nil, false
		}
		return e.Data, true
	}
}

// EvictPackageSettings 插件参数保存后失效引用该包的行缓存(参数烘焙进部件,改值必须重建)
func (r *Registry) EvictPackageSettings(pkgName string) {
	r.mu.Lock()
	var ids []int64
	for id, m := range r.byID {
		if m.Plugin == pkgName {
			ids = append(ids, id)
		}
	}
	var olds []*resolvedParts
	for _, id := range ids {
		if c, ok := r.instCache[id]; ok {
			delete(r.instCache, id)
			olds = append(olds, c)
		}
	}
	r.mu.Unlock()
	for _, old := range olds {
		go old.evict()
	}
}

// fingerprint 模型参数指纹(Save 会清缓存,此处为兜底)
func fingerprint(m *Model) string {
	b, _ := json.Marshal(struct {
		Params map[string]any `json:"p"`
	}{m.Params})
	return hex.EncodeToString(hashing(b))
}

// hashing sha256 摘要
func hashing(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// pkgStorage 部件存储(ns=包名,包间隔离,同包多模型共享)
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

// Package upstream 模型组(组=上游连接,成员=组内模型短名)/校验/路由解析/Resolve(v3)
package upstream

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// Feature 能力协商(与 convert.Feature 字符串语义一致,避免包依赖)
type Feature = string

// Group 模型组(= 一个上游连接:插件包 + 参数;前缀即命名空间)
type Group struct {
	ID      int64          `json:"id"`
	Prefix  string         `json:"prefix"` // 组前缀(全路径请求 namespace/member;唯一)
	Plugin  string         `json:"plugin"` // 包名引用(浮动跟随 latest revision)
	Params  map[string]any `json:"params"` // 组级参数覆盖(声明槽白名单;密钥禁入)
	Enabled bool           `json:"enabled"`
}

// Member 组内模型(短名组内唯一;上游请求的模型名)
type Member struct {
	ID      int64  `json:"id"`
	GroupID int64  `json:"group_id"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

// Candidate Pick 候选(组内成员命中)
type Candidate struct {
	Group  *Group
	Member *Member
}

// FullName 全路径请求名(prefix/member)
func (c *Candidate) FullName() string { return c.Group.Prefix + "/" + c.Member.Name }

// PickError 失败原因分类
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
		return "no model group declares model"
	case PickCapability:
		return "capability mismatch"
	default:
		return "pick ok"
	}
}

// Registry 模型组注册中心
type Registry struct {
	mu      sync.RWMutex
	db      *sql.DB
	pkgs    *plugin.Registry
	byGroup map[int64]*Group
	// byMember 成员索引(键=成员 ID;稳定序靠组内 ID 升序重建)
	byMember map[int64]*Member
	// memberOrder 各组成员 ID 升序列表(路由扫描与列表输出用)
	memberOrder map[int64][]int64
	// instCache 部件实例缓存(键=组 ID;包 revision/插件参数版本/Save 失效)
	instCache map[int64]*resolvedParts
	// rrRound 短名歧义轮询计数器(键=短名;内存态,重启归零)
	rrRound map[string]*atomic.Uint64
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
	return &Registry{
		db: db, pkgs: pkgs,
		byGroup: map[int64]*Group{}, byMember: map[int64]*Member{},
		memberOrder: map[int64][]int64{}, instCache: map[int64]*resolvedParts{},
		rrRound: map[string]*atomic.Uint64{},
	}
}

// LoadFromDB 启动恢复(组与成员双表)
func (r *Registry) LoadFromDB(ctx context.Context) error {
	if err := r.loadGroups(ctx); err != nil {
		return err
	}
	return r.loadMembers(ctx)
}

func (r *Registry) loadGroups(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx, `SELECT id, prefix, base_package, params_json, enabled FROM model_groups`)
	if err != nil {
		return fmt.Errorf("query groups: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		g := &Group{}
		var paramsRaw string
		if err := rows.Scan(&g.ID, &g.Prefix, &g.Plugin, &paramsRaw, &g.Enabled); err != nil {
			return fmt.Errorf("scan group: %w", err)
		}
		if err := json.Unmarshal([]byte(paramsRaw), &g.Params); err != nil {
			slog.Warn("load group: bad params_json (treated as empty)", "id", g.ID, "group", g.Prefix, "err", err)
		}
		r.byGroup[g.ID] = g
	}
	return rows.Err()
}

func (r *Registry) loadMembers(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx, `SELECT id, group_id, name, enabled FROM model_members ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query members: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		m := &Member{}
		if err := rows.Scan(&m.ID, &m.GroupID, &m.Name, &m.Enabled); err != nil {
			return fmt.Errorf("scan member: %w", err)
		}
		r.indexMember(m)
	}
	return rows.Err()
}

// indexMember 成员入索引(调用方持写锁);已跟踪的成员只覆写条目,不重复追加 order
func (r *Registry) indexMember(m *Member) {
	_, tracked := r.byMember[m.ID]
	r.byMember[m.ID] = m
	if !tracked {
		r.memberOrder[m.GroupID] = append(r.memberOrder[m.GroupID], m.ID)
	}
}

// ValidateGroup 组保存期校验(前缀/插件必填;插件存在且含协议;params 声明槽白名单)
func (r *Registry) ValidateGroup(g *Group) error {
	if g.Prefix == "" {
		return fmt.Errorf("prefix required")
	}
	if strings.Contains(g.Prefix, "/") {
		return fmt.Errorf("prefix must not contain '/'")
	}
	if g.Plugin == "" {
		return fmt.Errorf("plugin required")
	}
	pkg, err := r.pkgs.GetPackage(g.Plugin)
	if err != nil {
		return fmt.Errorf("plugin package: %w", err)
	}
	if pkg.HasProtocol() && pkg.HasHandler() {
		return fmt.Errorf("package %q declares both protocol and handler parts", g.Plugin)
	}
	if !pkg.HasProtocol() && !pkg.HasHandler() {
		return fmt.Errorf("package %q has no protocol or handler part", g.Plugin)
	}
	if _, err := r.resolveParams(g); err != nil {
		return err
	}
	return nil
}

// resolveParams 组参数解析(声明 ⊕ 插件参数 ⊕ 组覆盖;strict=保存期)
func (r *Registry) resolveParams(g *Group) (map[string]any, error) {
	pkg, err := r.pkgs.GetPackage(g.Plugin)
	if err != nil {
		return nil, err
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	pluginVals := map[string]any{}
	if cfg, ok := r.pkgs.SettingsOverrides(g.Plugin)["config"].(map[string]any); ok {
		pluginVals = cfg
	}
	merged, err := plugin.ResolveParams(decl, pluginVals, g.Params, plugin.ParamSave)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	return merged, nil
}

// SaveGroup 保存组。ID=0 插入(前缀冲突报错,调用方映射 409);ID>0 更新(拒绝改 plugin)。
func (r *Registry) SaveGroup(ctx context.Context, g *Group) error {
	if err := r.ValidateGroup(g); err != nil {
		return err
	}
	r.mu.RLock()
	var dup *Group
	for _, other := range r.byGroup {
		if other.Prefix == g.Prefix && other.ID != g.ID {
			dup = other
		}
	}
	if g.ID > 0 {
		if cur, ok := r.byGroup[g.ID]; !ok {
			r.mu.RUnlock()
			return fmt.Errorf("%w: group %d not found", ErrNotFound, g.ID)
		} else if cur.Plugin != g.Plugin {
			r.mu.RUnlock()
			return fmt.Errorf("plugin is immutable on group")
		}
	}
	r.mu.RUnlock()
	if dup != nil {
		return fmt.Errorf("%w: prefix %q already used by group %d", ErrConflict, g.Prefix, dup.ID)
	}
	paramsRaw, _ := json.Marshal(g.Params)
	if g.ID == 0 {
		res := r.db.QueryRowContext(ctx, `INSERT INTO model_groups(prefix, base_package, params_json, enabled)
			VALUES(?,?,?,?) RETURNING id`, g.Prefix, g.Plugin, string(paramsRaw), g.Enabled)
		if err := res.Scan(&g.ID); err != nil {
			// 并发下同前缀落库:检查-写入窗口的兜底,归一为友好冲突错误(admin 映射 409)
			if uerr := normalizeUniqueErr(err, fmt.Sprintf("prefix %q already used by group", g.Prefix)); uerr != err {
				return uerr
			}
			return fmt.Errorf("persist group: %w", err)
		}
	} else {
		if _, err := r.db.ExecContext(ctx, `UPDATE model_groups SET prefix=?, params_json=?, enabled=?,
			updated_at=datetime('now') WHERE id=?`, g.Prefix, string(paramsRaw), g.Enabled, g.ID); err != nil {
			return fmt.Errorf("update group: %w", err)
		}
	}
	r.mu.Lock()
	r.byGroup[g.ID] = &Group{ID: g.ID, Prefix: g.Prefix, Plugin: g.Plugin, Params: cloneParams(g.Params), Enabled: g.Enabled}
	old := r.instCache[g.ID]
	delete(r.instCache, g.ID)
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	return nil
}

// ValidateMember 成员保存期校验(组存在;组内短名唯一)
func (r *Registry) ValidateMember(m *Member) error {
	if m.Name == "" {
		return fmt.Errorf("name required")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.byGroup[m.GroupID]; !ok {
		return fmt.Errorf("%w: group %d not found", ErrNotFound, m.GroupID)
	}
	return nil
}

// SaveMember 保存成员。ID=0 插入((group,name) 冲突报错);ID>0 更新(拒绝改 group/name)。
func (r *Registry) SaveMember(ctx context.Context, m *Member) error {
	if err := r.ValidateMember(m); err != nil {
		return err
	}
	r.mu.RLock()
	if m.ID == 0 {
		for _, other := range r.byMember {
			if other.GroupID == m.GroupID && other.Name == m.Name {
				r.mu.RUnlock()
				return fmt.Errorf("%w: member %q already exists in group %d", ErrConflict, m.Name, m.GroupID)
			}
		}
	} else if cur, ok := r.byMember[m.ID]; !ok {
		r.mu.RUnlock()
		return fmt.Errorf("%w: member %d not found", ErrNotFound, m.ID)
	} else if cur.GroupID != m.GroupID || cur.Name != m.Name {
		r.mu.RUnlock()
		return fmt.Errorf("group/name are immutable on member")
	}
	r.mu.RUnlock()
	if m.ID == 0 {
		res := r.db.QueryRowContext(ctx, `INSERT INTO model_members(group_id, name, enabled) VALUES(?,?,?) RETURNING id`,
			m.GroupID, m.Name, m.Enabled)
		if err := res.Scan(&m.ID); err != nil {
			// 并发下同 (group,name) 落库兜底,归一为友好冲突错误(admin 409 / discover skip 依赖此文案)
			if uerr := normalizeUniqueErr(err, fmt.Sprintf("member %q already exists in group %d", m.Name, m.GroupID)); uerr != err {
				return uerr
			}
			return fmt.Errorf("persist member: %w", err)
		}
	} else {
		if _, err := r.db.ExecContext(ctx, `UPDATE model_members SET enabled=?, updated_at=datetime('now') WHERE id=?`,
			m.Enabled, m.ID); err != nil {
			return fmt.Errorf("update member: %w", err)
		}
	}
	r.mu.Lock()
	r.indexMember(&Member{ID: m.ID, GroupID: m.GroupID, Name: m.Name, Enabled: m.Enabled})
	r.mu.Unlock()
	return nil
}

// DeleteGroup 删除组(事务内先删成员再删组;内存索引、部件缓存与轮询计数级联清理)
func (r *Registry) DeleteGroup(ctx context.Context, id int64) error {
	r.mu.RLock()
	_, ok := r.byGroup[id]
	memberNames := make([]string, 0, len(r.memberOrder[id]))
	for _, mid := range r.memberOrder[id] {
		memberNames = append(memberNames, r.byMember[mid].Name)
	}
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: group %d not found", ErrNotFound, id)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete group: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM model_members WHERE group_id=?`, id); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete group members: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM model_groups WHERE id=?`, id); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete group: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete group: %w", err)
	}
	r.mu.Lock()
	delete(r.byGroup, id)
	ids := r.memberOrder[id]
	delete(r.memberOrder, id)
	for _, mid := range ids {
		delete(r.byMember, mid)
	}
	old := r.instCache[id]
	delete(r.instCache, id)
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	// 轮询计数器:该短名已无任何组命中时清理,防无限增长
	for _, name := range memberNames {
		r.cleanupRound(name)
	}
	return nil
}

// cleanupRound 短名已无任何组命中时移除轮询计数器
func (r *Registry) cleanupRound(name string) {
	if r.pollNameCount(name) > 0 {
		return
	}
	r.mu.Lock()
	delete(r.rrRound, name)
	r.mu.Unlock()
}

// pollNameCount 与 PollGroupCount 同口径,但不区分启用态(清理判定用)
func (r *Registry) pollNameCount(name string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, ids := range r.memberOrder {
		for _, mid := range ids {
			if r.byMember[mid].Name == name {
				n++
				break
			}
		}
	}
	return n
}

// DeleteMember 删除成员
func (r *Registry) DeleteMember(ctx context.Context, id int64) error {
	r.mu.RLock()
	m, ok := r.byMember[id]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: member %d not found", ErrNotFound, id)
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM model_members WHERE id=?`, id); err != nil {
		return fmt.Errorf("delete member: %w", err)
	}
	r.mu.Lock()
	delete(r.byMember, id)
	order := r.memberOrder[m.GroupID]
	for i, mid := range order {
		if mid == id {
			r.memberOrder[m.GroupID] = append(order[:i:i], order[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	r.cleanupRound(m.Name)
	return nil
}

// 哨兵错误:调用方(admin 路由层)用 errors.Is 分类 HTTP 状态码,禁止按错误文案字符串匹配
var (
	// ErrNotFound 目标不存在(→ 404)
	ErrNotFound = errors.New("not found")
	// ErrConflict 唯一键冲突(→ 409)
	ErrConflict = errors.New("conflict")
)

// normalizeUniqueErr SQLite UNIQUE 冲突 → ErrConflict 包装(并发检查-写入窗口兜底;非冲突原样返回)
func normalizeUniqueErr(err error, friendly string) error {
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return fmt.Errorf("%w: %s", ErrConflict, friendly)
	}
	return err
}

// GetGroup 取组(深拷贝)
func (r *Registry) GetGroup(id int64) (*Group, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.byGroup[id]
	if !ok {
		return nil, fmt.Errorf("%w: group %d not found", ErrNotFound, id)
	}
	return copyGroup(g), nil
}

// GetMember 取成员(深拷贝)
func (r *Registry) GetMember(id int64) (*Member, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byMember[id]
	if !ok {
		return nil, fmt.Errorf("member %d not found", id)
	}
	c := *m
	return &c, nil
}

// ListGroups 全部组(深拷贝,稳定 ID 序)
func (r *Registry) ListGroups() []*Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Group, 0, len(r.byGroup))
	ids := make([]int64, 0, len(r.byGroup))
	for id := range r.byGroup {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		out = append(out, copyGroup(r.byGroup[id]))
	}
	return out
}

// ListMembers 组内成员(深拷贝,稳定 ID 序)
func (r *Registry) ListMembers(groupID int64) []*Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.memberOrder[groupID]
	out := make([]*Member, 0, len(ids))
	for _, id := range ids {
		c := *r.byMember[id]
		out = append(out, &c)
	}
	return out
}

// DeclaredProtocol 组主包声明的协议全名(实例缓存命中即免查包表;缓存未命中回退包表)
func (r *Registry) DeclaredProtocol(g *Group) string {
	r.mu.RLock()
	if c := r.instCache[g.ID]; c != nil {
		d := c.Declared
		r.mu.RUnlock()
		return d
	}
	r.mu.RUnlock()
	return r.pkgs.DeclaredProtocol(g.Plugin)
}

// Pick 路由解析(R1-R7):
// ① 全路径:请求名以某启用组前缀 + "/" 开头,最长前缀优先 → 该组成员(短名不存在 → NoModel);
// ② 短名:启用组内有同名成员,唯一 → 直达;歧义 → 组间轮询(内存计数器,重启归零)取一组;
// ③ 零命中 → PickNoModel。命中后协议/能力/形态过滤,全滤 → PickCapability(逐组给原因)。
func (r *Registry) Pick(model string, features []Feature, entry string, stream bool) ([]Candidate, PickError, error) {
	r.mu.RLock()
	group, member, poll := r.resolve(model)
	r.mu.RUnlock()
	if group == nil {
		return nil, PickNoModel, PickNoModel
	}
	// 轮询计数在锁外推进(nextRound 内部加写锁,持读锁调用会自锁)
	if poll > 1 {
		group, member = r.rotate(model, group, member)
	}
	cand := Candidate{Group: group, Member: member}
	if reason, ok := r.mismatchReason(&cand, features, entry, stream); !ok {
		return nil, PickCapability, fmt.Errorf("%w: model %s: %s", PickCapability, model, reason)
	}
	return []Candidate{cand}, pickOK, nil
}

// resolve 请求名 → 命中组/成员与歧义组数(调用方持读锁;返回深拷贝;未命中返回 nil,nil,0)
func (r *Registry) resolve(model string) (*Group, *Member, int) {
	// ① 全路径:按声明前缀匹配(禁止按 "/" 切分——成员名可含 "/")
	best := int64(-1)
	var prefixGroup *Group
	for _, g := range r.byGroup {
		if !g.Enabled {
			continue
		}
		if !strings.HasPrefix(model, g.Prefix+"/") {
			continue
		}
		if int64(len(g.Prefix)) > best {
			best = int64(len(g.Prefix))
			prefixGroup = g
		}
	}
	if prefixGroup != nil {
		short := model[len(prefixGroup.Prefix)+1:]
		for _, mid := range r.memberOrder[prefixGroup.ID] {
			if m := r.byMember[mid]; m.Name == short {
				return copyGroup(prefixGroup), copyMember(m), 1
			}
		}
		// 全路径精确点名不存在的成员:声明性问题归 NoModel(404),与"组存在但全被滤"区分
		return nil, nil, 0
	}
	// ② 短名:收集启用组内同名成员
	var hits []*Member
	var hitGroups []*Group
	for gid, ids := range r.memberOrder {
		g := r.byGroup[gid]
		if !g.Enabled {
			continue
		}
		for _, mid := range ids {
			if m := r.byMember[mid]; m.Name == model {
				hits = append(hits, m)
				hitGroups = append(hitGroups, g)
			}
		}
	}
	if len(hits) == 0 {
		return nil, nil, 0
	}
	// 组间稳定序(按组 ID)保证轮询均匀;歧义时由 Pick 在锁外 rotate
	order := make([]int, len(hits))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return hitGroups[order[i]].ID < hitGroups[order[j]].ID })
	pick := order[0]
	return copyGroup(hitGroups[pick]), copyMember(hits[pick]), len(order)
}

// rotate 短名歧义轮询:按计数器在命中组序列中取第 n 组(锁外调用)
func (r *Registry) rotate(model string, g *Group, m *Member) (*Group, *Member) {
	n := int(r.nextRound(model))
	// 重新收集(锁内快照;组集合可能已变,漂移时退回首组)
	r.mu.RLock()
	defer r.mu.RUnlock()
	type hit struct {
		g *Group
		m *Member
	}
	var hits []hit
	for gid, ids := range r.memberOrder {
		grp := r.byGroup[gid]
		if !grp.Enabled {
			continue
		}
		for _, mid := range ids {
			if mem := r.byMember[mid]; mem.Name == model {
				hits = append(hits, hit{grp, mem})
			}
		}
	}
	if len(hits) == 0 {
		return g, m
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].g.ID < hits[j].g.ID })
	pick := hits[n%len(hits)]
	return copyGroup(pick.g), copyMember(pick.m)
}

// nextRound 短名轮询计数(懒建;溢出回绕无害)
func (r *Registry) nextRound(name string) uint64 {
	r.mu.RLock()
	c := r.rrRound[name]
	r.mu.RUnlock()
	if c != nil {
		return c.Add(1)
	}
	r.mu.Lock()
	c = r.rrRound[name]
	if c == nil {
		c = &atomic.Uint64{}
		r.rrRound[name] = c
	}
	r.mu.Unlock()
	return c.Add(1)
}

// mismatchReason 候选被滤原因(可服务=false 时返回 reason;文案含 member@组前缀 定位)
func (r *Registry) mismatchReason(c *Candidate, features []Feature, entry string, stream bool) (string, bool) {
	tag := c.Member.Name + "@" + c.Group.Prefix
	if !c.Member.Enabled {
		return tag + " member disabled", false
	}
	pluginName := c.Group.Plugin
	if !r.pkgs.IsEnabled(pluginName) {
		return tag + " package disabled", false
	}
	pkg, err := r.pkgs.GetPackage(pluginName)
	if err != nil {
		return tag + " package not loadable", false
	}
	// handler 包:声明 = 入口协议;形态两种均支持,能力由作者自保证(壳不声明 features)
	if pkg.Manifest.Parts.Handler != "" {
		if pkg.Manifest.Parts.Handler != entry {
			return tag + " declares " + pkg.Manifest.Parts.Handler, false
		}
		return "", true
	}
	d := pkg.Manifest.Parts.Protocol
	if d == nil {
		return tag + " package not loadable", false
	}
	if d.Protocol != entry {
		return tag + " declares " + d.Protocol, false
	}
	// 形态由实现推导(内置走工厂实例 Supports;不可判定如实说明,不虚构"only")
	supports := r.pkgs.ProtocolSupports(pluginName)
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

// Resolve 组 → 管道产物(部件池请求持有;release 必须在响应完全写完后调用)
func (r *Registry) Resolve(g *Group) (pipeline.Resolved, func(), error) {
	parts, release, err := r.cachedParts(g)
	if err != nil {
		return pipeline.Resolved{}, nil, err
	}
	return pipeline.Resolved{
		Filters:  parts.Filters,
		Protocol: parts.Protocol,
	}, release, nil
}

// cachedParts 取部件实例(请求持有 +1;缓存键=组 ID,revision/插件参数版本/fingerprint 失效)
// 旧实例 evict 后引用计数归零销毁池(在途请求不受影响)
func (r *Registry) cachedParts(g *Group) (*resolvedParts, func(), error) {
	r.mu.RLock()
	cached := r.instCache[g.ID]
	r.mu.RUnlock()
	if cached != nil && cached.acquire() {
		if cached.matches(r.pkgs, g) {
			return cached, cached.release, nil
		}
		cached.release() // 有效实例但配置漂移:释放走重建
	}
	parts, err := r.instantiateParts(g)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	old := r.instCache[g.ID]
	r.instCache[g.ID] = parts
	r.mu.Unlock()
	if old != nil {
		go old.evict()
	}
	if !parts.acquire() { // 刚构建不可能死亡;防御式
		return nil, nil, fmt.Errorf("parts evicted during instantiate")
	}
	return parts, parts.release, nil
}

// matches 缓存是否仍然有效(包 revision + 插件参数版本 + 组参数指纹一致)
func (c *resolvedParts) matches(pkgs *plugin.Registry, g *Group) bool {
	if pkgs.Revision(g.Plugin) != c.baseRev || pkgs.Settings().View(g.Plugin).Version != c.settingsVer {
		return false
	}
	return c.fingerprint == fingerprint(g)
}

// instantiateParts 实例化:Filters(包内序)+ Protocol(内置工厂优先);
// 参数闭包 = 声明 ⊕ 插件参数 ⊕ 组覆盖(请求期语义:未知键剥离)
func (r *Registry) instantiateParts(g *Group) (*resolvedParts, error) {
	pkg, err := r.pkgs.GetPackage(g.Plugin)
	if err != nil {
		return nil, err
	}
	decl := pkg.Declaration
	if decl == nil {
		decl = map[string]any{}
	}
	pluginVals := map[string]any{}
	if cfg, ok := r.pkgs.SettingsOverrides(g.Plugin)["config"].(map[string]any); ok {
		pluginVals = cfg
	}
	// 请求期:未知键剥离不阻断流量(剥离记日志,残留键来自包升级删槽)
	for k := range g.Params {
		if _, known := decl[k]; !known {
			slog.Warn("params: unknown param stripped (not declared by package)", "group", g.Prefix, "param", k, "plugin", g.Plugin)
		}
	}
	merged, err := plugin.ResolveParams(decl, pluginVals, g.Params, plugin.ParamRequest)
	if err != nil {
		return nil, err
	}
	parts := &resolvedParts{fingerprint: fingerprint(g), settingsVer: r.pkgs.Settings().View(g.Plugin).Version}
	parts.refs.Store(1) // 缓存持有份
	parts.baseRev = r.pkgs.Revision(g.Plugin)
	r.mu.RLock()
	transportEvict := r.transportEvict
	r.mu.RUnlock()
	var filters []pipeline.Filter
	for _, fp := range pkg.Manifest.Parts.Filters {
		f, err := plugin.NewFilter(pkg, fp, cloneParams(merged), r.pkgStorage(g.Plugin), r.maskValues(g.Plugin), transportEvict)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	parts.Filters = filters
	// Protocol:同名内置工厂优先(server 装配注册),handler 包 = 兼容壳(请求分流在 gateway),否则编译主包 JS 部件
	if pkg.Manifest.Parts.Handler != "" {
		parts.Declared = pkg.Manifest.Parts.Handler
	}
	if pkg.Manifest.Parts.Protocol != nil {
		parts.Declared = pkg.Manifest.Parts.Protocol.Protocol
	}
	var proto pipeline.Protocol
	if pkg.Manifest.Parts.Handler != "" {
		proto = plugin.NewHandlerShell(pkg.Manifest.Parts.Handler)
	} else if factory, ok := r.pkgs.BuiltinFactory(g.Plugin); ok {
		proto, err = factory(plugin.BuiltinDeps{Config: cloneParams(merged), PackageKey: r.currentKeyData(g.Plugin)})
		if err != nil {
			return nil, err
		}
	} else {
		proto, err = plugin.NewProtocol(pkg, cloneParams(merged), r.pkgStorage(g.Plugin), r.maskValues(g.Plugin), transportEvict)
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

// EvictPackageSettings 插件参数保存后失效引用该包的组缓存(参数烘焙进部件,改值必须重建)
func (r *Registry) EvictPackageSettings(pkgName string) {
	r.mu.Lock()
	var olds []*resolvedParts
	for gid, g := range r.byGroup {
		if g.Plugin != pkgName {
			continue
		}
		if c, ok := r.instCache[gid]; ok {
			delete(r.instCache, gid)
			olds = append(olds, c)
		}
	}
	r.mu.Unlock()
	for _, old := range olds {
		go old.evict()
	}
}

// PollGroupCount 短名在启用组中的命中组数(发现预览标注用;0=无歧义或无命中)
func (r *Registry) PollGroupCount(short string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for gid, ids := range r.memberOrder {
		if !r.byGroup[gid].Enabled {
			continue
		}
		for _, mid := range ids {
			if r.byMember[mid].Name == short {
				n++
				break
			}
		}
	}
	return n
}

// copyGroup 深拷贝(管理读取用,防共享指针污染)
func copyGroup(g *Group) *Group {
	c := *g
	c.Params = cloneParams(g.Params)
	return &c
}

func copyMember(m *Member) *Member {
	c := *m
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

// fingerprint 组参数指纹(Save 会清缓存,此处为兜底)
func fingerprint(g *Group) string {
	b, _ := json.Marshal(struct {
		Params map[string]any `json:"p"`
	}{g.Params})
	return hex.EncodeToString(hashing(b))
}

// hashing sha256 摘要
func hashing(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// pkgStorage 部件存储(ns=包名,包间隔离,同包多组共享)
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

// HandlerPackage handler 包查询(分流判定;非 handler 包 = false)
func (r *Registry) HandlerPackage(name string) (*plugin.Package, bool) {
	pkg, err := r.pkgs.GetPackage(name)
	if err != nil || !pkg.HasHandler() {
		return nil, false
	}
	return pkg, true
}

// MergedParams 组参数合并结果(声明 ⊕ 插件参数 ⊕ 组覆盖;handler ctx.params 同源)
func (r *Registry) MergedParams(g *Group) (map[string]any, error) {
	return r.resolveParams(g)
}

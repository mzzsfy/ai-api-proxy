package plugin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// BuiltinFactory Go 内置协议工厂(参数与密钥与 JS 部件同机制注入)
type BuiltinFactory func(deps BuiltinDeps) (pipeline.Protocol, error)

// BuiltinDeps 内置协议依赖(v2:解析后包参数 + 当前键 data 读值)
type BuiltinDeps struct {
	// Config 解析后的包参数(default ⊕ 插件参数 ⊕ 模型覆盖)
	Config map[string]any
	// PackageKey 当前键 data 只读(请求级选键后;string = 本身,map 取 api_key)
	PackageKey func() (any, bool)
}

// Registry 包注册中心(安装/升级/启停/列举)
type Registry struct {
	mu       sync.RWMutex
	db       *sql.DB
	pkgs     map[string]*Package
	disabled map[string]bool
	revs     map[string]int64
	builtins map[string]BuiltinFactory
	keys     *KeysStore
	settings *SettingsStore
	deps     HooksDeps // 声明提取依赖(装配根注入;nil = 提取用空依赖)
	// supportsCache 协议形态/能力探测缓存(值=条目携带探测所基于的 revision;Install/Delete/RegisterBuiltin 主动失效兜底)
	supportsCache map[string]*supportsEntry
	// OnLoad 包加载完成回调(导入/升级/启用;宿主注入执行 hooks.onLoad)
	OnLoad func(pkg *Package)
	// OnChange 任务声明变化回调(安装/启停/删除;宿主注入刷新调度)
	OnChange func()
	// NextChain next 自调度链宿主钩子(装配根注入;安装/升级/启用/重启 → 链重置)
	NextChain func(pkgName string)
}

// NewRegistry 构造
func NewRegistry(db *sql.DB) *Registry {
	return &Registry{
		db:            db,
		pkgs:          map[string]*Package{},
		disabled:      map[string]bool{},
		revs:          map[string]int64{},
		builtins:      map[string]BuiltinFactory{},
		supportsCache: map[string]*supportsEntry{},
		keys:          NewKeysStore(db),
		settings:      NewSettingsStore(db),
	}
}

// SetHooksDeps 注入声明提取依赖(装配根;nil = 空依赖)
func (r *Registry) SetHooksDeps(deps HooksDeps) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deps = deps
}

// RegisterBuiltin 注册 Go 内置协议(装配根调用;同名单价优先于 JS 部件)
func (r *Registry) RegisterBuiltin(name string, factory BuiltinFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builtins[name] = factory
	delete(r.supportsCache, name)
}

// BuiltinFactory 取内置协议工厂
func (r *Registry) BuiltinFactory(name string) (BuiltinFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.builtins[name]
	return f, ok
}

// LoadFromDB 启动时从库恢复(含禁用行与 revision + declaration_json)
func (r *Registry) LoadFromDB(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx, `SELECT name, manifest_json, parts_json, revision, enabled, declaration_json FROM packages`)
	if err != nil {
		return fmt.Errorf("query packages: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		var name, manifestRaw, partsRaw, declRaw string
		var revision int64
		var enabled bool
		if err := rows.Scan(&name, &manifestRaw, &partsRaw, &revision, &enabled, &declRaw); err != nil {
			return fmt.Errorf("scan package: %w", err)
		}
		m := &Manifest{}
		if err := json.Unmarshal([]byte(manifestRaw), m); err != nil {
			continue
		}
		files := map[string][]byte{}
		_ = json.Unmarshal([]byte(partsRaw), &files)
		decl := map[string]any{}
		_ = json.Unmarshal([]byte(declRaw), &decl)
		r.pkgs[name] = &Package{Manifest: m, Files: files, Revision: revision, Declaration: decl}
		r.revs[name] = revision
		if !enabled {
			r.disabled[name] = true
		}
	}
	return rows.Err()
}

// extractDeclaration 求值全部族文件收集 settings 片段(安装/升级/在线保存共用;nil deps = 空)
func extractDeclaration(pkg *Package, deps *HooksDeps) (map[string]any, error) {
	if deps == nil {
		deps = &HooksDeps{PackageName: pkg.Manifest.Name}
	}
	return ExtractDeclaration(pkg, *deps)
}

// Install 安装(zip 字节);同 name = 升级(revision 自增)
// 编排:结构/编译校验 → 声明提取(拒装点)→ 升级快照 → 落库(declaration_json 同事务)→ 内存替换 → onLoad/onChange/next 链
func (r *Registry) Install(ctx context.Context, data []byte) error {
	pkg, err := ParseAAP(data)
	if err != nil {
		return err
	}
	if err := r.validateParts(pkg); err != nil {
		return err
	}
	// 声明提取对全部包执行(v2 声明统一:protocol/filter 文件的 settings 片段同样参与)
	r.mu.RLock()
	deps := r.deps
	r.mu.RUnlock()
	decl, err := extractDeclaration(pkg, &deps)
	if err != nil {
		return fmt.Errorf("declaration: %w", err)
	}
	pkg.Declaration = decl
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.pkgs[pkg.Manifest.Name]
	prevRev := int64(0)
	if exists {
		prevRev = old.Revision
	}
	// 键保留:升级不动键行(无快照;prev 由首次写路径天然覆盖)
	pkg.Revision = prevRev + 1
	manifestRaw, _ := json.Marshal(pkg.Manifest)
	filesRaw, _ := json.Marshal(pkg.Files)
	declRaw, _ := json.Marshal(pkg.Declaration)
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO packages(name, manifest_json, parts_json, revision, enabled, declaration_json) VALUES(?,?,?,?,1,?)
		 ON CONFLICT(name) DO UPDATE SET manifest_json=excluded.manifest_json, parts_json=excluded.parts_json,
		 revision=excluded.revision, enabled=1, declaration_json=excluded.declaration_json, updated_at=datetime('now')`,
		pkg.Manifest.Name, string(manifestRaw), string(filesRaw), pkg.Revision, string(declRaw))
	if err != nil {
		return fmt.Errorf("persist package: %w", err)
	}
	r.pkgs[pkg.Manifest.Name] = pkg
	r.revs[pkg.Manifest.Name] = pkg.Revision
	r.keys.ResetRotation(pkg.Manifest.Name)
	delete(r.disabled, pkg.Manifest.Name)
	delete(r.supportsCache, pkg.Manifest.Name)
	if r.OnLoad != nil {
		go r.OnLoad(pkg)
	}
	if r.OnChange != nil {
		go r.OnChange()
	}
	if r.NextChain != nil {
		go r.NextChain(pkg.Manifest.Name)
	}
	return nil
}

// Inspect 解析并校验包字节但不安装(导入摘要确认;返回 manifest 摘要)
func (r *Registry) Inspect(data []byte) (map[string]any, error) {
	pkg, err := ParseAAP(data)
	if err != nil {
		return nil, err
	}
	if err := r.validateParts(pkg); err != nil {
		return nil, err
	}
	m := pkg.Manifest
	out := map[string]any{
		"name":        m.Name,
		"title":       m.MetaTitle(),
		"description": m.Description,
		"version":     m.Version,
		"filters":     filterNames(m),
	}
	if p := m.Parts.Protocol; p != nil {
		out["protocol"] = p.Protocol
		out["features"] = p.Features
	}
	if h := m.Parts.Hooks; h != nil {
		tasks := make([]map[string]any, 0, len(h.Tasks))
		for _, tk := range h.Tasks {
			tasks = append(tasks, map[string]any{"name": tk.Name, "cron": tk.Cron, "next": tk.Next})
		}
		out["hooks"] = map[string]any{"tasks": tasks}
	}
	r.mu.RLock()
	if old, ok := r.pkgs[m.Name]; ok {
		out["exists"] = true
		out["installedVersion"] = old.Manifest.Version
	} else {
		out["exists"] = false
	}
	r.mu.RUnlock()
	return out, nil
}

// filterNames 包内 filter 部件名列表
func filterNames(m *Manifest) []string {
	names := make([]string, 0, len(m.Parts.Filters))
	for _, fp := range m.Parts.Filters {
		names = append(names, fp.Name)
	}
	return names
}

// validateParts 逐部件实例化校验(结构/绑定;参数校验归调用方 ResolveParams,漂移按请求期 502)
func (r *Registry) validateParts(pkg *Package) error {
	if _, builtin := r.builtins[pkg.Manifest.Name]; builtin {
		return nil
	}
	if p := pkg.Manifest.Parts.Protocol; p != nil {
		if _, err := buildProtocol(pkg, nil, nil, nil, nil, false); err != nil {
			return err
		}
	}
	for _, fp := range pkg.Manifest.Parts.Filters {
		if _, err := buildFilter(pkg, fp, nil, nil, nil, nil, false); err != nil {
			return err
		}
	}
	// hooks 部件:安装期编译校验全部族文件(声明求值在 Install 编排)
	if pkg.Manifest.Parts.Hooks != nil {
		for _, f := range familyFiles(pkg) {
			if _, err := Compile(pkg.Files[f], f); err != nil {
				return err
			}
		}
	}
	return nil
}

// Enable 启停;包保留在内存(enabled 标记),可重新启用
func (r *Registry) Enable(ctx context.Context, name string, on bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pkgs[name]; !ok {
		return fmt.Errorf("package %q not found", name)
	}
	if _, err := r.db.ExecContext(ctx, `UPDATE packages SET enabled=?, updated_at=datetime('now') WHERE name=?`, on, name); err != nil {
		return fmt.Errorf("enable package: %w", err)
	}
	if on {
		delete(r.disabled, name)
	} else {
		r.disabled[name] = true
	}
	if on && r.OnLoad != nil {
		pkg := r.pkgs[name]
		go r.OnLoad(pkg)
	}
	if r.OnChange != nil {
		go r.OnChange()
	}
	if on && r.NextChain != nil {
		go r.NextChain(name)
	}
	return nil
}

// GetPackage 取包(禁用包同样可见,由调用方按 enabled 语义处理)
func (r *Registry) GetPackage(name string) (*Package, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pkgs[name]
	if !ok {
		return nil, fmt.Errorf("package %q not found", name)
	}
	return p, nil
}

// IsEnabled 包是否启用
func (r *Registry) IsEnabled(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.disabled[name]
}

// DeclaredProtocol 包主包声明的协议全名(无 protocol 部件或包缺失 = 空)
func (r *Registry) DeclaredProtocol(name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pkgs[name]
	if !ok || p.Manifest.Parts.Protocol == nil {
		return ""
	}
	return p.Manifest.Parts.Protocol.Protocol
}

// supportsEntry 形态缓存条目(pkg 指针判定防 Delete+重装 ABA(revision 可重置,指针恒新);rev 冗余校验)
type supportsEntry struct {
	pkg *Package
	rev int64
	s   *pipeline.Supports
}

// ProtocolSupports 包协议的形态/能力支持(内置走工厂实例;JS 包实现推导:mapEvent=流式 mapResponse=非流式;不可判定 nil)
// 探测含 JS 编译+VM 求值,结果按包缓存;条目与当前包指针/revision 不符即重探(升级后自愈,不依赖失效钩子)
func (r *Registry) ProtocolSupports(name string) *pipeline.Supports {
	r.mu.RLock()
	p, ok := r.pkgs[name]
	curRev := r.revs[name]
	if !ok || p.Manifest.Parts.Protocol == nil {
		r.mu.RUnlock()
		return nil
	}
	if e, hit := r.supportsCache[name]; hit && e.pkg == p && e.rev == curRev {
		r.mu.RUnlock()
		return e.s
	}
	r.mu.RUnlock()

	rev := p.Revision // 锁外探测基于的版本;写回时携带
	s := r.probeSupports(name, p)
	r.mu.Lock()
	r.supportsCache[name] = &supportsEntry{pkg: p, rev: rev, s: s}
	r.mu.Unlock()
	return s
}

// probeSupports 实际探测(锁外执行,避免 JS 求值持锁;内置注册仅装配期,直读 map 无竞争面)
func (r *Registry) probeSupports(name string, p *Package) *pipeline.Supports {
	if factory, isBuiltin := r.builtins[name]; isBuiltin {
		proto, err := factory(BuiltinDeps{})
		if err != nil {
			return nil
		}
		s := proto.Supports()
		return &s
	}
	src, ok := p.Files[ProtocolEntry]
	if !ok {
		return nil
	}
	prog, err := Compile(src, ProtocolEntry)
	if err != nil {
		return nil
	}
	hooks, err := ProbeHooks(prog)
	if err != nil {
		return nil
	}
	s := pipeline.Supports{Features: p.Manifest.Parts.Protocol.Features}
	if hooks.MapEvent != nil {
		s.Forms = append(s.Forms, string(FormStreaming))
	}
	if hooks.MapResponse != nil {
		s.Forms = append(s.Forms, string(FormNonStreaming))
	}
	return &s
}

// Export 导出 .aap 字节(manifest+parts 重打包;secrets 在 upstream 层,包内天然无凭据)
func (r *Registry) Export(name string) ([]byte, error) {
	r.mu.RLock()
	pkg, ok := r.pkgs[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("package %q not found", name)
	}
	return BuildAAP(pkg.Manifest, pkg.Files)
}

// Delete 卸载包(数据库+内存;引用检查归调用方——registry 不知上游层)
func (r *Registry) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pkgs[name]; !ok {
		return fmt.Errorf("package %q not found", name)
	}
	if _, builtinPkg := r.builtins[name]; builtinPkg {
		return fmt.Errorf("builtin package %q cannot be deleted", name)
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM packages WHERE name=?`, name); err != nil {
		return fmt.Errorf("delete package row: %w", err)
	}
	delete(r.pkgs, name)
	delete(r.revs, name)
	delete(r.disabled, name)
	delete(r.supportsCache, name)
	r.keys.DeletePackage(name)
	r.settings.Delete(name)
	if r.OnChange != nil {
		go r.OnChange()
	}
	return nil
}

// Keys 包级 key 存储(单键行;轮询调度共用)
func (r *Registry) Keys() *KeysStore { return r.keys }

// Settings 包级 settings 存储
func (r *Registry) Settings() *SettingsStore { return r.settings }

// SettingsOverrides 包的 overrides 原始文档(无定制 = 空)
func (r *Registry) SettingsOverrides(pkgName string) map[string]any {
	return r.settings.overrides(pkgName)
}

// DeleteSettings 卸载清理 overrides
func (r *Registry) DeleteSettings(name string) { r.settings.Delete(name) }

// Revision 包当前 revision(未知包 0)
func (r *Registry) Revision(name string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revs[name]
}

// mustJSON 序列化(失败回退空对象)
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ListPackages 包名清单(稳定序)
func (r *Registry) ListPackages() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.pkgs))
	for n := range r.pkgs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

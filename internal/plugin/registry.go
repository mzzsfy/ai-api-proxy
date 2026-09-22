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

// BuiltinFactory Go 内置协议工厂(secrets 与 JS 部件同机制注入)
type BuiltinFactory func(deps BuiltinDeps) (pipeline.Protocol, error)

// BuiltinDeps 内置协议依赖
type BuiltinDeps struct {
	// TargetSecrets 按 (target 名, 键) 解析凭据
	TargetSecrets func(target, key string) (string, bool)
}

// Registry 包注册中心(安装/升级/启停/列举)
type Registry struct {
	mu        sync.RWMutex
	db        *sql.DB
	pkgs      map[string]*Package
	disabled  map[string]bool
	revs      map[string]int64
	builtins  map[string]BuiltinFactory
	keys      *KeysStore
	settings  *SettingsStore
	deps      HooksDeps // 声明提取依赖(装配根注入;nil = 提取用空依赖)
	// OnLoad 包加载完成回调(导入/升级/启用;宿主注入执行 hooks.onLoad;previous=升级前 keys)
	OnLoad func(pkg *Package, previous map[string]any)
	// OnChange 任务声明变化回调(安装/启停/删除;宿主注入刷新调度)
	OnChange func()
	// NextChain next 自调度链宿主钩子(装配根注入;安装/升级/启用/重启 → 链重置)
	NextChain func(pkgName string)
}

// NewRegistry 构造
func NewRegistry(db *sql.DB) *Registry {
	return &Registry{
		db:       db,
		pkgs:     map[string]*Package{},
		disabled: map[string]bool{},
		revs:     map[string]int64{},
		builtins: map[string]BuiltinFactory{},
		keys:     NewKeysStore(db),
		settings: NewSettingsStore(db),
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
	if pkg.Manifest.Parts.Hooks != nil {
		r.mu.RLock()
		deps := r.deps
		r.mu.RUnlock()
		decl, err := extractDeclaration(pkg, &deps)
		if err != nil {
			return fmt.Errorf("declaration: %w", err)
		}
		pkg.Declaration = decl
	} else {
		pkg.Declaration = map[string]any{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.pkgs[pkg.Manifest.Name]
	prevRev := int64(0)
	if exists {
		prevRev = old.Revision
		// 升级快照:旧 current 移入 previous,current 原样保留(不丢弃用户既有 key)
		if err := r.keys.UpgradeSnapshot(pkg.Manifest.Name); err != nil {
			return fmt.Errorf("upgrade keys snapshot: %w", err)
		}
	}
	var previous map[string]any
	if exists {
		previous, _ = r.keys.PreviousAll(pkg.Manifest.Name)
	}
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
	delete(r.disabled, pkg.Manifest.Name)
	if r.OnLoad != nil {
		go r.OnLoad(pkg, previous)
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
		"name":       m.Name,
		"title":      m.MetaTitle(),
		"description": m.Description,
		"version":    m.Version,
		"filters":    filterNames(m),
		"secretRefs": pkg.SecretRefsUnion(),
	}
	if p := m.Parts.Protocol; p != nil {
		out["protocol"] = p.Protocol
		out["form"] = p.Form
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

// validateParts 逐部件实例化校验(结构/绑定;不查实例配置 schema——配置归上游,漂移按请求期 502)
func (r *Registry) validateParts(pkg *Package) error {
	if _, builtin := r.builtins[pkg.Manifest.Name]; builtin {
		return nil
	}
	if p := pkg.Manifest.Parts.Protocol; p != nil {
		if _, err := buildProtocol(pkg, nil, nil, nil, nil, nil, nil, false); err != nil {
			return err
		}
	}
	for _, fp := range pkg.Manifest.Parts.Filters {
		if _, err := buildFilter(pkg, fp, nil, nil, nil, nil, nil, nil, false); err != nil {
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
		go r.OnLoad(pkg, nil)
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

// GetPart 读部件源码(kind=protocol|filter;filter 需 partName)
func (r *Registry) GetPart(pkgName, kind, partName string) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pkg, ok := r.pkgs[pkgName]
	if !ok {
		return nil, fmt.Errorf("package %q not found", pkgName)
	}
	entry, err := partEntry(pkg, kind, partName)
	if err != nil {
		return nil, err
	}
	src, ok := pkg.Files[entry]
	if !ok {
		return nil, fmt.Errorf("entry %q missing in %q", entry, pkgName)
	}
	return src, nil
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
	r.keys.Delete(name)
	r.settings.Delete(name)
	if r.OnChange != nil {
		go r.OnChange()
	}
	return nil
}

// Keys 包级 key 存储(util.key 读取与升级快照共用)
func (r *Registry) Keys() *KeysStore { return r.keys }

// Settings 包级 settings 存储
func (r *Registry) Settings() *SettingsStore { return r.settings }

// SettingsOverrides 包的 overrides 原始文档(无定制 = 空)
func (r *Registry) SettingsOverrides(pkgName string) map[string]any {
	return r.settings.overrides(pkgName)
}

// DeleteSettings 卸载清理 overrides
func (r *Registry) DeleteSettings(name string) { r.settings.Delete(name) }

// KeyReader util.key 读取闭包(实时读当前值)
func (r *Registry) KeyReader(pkgName string) func(name string) (any, bool) {
	return func(name string) (any, bool) { return r.keys.Get(pkgName, name) }
}

// partEntry 按 kind/名定位部件 entry(hooks kind:name 即包内路径)
func partEntry(pkg *Package, kind, partName string) (string, error) {
	switch kind {
	case "protocol":
		if pkg.Manifest.Parts.Protocol == nil {
			return "", fmt.Errorf("package %q has no protocol part", pkg.Manifest.Name)
		}
		return pkg.Manifest.Parts.Protocol.Entry, nil
	case "filter":
		for _, fp := range pkg.Manifest.Parts.Filters {
			if fp.Name == partName {
				return fp.Entry, nil
			}
		}
		return "", fmt.Errorf("filter %q not found in %q", partName, pkg.Manifest.Name)
	case "hooks":
		if _, ok := pkg.Files[partName]; !ok {
			return "", fmt.Errorf("hooks file %q not found in %q", partName, pkg.Manifest.Name)
		}
		return partName, nil
	default:
		return "", fmt.Errorf("kind must be protocol|filter|hooks")
	}
}

// Revision 包当前 revision(未知包 0)
func (r *Registry) Revision(name string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revs[name]
}

// UpdatePart 替换部件源码并整包升级(revision+1;entry 定位,编译校验失败拒绝)
// hooks 族文件保存 → 声明重提取(失败 = 400 拒保存);protocol/filter 部件编译校验在下次实例化
func (r *Registry) UpdatePart(ctx context.Context, pkgName, kind, partName string, code []byte) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pkg, ok := r.pkgs[pkgName]
	if !ok {
		return 0, fmt.Errorf("package %q not found", pkgName)
	}
	files := make(map[string][]byte, len(pkg.Files))
	for k, v := range pkg.Files {
		files[k] = v
	}
	entry, err := partEntry(pkg, kind, partName)
	if err != nil {
		return 0, err
	}
	if kind == "hooks" {
		entry = partName // hooks kind:partName 即包内路径(init.js/keys.js/tasks/<n>.js)
		if _, ok := files[entry]; !ok {
			return 0, fmt.Errorf("hooks file %q not found in %q", entry, pkgName)
		}
	}
	files[entry] = code
	// 保存前编译校验(坏代码拒绝,防任务全挂)
	if _, err := Compile(code, entry); err != nil {
		return 0, err
	}
	updated := &Package{Manifest: pkg.Manifest, Files: files, Revision: pkg.Revision + 1}
	if updated.Manifest.Parts.Hooks != nil {
		deps := r.deps
		decl, err := extractDeclaration(updated, &deps)
		if err != nil {
			return 0, fmt.Errorf("declaration: %w", err)
		}
		updated.Declaration = decl
	} else {
		updated.Declaration = pkg.Declaration
	}
	filesRaw, _ := json.Marshal(updated.Files)
	if _, err := r.db.ExecContext(ctx,
		`UPDATE packages SET parts_json=?, declaration_json=?, revision=?, updated_at=datetime('now') WHERE name=?`,
		string(filesRaw), mustJSON(updated.Declaration), updated.Revision, pkgName); err != nil {
		return 0, fmt.Errorf("persist update: %w", err)
	}
	r.pkgs[pkgName] = updated
	r.revs[pkgName] = updated.Revision
	return updated.Revision, nil
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

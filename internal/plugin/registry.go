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
	mu       sync.RWMutex
	db       *sql.DB
	pkgs     map[string]*Package
	disabled map[string]bool
	revs     map[string]int64
	builtins map[string]BuiltinFactory
}

// NewRegistry 构造
func NewRegistry(db *sql.DB) *Registry {
	return &Registry{
		db:       db,
		pkgs:     map[string]*Package{},
		disabled: map[string]bool{},
		revs:     map[string]int64{},
		builtins: map[string]BuiltinFactory{},
	}
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

// LoadFromDB 启动时从库恢复(含禁用行与 revision)
func (r *Registry) LoadFromDB(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx, `SELECT name, manifest_json, parts_json, revision, enabled FROM packages`)
	if err != nil {
		return fmt.Errorf("query packages: %w", err)
	}
	defer rows.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for rows.Next() {
		var name, manifestRaw, partsRaw string
		var revision int64
		var enabled bool
		if err := rows.Scan(&name, &manifestRaw, &partsRaw, &revision, &enabled); err != nil {
			return fmt.Errorf("scan package: %w", err)
		}
		m := &Manifest{}
		if err := json.Unmarshal([]byte(manifestRaw), m); err != nil {
			continue
		}
		files := map[string][]byte{}
		_ = json.Unmarshal([]byte(partsRaw), &files)
		r.pkgs[name] = &Package{Manifest: m, Files: files, Revision: revision}
		r.revs[name] = revision
		if !enabled {
			r.disabled[name] = true
		}
	}
	return rows.Err()
}

// Install 安装(zip 字节);同 name = 升级(revision 自增)
// 安装期校验:结构 + 编译(报行号)+ 声明↔实现双向绑定(实例化校验,拒绝坏包落库)
func (r *Registry) Install(ctx context.Context, data []byte) error {
	pkg, err := ParseAAP(data)
	if err != nil {
		return err
	}
	if err := r.validateParts(pkg); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.pkgs[pkg.Manifest.Name]
	prevRev := int64(0)
	if exists {
		prevRev = old.Revision
	}
	pkg.Revision = prevRev + 1
	manifestRaw, _ := json.Marshal(pkg.Manifest)
	filesRaw, _ := json.Marshal(pkg.Files)
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO packages(name, manifest_json, parts_json, revision, enabled) VALUES(?,?,?,?,1)
		 ON CONFLICT(name) DO UPDATE SET manifest_json=excluded.manifest_json, parts_json=excluded.parts_json,
		 revision=excluded.revision, enabled=1, updated_at=datetime('now')`,
		pkg.Manifest.Name, string(manifestRaw), string(filesRaw), pkg.Revision)
	if err != nil {
		return fmt.Errorf("persist package: %w", err)
	}
	r.pkgs[pkg.Manifest.Name] = pkg
	r.revs[pkg.Manifest.Name] = pkg.Revision
	delete(r.disabled, pkg.Manifest.Name)
	return nil
}

// validateParts 逐部件实例化校验(结构/绑定;不查实例配置 schema——配置归上游,漂移按请求期 502)
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
	return nil
}

// partEntry 按 kind/名定位部件 entry
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
	default:
		return "", fmt.Errorf("kind must be protocol|filter")
	}
}

// Revision 包当前 revision(未知包 0)
func (r *Registry) Revision(name string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revs[name]
}

// UpdatePart 替换部件源码并整包升级(revision+1;entry 定位,编译校验在下次实例化时进行)
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
	files[entry] = code
	// 保存前编译校验(坏代码拒绝,防引用上游全量 502)
	if _, err := Compile(code, entry); err != nil {
		return 0, err
	}
	updated := &Package{Manifest: pkg.Manifest, Files: files, Revision: pkg.Revision + 1}
	filesRaw, _ := json.Marshal(updated.Files)
	if _, err := r.db.ExecContext(ctx,
		`UPDATE packages SET parts_json=?, revision=?, updated_at=datetime('now') WHERE name=?`,
		string(filesRaw), updated.Revision, pkgName); err != nil {
		return 0, fmt.Errorf("persist update: %w", err)
	}
	r.pkgs[pkgName] = updated
	r.revs[pkgName] = updated.Revision
	return updated.Revision, nil
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

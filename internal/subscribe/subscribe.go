// Package subscribe git 仓库订阅同步引擎(clone/fetch + PackDir + Registry.Install)
package subscribe

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/mzzsfy/ai-api-proxy/internal/ipguard"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/store"
)

// Store 订阅持久化依赖(消费侧最小接口)
type Store interface {
	GetSubscription(ctx context.Context, id int64) (*store.Subscription, error)
	DeleteSubscription(ctx context.Context, id int64) error
	UpdateSubscriptionSync(ctx context.Context, id int64, branch, commit string) error
	UpdateSubscriptionError(ctx context.Context, id int64, msg string) error
}

// Packages 包注册中心依赖(消费侧最小接口)
type Packages interface {
	Install(ctx context.Context, data []byte, source string) error
	Delete(ctx context.Context, name string) error
	ListPackages() []string
	PackageSource(name string) (string, bool)
}

// Engine 订阅同步引擎
type Engine struct {
	Store    Store
	Packages Packages
	WorkDir  string // 订阅克隆工作区根(如 data/subscriptions)
	// CheckURL URL 级出站校验(创建与同步共用);nil = CheckPublicURL
	CheckURL func(repoURL string) error

	// syncMu 同步/删除全局串行(手动低频操作;防同订阅并发 fetch/reset 交错与 delete 竞态)
	syncMu sync.Mutex
}

// SourceOf 订阅名 → 包来源标记
func SourceOf(subName string) string { return "subscription/" + subName }

// Sync 同步单条订阅:校验 → 准备工作区 → 幂等导入 → 更新同步态
func (e *Engine) Sync(ctx context.Context, id int64) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	sub, err := e.Store.GetSubscription(ctx, id)
	if err != nil {
		return err
	}
	err = e.sync(ctx, sub)
	if err != nil {
		msg := maskToken(err.Error(), sub.Token)
		_ = e.Store.UpdateSubscriptionError(ctx, id, msg)
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// sync 同步主体(错误原样上抛,token 掩码归 Sync)
func (e *Engine) sync(ctx context.Context, sub *store.Subscription) error {
	check := e.CheckURL
	if check == nil {
		check = CheckPublicURL
	}
	if err := check(sub.RepoURL); err != nil {
		return fmt.Errorf("repo url rejected: %w", err)
	}
	workdir := e.workdir(sub.ID)
	pkgDir, err := safePkgDir(workdir, sub.Path)
	if err != nil {
		return err
	}
	branch, commit, err := e.prepare(workdir, sub)
	if err != nil {
		return err
	}
	// manifest 缺失属上游删除 path:报错但不动本地包
	if _, err := os.Stat(filepath.Join(pkgDir, "manifest.json")); err != nil {
		return fmt.Errorf("manifest missing at %s", sub.Path)
	}
	if commit == sub.LastSyncedCommit {
		// 无变化跳过导入,但成功语义仍要落地(清历史 last_error / 刷新时间)
		return e.Store.UpdateSubscriptionSync(ctx, sub.ID, branch, commit)
	}
	data, err := plugin.PackDir(pkgDir)
	if err != nil {
		return fmt.Errorf("pack dir: %w", err)
	}
	parsed, err := plugin.ParseAAP(data)
	if err != nil {
		return fmt.Errorf("parse packed: %w", err)
	}
	source := SourceOf(sub.Name)
	if err := e.Packages.Install(ctx, data, source); err != nil {
		return err
	}
	e.uninstallRenamed(ctx, source, parsed.Manifest.Name)
	return e.Store.UpdateSubscriptionSync(ctx, sub.ID, branch, commit)
}

// prepare 工作区就绪:首克隆(记录默认分支)或 fetch+reset 到记录分支;返回分支与远端头 commit
func (e *Engine) prepare(workdir string, sub *store.Subscription) (branch, commit string, err error) {
	_, statErr := os.Stat(workdir)
	// branch 未知但目录残留:状态不可信,整体重克隆
	if statErr == nil && sub.Branch == "" {
		if err := os.RemoveAll(workdir); err != nil {
			return "", "", fmt.Errorf("reset stale workdir: %w", err)
		}
		statErr = os.ErrNotExist
	}
	if statErr != nil {
		if err := os.MkdirAll(filepath.Dir(workdir), 0o755); err != nil {
			return "", "", fmt.Errorf("mkdir workdir parent: %w", err)
		}
		remote, err := authURL(sub)
		if err != nil {
			return "", "", err
		}
		if _, err := git("", "clone", "--depth=1", remote, workdir); err != nil {
			return "", "", fmt.Errorf("clone: %w", err)
		}
		b, err := git(workdir, "symbolic-ref", "--short", "HEAD")
		if err != nil {
			return "", "", fmt.Errorf("detect branch: %w", err)
		}
		branch = strings.TrimSpace(b)
	} else {
		branch = sub.Branch
		if _, err := git(workdir, "fetch", "origin"); err != nil {
			return "", "", fmt.Errorf("fetch: %w", err)
		}
		if _, err := git(workdir, "reset", "--hard", "origin/"+branch); err != nil {
			return "", "", fmt.Errorf("reset: %w", err)
		}
	}
	c, err := git(workdir, "rev-parse", "origin/"+branch)
	if err != nil {
		return "", "", fmt.Errorf("remote head: %w", err)
	}
	return branch, strings.TrimSpace(c), nil
}

// uninstallRenamed 卸载本订阅来源下、与本次导入不同名的旧包(上游改名;组引用等卸载失败仅告警,由用户手动清理)
func (e *Engine) uninstallRenamed(ctx context.Context, source, keep string) {
	for _, name := range e.Packages.ListPackages() {
		if name == keep {
			continue
		}
		if src, ok := e.Packages.PackageSource(name); ok && src == source {
			if err := e.Packages.Delete(ctx, name); err != nil {
				log.Printf("subscribe: uninstall renamed %s: %v (left for manual cleanup)", name, err)
			}
		}
	}
}

// Delete 删订阅:卸载其来源包 + 清工作区 + 删行
func (e *Engine) Delete(ctx context.Context, id int64) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	sub, err := e.Store.GetSubscription(ctx, id)
	if err != nil {
		return err
	}
	source := SourceOf(sub.Name)
	for _, name := range e.Packages.ListPackages() {
		if src, ok := e.Packages.PackageSource(name); ok && src == source {
			if err := e.Packages.Delete(ctx, name); err != nil {
				return fmt.Errorf("uninstall %s: %w", name, err)
			}
		}
	}
	if err := os.RemoveAll(e.workdir(id)); err != nil {
		return fmt.Errorf("remove workdir: %w", err)
	}
	return e.Store.DeleteSubscription(ctx, id)
}

// workdir 单订阅克隆目录
func (e *Engine) workdir(id int64) string {
	return filepath.Join(e.WorkDir, strconv.FormatInt(id, 10))
}

// CheckPath 订阅 path 合法性(空 = ".";必须为相对路径且不穿越;创建入口与同步共用)
func CheckPath(subPath string) error {
	clean := path.Clean("/" + subPath) // 归一为根相对路径,消解 .. 与前导 /
	rel := strings.TrimPrefix(clean, "/")
	if rel == "" || rel == "." {
		return nil
	}
	if strings.HasPrefix(rel, "../") || rel == ".." {
		return fmt.Errorf("subscription path escapes workdir: %s", subPath)
	}
	return nil
}

// safePkgDir 订阅 path → 工作区内绝对目录(防穿越:解析后仍在工作区内)
func safePkgDir(workdir, subPath string) (string, error) {
	if err := CheckPath(subPath); err != nil {
		return "", err
	}
	clean := path.Clean("/" + subPath)
	rel := strings.TrimPrefix(clean, "/")
	dir := workdir
	if rel != "" && rel != "." {
		dir = filepath.Join(workdir, filepath.FromSlash(rel))
	}
	absRoot, err := filepath.Abs(workdir)
	if err != nil {
		return "", fmt.Errorf("abs workdir: %w", err)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("abs pkgdir: %w", err)
	}
	relBack, err := filepath.Rel(absRoot, absDir)
	if err != nil || strings.HasPrefix(relBack, "..") {
		return "", fmt.Errorf("subscription path escapes workdir: %s", subPath)
	}
	return absDir, nil
}

// authURL 认证 URL:https 注入 user:token(percent-encode);其余形态(file/本地路径)原样
func authURL(sub *store.Subscription) (string, error) {
	u, err := url.Parse(sub.RepoURL)
	if err != nil {
		return "", fmt.Errorf("parse repo url: %w", err)
	}
	if u.Scheme != "https" {
		return sub.RepoURL, nil
	}
	if sub.Token != "" {
		u.User = url.UserPassword(sub.User, sub.Token)
	} else if u.User != nil {
		// 用户把凭据粘进了 repo_url:剥掉密码,避免 git 错误回显泄漏(token 字段为空时掩码不可达)
		u.User = url.User(u.User.Username())
	}
	return u.String(), nil
}

// maskToken 错误消息掩码(git 输出常回显含凭据 URL)
func maskToken(msg, token string) string {
	if token == "" {
		return msg
	}
	return strings.ReplaceAll(msg, token, "***")
}

// CheckPublicURL URL 级出站校验:仅 https,域名全部解析 IP 须为公网(git CLI 自行拨号,此为创建/同步前防线)
func CheckPublicURL(repoURL string) error {
	u, err := url.Parse(repoURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("only https repo url allowed")
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		if ipguard.Forbidden(ip) {
			return fmt.Errorf("address %s is not allowed", host)
		}
		return nil
	}
	addrs, err := net.LookupIP(host)
	if err != nil || len(addrs) == 0 {
		return fmt.Errorf("resolve host %s", host)
	}
	for _, ip := range addrs {
		if ipguard.Forbidden(ip) {
			return fmt.Errorf("address %s is not allowed", ip)
		}
	}
	return nil
}

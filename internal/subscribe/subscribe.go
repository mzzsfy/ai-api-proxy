// Package subscribe git 仓库订阅同步引擎(clone/fetch + PackDir + Registry.Install)
package subscribe

import (
	"context"
	"fmt"
	"log/slog"
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
		// 独立 ctx:失败可能由请求 ctx 取消引起,错误态写入不能随之取消
		wctx, cancel := context.WithTimeout(context.Background(), writeErrorTimeout)
		defer cancel()
		_ = e.Store.UpdateSubscriptionError(wctx, id, msg)
		// 脱敏后重构造错误返回(原链可能含 token,不能 %w 透传)
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
	branch, commit, err := e.prepare(ctx, workdir, sub)
	if err != nil {
		return err
	}
	// manifest 缺失属上游删除 path:报错但不动本地包
	if _, err := os.Stat(filepath.Join(pkgDir, "manifest.json")); err != nil {
		return fmt.Errorf("manifest missing at %s", sub.Path)
	}
	// 上游无变化且本订阅来源下仍有包:跳过导入(成功语义落地);包被手动卸载则重新导入
	if commit == sub.LastSyncedCommit && e.sourceCount(SourceOf(sub.Name)) > 0 {
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
func (e *Engine) prepare(ctx context.Context, workdir string, sub *store.Subscription) (branch, commit string, err error) {
	_, statErr := os.Stat(workdir)
	// branch 未知但目录残留:状态不可信,整体重克隆
	if statErr == nil && sub.Branch == "" {
		if err := os.RemoveAll(workdir); err != nil {
			return "", "", fmt.Errorf("reset stale workdir: %w", err)
		}
		statErr = os.ErrNotExist
	}
	if statErr != nil {
		return e.clone(ctx, workdir, sub)
	}
	branch = sub.Branch
	if _, err := git(ctx, workdir, "fetch", "origin"); err != nil {
		return "", "", fmt.Errorf("fetch: %w", err)
	}
	if _, err := git(ctx, workdir, "reset", "--hard", "origin/"+branch); err != nil {
		// 上游默认分支改名自愈:丢弃旧工作区重克隆(重新探测默认分支)
		if rmErr := os.RemoveAll(workdir); rmErr != nil {
			return "", "", fmt.Errorf("heal stale branch (remove workdir): %w", rmErr)
		}
		return e.clone(ctx, workdir, sub)
	}
	c, err := git(ctx, workdir, "rev-parse", "origin/"+branch)
	if err != nil {
		return "", "", fmt.Errorf("remote head: %w", err)
	}
	return branch, strings.TrimSpace(c), nil
}

// clone 首克隆:浅克隆 + 探测默认分支 + 远端头 commit
func (e *Engine) clone(ctx context.Context, workdir string, sub *store.Subscription) (branch, commit string, err error) {
	if err := os.MkdirAll(filepath.Dir(workdir), 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir workdir parent: %w", err)
	}
	remote, err := authURL(sub)
	if err != nil {
		return "", "", err
	}
	if _, err := git(ctx, "", "clone", "--depth=1", remote, workdir); err != nil {
		return "", "", fmt.Errorf("clone: %w", err)
	}
	b, err := git(ctx, workdir, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return "", "", fmt.Errorf("detect branch: %w", err)
	}
	branch = strings.TrimSpace(b)
	c, err := git(ctx, workdir, "rev-parse", "origin/"+branch)
	if err != nil {
		return "", "", fmt.Errorf("remote head: %w", err)
	}
	return branch, strings.TrimSpace(c), nil
}

// SourceCount 来源标记下已装包数(GUI 列表与引擎共用)
func SourceCount(pk Packages, source string) int {
	n := 0
	for _, name := range pk.ListPackages() {
		if src, ok := pk.PackageSource(name); ok && src == source {
			n++
		}
	}
	return n
}

// sourceCount 来源标记下已装包数
func (e *Engine) sourceCount(source string) int {
	return SourceCount(e.Packages, source)
}

// uninstallRenamed 卸载本订阅来源下、与本次导入不同名的旧包(上游改名;组引用等卸载失败仅告警,由用户手动清理)
func (e *Engine) uninstallRenamed(ctx context.Context, source, keep string) {
	for _, name := range e.Packages.ListPackages() {
		if name == keep {
			continue
		}
		if src, ok := e.Packages.PackageSource(name); ok && src == source {
			if err := e.Packages.Delete(ctx, name); err != nil {
				slog.Warn("subscribe: uninstall renamed (left for manual cleanup)", "package", name, "err", err)
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

// CheckPath 订阅 path 合法性(空 = ".";入口预检;POSIX 语义,Windows 反斜杠与穿越由 safePkgDir 的 Rel 复核兜底)
func CheckPath(subPath string) error {
	if strings.Contains(subPath, `\`) {
		return fmt.Errorf("subscription path must use / separator: %s", subPath)
	}
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
		user := sub.User
		if user == "" {
			// 空用户名 + 令牌形态部分服务端拒绝,以惯例占位
			user = "token"
		}
		u.User = url.UserPassword(user, sub.Token)
	} else if u.User != nil {
		// 用户把凭据粘进了 repo_url:剥掉密码,避免 git 错误回显泄漏(token 字段为空时掩码不可达)
		u.User = url.User(u.User.Username())
	}
	return u.String(), nil
}

// maskToken 错误消息掩码(git 输出常回显含凭据 URL;编码形态一并掩码——URL 内凭据为 percent-encode 形式)
func maskToken(msg, token string) string {
	if token == "" {
		return msg
	}
	msg = strings.ReplaceAll(msg, token, "***")
	if esc := url.QueryEscape(token); esc != token {
		msg = strings.ReplaceAll(msg, esc, "***")
	}
	return msg
}

// CheckPublicURL URL 级出站校验:仅 https、禁 URL 内嵌凭据(走表单字段),域名全部解析 IP 须为公网(git CLI 自行拨号,此为创建/同步前防线)
func CheckPublicURL(repoURL string) error {
	u, err := url.Parse(repoURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("only https repo url allowed")
	}
	if u.User != nil {
		return fmt.Errorf("credentials in url not allowed (use user/token fields)")
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

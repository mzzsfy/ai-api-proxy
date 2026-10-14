package admin

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/mzzsfy/ai-api-proxy/internal/store"
	"github.com/mzzsfy/ai-api-proxy/internal/subscribe"
)

// subscriptionNameRe 订阅名字符集(source 标记成分,防可读性歧义)
var subscriptionNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// subscriptionView 列表视图(token 只暴露是否已设置)
func subscriptionView(sub *store.Subscription, packageCount int) map[string]any {
	return map[string]any{
		"id": sub.ID, "name": sub.Name, "repo_url": sub.RepoURL, "user": sub.User,
		"branch": sub.Branch, "path": sub.Path, "token_set": sub.Token != "",
		"package_count": packageCount,
		"last_synced_commit": sub.LastSyncedCommit, "last_synced_at": sub.LastSyncedAt,
		"last_error": sub.LastError,
	}
}

// sourcePackageCount 订阅来源下已装包数
func (d *Deps) sourcePackageCount(subName string) int {
	return subscribe.SourceCount(d.Packages, subscribe.SourceOf(subName))
}

// listSubscriptions 订阅列表
func (d *Deps) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	if d.SubscriptionStore == nil {
		httpError(w, http.StatusNotImplemented, "subscriptions disabled")
		return
	}
	subs, err := d.SubscriptionStore.ListSubscriptions(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(subs))
	for _, sub := range subs {
		out = append(out, subscriptionView(sub, d.sourcePackageCount(sub.Name)))
	}
	writeJSON(w, out)
}

// createSubscription 建订阅(name/repo_url 必填;repo_url 公网校验;path 相对路径)
func (d *Deps) createSubscription(w http.ResponseWriter, r *http.Request) {
	if d.SubscriptionStore == nil {
		httpError(w, http.StatusNotImplemented, "subscriptions disabled")
		return
	}
	var req struct {
		Name    string `json:"name"`
		RepoURL string `json:"repo_url"`
		User    string `json:"user"`
		Path    string `json:"path"`
		Token   string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad request")
		return
	}
	if req.Name == "" || req.RepoURL == "" {
		httpError(w, http.StatusBadRequest, "name and repo_url required")
		return
	}
	// name 进入 source 标记(subscription/<name>),限可读字符集
	if !subscriptionNameRe.MatchString(req.Name) {
		httpError(w, http.StatusBadRequest, "name: letters/digits/-/_ only, must start with letter or digit")
		return
	}
	check := d.SubscriptionCheck
	if check == nil {
		check = subscribe.CheckPublicURL
	}
	if err := check(req.RepoURL); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := subscribe.CheckPath(req.Path); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	sub := &store.Subscription{Name: req.Name, RepoURL: req.RepoURL, User: req.User, Path: req.Path, Token: req.Token}
	id, err := d.SubscriptionStore.CreateSubscription(r.Context(), sub)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "id": id})
}

// deleteSubscription 删订阅(联动卸载来源包;被渠道引用的包先阻塞,409 提示先解绑)
func (d *Deps) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	if d.Subscriptions == nil || d.SubscriptionStore == nil {
		httpError(w, http.StatusNotImplemented, "subscriptions disabled")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	ctx := r.Context()
	sub, err := d.SubscriptionStore.GetSubscription(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, "subscription not found")
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	source := subscribe.SourceOf(sub.Name)
	for _, name := range d.Packages.ListPackages() {
		if src, ok := d.Packages.PackageSource(name); ok && src == source {
			if refs := d.groupRefs(name); len(refs) > 0 {
				httpError(w, http.StatusConflict, "包 "+name+" 被以下渠道引用: "+strings.Join(refs, ","))
				return
			}
		}
	}
	if err := d.Subscriptions.Delete(ctx, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, "subscription not found")
			return
		}
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// syncSubscription 手动同步订阅(错误详情已脱敏)
func (d *Deps) syncSubscription(w http.ResponseWriter, r *http.Request) {
	if d.Subscriptions == nil {
		httpError(w, http.StatusNotImplemented, "subscriptions disabled")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad id")
		return
	}
	if err := d.Subscriptions.Sync(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, "subscription not found")
			return
		}
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotFound 目标行不存在
var ErrNotFound = errors.New("not found")

// Subscription git 仓库订阅条目
type Subscription struct {
	ID               int64
	Name             string
	RepoURL          string
	User             string
	Branch           string
	Path             string
	Token            string
	LastSyncedCommit string
	LastSyncedAt     string
	LastError        string
}

// CreateSubscription 建订阅(name 冲突报错),返回自增 id
func (s *Store) CreateSubscription(ctx context.Context, sub *Subscription) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO subscriptions(name, repo_url, user, path, token) VALUES(?,?,?,?,?)`,
		sub.Name, sub.RepoURL, sub.User, sub.Path, sub.Token)
	if err != nil {
		return 0, fmt.Errorf("create subscription: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("create subscription id: %w", err)
	}
	return id, nil
}

const subscriptionCols = `id, name, repo_url, user, branch, path, token, last_synced_commit, last_synced_at, last_error`

// scanSubscription 行绑定
func scanSubscription(scan func(...any) error) (*Subscription, error) {
	sub := &Subscription{}
	if err := scan(&sub.ID, &sub.Name, &sub.RepoURL, &sub.User, &sub.Branch, &sub.Path,
		&sub.Token, &sub.LastSyncedCommit, &sub.LastSyncedAt, &sub.LastError); err != nil {
		return nil, err
	}
	return sub, nil
}

// ListSubscriptions 全量列表(按 name 稳定序)
func (s *Store) ListSubscriptions(ctx context.Context) ([]*Subscription, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+subscriptionCols+` FROM subscriptions ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()
	out := []*Subscription{}
	for rows.Next() {
		sub, err := scanSubscription(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// GetSubscription 单读
func (s *Store) GetSubscription(ctx context.Context, id int64) (*Subscription, error) {
	sub, err := scanSubscription(s.db.QueryRowContext(ctx,
		`SELECT `+subscriptionCols+` FROM subscriptions WHERE id=?`, id).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get subscription: %w", err)
	}
	return sub, nil
}

// DeleteSubscription 删订阅(幂等;联动卸载与工作区清理归调用方)
func (s *Store) DeleteSubscription(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM subscriptions WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}
	return nil
}

// UpdateSubscriptionSync 成功态:branch/commit/时间写入,错误清空
func (s *Store) UpdateSubscriptionSync(ctx context.Context, id int64, branch, commit string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE subscriptions SET branch=?, last_synced_commit=?, last_synced_at=datetime('now'), last_error='' WHERE id=?`,
		branch, commit, id)
	if err != nil {
		return fmt.Errorf("update subscription sync: %w", err)
	}
	return nil
}

// UpdateSubscriptionError 失败态:记录错误(同步态保留)
func (s *Store) UpdateSubscriptionError(ctx context.Context, id int64, msg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE subscriptions SET last_error=? WHERE id=?`, msg, id)
	if err != nil {
		return fmt.Errorf("update subscription error: %w", err)
	}
	return nil
}

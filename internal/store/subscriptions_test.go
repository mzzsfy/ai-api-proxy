package store

import (
	"context"
	"errors"
	"testing"
)

// newSub 测试订阅模板(逐字段显式赋值,断言可证伪)
func newSub(name string) *Subscription {
	return &Subscription{
		Name:    name,
		RepoURL: "https://git.example.com/org/repo",
		User:    "deploy",
		Path:    "plugins/my-pkg",
		Token:   "pat-secret",
	}
}

func TestSubscription_CRUD(t *testing.T) {
	// Given 干净库 When 建订阅读列表/单读/删除 Then 字段往返一致
	s := newTestStore(t)
	ctx := context.Background()

	id, err := s.CreateSubscription(ctx, newSub("repo-a"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id <= 0 {
		t.Fatalf("id must be positive: %d", id)
	}

	subs, err := s.ListSubscriptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 1 || subs[0].Name != "repo-a" || subs[0].User != "deploy" ||
		subs[0].RepoURL != "https://git.example.com/org/repo" || subs[0].Path != "plugins/my-pkg" {
		t.Fatalf("list roundtrip: %+v", subs)
	}

	got, err := s.GetSubscription(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Token != "pat-secret" || got.Branch != "" || got.LastError != "" {
		t.Fatalf("get roundtrip: %+v", got)
	}

	if err := s.DeleteSubscription(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSubscription(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expect ErrNotFound after delete, got %v", err)
	}
	// 幂等删除:不存在的 id 不报错
	if err := s.DeleteSubscription(ctx, id); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestSubscription_NameUnique(t *testing.T) {
	// Given 已存在 name=repo-a When 再建同名 Then 报错
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.CreateSubscription(ctx, newSub("repo-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSubscription(ctx, newSub("repo-a")); err == nil {
		t.Fatal("duplicate name must fail")
	}
}

func TestSubscription_UpdateSync(t *testing.T) {
	// Given 订阅带历史错误 When 同步成功 Then branch/commit/时间写入且错误清空
	s := newTestStore(t)
	ctx := context.Background()
	id, err := s.CreateSubscription(ctx, newSub("repo-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSubscriptionError(ctx, id, "fetch failed"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSubscriptionSync(ctx, id, "main", "abc123"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSubscription(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Branch != "main" || got.LastSyncedCommit != "abc123" {
		t.Fatalf("sync state: %+v", got)
	}
	if got.LastError != "" {
		t.Fatalf("error must be cleared, got %q", got.LastError)
	}
	if got.LastSyncedAt == "" {
		t.Fatal("last_synced_at must be set")
	}
}

func TestSubscription_UpdateError(t *testing.T) {
	// Given 同步成功的订阅 When 写错误 Then last_error 记录且同步态不变
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.CreateSubscription(ctx, newSub("repo-a"))
	if err := s.UpdateSubscriptionSync(ctx, id, "main", "abc123"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSubscriptionError(ctx, id, "boom"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetSubscription(ctx, id)
	if got.LastError != "boom" || got.LastSyncedCommit != "abc123" || got.Branch != "main" {
		t.Fatalf("error state: %+v", got)
	}
}

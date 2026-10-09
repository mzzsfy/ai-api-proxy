package store

import (
	"context"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func TestMigrate_Idempotent(t *testing.T) {
	// Given 已建 schema 的库 When 重复 Migrate Then 幂等且表可写
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO packages(name, manifest_json, parts_json) VALUES('p', '{}', '{}')`); err != nil {
		t.Fatalf("insert after re-migrate: %v", err)
	}
}

func TestMigrate_ReopenIdempotent(t *testing.T) {
	// Given 同一库文件重开 When 再次 Migrate Then 无错(终态 DDL 全 IF NOT EXISTS)
	dir := t.TempDir()
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = s1.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	if err := s2.Migrate(context.Background()); err != nil {
		t.Fatalf("resume migrate: %v", err)
	}
}

func TestSchema_TerminalTables(t *testing.T) {
	// Given 终态 schema When 写各表典型行 Then 全部可写(表集完整性)
	s := newTestStore(t)
	ctx := context.Background()
	stmts := []string{
		`INSERT INTO packages(name, manifest_json, parts_json) VALUES('p', '{}', '{}')`,
		`INSERT INTO package_keys(name, data_json, prev_json) VALUES('k', '{}', NULL)`,
		`INSERT INTO package_settings(name) VALUES('p')`,
		`INSERT INTO metrics_minutely(minute) VALUES('2024-01-01T00:00')`,
		`INSERT INTO model_groups(prefix, base_package) VALUES('gw', 'p')`,
		`INSERT INTO model_members(group_id, name) VALUES(1, 'm1')`,
		`INSERT INTO subscriptions(name, repo_url) VALUES('s', 'https://x')`,
	}
	for _, stmt := range stmts {
		if _, err := s.DB().ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var pkgJSON string
	if err := s.DB().QueryRowContext(ctx,
		`SELECT by_package_json FROM metrics_minutely WHERE minute='2024-01-01T00:00'`).Scan(&pkgJSON); err != nil || pkgJSON != "{}" {
		t.Fatalf("metrics default: %q err=%v", pkgJSON, err)
	}
}

func TestKV_NamespaceIsolation(t *testing.T) {
	// Given 两个 ns 写同名键 When 互读 Then 互不可见
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.KVSet(ctx, "pkg-a", "token", "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.KVSet(ctx, "pkg-b", "token", "B"); err != nil {
		t.Fatal(err)
	}
	va, okA, _ := s.KVGet(ctx, "pkg-a", "token")
	vb, okB, _ := s.KVGet(ctx, "pkg-b", "token")
	_, okC, _ := s.KVGet(ctx, "pkg-c", "token")
	if va != "A" || vb != "B" || okA != true || okB != true || okC != false {
		t.Fatalf("isolation broken: a=%q b=%q c-ok=%v", va, vb, okC)
	}
	if err := s.KVDelete(ctx, "pkg-a", "token"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.KVGet(ctx, "pkg-a", "token"); ok {
		t.Fatal("delete failed")
	}
}

func TestKV_ConcurrentWriteNoBusy(t *testing.T) {
	// Given 并发 KV 写(单写连接下实际串行) When 并发执行 Then 调用面无竞态数据错误(WAL 多连接场景由部署层验证)
	s := newTestStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 10 {
				if err := s.KVSet(ctx, "load", string(rune('a'+i)), string(rune('a'+j))); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent kv: %v", err)
	}
}

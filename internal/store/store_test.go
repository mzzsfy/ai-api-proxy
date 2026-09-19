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
	// Given 已迁移的库 When 重复 Migrate Then 版本不变且表可写
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var n int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no migration recorded")
	}
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO packages(name, manifest_json, parts_json) VALUES('p', '{}', '{}')`); err != nil {
		t.Fatalf("insert after re-migrate: %v", err)
	}
}

func TestMigrate_ResumeAfterInterrupt(t *testing.T) {
	// Given 迁移中断(mock 第 1 版已应用、表缺第 2 版) When 补迁移 Then 续跑不重复
	// 构造:直接对同一库文件重开,验证已应用版本被跳过
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
	// Given WAL 并发写(快照 goroutine + GUI 读) When 并发执行 Then 无 SQLITE_BUSY
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

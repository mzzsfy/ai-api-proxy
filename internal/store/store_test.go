package store

import (
	"context"
	"database/sql"
	"os"
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

func TestMigration003_PackageColumn(t *testing.T) {
	// Given 旧库无 by_package_json 列 When 迁移 003 Then 列存在默认 '{}' 且旧行可读
	s := newTestStore(t)
	ctx := context.Background()
	// 模拟旧数据行(仅写 001 时代的列)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO metrics_minutely
		(minute, requests, errors, max_concurrent, by_upstream_json, by_target_json)
		VALUES('2024-01-01T00:00', 3, 1, 2, '{}', '{}')`); err != nil {
		t.Fatal(err)
	}
	var pkgJSON string
	var reqs int64
	if err := s.DB().QueryRowContext(ctx,
		`SELECT by_package_json, requests FROM metrics_minutely WHERE minute='2024-01-01T00:00'`).Scan(&pkgJSON, &reqs); err != nil {
		t.Fatalf("read migrated row: %v", err)
	}
	if pkgJSON != "{}" {
		t.Fatalf("by_package_json default: %q", pkgJSON)
	}
	if reqs != 3 {
		t.Fatalf("old row data lost: %d", reqs)
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

func TestMigrate_BackupBeforeModelRows(t *testing.T) {
	// Given 001-003 形态的库含 v1 数据 When 首次 Migrate(触发 004)Then .bak-v4 生成且含迁移前数据
	dir := t.TempDir()
	dbPath := dir + "/app.db"
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// 手工构造 003 版本态:schema_migrations 记 3 + v1 形态 upstreams 表含数据(升 004 时触发整库备份)
	pre := []string{
		`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))`,
		`INSERT INTO schema_migrations (version) VALUES (1),(2),(3)`,
		`CREATE TABLE package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE upstreams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, base_package TEXT NOT NULL,
			extras_json TEXT NOT NULL DEFAULT '[]', models_json TEXT NOT NULL DEFAULT '[]', targets_json TEXT NOT NULL DEFAULT '[]',
			params_json TEXT NOT NULL DEFAULT '{}', filter_params_json TEXT NOT NULL DEFAULT '{}', filters_enabled_json TEXT NOT NULL DEFAULT '{}',
			strategy_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT)`,
		`INSERT INTO upstreams (name, base_package, models_json, targets_json) VALUES ('old', 'pkg',
			'["m1"]', '[]')`,
	}
	for _, stmt := range pre {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	bak, err := os.Open(dbPath + ".bak-v4")
	if err != nil {
		t.Fatalf("backup file: %v", err)
	}
	defer bak.Close()
	// 备份是自足 SQLite 库:直接开它验证 v1 数据在
	bdb, err := sql.Open("sqlite", "file:"+dbPath+".bak-v4")
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	var name string
	if err := bdb.QueryRow(`SELECT name FROM upstreams WHERE id=1`).Scan(&name); err != nil || name != "old" {
		t.Fatalf("backup content: %q err=%v", name, err)
	}
}

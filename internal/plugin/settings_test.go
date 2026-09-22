package plugin

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE package_keys (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE package_settings (name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE packages (name TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, parts_json TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT (datetime('now')), declaration_json TEXT NOT NULL DEFAULT '{}')`,
		`CREATE TABLE kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestBlobStoreSparse(t *testing.T) {
	db := newTestDB(t)
	bs := newBlobStore(db, "package_settings")
	// 稀疏不变式:无定制无行
	if _, ok := bs.load("checkin"); ok {
		t.Fatal("empty package should have no row")
	}
	// 定制即行
	if err := bs.save("checkin", `{"config":{"a":1}}`); err != nil {
		t.Fatal(err)
	}
	raw, ok := bs.load("checkin")
	if !ok || raw != `{"config":{"a":1}}` {
		t.Fatalf("load after save: %q %v", raw, ok)
	}
	var updated int64
	if err := db.QueryRow(`SELECT updated_at FROM package_settings WHERE name='checkin'`).Scan(&updated); err != nil {
		t.Fatal(err)
	}
	if updated == 0 {
		t.Fatal("updated_at should be set on save")
	}
	// 删行
	bs.delete("checkin")
	if _, ok := bs.load("checkin"); ok {
		t.Fatal("row should be deleted")
	}
}

func TestSettingsStorePut(t *testing.T) {
	db := newTestDB(t)
	ss := NewSettingsStore(db)

	// PUT 合法值落层 + version 可用于乐观锁
	up, err := ss.Put("checkin", PutInput{Config: map[string]any{"account": "mine@x.y"}, Tasks: map[string]map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if up.Overrides["config"].(map[string]any)["account"] != "mine@x.y" {
		t.Fatalf("config merged wrong: %v", up.Overrides)
	}
	if up.Version == 0 {
		t.Fatal("version should be set after write")
	}

	// 跳写:与现存储等值 → updatedAt 不变(不消费版本)
	stored := ss.overrides("checkin")
	before := ss.View("checkin").Version
	if err := ss.putIfChanged("checkin", stored); err != nil {
		t.Fatal(err)
	}
	if after := ss.View("checkin").Version; after != before {
		t.Fatalf("skip-write should keep updatedAt: %d -> %d", before, after)
	}

	// 空定制 → 行删除(稀疏不变式)
	if _, err := ss.Put("checkin", PutInput{Config: map[string]any{}, Tasks: map[string]map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := newBlobStore(db, "package_settings").load("checkin"); ok {
		t.Fatal("empty overrides should delete row")
	}
	_ = errors.New
	_ = strings.TrimSpace
}

func TestSettingsStoreLimit(t *testing.T) {
	db := newTestDB(t)
	ss := NewSettingsStore(db)
	big := make([]byte, SettingsLimit)
	for i := range big {
		big[i] = 'x'
	}
	err := ss.putIfChanged("checkin", map[string]any{"config": map[string]any{"blob": string(big)}})
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("want limit error, got %v", err)
	}
}

func TestKeysStoreOnBlobTable(t *testing.T) {
	db := newTestDB(t)
	ks := NewKeysStore(db)
	if err := ks.Merge("checkin", map[string]any{"token": "t1"}); err != nil {
		t.Fatal(err)
	}
	if v, ok := ks.Get("checkin", "token"); !ok || v != "t1" {
		t.Fatalf("get: %v %v", v, ok)
	}
	// 行落在 package_keys 表(kv 无 __keys__ 行)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM package_keys WHERE name='checkin'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("package_keys row: %d %v", n, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM kv WHERE key='__keys__'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("kv should not hold keys: %d %v", n, err)
	}
	// 64KB 上限
	big := strings.Repeat("x", KeysLimit)
	if err := ks.Merge("checkin", map[string]any{"blob": big}); err == nil {
		t.Fatal("want limit error")
	}
	ks.Delete("checkin")
	if _, ok := ks.Get("checkin", "token"); ok {
		t.Fatal("delete failed")
	}
	_ = json.Marshal
}

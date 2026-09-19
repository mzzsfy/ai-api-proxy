// Package store SQLite 唯一持久化出口:连接/迁移/kv
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store 数据库门面
type Store struct {
	db *sql.DB
}

// Open 打开连接并设置 pragma
func Open(dataDir string) (*Store, error) {
	dsn := "file:" + dataDir + "/app.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// 单写连接串行化写事务,避免 SQLITE_BUSY
	db.SetMaxOpenConns(1)
	return &Store{db: db}, nil
}

// Migrate 幂等执行嵌入迁移,版本表记录
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (datetime('now'))
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	var current int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		var version int
		if _, err := fmt.Sscanf(name, "%d", &version); err != nil {
			return fmt.Errorf("bad migration name %q: %w", name, err)
		}
		if version <= current {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", name, err)
		}
		for _, stmt := range splitSQL(string(body)) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply migration %s stmt: %w", name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

// DB 唯一连接出口;事务由调用方管理
func (s *Store) DB() *sql.DB { return s.db }

// KVGet 读 kv 值;不存在返回空串与 false
func (s *Store) KVGet(ctx context.Context, ns, key string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE ns=? AND key=?`, ns, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("kv get %s/%s: %w", ns, key, err)
	}
	return v, true, nil
}

// KVSet 写 kv(upsert)
func (s *Store) KVSet(ctx context.Context, ns, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv(ns, key, value) VALUES(?,?,?)
		 ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`,
		ns, key, value)
	if err != nil {
		return fmt.Errorf("kv set %s/%s: %w", ns, key, err)
	}
	return nil
}

// KVDelete 删 kv
func (s *Store) KVDelete(ctx context.Context, ns, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kv WHERE ns=? AND key=?`, ns, key)
	if err != nil {
		return fmt.Errorf("kv delete %s/%s: %w", ns, key, err)
	}
	return nil
}

// Close 关闭连接
func (s *Store) Close() error { return s.db.Close() }

// splitSQL 按 语句分隔符拆分迁移脚本(迁移文件内禁止在字符串字面量中使用分号)
func splitSQL(body string) []string {
	parts := strings.Split(body, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}

// Package store SQLite 唯一持久化出口:连接/终态 schema/kv
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed migrations/schema.sql
var schemaFS embed.FS

// Store 数据库门面
type Store struct {
	db *sql.DB
}

// Open 打开主库并设置 pragma
func Open(dataDir string) (*Store, error) {
	db, err := OpenDB(dataDir + "/app.db")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

// OpenDB 打开独立 SQLite 连接并设置 pragma(主库之外的单独库共用此出口)
func OpenDB(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// 单写连接串行化写事务,避免 SQLITE_BUSY
	db.SetMaxOpenConns(1)
	return db, nil
}

// Migrate 幂等直建终态 schema(无历史迁移链,不考虑旧库升级)
func (s *Store) Migrate(ctx context.Context) error {
	body, err := schemaFS.ReadFile("migrations/schema.sql")
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	for _, stmt := range splitSQL(string(body)) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema stmt: %w", err)
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

// splitSQL 按语句分隔符拆分迁移脚本(注释行整行剥离——其内分号不代表语句边界;迁移文件内禁止在字符串字面量中使用分号)
func splitSQL(body string) []string {
	var out []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			joined := strings.Join(cur, "\n")
			for _, part := range strings.Split(joined, ";") {
				if strings.TrimSpace(part) != "" {
					out = append(out, part)
				}
			}
			cur = nil
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cur = append(cur, line)
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			flush()
		}
	}
	flush()
	return out
}


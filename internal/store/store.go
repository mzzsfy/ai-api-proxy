// Package store SQLite 唯一持久化出口:连接/迁移/kv
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store 数据库门面
type Store struct {
	db     *sql.DB
	dbPath string // 主库文件路径(破坏性迁移前备份用)
}

// modelRowsMigrationVersion 模型行 v2 迁移版本(改表+清 kv 的破坏性迁移,前置整库备份)
const modelRowsMigrationVersion = 4

// Open 打开主库并设置 pragma
func Open(dataDir string) (*Store, error) {
	dbPath := dataDir + "/app.db"
	db, err := OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	return &Store{db: db, dbPath: dbPath}, nil
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
		// 004(模型行 v2)前的破坏性迁移:先 VACUUM INTO 快照备份,迁移失败可整库回退
		if version == modelRowsMigrationVersion {
			if err := s.backupBeforeV2(ctx, modelRowsMigrationVersion); err != nil {
				return fmt.Errorf("pre-migration backup: %w", err)
			}
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

// backupBeforeV2 破坏性迁移前置备份(VACUUM INTO 单文件快照;已存在则跳过——保留最早一次 v1 态)
// 触发于 004 应用前,此刻 v1 表尚未改名(upstreams_v1 由迁移自身创建),不能以表名探测——无条件备份,
// 空库快照成本可忽略;是否真有 v1 数据由 migrateV1 自行判定
func (s *Store) backupBeforeV2(ctx context.Context, version int) error {
	backupPath := fmt.Sprintf("%s.bak-v%d", s.dbPath, version)
	if _, err := os.Stat(backupPath); err == nil {
		log.Printf("[migrate-v2] backup %s already exists; keeping earliest snapshot", backupPath)
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, backupPath); err != nil {
		_ = os.Remove(backupPath) // 半途失败残留截断文件会被后续 Stat 当作有效快照,必须清理
		return fmt.Errorf("vacuum into %s: %w", backupPath, err)
	}
	log.Printf("[migrate-v2] database backed up to %s", backupPath)
	return nil
}

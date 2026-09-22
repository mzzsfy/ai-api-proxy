// Package history 请求历史:独立 SQLite 存储、条件查询与保留期清理
package history

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/store"
)

// BodyLimit 单条记录请求/响应体的截断上限(字节)
const BodyLimit = 16 * 1024

// 分页边界:缺省页大小与上限
const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// hoursPerDay 一天的小时数(保留期换算)
const hoursPerDay = 24

// maxRetentionDays 保留天数钳制上限(防超大配置经 Duration 换算整型溢出,落得删除全表)
const maxRetentionDays = 36500

// ErrNotFound 历史 id 不存在
var ErrNotFound = errors.New("history not found")

// Entry 单条请求历史
type Entry struct {
	ID           int64  `json:"id"`
	TS           int64  `json:"ts"` // epoch 毫秒
	Method       string `json:"method"`
	Path         string `json:"path"`
	Model        string `json:"model"`
	Upstream     string `json:"upstream"`
	Target       string `json:"target"`
	Status       int    `json:"status"`
	DurationMS   int64  `json:"duration_ms"`
	Stream       bool   `json:"stream"`
	RequestBody  string `json:"request_body,omitempty"`
	ResponseBody string `json:"response_body,omitempty"`
}

// Filter 查询条件(空值 = 不过滤;时间含端点,零值不参与)
type Filter struct {
	Model    string
	Upstream string
	Error    bool  // 仅错误(状态 >= 400)
	Start    int64 // ts >= Start(epoch ms,零值不过滤)
	End      int64 // ts <= End(epoch ms,零值不过滤)
	Limit    int
	Offset   int
}

// Store 请求历史库(独立文件;单写连接由 store.OpenDB 保证)
type Store struct{ db *sql.DB }

// New 打开独立历史库并建表
func New(path string) (*Store, error) {
	db, err := store.OpenDB(path)
	if err != nil {
		return nil, fmt.Errorf("open history db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// migrate 幂等建表与索引(ts 索引服务保留期范围删除)
func (s *Store) migrate() error {
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS request_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ts INTEGER NOT NULL,
			method TEXT NOT NULL,
			path TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			upstream TEXT NOT NULL DEFAULT '',
			target TEXT NOT NULL DEFAULT '',
			status INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			stream INTEGER NOT NULL DEFAULT 0,
			request_body TEXT NOT NULL DEFAULT '',
			response_body TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_request_history_ts ON request_history(ts)`,
	} {
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("migrate history: %w", err)
		}
	}
	return nil
}

// Record 同步落一条历史(超限 body 截断;失败仅日志,不影响请求路径)
func (s *Store) Record(e Entry) {
	if _, err := s.insert(e, time.Now().UnixMilli()); err != nil {
		log.Printf("history record: %v", err)
	}
}

// insert 落库并回填 id;ts 由调用方给定
func (s *Store) insert(e Entry, ts int64) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO request_history
		(ts, method, path, model, upstream, target, status, duration_ms, stream, request_body, response_body)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		ts, e.Method, e.Path, e.Model, e.Upstream, e.Target, e.Status, e.DurationMS, b2i(e.Stream),
		clampBody(e.RequestBody), clampBody(e.ResponseBody))
	if err != nil {
		return 0, fmt.Errorf("insert history: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("history last insert id: %w", err)
	}
	return id, nil
}

// Query 条件分页查询(id 倒序),返回行与过滤后的总数;不含 bodies(详情走 Get)
func (s *Store) Query(ctx context.Context, f Filter) ([]Entry, int, error) {
	where, args := buildWhere(f)
	var total int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM request_history"+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count history: %w", err)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, ts, method, path, model, upstream, target, status, duration_ms, stream
		 FROM request_history`+where+" ORDER BY id DESC LIMIT ? OFFSET ?",
		append(args, limit, offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()
	out := make([]Entry, 0, limit)
	for rows.Next() {
		var e Entry
		var stream int
		if err := rows.Scan(&e.ID, &e.TS, &e.Method, &e.Path, &e.Model, &e.Upstream, &e.Target,
			&e.Status, &e.DurationMS, &stream); err != nil {
			return nil, 0, fmt.Errorf("scan history: %w", err)
		}
		e.Stream = stream != 0
		out = append(out, e)
	}
	return out, total, rows.Err()
}

// Get 按 id 取单条(含 bodies);不存在返回 ErrNotFound
func (s *Store) Get(ctx context.Context, id int64) (Entry, error) {
	var e Entry
	var stream int
	err := s.db.QueryRowContext(ctx,
		`SELECT id, ts, method, path, model, upstream, target, status, duration_ms, stream, request_body, response_body
		 FROM request_history WHERE id = ?`, id).
		Scan(&e.ID, &e.TS, &e.Method, &e.Path, &e.Model, &e.Upstream, &e.Target,
			&e.Status, &e.DurationMS, &stream, &e.RequestBody, &e.ResponseBody)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("get history %d: %w", id, err)
	}
	e.Stream = stream != 0
	return e, nil
}

// Cleanup 删除保留期之前的历史;retentionDays<=0 表示永久保留(不清理)
func (s *Store) Cleanup(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	if retentionDays > maxRetentionDays {
		retentionDays = maxRetentionDays
	}
	cutoff := time.Now().Add(-time.Duration(retentionDays) * hoursPerDay * time.Hour).UnixMilli()
	res, err := s.db.ExecContext(ctx, "DELETE FROM request_history WHERE ts < ?", cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup history: %w", err)
	}
	return res.RowsAffected()
}

// Close 关闭独立库连接
func (s *Store) Close() error { return s.db.Close() }

// buildWhere 过滤条件拼装(参数化;固定顺序保证可测)
func buildWhere(f Filter) (string, []any) {
	where := " WHERE 1=1"
	var args []any
	if f.Model != "" {
		where += " AND model = ?"
		args = append(args, f.Model)
	}
	if f.Upstream != "" {
		where += " AND upstream = ?"
		args = append(args, f.Upstream)
	}
	if f.Error {
		where += " AND status >= ?"
		args = append(args, http.StatusBadRequest)
	}
	if f.Start > 0 {
		where += " AND ts >= ?"
		args = append(args, f.Start)
	}
	if f.End > 0 {
		where += " AND ts <= ?"
		args = append(args, f.End)
	}
	return where, args
}

// clampBody 超限截断(存储上限不变式)
func clampBody(b string) string {
	if len(b) > BodyLimit {
		return b[:BodyLimit]
	}
	return b
}

// b2i 布尔转整数(存储列形态)
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

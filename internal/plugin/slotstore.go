// 权威槽存储:值(kv 表)与权威标记(slot_authority 表)同事务提交,无半态
package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// unlockWriteTimeout unlock 同步写超时(独立于执行 ctx,秒级有界)
const unlockWriteTimeout = 5 * time.Second

// ErrUnlockWrite unlock 同步写失败(白放语义由调用方执行)
var ErrUnlockWrite = errors.New("unlock write failed")

// SlotStore 单包权威槽存储(ns=包名)
type SlotStore struct {
	db *sql.DB
	ns string
}

// NewSlotStore 构造(表须已建)
func NewSlotStore(db *sql.DB, ns string) *SlotStore {
	return &SlotStore{db: db, ns: ns}
}

// GetAuth 权威判定与③值同只读事务快照(值, 是否权威)
func (s *SlotStore) GetAuth(key string) (string, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", false, fmt.Errorf("get auth begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var v string
	err = tx.QueryRow(`SELECT value FROM kv WHERE ns=? AND key=?`, s.ns, key).Scan(&v)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return "", false, fmt.Errorf("get auth read: %w", err)
	}
	var marked int
	err = tx.QueryRow(`SELECT COUNT(*) FROM slot_authority WHERE ns=? AND key=?`, s.ns, key).Scan(&marked)
	if err != nil {
		return "", false, fmt.Errorf("get auth mark: %w", err)
	}
	return v, marked > 0, nil
}

// GetAuthMust 快照读(不存在 false;空串值按存在返回 true 与空串)
func (s *SlotStore) GetAuthMust(key string) (string, bool) {
	v, _, err := s.GetAuth(key)
	if err != nil {
		return "", false
	}
	return v, true
}

// IsAuthoritative 权威标记查询
func (s *SlotStore) IsAuthoritative(key string) (bool, error) {
	var marked int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM slot_authority WHERE ns=? AND key=?`, s.ns, key).Scan(&marked); err != nil {
		return false, fmt.Errorf("authority check %s: %w", key, err)
	}
	return marked > 0, nil
}

// UnlockWrite 值+标记同事务同步写(写超时独立);失败 = 无半落
func (s *SlotStore) UnlockWrite(key, value string) error {
	return s.unlockWriteCtx(context.Background(), key, value, unlockWriteTimeout)
}

func (s *SlotStore) unlockWriteCtx(ctx context.Context, key, value string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: begin: %w", ErrUnlockWrite, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kv(ns, key, value) VALUES(?,?,?)
		ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`, s.ns, key, value); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("%w: value: %w", ErrUnlockWrite, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slot_authority(ns, key) VALUES(?,?)
		ON CONFLICT DO NOTHING`, s.ns, key); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("%w: mark: %w", ErrUnlockWrite, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit: %w", ErrUnlockWrite, err)
	}
	return nil
}

// ExternalWrite 外部写③打标(任务/login/onLoad 通道;与 unlock 落值同事务语义)
func (s *SlotStore) ExternalWrite(key, value string) error {
	return s.UnlockWrite(key, value)
}

// ExternalWritePlain 外部写③不打标(非凭据普通槽)
func (s *SlotStore) ExternalWritePlain(key, value string) error {
	return s.writePlain(key, &value)
}

// Delete 删值(标记独立保留,曾权威槽不退化为普通槽)
func (s *SlotStore) Delete(key string) error {
	_, err := s.db.Exec(`DELETE FROM kv WHERE ns=? AND key=?`, s.ns, key)
	if err != nil {
		return fmt.Errorf("slot delete: %w", err)
	}
	return nil
}

func (s *SlotStore) writePlain(key string, vp *string) error {
	if vp == nil {
		return s.Delete(key)
	}
	_, err := s.db.Exec(`INSERT INTO kv(ns, key, value) VALUES(?,?,?)
		ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`, s.ns, key, *vp)
	if err != nil {
		return fmt.Errorf("slot write: %w", err)
	}
	return nil
}

// Merge ②缓冲整批单事务归并;事务内复核权威标记,权威槽写丢弃并回报
func (s *SlotStore) Merge(ctx context.Context, writes map[string]*string) (dropped []string, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("merge begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	dropped = []string{}
	for k, vp := range writes {
		var marked int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM slot_authority WHERE ns=? AND key=?`, s.ns, k).Scan(&marked); err != nil {
			return nil, fmt.Errorf("merge check %s: %w", k, err)
		}
		if marked > 0 {
			dropped = append(dropped, k)
			continue
		}
		if vp == nil {
			if _, err := tx.Exec(`DELETE FROM kv WHERE ns=? AND key=?`, s.ns, k); err != nil {
				return nil, fmt.Errorf("merge delete %s: %w", k, err)
			}
			continue
		}
		if _, err := tx.Exec(`INSERT INTO kv(ns, key, value) VALUES(?,?,?)
			ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')`, s.ns, k, *vp); err != nil {
			return nil, fmt.Errorf("merge set %s: %w", k, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("merge commit: %w", err)
	}
	return dropped, nil
}

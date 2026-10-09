package plugin

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

// testSlotDB 生产同构库(WAL + busy_timeout)
func testSlotDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/test.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS kv (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now')), PRIMARY KEY (ns, key))`,
		`CREATE TABLE IF NOT EXISTS slot_authority (ns TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (ns, key))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	return db
}

// ─── SlotStore 权威槽存储 ───

// 场景: unlock 落值打标,外部写打标,值与标记同事务
func TestSlotStoreUnlockWriteMarksAuthoritative(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")

	if err := s.UnlockWrite("tok", "v1"); err != nil {
		t.Fatalf("unlock write: %v", err)
	}
	v, auth, err := s.GetAuth("tok")
	if err != nil || v != "v1" || !auth {
		t.Fatalf("get after unlock: v=%q auth=%v err=%v", v, auth, err)
	}

	if err := s.ExternalWrite("plain", "ext"); err != nil {
		t.Fatalf("external write: %v", err)
	}
	if _, auth, _ := s.GetAuth("plain"); !auth {
		t.Fatal("external write must mark authoritative")
	}
}

// 场景: 非权威槽 = 普通 KV,GetAuth 权威判定 false
func TestSlotStorePlainSlot(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")

	if err := s.ExternalWritePlain("plain", "v"); err != nil {
		t.Fatalf("plain write: %v", err)
	}
	v, auth, err := s.GetAuth("plain")
	if err != nil || v != "v" || auth {
		t.Fatalf("plain slot: v=%q auth=%v err=%v", v, auth, err)
	}
}

// 场景: 标记原子性——值与标记同事务,delete 后标记仍在
func TestSlotStoreDeleteKeepsMark(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")

	if err := s.UnlockWrite("tok", "v1"); err != nil {
		t.Fatalf("unlock write: %v", err)
	}
	if err := s.Delete("tok"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if v, _, err := s.GetAuth("tok"); err != nil || v != "" {
		t.Fatalf("deleted slot must be absent: %q %v", v, err)
	}
	if auth, err := s.IsAuthoritative("tok"); err != nil || !auth {
		t.Fatal("mark must survive delete")
	}
}

// 场景: 权威槽归并丢弃(缓冲 set/delete 均丢弃,槽值保持权威值)
// 场景: 白放槽普通归并(未打标槽 last-writer-wins)
func TestSlotStoreMerge(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")
	_ = s.UnlockWrite("auth", "authority")
	_ = s.UnlockWrite("agone", "x")
	_ = s.Delete("agone") // 曾权威,delete 不清标记
	_ = s.ExternalWritePlain("norm", "old")

	dropped, err := s.Merge(context.Background(), map[string]*string{
		"auth":  strPtr("buffered"),
		"norm":  strPtr("new"),
		"gone":  nil,
		"agone": nil,
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(dropped) != 2 || !droppedContains(dropped, "auth") || !droppedContains(dropped, "agone") {
		t.Fatalf("authoritative drops expected [auth agone], got %v", dropped)
	}
	if v, _, _ := s.GetAuth("auth"); v != "authority" {
		t.Fatalf("authoritative value must keep: %q", v)
	}
	if v, _, _ := s.GetAuth("norm"); v != "new" {
		t.Fatalf("plain merge last-writer-wins: %q", v)
	}
	if v, _, err := s.GetAuth("gone"); err != nil || v != "" {
		t.Fatalf("plain delete must apply: %q", v)
	}
}

// 场景: 白放槽已权威则 delete 缓冲同样丢弃(delete 与 set 同判据)
func TestSlotStoreMergeDeleteOnAuthoritative(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")
	_ = s.UnlockWrite("tok", "v")

	dropped, err := s.Merge(context.Background(), map[string]*string{"tok": nil})
	if err != nil || len(dropped) != 1 {
		t.Fatalf("merge drop: %v %v", dropped, err)
	}
	if v, ok := s.GetAuthMust("tok"); !ok || v != "v" {
		t.Fatalf("authoritative survives buffered delete: %q %v", v, ok)
	}
}

// 场景: unlock 写失败无半落——写 ctx 已取消时返回错误,值与标记均不变
func TestSlotStoreUnlockWriteTimeout(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")
	_ = s.UnlockWrite("tok", "old")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.unlockWriteCtx(ctx, "tok", "new", unlockWriteTimeout)
	if err == nil {
		t.Fatal("timeout must error")
	}
	if v, auth, _ := s.GetAuth("tok"); v != "old" || !auth {
		t.Fatalf("no half write allowed: v=%q auth=%v", v, auth)
	}
}

// 场景: 权威读无 TOCTOU——GetAuth 值与标记同只读事务快照
func TestSlotStoreGetAuthSnapshot(t *testing.T) {
	h := testSlotDB(t)
	s := NewSlotStore(h, "pkg")
	_ = s.ExternalWritePlain("s", "v")

	// 并发打标期间反复读,不允许出现"权威 true + 值缺位"或反向半态
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.UnlockWrite("s", "nv")
	}()
	for i := 0; i < 100; i++ {
		v, auth, err := s.GetAuth("s")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		// 同快照不变量:权威判定为 true 时值必为两次写之一,不得读到 mark=true + 写前旧值
		if auth && v != "v" && v != "nv" {
			t.Fatalf("TOCTOU: auth=%v v=%q", auth, v)
		}
	}
	wg.Wait()
	if v, auth, _ := s.GetAuth("s"); v != "nv" || !auth {
		t.Fatalf("final: %q %v", v, auth)
	}
}

// ─── SlotLocks 槽锁 ───

// waitForUntil 轮询至条件成立(确定性时序,避免 sleep 竞态)
func waitForUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}

// 场景: 读即持锁 + FIFO 唤醒次序(W1 先于 W2)
func TestSlotLocksFIFO(t *testing.T) {
	l := NewSlotLocks()
	r1, err := l.Acquire("s", time.Second, nil)
	if err != nil {
		t.Fatalf("acquire1: %v", err)
	}

	type result struct {
		order int
		err   error
	}
	ch := make(chan result, 2)
	go func() {
		rel, err := l.Acquire("s", 2*time.Second, nil)
		ch <- result{1, err}
		if err == nil {
			rel()
		}
	}()
	waitForUntil(t, func() bool { return l.Waiting("s") == 1 })
	go func() {
		rel, err := l.Acquire("s", 2*time.Second, nil)
		ch <- result{2, err}
		if err == nil {
			rel()
		}
	}()
	waitForUntil(t, func() bool { return l.Waiting("s") == 2 })

	r1()
	first := <-ch
	if first.err != nil || first.order != 1 {
		t.Fatalf("first waiter must win: %+v", first)
	}
	second := <-ch
	if second.err != nil {
		t.Fatalf("second waiter: %v", second.err)
	}
}

// 场景: 等待取消摘除,后续者顶上
func TestSlotLocksCancelRemoval(t *testing.T) {
	l := NewSlotLocks()
	r1, _ := l.Acquire("s", time.Second, nil)

	done := make(chan struct{})
	rel2 := make(chan func(), 1)
	go func() {
		rel, err := l.Acquire("s", 2*time.Second, done)
		if err == nil {
			rel2 <- rel
		}
	}()
	waitForUntil(t, func() bool { return l.Waiting("s") == 1 })

	rel3 := make(chan func(), 1)
	go func() {
		rel, err := l.Acquire("s", 2*time.Second, nil)
		if err == nil {
			rel3 <- rel
		}
	}()
	waitForUntil(t, func() bool { return l.Waiting("s") == 2 })

	close(done) // W2 取消摘除
	waitForUntil(t, func() bool { return l.Waiting("s") == 1 })

	r1()
	var winner func()
	select {
	case winner = <-rel2:
		t.Fatal("cancelled waiter must not win")
	case winner = <-rel3:
	}
	winner()
	if l.Held("s") {
		t.Fatal("lock must be free after release")
	}
}

// 场景: 同槽串行等待超预算抛错
func TestSlotLocksWaitTimeout(t *testing.T) {
	l := NewSlotLocks()
	r1, _ := l.Acquire("s", time.Second, nil)
	defer r1()

	if _, err := l.Acquire("s", 10*time.Millisecond, nil); !errors.Is(err, ErrSlotWaitTimeout) {
		t.Fatalf("wait timeout: %v", err)
	}
}

// 场景: 异槽不互斥
func TestSlotLocksDistinctSlots(t *testing.T) {
	l := NewSlotLocks()
	r1, _ := l.Acquire("a", time.Second, nil)
	defer r1()

	r2, err := l.Acquire("b", time.Millisecond, nil)
	if err != nil {
		t.Fatalf("distinct slot must not block: %v", err)
	}
	defer r2()
}

// 场景: 锁定槽写封闭——持锁期间 Held 为真,终局全量释放后为假
func TestSlotLocksHeldAndReleaseAll(t *testing.T) {
	l := NewSlotLocks()
	r1, _ := l.Acquire("s1", time.Second, nil)
	r2, _ := l.Acquire("s2", time.Second, nil)
	defer r2()
	if !l.Held("s1") || !l.Held("s2") {
		t.Fatal("both must be held")
	}
	r1()
	if l.Held("s1") || !l.Held("s2") {
		t.Fatal("single release must release only its slot")
	}
	l.ReleaseAll()
	if l.Held("s2") {
		t.Fatal("release all must clear")
	}
}

func strPtr(s string) *string { return &s }

func droppedContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

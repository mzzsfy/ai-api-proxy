package history

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestStore 每测试独立临时库
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seed 按指定时间戳注入行(测试确定性;Record 恒取当前时间)
func seed(t *testing.T, s *Store, e Entry, ts int64) {
	t.Helper()
	if _, err := s.insert(e, ts); err != nil {
		t.Fatal(err)
	}
}

func TestStore_Cleanup_RemovesOldKeepsNew(t *testing.T) {
	// Given 新旧行混合 When Cleanup(保留 7 天) Then 旧行删除,保留期内行留存
	s := newTestStore(t)
	now := time.Now()
	seed(t, s, Entry{Method: "POST", Path: "/v1/chat/completions", Model: "m", Status: 200}, now.Add(-8*hoursPerDay*time.Hour).UnixMilli())
	seed(t, s, Entry{Method: "POST", Path: "/v1/chat/completions", Model: "m", Status: 200}, now.Add(-1*hoursPerDay*time.Hour).UnixMilli())
	removed, err := s.Cleanup(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed: %d", removed)
	}
	rows, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(rows) != 1 || rows[0].Model != "m" {
		t.Fatalf("survivors: total=%d rows=%v", total, rows)
	}
}

func TestStore_Cleanup_ClampsHugeRetention(t *testing.T) {
	// Given 超大 retention When Cleanup Then 钳制到上限(不因 Duration 溢出把 cutoff 推到未来导致全表误删或漏删)
	s := newTestStore(t)
	now := time.Now()
	seed(t, s, Entry{Method: "POST", Status: 200}, now.Add(-150*365*hoursPerDay*time.Hour).UnixMilli())
	seed(t, s, Entry{Method: "POST", Status: 200}, now.Add(-50*365*hoursPerDay*time.Hour).UnixMilli())
	removed, err := s.Cleanup(context.Background(), maxRetentionDays*2)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed: %d", removed)
	}
	_, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("survivor total: %d", total)
	}
}

func TestStore_Cleanup_NonPositiveRetentionNoop(t *testing.T) {
	// Given retention<=0(永久保留) When Cleanup Then 无删除
	s := newTestStore(t)
	seed(t, s, Entry{Method: "POST", Status: 200}, time.Now().Add(-365*hoursPerDay*time.Hour).UnixMilli())
	removed, err := s.Cleanup(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed: %d", removed)
	}
	_, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("row must survive: total=%d", total)
	}
}

func TestStore_Query_FiltersAndPagination(t *testing.T) {
	// Given 多行不同 model/upstream/status When 按条件与分页查询 Then 正确子集与总数
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	seed(t, s, Entry{Method: "POST", Path: "/v1/chat/completions", Model: "a", Upstream: "u1", Status: 200}, now)
	seed(t, s, Entry{Method: "POST", Path: "/v1/chat/completions", Model: "b", Upstream: "u1", Status: 429}, now+1)
	seed(t, s, Entry{Method: "POST", Path: "/v1/messages", Model: "a", Upstream: "u2", Status: 502}, now+2)

	cases := []struct {
		name   string
		filter Filter
		total  int
		first  string // 首行 model(倒序)
	}{
		{"无过滤", Filter{}, 3, "a"},
		{"按模型", Filter{Model: "a"}, 2, "a"},
		{"按上游", Filter{Upstream: "u2"}, 1, "a"},
		{"仅错误", Filter{Error: true}, 2, "a"},
		{"分页", Filter{Limit: 2, Offset: 1}, 3, "b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, total, err := s.Query(ctx, tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			if total != tc.total {
				t.Fatalf("total: %d want %d", total, tc.total)
			}
			if len(rows) == 0 {
				t.Fatal("rows empty")
			}
			if rows[0].Model != tc.first {
				t.Fatalf("first: %s want %s", rows[0].Model, tc.first)
			}
		})
	}
	filt := Filter{Model: "a", Limit: 1, Offset: 1}
	rows, total, err := s.Query(ctx, filt)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(rows) != 1 {
		t.Fatalf("combined filter+page: total=%d len=%d", total, len(rows))
	}
}

func TestStore_Get_FullFields(t *testing.T) {
	// Given 已记录一条完整历史 When Get(id) Then 全字段一致含 bodies
	s := newTestStore(t)
	src := Entry{
		Method: "POST", Path: "/v1/chat/completions", Model: "gpt-x", Upstream: "u1", Target: "t1",
		Status: 200, DurationMS: 1234, Stream: true,
		RequestBody: `{"model":"gpt-x"}`, ResponseBody: "",
	}
	seed(t, s, src, time.Now().UnixMilli())
	rows, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("total: %d", total)
	}
	got, err := s.Get(context.Background(), rows[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != src.Method || got.Path != src.Path || got.Model != src.Model ||
		got.Upstream != src.Upstream || got.Target != src.Target ||
		got.Status != src.Status || got.DurationMS != src.DurationMS ||
		got.Stream != src.Stream || got.RequestBody != src.RequestBody {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if _, err := s.Get(context.Background(), 99999); err != ErrNotFound {
		t.Fatalf("missing id err: %v", err)
	}
}

func TestStore_Record_TruncatesOversizedBody(t *testing.T) {
	// Given 请求体超过上限 When Record Then 存储截断到上限(详情路径取回)
	s := newTestStore(t)
	big := strings.Repeat("x", BodyLimit+4096)
	s.Record(Entry{Method: "POST", Status: 200, RequestBody: big, ResponseBody: big})
	rows, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("total: %d", total)
	}
	got, err := s.Get(context.Background(), rows[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.RequestBody) != BodyLimit || len(got.ResponseBody) != BodyLimit {
		t.Fatalf("body not truncated: req=%d resp=%d", len(got.RequestBody), len(got.ResponseBody))
	}
}

func TestStore_Query_ListExcludesBodies(t *testing.T) {
	// Given 已记录带 bodies 的行 When Query 列表 Then 不含 bodies(详情才返回)
	s := newTestStore(t)
	s.Record(Entry{Method: "POST", Status: 200, RequestBody: "req", ResponseBody: "resp"})
	rows, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("total: %d", total)
	}
	if rows[0].RequestBody != "" || rows[0].ResponseBody != "" {
		t.Fatalf("list must exclude bodies: %+v", rows[0])
	}
}

func TestStore_Query_LimitOffsetBounds(t *testing.T) {
	// Given 多行 When limit/offset 越界与缺省 Then 钳制生效(缺省页大小、上限、负 offset 视 0)
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < maxPageSize+1; i++ {
		if _, err := s.insert(Entry{Method: "POST", Status: 200}, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	rows, total, err := s.Query(ctx, Filter{Limit: maxPageSize + 1})
	if err != nil {
		t.Fatal(err)
	}
	if total != maxPageSize+1 || len(rows) != maxPageSize {
		t.Fatalf("limit clamp: total=%d rows=%d", total, len(rows))
	}
	rows, _, err = s.Query(ctx, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != defaultPageSize {
		t.Fatalf("default page: %d", len(rows))
	}
	neg, _, err := s.Query(ctx, Filter{Limit: 1, Offset: -5})
	if err != nil {
		t.Fatal(err)
	}
	zero, _, err := s.Query(ctx, Filter{Limit: 1, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if neg[0].ID != zero[0].ID {
		t.Fatalf("negative offset must behave as 0: %d vs %d", neg[0].ID, zero[0].ID)
	}
}

func TestStore_ConcurrentRecord(t *testing.T) {
	// Given 并发请求 When 同时 Record Then 全部落库无丢失
	s := newTestStore(t)
	const concurrent = 20
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Record(Entry{Method: "POST", Status: 200, Model: "m"})
		}()
	}
	wg.Wait()
	_, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != concurrent {
		t.Fatalf("concurrent records: %d want %d", total, concurrent)
	}
}

func TestStore_Query_EmptyDB(t *testing.T) {
	// Given 空库 When Query Then 空集与零总数,无错误
	s := newTestStore(t)
	rows, total, err := s.Query(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(rows) != 0 {
		t.Fatalf("empty db: total=%d rows=%d", total, len(rows))
	}
}

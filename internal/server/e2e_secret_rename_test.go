package server

import (
	"testing"
	"time"
)

// ─── 分钟标签语义(v2:targets/secrets 改名迁移语义随目标概念删除) ───

func TestPersist_SnapshotMinuteIsCoveredMinute(t *testing.T) {
	// Given tick 对齐 T(计数属 [T-1min,T))When persistSnapshot Then 行 minute=T-1min
	f := newFourGroups(t)
	done := f.app.Recorder.EnterRequest()
	done()
	tick := time.Now().Truncate(time.Minute).Add(time.Minute)
	persistSnapshot(f.app, tick)
	want := tick.Add(-time.Minute).Format("2006-01-02T15:04")
	var reqs int64
	err := f.app.St.DB().QueryRow(`SELECT requests FROM metrics_minutely WHERE minute = ?`, want).Scan(&reqs)
	if err != nil {
		t.Fatalf("row %s missing: %v", want, err)
	}
	if reqs != 1 {
		t.Fatalf("requests: %d", reqs)
	}
}

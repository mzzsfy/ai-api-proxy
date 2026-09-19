package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestRecorder_CountAndConcurrency(t *testing.T) {
	// Given 进入 2 请求且 1 失败 When 快照 Then 计数正确且快照后归零
	r := NewRecorder()
	done1 := r.EnterRequest()
	done2 := r.EnterRequest()
	r.IncError()
	done1()
	done2()
	reqs, errs, conc, _, _ := r.Snapshot()
	if reqs != 2 || errs != 1 || conc != 2 {
		t.Fatalf("got reqs=%d errs=%d conc=%d", reqs, errs, conc)
	}
	reqs2, _, _, _, _ := r.Snapshot()
	if reqs2 != 0 {
		t.Fatalf("snapshot not reset: %d", reqs2)
	}
}

func TestRecorder_CancelNotError(t *testing.T) {
	// Given 请求退出且不调用 IncError When 快照 Then errors=0
	r := NewRecorder()
	done := r.EnterRequest()
	done()
	_, errs, _, _, _ := r.Snapshot()
	if errs != 0 {
		t.Fatalf("cancel counted as error: %d", errs)
	}
}

func TestRecorder_ByUpstreamAndTarget(t *testing.T) {
	// Given 上游与目标维度计数 When 快照 Then 键为 {upstream}/{target} 且并发独立
	r := NewRecorder()
	endT := r.EnterTarget("u1", "t1")
	r.IncUpstream("u1", true)
	endT(true)
	endT2 := r.EnterTarget("u1", "t2")
	endT2(false)

	_, _, _, byUp, byTarget := r.Snapshot()
	if byUp["u1"].Requests != 1 || byUp["u1"].Errors != 1 {
		t.Fatalf("byUp wrong: %+v", byUp["u1"])
	}
	tk := TargetKey("u1", "t1")
	if byTarget[tk].Requests != 1 || byTarget[tk].Errors != 1 {
		t.Fatalf("byTarget wrong: %+v", byTarget[tk])
	}
}

func TestRecorder_ConcurrentMax(t *testing.T) {
	// Given 同目标并发进入 3 个请求 Then MaxConc=3
	r := NewRecorder()
	ends := make([]func(bool), 3)
	for i := range 3 {
		ends[i] = r.EnterTarget("u", "t")
	}
	for _, e := range ends {
		e(false)
	}
	_, _, _, _, byTarget := r.Snapshot()
	if byTarget[TargetKey("u", "t")].MaxConc != 3 {
		t.Fatalf("max conc: %+v", byTarget[TargetKey("u", "t")])
	}
}

func TestRecorder_RaceSafe(t *testing.T) {
	// Given 并发计数与快照 When 同时执行 Then 无数据竞争(配合 -race)
	r := NewRecorder()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 50 {
				done := r.EnterRequest()
				r.IncUpstream("u", j%2 == 0)
				done()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			r.Take(time.Now())
		}
	}()
	wg.Wait()
}

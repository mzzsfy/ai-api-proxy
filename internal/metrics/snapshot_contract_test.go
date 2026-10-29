package metrics

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestRecorder_SnapshotLiveReturnsDetachedCounters(t *testing.T) {
	// Given 各维度计数及在途请求 When 修改实时快照 Then 后续实时读与持久快照均不受影响
	r := NewRecorder()
	end := r.EnterRequest()
	defer end()
	r.IncError()
	r.IncUpstream("u", true)
	r.IncPackage("p", false)
	done := r.EnterTarget("u", "t")
	done(true)
	reqs, errs, conc, up, target, pkg := r.SnapshotLive()
	if reqs != 1 || errs != 1 || conc != 1 {
		t.Fatalf("live counts = %d/%d/%d", reqs, errs, conc)
	}
	for _, table := range []map[string]*UpstreamCounters{up, target, pkg} {
		for key, value := range table {
			value.Requests = 99
			value.Errors = 99
			delete(table, key)
		}
		table["injected"] = &UpstreamCounters{Requests: 99}
	}
	_, _, _, up2, target2, pkg2 := r.SnapshotLive()
	wantUp := map[string]*UpstreamCounters{"u": {Requests: 1, Errors: 1}}
	wantTarget := map[string]*UpstreamCounters{"u/t": {Requests: 1, Errors: 1, MaxConc: 1}}
	wantPkg := map[string]*UpstreamCounters{"p": {Requests: 1}}
	if !reflect.DeepEqual(up2, wantUp) || !reflect.DeepEqual(target2, wantTarget) || !reflect.DeepEqual(pkg2, wantPkg) {
		t.Fatalf("live snapshot aliases recorder: up=%v target=%v package=%v", up2, target2, pkg2)
	}
	requests, failures, maximum, up3, target3, pkg3 := r.Snapshot()
	if requests != 1 || failures != 1 || maximum != 1 || !reflect.DeepEqual(up3, wantUp) || !reflect.DeepEqual(target3, wantTarget) || !reflect.DeepEqual(pkg3, wantPkg) {
		t.Fatal("live read reset or changed persistent counters")
	}
}

func TestRecorder_TakePreservesMinuteAndDimensionPayloads(t *testing.T) {
	// Given 分秒不同的固定时刻及不同维度计数 When Take Then 分钟键与序列化字段各自正确
	r := NewRecorder()
	end := r.EnterRequest()
	end()
	r.IncError()
	r.IncUpstream("u", true)
	r.IncPackage("p", false)
	r.EnterTarget("u", "t")(true)
	row := r.Take(time.Date(2026, time.October, 29, 14, 37, 52, 0, time.UTC))
	if row.Minute != "2026-10-29T14:37" || row.Requests != 1 || row.Errors != 1 || row.MaxConc != 1 {
		t.Fatalf("snapshot row = %+v", row)
	}
	cases := []struct {
		name   string
		encode func() (string, error)
		want   string
	}{
		{"upstream", row.UpstreamJSON, `{"u":{"requests":1,"errors":1,"max_concurrent":0}}`},
		{"target", row.TargetJSON, `{"u/t":{"requests":1,"errors":1,"max_concurrent":1}}`},
		{"package", row.PackageJSON, `{"p":{"requests":1,"errors":0,"max_concurrent":0}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := tc.encode()
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("payload = %s; want %s", raw, tc.want)
			}
		})
	}
	next := r.Take(time.Date(2026, time.October, 29, 14, 38, 0, 0, time.UTC))
	if next.Requests != 0 || next.Errors != 0 || len(next.ByUpstream)+len(next.ByTarget)+len(next.ByPackage) != 0 {
		t.Fatalf("snapshot did not reset: %+v", next)
	}
}

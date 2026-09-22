package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// dimCount 从维度计数表中取指定键的 requests/errors
func dimCount(t *testing.T, dim any, key string) (int64, int64) {
	t.Helper()
	m, ok := dim.(map[string]any)
	if !ok {
		t.Fatalf("dim not object: %T", dim)
	}
	c, ok := m[key].(map[string]any)
	if !ok {
		return 0, 0
	}
	reqs, _ := c["requests"].(float64)
	errs, _ := c["errors"].(float64)
	return int64(reqs), int64(errs)
}

// postOneTraffic 经网关打一次真实请求(主包 js-openai,上游 g1-direct-nofilter)
func postOneTraffic(t *testing.T, f *fourGroupsFixture) {
	t.Helper()
	status, _ := f.post(t, "/v1/chat/completions", map[string]string{
		"Authorization": "Bearer sk-test", "Content-Type": "application/json",
	}, `{"model":"m-direct","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	if status != http.StatusOK {
		t.Fatalf("request: %d", status)
	}
}

func TestMetrics_LiveByPackage(t *testing.T) {
	// Given 请求经上游 g1-direct-nofilter(主包 js-openai) When 查询 live 指标 Then by_package 与 by_upstream 同步计数
	f := newFourGroups(t)
	postOneTraffic(t, f)
	code, body := f.adminGet(t, "/admin/api/metrics/live")
	if code != http.StatusOK {
		t.Fatalf("live: %d %s", code, body)
	}
	var live map[string]any
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	pkgReqs, pkgErrs := dimCount(t, live["by_package"], "js-openai")
	upReqs, _ := dimCount(t, live["by_upstream"], "g1-direct-nofilter")
	if pkgReqs != 1 || pkgErrs != 0 {
		t.Fatalf("by_package[js-openai] = (%d,%d), want (1,0)", pkgReqs, pkgErrs)
	}
	if upReqs != 1 {
		t.Fatalf("by_upstream[g1-direct-nofilter].requests = %d, want 1", upReqs)
	}
}

func TestMetrics_SeriesByPackage(t *testing.T) {
	// Given 真实请求计数经快照落库 When 查询 series Then 每行含 by_package 且与落库注入一致
	f := newFourGroups(t)
	postOneTraffic(t, f)
	tick := time.Now().Truncate(time.Minute).Add(time.Minute)
	persistSnapshot(f.app, tick)

	code, body := f.adminGet(t, "/admin/api/metrics/series?minutes=5")
	if code != http.StatusOK {
		t.Fatalf("series: %d %s", code, body)
	}
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("series empty")
	}
	found := false
	for _, row := range rows {
		byPkg, ok := row["by_package"].(map[string]any)
		if !ok {
			t.Fatalf("row %v missing by_package object", row["minute"])
		}
		if c, ok := byPkg["js-openai"].(map[string]any); ok {
			reqs, _ := c["requests"].(float64)
			if reqs == 1 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("no series row carries by_package[js-openai].requests=1: %s", body)
	}
}

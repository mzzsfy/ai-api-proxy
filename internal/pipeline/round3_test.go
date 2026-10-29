package pipeline

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
)

func TestRun_UndeclaredEntryFormRejectsBeforeComponents(t *testing.T) {
	cases := []struct {
		name        string
		entryStream bool
		forms       []string
	}{
		{name: "streaming_entry", entryStream: true, forms: []string{FormNonStreaming}},
		{name: "non_streaming_entry", entryStream: false, forms: []string{FormStreaming}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given 上游只声明与入口相反的请求形态，修改链与传输均可用。
			var log []string
			proto := &fakeProtocol{forms: tc.forms}
			tr := &fakeTransport{name: TransportRef, fixed: TransportResponse{Status: http.StatusFound}}
			lookups := 0
			ex := &Executor{Transports: func(string) (Transport, bool) {
				lookups++
				return tr, true
			}}
			pctx := NewContext("unsupported-form", UpstreamInfo{Name: "upstream"}, Vars{EntryStream: tc.entryStream})
			filters := []Filter{&recordingFilter{name: "observer", log: &log}}

			// When 执行入口请求。
			resp, err := ex.Run(context.Background(), pctx, resolved(proto, filters), []byte(`{}`))

			// Then 返回能力错误，且拒绝发生在修改请求、构造请求及访问传输之前。
			var capabilityErr *CapabilityError
			if !errors.As(err, &capabilityErr) {
				t.Errorf("error = %v, want CapabilityError", err)
			}
			if resp != nil {
				t.Errorf("response = %#v, want nil", resp)
			}
			if len(log) != 0 {
				t.Errorf("filter calls = %v, want none", log)
			}
			if count := proto.count(); count != 0 {
				t.Errorf("BuildRequest calls = %d, want 0", count)
			}
			if lookups != 0 || tr.calls != 0 {
				t.Errorf("transport lookups = %d, round trips = %d, want neither", lookups, tr.calls)
			}
		})
	}
}

func TestRun_MissingTransportReportsFailureWithoutRoundTrip(t *testing.T) {
	// Given 请求引用的传输未注册，并注入完成回调。
	proto := &fakeProtocol{transport: "missing"}
	tr := &fakeTransport{name: "missing"}
	var lookedUp []string
	var done []struct {
		upstream string
		failed   bool
	}
	ex := &Executor{
		Transports: func(name string) (Transport, bool) {
			lookedUp = append(lookedUp, name)
			return tr, false
		},
		OnDone: func(upstream string, failed bool) {
			done = append(done, struct {
				upstream string
				failed   bool
			}{upstream, failed})
		},
	}
	pctx := NewContext("missing-transport", UpstreamInfo{Name: "upstream"}, Vars{})

	// When 执行一次管道请求。
	resp, err := ex.Run(context.Background(), pctx, resolved(proto, nil), []byte(`{}`))

	// Then 返回构造错误，完成回调恰一次报告该上游失败，且不发出请求。
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Errorf("error = %v, want BuildError", err)
	}
	if resp != nil {
		t.Errorf("response = %#v, want nil", resp)
	}
	if !slices.Equal(lookedUp, []string{"missing"}) {
		t.Errorf("transport lookups = %v, want [missing]", lookedUp)
	}
	if len(done) != 1 || done[0].upstream != "upstream" || !done[0].failed {
		t.Errorf("OnDone = %v, want one (upstream, true)", done)
	}
	if tr.calls != 0 {
		t.Errorf("round trips = %d, want 0", tr.calls)
	}
}

func TestRun_RedirectIsTerminalWithoutSuccessMapping(t *testing.T) {
	// Given 上游返回重定向且成功映射会改写正文。
	var log []string
	proto := &fakeProtocol{respBody: `{"mapped":"success"}`}
	tr := &fakeTransport{name: TransportRef, fixed: TransportResponse{
		Status: http.StatusFound,
		Body:   []byte("redirect body\n"),
	}}
	ex := &Executor{Transports: func(string) (Transport, bool) { return tr, true }}
	pctx := NewContext("redirect", UpstreamInfo{Name: "upstream"}, Vars{})
	filters := []Filter{&recordingFilter{name: "observer", log: &log}}

	// When 执行一次管道请求。
	resp, err := ex.Run(context.Background(), pctx, resolved(proto, filters), []byte(`{}`))

	// Then 原样返回终局状态与正文，且不进入成功响应修改链或重试。
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil {
		t.Fatal("want terminal response")
	}
	if resp.Status != http.StatusFound || string(resp.Body) != "redirect body\n" {
		t.Errorf("terminal response = %d %q, want %d %q", resp.Status, resp.Body, http.StatusFound, "redirect body\n")
	}
	if resp.Stream || resp.Chunks != nil {
		t.Error("terminal response must be non-streaming")
	}
	if !slices.Equal(log, []string{"req:observer"}) {
		t.Errorf("filter calls = %v, want only request mapping", log)
	}
	if tr.calls != 1 {
		t.Errorf("round trips = %d, want 1", tr.calls)
	}
}

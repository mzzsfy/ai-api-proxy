package ipprovider

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// 真实 warp 端到端(env 门控:IPPROVIDER_WARP_E2E=1 才跑;CI 无外网/无凭据 skip)
// 验收:Acquire→Dial 真实地址→EgressIP 非空→Report(Bad) → 实例 Draining

func warpE2EEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("IPPROVIDER_WARP_E2E") != "1" {
		t.Skip("set IPPROVIDER_WARP_E2E=1 to run real warp e2e")
	}
}

func TestWarpE2E_全链路(t *testing.T) {
	warpE2EEnabled(t)
	p, err := Create("ipp_warp", ProviderCfg{
		Name:     "warp-e2e",
		StateDir: t.TempDir(),
		Options: map[string]any{
			"min": 1, "max": 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	// 池就绪(实例探测 + 出口确认)至多 120s(amzStartTimeout 量级)
	deadline := time.Now().Add(120 * time.Second)
	for p.Stats().Normal == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("warp pool not ready: %+v", p.Stats())
		}
		time.Sleep(2 * time.Second)
	}
	lease, err := p.Acquire(context.Background(), Hint{SessionKey: "e2e-key"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	conn, err := lease.Dial(context.Background(), "tcp", "cp.cloudflare.com:80")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if lease.EgressIP() == "" {
		t.Fatal("egress must be known after dial")
	}
	if lease.EgressIP() != "" {
		req, _ := http.NewRequest(http.MethodGet, "https://api.ipify.org", nil)
		tr := &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return lease.Dial(ctx, network, addr)
			},
		}
		cl := &http.Client{Transport: tr, Timeout: 20 * time.Second}
		resp, err := cl.Do(req)
		if err != nil {
			t.Fatalf("egress probe: %v", err)
		}
		_ = resp.Body.Close()
	}
	// Report(Bad) → 实例 Draining(重播换身份)
	before := p.Stats()
	lease.Report(ReportBad, ReasonTargetBlacklist)
	deadline = time.Now().Add(30 * time.Second)
	for p.Stats().Draining == before.Draining {
		if time.Now().After(deadline) {
			t.Fatalf("instance not draining after bad report: %+v", p.Stats())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

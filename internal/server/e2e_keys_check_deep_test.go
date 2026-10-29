package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/gateway"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── keys-check 深度测试:断言强度补强(latency/model/anthropic/ctx 取消)───

// TestKeysCheck_LatencyPositive latency 必为正观测值(上游 20ms 时延下 ≥ 10ms;写死 0 必红)
func TestKeysCheck_LatencyPositive(t *testing.T) {
	f := newKeysCheckFixture(t, true, nil, 20*time.Millisecond)
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID), `{}`)
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, body)
	}
	var out keysCheckReport
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v body %s", err, body)
	}
	for _, row := range out.Results {
		if row.LatencyMS < 10 { // 上游 20ms 时延,阈值取一半留抖动余量
			t.Fatalf("row %s latency %dms too low (upstream sleeps 20ms)", row.Key, row.LatencyMS)
		}
	}
}

// TestKeysCheck_ProbeBodyModel 探测体锁定:probe/chat 的 model 必为成员名(选路错误键必红)
func TestKeysCheck_ProbeBodyModel(t *testing.T) {
	f := newKeysCheckFixture(t, true, nil, 0)
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID), `{}`)
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, body)
	}
	for _, r := range f.probeUpstreamRequests() {
		var probe struct {
			Model    string `json:"model"`
			MaxTok   int    `json:"max_tokens"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal([]byte(r.body), &probe); err != nil {
			t.Fatalf("probe body not json: %s", r.body)
		}
		if probe.Model != "gpt-check" {
			t.Fatalf("probe model %q want member name gpt-check: %s", probe.Model, r.body)
		}
		if probe.MaxTok != 1 {
			t.Fatalf("probe max_tokens %d want 1: %s", probe.MaxTok, r.body)
		}
		if len(probe.Messages) != 1 || probe.Messages[0].Role != "user" || probe.Messages[0].Content != "ping" {
			t.Fatalf("probe messages shape wrong: %s", r.body)
		}
	}
}

// TestKeysCheck_AnthropicGroup anthropic 声明协议组(JS 包):probe/chat 均恒带 max_tokens(必填豁免)
func TestKeysCheck_AnthropicGroup(t *testing.T) {
	f := newKeysCheckFixture(t, true, nil, 0)
	// 换 anthropic 声明组:js-anthropic 包(protocol 部件声明 anthropic-messages),上游同址
	if err := f.app.AdminDeps.Packages.Install(context.Background(), aapZip(t, jsProtoAnthropicManifest, map[string]string{
		plugin.ProtocolEntry: jsProtoSrc}), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := f.app.AdminDeps.Packages.Settings().Put("js-anthropic", plugin.PutInput{Config: map[string]any{
		"base_url": f.upURL, "protocol": "anthropic-messages"}}); err != nil {
		t.Fatal(err)
	}
	for id, val := range keysCheckKeys {
		if _, err := f.app.AdminDeps.Packages.Keys().Set("js-anthropic", id, map[string]any{"api_key": val}); err != nil {
			t.Fatal(err)
		}
	}
	ga := &upstream.Group{Prefix: "e2e-anthropic", Plugin: "js-anthropic", Enabled: true,
		Params: map[string]any{"base_url": f.upURL, "protocol": "anthropic-messages"}}
	if err := f.app.Registry.SaveGroup(context.Background(), ga); err != nil {
		t.Fatal(err)
	}
	if err := f.app.Registry.SaveMember(context.Background(), &upstream.Member{GroupID: ga.ID, Name: "gpt-check", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	grp, err := f.app.Registry.GetGroup(ga.ID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := f.app.Gateway.KeysCheck(context.Background(), grp, gateway.KeysCheckRequest{Mode: gateway.KeysCheckModeChat})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range report.Results {
		if row.Verdict != "ok" {
			t.Fatalf("key %s verdict %q (anthropic chat must carry required max_tokens): %s", row.Key, row.Verdict, row.Error)
		}
	}
	for _, r := range f.probeUpstreamRequests() {
		if !strings.Contains(r.body, `"max_tokens":1`) {
			t.Fatalf("anthropic chat must still carry max_tokens=1 (required field): %s", r.body)
		}
		if !strings.Contains(r.body, "/v1/messages") && !strings.Contains(r.auth, "Bearer") {
			t.Fatalf("anthropic endpoint/auth wrong: %s %s", r.auth, r.body)
		}
	}
}

// TestKeysCheck_HandlerEmptyKey handler 组键缺 api_key 字段 → 执行错误归 unknown(不误判 ok/invalid)
func TestKeysCheck_HandlerEmptyKey(t *testing.T) {
	f := newKeysCheckFixture(t, true, nil, 0)
	pkg := "hd-empty"
	if err := f.app.AdminDeps.Packages.Install(context.Background(), aapZip(t,
		`{"manifestVersion":1,"name":"hd-empty","version":"1","parts":{"handler":"openai-completions"}}`,
		map[string]string{
			plugin.HandlerEntry: handlerKeySrc,
			"settings.js":       `module.exports.settings = { base_url: setting.string({required: true}) };`,
		}), ""); err != nil {
		t.Fatal(err)
	}
	// 好键 + 空键(data 缺 api_key 字段)
	if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, "good", map[string]any{"api_key": "sk-check-good"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, "blank", map[string]any{"other": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.app.AdminDeps.Packages.Settings().Put(pkg, plugin.PutInput{Config: map[string]any{
		"base_url": f.upstreamBase()}}); err != nil {
		t.Fatal(err)
	}
	g := &upstream.Group{Prefix: "hde", Plugin: pkg, Enabled: true}
	if err := f.app.Registry.SaveGroup(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if err := f.app.Registry.SaveMember(context.Background(), &upstream.Member{GroupID: g.ID, Name: "hd-model", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/groups/%d/keys-check", g.ID), `{}`)
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, body)
	}
	var out keysCheckReport
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v body %s", err, body)
	}
	want := map[string]string{"good": "ok", "blank": "unknown"}
	if out.Total != len(want) {
		t.Fatalf("total %d: %s", out.Total, body)
	}
	for _, row := range out.Results {
		if row.Verdict != want[row.Key] {
			t.Fatalf("key %s verdict %q want %q: %s", row.Key, row.Verdict, want[row.Key], body)
		}
	}
}
// TestKeysCheck_ContextCancel 客户端断开(ctx 取消)→ 在途探测归 unknown 且调用及时返回(不空耗上游)
func TestKeysCheck_ContextCancel(t *testing.T) {
	// 上游延迟 800ms:50ms 时取消,所有键在途,KeysCheck 必须远早于自然完成返回
	f := newKeysCheckFixture(t, true, nil, 800*time.Millisecond)
	grp, err := f.app.Registry.GetGroup(f.groupID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	report, err := f.app.Gateway.KeysCheck(ctx, grp, gateway.KeysCheckRequest{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ctx cancel: %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("ctx cancel not honored: %v elapsed", elapsed)
	}
	for _, r := range report.Results {
		if r.Verdict != "unknown" {
			t.Fatalf("canceled key %s verdict %q want unknown: %+v", r.Key, r.Verdict, r)
		}
	}
}

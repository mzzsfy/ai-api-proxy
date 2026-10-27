package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/admin"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 组级 key 池逐键探测(keys-check):强制单键注入 + verdict 分类 ───

// keysCheckKeys 测试键池(键 id → 上游 api_key 值,值互异以区分注入来源)
var keysCheckKeys = map[string]string{
	"good":   "sk-check-good",
	"second": "sk-check-second",
}

// keyProbeRec 上游收到的单次探测请求
type keyProbeRec struct {
	auth string
	body string
}

// keysCheckFixture 最小 keys-check 环境:协议包 + 按鉴权头控状态的假上游
// useBuiltin = 内置 openai-compatible 协议;false = js-openai 真实 JS 协议包
type keysCheckFixture struct {
	app      *App
	gateway  *httptest.Server
	groupID  int64
	memberID int64
	upURL    string
	mu          sync.Mutex
	requests    []keyProbeRec
	inflight    atomic.Int32
	maxInflight atomic.Int32
}

// newKeysCheckFixture 建夹具:statusByAuth 按 Authorization 定上游状态(缺省 200);sleep 拉长单次上游时延
func newKeysCheckFixture(t *testing.T, useBuiltin bool, statusByAuth map[string]int, sleep time.Duration) *keysCheckFixture {
	t.Helper()
	f := &keysCheckFixture{}
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		auth := r.Header.Get("Authorization")
		cur := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for {
			max := f.maxInflight.Load()
			if cur <= max || f.maxInflight.CompareAndSwap(max, cur) {
				break
			}
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
		f.mu.Lock()
		f.requests = append(f.requests, keyProbeRec{auth: auth, body: string(body)})
		f.mu.Unlock()
		status, ok := statusByAuth[auth]
		if !ok {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`{"id":"kc","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`))
		} else {
			_, _ = w.Write([]byte(`{"error":{"message":"probe-status"}}`))
		}
	}))
	t.Cleanup(upSrv.Close)

	passHash, err := admin.BcryptHash(adminTestPass)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Listen: ":0", DataDir: t.TempDir(),
		APIKeys: []string{"sk-test"}, AdminUser: adminTestUser, AdminPassBcrypt: passHash,
	}
	app, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })

	pkg := "openai-compatible"
	if !useBuiltin {
		pkg = "js-openai"
		if err := app.AdminDeps.Packages.Install(context.Background(), aapZip(t, jsProtoManifest, map[string]string{
			plugin.ProtocolEntry: jsProtoSrc, "filters/rewrite.js": rewriteSrc}), ""); err != nil {
			t.Fatal(err)
		}
	}
	for id, val := range keysCheckKeys {
		if _, err := app.AdminDeps.Packages.Keys().Set(pkg, id, map[string]any{"api_key": val}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := app.AdminDeps.Packages.Settings().Put(pkg, plugin.PutInput{Config: func() map[string]any {
		cfg := map[string]any{"base_url": upSrv.URL}
		if !useBuiltin {
			cfg["protocol"] = "openai-completions"
		}
		return cfg
	}()}); err != nil {
		t.Fatal(err)
	}
	g := &upstream.Group{Prefix: "e2e", Plugin: pkg, Enabled: true}
	if err := app.Registry.SaveGroup(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	mem := &upstream.Member{GroupID: g.ID, Name: "gpt-check", Enabled: true}
	if err := app.Registry.SaveMember(context.Background(), mem); err != nil {
		t.Fatal(err)
	}
	f.app = app
	f.groupID = g.ID
	f.memberID = mem.ID
	f.upURL = upSrv.URL
	f.gateway = httptest.NewServer(app.Mux)
	t.Cleanup(f.gateway.Close)
	return f
}

// upstreamBase 假上游基址(handler 包组 base_url 参数来源)
func (f *keysCheckFixture) upstreamBase() string { return f.upURL }

// probeUpstreamRequests 上游已收到的探测请求快照
func (f *keysCheckFixture) probeUpstreamRequests() []keyProbeRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]keyProbeRec(nil), f.requests...)
}

// adminLogin 管理端登录拿会话 cookie
func (f *keysCheckFixture) adminLogin(t *testing.T) *http.Cookie {
	t.Helper()
	resp, err := http.Post(f.gateway.URL+"/admin/api/login", "application/json",
		strings.NewReader(fmt.Sprintf(`{"user":%q,"password":%q}`, adminTestUser, adminTestPass)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login: %d", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == "aap_session" || strings.Contains(c.Name, "session") {
			return c
		}
	}
	t.Fatal("session cookie missing")
	return nil
}

// adminPost 管理端带会话 POST
func (f *keysCheckFixture) adminPost(t *testing.T, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(f.adminLogin(t))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// keysCheckReport keys-check 响应体
type keysCheckReport struct {
	Ok      bool `json:"ok"`
	Total   int  `json:"total"`
	Results []struct {
		Key       string `json:"key"`
		Verdict   string `json:"verdict"`
		Status    int    `json:"status"`
		LatencyMS int64  `json:"latency_ms"`
		Error     string `json:"error"`
	} `json:"results"`
	Error string `json:"error"`
}

func TestKeysCheck_MainPath_PerKeyForcedInjection(t *testing.T) {
	// Given js-openai 组键池两键(值互异)When POST keys-check(默认 probe)Then 200 ok=true total=2
	// 且逐 key verdict=ok;上游收到两键各自的 Authorization;诊断不计 metrics
	f := newKeysCheckFixture(t, false, nil, 0)
	reqs0, errs0, _, _, _, _ := f.app.Recorder.Snapshot()
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID), `{}`)
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, body)
	}
	var out keysCheckReport
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v body %s", err, body)
	}
	if !out.Ok || out.Total != len(keysCheckKeys) || len(out.Results) != len(keysCheckKeys) {
		t.Fatalf("report: %s", body)
	}
	verdictByKey := map[string]string{}
	for _, row := range out.Results {
		verdictByKey[row.Key] = row.Verdict
	}
	for id := range keysCheckKeys {
		if verdictByKey[id] != "ok" {
			t.Fatalf("key %s verdict %q: %s", id, verdictByKey[id], body)
		}
	}
	seen := map[string]bool{}
	for _, r := range f.probeUpstreamRequests() {
		seen[r.auth] = true
	}
	for _, val := range keysCheckKeys {
		if !seen["Bearer "+val] {
			t.Fatalf("upstream missing auth for key value %s: %v", val, seen)
		}
	}
	reqs1, errs1, _, _, _, _ := f.app.Recorder.Snapshot()
	if reqs1 != reqs0 || errs1 != errs0 {
		t.Fatalf("diagnostic must not count metrics: before(%d,%d) after(%d,%d)", reqs0, errs0, reqs1, errs1)
	}
}

func TestKeysCheck_VerdictClassification(t *testing.T) {
	// Given 内置协议组键池,上游按键值分别返回 200/401/403/429/500
	// When keys-check Then verdict 依次 ok/invalid/invalid/rate_limited/unknown
	statusByAuth := map[string]int{
		"Bearer sk-check-good":   http.StatusOK,
		"Bearer sk-check-second": http.StatusUnauthorized,
		"Bearer v403":            http.StatusForbidden,
		"Bearer v429":            http.StatusTooManyRequests,
		"Bearer v500":            http.StatusInternalServerError,
	}
	f := newKeysCheckFixture(t, true, statusByAuth, 0)
	// 扩键:v403/v429/v500(值即状态路由标记)
	pkg := "openai-compatible"
	for _, id := range []string{"v403", "v429", "v500"} {
		if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, id, map[string]any{"api_key": id}); err != nil {
			t.Fatal(err)
		}
	}
	status, body := f.adminPost(t, fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID), `{}`)
	if status != http.StatusOK {
		t.Fatalf("status %d body %s", status, body)
	}
	var out keysCheckReport
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v body %s", err, body)
	}
	want := map[string]string{
		"good": "ok", "second": "invalid", "v403": "invalid",
		"v429": "rate_limited", "v500": "unknown",
	}
	if out.Total != len(want) || len(out.Results) != len(want) {
		t.Fatalf("total %d: %s", out.Total, body)
	}
	for _, row := range out.Results {
		if row.Verdict != want[row.Key] {
			t.Fatalf("key %s verdict %q want %q: %s", row.Key, row.Verdict, want[row.Key], body)
		}
	}
}

func TestKeysCheck_ParamsAndClamp(t *testing.T) {
	// Given 内置协议组键池 3 键(上游时延拉平)When:
	// ① 默认 probe Then 上游请求体含 max_tokens=1
	// ② mode=chat + message Then 上游收到该 message 且无 max_tokens
	// ③ concurrency=1 Then 上游观测最大并发 = 1
	f := newKeysCheckFixture(t, true, nil, 20*time.Millisecond)
	base := fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID)

	// ① probe 默认
	if status, body := f.adminPost(t, base, `{}`); status != http.StatusOK {
		t.Fatalf("probe: %d %s", status, body)
	}
	for _, r := range f.probeUpstreamRequests() {
		if !strings.Contains(r.body, `"max_tokens":1`) {
			t.Fatalf("probe body must carry min budget: %s", r.body)
		}
	}

	// ② chat 实际探测
	f2Count := len(f.probeUpstreamRequests())
	if status, body := f.adminPost(t, base, `{"mode":"chat","message":"hello-there"}`); status != http.StatusOK {
		t.Fatalf("chat: %d %s", status, body)
	}
	chatReqs := f.probeUpstreamRequests()[f2Count:]
	if len(chatReqs) == 0 {
		t.Fatal("chat probe did not reach upstream")
	}
	for _, r := range chatReqs {
		if !strings.Contains(r.body, "hello-there") {
			t.Fatalf("chat message lost: %s", r.body)
		}
		if strings.Contains(r.body, "max_tokens") {
			t.Fatalf("chat must not carry budget: %s", r.body)
		}
	}

	// ③ 并发钳制:concurrency=1 时上游观测最大并发恰为 1
	f.maxInflight.Store(0)
	if status, body := f.adminPost(t, base, `{"concurrency":1}`); status != http.StatusOK {
		t.Fatalf("serial: %d %s", status, body)
	}
	if got := f.maxInflight.Load(); got != 1 {
		t.Fatalf("concurrency=1 max inflight = %d", got)
	}
	// ④ 越界钳制语义:0 与 999 均合法收敛(0=默认,999=上限),不拒绝
	for _, c := range []int{0, 999} {
		if status, body := f.adminPost(t, base, fmt.Sprintf(`{"concurrency":%d}`, c)); status != http.StatusOK {
			t.Fatalf("concurrency=%d: %d %s", c, status, body)
		}
	}
}

func TestKeysCheck_ErrorPaths(t *testing.T) {
	// Given 组不存在/键池空/无启用成员 When keys-check Then 404/400/400
	f := newKeysCheckFixture(t, true, nil, 0)
	base := fmt.Sprintf("/admin/api/groups/%d/keys-check", f.groupID)

	// 404 组不存在
	if status, _ := f.adminPost(t, "/admin/api/groups/99999/keys-check", `{}`); status != http.StatusNotFound {
		t.Fatalf("missing group: %d", status)
	}
	// 400 键池空
	pkg := "openai-compatible"
	if err := f.app.AdminDeps.Packages.Keys().Delete(pkg, "good", "second"); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.adminPost(t, base, `{}`); status != http.StatusBadRequest {
		t.Fatalf("empty pool: %d", status)
	}
	// 400 无启用成员
	if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, "good", map[string]any{"api_key": keysCheckKeys["good"]}); err != nil {
		t.Fatal(err)
	}
	if err := f.app.Registry.SaveMember(context.Background(), &upstream.Member{ID: f.memberID, GroupID: f.groupID, Name: "gpt-check", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.adminPost(t, base, `{}`); status != http.StatusBadRequest {
		t.Fatalf("no enabled member: %d", status)
	}
}

// handlerKeySrc handler 部件:按 ctx.params.base_url 出站透传认证,回传上游状态(reply 透传 status)
const handlerKeySrc = `module.exports = function(ctx){
	var r = ctx.http.request({url: ctx.params.base_url + "/v1/chat/completions", method: "POST",
		headers: {"Authorization": "Bearer " + ctx.key.data.api_key, "Content-Type": "application/json"},
		body: ctx.request.body});
	ctx.reply.json(r.status, JSON.stringify({upstream_status: r.status}));
};`

func TestKeysCheck_HandlerPackageVerdict(t *testing.T) {
	// Given handler 包组键池(上游按鉴权头返回 200/401/429/500)
	// When keys-check Then verdict 依次 ok/invalid/rate_limited/unknown(handler 分支状态分类与管道分支一致)
	statusByAuth := map[string]int{
		"Bearer sk-check-good": http.StatusOK,
		"Bearer v401":          http.StatusUnauthorized,
		"Bearer v429":          http.StatusTooManyRequests,
		"Bearer v500":          http.StatusInternalServerError,
	}
	f := newKeysCheckFixture(t, true, statusByAuth, 0)
	// 换 handler 包组:同 base_url 上游,handler 部件经 ctx.http 出站
	pkg := "hd-check"
	if err := f.app.AdminDeps.Packages.Install(context.Background(), aapZip(t,
		`{"manifestVersion":1,"name":"hd-check","version":"1","parts":{"handler":"openai-completions"}}`,
		map[string]string{
			plugin.HandlerEntry: handlerKeySrc,
			"settings.js":       `module.exports.settings = { base_url: setting.string({required: true}) };`,
		}), ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"v401", "v429", "v500"} {
		if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, id, map[string]any{"api_key": id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.app.AdminDeps.Packages.Keys().Set(pkg, "good", map[string]any{"api_key": "sk-check-good"}); err != nil {
		t.Fatal(err)
	}
	// base_url 参数(handler 参数来自 ctx.params)
	if _, err := f.app.AdminDeps.Packages.Settings().Put(pkg, plugin.PutInput{Config: map[string]any{
		"base_url": f.upstreamBase()}}); err != nil {
		t.Fatal(err)
	}
	g := &upstream.Group{Prefix: "hd", Plugin: pkg, Enabled: true}
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
	want := map[string]string{"good": "ok", "v401": "invalid", "v429": "rate_limited", "v500": "unknown"}
	if out.Total != len(want) {
		t.Fatalf("total %d: %s", out.Total, body)
	}
	for _, row := range out.Results {
		if row.Verdict != want[row.Key] {
			t.Fatalf("key %s verdict %q want %q: %s", row.Key, row.Verdict, want[row.Key], body)
		}
	}
}

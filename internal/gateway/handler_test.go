package gateway

import (
	"context"
	"sync"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// installHandlerPackage 换装 handler 包并装配执行器(假上游;Ddl 补 slot_authority)
func (f *fixture) installHandlerPackage(t *testing.T, handlerSrc string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/hd.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range v2Ddl() {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS slot_authority (ns TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (ns, key))`); err != nil {
		t.Fatal(err)
	}
	pkgs := plugin.NewRegistry(db)
	manifest := `{"manifestVersion":1,"name":"hd-pkg","version":"1","parts":{"handler":"openai-completions"}}`
	settings := `module.exports.settings = { base_url: setting.string({required: true}) };`
	files := map[string]string{plugin.HandlerEntry: handlerSrc, "settings.js": settings}
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, files), ""); err != nil {
		t.Fatalf("install: %v", err)
	}
	if _, err := pkgs.Keys().Set("hd-pkg", "acct-1", map[string]any{"api_key": "sk-hd"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pkgs.Settings().Put("hd-pkg", plugin.PutInput{Config: map[string]any{"base_url": f.upstreamSrv.URL}}); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(db, pkgs)
	g := &upstream.Group{Prefix: "hd", Plugin: "hd-pkg", Enabled: true}
	if err := reg.SaveGroup(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if err := reg.SaveMember(context.Background(), &upstream.Member{GroupID: g.ID, Name: "hd-model", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	f.reg = reg
	f.g.Registry = reg
	f.g.Keys = pkgs.Keys()
	shared := newGatewayShared(db)
	f.g.Handlers = plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps {
		return plugin.HandlerDeps{
			Package: pkg, Store: shared.storeFor(pkg.Manifest.Name), Locks: shared.locksFor(pkg.Manifest.Name),
			Transport: func(name string) (pipeline.Transport, error) {
				if name == "" {
					tr, err := transport.NewManager([]transport.TransportDef{{Name: "direct", URL: "direct"}})
					if err != nil {
						return nil, err
					}
					dtr, ok := tr.Get("direct")
					if !ok {
						return nil, fmt.Errorf("direct transport missing")
					}
					return dtr, nil
				}
				return nil, fmt.Errorf("transport %q not found", name)
			},
			Log: func(level, msg string) { t.Logf("[%s] %s", level, msg) },
		}
	})
}

// 场景: 请求分流——handler 包请求不经 pipeline,直入执行器(ctx.http 出站+ctx.key/ctx.params)
func TestHandlerRoute_NonStream(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"ok":"up"}`)
	f.installHandlerPackage(t, `module.exports = function(ctx){
		var r = ctx.http.request({url: ctx.params.base_url + "/gen", method: "POST", body: ctx.request.body});
		ctx.reply.json(200, JSON.stringify({echo: r.body, model: ctx.request.model, key: ctx.key.data.api_key}));
	}`)
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"hd-model","messages":[{"role":"user","content":"hi"}]}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{`"model":"hd-model"`, `"key":"sk-hd"`, `ok\":\"up`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	if !strings.Contains(string(f.seenBody), `"content":"hi"`) {
		t.Fatalf("outbound body: %s", f.seenBody)
	}
}

// 场景: 空键池不进 handler(502)
func TestHandlerRoute_NoKey502(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{}`)
	f.installHandlerPackage(t, `module.exports = function(ctx){ ctx.reply.json(200, "x"); }`)
	// 删除唯一键(KeysStore.Delete 幂等)
	if err := f.g.Keys.Delete("hd-pkg", "acct-1"); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	f.g.ChatCompletions(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"hd-model","messages":[]}`)))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("502 expected: %d %s", w.Code, w.Body.String())
	}
}

// 场景: 兼容壳——ValidateGroup 放行 handler 包;BuildRequest 必败(空壳不能照旧)
func TestHandlerShell_BuildRequestFails(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{}`)
	f.installHandlerPackage(t, `module.exports = function(ctx){ ctx.reply.json(200, "x"); }`)
	hp, ok := f.reg.HandlerPackage("hd-pkg")
	if !ok {
		t.Fatal("not handler")
	}
	// 组保存校验通过(handler 包)
	g := groupByID(f.reg)
	if err := f.reg.ValidateGroup(g); err != nil {
		t.Fatalf("validate: %v", err)
	}
	resolved, release, err := f.reg.Resolve(g)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := resolved.Protocol.BuildRequest(nil, nil); err == nil {
		t.Fatal("shell BuildRequest must fail")
	}
	_ = hp
	fmt.Sprint()
}

// 场景: TestUpstream/ChatTest 分流(空壳必败,合成请求直入执行器)
func TestHandlerRoute_AdminTests(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"up":true}`)
	f.installHandlerPackage(t, `module.exports = function(ctx){
		var r = ctx.http.request({url: ctx.params.base_url + "/ping", method: "GET"});
		ctx.reply.json(200, JSON.stringify({pong: r.body}));
	}`)
	grp := groupByID(f.reg)
	latency, errMsg := f.g.TestUpstream(context.Background(), grp, "hd-model")
	if errMsg != "" {
		t.Fatalf("test upstream: %s", errMsg)
	}
	if latency < 0 {
		t.Fatal("latency")
	}
	_, status, body, errMsg := f.g.ChatTest(context.Background(), grp, "hd-model", "hello")
	if errMsg != "" || status != http.StatusOK || !strings.Contains(string(body), `"pong"`) {
		t.Fatalf("chat test: %d %s %s", status, body, errMsg)
	}
}

// newGatewayShared 测试版槽资源注册表(无锁注册表实现依赖;最小面)
func newGatewayShared(db *sql.DB) *gatewayShared {
	return &gatewayShared{db: db, slots: map[string]*plugin.SlotStore{}, locks: map[string]*plugin.SlotLocks{}}
}

type gatewayShared struct {
	db     *sql.DB
	mu     sync.Mutex
	slots  map[string]*plugin.SlotStore
	locks  map[string]*plugin.SlotLocks
}

func (h *gatewayShared) storeFor(ns string) *plugin.SlotStore {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.slots[ns]; ok {
		return s
	}
	s := plugin.NewSlotStore(h.db, ns)
	h.slots[ns] = s
	return s
}

func (h *gatewayShared) locksFor(ns string) *plugin.SlotLocks {
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.locks[ns]; ok {
		return l
	}
	l := plugin.NewSlotLocks()
	h.locks[ns] = l
	return l
}

// groupByID 取测试组(单组 fixture)
func groupByID(reg *upstream.Registry) *upstream.Group {
	gs := reg.ListGroups()
	if len(gs) == 0 {
		return nil
	}
	return gs[0]
}

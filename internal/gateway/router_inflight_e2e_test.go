package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
	"github.com/mzzsfy/ai-api-proxy/internal/transport"
	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── handler 面 inFlight 全程覆盖(handler 执行期间计数>0;债务修复回归锁)───

// installHandlerRoutedPackage 双键 + router.js;handler 打 slowSrv 可控挂起
func installHandlerRoutedPackage(t *testing.T, f *fixture, selectJS string, slowSrv *httptest.Server) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/hr.db")
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
	manifest := `{"manifestVersion":1,"name":"hr-pkg","version":"1","parts":{
		"handler":"openai-completions","hooks":{"router":{}}}}`
	handlerSrc := `module.exports = function(ctx){
		var r = ctx.http.request({url: ctx.params.base_url + "/gen", method: "POST",
			headers: {"Authorization": "Bearer " + ctx.key.data.api_key}, body: ctx.request.body});
		ctx.reply.json(200, JSON.stringify({key: ctx.key.data.api_key, up: r.body}));
	}`
	settings := `module.exports.settings = { base_url: setting.string({required: true}) };`
	files := map[string]string{
		plugin.HandlerEntry: handlerSrc,
		"settings.js":       settings,
		plugin.RouterEntry:  "module.exports = {select:" + selectJS + "}",
	}
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, files), ""); err != nil {
		t.Fatalf("install: %v", err)
	}
	if _, err := pkgs.Keys().Set("hr-pkg", "acct-1", map[string]any{"api_key": "sk-1", "pref": 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := pkgs.Keys().Set("hr-pkg", "acct-2", map[string]any{"api_key": "sk-2", "pref": 1}); err != nil {
		t.Fatal(err)
	}
	base := f.upstreamSrv.URL
	if slowSrv != nil {
		base = slowSrv.URL
	}
	if _, err := pkgs.Settings().Put("hr-pkg", plugin.PutInput{Config: map[string]any{"base_url": base}}); err != nil {
		t.Fatal(err)
	}
	reg := upstream.NewRegistry(db, pkgs)
	g := &upstream.Group{Prefix: "hr", Plugin: "hr-pkg", Enabled: true}
	if err := reg.SaveGroup(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if err := reg.SaveMember(context.Background(), &upstream.Member{GroupID: g.ID, Name: "hr-model", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	f.reg = reg
	f.g.Registry = reg
	f.g.Keys = pkgs.Keys()
	f.g.Routers = plugin.NewRouterEngines(func(p *plugin.Package) plugin.RouterDeps { return plugin.RouterDeps{Package: p} })
	shared := newGatewayShared(db)
	f.g.Handlers = plugin.NewHandlerEngines(func(pkg *plugin.Package) plugin.HandlerDeps {
		return plugin.HandlerDeps{
			Package: pkg, Store: shared.storeFor(pkg.Manifest.Name), Locks: shared.locksFor(pkg.Manifest.Name),
			Transport: func(name string) (pipeline.Transport, error) {
				tr, err := transport.NewManager([]transport.TransportDef{{Name: "direct", URL: "direct"}})
				if err != nil {
					return nil, err
				}
				dtr, ok := tr.Get("direct")
				if !ok {
					return nil, fmt.Errorf("direct transport missing")
				}
				return dtr, nil
			},
			Log: func(level, msg string) { t.Logf("[%s] %s", level, msg) },
		}
	})
}

// 场景: handler A 挂起期间(上游阻塞),请求 B 的 select 观察到 A 键 inFlight>0 并选另一键
// 修复前:handlerInputFor 在 input 合成后即归还,inFlight 恒 0,select 恒选 A 键(两请求同键)
func TestHandlerRoute_InFlightHeldDuringExecute(t *testing.T) {
	f := newFixture(t, http.StatusOK, "application/json", `{"ok":true}`)
	releaseSlow := make(chan struct{})
	var hits sync.Map
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Authorization")
		if strings.Contains(key, "sk-1") {
			hits.Store("sk-1", true)
			<-releaseSlow // sk-1 的请求挂起直到测试放行
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer slowSrv.Close()
	// select:pref 最高者优先,在途键整组跳过(acct-1 pref=2 高,挂起后 B 只剩 acct-2)
	installHandlerRoutedPackage(t, f,
		`function(input){var best=null,bp=-1;for(var i=0;i<input.keys.length;i++){var k=input.keys[i];if(input.inFlight[k.id]){continue;}var p=(k.data&&k.data.pref)||0;if(p>bp){bp=p;best=k.id;}}return best;}`,
		slowSrv)
	body := `{"model":"hr-model","messages":[{"role":"user","content":"hi"}]}`
	// 请求 A(异步发出;选 acct-1 后挂起于 slow 上游;终局经 done 通知避免跨 goroutine 读 recorder)
	wa := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.g.ChatCompletions(wa, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	}()
	if !waitFor(t, 5*time.Second, func() bool { _, ok := hits.Load("sk-1"); return ok }) {
		close(releaseSlow) // 失败卫生:放行挂起的 A,防 slowSrv.Close 拖死测试进程
		<-done
		t.Fatal("request A never reached slow upstream as sk-1")
	}
	// 请求 B:此时 A 在途,B 的 select 应见 acct-1 inFlight=1 → 选 acct-2
	wb := httptest.NewRecorder()
	f.g.ChatCompletions(wb, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if wb.Code != http.StatusOK {
		close(releaseSlow)
		<-done
		t.Fatalf("B status=%d body=%s", wb.Code, wb.Body.String())
	}
	if !strings.Contains(wb.Body.String(), "sk-2") {
		close(releaseSlow)
		<-done
		t.Fatalf("B should route to sk-2 while sk-1 in flight, got %s", wb.Body.String())
	}
	// 放行 A 并等其终局(避免 t.Cleanup 关库先于 A 的 storage 归并)
	close(releaseSlow)
	<-done
	if wa.Code != http.StatusOK {
		t.Fatalf("A status=%d body=%s", wa.Code, wa.Body.String())
	}
}

// waitFor 轮询条件(有界;false = 超时)
func waitFor(t *testing.T, limit time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

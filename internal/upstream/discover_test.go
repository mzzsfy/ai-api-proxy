package upstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// installDiscoverPkg 装 openai-completions 包(声明 base_url 槽 + discovery 部件)
func installDiscoverPkg(t *testing.T, pkgs *plugin.Registry, name, style string) {
	t.Helper()
	manifest := `{"manifestVersion":1,"name":"` + name + `","version":"1.0.0","parts":{
		"protocol":{"protocol":"openai-completions","features":["tools"]},
		"discovery":{"baseUrlSlot":"base_url","protocolStyle":"` + style + `"}}}`
	src := `module.exports = function (config) {
		return { buildRequest: function (ctx, entry) {
			return { url: config.base_url + "/v1/chat/completions", method: "POST", headers: {}, body: entry, stream: false };
		}, mapResponse: function (ctx, b) { return b; } };
	};
	module.exports.settings = { base_url: setting.string({required: true}) };`
	if err := pkgs.Install(context.Background(), buildZip(t, manifest, map[string]string{plugin.ProtocolEntry: src}), ""); err != nil {
		t.Fatal(err)
	}
}

func modelsBody(names ...string) string {
	out := `{"data":[`
	for i, n := range names {
		if i > 0 {
			out += ","
		}
		b, _ := json.Marshal(map[string]string{"id": n})
		out += string(b)
	}
	return out + "]}"
}

func TestDiscover_FilterRequired_D1(t *testing.T) {
	// D1 规则管道无 only/exclude → 400 语义错误(用户未表态)
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	g := mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": "https://x"})
	_, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{Rules: []DiscoveryRule{
		{Op: "rename", From: "^", To: "x-"},
	}}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "only/exclude") {
		t.Fatalf("filter表态 required: %v", err)
	}
}

func TestDiscover_PreviewPrefixFilter_D2(t *testing.T) {
	// D2 前缀过滤预览:apply=false 不落库,只返回匹配项
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	var upstreamNames []string
	for i := 0; i < 3; i++ {
		upstreamNames = append(upstreamNames, "gemini-pro-"+string(rune('a'+i)), "gpt-4o")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(modelsBody(upstreamNames...)))
	}))
	defer srv.Close()
	g := mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": srv.URL})
	res, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{Rules: []DiscoveryRule{
		{Op: "only", Regex: "^gemini"},
	}}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if res.FetchedTotal != len(upstreamNames) {
		t.Fatalf("fetched_total: %d", res.FetchedTotal)
	}
	if len(res.Models) != 3 {
		t.Fatalf("filtered models: %+v", res.Models)
	}
	for _, h := range res.Models {
		if !strings.HasPrefix(h.MemberName, "gemini") || h.Action != "create" {
			t.Fatalf("hit: %+v", h)
		}
	}
	if len(reg.ListMembers(g.ID)) != 0 {
		t.Fatal("preview must not persist")
	}
}

func TestDiscover_RenameApplyDisabled_D3(t *testing.T) {
	// D3 rename 后 apply=true:成员以改名落库且 enabled=false;已存在同名跳过
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(modelsBody("gemini-pro", "gemini-flash")))
	}))
	defer srv.Close()
	g := mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": srv.URL})
	mustMember(t, reg, g.ID, "pro", true) // gemini-pro → pro 将命中 skip_exists
	res, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{
		Rules: []DiscoveryRule{
			{Op: "only", Regex: "."},
			{Op: "rename", From: "^gemini-", To: ""},
		},
		Apply: true,
	}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied == nil || res.Applied.Created != 1 || res.Applied.Skipped != 1 {
		t.Fatalf("applied: %+v", res.Applied)
	}
	mems := reg.ListMembers(g.ID)
	if len(mems) != 2 {
		t.Fatalf("members: %+v", mems)
	}
	for _, m := range mems {
		// 预置成员 pro 本就 enabled;发现新建的成员必须默认 disabled
		if m.Name == "flash" && m.Enabled {
			t.Fatalf("discovered member must be disabled by default: %+v", m)
		}
	}
}

func TestDiscover_UnreachableUpstream_D4(t *testing.T) {
	// D4 上游不可达 → fetch models 错误,零副作用
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	g := mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": "http://127.0.0.1:1"})
	_, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{
		Rules: []DiscoveryRule{{Op: "only", Regex: "."}},
	}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "fetch models") {
		t.Fatalf("unreachable: %v", err)
	}
	if len(reg.ListMembers(g.ID)) != 0 {
		t.Fatal("failed discover must not persist")
	}
}

func TestDiscover_UndeclaredPackageRejected(t *testing.T) {
	// 无 discovery 声明的包 → 拒绝
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "plain")
	g := mustGroup(t, reg, "plain", "plain", nil)
	_, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{
		Rules: []DiscoveryRule{{Op: "only", Regex: "."}},
	}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "discovery support") {
		t.Fatalf("undeclared: %v", err)
	}
}

func TestDiscover_LimitRule(t *testing.T) {
	// limit 规则截断候选
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(modelsBody("a1", "a2", "a3", "a4")))
	}))
	defer srv.Close()
	g := mustGroup(t, reg, "demo", "demo", map[string]any{"base_url": srv.URL})
	res, err := reg.Discover(context.Background(), g.ID, DiscoveryRequest{
		Rules: []DiscoveryRule{
			{Op: "only", Regex: "."},
			{Op: "limit", Limit: 2},
		},
	}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Models) != 2 {
		t.Fatalf("limited: %+v", res.Models)
	}
}

func TestDiscover_PollShareMarked(t *testing.T) {
	// 同短名已在他组:预览标注 poll_groups>0(落库后参与轮询)
	pkgs, reg := testEnv(t)
	installDiscoverPkg(t, pkgs, "demo", "openai")
	installBuiltinLikePkg(t, pkgs, "other")
	g := mustGroup(t, reg, "demo", "demo", nil)
	_ = g
	go2 := mustGroup(t, reg, "other", "other", nil)
	mustMember(t, reg, go2.ID, "shared", true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(modelsBody("shared")))
	}))
	defer srv.Close()
	g2 := mustGroup(t, reg, "demo2", "demo", map[string]any{"base_url": srv.URL})
	res, err := reg.Discover(context.Background(), g2.ID, DiscoveryRequest{
		Rules: []DiscoveryRule{{Op: "only", Regex: "."}},
	}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Models) != 1 || res.Models[0].PollGroups != 1 {
		t.Fatalf("poll share: %+v", res.Models)
	}
}

package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// ─── 改名凭据迁移 + 分钟标签语义 ───

func findUpstreamByName(t *testing.T, f *fourGroupsFixture, name string) *upstream.Upstream {
	t.Helper()
	code, body := f.adminGet(t, "/admin/api/upstreams")
	if code != 200 {
		t.Fatalf("list upstreams: %d %s", code, body)
	}
	var ups []*upstream.Upstream
	if err := json.Unmarshal(body, &ups); err != nil {
		t.Fatal(err)
	}
	for _, u := range ups {
		if u.Name == name {
			return u
		}
	}
	return nil
}

func TestSecret_TargetRenameMigratesMaskedKey(t *testing.T) {
	// Given u-ren 目标 t1(凭据 K)When PUT 改名 t2 + key 留空掩码 + rename_from=t1 Then (u-ren,t2)=K 且 (u-ren,t1) 删除
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "u-ren", Enabled: true, Base: upstream.PackageRef{Package: "js-openai"}, Models: []string{"m-ren"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Enabled: true,
			Secrets: map[string]string{"api_key": "sk-real"}}},
	}); err != nil {
		t.Fatal(err)
	}
	u := findUpstreamByName(t, f, "u-ren")
	if u == nil {
		t.Fatal("u-ren missing")
	}
	body := fmt.Sprintf(`{"name":"u-ren","enabled":true,"base":{"package":"js-openai"},"models":["m-ren"],
		"targets":[{"name":"t2","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,
		"secrets":{"api_key":"***"},"rename_from":"t1"}]}`)
	code, resp := f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 200 {
		t.Fatalf("put: %d %s", code, resp)
	}
	if got, ok := f.app.AdminDeps.Secrets.GetTargetSecrets("u-ren", "t2"); !ok || got["api_key"] != "sk-real" {
		t.Fatalf("t2 secret not migrated: %v %v", got, ok)
	}
	if _, ok := f.app.AdminDeps.Secrets.GetTargetSecrets("u-ren", "t1"); ok {
		t.Fatal("old t1 secret not cleaned")
	}
}

func TestSecret_UnrenamedTargetKeepsMaskedKey(t *testing.T) {
	// Given u-ren 目标 t1 When PUT 未改名 + key 留空掩码 Then 凭据保留(回归)
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "u-keep", Enabled: true, Base: upstream.PackageRef{Package: "js-openai"}, Models: []string{"m-keep"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Enabled: true,
			Secrets: map[string]string{"api_key": "sk-real"}}},
	}); err != nil {
		t.Fatal(err)
	}
	u := findUpstreamByName(t, f, "u-keep")
	body := `{"name":"u-keep","enabled":true,"base":{"package":"js-openai"},"models":["m-keep"],
		"targets":[{"name":"t1","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,
		"secrets":{"api_key":"***"}}]}`
	code, resp := f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 200 {
		t.Fatalf("put: %d %s", code, resp)
	}
	if got, ok := f.app.AdminDeps.Secrets.GetTargetSecrets("u-keep", "t1"); !ok || got["api_key"] != "sk-real" {
		t.Fatalf("secret lost: %v %v", got, ok)
	}
}

func TestSecret_UpstreamRenameRejected(t *testing.T) {
	// Given by-id 保存改 upstream 名 When PUT Then 400 且不产生重复实例
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "u-old", Enabled: true, Base: upstream.PackageRef{Package: "js-openai"}, Models: []string{"m-old"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Enabled: true,
			Secrets: map[string]string{"api_key": "sk-real"}}},
	}); err != nil {
		t.Fatal(err)
	}
	u := findUpstreamByName(t, f, "u-old")
	body := `{"name":"u-new","enabled":true,"base":{"package":"js-openai"},"models":["m-old"],
		"targets":[{"name":"t1","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,
		"secrets":{"api_key":"***"}}]}`
	code, _ := f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 400 {
		t.Fatalf("rename must be rejected, got %d", code)
	}
	if findUpstreamByName(t, f, "u-new") != nil {
		t.Fatal("duplicate upstream created by rename")
	}
	if findUpstreamByName(t, f, "u-old") == nil {
		t.Fatal("original upstream missing")
	}
}

func TestSecret_TargetSwapNamesMigrateByOrigin(t *testing.T) {
	// Given (u,t_a)=A、(u,t_b)=B When 两行互换名 + 掩码 + rename_from Then (u,t_b)=A 且 (u,t_a)=B
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "u-swap", Enabled: true, Base: upstream.PackageRef{Package: "js-openai"}, Models: []string{"m-swap"},
		Targets: []upstream.Target{
			{Name: "t_a", BaseURL: "http://127.0.0.1:1", Enabled: true, Secrets: map[string]string{"api_key": "sk-aaa"}},
			{Name: "t_b", BaseURL: "http://127.0.0.1:1", Enabled: true, Secrets: map[string]string{"api_key": "sk-bbb"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	u := findUpstreamByName(t, f, "u-swap")
	body := `{"name":"u-swap","enabled":true,"base":{"package":"js-openai"},"models":["m-swap"],
		"targets":[
			{"name":"t_b","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,"secrets":{"api_key":"***"},"rename_from":"t_a"},
			{"name":"t_a","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,"secrets":{"api_key":"***"},"rename_from":"t_b"}
		]}`
	code, resp := f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 200 {
		t.Fatalf("put: %d %s", code, resp)
	}
	if got, _ := f.app.AdminDeps.Secrets.GetTargetSecrets("u-swap", "t_b"); got["api_key"] != "sk-aaa" {
		t.Fatalf("t_b must inherit t_a secret: %v", got)
	}
	if got, _ := f.app.AdminDeps.Secrets.GetTargetSecrets("u-swap", "t_a"); got["api_key"] != "sk-bbb" {
		t.Fatalf("t_a must inherit t_b secret: %v", got)
	}
}

func TestSecret_RenameFromGuardBranches(t *testing.T) {
	// Given rename_from == 新名(守卫分支)When 掩码保存 Then 按新名回读,凭据保留
	// Given rename_from 指向不存在旧名 When 掩码保存 Then 掩码删除,启用目标缺键 400 拒绝(fail-loud)
	f := newFourGroups(t)
	if err := f.app.Registry.Save(t.Context(), &upstream.Upstream{
		Name: "u-guard", Enabled: true, Base: upstream.PackageRef{Package: "js-openai"}, Models: []string{"m-guard"},
		Targets: []upstream.Target{{Name: "t1", BaseURL: "http://127.0.0.1:1", Enabled: true,
			Secrets: map[string]string{"api_key": "sk-real"}}},
	}); err != nil {
		t.Fatal(err)
	}
	u := findUpstreamByName(t, f, "u-guard")
	// rename_from == 新名:等价未改名
	body := `{"name":"u-guard","enabled":true,"base":{"package":"js-openai"},"models":["m-guard"],
		"targets":[{"name":"t1","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,
		"secrets":{"api_key":"***"},"rename_from":"t1"}]}`
	code, resp := f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 200 {
		t.Fatalf("rename_from==name: %d %s", code, resp)
	}
	if got, ok := f.app.AdminDeps.Secrets.GetTargetSecrets("u-guard", "t1"); !ok || got["api_key"] != "sk-real" {
		t.Fatalf("secret lost: %v %v", got, ok)
	}
	// rename_from 指向不存在旧名:掩码删除 → 缺键 400
	body = `{"name":"u-guard","enabled":true,"base":{"package":"js-openai"},"models":["m-guard"],
		"targets":[{"name":"t2","base_url":"http://127.0.0.1:1","transport":"","enabled":true,"weight":1,
		"secrets":{"api_key":"***"},"rename_from":"t-ghost"}]}`
	code, _ = f.adminPut(t, fmt.Sprintf("/admin/api/upstreams/%d", u.ID), body)
	if code != 400 {
		t.Fatalf("ghost rename_from must fail loud, got %d", code)
	}
	if _, ok := f.app.AdminDeps.Secrets.GetTargetSecrets("u-guard", "t2"); ok {
		t.Fatal("ghost rename_from must not create kv entry")
	}
}

func TestPersist_SnapshotMinuteIsCoveredMinute(t *testing.T) {
	// Given tick 对齐 T(计数属 [T-1min,T))When persistSnapshot Then 行 minute=T-1min
	f := newFourGroups(t)
	done := f.app.Recorder.EnterRequest()
	done()
	tick := time.Now().Truncate(time.Minute).Add(time.Minute)
	persistSnapshot(f.app, tick)
	want := tick.Add(-time.Minute).Format("2006-01-02T15:04")
	var reqs int64
	err := f.app.St.DB().QueryRow(`SELECT requests FROM metrics_minutely WHERE minute = ?`, want).Scan(&reqs)
	if err != nil {
		t.Fatalf("row %s missing: %v", want, err)
	}
	if reqs != 1 {
		t.Fatalf("requests: %d", reqs)
	}
}

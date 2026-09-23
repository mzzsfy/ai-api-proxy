package plugin

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ─── 包级 key 存储(单键一等实体;BDD:单键独立成档/写自动备份/删除/轮询/清空上限/卸载)───

func testKeysDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// 005 后形态:name=包/键id + prev_json 列(迁移测试覆盖存量形态)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS package_keys (
		name TEXT PRIMARY KEY, data_json TEXT NOT NULL DEFAULT '{}',
		prev_json TEXT, updated_at INTEGER NOT NULL DEFAULT 0)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestKeys_SetPerEntry(t *testing.T) {
	// BDD 1 单键独立成档:Given 空池 When Set(api_key,data) Then 一行 Entry{id,data,updatedAt,prev 空}
	ks := NewKeysStore(testKeysDB(t))
	ts, err := ks.Set("pkg", "api_key", map[string]any{"api_key": "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if ts == 0 {
		t.Fatal("updatedAt zero")
	}
	list := ks.List("pkg")
	if len(list) != 1 || list[0].ID != "api_key" {
		t.Fatalf("list: %+v", list)
	}
	if m, ok := list[0].Data.(map[string]any); !ok || m["api_key"] != "k1" {
		t.Fatalf("data: %+v", list[0].Data)
	}
	if list[0].Prev != nil {
		t.Fatalf("fresh prev: %v", list[0].Prev)
	}
	if list[0].UpdatedAt != ts {
		t.Fatalf("updatedAt: %d != %d", list[0].UpdatedAt, ts)
	}
}

func TestKeys_SetIsolatesEntries(t *testing.T) {
	// BDD 2 单键隔离:写第二键,第一键 updatedAt/prev 不变
	ks := NewKeysStore(testKeysDB(t))
	if _, err := ks.Set("pkg", "a", "one"); err != nil {
		t.Fatal(err)
	}
	first := ks.List("pkg")[0]
	if _, err := ks.Set("pkg", "b", "two"); err != nil {
		t.Fatal(err)
	}
	e, ok := ks.Get("pkg", "a")
	if !ok || e.UpdatedAt != first.UpdatedAt {
		t.Fatalf("a drifted: %+v vs %+v", e, first)
	}
	if e.Prev != nil {
		t.Fatalf("a prev polluted: %v", e.Prev)
	}
}

func TestKeys_SetBacksUpPrev(t *testing.T) {
	// BDD 3 写自动备份:data=A 再写 B → data=B,prev=A;二写后 prev=B(A 被覆盖,单级)
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("pkg", "k", "A")
	if _, err := ks.Set("pkg", "k", "B"); err != nil {
		t.Fatal(err)
	}
	e, ok := ks.Get("pkg", "k")
	if !ok || e.Data != "B" || e.Prev != "A" {
		t.Fatalf("entry: %+v ok=%v", e, ok)
	}
	_, _ = ks.Set("pkg", "k", "C")
	if e, _ = ks.Get("pkg", "k"); e.Prev != "B" {
		t.Fatalf("prev not single-level: %v", e.Prev)
	}
}

func TestKeys_Delete(t *testing.T) {
	// BDD 4 删除:行消失;Rotate 不再选中;重复删除幂等
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("pkg", "api_key", "v")
	if err := ks.Delete("pkg", "api_key"); err != nil {
		t.Fatal(err)
	}
	if _, ok := ks.Get("pkg", "api_key"); ok {
		t.Fatal("key survived delete")
	}
	if _, ok := ks.Rotate("pkg"); ok {
		t.Fatal("rotate picked deleted key")
	}
	if err := ks.Delete("pkg", "api_key"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestKeys_Rotate(t *testing.T) {
	// BDD 6 轮询:2 键连续 Rotate 交替;删 1 键后恒落余键;空池 false
	ks := NewKeysStore(testKeysDB(t))
	if _, ok := ks.Rotate("pkg"); ok {
		t.Fatal("empty pool rotated")
	}
	_, _ = ks.Set("pkg", "a", 1)
	_, _ = ks.Set("pkg", "b", 2)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		e, ok := ks.Rotate("pkg")
		if !ok {
			t.Fatal("rotate failed on non-empty pool")
		}
		seen[e.ID]++
	}
	if seen["a"] != 2 || seen["b"] != 2 {
		t.Fatalf("not alternating: %v", seen)
	}
	_ = ks.Delete("pkg", "b")
	for i := 0; i < 3; i++ {
		if e, ok := ks.Rotate("pkg"); !ok || e.ID != "a" {
			t.Fatalf("post-delete rotate: %+v %v", e, ok)
		}
	}
}

func TestKeys_UpdatedAt(t *testing.T) {
	// BDD 9 基准:不存在 = 0;写入后 = 行 updated_at
	ks := NewKeysStore(testKeysDB(t))
	if ks.UpdatedAt("pkg", "ghost") != 0 {
		t.Fatal("ghost has updatedAt")
	}
	ts, _ := ks.Set("pkg", "k", "v")
	if ks.UpdatedAt("pkg", "k") != ts {
		t.Fatalf("updatedAt mismatch: %d != %d", ks.UpdatedAt("pkg", "k"), ts)
	}
}

func TestKeys_LimitRejects(t *testing.T) {
	// 单键 64KB 上限拒写(blobStore SettingsLimit 同源)
	ks := NewKeysStore(testKeysDB(t))
	if _, err := ks.Set("pkg", "big", string(make([]byte, SettingsLimit+1))); err == nil {
		t.Fatal("oversized key accepted")
	}
}

func TestKeys_DeletePackageAndMask(t *testing.T) {
	// BDD 卸载清理:前缀删只清本包;MaskStrings 收集字符串叶子(嵌套对象含数组)
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("pkg", "api_key", "secret-value")
	_, _ = ks.Set("pkg", "nested", map[string]any{"arr": []any{"x", 1, true}, "t": "y"})
	_, _ = ks.Set("other", "api_key", "keep-me")
	masks := ks.MaskStrings("pkg")
	joined := strings.Join(masks, ",")
	if !strings.Contains(joined, "secret-value") || !strings.Contains(joined, "x") || !strings.Contains(joined, "y") {
		t.Fatalf("masks: %v", masks)
	}
	if strings.Contains(joined, "keep-me") {
		t.Fatalf("cross package mask: %v", masks)
	}
	ks.DeletePackage("pkg")
	if _, ok := ks.Get("pkg", "api_key"); ok {
		t.Fatal("key survived package delete")
	}
	if _, ok := ks.Get("other", "api_key"); !ok {
		t.Fatal("other package purged")
	}
}

func TestKeys_PrevNotMigratedOrRotated(t *testing.T) {
	// prev 仅由写路径产生;Rotate 返回的 Entry 同样携带 prev(只读展示);升级无快照语义(Set 之外的路径不动 prev)
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("pkg", "k", "v1")
	e1, _ := ks.Get("pkg", "k")
	// 模拟"升级":直接再读,无任何写 → prev 不变
	e2, _ := ks.Get("pkg", "k")
	if e1.Prev != e2.Prev || e1.UpdatedAt != e2.UpdatedAt {
		t.Fatal("read mutated entry")
	}
	if _, ok := ks.Rotate("pkg"); !ok {
		t.Fatal("rotate failed")
	}
	e3, _ := ks.Get("pkg", "k")
	if e3.Prev != nil && e3.Data == e3.Prev {
		t.Fatal("prev equal data after single write")
	}
	_, _ = json.Marshal(e3) // Entry 可序列化(admin 视图)
	_ = time.Now
}

// ─── hooks ctx.key / 写窗(JS 面;BDD 3/7/10/15/16)───

func TestHooks_CtxKeyInjection(t *testing.T) {
	// BDD 10 注入:任务 ctx.key={id,data};写窗 set(data) 整体替换当前键(prev 自动)
	files := map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){
		storage.set("id", String(ctx.key && ctx.key.id));
		storage.set("data", JSON.stringify(ctx.key && ctx.key.data));
		ctx.keys.set({api_key: "new-" + ctx.key.data.api_key});
		storage.set("after", JSON.stringify(ctx.key.data));
	}}`}
	tk := HooksTask{Name: "a", Cron: "* * * * *"}
	pkg := hooksPkg(t, files, tk)
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("hp", "main", map[string]any{"api_key": "old"})
	mem := &memKV{m: map[string]string{}}
	deps := HooksDeps{Keys: ks, Storage: mem, Key: &KeyRef{ID: "main", Data: map[string]any{"api_key": "old"}}, Now: func() time.Time { return time.Unix(0, 0) }}
	rt, err := LoadTask(pkg, tk, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(tk, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if mem.m["id"] != "main" || mem.m["data"] != `{"api_key":"old"}` {
		t.Fatalf("ctx.key: %v %v", mem.m["id"], mem.m["data"])
	}
	e, ok := ks.Get("hp", "main")
	if !ok {
		t.Fatal("entry vanished")
	}
	if m, _ := e.Data.(map[string]any); m["api_key"] != "new-old" {
		t.Fatalf("set(data) failed: %+v", e.Data)
	}
	if p, _ := e.Prev.(map[string]any); p == nil || p["api_key"] != "old" {
		t.Fatalf("prev: %+v", e.Prev)
	}
	// 运行中 ctx.key.data 即新值(运行时切片)
	if mem.m["after"] != `{"api_key":"new-old"}` {
		t.Fatalf("after: %v", mem.m["after"])
	}
}

func TestHooks_MergePatch(t *testing.T) {
	// merge(patch) 浅合并进当前键 data;标量 data 报错
	files := map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){ctx.keys.merge({extra: 2});}}`}
	tk := HooksTask{Name: "a", Cron: "* * * * *"}
	pkg := hooksPkg(t, files, tk)
	ks := NewKeysStore(testKeysDB(t))
	_, _ = ks.Set("hp", "main", map[string]any{"keep": 1})
	mem := &memKV{m: map[string]string{}}
	deps := HooksDeps{Keys: ks, Storage: mem, Key: &KeyRef{ID: "main", Data: map[string]any{"keep": 1}}, Now: func() time.Time { return time.Unix(0, 0) }}
	rt, err := LoadTask(pkg, tk, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(tk, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	e, _ := ks.Get("hp", "main")
	m := e.Data.(map[string]any)
	if m["keep"] != float64(1) || m["extra"] != float64(2) {
		t.Fatalf("merged: %+v", m)
	}
	// 标量 data + merge → 报错
	files2 := map[string]string{"tasks/b.js": `module.exports={handler:function(ctx){ctx.keys.merge({x:1});}}`}
	pkg2 := hooksPkg(t, files2, HooksTask{Name: "b", Cron: "* * * * *"})
	_, _ = ks.Set("hp", "scalar", "bare")
	deps2 := HooksDeps{Keys: ks, Storage: mem, Key: &KeyRef{ID: "scalar", Data: "bare"}, Now: func() time.Time { return time.Unix(0, 0) }}
	rt2, err := LoadTask(pkg2, HooksTask{Name: "b", Cron: "* * * * *"}, deps2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.RunTask(HooksTask{Name: "b", Cron: "* * * * *"}, time.Unix(0, 0)); err == nil {
		t.Fatal("merge on scalar accepted")
	}
}

func TestHooks_KeySubmitCreatesEntry(t *testing.T) {
	// BDD 8 表单建键:keySubmit 窗 set(data) = 创建新条目(宿主生成 id,同毫秒多次不碰撞)
	files := map[string]string{"keys.js": `module.exports={
		keyForm: function(ctx){ return {fields: []}; },
		keySubmit: function(ctx, values){ ctx.keys.set({token: values.token}); ctx.keys.set({token: values.token + "2"}); }
	}`}
	pkg := hooksPkg(t, files)
	ks := NewKeysStore(testKeysDB(t))
	mem := &memKV{m: map[string]string{}}
	deps := HooksDeps{Keys: ks, Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}
	rt, err := LoadKeys(pkg, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids, errs, err := rt.CallKeySubmit(map[string]any{"token": "tk"})
	if err != nil {
		t.Fatal(err)
	}
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if len(ids) != 2 {
		t.Fatalf("ids: %v", ids)
	}
	if ids[0] == ids[1] {
		t.Fatalf("id collision: %v", ids)
	}
	for _, id := range ids {
		if e, ok := ks.Get("hp", id); !ok {
			t.Fatalf("entry %s missing", id)
		} else if m := e.Data.(map[string]any); m["token"] == nil {
			t.Fatalf("entry data: %+v", e.Data)
		}
	}
}

func TestHooks_NoMultiKeyJS(t *testing.T) {
	// BDD 15 JS 权限面:get/list/previous/remove 不存在;util.key("x") 忽略参数等同无参
	files := map[string]string{"tasks/a.js": `module.exports={handler:function(ctx){
		storage.set("probe", JSON.stringify([
			String(ctx.keys.get === undefined),
			String(ctx.keys.list === undefined),
			String(ctx.keys.previous === undefined),
			String(ctx.keys.remove === undefined),
			String(ctx.key === undefined),
			String(util.key("ignored") === undefined)
		]));
	}}`}
	tk := HooksTask{Name: "a", Cron: "* * * * *"}
	pkg := hooksPkg(t, files, tk)
	mem := &memKV{m: map[string]string{}}
	// 无键上下文:ctx.key undefined,util.key() undefined(HostDeps 无 Key)
	rt, err := LoadTask(pkg, tk, HooksDeps{Keys: NewKeysStore(testKeysDB(t)), Storage: mem, Now: func() time.Time { return time.Unix(0, 0) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunTask(tk, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if mem.m["probe"] != `["true","true","true","true","true","true"]` {
		t.Fatalf("probe: %s", mem.m["probe"])
	}
}

func TestHooks_WriteWindowOnlyTaskAndSubmit(t *testing.T) {
	// BDD 16/写窗矩阵:onLoad ctx 无 set/merge(阉割即权限)
	deps := HooksDeps{Storage: &memKV{m: map[string]string{}}, Keys: NewKeysStore(testKeysDB(t)), Now: func() time.Time { return time.Unix(0, 0) }}
	pkg := hooksPkg(t, map[string]string{"init.js": `module.exports={onLoad:function(ctx){storage.set("probe", String(ctx.keys.set===undefined && ctx.keys.merge===undefined));}}`})
	rt, err := LoadInit(pkg, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.RunOnLoad(); err != nil {
		t.Fatal(err)
	}
	if v, _ := deps.Storage.Get("probe"); v != "true" {
		t.Fatalf("set visible in onLoad: %v", v)
	}
}
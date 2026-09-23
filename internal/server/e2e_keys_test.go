package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// keysFixture 装配 keys 钩子通道的测试环境(真 wireKeyHooks;trMgr nil = 出站报错,非出站场景不受影响)
func keysFixture(t *testing.T) (*plugin.Registry, adminMuxer) {
	t.Helper()
	pkgs, st := testRegistry(t)
	app := &App{AdminDeps: newAdminDeps(pkgs), St: st}
	wireKeyHooks(app)
	return pkgs, app.AdminDeps.Mux()
}

// adminMuxer 路由面最小抽象
type adminMuxer interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

func TestKeys_ApiWriteReadFlow(t *testing.T) {
	// Given keyWrite 归一化 + keyRead 解释 When PUT→GET→detail→DELETE Then 单键各环节语义成立
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js": `module.exports={
			keyWrite:function(ctx,id,value,old){ if(id==="token" && value){ return String(value).trim(); } return value; },
			keyRead:function(ctx,id){ return {masked:String(ctx.key && ctx.key.data).slice(0,2)+"**"}; },
		}`,
	})); err != nil {
		t.Fatal(err)
	}
	// GET keys:能力标记三 bool
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("get status %d: %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"keyWrite":true`, `"keyRead":true`, `"keyForm":false`} {
		if !containsStr(w.Body.String(), want) {
			t.Fatalf("hooks flags missing %s: %s", want, w.Body.String())
		}
	}
	// PUT 新键:归一化生效(trim);baseUpdatedAt=0 = 创建
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/token",
		strings.NewReader(`{"value":"  tok1  ","baseUpdatedAt":0}`)))
	if w2.Code != http.StatusOK {
		t.Fatalf("put status %d: %s", w2.Code, w2.Body.String())
	}
	var putOut struct {
		UpdatedAt   int64 `json:"updatedAt"`
		Transformed bool  `json:"transformed"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &putOut)
	if !putOut.Transformed {
		t.Fatalf("transformed flag: %s", w2.Body.String())
	}
	if v, ok := pkgs.Keys().Get("checkin", "token"); !ok || v.Data != "tok1" {
		t.Fatalf("normalized value: %+v", v)
	}
	// 旧 baseUpdatedAt 再写 → 409(per-key 冲突)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/token",
		strings.NewReader(`{"value":"x","baseUpdatedAt":0}`)))
	if w3.Code != http.StatusConflict {
		t.Fatalf("stale status %d: %s", w3.Code, w3.Body.String())
	}
	// 二写自动备份:data=tok1 → prev
	e, _ := pkgs.Keys().Get("checkin", "token")
	w3b := httptest.NewRecorder()
	mux.ServeHTTP(w3b, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/token",
		strings.NewReader(`{"value":"tok2","baseUpdatedAt":`+strconv.FormatInt(e.UpdatedAt, 10)+`}`)))
	if w3b.Code != http.StatusOK {
		t.Fatalf("second put status %d: %s", w3b.Code, w3b.Body.String())
	}
	e2, _ := pkgs.Keys().Get("checkin", "token")
	if e2.Data != "tok2" || e2.Prev != "tok1" {
		t.Fatalf("backup: %+v", e2)
	}
	// detail:data + prev + keyRead 解释(仅 detail 触发 keyRead;masked = data 前两位)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys/token/detail", nil))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), "to**") || !containsStr(w4.Body.String(), `"prev":"tok1"`) {
		t.Fatalf("detail status %d: %s", w4.Code, w4.Body.String())
	}
	// DELETE:幂等 200;删除后键消失
	w5 := httptest.NewRecorder()
	mux.ServeHTTP(w5, httptest.NewRequest(http.MethodDelete, "/admin/api/packages/checkin/keys/token", nil))
	if w5.Code != http.StatusOK {
		t.Fatalf("delete status %d: %s", w5.Code, w5.Body.String())
	}
	w6 := httptest.NewRecorder()
	mux.ServeHTTP(w6, httptest.NewRequest(http.MethodDelete, "/admin/api/packages/checkin/keys/token", nil))
	if w6.Code != http.StatusOK {
		t.Fatalf("idempotent delete status %d: %s", w6.Code, w6.Body.String())
	}
	if _, ok := pkgs.Keys().Get("checkin", "token"); ok {
		t.Fatal("key survived delete")
	}
}

func TestKeys_ApiBoundaries(t *testing.T) {
	// HW9 键名非法 400 / 无 keys.js 包 PUT 原样保存(HW1) / 键不存在 detail 404
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
	})); err != nil {
		t.Fatal(err)
	}
	// 键名含 /(路由无法寻址)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/a%2Fb",
		strings.NewReader(`{"value":1,"baseUpdatedAt":0}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("slash key status %d", w.Code)
	}
	// 无 keys.js:PUT 原样保存
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/plain",
		strings.NewReader(`{"value":{"a":1},"baseUpdatedAt":0}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("plain put status %d: %s", w3.Code, w3.Body.String())
	}
	if v, ok := pkgs.Keys().Get("checkin", "plain"); !ok || v.Data == nil {
		t.Fatal("plain put lost")
	}
	// detail:无 keyRead = 200 无 explain(读取不再 501;键数据明文可查)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys/plain/detail", nil))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), `"data"`) {
		t.Fatalf("no keyRead detail status %d: %s", w4.Code, w4.Body.String())
	}
}

func TestKeys_FormFlow(t *testing.T) {
	// Given keyForm/keyAction/keySubmit When form→form-action→form-submit Then 声明展开/按钮回调/创建条目(KF1/KF3/KF4/KF5/KF2)
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js": `module.exports={
			keyForm:function(ctx){ return {fields:[{type:"string",name:"account",description:"账号",required:true},{type:"string",name:"code",description:"验证码"}], actions:[{name:"sendCode",label:"发送验证码"}]}; },
			keyAction:function(ctx,action,values){ if(action==="sendCode"){ return "sent to "+values.account; } return "unknown"; },
			keySubmit:function(ctx,values){
				if(!values.account){ return {errors:{account:"账号必填"}}; }
				ctx.keys.set({token:"t-"+values.account});
				return null;
			},
		}`,
	})); err != nil {
		t.Fatal(err)
	}
	// form 声明
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form", nil))
	if w.Code != http.StatusOK || !containsStr(w.Body.String(), "sendCode") {
		t.Fatalf("form status %d: %s", w.Code, w.Body.String())
	}
	// form-action
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-action",
		strings.NewReader(`{"action":"sendCode","values":{"account":"me@x"}}`)))
	if w2.Code != http.StatusOK || !containsStr(w2.Body.String(), "sent to me@x") {
		t.Fatalf("action status %d: %s", w2.Code, w2.Body.String())
	}
	// form-submit:set 创建新条目(宿主生成 id)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"account":"me@x","code":"123"}}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("submit status %d: %s", w3.Code, w3.Body.String())
	}
	if !containsStr(w3.Body.String(), `"ids"`) {
		t.Fatalf("ids missing: %s", w3.Body.String())
	}
	keys := pkgs.Keys().List("checkin")
	if len(keys) != 1 || keys[0].ID == "" || !strings.HasPrefix(keys[0].ID, "key-") {
		t.Fatalf("created entry: %+v", keys)
	}
	if m := keys[0].Data.(map[string]any); m["token"] != "t-me@x" {
		t.Fatalf("submitted data: %+v", keys[0].Data)
	}
	// 同表单再提交 = 新条目(池入口;同毫秒序号防碰撞)
	w3b := httptest.NewRecorder()
	mux.ServeHTTP(w3b, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"account":"me@x","code":"123"}}`)))
	if w3b.Code != http.StatusOK {
		t.Fatalf("second submit status %d: %s", w3b.Code, w3b.Body.String())
	}
	if n := len(pkgs.Keys().List("checkin")); n != 2 {
		t.Fatalf("entries after resubmit: %d", n)
	}
	// 校验 errors 返回 → 200+ok:false 字段级拒绝,存储不变(KF5;非异常通道)
	nBefore := len(pkgs.Keys().List("checkin"))
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"code":"123"}}`)))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), `"ok":false`) || !containsStr(w4.Body.String(), "账号必填") {
		t.Fatalf("reject: %d %s", w4.Code, w4.Body.String())
	}
	if nAfter := len(pkgs.Keys().List("checkin")); nAfter != nBefore {
		t.Fatalf("rejected submit created entries: %d -> %d", nBefore, nAfter)
	}
	// form 501:无 keyForm 的包(KF2)
	pkgs2, mux2 := keysFixture(t)
	if err := pkgs2.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js":         `module.exports={keyWrite:function(ctx,id,v){return v;}}`,
	})); err != nil {
		t.Fatal(err)
	}
	w5 := httptest.NewRecorder()
	mux2.ServeHTTP(w5, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form", nil))
	if w5.Code != http.StatusNotImplemented {
		t.Fatalf("no form status %d: %s", w5.Code, w5.Body.String())
	}
}

func TestKeys_Timeout500(t *testing.T) {
	// HW11:keyWrite 挂起 → 约 5s 超时 500
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js":         `module.exports={keyWrite:function(ctx,id,v){while(true){}}}`,
	})); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/token",
		strings.NewReader(`{"value":"x","baseUpdatedAt":0}`)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("timeout status %d: %s", w.Code, w.Body.String())
	}
}

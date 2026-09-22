package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	// Given keyWrite 归一化 + keyRead 解释 When PUT→GET→detail Then 各环节语义成立(HW1/HW2/HR1)
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js": `module.exports={
			keyWrite:function(ctx,key,value,old){ if(key==="token" && value){ return String(value).trim(); } return value; },
			keyRead:function(ctx,key){ return {masked:String(ctx.keys.get(key)).slice(0,2)+"**"}; },
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
	// PUT:归一化生效(trim)且轮转
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
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "tok1" {
		t.Fatalf("normalized value: %v", v)
	}
	// 旧 baseUpdatedAt 再写 → 409
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPut, "/admin/api/packages/checkin/keys/token",
		strings.NewReader(`{"value":"x","baseUpdatedAt":0}`)))
	if w3.Code != http.StatusConflict {
		t.Fatalf("stale status %d: %s", w3.Code, w3.Body.String())
	}
	// detail:keyRead 解释
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys/token/detail", nil))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), "to**") {
		t.Fatalf("detail status %d: %s", w4.Code, w4.Body.String())
	}
}

func TestKeys_ApiBoundaries(t *testing.T) {
	// HW9 键名非法 400 / 无 keys.js 包 PUT 原样保存(HW1) + detail 501
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
	if v, _ := pkgs.Keys().Get("checkin", "plain"); v == nil {
		t.Fatal("plain put lost")
	}
	// detail 501(无 keyRead)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/admin/api/packages/checkin/keys/plain/detail", nil))
	if w4.Code != http.StatusNotImplemented {
		t.Fatalf("no keyRead status %d", w4.Code)
	}
}

func TestKeys_FormFlow(t *testing.T) {
	// Given keyForm/keyAction/keySubmit When form→form-action→form-submit Then 声明展开/按钮回调/写入收集(KF1/KF3/KF4/KF5/KF2)
	ctx := context.Background()
	pkgs, mux := keysFixture(t)
	if err := pkgs.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js": `module.exports={
			keyForm:function(ctx){ return {fields:[{type:"string",name:"account",description:"账号",required:true},{type:"string",name:"code",description:"验证码"}], actions:[{name:"sendCode",label:"发送验证码"}]}; },
			keyAction:function(ctx,action,values){ if(action==="sendCode"){ return "sent to "+values.account; } return "unknown"; },
			keySubmit:function(ctx,values){
				if(!values.account){ return {errors:{account:"账号必填"}}; }
				ctx.keys.merge({token:"t-"+values.account});
				ctx.keys.remove("password", "oldToken");
				return "ok";
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
	// form-submit:写入 + written 收集
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"account":"me@x","code":"123"}}`)))
	if w3.Code != http.StatusOK {
		t.Fatalf("submit status %d: %s", w3.Code, w3.Body.String())
	}
	if !containsStr(w3.Body.String(), `"token"`) {
		t.Fatalf("written: %s", w3.Body.String())
	}
	if v, _ := pkgs.Keys().Get("checkin", "token"); v != "t-me@x" {
		t.Fatalf("submitted token: %v", v)
	}
	// 变更集含删除键(密码换 token 后清理无用凭据;先放键再删,验证 remove 真生效)
	if err := pkgs.Keys().Merge("checkin", map[string]any{"password": "p", "oldToken": "ot"}); err != nil {
		t.Fatal(err)
	}
	w6 := httptest.NewRecorder()
	mux.ServeHTTP(w6, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"account":"me@x","code":"123"}}`)))
	if w6.Code != http.StatusOK || !containsStr(w6.Body.String(), `"password"`) {
		t.Fatalf("written with removed: %d %s", w6.Code, w6.Body.String())
	}
	if v, _ := pkgs.Keys().Get("checkin", "password"); v != nil {
		t.Fatalf("password survived submit: %v", v)
	}
	if p, _ := pkgs.Keys().Previous("checkin", "password"); p != "p" {
		t.Fatalf("removed key not in previous: %v", p)
	}
	// 校验 errors 返回 → 200+ok:false 字段级拒绝,存储不变(KF5;非异常通道)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, httptest.NewRequest(http.MethodPost, "/admin/api/packages/checkin/keys/form-submit",
		strings.NewReader(`{"values":{"code":"123"}}`)))
	if w4.Code != http.StatusOK || !containsStr(w4.Body.String(), `"ok":false`) || !containsStr(w4.Body.String(), "账号必填") {
		t.Fatalf("reject: %d %s", w4.Code, w4.Body.String())
	}
	// form 501:无 keyForm 的包(KF2)
	pkgs2, mux2 := keysFixture(t)
	if err := pkgs2.Install(ctx, hooksAAP(t, "0.1.0", map[string]string{
		"tasks/signIn.js": `module.exports={handler:function(ctx){}}`,
		"keys.js":         `module.exports={keyWrite:function(ctx,k,v){return v;}}`,
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
		"keys.js":         `module.exports={keyWrite:function(ctx,k,v){while(true){}}}`,
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

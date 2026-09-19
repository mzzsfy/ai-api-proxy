package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func login(t *testing.T, s *Service, user, pass string) (*httptest.ResponseRecorder, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"user": user, "password": pass})
	req := httptest.NewRequest("POST", "/login", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Login(w, req)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	token := ""
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookie {
			token = c.Value
		}
	}
	return w, token
}

func TestLogin_OKAndSession(t *testing.T) {
	// Given 随机生成口令 When 以该明文登录 Then 200 + 会话 cookie 可过中间件
	s, plain, err := New("admin", "")
	if err != nil {
		t.Fatal(err)
	}
	if plain == "" {
		t.Fatal("random password expected")
	}
	w, token := login(t, s, "admin", plain)
	if w.Code != 200 {
		t.Fatalf("login status: %d %s", w.Code, w.Body.String())
	}
	if token == "" {
		t.Fatal("no session cookie")
	}
	req := httptest.NewRequest("GET", "/me", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	rec := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.Me(w, r) })).ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("middleware: %d", rec.Code)
	}
}

func TestLogin_WrongPassword401(t *testing.T) {
	// Given 错误口令 When Login Then 401
	s, _, _ := New("admin", "")
	w, _ := login(t, s, "admin", "wrong")
	if w.Code != 401 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestLogin_BcryptConfigured(t *testing.T) {
	// Given 配置 bcrypt 口令 When 以明文登录 Then 通过
	// (bcrypt 哈希由部署生成;此处自造一圈)
	hash, err := BcryptHash("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	s, plain, _ := New("admin", hash)
	if plain != "" {
		t.Fatal("configured password must not generate plain")
	}
	w, token := login(t, s, "admin", "s3cret")
	if w.Code != 200 || token == "" {
		t.Fatalf("bcrypt login: %d", w.Code)
	}
}

func TestLogin_BruteForceLock(t *testing.T) {
	// Given 连续 5 次失败 When 第 6 次(即便正确) Then 429 锁定
	s, plain, _ := New("admin", "")
	for range 5 {
		w, _ := login(t, s, "admin", "wrong")
		if w.Code != 401 {
			t.Fatalf("expect 401, got %d", w.Code)
		}
	}
	w, _ := login(t, s, "admin", plain)
	if w.Code != 429 {
		t.Fatalf("expect 429, got %d", w.Code)
	}
}

func TestMiddleware_Unauthenticated401(t *testing.T) {
	// Given 无会话 When 访问受保护路由 Then 401
	s, _, _ := New("admin", "")
	req := httptest.NewRequest("GET", "/admin/api/upstreams", nil)
	rec := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatalf("status: %d", rec.Code)
	}
}

func TestMiddleware_CSRFUploadRequiresSameOrigin(t *testing.T) {
	// Given 会话有效 + zip 上传但跨源 Origin When 请求 Then 403
	s, plain, _ := New("admin", "")
	_, token := login(t, s, "admin", plain)
	req := httptest.NewRequest("POST", "/admin/api/packages", strings.NewReader("zip-bytes"))
	req.Header.Set("Content-Type", "application/zip")
	req.Header.Set("Origin", "https://evil.example")
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	rec := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Fatalf("csrf upload: %d", rec.Code)
	}
	// 同源放行
	req2 := httptest.NewRequest("POST", "/admin/api/packages", strings.NewReader("zip-bytes"))
	req2.Header.Set("Content-Type", "application/zip")
	req2.Header.Set("Sec-Fetch-Site", "same-origin")
	req2.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	rec2 := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec2, req2)
	if rec2.Code != 200 {
		t.Fatalf("same origin upload: %d", rec2.Code)
	}
}

func TestMiddleware_EmptyBodyRouteCSRF(t *testing.T) {
	// Given 空体写路由(如 test)When 跨源 Then 403;同源 Then 放行
	s, plain, _ := New("admin", "")
	_, token := login(t, s, "admin", plain)
	mk := func(origin string) *http.Request {
		req := httptest.NewRequest("POST", "/admin/api/upstreams/1/test", nil)
		req.ContentLength = 0
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
		return req
	}
	rec := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec, mk("https://evil.example"))
	if rec.Code != 403 {
		t.Fatalf("empty body csrf: %d", rec.Code)
	}
	rec2 := httptest.NewRecorder()
	s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec2, mk(""))
	if rec2.Code != 200 {
		t.Fatalf("empty body no-origin: %d", rec2.Code)
	}
}

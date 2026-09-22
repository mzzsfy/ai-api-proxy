// Package admin 管理 REST API:登录会话/上游与包 CRUD/metrics live
package admin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// 会话参数
const (
	sessionCookie  = "aap_session"
	bruteForceMax  = 5
	bruteForceLock = time.Minute
)

// Service 管理 API 服务
type Service struct {
	user     string
	passHash []byte
	hmacKey  []byte

	mu       sync.Mutex
	sessions map[string]time.Time
	fails    int
	lockedAt time.Time
}

// BcryptHash 明文 → bcrypt(部署生成口令哈希;MinCost 仅供测试,生产由部署侧生成)
func BcryptHash(plain string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.MinCost)
	return string(b), err
}

// bcryptVersions bcrypt 哈希版本前缀,命中即按哈希解析而非明文
var bcryptVersions = []string{"$2a$", "$2b$", "$2x$", "$2y$"}

func isBcryptHash(v string) bool {
	for _, p := range bcryptVersions {
		if strings.HasPrefix(v, p) {
			return true
		}
	}
	return false
}

// hashPass 生产口令哈希,统一成本
func hashPass(plain string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
}

// New 构造;配置值支持明文或 bcrypt 哈希($2a$/$2b$/$2x$/$2y$ 开头按哈希解析,仅校验结构格式,哈希体错误会在登录时暴露),其余非空按明文,为空则生成随机口令(返回明文供启动打印一次)
func New(user, pass string) (*Service, string, error) {
	s := &Service{
		user:     user,
		hmacKey:  make([]byte, 32),
		sessions: map[string]time.Time{},
	}
	if _, err := rand.Read(s.hmacKey); err != nil {
		return nil, "", err
	}
	var plain string
	switch {
	case pass == "":
		b := make([]byte, 18)
		if _, err := rand.Read(b); err != nil {
			return nil, "", err
		}
		plain = base64.RawURLEncoding.EncodeToString(b)
		hash, err := hashPass(plain)
		if err != nil {
			return nil, "", err
		}
		s.passHash = hash
	case isBcryptHash(pass):
		// 结构格式非法则启动失败,哈希体错误留待登录比较暴露
		if _, err := bcrypt.Cost([]byte(pass)); err != nil {
			return nil, "", fmt.Errorf("admin_pass_bcrypt: %w", err)
		}
		s.passHash = []byte(pass)
	default:
		hash, err := hashPass(pass)
		if err != nil {
			return nil, "", fmt.Errorf("hash admin pass: %w", err)
		}
		s.passHash = hash
	}
	return s, plain, nil
}

// Login POST /login
func (s *Service) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad request")
		return
	}
	s.mu.Lock()
	if s.lockedAt.After(time.Now()) {
		s.mu.Unlock()
		httpError(w, http.StatusTooManyRequests, "locked, retry later")
		return
	}
	userOK := subtle.ConstantTimeCompare([]byte(req.User), []byte(s.user)) == 1
	passOK := bcrypt.CompareHashAndPassword(s.passHash, []byte(req.Password)) == nil
	if !userOK || !passOK {
		s.fails++
		if s.fails >= bruteForceMax {
			s.lockedAt = time.Now().Add(bruteForceLock)
			s.fails = 0
		}
		s.mu.Unlock()
		httpError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	s.fails = 0
	token := s.newToken()
	s.sessions[token] = time.Now().Add(24 * time.Hour)
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, map[string]any{"ok": true})
}

// newToken 生成 HMAC 签名会话令牌
func (s *Service) newToken() string {
	raw := make([]byte, 24)
	_, _ = rand.Read(raw)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, s.hmacKey)
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// verifyToken 校验令牌签名与有效期;顺带清理过期会话(低频登录,一次遍历成本可忽略)
func (s *Service) verifyToken(token string) bool {
	dot := strings.LastIndex(token, ".")
	if dot <= 0 || dot == len(token)-1 {
		return false
	}
	payload, sigEnc := token[:dot], token[dot+1:]
	sig, err := base64.RawURLEncoding.DecodeString(sigEnc)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, s.hmacKey)
	mac.Write([]byte(payload))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for t, exp := range s.sessions {
		if exp.Before(now) {
			delete(s.sessions, t)
		}
	}
	exp, ok := s.sessions[token]
	return ok && exp.After(now)
}

// Logout POST /logout
func (s *Service) Logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.mu.Lock()
		delete(s.sessions, c.Value)
		s.mu.Unlock()
	}
	writeJSON(w, map[string]any{"ok": true})
}

// Me GET /me
func (s *Service) Me(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"user": s.user})
}

// Middleware 会话校验(除豁免路径);CSRF:写方法校验 Content-Type/Origin 同源
func (s *Service) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil || !s.verifyToken(c.Value) {
			httpError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			ct := r.Header.Get("Content-Type")
			isJSON := strings.HasPrefix(ct, "application/json")
			isUpload := strings.HasPrefix(ct, "application/zip") || strings.HasPrefix(ct, "application/octet-stream")
			emptyBody := r.ContentLength == 0
			if !isJSON && !isUpload && !emptyBody {
				httpError(w, http.StatusForbidden, "csrf: content-type")
				return
			}
			if !isJSON && !sameOrigin(r) {
				httpError(w, http.StatusForbidden, "csrf: origin")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// sameOrigin Origin/Sec-Fetch-Site 同源校验(上传与空体路由)
func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" {
		return site == "same-origin"
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return u.Host == r.Host
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

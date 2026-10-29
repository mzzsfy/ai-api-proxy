package admin

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

func TestLogin_LockExpiresAndCorrectCredentialsRecover(t *testing.T) {
	// Given 暴力登录触发锁定 When 等待锁窗口结束 Then 正确凭据恢复登录
	synctest.Test(t, func(t *testing.T) {
		hash, err := BcryptHash("password")
		if err != nil {
			t.Fatal(err)
		}
		s, _, err := New("admin", hash)
		if err != nil {
			t.Fatal(err)
		}
		for range 5 {
			w, _ := login(t, s, "admin", "wrong")
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("failed credential status = %d", w.Code)
			}
		}
		w, _ := login(t, s, "admin", "password")
		if w.Code != http.StatusTooManyRequests {
			t.Fatalf("locked status = %d", w.Code)
		}
		time.Sleep(time.Minute + time.Second)
		w, token := login(t, s, "admin", "password")
		if w.Code != http.StatusOK || token == "" {
			t.Fatalf("expired lock status = %d, token=%q", w.Code, token)
		}
	})
}

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKeyAuth_RejectsInvalidCredentialsBeforeHandler(t *testing.T) {
	// Given 固定入口密钥 When 不同认证头请求 Then 仅有效密钥能执行后续处理器
	cases := []struct {
		name, bearer, apiKey string
		want                 int
	}{
		{"missing", "", "", http.StatusUnauthorized},
		{"wrong_bearer", "Bearer wrong", "", http.StatusUnauthorized},
		{"wrong_api_key", "", "wrong", http.StatusUnauthorized},
		{"valid_bearer", "Bearer secret", "", http.StatusNoContent},
		{"valid_api_key", "", "secret", http.StatusNoContent},
		{"api_key_takes_precedence", "Bearer secret", "wrong", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(http.StatusNoContent) })
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set("Authorization", tc.bearer)
			req.Header.Set("x-api-key", tc.apiKey)
			response := httptest.NewRecorder()
			keyAuth([]string{"secret"}, next).ServeHTTP(response, req)
			if response.Code != tc.want || called != (tc.want == http.StatusNoContent) {
				t.Fatalf("status=%d called=%v; want status=%d", response.Code, called, tc.want)
			}
		})
	}
}

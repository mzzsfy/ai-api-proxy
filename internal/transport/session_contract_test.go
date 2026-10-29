package transport

import (
	"crypto/sha256"
	"testing"
)

func TestSessionKey_EmptyModelUsesSingleMaterial(t *testing.T) {
	// Given 同一密钥且模型为空或非空 When 计算会话键 Then 空模型不添加分隔符而有模型时保留分隔
	cases := []struct{ name, model, material string }{
		{"empty", "", "key"},
		{"model", "model", "key\x00model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			digest := sha256.Sum256([]byte(tc.material))
			if got := sessionKey("key", tc.model); got != string(digest[:16]) {
				t.Fatalf("session key = %x; want %x", got, digest[:16])
			}
		})
	}
}

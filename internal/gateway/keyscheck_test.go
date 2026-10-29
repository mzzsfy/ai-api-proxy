package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/upstream"
)

// TestKeysCheck_KeysStoreNotWired Keys 存储未装配 → 参数级错误(契约:Keys==nil→400)
func TestKeysCheck_KeysStoreNotWired(t *testing.T) {
	g := &Gateway{}
	_, err := g.KeysCheck(context.Background(), &upstream.Group{ID: 1, Plugin: "openai-compatible", Enabled: true}, KeysCheckRequest{})
	if !errors.Is(err, ErrKeysCheckParam) {
		t.Fatalf("Keys nil: %v", err)
	}
}

package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_FileOverridesDefaults(t *testing.T) {
	// Given 含完整字段的配置文件 When Load Then 字段与文件一致
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	content := `
listen: ":9999"
data_dir: "/tmp/d"
api_keys: ["k1", "k2"]
admin_user: "ops"
log_level: "debug"
plugins_dir: "./p"
transports:
  - name: relay
    type: http_proxy
    url: "http://a:b@h:1"
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != ":9999" || cfg.DataDir != "/tmp/d" || len(cfg.APIKeys) != 2 {
		t.Fatalf("scalar fields mismatch: %+v", cfg)
	}
	if len(cfg.Transports) != 1 || cfg.Transports[0].Name != "relay" || cfg.Transports[0].Type != "http_proxy" {
		t.Fatalf("transports mismatch: %+v", cfg.Transports)
	}
}

func TestLoad_MissingFileError(t *testing.T) {
	// Given 不存在的配置文件路径 When Load Then 报错且含路径
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("expect error for missing file")
	}
}

func TestValidate_EmptyAPIKeysRefused(t *testing.T) {
	// Given 空 APIKeys When Validate Then 拒绝启动
	cfg := &Config{Listen: ":0"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty api_keys must be refused")
	}
	cfg.APIKeys = []string{"k"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidate_TransportRules(t *testing.T) {
	// Given 传输实例缺名或缺 URL When Validate Then 拒绝
	base := &Config{APIKeys: []string{"k"}}
	bad := []TransportCfg{
		{Name: "", Type: "direct"},
		{Name: "p", Type: "http_proxy", URL: ""},
		{Name: "x", Type: "grpc", URL: "u"},
	}
	for i, tc := range bad {
		cfg := *base
		cfg.Transports = []TransportCfg{tc}
		if err := (&cfg).Validate(); err == nil {
			t.Fatalf("case %d should fail: %+v", i, tc)
		}
	}
}

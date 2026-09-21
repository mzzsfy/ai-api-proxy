package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
	if len(cfg.Transports) != 1 || cfg.Transports[0].Name != "relay" || cfg.Transports[0].URL != "http://a:b@h:1" {
		t.Fatalf("transports mismatch: %+v", cfg.Transports)
	}
}

func TestLoad_MissingFileReleasesExample(t *testing.T) {
	// Given 不存在的配置路径 When Load Then 释放示例配置且解析成功
	p := filepath.Join(t.TempDir(), "sub", "config.yaml")
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load with missing file: %v", err)
	}
	if len(cfg.APIKeys) == 0 {
		t.Fatalf("released example should contain api_keys: %+v", cfg)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("released file unreadable: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("released example config is empty")
	}
}

func TestLoad_ExistingFileNotOverwritten(t *testing.T) {
	// Given 已存在的配置文件 When Load Then 内容不被覆盖
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("listen: \":9999\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err != nil {
		t.Fatalf("Load: %v", err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "listen: \":9999\"\n" {
		t.Fatalf("existing config overwritten: %q", b)
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
	// Given 传输实例缺名或非法 URL/字段 When Validate Then 拒绝
	base := &Config{APIKeys: []string{"k"}}
	bad := []TransportCfg{
		{Name: "", URL: "direct"},
		{Name: "p", URL: ""},
		{Name: "x", URL: "grpc://u"},                                    // 未知 scheme
		{Name: "y", URL: "aap://h:1"},                                   // 缺 token
		{Name: "z", URL: "aap://h:1?token=" + strings.Repeat("t", 256)}, // token 超长
	}
	for i, tc := range bad {
		cfg := *base
		cfg.Transports = []TransportCfg{tc}
		if err := (&cfg).Validate(); err == nil {
			t.Fatalf("case %d should fail: %+v", i, tc)
		}
	}
}

func TestValidate_TransportOptionsWhitelist(t *testing.T) {
	// Given options 白名单规则 When Validate Then 仅 aap 允许 affinity_ttl
	base := &Config{APIKeys: []string{"k"}}
	good := []TransportCfg{
		{Name: "a", URL: "aap://h:1?token=t", Options: yamlNode(t, map[string]any{"affinity_ttl": 3600})},
		{Name: "b", URL: "direct"},
	}
	for i, tc := range good {
		cfg := *base
		cfg.Transports = []TransportCfg{tc}
		if err := (&cfg).Validate(); err != nil {
			t.Fatalf("case %d should pass: %v", i, err)
		}
	}
	bad := []TransportCfg{
		{Name: "c", URL: "aap://h:1?token=t", Options: yamlNode(t, map[string]any{"unknown": 1})},
		{Name: "d", URL: "direct", Options: yamlNode(t, map[string]any{"x": 1})}, // 非 aap 有 options
	}
	for i, tc := range bad {
		cfg := *base
		cfg.Transports = []TransportCfg{tc}
		if err := (&cfg).Validate(); err == nil {
			t.Fatalf("bad case %d should fail", i)
		}
	}
}

// yamlNode 测试辅助:map → yaml.Node
func yamlNode(t *testing.T, m map[string]any) yaml.Node {
	t.Helper()
	b, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var n yaml.Node
	if err := yaml.Unmarshal(b, &n); err != nil {
		t.Fatal(err)
	}
	return *n.Content[0]
}

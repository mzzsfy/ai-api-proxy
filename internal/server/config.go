// Package server 提供配置加载:config.yaml > 环境变量 > 默认值
package server

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// TransportCfg 命名传输实例定义(部署资产,仅 yaml 可表达嵌套)
type TransportCfg struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // direct | http_proxy | socks5
	URL  string `yaml:"url"`
}

// Config 服务配置
type Config struct {
	Listen          string         `yaml:"listen"`
	DataDir         string         `yaml:"data_dir"`
	APIKeys         []string       `yaml:"api_keys"`
	AdminUser       string         `yaml:"admin_user"`
	AdminPassBcrypt string         `yaml:"admin_pass_bcrypt"`
	LogLevel        string         `yaml:"log_level"`
	PluginsDir      string         `yaml:"plugins_dir"`
	Transports      []TransportCfg `yaml:"transports"`
	// 传输健康探测周期(秒;0=关闭周期探测,仅保留手动测试)
	TransportProbeIntervalSec int `yaml:"transport_probe_interval_sec"`
}

// Load 按优先级合并配置;空 APIKeys 视为拒绝启动的调用方职责
func Load(path string) (*Config, error) {
	cfg := &Config{}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := yaml.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	applyEnv(cfg)
	applyDefaults(cfg)
	return cfg, nil
}

// applyEnv 扁平标量的环境变量覆盖(API_PROXY_ 前缀)
func applyEnv(cfg *Config) {
	if v := os.Getenv("API_PROXY_LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := os.Getenv("API_PROXY_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("API_PROXY_API_KEYS"); v != "" {
		cfg.APIKeys = strings.Split(v, ",")
	}
	if v := os.Getenv("API_PROXY_ADMIN_USER"); v != "" {
		cfg.AdminUser = v
	}
	if v := os.Getenv("API_PROXY_ADMIN_PASS_BCRYPT"); v != "" {
		cfg.AdminPassBcrypt = v
	}
	if v := os.Getenv("API_PROXY_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("API_PROXY_PLUGINS_DIR"); v != "" {
		cfg.PluginsDir = v
	}
}

func applyDefaults(cfg *Config) {
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.PluginsDir == "" {
		cfg.PluginsDir = "./plugins"
	}
	if cfg.AdminUser == "" {
		cfg.AdminUser = "admin"
	}
}

// Validate 启动期校验;无 key 拒绝启动
func (c *Config) Validate() error {
	if len(c.APIKeys) == 0 {
		return fmt.Errorf("api_keys is empty: refuse to start")
	}
	for i, t := range c.Transports {
		if t.Name == "" {
			return fmt.Errorf("transports[%d]: name required", i)
		}
		switch t.Type {
		case "direct":
		case "http_proxy", "socks5":
			if t.URL == "" {
				return fmt.Errorf("transports[%s]: url required", t.Name)
			}
		default:
			return fmt.Errorf("transports[%s]: unknown type %q", t.Name, t.Type)
		}
	}
	return nil
}

// BcryptCost bcrypt 计算成本(注释指语义,值随库常量演进)
const BcryptCost = 10

// envInt 读整型环境变量,缺省返回 def(预留逃生口用)
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

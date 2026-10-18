// Package server 提供配置加载:config.yaml > 环境变量 > 默认值
package server

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/mzzsfy/ai-api-proxy/configexample"
	"github.com/mzzsfy/ai-api-proxy/ipprovider"
)

// yamlUnmarshal yaml 节点 → 目标(options 展开)
func yamlUnmarshal(node *yaml.Node, target any) error {
	return node.Decode(target)
}

// TransportCfg 命名传输实例定义(部署资产,仅 yaml 可表达嵌套)
type TransportCfg struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"` // scheme 判型:direct(字面值)|socks5://|http://|https://|aap://|ipp+<kind>://
	// Options 供给方私有配置(aap 仅 affinity_ttl;ipp+ 键由供给方定义;其余 scheme 必须为空)
	Options yaml.Node `yaml:"options"`
}

// Config 服务配置
type Config struct {
	Listen          string   `yaml:"listen"`
	DataDir         string   `yaml:"data_dir"`
	APIKeys         []string `yaml:"api_keys"`
	AdminUser       string   `yaml:"admin_user"`
	AdminPassBcrypt string   `yaml:"admin_pass_bcrypt"`
	LogLevel        string   `yaml:"log_level"`
	// PluginsDir 普通插件目录(裸 .aap 或含 manifest.json 的包目录;同名后者胜)
	PluginsDir string `yaml:"plugins_dir"`
	// BuiltinDir 内置插件目录(不入库的分发资产;与普通目录同形,同名唯一)
	BuiltinDir string `yaml:"builtin_dir"`
	// PackDir 打包输出目录(非空则打包两级目录内的包到该目录后退出;唯一 .aap 产出路径)
	PackDir string `yaml:"pack_dir"`
	// HistoryRetentionDays 请求历史保留天数(独立库 history.db);<0 永久保留,0 视为未配置取默认
	HistoryRetentionDays int `yaml:"history_retention_days"`
	// ModelsListing /v1/models 列表模式:full(全路径,默认)|short(短名,歧义名轮询)|both(全路径∪短名)
	ModelsListing string `yaml:"models_listing"`
	// HandlerVMMaxBorrow handler 池实例借用上限,超过即丢(yaml 绑定不走 applyEnv;≤0 = 缺省 100)
	HandlerVMMaxBorrow int `yaml:"handler_vm_max_borrow"`
	// HandlerRSSSoftPercent RSS 看门狗软限(物理内存百分比;≤0 = 关闭;硬限 = 软限×1.5,独立进程退出)
	HandlerRSSSoftPercent int `yaml:"handler_rss_soft_percent"`
	// PoolSize JS 池常驻实例上限(每池;归还保留的空闲数,弹性新建总量归 VM 预算;≤0 = 缺省)
	PoolSize int `yaml:"pool_size"`
	// PoolWaitMS 池空闲等待窗(窗内归还即复用,超窗新建;≤0 = 缺省)
	PoolWaitMS int `yaml:"pool_wait_ms"`
	Transports    []TransportCfg `yaml:"transports"`
}

// defaultHistoryRetentionDays 未配置时的请求历史保留天数
const defaultHistoryRetentionDays = 7

// Load 按优先级合并配置;空 APIKeys 视为拒绝启动的调用方职责。
// 配置文件不存在时释放内嵌示例配置到该路径(缺失才写,不覆盖),再行读取
func Load(path string) (*Config, error) {
	cfg := &Config{}
	if path != "" {
		if err := releaseExample(path); err != nil {
			return nil, err
		}
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

// releaseExample 配置文件缺失时把内嵌示例写到 path(含父目录创建);已存在则不动
func releaseExample(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat config %s: %w", path, err)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, configexample.Example, 0o600); err != nil {
		return fmt.Errorf("release example config %s: %w", path, err)
	}
	slog.Info("config not found: released example config (edit api_keys before production use)", "path", path)
	return nil
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
	if v := os.Getenv("API_PROXY_BUILTIN_DIR"); v != "" {
		cfg.BuiltinDir = v
	}
	if v := os.Getenv("API_PROXY_PACK_DIR"); v != "" {
		cfg.PackDir = v
	}
	if v := os.Getenv("API_PROXY_HISTORY_RETENTION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HistoryRetentionDays = n
		}
	}
	if v := os.Getenv("API_PROXY_MODELS_LISTING"); v != "" {
		cfg.ModelsListing = v
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
	if cfg.BuiltinDir == "" {
		cfg.BuiltinDir = "./builtin-plugins"
	}
	if cfg.AdminUser == "" {
		cfg.AdminUser = "admin"
	}
	if cfg.HistoryRetentionDays == 0 {
		cfg.HistoryRetentionDays = defaultHistoryRetentionDays
	}
	if cfg.ModelsListing == "" {
		cfg.ModelsListing = "full"
	}
}

// EnsureDirs 建齐数据/插件/打包目录
func (c *Config) EnsureDirs() error {
	for _, d := range []string{c.DataDir, c.PluginsDir, c.BuiltinDir, c.PackDir} {
		if d == "" {
			continue
		}
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	return nil
}

// Validate 启动期校验;无 key 拒绝启动;PackDir 非空 = 打包模式(只打包不服务)
func (c *Config) Validate() error {
	if c.PackDir != "" {
		return nil
	}
	if len(c.APIKeys) == 0 {
		return fmt.Errorf("api_keys is empty: refuse to start")
	}
	switch c.ModelsListing {
	case "", "full", "short", "both": // 空 = 未配置(Load 会取默认 full;直接构造 Config 的调用方合法)
	default:
		return fmt.Errorf("models_listing must be full|short|both, got %q", c.ModelsListing)
	}
	if _, ok := logLevels[c.LogLevel]; !ok && c.LogLevel != "" {
		// 空 = 未配置(Load 会取默认 info;直接构造 Config 的调用方合法)
		return fmt.Errorf("log_level must be debug|info|warn|error, got %q", c.LogLevel)
	}
	for i, t := range c.Transports {
		if t.Name == "" {
			return fmt.Errorf("transports[%d]: name required", i)
		}
		if err := validateTransportURL(t.Name, t.URL); err != nil {
			return err
		}
		if err := validateTransportOptions(t.Name, t.URL, t.Options); err != nil {
			return err
		}
	}
	return nil
}

// validateTransportURL scheme 判型校验
func validateTransportURL(name, raw string) error {
	if raw == "direct" { // 精确字节匹配,大小写敏感
		return nil
	}
	scheme, rest, found := strings.Cut(raw, "://")
	if !found || rest == "" {
		return fmt.Errorf("transports[%s]: url 须为 direct 或带 scheme 的完整地址, got %q", name, raw)
	}
	switch scheme {
	case "http", "https", "socks5", "aap":
		if scheme == "aap" {
			if !strings.Contains(raw, "token=") {
				return fmt.Errorf("transports[%s]: aap url 须含 token", name)
			}
			if idx := strings.Index(raw, "token="); idx >= 0 {
				token := raw[idx+len("token="):]
				if end := strings.IndexByte(token, '&'); end >= 0 {
					token = token[:end]
				}
				if token == "" || len(token) > 255 {
					return fmt.Errorf("transports[%s]: aap token 长度须 1..255", name)
				}
			}
		}
		return nil
	default:
		if !strings.HasPrefix(scheme, "ipp+") || len(scheme) <= len("ipp+") {
			return fmt.Errorf("transports[%s]: unknown url scheme %q", name, scheme)
		}
		if !ipproviderKindRegistered("ipp_" + scheme[len("ipp+"):]) {
			return fmt.Errorf("transports[%s]: provider %q not registered(缺构建标签或未注册)", name, scheme)
		}
		return nil
	}
}

// validateTransportOptions options 白名单:aap 仅 affinity_ttl,ipp+ 任意键(供给方构建期严格解码),其余必须为空
func validateTransportOptions(name, raw string, node yaml.Node) error {
	if node.Kind == 0 {
		return nil
	}
	var opts map[string]any
	if err := node.Decode(&opts); err != nil {
		return fmt.Errorf("transports[%s].options: %w", name, err)
	}
	if strings.HasPrefix(raw, "ipp+") {
		return nil
	}
	if !strings.HasPrefix(raw, "aap://") {
		if len(opts) > 0 {
			return fmt.Errorf("transports[%s]: 仅 aap 与 ipp+ 传输支持 options", name)
		}
		return nil
	}
	for k := range opts {
		if k != "affinity_ttl" {
			return fmt.Errorf("transports[%s]: unknown aap option %q", name, k)
		}
	}
	return nil
}

// ipproviderKindRegistered 供给方类型是否已注册
func ipproviderKindRegistered(kind string) bool {
	for _, k := range ipprovider.Kinds() {
		if k == kind {
			return true
		}
	}
	return false
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

// logLevels 合法日志级别名 → slog 级别(Validate 与 parseLogLevel 共用的单一来源)
var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// parseLogLevel 日志级别名 → slog 级别;未知回退 info(Validate 负责拒绝非法配置)
func parseLogLevel(name string) slog.Level {
	if lv, ok := logLevels[name]; ok {
		return lv
	}
	return slog.LevelInfo
}

// applyLogLevel 把配置级别挂到 log/slog 默认桥,使 slog 输出按级别过滤
func applyLogLevel(cfg *Config) {
	slog.SetLogLoggerLevel(parseLogLevel(cfg.LogLevel))
}

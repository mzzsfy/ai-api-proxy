// Package plugin .aap 包(zip)解析/校验/加载与 goja 部件绑定
package plugin

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// ManifestVersion 当前支持的 manifest 版本
const ManifestVersion = 1

// ProtocolSlot 协议槽全名(插件协议槽与入口协议共用的枚举;唯一定义在 pipeline)
type ProtocolSlot = pipeline.ProtocolSlot

// 协议全名枚举别名(调用点可读性)
const (
	ProtocolOpenAICompletions = pipeline.ProtocolOpenAICompletions
	ProtocolAnthropicMessages = pipeline.ProtocolAnthropicMessages
)

// Form 请求形态(流式/非流式;协议槽声明的可处理形态枚举;唯一定义在 pipeline)
type Form = pipeline.Form

// 形态枚举别名
const (
	FormStreaming    = pipeline.FormStreaming
	FormNonStreaming = pipeline.FormNonStreaming
)

// ProtocolPart protocol 部件声明(声明式单协议:一个包恰服务一种协议)
type ProtocolPart struct {
	Entry      string   `json:"entry"`
	Protocol   string   `json:"protocol"`
	Form       []string `json:"form"`
	Features   []string `json:"features"`
	SecretRefs []string `json:"secretRefs"`
}

// FilterPart filter 部件声明
type FilterPart struct {
	Name         string          `json:"name"`
	Entry        string          `json:"entry"`
	ConfigSchema json.RawMessage `json:"configSchema,omitempty"`
	SecretRefs   []string        `json:"secretRefs,omitempty"`
}

// HooksTask 定时任务声明(crontab 心智模型:每行 = 一时刻 + 一命令;调度形态 cron/next 互斥)
type HooksTask struct {
	Name      string `json:"name"`
	Cron      string `json:"cron,omitempty"`      // 宿主调度形态(单值)
	Next      bool   `json:"next,omitempty"`      // 自调度形态(与 cron 互斥):文件须导出 next(ctx)
	Entry     string `json:"entry,omitempty"`     // 缺省 tasks/<name>.js(相对包根;多行可共用一实现)
	TimeoutMs int64  `json:"timeoutMs,omitempty"` // ≤0 按缺省;上限钳制在运行时
}

// HooksPart hooks 部件声明(定时任务;计入合法插槽)
type HooksPart struct {
	Tasks []HooksTask `json:"tasks,omitempty"`
}

// Manifest 包元数据与模板载体
type Manifest struct {
	ManifestVersion int               `json:"manifestVersion"`
	Name            string            `json:"name"`
	Version         string            `json:"version"`
	Title           string            `json:"title,omitempty"`           // 显示名(缺省回退 name;纯展示)
	Description     string            `json:"description,omitempty"`     // 一句话描述(纯展示)
	Author          string            `json:"author,omitempty"`          // 纯展示
	Homepage        string            `json:"homepage,omitempty"`        // 纯展示
	License         string            `json:"license,omitempty"`         // 纯展示
	Compat          map[string]string `json:"compat,omitempty"`
	Parts           struct {
		Protocol *ProtocolPart `json:"protocol,omitempty"`
		Filters  []FilterPart  `json:"filters,omitempty"`
		Hooks    *HooksPart    `json:"hooks,omitempty"`
	} `json:"parts"`
	UpstreamTemplate json.RawMessage            `json:"upstreamTemplate,omitempty"`
	ConfigSchema     json.RawMessage            `json:"configSchema,omitempty"`
	KeySchema        json.RawMessage            `json:"keySchema,omitempty"` // 包级 key 键声明(仅 GUI 提示,不校验)
	Extra            map[string]json.RawMessage `json:"-"`
}

// MetaTitle 显示名(缺省回退 name;Inspect 摘要用)
func (m *Manifest) MetaTitle() string {
	if m.Title != "" {
		return m.Title
	}
	return m.Name
}

// MetaTitle 显示名(缺省回退 name)
func (p *Package) MetaTitle() string { return p.Manifest.MetaTitle() }

// Package 解析后的包(部件代码按 entry 存放)
type Package struct {
	Manifest    *Manifest
	Files       map[string][]byte // 文件路径 → 源码(多文件布局;族文件 + lib/*.js)
	Revision    int64
	Declaration map[string]any // 合并后的 settings 声明(安装/升级/在线保存时提取;declaration_json 持久化)
}

// HasProtocol 是否含 protocol 部件(主包判定)
func (p *Package) HasProtocol() bool { return p.Manifest.Parts.Protocol != nil }

// Validate 安装期校验:manifest 一致性 + 部件声明绑定(编译校验在加载期)
func (p *Package) Validate() error {
	m := p.Manifest
	if m.ManifestVersion != ManifestVersion {
		return fmt.Errorf("manifestVersion %d unsupported (want %d)", m.ManifestVersion, ManifestVersion)
	}
	if m.Name == "" {
		return fmt.Errorf("name required")
	}
	// upstream: 前缀 = target secrets 的 kv ns,插件包重名可读写凭据——安装期堵死
	if strings.HasPrefix(m.Name, "upstream:") {
		return fmt.Errorf("package name %q: reserved prefix", m.Name)
	}
	if m.Version == "" {
		return fmt.Errorf("version required")
	}
	proto := m.Parts.Protocol
	filters := m.Parts.Filters
	// 至少含一层插槽(hooks 计入)
	if proto == nil && len(filters) == 0 && m.Parts.Hooks == nil {
		return fmt.Errorf("package has no parts: need protocol, filters or hooks")
	}
	// protocol 声明绑定:协议全名 + form 必填至少一
	if proto != nil {
		if proto.Protocol != string(ProtocolOpenAICompletions) && proto.Protocol != string(ProtocolAnthropicMessages) {
			return fmt.Errorf("protocol: unknown protocol %q (want %s | %s)",
				proto.Protocol, ProtocolOpenAICompletions, ProtocolAnthropicMessages)
		}
		if len(proto.Form) == 0 {
			return fmt.Errorf("protocol: form required (at least one of streaming/non_streaming)")
		}
		for _, f := range proto.Form {
			if f != string(FormStreaming) && f != string(FormNonStreaming) {
				return fmt.Errorf("protocol: unknown form %q", f)
			}
		}
		if proto.Entry == "" {
			return fmt.Errorf("protocol: entry required")
		}
		if _, ok := p.Files[proto.Entry]; !ok {
			return fmt.Errorf("protocol: entry %q missing in package", proto.Entry)
		}
	}
	// filter 声明绑定:name 包内唯一 + entry 存在
	seen := map[string]bool{}
	for _, fp := range filters {
		if fp.Name == "" {
			return fmt.Errorf("filter: name required")
		}
		if seen[fp.Name] {
			return fmt.Errorf("filter %q duplicated", fp.Name)
		}
		seen[fp.Name] = true
		if fp.Entry == "" {
			return fmt.Errorf("filter %s: entry required", fp.Name)
		}
		if _, ok := p.Files[fp.Entry]; !ok {
			return fmt.Errorf("filter %s: entry %q missing in package", fp.Name, fp.Entry)
		}
	}
	// hooks 声明绑定:任务文件存在 + task 名唯一 + 调度形态合法(cron 或 next 二选一)
	if h := m.Parts.Hooks; h != nil {
		seenTask := map[string]bool{}
		for _, tk := range h.Tasks {
			if tk.Name == "" {
				return fmt.Errorf("hooks task: name required")
			}
			if seenTask[tk.Name] {
				return fmt.Errorf("hooks task %q duplicated", tk.Name)
			}
			seenTask[tk.Name] = true
			entry := tk.Entry
			if entry == "" {
				entry = "tasks/" + tk.Name + ".js"
			}
			if _, ok := p.Files[entry]; !ok {
				return fmt.Errorf("hooks task %s: entry %q missing in package", tk.Name, entry)
			}
			if tk.Next && tk.Cron != "" {
				return fmt.Errorf("hooks task %s: cron and next are mutually exclusive", tk.Name)
			}
			if !tk.Next {
				if _, err := CronSchedule(tk.Cron); err != nil {
					return fmt.Errorf("hooks task %s: %w", tk.Name, err)
				}
			}
		}
	}
	// 元数据纯展示字段限长(超限拒装注明)
	if l := len([]rune(m.Title)); l > 64 {
		return fmt.Errorf("title too long (%d > 64)", l)
	}
	if l := len([]rune(m.Description)); l > 256 {
		return fmt.Errorf("description too long (%d > 256)", l)
	}
	return nil
}

// SecretRefsUnion protocol ∪ filters 的 secretRefs 并集(实例 secrets 覆盖校验)
func (p *Package) SecretRefsUnion() []string {
	set := map[string]bool{}
	var order []string
	add := func(refs []string) {
		for _, r := range refs {
			if !set[r] {
				set[r] = true
				order = append(order, r)
			}
		}
	}
	if p.Manifest.Parts.Protocol != nil {
		add(p.Manifest.Parts.Protocol.SecretRefs)
	}
	for _, f := range p.Manifest.Parts.Filters {
		add(f.SecretRefs)
	}
	return order
}

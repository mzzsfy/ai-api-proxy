// Package plugin .aap 包(zip)解析/校验/加载与 goja 部件绑定
package plugin

import (
	"encoding/json"
	"fmt"

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

// HooksTask 定时任务声明
type HooksTask struct {
	Name      string `json:"name"`
	Cron      string `json:"cron"`
	TimeoutMs int64  `json:"timeoutMs,omitempty"` // ≤0 按缺省;上限钳制在运行时
}

// HooksPart hooks 部件声明(加载钩子 + 定时任务;计入合法插槽)
type HooksPart struct {
	Entry string      `json:"entry"`
	Tasks []HooksTask `json:"tasks,omitempty"`
}

// Manifest 包元数据与模板载体
type Manifest struct {
	ManifestVersion int               `json:"manifestVersion"`
	Name            string            `json:"name"`
	Version         string            `json:"version"`
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

// Package 解析后的包(部件代码按 entry 存放)
type Package struct {
	Manifest *Manifest
	Files    map[string][]byte // entry 路径 → 代码
	Revision int64
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
	// hooks 声明绑定:entry 存在(安装期) + task 名唯一 + cron 语法(5 字段)
	if h := m.Parts.Hooks; h != nil {
		if h.Entry == "" {
			return fmt.Errorf("hooks: entry required")
		}
		if _, ok := p.Files[h.Entry]; !ok {
			return fmt.Errorf("hooks: entry %q missing in package", h.Entry)
		}
		seenTask := map[string]bool{}
		for _, tk := range h.Tasks {
			if tk.Name == "" {
				return fmt.Errorf("hooks task: name required")
			}
			if seenTask[tk.Name] {
				return fmt.Errorf("hooks task %q duplicated", tk.Name)
			}
			seenTask[tk.Name] = true
			if _, err := CronSchedule(tk.Cron); err != nil {
				return fmt.Errorf("hooks task %s: %w", tk.Name, err)
			}
		}
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

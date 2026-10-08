// discover.go 模型自动发现:拉上游 /v1/models,规则管道过滤改名,预览或落成员
package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

// ErrUpstream 上游不可达或状态异常(→ 502;区别于 400 规则/参数问题)
var ErrUpstream = errors.New("upstream error")

// DiscoveryRule 规则管道单步(顺序执行;op 语义见 DiscoveryRequest)
type DiscoveryRule struct {
	Op    string `json:"op"`              // only|exclude|rename|limit
	Regex string `json:"regex,omitempty"` // only/exclude:保留/丢弃匹配者;rename:被替换模式
	From  string `json:"from,omitempty"`  // rename:被替换模式(与 Regex 同义,取 From)
	To    string `json:"to,omitempty"`    // rename:替换为
	Limit int    `json:"limit,omitempty"` // limit:截断上限
}

// DiscoveryRequest 发现请求(请求级配置,无全默认:至少一个 only/exclude 规则)
type DiscoveryRequest struct {
	Rules   []DiscoveryRule `json:"rules"`
	Enabled bool            `json:"enabled"` // 落库成员启用态(默认 false,人工确认后启用)
	Apply   bool            `json:"apply"`   // false=仅预览;true=落库
}

// DiscoveryHit 单个候选结果
type DiscoveryHit struct {
	UpstreamName string `json:"upstream_name"` // 上游返回的模型名
	MemberName   string `json:"member_name"`   // 规则管道后的成员名
	Action       string `json:"action"`        // create|skip_exists
	PollGroups   int    `json:"poll_groups"`   // 同短名启用组数(含本组;>1 即落库后参与轮询)
}

// DiscoveryApplied 落库计数(Apply=true 时返回)
type DiscoveryApplied struct {
	Created int `json:"created"`
	Skipped int `json:"skipped"`
}

// DiscoveryResult 发现结果
type DiscoveryResult struct {
	FetchedTotal int               `json:"fetched_total"` // 上游返回模型总数(过滤前)
	Models       []DiscoveryHit    `json:"models"`
	Applied      *DiscoveryApplied `json:"applied,omitempty"`
}

// discoveryMaxFetch 上游列表读取上限(防御异常上游返回超大响应体)
const discoveryMaxFetch = 4 * 1024 * 1024

// Discover 发现流程:组声明校验 → 组参数取连接地址 → 拉列表 → 规则管道 → 预览/落成员
// hc 由装配层注入(经传输层客户端,继承代理出口);超时由 ctx 控制
func (r *Registry) Discover(ctx context.Context, groupID int64, req DiscoveryRequest, hc *http.Client) (*DiscoveryResult, error) {
	if hc == nil {
		return nil, fmt.Errorf("http client not configured")
	}
	// 规则表态检查:无 only 且无 exclude = 用户未表态,拒绝(决策 7)
	hasFilter := false
	for _, rule := range req.Rules {
		if rule.Op == "only" || rule.Op == "exclude" {
			hasFilter = true
		}
	}
	if !hasFilter {
		return nil, fmt.Errorf("rules must contain at least one only/exclude (explicit filtering required)")
	}
	g, err := r.GetGroup(groupID)
	if err != nil {
		return nil, err
	}
	pkg, err := r.pkgs.GetPackage(g.Plugin)
	if err != nil {
		return nil, err
	}
	decl := pkg.Manifest.Parts.Discovery
	if decl == nil {
		return nil, fmt.Errorf("package %s does not declare discovery support", g.Plugin)
	}
	merged, err := r.resolveParams(g)
	if err != nil {
		return nil, err
	}
	rawBase, _ := merged[decl.BaseURLSlot].(string)
	if strings.TrimSpace(rawBase) == "" {
		return nil, fmt.Errorf("param %q empty on group %s: configure the group first", decl.BaseURLSlot, g.Prefix)
	}
	names, err := fetchUpstreamModels(ctx, hc, decl.ProtocolStyle, rawBase, r.discoveryAuth(g.Plugin))
	if err != nil {
		return nil, fmt.Errorf("%w: fetch models: %v", ErrUpstream, err)
	}
	hits, err := r.applyRules(g, names, req.Rules)
	if err != nil {
		return nil, err
	}
	out := &DiscoveryResult{FetchedTotal: len(names), Models: hits}
	if !req.Apply {
		return out, nil
	}
	applied := &DiscoveryApplied{}
	for i := range hits {
		if hits[i].Action != "create" {
			applied.Skipped++
			continue
		}
		m := &Member{GroupID: g.ID, Name: hits[i].MemberName, Enabled: req.Enabled}
		if err := r.SaveMember(ctx, m); err != nil {
			// 并发下同名成员已建:记为跳过,不中断整批
			if errors.Is(err, ErrConflict) {
				hits[i].Action = "skip_exists"
				applied.Skipped++
				continue
			}
			return nil, err
		}
		applied.Created++
	}
	out.Applied = applied
	return out, nil
}

// applyRules 规则管道:only(保留匹配)/exclude(丢弃匹配)/rename(from→to 正则替换)/limit(截断)
func (r *Registry) applyRules(g *Group, names []string, rules []DiscoveryRule) ([]DiscoveryHit, error) {
	// 每条匹配型规则绑定自己的正则(limit 无正则)
	res := make([]*regexp.Regexp, len(rules))
	for i, rule := range rules {
		pattern := rule.Regex
		if rule.Op == "rename" {
			pattern = rule.From
		}
		if rule.Op == "limit" || rule.Op == "" {
			continue
		}
		if pattern == "" {
			return nil, fmt.Errorf("rule %s: regex/from required", rule.Op)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("rule %s: bad regex: %w", rule.Op, err)
		}
		res[i] = re
	}
	var out []DiscoveryHit
nameLoop:
	for _, name := range names {
		member := name
		for ri, rule := range rules {
			switch rule.Op {
			case "only":
				if !res[ri].MatchString(member) {
					continue nameLoop
				}
			case "exclude":
				if res[ri].MatchString(member) {
					continue nameLoop
				}
			case "rename":
				member = res[ri].ReplaceAllString(member, rule.To)
				if member == "" {
					continue nameLoop
				}
			case "limit":
				if rule.Limit > 0 && len(out) >= rule.Limit {
					break nameLoop
				}
			default:
				return nil, fmt.Errorf("unknown rule op %q", rule.Op)
			}
		}
		action := "create"
		for _, existing := range r.ListMembers(g.ID) {
			if existing.Name == member {
				action = "skip_exists"
				break
			}
		}
		out = append(out, DiscoveryHit{
			UpstreamName: name, MemberName: member, Action: action,
			PollGroups: r.PollGroupCount(member),
		})
	}
	return out, nil
}

// discoveryAuth 包当前键 → 鉴权头(openai=Bearer;anthropic=x-api-key+版本;无键 = 无鉴权头)
func (r *Registry) discoveryAuth(pkgName string) func(http.Header) {
	return func(h http.Header) {
		e, ok := r.pkgs.Keys().Rotate(pkgName)
		if !ok {
			return
		}
		var key string
		switch v := e.Data.(type) {
		case string:
			key = v
		case map[string]any:
			if s, ok := v["api_key"].(string); ok {
				key = s
			}
		}
		if key == "" {
			return
		}
		h.Set("Authorization", "Bearer "+key)
	}
}

// fetchUpstreamModels 拉上游 /v1/models(openai 与 anthropic 列表同形 {data:[{id}]})
func fetchUpstreamModels(ctx context.Context, hc *http.Client, style, baseURL string, auth func(http.Header)) ([]string, error) {
	url := strings.TrimRight(baseURL, "/") + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if auth != nil {
		auth(req.Header)
	}
	if style == "anthropic" {
		// anthropic 用裸 key(x-api-key),剥离 Bearer 前缀;无键不发空头
		if auth := req.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			req.Header.Set("x-api-key", strings.TrimPrefix(auth, "Bearer "))
		}
		req.Header.Del("Authorization")
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, discoveryMaxFetch))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: upstream status %d: %s", ErrUpstream, resp.StatusCode, truncateErrBody(body))
	}
	var doc struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decode model list: %w", err)
	}
	out := make([]string, 0, len(doc.Data))
	for _, d := range doc.Data {
		if d.ID != "" {
			out = append(out, d.ID)
		}
	}
	return out, nil
}

// truncateErrBody 错误体截断(日志/错误文案用)
func truncateErrBody(b []byte) string {
	const max = 512
	if len(b) > max {
		return string(b[:max])
	}
	return string(b)
}

// Package ipprovider IP 供给方契约:proxy 层与出口 IP 供给方(warp 池/clash 单出口/
// 远程代理 API/mihomo 内核)的稳定交互面。契约语义见 docs/ai-api-proxy/feat/ipprovider.md。
//
// 执行模型边界(恰一次):请求字节一经发往上游绝不重发;连接建立失败(零字节触达)
// 允许换出口重试,固定预算 1。
package ipprovider

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// 公开错误值
var (
	// ErrNoExits 供给方无可用出口(proxy 侧映射 503)
	ErrNoExits = errors.New("无可用出口")
	// ErrLeaseReleased 租约已释放后仍被使用
	ErrLeaseReleased = errors.New("租约已释放")
	// ErrUnknownKind registry 中未注册的供给方类型
	ErrUnknownKind = errors.New("未知供给方类型")
)

// Provider IP 供给方;实现随 transport.Manager 生命周期创建与 Close
type Provider interface {
	// Capabilities 构造时确定的固定能力声明,运行期不变
	Capabilities() Capabilities
	// Acquire 取一个可用出口租约;供给方内部完成选路;全部出口不可用返回 ErrNoExits;
	// ctx 取消返回 ctx.Err()
	Acquire(ctx context.Context, hint Hint) (Lease, error)
	// Stats 池快照(观测面;EgressIPs 只含探明出口)
	Stats() Stats
	// Close 幂等;关闭后 Acquire 返回错误
	Close() error
}

// Lease 出口租约;一次 Acquire 的使用句柄,非并发安全(单请求作用域)
type Lease interface {
	// Dial 经该出口建连;发生候选顺延后 EgressIP 更新为实际选中实例的出口;
	// Release 后调用返回 ErrLeaseReleased;候选全部耗尽返回最后错误
	Dial(ctx context.Context, network, addr string) (net.Conn, error)
	// EgressIP 当前出口 IP;未探明返回空串;Dial 顺延后更新为实际值
	EgressIP() string
	// Release 归还租约;幂等;实现须在 RoundTrip 结束时被调用
	Release()
	// Capabilities lease 级能力(warp/clash 与 Provider 一致;remote 以服务端为准);
	// 重试判定以本值为准
	Capabilities() Capabilities
}

// Excluder 支持 exclude 换出口重试的 Lease(aap);实现返回携带排除集的新租约视图
type Excluder interface {
	WithExclude(ids ...string) Lease
}

// RepError aap TUNNEL 应答错误;Rep 为节点 rep 码(协议 §rep)
type RepError struct {
	// Rep rep 码:1 出口不可用/目标连通失败;2 并发上限;3 请求含节点不支持的能力
	Rep uint8
	// LeaseID 失败前原绑定 lease_id(无绑定全零字符串,16 个 0x00)
	LeaseID string
}

func (e *RepError) Error() string {
	return fmt.Sprintf("aap node rep=%d", e.Rep)
}

// Evictor 支持管理面失效命令的 Provider(aap)
type Evictor interface {
	// Evict 按 scope 失效节点侧绑定;幂等(目标不存在亦成功)
	// scope:1=lease(16B lease_id 原始字节)/2=egress(SOCKS5 地址编码)
	Evict(ctx context.Context, scope uint8, value []byte) error
}

// Hint 取租约提示
type Hint struct {
	// SessionKey 会话亲和键;非空且供给方支持亲和时尽量同出口
	SessionKey string
}

// Capabilities 供给方能力声明
type Capabilities struct {
	// CanRotateIP 出口可轮换;false 时 Dial 失败不换出口重试(ipp_* 路径判定用)
	CanRotateIP bool
	// SessionAffinity 支持会话亲和;false 时 Hint.SessionKey 被忽略
	SessionAffinity bool
}

// Stats 池快照(观测面)
type Stats struct {
	Total    int
	Normal   int
	Probing  int
	Draining int
	Disabled int
	// EgressIPs 探明出口 IP 列表(未知出口不出现;clash 单出口恒空)
	EgressIPs []string
}

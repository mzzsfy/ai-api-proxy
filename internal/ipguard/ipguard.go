// Package ipguard 出站地址公网判定(SSRF 防线共用谓词)
package ipguard

import "net"

// Forbidden 非公网地址判定(环回/私网/链路本地/组播/未指定/CGNAT/基准测试/保留段/IPv6 文档段)
func Forbidden(ip net.IP) bool {
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		// 0.0.0.0/8"本网络"段、240/4 保留段(含 255.255.255.255 广播)
		if v4[0] == 0 || v4[0] >= 240 {
			return true
		}
		// 100.64/10 CGNAT 共享地址段
		if v4[0] == 100 && v4[1] >= 64 && v4[1] < 128 {
			return true
		}
		// 198.18/15 基准测试段;192.0.0/24 IETF 协议分配段
		if v4[0] == 198 && (v4[1] == 18 || v4[1] == 19) {
			return true
		}
		if v4[0] == 192 && v4[1] == 0 && v4[2] == 0 {
			return true
		}
		return false
	}
	// IPv6:2001:db8::/32 文档段(ULA fc00::/7 已由 IsPrivate 覆盖)
	return len(ip) == 16 && ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8
}

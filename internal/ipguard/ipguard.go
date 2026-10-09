// Package ipguard 出站地址公网判定(SSRF 防线共用谓词)
package ipguard

import "net"

// Forbidden 非公网地址判定(环回/私网/链路本地/组播/未指定/CGNAT/基准测试/保留段/NAT64/IPv6 文档段/IPv4 兼容段)
func Forbidden(ip net.IP) bool {
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		return forbiddenV4(v4)
	}
	if len(ip) == 16 {
		// IPv4 兼容段 ::/96(含 ::a.b.c.d 字面量,双栈解析可映射回 v4 环回/内网):归一为 v4 判定
		if isZeros(ip[:12]) {
			return forbiddenV4(ip[12:])
		}
		// NAT64 64:ff9b::/96
		if ip[0] == 0x00 && ip[1] == 0x64 && ip[2] == 0xff && ip[3] == 0x9b && isZeros(ip[4:12]) {
			return true
		}
		// 6to4 2002::/16 与 Teredo 2001:0::/32:嵌入内网 v4 的封装段,整段禁用(fail-closed,现代环境已废弃)
		if ip[0] == 0x20 && ip[1] == 0x02 {
			return true
		}
		if ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0 && ip[3] == 0 {
			return true
		}
		// 废弃 site-local fec0::/10
		if ip[0] == 0xfe && ip[1]&0xc0 == 0xc0 {
			return true
		}
		// 2001:db8::/32 文档段
		return ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8
	}
	return true
}

// forbiddenV4 v4 全量保留段判定
func forbiddenV4(v4 net.IP) bool {
	if v4.IsUnspecified() || v4.IsLoopback() || v4.IsPrivate() || v4.IsLinkLocalUnicast() || v4.IsMulticast() {
		return true
	}
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
	// 文档/测试段:192.0.2/24、198.51.100/24、203.0.113/24(不可路由,fail-closed)
	if (v4[0] == 192 && v4[1] == 0 && v4[2] == 2) ||
		(v4[0] == 198 && v4[1] == 51 && v4[2] == 100) ||
		(v4[0] == 203 && v4[1] == 0 && v4[2] == 113) {
		return true
	}
	return false
}

// isZeros 全零字节段判定
func isZeros(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

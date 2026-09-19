//go:build withproxy

// withproxy 构建标签:mihomo 内核集成(vless/vmess/trojan/ss 等)。
// 默认构建不含本文件;`go build -tags withproxy` 启用。
// 依赖来源:github.com/mzzsfy/ai-api-proxy-plugin-proxy(独立 go.mod,依赖隔离)。
package main

import (
	_ "github.com/mzzsfy/ai-api-proxy-plugin-proxy/mihomoprovider"
)

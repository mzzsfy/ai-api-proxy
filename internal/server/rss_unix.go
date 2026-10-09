//go:build !windows

// Unix 进程 RSS/物理内存读取(/proc)
package server

import (
	"os"
	"strconv"
	"strings"
)

// readRSS 当前进程 RSS 字节(/proc/self/statm 第二列;失败 0 = 看门狗跳过)
func readRSS() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

// sysTotalMemory 物理内存总量(阈值百分比换算用)
func sysTotalMemory() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			kb, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "MemTotal:")), 10, 64)
			return kb * 1024
		}
	}
	return 0
}

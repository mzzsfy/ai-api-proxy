//go:build windows

// Windows 进程 RSS/物理内存读取(psapi/kernel32)
package server

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

type processMemoryCounters struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr // 进程 RSS
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

var (
	kernel32            = windows.NewLazySystemDLL("kernel32.dll")
	procGlobalMemStatus = kernel32.NewProc("GlobalMemoryStatusEx")
	psapi               = windows.NewLazySystemDLL("psapi.dll")
	procGetProcessMem   = psapi.NewProc("GetProcessMemoryInfo")
)

// readRSS 进程工作集字节(失败 0 = 看门狗跳过)
func readRSS() uint64 {
	var mc processMemoryCounters
	mc.CB = uint32(unsafe.Sizeof(mc))
	// 自进程伪句柄(免 OpenProcess/CloseHandle)
	r1, _, _ := procGetProcessMem.Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&mc)), uintptr(unsafe.Sizeof(mc)))
	if r1 == 0 {
		return 0
	}
	return uint64(mc.WorkingSetSize)
}

// sysTotalMemory 物理内存总量(阈值百分比换算用)
func sysTotalMemory() uint64 {
	var mem memoryStatusEx
	mem.Length = uint32(unsafe.Sizeof(mem))
	r1, _, _ := procGlobalMemStatus.Call(uintptr(unsafe.Pointer(&mem)))
	if r1 == 0 {
		return 0
	}
	return mem.TotalPhys
}

//go:build windows

package hardware

import (
	"sync"
	"syscall"
	"unsafe"
)

type systemFileTime struct {
	lowDateTime  uint32
	highDateTime uint32
}

var (
	procGetSystemTimes = syscall.NewLazyDLL("kernel32.dll").NewProc("GetSystemTimes")
	windowsCPUUsage    struct {
		sync.Mutex
		idle, kernel, user uint64
		ready              bool
	}
)

func currentSystemUsage() (int, bool, int, bool) {
	cpu, cpuOK := windowsCPUPercent()
	memory, memoryOK := windowsMemoryPercent()
	return cpu, cpuOK, memory, memoryOK
}

func windowsCPUPercent() (int, bool) {
	var idle, kernel, user systemFileTime
	result, _, _ := procGetSystemTimes.Call(
		uintptr(unsafe.Pointer(&idle)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if result == 0 {
		return 0, false
	}
	idleTicks := uint64(idle.highDateTime)<<32 | uint64(idle.lowDateTime)
	kernelTicks := uint64(kernel.highDateTime)<<32 | uint64(kernel.lowDateTime)
	userTicks := uint64(user.highDateTime)<<32 | uint64(user.lowDateTime)

	windowsCPUUsage.Lock()
	defer windowsCPUUsage.Unlock()
	if !windowsCPUUsage.ready ||
		idleTicks < windowsCPUUsage.idle ||
		kernelTicks < windowsCPUUsage.kernel ||
		userTicks < windowsCPUUsage.user {
		windowsCPUUsage.idle = idleTicks
		windowsCPUUsage.kernel = kernelTicks
		windowsCPUUsage.user = userTicks
		windowsCPUUsage.ready = true
		return 0, false
	}
	idleDelta := idleTicks - windowsCPUUsage.idle
	totalDelta := kernelTicks - windowsCPUUsage.kernel + userTicks - windowsCPUUsage.user
	windowsCPUUsage.idle = idleTicks
	windowsCPUUsage.kernel = kernelTicks
	windowsCPUUsage.user = userTicks
	if totalDelta == 0 {
		return 0, false
	}
	return 100 - int(idleDelta*100/totalDelta), true
}

func windowsMemoryPercent() (int, bool) {
	var status memoryStatusEx
	status.Length = uint32(unsafe.Sizeof(status))
	result, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if result == 0 {
		return 0, false
	}
	return int(status.MemoryLoad), true
}

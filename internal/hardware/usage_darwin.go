//go:build darwin

package hardware

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var darwinCPUUsage struct {
	sync.Mutex
	total uint64
	idle  uint64
	ready bool
}

func currentSystemUsage() (int, bool, int, bool) {
	cpu, cpuOK := darwinCPUPercent()
	memory, memoryOK := darwinMemoryPercent()
	return cpu, cpuOK, memory, memoryOK
}

func darwinCPUPercent() (int, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "sysctl", "-n", "kern.cp_time").Output()
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(output))
	if len(fields) < 5 {
		return 0, false
	}
	values := make([]uint64, len(fields))
	for index := range fields {
		value, err := strconv.ParseUint(strings.TrimSuffix(fields[index], ","), 10, 64)
		if err != nil {
			return 0, false
		}
		values[index] = value
	}
	var total uint64
	for _, value := range values {
		total += value
	}
	idle := values[len(values)-1]

	darwinCPUUsage.Lock()
	defer darwinCPUUsage.Unlock()
	if !darwinCPUUsage.ready || total <= darwinCPUUsage.total || idle < darwinCPUUsage.idle {
		darwinCPUUsage.total = total
		darwinCPUUsage.idle = idle
		darwinCPUUsage.ready = true
		return 0, false
	}
	totalDelta := total - darwinCPUUsage.total
	idleDelta := idle - darwinCPUUsage.idle
	darwinCPUUsage.total = total
	darwinCPUUsage.idle = idle
	if totalDelta == 0 {
		return 0, false
	}
	return 100 - int(idleDelta*100/totalDelta), true
}

func darwinMemoryPercent() (int, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "vm_stat").Output()
	if err != nil {
		return 0, false
	}
	var pageSize, availablePages uint64
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if strings.HasPrefix(line, "Mach Virtual Memory Statistics:") {
			for index, field := range fields {
				if field == "size" && index+1 < len(fields) {
					for _, candidate := range fields[index+1:] {
						value, parseErr := strconv.ParseUint(strings.Trim(candidate, "()"), 10, 64)
						if parseErr == nil {
							pageSize = value
							break
						}
					}
					break
				}
			}
			continue
		}
		if len(fields) < 3 {
			continue
		}
		switch fields[0] {
		case "Pages":
			if fields[1] != "free:" && fields[1] != "inactive:" && fields[1] != "speculative:" && fields[1] != "purgeable:" {
				continue
			}
		default:
			continue
		}
		value, parseErr := strconv.ParseUint(strings.TrimRight(fields[2], "."), 10, 64)
		if parseErr != nil {
			continue
		}
		availablePages += value
	}
	if pageSize == 0 {
		return 0, false
	}
	totalBytes := totalMemoryMB() * 1024 * 1024
	availableBytes := availablePages * pageSize
	if availableBytes > totalBytes {
		return 0, false
	}
	return percentUsed(totalBytes, availableBytes)
}

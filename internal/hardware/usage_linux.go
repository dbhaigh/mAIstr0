//go:build linux

package hardware

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
)

var linuxCPUUsageState struct {
	sync.Mutex
	total uint64
	idle  uint64
	ready bool
}

func currentSystemUsage() (int, bool, int, bool) {
	cpu, cpuOK := linuxCPUPercent()
	memory, memoryOK := linuxMemoryPercent()
	return cpu, cpuOK, memory, memoryOK
}

func linuxCPUPercent() (int, bool) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return 0, false
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, false
	}
	var total, idle uint64
	for index, field := range fields[1:] {
		if index == 8 {
			break
		}
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, false
		}
		total += value
		if index == 3 || index == 4 {
			idle += value
		}
	}

	linuxCPUUsageState.Lock()
	defer linuxCPUUsageState.Unlock()
	if !linuxCPUUsageState.ready || total <= linuxCPUUsageState.total || idle < linuxCPUUsageState.idle {
		linuxCPUUsageState.total = total
		linuxCPUUsageState.idle = idle
		linuxCPUUsageState.ready = true
		return 0, false
	}
	totalDelta := total - linuxCPUUsageState.total
	idleDelta := idle - linuxCPUUsageState.idle
	linuxCPUUsageState.total = total
	linuxCPUUsageState.idle = idle
	if totalDelta == 0 {
		return 0, false
	}
	return 100 - int(idleDelta*100/totalDelta), true
}

func linuxMemoryPercent() (int, bool) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	var total, available uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total = value
		case "MemAvailable:":
			available = value
		}
	}
	return percentUsed(total, available)
}

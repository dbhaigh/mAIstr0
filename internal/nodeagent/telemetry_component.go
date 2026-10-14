package nodeagent

import (
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hardware"
)

func fastScore(hw hardware.Info) float64 {
	return cluster.FastScore(hw)
}

func (c *telemetryComponent) telemetryTimestamp() time.Time {
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	if len(c.gpuHistory) == 0 {
		return time.Time{}
	}
	return c.gpuHistory[len(c.gpuHistory)-1].Timestamp
}

func (c *telemetryComponent) hardwareSnapshot() hardware.Info {
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	return c.hw
}

func (c *telemetryComponent) hardwareUsageLoop() {
	refresh := func() {
		utilization, freeMemoryMB, available := hardware.CurrentGPUUsage()
		npuUtilization, npuAvailable := hardware.CurrentNPUUsage()
		cpuUtilization, cpuAvailable, memoryUsage, memoryAvailable := hardware.CurrentSystemUsage()
		sample := hardware.GPUUsageSample{
			Timestamp: time.Now().UTC(), Available: available, Utilization: utilization,
		}
		systemSample := hardware.SystemUsageSample{
			Timestamp: time.Now().UTC(), CPUAvailable: cpuAvailable, CPUUtilization: cpuUtilization,
			MemoryAvailable: memoryAvailable, MemoryUtilization: memoryUsage,
		}
		npuSample := hardware.NPUUsageSample{
			Timestamp: time.Now().UTC(), Available: npuAvailable, Utilization: npuUtilization,
		}
		c.hwMu.Lock()
		c.hw.GPUStatsAvailable = available
		c.hw.GPUUtilization = utilization
		c.hw.GPUMemoryFreeMB = freeMemoryMB
		c.gpuHistory = appendGPUUsageSample(c.gpuHistory, sample)
		c.hw.NPUStatsAvailable = npuAvailable
		c.hw.NPUUtilization = npuUtilization
		c.npuHistory = appendNPUUsageSample(c.npuHistory, npuSample)
		c.hw.CPUStatsAvailable = cpuAvailable
		c.hw.CPUUtilization = cpuUtilization
		c.hw.MemoryStatsAvailable = memoryAvailable
		c.hw.MemoryUsage = memoryUsage
		c.systemHistory = appendSystemUsageSample(c.systemHistory, systemSample)
		c.hwMu.Unlock()
	}
	refresh()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		refresh()
	}
}

const gpuHistoryLimit = 60
const npuHistoryLimit = 60
const systemHistoryLimit = 60

func appendGPUUsageSample(history []hardware.GPUUsageSample, sample hardware.GPUUsageSample) []hardware.GPUUsageSample {
	if len(history) < gpuHistoryLimit {
		return append(history, sample)
	}
	copy(history, history[1:])
	history[len(history)-1] = sample
	return history
}

func appendNPUUsageSample(history []hardware.NPUUsageSample, sample hardware.NPUUsageSample) []hardware.NPUUsageSample {
	if len(history) < npuHistoryLimit {
		return append(history, sample)
	}
	copy(history, history[1:])
	history[len(history)-1] = sample
	return history
}

func appendSystemUsageSample(history []hardware.SystemUsageSample, sample hardware.SystemUsageSample) []hardware.SystemUsageSample {
	if len(history) < systemHistoryLimit {
		return append(history, sample)
	}
	copy(history, history[1:])
	history[len(history)-1] = sample
	return history
}

func (c *telemetryComponent) gpuHistorySnapshot() []hardware.GPUUsageSample {
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	return append([]hardware.GPUUsageSample(nil), c.gpuHistory...)
}

func (c *telemetryComponent) npuHistorySnapshot() []hardware.NPUUsageSample {
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	return append([]hardware.NPUUsageSample(nil), c.npuHistory...)
}

func (c *telemetryComponent) systemHistorySnapshot() []hardware.SystemUsageSample {
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	return append([]hardware.SystemUsageSample(nil), c.systemHistory...)
}

func (a *Agent) telemetryTimestamp() time.Time {
	return a.telemetryComponent.telemetryTimestamp()
}
func (a *Agent) hardwareSnapshot() hardware.Info {
	return a.telemetryComponent.hardwareSnapshot()
}
func (a *Agent) hardwareUsageLoop() {
	a.telemetryComponent.hardwareUsageLoop()
}
func (a *Agent) gpuHistorySnapshot() []hardware.GPUUsageSample {
	return a.telemetryComponent.gpuHistorySnapshot()
}
func (a *Agent) npuHistorySnapshot() []hardware.NPUUsageSample {
	return a.telemetryComponent.npuHistorySnapshot()
}
func (a *Agent) systemHistorySnapshot() []hardware.SystemUsageSample {
	return a.telemetryComponent.systemHistorySnapshot()
}

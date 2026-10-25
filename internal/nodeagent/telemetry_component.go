package nodeagent

import (
	"context"
	"time"

	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hardware"
)

const hardwareProbeInterval = 5 * time.Second
const hardwareTelemetryStaleAfter = 30 * time.Second

type hardwareUsageSample struct {
	gpuUtilization  int
	gpuFreeMemory   uint64
	gpuAvailable    bool
	npuUtilization  int
	npuAvailable    bool
	cpuUtilization  int
	cpuAvailable    bool
	memoryUsage     int
	memoryAvailable bool
	runtimeDevices  []hardware.Device
	runtimeProbe    bool
}

func currentHardwareUsage() hardwareUsageSample {
	gpuUtilization, gpuFreeMemory, gpuAvailable := hardware.CurrentGPUUsage()
	npuUtilization, npuAvailable := hardware.CurrentNPUUsage()
	cpuUtilization, cpuAvailable, memoryUsage, memoryAvailable := hardware.CurrentSystemUsage()
	return hardwareUsageSample{
		gpuUtilization: gpuUtilization, gpuFreeMemory: gpuFreeMemory, gpuAvailable: gpuAvailable,
		npuUtilization: npuUtilization, npuAvailable: npuAvailable,
		cpuUtilization: cpuUtilization, cpuAvailable: cpuAvailable,
		memoryUsage: memoryUsage, memoryAvailable: memoryAvailable,
	}
}

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
	snapshot, _ := c.hardwareSnapshotVersioned()
	return snapshot
}

func (c *telemetryComponent) hardwareSnapshotVersioned() (hardware.Info, uint64) {
	c.refreshHardwareUsageIfStale()
	c.hwMu.RLock()
	defer c.hwMu.RUnlock()
	return c.hw, c.hwGeneration
}

func (c *telemetryComponent) hardwareTelemetryStale(now time.Time) bool {
	c.hwMu.RLock()
	if len(c.gpuHistory) == 0 {
		c.hwMu.RUnlock()
		return false
	}
	lastUpdated := c.gpuHistory[len(c.gpuHistory)-1].Timestamp
	c.hwMu.RUnlock()
	return lastUpdated.IsZero() || lastUpdated.After(now) || now.Sub(lastUpdated) > hardwareTelemetryStaleAfter
}

func (c *telemetryComponent) refreshHardwareUsageIfStale() {
	if c.hardwareUsageProbe == nil || !c.hardwareTelemetryStale(time.Now()) {
		return
	}
	c.hwRefreshMu.Lock()
	defer c.hwRefreshMu.Unlock()
	if c.hardwareTelemetryStale(time.Now()) {
		c.recordHardwareUsage(c.hardwareUsageProbe())
	}
}

func (c *telemetryComponent) refreshHardwareUsage() {
	if c.hardwareUsageProbe == nil {
		return
	}
	c.hwRefreshMu.Lock()
	defer c.hwRefreshMu.Unlock()
	c.recordHardwareUsage(c.hardwareUsageProbe())
}

func (c *telemetryComponent) recordHardwareUsage(usage hardwareUsageSample) {
	now := time.Now().UTC()
	sample := hardware.GPUUsageSample{
		Timestamp: now, Available: usage.gpuAvailable, Utilization: usage.gpuUtilization,
	}
	systemSample := hardware.SystemUsageSample{
		Timestamp: now, CPUAvailable: usage.cpuAvailable, CPUUtilization: usage.cpuUtilization,
		MemoryAvailable: usage.memoryAvailable, MemoryUtilization: usage.memoryUsage,
	}
	npuSample := hardware.NPUUsageSample{
		Timestamp: now, Available: usage.npuAvailable, Utilization: usage.npuUtilization,
	}
	c.hwMu.Lock()
	c.hw.GPUStatsAvailable = usage.gpuAvailable
	c.hw.GPUUtilization = usage.gpuUtilization
	c.hw.GPUMemoryFreeMB = usage.gpuFreeMemory
	if usage.runtimeProbe {
		c.hw.Devices = hardware.ApplyRuntimeDevices(c.hw.Devices, c.runtimeDevices)
		c.hw.Devices = hardware.ApplyRuntimeDevices(c.hw.Devices, usage.runtimeDevices)
		c.runtimeDevices = append(c.runtimeDevices[:0], usage.runtimeDevices...)
		for i := range c.runtimeDevices {
			c.runtimeDevices[i].FreeMemoryMB = 0
		}
		for _, device := range usage.runtimeDevices {
			switch device.Kind {
			case "gpu":
				c.hw.HasGPU = true
				if c.hw.GPUVendor == "" {
					c.hw.GPUVendor = device.Vendor
				}
			case "npu":
				c.hw.HasNPU = true
				if c.hw.NPUVendor == "" {
					c.hw.NPUVendor = device.Vendor
				}
			}
			if device.Kind == "gpu" && device.FreeMemoryMB > c.hw.GPUMemoryFreeMB {
				c.hw.GPUMemoryFreeMB = device.FreeMemoryMB
			}
		}
	}
	c.gpuHistory = appendGPUUsageSample(c.gpuHistory, sample)
	c.hw.NPUStatsAvailable = usage.npuAvailable
	c.hw.NPUUtilization = usage.npuUtilization
	c.npuHistory = appendNPUUsageSample(c.npuHistory, npuSample)
	c.hw.CPUStatsAvailable = usage.cpuAvailable
	c.hw.CPUUtilization = usage.cpuUtilization
	c.hw.MemoryStatsAvailable = usage.memoryAvailable
	c.hw.MemoryUsage = usage.memoryUsage
	c.systemHistory = appendSystemUsageSample(c.systemHistory, systemSample)
	c.hwGeneration++
	c.hwMu.Unlock()
}

func (c *telemetryComponent) hardwareUsageLoop() {
	c.hardwareUsageLoopContext(context.Background())
}

func (c *telemetryComponent) hardwareUsageLoopContext(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	c.refreshHardwareUsage()
	background.RunEvery(ctx, hardwareProbeInterval, func(context.Context) {
		c.refreshHardwareUsage()
	})
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

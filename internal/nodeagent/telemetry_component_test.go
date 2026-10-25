package nodeagent

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/hardware"
)

func TestRuntimeDeviceTelemetryRefreshAndFailure(t *testing.T) {
	component := newTelemetryComponent(hardware.Info{Devices: []hardware.Device{
		{Kind: "cpu", Name: "CPU", MemoryMB: 32768},
		{Kind: "gpu", Vendor: "amd", Name: "AMD GPU", Backends: []string{"vulkan"}},
	}})
	reading := []hardware.Device{{Kind: "gpu", Vendor: "amd", Name: "AMD GPU", MemoryMB: 8192, FreeMemoryMB: 6000, Backends: []string{"vulkan"}}}
	component.recordHardwareUsage(hardwareUsageSample{runtimeProbe: true, runtimeDevices: reading})
	if component.hw.Devices[1].FreeMemoryMB != 6000 || !component.hw.HasGPU {
		t.Fatalf("runtime telemetry=%+v", component.hw)
	}
	component.recordHardwareUsage(hardwareUsageSample{runtimeProbe: true})
	if component.hw.Devices[1].FreeMemoryMB != 0 || component.hw.Devices[0].MemoryMB != 32768 {
		t.Fatalf("failed runtime probe retained memory or changed RAM: %+v", component.hw.Devices)
	}
	reading[0].FreeMemoryMB = 4500
	component.recordHardwareUsage(hardwareUsageSample{runtimeProbe: true, runtimeDevices: reading})
	if component.hw.Devices[1].FreeMemoryMB != 4500 || len(component.hw.Devices) != 2 {
		t.Fatalf("fresh runtime telemetry=%+v", component.hw.Devices)
	}
}

func TestHardwareSnapshotRefreshesStaleTelemetry(t *testing.T) {
	probeCalls := 0
	component := &telemetryComponent{
		hw: hardware.Info{GPUStatsAvailable: true, GPUUtilization: 4},
		gpuHistory: []hardware.GPUUsageSample{{
			Timestamp: time.Now().Add(-hardwareTelemetryStaleAfter - time.Second),
			Available: true, Utilization: 4,
		}},
		hardwareUsageProbe: func() hardwareUsageSample {
			probeCalls++
			return hardwareUsageSample{memoryUsage: 45, memoryAvailable: true}
		},
	}

	snapshot, generation := component.hardwareSnapshotVersioned()
	if probeCalls != 1 {
		t.Fatalf("stale snapshot triggered %d probes, want one", probeCalls)
	}
	if generation != 1 {
		t.Fatalf("hardware generation = %d, want 1 after refresh", generation)
	}
	if snapshot.GPUStatsAvailable {
		t.Fatal("unavailable fresh GPU stats remained marked available")
	}
	if snapshot.MemoryStatsAvailable != true || snapshot.MemoryUsage != 45 {
		t.Fatalf("fresh system memory usage = available %t, usage %d; want true, 45",
			snapshot.MemoryStatsAvailable, snapshot.MemoryUsage)
	}

	component.hardwareSnapshotVersioned()
	if probeCalls != 1 {
		t.Fatalf("fresh snapshot triggered another probe; total %d", probeCalls)
	}
}

package nodeagent

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/hardware"
)

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

package nodeagent

import (
	"strconv"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/testutil"
)

func BenchmarkProfileInventory(b *testing.B) {
	for _, refresh := range []bool{false, true} {
		b.Run(strconv.FormatBool(refresh), func(b *testing.B) {
			models := make([]engine.Model, 64)
			for i := range models {
				models[i] = engine.Model{Name: "synthetic-" + strconv.Itoa(i), Tags: []string{"general"}, SizeGB: 4}
			}
			backend := &inventoryTestEngine{models: models}
			component := newEngineComponent([]engine.Engine{backend}, func() []engine.Engine { return nil },
				&telemetryComponent{}, nil, "")
			component.refreshReportedModels()
			latencies := &testutil.Latencies{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				if refresh {
					component.invalidateReportedModels()
				}
				if got := component.enabledModels(); len(got) != 64 {
					b.Fatal("inventory lost models")
				}
				latencies.Record(time.Since(start))
			}
			latencies.Report(b, 1)
			calls := backend.listModelCalls()
			if (!refresh && calls != 1) || (refresh && calls != b.N+1) {
				b.Fatalf("inventory probe count = %d, refresh=%t N=%d", calls, refresh, b.N)
			}
			b.ReportMetric(float64(calls-1)/float64(b.N), "probes/op")
		})
	}
}

func BenchmarkProfileTelemetry(b *testing.B) {
	component := newTelemetryComponent(hardware.Info{Devices: []hardware.Device{
		{Kind: "cpu", MemoryMB: 32768},
		{Kind: "gpu", Vendor: "amd", MemoryMB: 8192, FreeMemoryMB: 4096, Backends: []string{"vulkan"}},
	}})
	usage := hardwareUsageSample{
		gpuAvailable: true, gpuUtilization: 25, gpuFreeMemory: 4096,
		cpuAvailable: true, cpuUtilization: 50, memoryAvailable: true, memoryUsage: 40,
		runtimeProbe: true, runtimeDevices: []hardware.Device{{
			Kind: "gpu", Vendor: "amd", MemoryMB: 8192, FreeMemoryMB: 4096, Backends: []string{"vulkan"},
		}},
	}
	latencies := &testutil.Latencies{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		component.recordHardwareUsage(usage)
		_, generation := component.hardwareSnapshotVersioned()
		if generation == 0 {
			b.Fatal("telemetry generation did not advance")
		}
		latencies.Record(time.Since(start))
	}
	latencies.Report(b, 1)
}

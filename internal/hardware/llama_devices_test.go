package hardware

import "testing"

func BenchmarkApplyRuntimeDevices(b *testing.B) {
	existing := []Device{{Kind: "cpu", Name: "CPU"}, {Kind: "gpu", Vendor: "nvidia", Name: "NVIDIA RTX", Backends: []string{"cuda", "vulkan"}}}
	runtime := []Device{{Kind: "gpu", Vendor: "nvidia", Name: "NVIDIA RTX", MemoryMB: 16384, FreeMemoryMB: 10000, Backends: []string{"vulkan"}}}
	b.ReportAllocs()
	for b.Loop() {
		ApplyRuntimeDevices(existing, runtime)
	}
}

func TestRuntimeDeviceMemoryMatchesDifferentBackendIndices(t *testing.T) {
	existing := []Device{{Kind: "gpu", Vendor: "nvidia", Name: "NVIDIA RTX", Index: 0, Backends: []string{"cuda"}}}
	runtime := []Device{{Kind: "gpu", Vendor: "nvidia", Name: "NVIDIA RTX", Index: 1, MemoryMB: 16384, FreeMemoryMB: 10000, Backends: []string{"vulkan"}}}
	got := ApplyRuntimeDevices(existing, runtime)
	if len(got) != 1 || got[0].FreeMemoryMB != 10000 || len(got[0].Backends) != 2 {
		t.Fatalf("runtime backend double-counted a device: %+v", got)
	}
}

func TestParseLlamaDevices(t *testing.T) {
	for _, test := range []struct {
		name, output string
		count        int
		free         uint64
		kind         string
	}{
		{"vulkan", "Available devices:\n  Vulkan0: AMD Radeon 780M (8192 MiB, 6144 MiB free)\n", 1, 6144, "gpu"},
		{"cuda", "CUDA0: NVIDIA RTX (24576 MiB, 0 MiB free)", 1, 0, "gpu"},
		{"npu", "XDNA0: Ryzen AI (8192 MiB, 4096 MiB free)", 1, 4096, "npu"},
		{"over total", "Vulkan0: GPU (4096 MiB, 8192 MiB free)", 0, 0, ""},
		{"unknown", "Vulkan0: GPU (0 MiB, 0 MiB free)", 0, 0, ""},
		{"noise", "ggml_vulkan: found 1 device\nGPU memory unavailable", 0, 0, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := ParseLlamaDevices([]byte(test.output))
			if len(got) != test.count {
				t.Fatalf("devices=%+v", got)
			}
			if len(got) > 0 && (got[0].FreeMemoryMB != test.free || got[0].Kind != test.kind) {
				t.Fatalf("device=%+v", got[0])
			}
		})
	}
}

func TestApplyRuntimeDevicesRefreshesWithoutDuplicatingAccelerators(t *testing.T) {
	existing := []Device{
		{Kind: "cpu", Name: "CPU", MemoryMB: 32768},
		{Kind: "gpu", Vendor: "amd", Name: "AMD GPU", Index: 0, Backends: []string{"vulkan"}},
	}

	runtime := []Device{
		{Kind: "gpu", Vendor: "amd", Name: "AMD GPU (RADV)", Index: 0, MemoryMB: 8192, FreeMemoryMB: 6000, Backends: []string{"vulkan"}},
		{Kind: "gpu", Vendor: "amd", Name: "AMD GPU (RADV)", Index: 1, MemoryMB: 8192, FreeMemoryMB: 5000, Backends: []string{"vulkan"}},
	}
	got := ApplyRuntimeDevices(existing, runtime)
	if len(got) != 3 || got[1].FreeMemoryMB != 6000 || got[2].Index != 1 || existing[1].FreeMemoryMB != 0 {
		t.Fatalf("devices=%+v original=%+v", got, existing)
	}
	runtime[0].FreeMemoryMB = 0
	got = ApplyRuntimeDevices(got, runtime)
	if got[1].FreeMemoryMB != 0 || len(got) != 3 {
		t.Fatalf("refresh retained stale capacity: %+v", got)
	}
}

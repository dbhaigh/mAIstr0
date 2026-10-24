package hardware

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type fakeDeviceProbe struct {
	paths         map[string]string
	commands      map[string]string
	commandErrors map[string]error
	files         map[string]string
	globs         map[string][]string
	calls         []string
}

func (probe *fakeDeviceProbe) LookPath(name string) (string, error) {
	if path, ok := probe.paths[name]; ok {
		return path, nil
	}
	return "", fmt.Errorf("%s is unavailable", name)
}

func (probe *fakeDeviceProbe) Run(name string, args ...string) ([]byte, error) {
	key := deviceCommandKey(name, args...)
	probe.calls = append(probe.calls, key)
	if err := probe.commandErrors[key]; err != nil {
		return nil, err
	}
	if output, ok := probe.commands[key]; ok {
		return []byte(output), nil
	}
	return nil, fmt.Errorf("no fake output for %s", key)
}

func (probe *fakeDeviceProbe) Glob(pattern string) ([]string, error) {
	return append([]string(nil), probe.globs[pattern]...), nil
}

func (probe *fakeDeviceProbe) ReadFile(path string) ([]byte, error) {
	path = strings.ReplaceAll(path, "\\", "/")
	if content, ok := probe.files[path]; ok {
		return []byte(content), nil
	}
	return nil, fmt.Errorf("%s is unavailable", path)
}

func deviceCommandKey(name string, args ...string) string {
	return strings.Join(append([]string{name}, args...), " ")
}

func TestDetectNVIDIADevicesQueriesAndParsesEveryGPU(t *testing.T) {
	const command = "nvidia-smi --query-gpu=index,name,memory.total,memory.free --format=csv,noheader,nounits"
	probe := &fakeDeviceProbe{
		paths: map[string]string{"nvidia-smi": "nvidia-smi"},
		commands: map[string]string{
			command: "0, NVIDIA GeForce RTX 4090, 24564, 16384\n1, NVIDIA RTX A2000, 6144, 2048\n",
		},
	}

	devices := detectNVIDIADevices(probe)
	if len(devices) != 2 {
		t.Fatalf("detected %d NVIDIA devices, want 2: %+v", len(devices), devices)
	}
	if devices[0].Index != 0 || devices[0].Name != "NVIDIA GeForce RTX 4090" ||
		devices[0].MemoryMB != 24564 || devices[0].FreeMemoryMB != 16384 ||
		!reflect.DeepEqual(devices[0].Backends, []string{"cuda", "vulkan"}) {
		t.Fatalf("first NVIDIA device = %+v", devices[0])
	}
	if !reflect.DeepEqual(probe.calls, []string{command}) {
		t.Fatalf("nvidia-smi calls = %v, want %q", probe.calls, command)
	}
}

func TestParseCPUInventories(t *testing.T) {
	linux := parseLinuxCPUInfo(`processor : 0
physical id : 0
vendor_id : GenuineIntel
model name : Intel(R) Xeon(R) CPU 8480+

processor : 1
physical id : 1
vendor_id : AuthenticAMD
model name : AMD EPYC 9654
`)
	if len(linux) != 2 || linux[0].Index != 0 || linux[0].Vendor != "intel" ||
		linux[1].Index != 1 || linux[1].Vendor != "amd" {
		t.Fatalf("Linux CPU inventory = %+v", linux)
	}

	windows := parseWindowsCPUInfo([]byte(`"13th Gen Intel(R) Core(TM) i9-13900K","GenuineIntel"
"AMD Ryzen 9 7950X","AuthenticAMD"`))
	if len(windows) != 2 || windows[0].Name != "13th Gen Intel(R) Core(TM) i9-13900K" ||
		windows[0].Vendor != "intel" || windows[1].Vendor != "amd" {
		t.Fatalf("Windows CPU inventory = %+v", windows)
	}
}

func TestDetectAMDDevicesParsesROCMSMIInventory(t *testing.T) {
	const command = "rocm-smi --showproductname --showmeminfo vram --json"
	probe := &fakeDeviceProbe{
		paths: map[string]string{"rocm-smi": "rocm-smi"},
		commands: map[string]string{
			command: `{"card0":{"Card series":"AMD Radeon RX 7900 XTX","VRAM Total Memory (B)":"25769803776","VRAM Total Used Memory (B)":"2147483648","Driver version":"6.0"}}`,
		},
	}

	devices := detectAMDDevices(probe)
	if len(devices) != 1 {
		t.Fatalf("AMD devices = %+v, want one device", devices)
	}
	if devices[0].Vendor != "amd" || devices[0].Index != 0 ||
		devices[0].Name != "AMD Radeon RX 7900 XTX" ||
		devices[0].MemoryMB != 24576 || devices[0].FreeMemoryMB != 22528 ||
		devices[0].Driver != "6.0" {
		t.Fatalf("AMD device = %+v", devices[0])
	}
}

func TestDetectAppleDevicesUsesSysctlAndUnifiedMemory(t *testing.T) {
	probe := &fakeDeviceProbe{
		paths: map[string]string{"sysctl": "sysctl"},
		commands: map[string]string{
			"sysctl -n hw.memsize":               "17179869184\n",
			"sysctl -n machdep.cpu.brand_string": "Apple M4\n",
		},
	}

	devices := detectAppleDevices(probe, "arm64")
	if len(devices) != 2 {
		t.Fatalf("Apple devices = %+v, want GPU and NPU", devices)
	}
	if devices[0].Kind != deviceGPU || devices[0].Vendor != "apple" ||
		devices[0].Name != "Apple M4 GPU" || devices[0].MemoryMB != 16384 ||
		!devices[0].UnifiedMemory || devices[1].Vendor != "apple-neural-engine" {
		t.Fatalf("Apple inventory = %+v", devices)
	}
	if got := detectAppleDevices(probe, "amd64"); len(got) != 0 {
		t.Fatalf("non-Apple-Silicon inventory = %+v, want none", got)
	}
	fallback := detectAppleDevices(&fakeDeviceProbe{}, "arm64")
	if len(fallback) != 2 || fallback[0].MemoryMB != 0 || fallback[1].MemoryMB != 0 {
		t.Fatalf("Apple inventory without memory data = %+v, want devices with zero memory", fallback)
	}
}

func TestParseVulkanAndSYCLDeviceInventory(t *testing.T) {
	vulkan := parseVulkanSummary([]byte(`GPU0:
  vendorID = 0x8086
  deviceType = PHYSICAL_DEVICE_TYPE_INTEGRATED_GPU
  deviceName = Intel(R) Arc(TM) Graphics
  driverName = Intel Mesa
GPU1:
  vendorID = 0x1234
  deviceType = PHYSICAL_DEVICE_TYPE_DISCRETE_GPU
  deviceName = Example Unknown GPU
GPU2:
  deviceType = PHYSICAL_DEVICE_TYPE_CPU
  deviceName = llvmpipe
`))
	if len(vulkan) != 2 || vulkan[0].Vendor != "intel" ||
		vulkan[1].Vendor != "" || vulkan[1].Name != "Example Unknown GPU" {
		t.Fatalf("Vulkan inventory = %+v", vulkan)
	}
	if vulkan[0].Driver != "Intel Mesa" || vulkan[0].Backends[0] != "vulkan" {
		t.Fatalf("Vulkan driver/backend = %+v", vulkan[0])
	}

	sycl := parseSYCLDevices([]byte(
		"[level_zero:gpu:0] Intel(R) Arc(TM) Graphics [0x1234]\n[opencl:gpu:1] Unknown Device\n[level_zero:cpu:0] Intel CPU\n",
	))
	if len(sycl) != 2 || sycl[0].Vendor != "intel" ||
		!reflect.DeepEqual(sycl[0].Backends, []string{"sycl", "level_zero", "openvino"}) ||
		sycl[1].Vendor != "" || sycl[1].Index != 1 {
		t.Fatalf("SYCL inventory = %+v", sycl)
	}
}

func TestDetectLinuxNPUDevicesUsesSysfsAndDeviceNodes(t *testing.T) {
	probe := &fakeDeviceProbe{
		globs: map[string][]string{
			"/sys/class/accel/accel*": {"/sys/class/accel/accel0"},
			"/dev/accel*":             {"/dev/accel", "/dev/accel/accel0"},
			"/dev/accel/accel*":       {"/dev/accel/accel0"},
			"/dev/ivpu*":              {"/dev/ivpu0"},
		},
		files: map[string]string{
			"/sys/class/accel/accel0/device/vendor": "0x8086\n",
			"/sys/class/accel/accel0/device/label":  "Intel VPU",
			"/sys/class/accel/accel0/device/uevent": "DRIVER=intel_vpu\n",
		},
	}

	devices := mergeDevices(detectLinuxNPUDevices(probe))
	if len(devices) != 1 || devices[0].Kind != deviceNPU ||
		devices[0].Vendor != "intel-npu" || devices[0].Name != "Intel VPU" ||
		devices[0].Driver != "intel_vpu" {
		t.Fatalf("Linux NPU inventory = %+v", devices)
	}
	if got := probe.globs["/dev/nnapi"]; len(got) != 0 {
		t.Fatalf("legacy NNAPI path was probed: %v", got)
	}
}

func TestDetectLinuxNPUDevicesRecognizesDocumentedNPUVendors(t *testing.T) {
	tests := []struct {
		name     string
		pci      string
		label    string
		vendor   string
		backends []string
	}{
		{name: "Intel", pci: "0x8086", label: "Intel VPU", vendor: "intel-npu", backends: []string{"openvino", "directml"}},
		{name: "AMD XDNA", pci: "0x1022", label: "AMD XDNA 2", vendor: "amd-npu", backends: []string{"xdna"}},
		{name: "Qualcomm Hexagon", pci: "0x17cb", label: "Qualcomm Hexagon", vendor: "qualcomm-npu", backends: []string{"hexagon", "qnn"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &fakeDeviceProbe{
				globs: map[string][]string{
					"/sys/class/accel/accel*": {"/sys/class/accel/accel0"},
				},
				files: map[string]string{
					"/sys/class/accel/accel0/device/vendor": test.pci,
					"/sys/class/accel/accel0/device/label":  test.label,
				},
			}
			devices := detectLinuxNPUDevices(probe)
			if len(devices) != 1 || devices[0].Vendor != test.vendor ||
				!reflect.DeepEqual(devices[0].Backends, test.backends) {
				t.Fatalf("Linux NPU inventory = %+v, want vendor %q and backends %v", devices, test.vendor, test.backends)
			}
		})
	}
}

func TestLinuxTopologyDetectsNUMAAndNetworkLinks(t *testing.T) {
	probe := &fakeDeviceProbe{
		globs: map[string][]string{
			"/sys/devices/system/node/node*":         {"/sys/devices/system/node/node1", "/sys/devices/system/node/node0"},
			"/sys/class/net/*":                       {"/sys/class/net/lo", "/sys/class/net/ib0", "/sys/class/net/eth0"},
			"/sys/class/net/ib0/device/infiniband/*": {"/sys/class/net/ib0/device/infiniband/mlx5_0"},
		},
		files: map[string]string{
			"/sys/class/net/ib0/speed":  "100000\n",
			"/sys/class/net/eth0/speed": "2500\n",
		},
	}

	if got := detectNUMANodes(probe); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("NUMA nodes = %v, want [0 1]", got)
	}
	links := detectNetworkLinks(probe)
	want := []NetworkLink{
		{Name: "eth0", SpeedMbps: 2500},
		{Name: "ib0", SpeedMbps: 100000, RDMA: true},
	}
	if !reflect.DeepEqual(links, want) {
		t.Fatalf("network links = %+v, want %+v", links, want)
	}
}

func TestWindowsNPUInventoryCanBeCached(t *testing.T) {
	const command = "powershell.exe -NoProfile -NonInteractive -Command " + windowsNPUQuery
	probe := &fakeDeviceProbe{
		paths: map[string]string{"powershell.exe": "powershell.exe"},
		commands: map[string]string{
			command: "Intel(R) AI Boost\tIntel Corporation\tPCI\\VEN_8086\nUnknown NPU\tUnknown\tDEVICE_0\n",
		},
	}
	var cache cachedDeviceInventory
	calls := 0
	detect := func() []Device {
		calls++
		return detectWindowsNPUDevices(probe)
	}

	first := cache.get(detect)
	second := cache.get(detect)
	if calls != 1 || !reflect.DeepEqual(first, second) {
		t.Fatalf("cache calls=%d, first=%+v, second=%+v", calls, first, second)
	}
	if len(first) != 2 || first[0].Vendor != "intel-npu" || first[1].Vendor != "" {
		t.Fatalf("Windows NPU inventory = %+v", first)
	}
}

func TestPopulateLegacyHardwareFieldsFromDevices(t *testing.T) {
	info := Info{Devices: []Device{
		{Kind: deviceGPU, Vendor: "nvidia", FreeMemoryMB: 2048},
		{Kind: deviceGPU, Vendor: "amd", FreeMemoryMB: 4096},
		{Kind: deviceNPU, Vendor: "intel-npu"},
	}}
	populateLegacyDeviceFields(&info)
	if !info.HasGPU || info.GPUVendor != "nvidia" || info.GPUMemoryFreeMB != 4096 ||
		!info.HasNPU || info.NPUVendor != "intel-npu" {
		t.Fatalf("legacy fields = %+v", info)
	}
}

func TestDeviceDetectionFailuresDoNotFailInventory(t *testing.T) {
	probe := &fakeDeviceProbe{
		paths: map[string]string{"nvidia-smi": "nvidia-smi"},
		commandErrors: map[string]error{
			"nvidia-smi --query-gpu=index,name,memory.total,memory.free --format=csv,noheader,nounits": errors.New("probe failed"),
		},
	}
	devices, numaNodes, links := detectHardwareInventory(probe, "linux", "amd64")
	if len(devices) == 0 || devices[0].Kind != deviceCPU || len(numaNodes) != 0 || len(links) != 0 {
		t.Fatalf("inventory after probe failures = devices:%+v numa:%v links:%v", devices, numaNodes, links)
	}
}

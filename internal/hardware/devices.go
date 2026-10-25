package hardware

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	deviceCPU = "cpu"
	deviceGPU = "gpu"
	deviceNPU = "npu"
)

type deviceProbe interface {
	LookPath(string) (string, error)
	Run(string, ...string) ([]byte, error)
	Glob(string) ([]string, error)
	ReadFile(string) ([]byte, error)
}

type systemDeviceProbe struct{}

func (systemDeviceProbe) LookPath(name string) (string, error) {
	return exec.LookPath(name)
}

func (systemDeviceProbe) Run(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, name, args...).Output()
}

func (systemDeviceProbe) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

func (systemDeviceProbe) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func detectHardwareInventory(probe deviceProbe, goos, goarch string) ([]Device, []int, []NetworkLink) {
	devices := detectCPUDevices(probe, goos)
	devices = append(devices, detectNVIDIADevices(probe)...)
	devices = append(devices, detectAMDDevices(probe)...)
	switch goos {
	case "darwin":
		devices = append(devices, detectAppleDevices(probe, goarch)...)
	case "linux":
		devices = append(devices, detectLinuxNPUDevices(probe)...)
	case "windows":
		devices = append(devices, windowsNPUDevices(probe)...)
	}
	devices = append(devices, detectVulkanDevices(probe)...)
	devices = mergeDevices(devices)
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].Kind != devices[j].Kind {
			return deviceKindOrder(devices[i].Kind) < deviceKindOrder(devices[j].Kind)
		}
		if devices[i].Index != devices[j].Index {
			return devices[i].Index < devices[j].Index
		}
		if devices[i].Vendor != devices[j].Vendor {
			return devices[i].Vendor < devices[j].Vendor
		}
		return devices[i].Name < devices[j].Name
	})
	if goos != "linux" {
		return devices, nil, nil
	}
	return devices, detectNUMANodes(probe), detectNetworkLinks(probe)
}

func populateLegacyDeviceFields(info *Info) {
	for _, device := range info.Devices {
		switch device.Kind {
		case deviceGPU:
			info.HasGPU = true
			if info.GPUVendor == "" && device.Vendor != "" {
				info.GPUVendor = device.Vendor
			}
			if device.FreeMemoryMB > info.GPUMemoryFreeMB {
				info.GPUMemoryFreeMB = device.FreeMemoryMB
			}
		case deviceNPU:
			info.HasNPU = true
			if info.NPUVendor == "" && device.Vendor != "" {
				info.NPUVendor = device.Vendor
			}
		}
	}
}

func detectCPUDevices(probe deviceProbe, goos string) []Device {
	switch goos {
	case "linux":
		if contents, err := probe.ReadFile("/proc/cpuinfo"); err == nil {
			if devices := parseLinuxCPUInfo(string(contents)); len(devices) > 0 {
				return devices
			}
		}
	case "darwin":
		if output, ok := runDeviceTool(probe, "sysctl", "-n", "machdep.cpu.brand_string"); ok {
			return []Device{cpuDevice(0, cpuVendor(string(output)), strings.TrimSpace(string(output)))}
		}
	case "windows":
		return windowsCPUDevices(probe)
	}
	return []Device{cpuDevice(0, "", "CPU")}
}

var windowsCPUInventory cachedDeviceInventory

func windowsCPUDevices(probe deviceProbe) []Device {
	if _, isSystemProbe := probe.(systemDeviceProbe); isSystemProbe {
		return windowsCPUInventory.get(func() []Device { return detectWindowsCPUDevices(probe) })
	}
	return detectWindowsCPUDevices(probe)
}

func detectWindowsCPUDevices(probe deviceProbe) []Device {
	const query = "Get-CimInstance Win32_Processor | Select-Object Name,Manufacturer | ConvertTo-Csv -NoTypeInformation | Select-Object -Skip 1"
	for _, executable := range []string{"powershell.exe", "pwsh.exe"} {
		if output, ok := runDeviceTool(probe, executable, "-NoProfile", "-NonInteractive", "-Command", query); ok {
			if devices := parseWindowsCPUInfo(output); len(devices) > 0 {
				return devices
			}
		}
	}
	return []Device{cpuDevice(0, "", "CPU")}
}

func cpuDevice(index int, vendor, name string) Device {
	return Device{
		Kind: deviceCPU, Vendor: vendor, Name: name, Index: index,
		Backends: []string{"cpu"},
	}
}

func parseLinuxCPUInfo(contents string) []Device {
	devicesByIndex := make(map[int]Device)
	index := 0
	var vendor, name string
	add := func() {
		if name == "" && vendor == "" {
			return
		}
		devicesByIndex[index] = cpuDevice(index, cpuVendor(vendor), name)
		index++
		vendor, name = "", ""
	}
	for _, line := range strings.Split(contents, "\n") {
		if strings.TrimSpace(line) == "" {
			add()
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "physical id":
			if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
				index = parsed
			}
		case "vendor_id":
			vendor = strings.TrimSpace(value)
		case "model name":
			name = strings.TrimSpace(value)
		}
	}
	add()
	if len(devicesByIndex) == 0 {
		return nil
	}
	devices := make([]Device, 0, len(devicesByIndex))
	for _, device := range devicesByIndex {
		devices = append(devices, device)
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Index < devices[j].Index })
	return devices
}

func parseWindowsCPUInfo(output []byte) []Device {
	reader := csv.NewReader(bytes.NewReader(output))
	var devices []Device
	for index := 0; ; index++ {
		fields, err := reader.Read()
		if err != nil {
			break
		}
		if len(fields) < 2 {
			continue
		}
		devices = append(devices, cpuDevice(index, cpuVendor(fields[1]), strings.TrimSpace(fields[0])))
	}
	return devices
}

func cpuVendor(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	switch {
	case lower == "genuineintel", strings.Contains(lower, "intel"):
		return "intel"
	case lower == "authenticamd", strings.Contains(lower, "amd"):
		return "amd"
	case strings.Contains(lower, "apple"):
		return "apple"
	default:
		return ""
	}
}

func detectNVIDIADevices(probe deviceProbe) []Device {
	output, ok := runDeviceTool(probe, "nvidia-smi",
		"--query-gpu=index,name,memory.total,memory.free", "--format=csv,noheader,nounits")
	if !ok {
		return nil
	}
	reader := csv.NewReader(bytes.NewReader(output))
	var devices []Device
	for {
		fields, err := reader.Read()
		if err != nil {
			break
		}
		if len(fields) < 4 {
			continue
		}
		index, err := strconv.Atoi(strings.TrimSpace(fields[0]))
		name := strings.TrimSpace(fields[1])
		if err != nil || name == "" {
			continue
		}
		devices = append(devices, Device{
			Kind: deviceGPU, Vendor: "nvidia", Name: name, Index: index,
			MemoryMB: parseUint(fields[2]), FreeMemoryMB: parseUint(fields[3]),
			Backends: []string{"cuda", "vulkan"},
		})
	}
	return devices
}

func detectAMDDevices(probe deviceProbe) []Device {
	output, ok := runDeviceTool(probe, "rocm-smi", "--showproductname", "--showmeminfo", "vram", "--json")
	if !ok {
		return nil
	}
	return parseROCMSMI(output)
}

func parseROCMSMI(output []byte) []Device {
	var cards map[string]json.RawMessage
	if err := json.Unmarshal(output, &cards); err != nil {
		return nil
	}
	var devices []Device
	for card, raw := range cards {
		index, ok := numericSuffix(card, "card")
		if !ok {
			continue
		}
		var properties map[string]json.RawMessage
		if err := json.Unmarshal(raw, &properties); err != nil {
			continue
		}
		fields := make(map[string]string, len(properties))
		for key, value := range properties {
			fields[normalizedKey(key)] = rawString(value)
		}
		name := firstField(fields, "productname", "cardseries", "gpumodel", "gpuproductname")
		if name == "" {
			name = "AMD GPU"
		}
		totalBytes, _ := firstUintField(fields, "vramtotalmemoryb", "memorytotalb", "totalvram")
		freeBytes, hasFree := firstUintField(fields, "vramtotalfreememoryb", "vramtotalavailablememoryb", "memoryfreeb")
		if !hasFree {
			usedBytes, hasUsed := firstUintField(fields, "vramtotalusedmemoryb", "memoryusedb")
			if hasUsed && totalBytes >= usedBytes {
				freeBytes = totalBytes - usedBytes
			}
		}
		driver := firstField(fields, "driverversion", "driver")
		devices = append(devices, Device{
			Kind: deviceGPU, Vendor: "amd", Name: name, Index: index,
			MemoryMB: totalBytes / (1024 * 1024), FreeMemoryMB: freeBytes / (1024 * 1024),
			Driver: driver, Backends: []string{"rocm", "vulkan"},
		})
	}
	return devices
}

func detectAppleDevices(probe deviceProbe, goarch string) []Device {
	if goarch != "arm64" {
		return nil
	}
	var memoryMB uint64
	if output, ok := runDeviceTool(probe, "sysctl", "-n", "hw.memsize"); ok {
		if totalBytes, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64); err == nil {
			memoryMB = totalBytes / (1024 * 1024)
		}
	}
	name := "Apple GPU"
	if cpuOutput, found := runDeviceTool(probe, "sysctl", "-n", "machdep.cpu.brand_string"); found {
		if cpuName := strings.TrimSpace(string(cpuOutput)); cpuName != "" {
			name = cpuName + " GPU"
		}
	}
	return []Device{
		{
			Kind: deviceGPU, Vendor: "apple", Name: name, Index: 0,
			MemoryMB: memoryMB, UnifiedMemory: true, Driver: "Metal",
			Backends: []string{"metal"},
		},
		{
			Kind: deviceNPU, Vendor: "apple-neural-engine", Name: "Apple Neural Engine", Index: 0,
			MemoryMB: memoryMB, UnifiedMemory: true, Driver: "Apple Neural Engine",
			Backends: []string{"coreml", "metal"},
		},
	}
}

func detectLinuxNPUDevices(probe deviceProbe) []Device {
	var devices []Device
	paths, _ := probe.Glob("/sys/class/accel/accel*")
	for _, path := range paths {
		index, ok := numericSuffix(filepath.Base(path), "accel")
		if !ok {
			continue
		}
		devicePath := filepath.Join(path, "device")
		vendor := npuVendorFromPCI(readProbeFile(probe, filepath.Join(devicePath, "vendor")))
		name := firstNonEmpty(
			readProbeFile(probe, filepath.Join(devicePath, "label")),
			readProbeFile(probe, filepath.Join(devicePath, "name")),
		)
		uevent := readProbeFile(probe, filepath.Join(devicePath, "uevent"))
		if vendor == "" {
			vendor = npuVendor(name + " " + uevent)
		}
		driver := driverFromUEvent(uevent)
		if name == "" {
			if vendor == "intel-npu" {
				name = "Intel VPU"
			} else {
				name = fmt.Sprintf("Accelerator %d", index)
			}
		}
		devices = append(devices, Device{
			Kind: deviceNPU, Vendor: vendor, Name: name, Index: index,
			Driver: driver, Backends: npuBackends(vendor),
		})
	}

	for _, pattern := range []string{"/dev/accel*", "/dev/accel/accel*", "/dev/ivpu*"} {
		paths, _ = probe.Glob(pattern)
		for _, path := range paths {
			base := filepath.Base(path)
			prefix := "accel"
			vendor := ""
			namePrefix := "Accelerator"
			backends := []string(nil)
			if strings.HasPrefix(base, "ivpu") {
				prefix = "ivpu"
				vendor = "intel-npu"
				namePrefix = "Intel VPU"
				backends = npuBackends(vendor)
			}
			index, ok := numericSuffix(base, prefix)
			if !ok {
				if prefix != "ivpu" || base != "ivpu" {
					continue
				}
				index = 0
			}
			devices = append(devices, Device{
				Kind: deviceNPU, Vendor: vendor, Name: fmt.Sprintf("%s %d", namePrefix, index),
				Index: index, Driver: prefix, Backends: backends,
			})
		}
	}
	return devices
}

const windowsNPUQuery = `$ErrorActionPreference = 'Stop'
Get-PnpDevice -PresentOnly |
  Where-Object { $_.FriendlyName -match '(?i)NPU|Neural|AI Boost|Hexagon|Gaudi|Hailo|IPU' } |
  ForEach-Object { "$($_.FriendlyName)$([char]9)$($_.Manufacturer)$([char]9)$($_.InstanceId)" }`

type cachedDeviceInventory struct {
	once    sync.Once
	devices []Device
}

func (cache *cachedDeviceInventory) get(detect func() []Device) []Device {
	cache.once.Do(func() {
		cache.devices = cloneDevices(detect())
	})
	return cloneDevices(cache.devices)
}

var windowsNPUInventory cachedDeviceInventory

func windowsNPUDevices(probe deviceProbe) []Device {
	if _, isSystemProbe := probe.(systemDeviceProbe); isSystemProbe {
		return windowsNPUInventory.get(func() []Device { return detectWindowsNPUDevices(probe) })
	}
	return detectWindowsNPUDevices(probe)
}

func detectWindowsNPUDevices(probe deviceProbe) []Device {
	executable := "powershell.exe"
	if _, err := probe.LookPath(executable); err != nil {
		executable = "pwsh.exe"
		if _, err := probe.LookPath(executable); err != nil {
			return nil
		}
	}
	output, err := probe.Run(executable, "-NoProfile", "-NonInteractive", "-Command", windowsNPUQuery)
	if err != nil {
		return nil
	}
	var devices []Device
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Split(strings.TrimSpace(line), "\t")
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimSpace(fields[0])
		manufacturer := strings.TrimSpace(fields[1])
		vendor := npuVendor(name + " " + manufacturer)
		if name == "" || vendor == "" && !hasToken(strings.ToLower(name+" "+manufacturer), "npu") {
			continue
		}
		index := len(devices)
		driver := ""
		if len(fields) > 2 {
			driver = strings.TrimSpace(fields[2])
		}
		devices = append(devices, Device{
			Kind: deviceNPU, Vendor: vendor, Name: name, Index: index,
			Driver: driver, Backends: npuBackends(vendor),
		})
	}
	return devices
}

func detectVulkanDevices(probe deviceProbe) []Device {
	var devices []Device
	if output, ok := runDeviceTool(probe, "vulkaninfo", "--summary"); ok {
		devices = append(devices, parseVulkanSummary(output)...)
	}
	if output, ok := runDeviceTool(probe, "sycl-ls"); ok {
		devices = append(devices, parseSYCLDevices(output)...)
	}
	return devices
}

func parseVulkanSummary(output []byte) []Device {
	type vulkanDevice struct {
		index      int
		vendorID   string
		name       string
		driver     string
		deviceType string
	}
	var devices []Device
	current := vulkanDevice{index: -1}
	add := func() {
		if current.name == "" || strings.Contains(strings.ToLower(current.deviceType), "cpu") {
			current = vulkanDevice{index: -1}
			return
		}
		vendor := gpuVendor(current.vendorID, current.name)
		index := current.index
		if index < 0 {
			index = len(devices)
		}
		devices = append(devices, Device{
			Kind: deviceGPU, Vendor: vendor, Name: current.name, Index: index,
			Driver: current.driver, Backends: []string{"vulkan"},
		})
		current = vulkanDevice{index: -1}
	}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "gpu") && strings.HasSuffix(line, ":") {
			add()
			if index, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(lower, "gpu"), ":")); err == nil {
				current.index = index
			}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "vendorid":
			current.vendorID = strings.TrimPrefix(strings.ToLower(value), "0x")
		case "devicename":
			current.name = value
		case "drivername", "driverid", "driverversion":
			if current.driver == "" {
				current.driver = value
			}
		case "devicetype":
			current.deviceType = value
		}
	}
	add()
	return devices
}

func parseSYCLDevices(output []byte) []Device {
	var devices []Device
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[") {
			continue
		}
		end := strings.IndexByte(line, ']')
		if end < 0 {
			continue
		}
		parts := strings.Split(strings.Trim(line[1:end], " "), ":")
		kindIndex := -1
		for index, part := range parts {
			if part == "gpu" || part == "npu" {
				kindIndex = index
				break
			}
		}
		if kindIndex < 0 || kindIndex+1 >= len(parts) {
			continue
		}
		index, err := strconv.Atoi(parts[kindIndex+1])
		if err != nil {
			continue
		}
		name := strings.TrimSpace(line[end+1:])
		if name == "" {
			continue
		}
		vendor := inferVendorFromName(name)
		backends := []string{"sycl"}
		if kindIndex > 0 && parts[0] != "" {
			backends = append(backends, strings.ToLower(parts[0]))
		}
		if vendor == "intel" {
			backends = append(backends, "openvino")
		}
		deviceKind := deviceGPU
		if parts[kindIndex] == "npu" {
			deviceKind = deviceNPU
		}
		devices = append(devices, Device{
			Kind: deviceKind, Vendor: vendor, Name: name, Index: index, Backends: uniqueStrings(backends),
		})
	}
	return devices
}

func detectNUMANodes(probe deviceProbe) []int {
	paths, _ := probe.Glob("/sys/devices/system/node/node*")
	nodes := make([]int, 0, len(paths))
	for _, path := range paths {
		index, ok := numericSuffix(filepath.Base(path), "node")
		if ok {
			nodes = append(nodes, index)
		}
	}
	sort.Ints(nodes)
	return nodes
}

func detectNetworkLinks(probe deviceProbe) []NetworkLink {
	paths, _ := probe.Glob("/sys/class/net/*")
	links := make([]NetworkLink, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if name == "lo" {
			continue
		}
		speed, _ := strconv.ParseUint(strings.TrimSpace(readProbeFile(probe, filepath.Join(path, "speed"))), 10, 64)
		rdmaPaths, _ := probe.Glob(filepath.Join(path, "device", "infiniband", "*"))
		links = append(links, NetworkLink{
			Name: name, SpeedMbps: speed,
			RDMA: len(rdmaPaths) > 0 || strings.HasPrefix(name, "ib"),
		})
	}
	sort.Slice(links, func(i, j int) bool { return links[i].Name < links[j].Name })
	return links
}

func npuVendor(text string) string {
	value := strings.ToLower(text)
	for _, match := range []struct {
		needle string
		vendor string
	}{
		{"neural engine", "apple-neural-engine"},
		{"ai boost", "intel-npu"},
		{"gaudi", "habana"},
		{"hexagon", "qualcomm-npu"},
		{"hiai", "huawei-npu"},
		{"hailo", "hailo-npu"},
	} {
		if strings.Contains(value, match.needle) {
			return match.vendor
		}
	}
	if strings.Contains(value, "ryzen ai") || strings.Contains(value, "xdna") {
		return "amd-npu"
	}
	if hasToken(value, "ipu") {
		return "intel-npu"
	}
	return ""
}

func hasToken(text, target string) bool {
	for _, token := range strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if token == target {
			return true
		}
	}
	return false
}

func npuVendorFromPCI(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0x8086", "8086":
		return "intel-npu"
	case "0x1022", "1022":
		return "amd-npu"
	case "0x17cb", "17cb":
		return "qualcomm-npu"
	case "0x1e60", "1e60":
		return "huawei-npu"
	default:
		return ""
	}
}

func npuBackends(vendor string) []string {
	switch vendor {
	case "apple-neural-engine":
		return []string{"coreml", "metal"}
	case "intel-npu":
		return []string{"openvino", "directml"}
	case "qualcomm-npu":
		return []string{"hexagon", "qnn"}
	case "amd-npu":
		return []string{"xdna"}
	case "huawei-npu":
		return []string{"cann"}
	case "hailo-npu":
		return []string{"hailo"}
	case "habana":
		return []string{"habana"}
	default:
		return nil
	}
}

func gpuVendor(vendorID, name string) string {
	id, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(vendorID), "0x"), 16, 16)
	if err == nil {
		switch id {
		case 0x10de:
			return "nvidia"
		case 0x1002, 0x1022:
			return "amd"
		case 0x8086:
			return "intel"
		case 0x106b:
			return "apple"
		}
	}
	return inferVendorFromName(name)
}

func inferVendorFromName(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "nvidia"):
		return "nvidia"
	case strings.Contains(lower, "intel"):
		return "intel"
	case strings.Contains(lower, "amd"), strings.Contains(lower, "radeon"):
		return "amd"
	case strings.Contains(lower, "apple"):
		return "apple"
	default:
		return ""
	}
}

func mergeDevices(devices []Device) []Device {
	merged := make([]Device, 0, len(devices))
	for _, device := range devices {
		found := -1
		for index := range merged {
			if sameDevice(merged[index], device) {
				found = index
				break
			}
		}
		if found < 0 {
			device.Backends = uniqueStrings(device.Backends)
			merged = append(merged, device)
			continue
		}
		current := &merged[found]
		if current.Vendor == "" {
			current.Vendor = device.Vendor
		}
		if current.Name == "" || current.Name == "GPU" || current.Name == "CPU" {
			current.Name = device.Name
		}
		if current.MemoryMB == 0 {
			current.MemoryMB = device.MemoryMB
		}
		if current.FreeMemoryMB == 0 {
			current.FreeMemoryMB = device.FreeMemoryMB
		}
		if current.Driver == "" {
			current.Driver = device.Driver
		}
		current.UnifiedMemory = current.UnifiedMemory || device.UnifiedMemory
		current.Backends = uniqueStrings(append(current.Backends, device.Backends...))
	}
	return merged
}

func sameDevice(left, right Device) bool {
	if left.Kind != right.Kind {
		return false
	}
	if left.Vendor != "" && right.Vendor != "" && left.Vendor != right.Vendor {
		return false
	}
	if left.Name != "" && right.Name != "" && strings.EqualFold(left.Name, right.Name) {
		return true
	}
	if left.Name != "" && right.Name != "" && left.Vendor == "" && right.Vendor == "" {
		return false
	}
	return left.Index == right.Index
}

func cloneDevices(devices []Device) []Device {
	if devices == nil {
		return nil
	}
	cloned := make([]Device, len(devices))
	copy(cloned, devices)
	for index := range cloned {
		cloned[index].Backends = append([]string(nil), devices[index].Backends...)
	}
	return cloned
}

func deviceKindOrder(kind string) int {
	switch kind {
	case deviceCPU:
		return 0
	case deviceGPU:
		return 1
	case deviceNPU:
		return 2
	default:
		return 3
	}
}

func runDeviceTool(probe deviceProbe, name string, args ...string) ([]byte, bool) {
	path, err := probe.LookPath(name)
	if err != nil {
		return nil, false
	}
	output, err := probe.Run(path, args...)
	return output, err == nil
}

func parseUint(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	return parsed
}

func numericSuffix(value, prefix string) (int, bool) {
	if !strings.HasPrefix(value, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(value, prefix)
	if suffix == "" {
		return 0, false
	}
	index, err := strconv.Atoi(suffix)
	return index, err == nil
}

func normalizedKey(value string) string {
	var normalized strings.Builder
	for _, char := range strings.ToLower(value) {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			normalized.WriteRune(char)
		}
	}
	return normalized.String()
}

func rawString(value json.RawMessage) string {
	var text string
	if json.Unmarshal(value, &text) == nil {
		return text
	}
	return strings.TrimSpace(string(value))
}

func firstField(fields map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(fields[key]); value != "" {
			return value
		}
	}
	return ""
}

func firstUintField(fields map[string]string, keys ...string) (uint64, bool) {
	for _, key := range keys {
		if value, ok := fields[key]; ok {
			if parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64); err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func readProbeFile(probe deviceProbe, path string) string {
	output, err := probe.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func driverFromUEvent(contents string) string {
	for _, line := range strings.Split(contents, "\n") {
		if driver, ok := strings.CutPrefix(strings.TrimSpace(line), "DRIVER="); ok {
			return strings.TrimSpace(driver)
		}
	}
	return ""
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

package hardware

import (
	"bufio"
	"bytes"
	"regexp"
	"strconv"
	"strings"
)

var llamaDeviceLine = regexp.MustCompile(`^\s*(Vulkan|CUDA|ROCm|XDNA)([0-9]+):\s+(.+)\s+\(([0-9]+) MiB, ([0-9]+) MiB free\)\s*$`)

// ParseLlamaDevices reads the runtime's actual per-device memory, not host RAM.
func ParseLlamaDevices(output []byte) []Device {
	devices := []Device{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		match := llamaDeviceLine.FindStringSubmatch(scanner.Text())
		if match == nil {
			continue
		}
		index, err := strconv.Atoi(match[2])
		if err != nil {
			continue
		}
		total, totalErr := strconv.ParseUint(match[4], 10, 64)
		free, freeErr := strconv.ParseUint(match[5], 10, 64)
		if totalErr != nil || freeErr != nil || total == 0 || free > total {
			continue
		}
		kind := deviceGPU
		backend := strings.ToLower(match[1])
		vendor := gpuVendor("", match[3])
		if backend == "xdna" {
			kind, vendor = deviceNPU, "amd-npu"
		}
		devices = append(devices, Device{
			Kind: kind, Vendor: vendor, Name: strings.TrimSpace(match[3]), Index: index,
			MemoryMB: total, FreeMemoryMB: free, Backends: []string{backend},
		})
	}
	if len(devices) == 0 {
		return devices
	}
	return ApplyRuntimeDevices(nil, devices)
}

// ApplyRuntimeDevices replaces accelerator readings from a successful runtime probe.
func ApplyRuntimeDevices(existing, runtimeDevices []Device) []Device {
	result := append([]Device(nil), existing...)
	for _, device := range runtimeDevices {
		nameMatches, runtimeNameMatches := 0, 0
		nameIndex := -1
		for i, candidate := range result {
			if candidate.Kind == device.Kind && strings.EqualFold(candidate.Name, device.Name) {
				nameMatches++
				nameIndex = i
			}
		}
		for _, candidate := range runtimeDevices {
			if candidate.Kind == device.Kind && strings.EqualFold(candidate.Name, device.Name) {
				runtimeNameMatches++
			}
		}
		matched := false
		for i := range result {
			if (nameMatches == 1 && runtimeNameMatches == 1 && i == nameIndex) ||
				(result[i].Index == device.Index && sameDevice(result[i], device)) {
				result[i].FreeMemoryMB = device.FreeMemoryMB
				result[i].MemoryMB = device.MemoryMB
				result[i].Backends = uniqueStrings(append(append([]string(nil), result[i].Backends...), device.Backends...))
				matched = true
				break
			}
		}
		if !matched {
			result = append(result, device)
		}
	}
	return result
}

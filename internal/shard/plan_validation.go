package shard

import (
	"fmt"
	"sort"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func normalizeCandidates(candidates []CandidateNode) ([]CandidateNode, error) {
	nodes := make([]CandidateNode, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for i, candidate := range candidates {
		candidate.ID = strings.TrimSpace(candidate.ID)
		if candidate.ID == "" {
			return nil, fmt.Errorf("candidate node at index %d has an empty ID", i)
		}
		if _, exists := seen[candidate.ID]; exists {
			return nil, fmt.Errorf("candidate node ID %q is duplicated", candidate.ID)
		}
		seen[candidate.ID] = struct{}{}
		deviceIDs := make(map[string]struct{}, len(candidate.Devices))
		for _, device := range candidate.Devices {
			kind := normalizeDeviceKind(device.Kind)
			key := fmt.Sprintf("%s:%d", kind, device.Index)
			if _, exists := deviceIDs[key]; exists {
				return nil, fmt.Errorf("candidate node %q repeats device %s", candidate.ID, key)
			}
			deviceIDs[key] = struct{}{}
			if device.Index < 0 {
				return nil, fmt.Errorf("candidate node %q has a negative device index", candidate.ID)
			}
			if device.MemoryMB > 0 && device.FreeMemoryMB > device.MemoryMB {
				return nil, fmt.Errorf(
					"candidate node %q device %s has more free memory than total memory",
					candidate.ID, key,
				)
			}
		}
		candidate.OS = strings.ToLower(strings.TrimSpace(candidate.OS))
		candidate.RPCAddress = strings.TrimSpace(candidate.RPCAddress)
		candidate.Devices = append([]hardware.Device(nil), candidate.Devices...)
		candidate.NetworkLinks = append([]hardware.NetworkLink(nil), candidate.NetworkLinks...)
		candidate.Capabilities = engine.CloneCapabilities(candidate.Capabilities)
		nodes[i] = candidate
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

func nodeModeRejection(node CandidateNode, mode string) (string, bool) {
	if len(node.Capabilities.SupportsSharding) > 0 {
		found := false
		for _, supported := range node.Capabilities.SupportsSharding {
			if strings.EqualFold(strings.TrimSpace(supported), mode) {
				found = true
				break
			}
		}
		if !found {
			return "engine capability does not advertise " + mode, false
		}
	}
	if len(node.Capabilities.OS) > 0 {
		found := false
		for _, supported := range node.Capabilities.OS {
			if strings.EqualFold(strings.TrimSpace(supported), node.OS) {
				found = true
				break
			}
		}
		if !found {
			return "engine capability does not support operating system " + node.OS, false
		}
	}
	return "", true
}

func deviceSupportedByEngine(capabilities engine.Capabilities, device hardware.Device) bool {
	if len(capabilities.Devices) > 0 &&
		!engine.SupportsDeviceKind(capabilities.Devices, normalizeDeviceKind(device.Kind)) {
		return false
	}
	return true
}

func sortedDevices(devices []hardware.Device) []hardware.Device {
	result := append([]hardware.Device(nil), devices...)
	sort.Slice(result, func(i, j int) bool {
		left, right := normalizeDeviceKind(result[i].Kind), normalizeDeviceKind(result[j].Kind)
		if left != right {
			return deviceKindOrder(left) < deviceKindOrder(right)
		}
		if result[i].Index != result[j].Index {
			return result[i].Index < result[j].Index
		}
		if result[i].Vendor != result[j].Vendor {
			return result[i].Vendor < result[j].Vendor
		}
		return result[i].Name < result[j].Name
	})
	return result
}

func normalizeDeviceKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}

func deviceKindOrder(kind string) int {
	switch kind {
	case "gpu":
		return 0
	case "npu":
		return 1
	case "cpu":
		return 2
	default:
		return 3
	}
}

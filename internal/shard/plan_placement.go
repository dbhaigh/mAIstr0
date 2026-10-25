package shard

import (
	"fmt"
	"math"
	"sort"
)

func layerRange(totalLayers, partitions, index int) (int, int) {
	base, remainder := totalLayers/partitions, totalLayers%partitions
	start := index * base
	if index < remainder {
		start += index
	} else {
		start += remainder
	}
	count := base
	if index < remainder {
		count++
	}
	return start, start + count
}

func proportionalLayerAssignments(resources []memoryDevice, layerCount int) ([]int, bool) {
	if len(resources) == 0 {
		return nil, false
	}
	totalCapacity := uint64(0)
	for i := range resources {
		if resources[i].freeBytes > math.MaxUint64-totalCapacity {
			return nil, false
		}
		totalCapacity += resources[i].freeBytes
	}
	if totalCapacity == 0 {
		return nil, false
	}
	maxLayerCapacity := 0
	for _, resource := range resources {
		if resource.maxLayers >= layerCount-maxLayerCapacity {
			maxLayerCapacity = layerCount
			break
		}
		maxLayerCapacity += resource.maxLayers
	}
	if maxLayerCapacity < layerCount {
		return nil, false
	}

	assignments := make([]int, len(resources))
	initialRemainders := make([]uint64, len(resources))
	assigned := 0
	for i, resource := range resources {
		if resource.freeBytes == 0 {
			continue
		}
		share, remainder, ok := multiplyDivideFloorRem(uint64(layerCount), resource.freeBytes, totalCapacity)
		if !ok {
			return nil, false
		}
		if share > uint64(resource.maxLayers) {
			share = uint64(resource.maxLayers)
		}
		assignments[i] = int(share)
		initialRemainders[i] = remainder
		assigned += int(share)
	}
	remaining := layerCount - assigned
	if remaining > 0 {
		order := make([]int, 0, len(resources))
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				order = append(order, i)
			}
		}
		sort.SliceStable(order, func(i, j int) bool {
			left, right := order[i], order[j]
			if initialRemainders[left] != initialRemainders[right] {
				return initialRemainders[left] > initialRemainders[right]
			}
			return left < right
		})
		for _, i := range order {
			if remaining == 0 {
				break
			}
			assignments[i]++
			remaining--
		}
	}
	for remaining > 0 {
		activeWeight := uint64(0)
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				if resource.freeBytes > math.MaxUint64-activeWeight {
					return nil, false
				}
				activeWeight += resource.freeBytes
			}
		}
		if activeWeight == 0 {
			return nil, false
		}
		before := remaining
		roundLayers := remaining
		roundRemainders := make([]uint64, len(resources))
		for i, resource := range resources {
			available := resource.maxLayers - assignments[i]
			if available <= 0 {
				continue
			}
			share, remainder, ok := multiplyDivideFloorRem(
				uint64(roundLayers), resource.freeBytes, activeWeight,
			)
			if !ok {
				return nil, false
			}
			if share > uint64(available) {
				share = uint64(available)
			}
			assignments[i] += int(share)
			remaining -= int(share)
			roundRemainders[i] = remainder
		}
		if remaining == 0 {
			break
		}
		order := make([]int, 0, len(resources))
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				order = append(order, i)
			}
		}
		sort.SliceStable(order, func(i, j int) bool {
			left, right := order[i], order[j]
			if roundRemainders[left] != roundRemainders[right] {
				return roundRemainders[left] > roundRemainders[right]
			}
			return left < right
		})
		for _, i := range order {
			if remaining == 0 {
				break
			}
			assignments[i]++
			remaining--
		}
		if remaining == before {
			return nil, false
		}
	}
	return assignments, true
}

func maximumLayersThatFit(usableBytes, weightsBytes, kvBytes uint64, layerCount int) int {
	low, high := 0, layerCount
	for low < high {
		middle := low + (high-low+1)/2
		weights, weightsErr := multiplyDivideCeil(weightsBytes, uint64(middle), uint64(layerCount))
		kv, kvErr := multiplyDivideCeil(kvBytes, uint64(middle), uint64(layerCount))
		total, totalOK := addTwo(weights, kv)
		if weightsErr == nil && kvErr == nil && totalOK && total <= usableBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return low
}

func layerCapacityRationale(resources []memoryDevice, requiredLayers int) []string {
	result := make([]string, 0, len(resources)+1)
	maximum := 0
	for _, resource := range resources {
		maximum += resource.maxLayers
		result = append(result, fmt.Sprintf(
			"node %q device %s:%d can safely hold at most %d of %d layers after its safety margin",
			resource.node.ID, resource.device.Kind, resource.device.Index,
			resource.maxLayers, requiredLayers,
		))
	}
	result = append(result, fmt.Sprintf(
		"compatible devices can hold at most %d of %d layers in total", maximum, requiredLayers,
	))
	return result
}

func sortMemoryDevices(devices []memoryDevice) {
	sort.Slice(devices, func(i, j int) bool {
		left, right := devices[i], devices[j]
		if left.node.ID != right.node.ID {
			return left.node.ID < right.node.ID
		}
		leftKind, rightKind := normalizeDeviceKind(left.device.Kind), normalizeDeviceKind(right.device.Kind)
		if leftKind != rightKind {
			return deviceKindOrder(leftKind) < deviceKindOrder(rightKind)
		}
		if left.device.Index != right.device.Index {
			return left.device.Index < right.device.Index
		}
		if left.device.Vendor != right.device.Vendor {
			return left.device.Vendor < right.device.Vendor
		}
		return left.device.Name < right.device.Name
	})
}

package shard

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/maistr0/maistr0/internal/hardware"
)

type memoryDevice struct {
	node        CandidateNode
	device      hardware.Device
	freeBytes   uint64
	usableBytes uint64
	maxLayers   int
}

const mebibyte = uint64(1024 * 1024)

func makeMemoryDevice(node CandidateNode, device hardware.Device, marginBytes uint64) (memoryDevice, bool) {
	freeBytes, ok := mbToBytes(device.FreeMemoryMB)
	if !ok || freeBytes <= marginBytes {
		return memoryDevice{}, false
	}
	return memoryDevice{
		node: node, device: device, freeBytes: freeBytes,
		usableBytes: freeBytes - marginBytes,
	}, true
}

func makeAllocation(
	device hardware.Device,
	startLayer, endLayer int,
	weightsBytes, kvBytes, marginBytes, freeBytes uint64,
) DeviceAllocation {
	required, ok := addThree(weightsBytes, kvBytes, marginBytes)
	if !ok {
		required = math.MaxUint64
	}
	return DeviceAllocation{
		Kind: normalizeDeviceKind(device.Kind), Vendor: device.Vendor, Name: device.Name,
		Index: device.Index, StartLayer: startLayer, EndLayer: endLayer,
		WeightsBytes: weightsBytes, KVCacheBytes: kvBytes,
		SafetyMarginBytes: marginBytes, RequiredBytes: required, FreeMemoryBytes: freeBytes,
	}
}

func modelKVBytes(model ModelProfile, bytesPerElement uint64) (uint64, bool) {
	value := uint64(2)
	factors := []uint64{
		uint64(model.LayerCount), uint64(model.KVHeads), uint64(model.HeadDim),
		uint64(model.RequestedContext), bytesPerElement,
	}
	for _, factor := range factors {
		if value != 0 && factor > math.MaxUint64/value {
			return 0, false
		}
		value *= factor
	}
	return value, true
}

func mbToBytes(memoryMB uint64) (uint64, bool) {
	if memoryMB > math.MaxUint64/mebibyte {
		return 0, false
	}
	return memoryMB * mebibyte, true
}

func multiplyDivideFloorRem(a, b, divisor uint64) (uint64, uint64, bool) {
	if divisor == 0 {
		return 0, 0, false
	}
	hi, lo := bits.Mul64(a, b)
	if hi >= divisor {
		return 0, 0, false
	}
	quotient, remainder := bits.Div64(hi, lo, divisor)
	return quotient, remainder, true
}

func addTwo(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func addThree(a, b, c uint64) (uint64, bool) {
	sum, ok := addTwo(a, b)
	if !ok {
		return 0, false
	}
	return addTwo(sum, c)
}

func humanBytes(value uint64) string {
	const gib = uint64(1024 * 1024 * 1024)
	if value%gib == 0 {
		return fmt.Sprintf("%d GiB", value/gib)
	}
	return fmt.Sprintf("%.2f GiB", float64(value)/float64(gib))
}

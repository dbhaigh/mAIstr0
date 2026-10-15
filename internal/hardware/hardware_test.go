package hardware

import (
	"encoding/json"
	"testing"
)

func TestNPUVendor(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "intel", text: "Intel(R) AI Boost", want: "intel-npu"},
		{name: "qualcomm", text: "Qualcomm Hexagon NPU", want: "qualcomm-npu"},
		{name: "hailo", text: "Hailo-8 AI Processor", want: "hailo-npu"},
		{name: "generic NPU token", text: "Unknown NPU device", want: "npu"},
		{name: "input device is not an NPU", text: "HID-compliant input device", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := npuVendor(tt.text); got != tt.want {
				t.Fatalf("npuVendor(%q) = %q, want %q", tt.text, got, tt.want)
			}
		})
	}
}

func TestParseNVIDIAUsageUsesBusyAndAvailableGPU(t *testing.T) {
	utilization, freeMemory, ok := parseNVIDIAUsage("10, 1024\n70, 4096\n")
	if !ok || utilization != 70 || freeMemory != 4096 {
		t.Fatalf("parseNVIDIAUsage returned (%d, %d, %t), want (70, 4096, true)", utilization, freeMemory, ok)
	}
}

func TestParseNVIDIAUsageDoesNotTreatUnknownAsIdle(t *testing.T) {
	utilization, freeMemory, ok := parseNVIDIAUsage("N/A, N/A\ninvalid")
	if ok || utilization != 0 || freeMemory != 0 {
		t.Fatalf("invalid telemetry returned (%d, %d, %t), want zero values and unavailable", utilization, freeMemory, ok)
	}
}

func TestParseNPUUsage(t *testing.T) {
	utilization, ok := parseNPUUsage("17.5\n84.2\n")
	if !ok || utilization != 84 {
		t.Fatalf("parseNPUUsage returned (%d, %t), want (84, true)", utilization, ok)
	}
}

func TestParseNPUUsageDoesNotTreatUnknownAsIdle(t *testing.T) {
	for _, output := range []string{"", "N/A", "101", "-1", "invalid"} {
		if utilization, ok := parseNPUUsage(output); ok || utilization != 0 {
			t.Fatalf("parseNPUUsage(%q) = (%d, %t), want (0, false)", output, utilization, ok)
		}
	}
}

func TestInfoJSONPreservesAvailableZeroUtilization(t *testing.T) {
	info := Info{
		GPUStatsAvailable: true,
		NPUStatsAvailable: true,
	}
	payload, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"gpu_utilization_percent", "npu_utilization_percent"} {
		if value, ok := fields[key]; !ok || value != float64(0) {
			t.Errorf("%s = %v (present: %t), want zero retained in JSON", key, value, ok)
		}
	}
}

func TestPercentUsed(t *testing.T) {
	tests := []struct {
		name      string
		total     uint64
		available uint64
		want      int
		ok        bool
	}{
		{name: "valid", total: 100, available: 25, want: 75, ok: true},
		{name: "zero total", total: 0, available: 0},
		{name: "invalid available", total: 100, available: 101},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := percentUsed(tt.total, tt.available)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("percentUsed(%d, %d) = (%d, %t), want (%d, %t)", tt.total, tt.available, got, ok, tt.want, tt.ok)
			}
		})
	}
}

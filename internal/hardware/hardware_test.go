package hardware

import "testing"

func TestNPUVendor(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "intel", text: "Intel(R) AI Boost", want: "intel-npu"},
		{name: "qualcomm", text: "Qualcomm Hexagon NPU", want: "qualcomm-npu"},
		{name: "hailo", text: "Hailo-8 AI Processor", want: "hailo-npu"},
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

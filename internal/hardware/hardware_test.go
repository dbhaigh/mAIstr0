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

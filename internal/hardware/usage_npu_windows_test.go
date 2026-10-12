//go:build windows

package hardware

import "testing"

func TestWindowsNPUEngineTelemetry(t *testing.T) {
	adapterLUIDs, err := windowsNPUAdapterLUIDs()
	if err != nil {
		t.Skipf("DXCore NPU adapter discovery is unavailable: %v", err)
	}
	if len(adapterLUIDs) == 0 {
		t.Skip("no DXCore NPU adapters are present")
	}
	utilization, available, err := readWindowsNPUUsage(adapterLUIDs)
	if err != nil {
		t.Fatal(err)
	}
	if !available || utilization < 0 || utilization > 100 {
		t.Fatalf("NPU usage = (%d, %t), want an available percentage in [0, 100]", utilization, available)
	}
	t.Logf("detected NPU engine utilization: %d%%", utilization)
}

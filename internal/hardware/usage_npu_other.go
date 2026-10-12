//go:build !windows

package hardware

func currentNPUUsage() (int, bool) {
	return 0, false
}

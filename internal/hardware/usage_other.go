//go:build !windows && !linux && !darwin

package hardware

func currentSystemUsage() (int, bool, int, bool) {
	return 0, false, 0, false
}

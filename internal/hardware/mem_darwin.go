//go:build darwin

package hardware

import (
	"os/exec"
	"strconv"
	"strings"
)

// totalMemoryMB returns total physical RAM in MB on macOS via sysctl.
func totalMemoryMB() uint64 {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	bytes, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return bytes / (1024 * 1024)
}

//go:build linux

package hardware

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// totalMemoryMB returns total physical RAM in MB on Linux by parsing /proc/meminfo.
func totalMemoryMB() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil {
					return kb / 1024
				}
			}
		}
	}
	return 0
}

//go:build linux

package supervisor

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processAlive(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	fields := strings.Fields(string(data))
	return len(fields) < 3 || fields[2] != "Z" && fields[2] != "X"
}

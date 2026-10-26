//go:build !windows && !linux && !darwin

package evaluation

import (
	"fmt"
	"runtime"
	"time"
)

func processCPUTime() (time.Duration, error) {
	return 0, fmt.Errorf("process CPU measurement is unsupported on %s", runtime.GOOS)
}

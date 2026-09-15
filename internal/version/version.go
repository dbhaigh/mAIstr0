// Package version contains the build version shared by every mAIstr0 role.
package version

import (
	"fmt"
	"strconv"
)

// Value is replaced by the build scripts with the release number.
var Value = "0.0001"

func Number() float64 {
	n, err := strconv.ParseFloat(Value, 64)
	if err != nil {
		return 0
	}
	return n
}

func String() string {
	return fmt.Sprintf("%.4f", Number())
}

// Compare returns -1, 0, or 1 for two numeric release versions.
func Compare(left, right string) int {
	l, lerr := strconv.ParseFloat(left, 64)
	r, rerr := strconv.ParseFloat(right, 64)
	if lerr != nil || rerr != nil {
		return 0
	}
	if l < r {
		return -1
	}
	if l > r {
		return 1
	}
	return 0
}

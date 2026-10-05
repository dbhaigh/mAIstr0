// Package version contains the build version shared by every mAIstr0 role.
package version

import (
	"strconv"
	"strings"
)

// Value is replaced by the build scripts with the release number.
var Value = "0.0.1"

func Number() float64 {
	return numericVersion(Value)
}

func String() string { return Value }

// Compare returns -1, 0, or 1 for semantic versions. It also compares a
// legacy two-component numeric version with a semantic version during the
// migration from the old auto-incrementing 0.0001 format.
func Compare(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	if len(leftParts) >= 3 && len(rightParts) >= 3 {
		count := len(leftParts)
		if len(rightParts) > count {
			count = len(rightParts)
		}
		for i := 0; i < count; i++ {
			l, r := 0, 0
			var err error
			if i < len(leftParts) {
				l, err = strconv.Atoi(leftParts[i])
				if err != nil {
					return 0
				}
			}
			if i < len(rightParts) {
				r, err = strconv.Atoi(rightParts[i])
				if err != nil {
					return 0
				}
			}
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
		}
		return 0
	}
	l, r := numericVersion(left), numericVersion(right)
	if l < r {
		return -1
	}
	if l > r {
		return 1
	}
	return 0
}

func numericVersion(version string) float64 {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		n, _ := strconv.ParseFloat(version, 64)
		return n
	}
	n, _ := strconv.ParseFloat(parts[0]+"."+strings.Join(parts[1:], ""), 64)
	return n
}

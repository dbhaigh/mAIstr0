// Package version contains the build version shared by every mAIstr0 role.
package version

import (
	"strconv"
	"strings"
)

// Value is replaced by the build scripts with the release number.
var Value = "0.1.0"

func String() string { return Value }

// Compare returns -1, 0, or 1 for numeric release versions, including a fourth
// iteration component (major.minor.patch.iteration). It also compares a
// legacy two-component numeric version with a semantic version during the
// migration from the old auto-incrementing 0.0001 format.
func Compare(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	if len(leftParts) >= 3 && len(rightParts) >= 3 {
		for _, parts := range [][]string{leftParts, rightParts} {
			for _, part := range parts {
				if part == "" {
					return 0
				}
				for _, digit := range part {
					if digit < '0' || digit > '9' {
						return 0
					}
				}
			}
		}
		count := len(leftParts)
		if len(rightParts) > count {
			count = len(rightParts)
		}
		for i := 0; i < count; i++ {
			l, r := "", ""
			if i < len(leftParts) {
				l = strings.TrimLeft(leftParts[i], "0")
			}
			if i < len(rightParts) {
				r = strings.TrimLeft(rightParts[i], "0")
			}
			if len(l) < len(r) || (len(l) == len(r) && l < r) {
				return -1
			}
			if len(l) > len(r) || (len(l) == len(r) && l > r) {
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

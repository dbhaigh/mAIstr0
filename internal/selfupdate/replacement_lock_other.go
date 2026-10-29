//go:build !windows && !linux && !darwin

package selfupdate

import "errors"

func acquireReplacementLock(string) (func() error, bool, error) {
	return nil, false, errors.New("peer update replacement lock is unsupported on this platform")
}

//go:build linux

package selfupdate

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func acquireReplacementLock(target string) (func() error, bool, error) {
	file, err := os.OpenFile(target+".peer-update.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("open replacement lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			if closeErr := file.Close(); closeErr != nil {
				return nil, false, fmt.Errorf("close contended replacement lock: %w", closeErr)
			}
			return nil, false, nil
		}
		return nil, false, errors.Join(fmt.Errorf("acquire replacement lock: %w", err), file.Close())
	}
	return func() error {
		return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
	}, true, nil
}

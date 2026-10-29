//go:build windows

package selfupdate

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func acquireReplacementLock(target string) (func() error, bool, error) {
	file, err := os.OpenFile(target+".peer-update.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("open replacement lock: %w", err)
	}
	var overlapped windows.Overlapped
	err = windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &overlapped,
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		if closeErr := file.Close(); closeErr != nil {
			return nil, false, fmt.Errorf("close contended replacement lock: %w", closeErr)
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Join(fmt.Errorf("acquire replacement lock: %w", err), file.Close())
	}
	return func() error {
		return errors.Join(
			windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlapped),
			file.Close(),
		)
	}, true, nil
}

//go:build windows

package selfupdate

import (
	"fmt"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

func detach(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.DETACHED_PROCESS, HideWindow: true}
}

func waitForParent(pid int, timeout time.Duration) error {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err == windows.ERROR_INVALID_PARAMETER {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open update parent: %w", err)
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	status, err := windows.WaitForSingleObject(handle, uint32(timeout.Milliseconds()))
	if err != nil {
		return err
	}
	if status != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("update parent did not exit before replacement deadline")
	}
	return nil
}

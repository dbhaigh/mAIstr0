//go:build windows

package nodecontrol

import (
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func configureChildProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_NO_WINDOW,
	}
}

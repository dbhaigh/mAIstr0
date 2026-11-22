//go:build windows

package execution

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsProcessTree struct {
	mu  sync.Mutex
	job windows.Handle
}

type jobAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func processTreeSupported() bool { return true }

func startProcessTree(command *exec.Cmd) (processTree, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	information := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	information.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	_, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&information)),
		uint32(unsafe.Sizeof(information)),
	)
	if err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	command.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_NO_WINDOW | windows.CREATE_SUSPENDED,
	}
	if err := command.Start(); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	var assignErr error
	if err := command.Process.WithHandle(func(handle uintptr) {
		assignErr = windows.AssignProcessToJobObject(job, windows.Handle(handle))
	}); err != nil {
		assignErr = err
	}
	if assignErr != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = windows.CloseHandle(job)
		return nil, assignErr
	}
	if err := resumeInitialThread(uint32(command.Process.Pid)); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = windows.CloseHandle(job)
		return nil, err
	}
	return &windowsProcessTree{job: job}, nil
}

func resumeInitialThread(processID uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return err
	}
	for {
		if entry.OwnerProcessID == processID {
			thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if err != nil {
				return err
			}
			_, resumeErr := windows.ResumeThread(thread)
			closeErr := windows.CloseHandle(thread)
			return errors.Join(resumeErr, closeErr)
		}
		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			return err
		}
	}
}

func (tree *windowsProcessTree) Kill() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if tree.job == 0 {
		return nil
	}
	return windows.TerminateJobObject(tree.job, 1)
}

func (tree *windowsProcessTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if tree.job == 0 {
		return nil
	}
	job := tree.job
	information := jobAccountingInformation{}
	err := windows.QueryInformationJobObject(
		job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&information)),
		uint32(unsafe.Sizeof(information)),
		nil,
	)
	if err == nil && information.ActiveProcesses > 0 {
		err = windows.TerminateJobObject(job, 1)
	}
	if err == nil {
		deadline := time.Now().Add(processTreeTerminationWait)
		for {
			information = jobAccountingInformation{}
			err = windows.QueryInformationJobObject(
				job,
				windows.JobObjectBasicAccountingInformation,
				uintptr(unsafe.Pointer(&information)),
				uint32(unsafe.Sizeof(information)),
				nil,
			)
			if err != nil || information.ActiveProcesses == 0 {
				break
			}
			if time.Now().After(deadline) {
				err = errors.New("process job did not terminate")
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	tree.job = 0
	return errors.Join(err, windows.CloseHandle(job))
}

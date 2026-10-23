//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package supervisor

import (
	"os/exec"
)

type basicProcessTree struct {
	command *exec.Cmd
}

func nativeWindows() bool { return false }

func startProcessTree(command *exec.Cmd) (processTree, error) {
	if err := command.Start(); err != nil {
		return nil, err
	}
	return &basicProcessTree{command: command}, nil
}

func (tree *basicProcessTree) Kill() error  { return tree.command.Process.Kill() }
func (tree *basicProcessTree) Close() error { return nil }

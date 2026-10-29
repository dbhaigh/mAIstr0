//go:build !windows

package hardware

import (
	"context"
	"os/exec"
)

func newHardwareCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}

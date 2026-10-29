//go:build !windows

package nodecontrol

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func runAsService(run func(context.Context, bool) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, false)
}

//go:build !windows

package trayapp

import (
	"github.com/maistr0/maistr0/internal/logging"
)

// Run is a no-op on non-Windows platforms (no cgo-free system tray API is
// available cross-compiled from this toolchain). The process just keeps
// running normally as a foreground/background service; the dashboard is
// still reachable at opts.DashboardURL.
func Run(opts Options) {
	logging.Infof("trayapp", "trayapp: system tray is only supported on Windows builds; dashboard available at %s", opts.DashboardURL)
	select {}
}

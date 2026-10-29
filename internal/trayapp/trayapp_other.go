//go:build !windows && !darwin

package trayapp

import (
	"github.com/maistr0/maistr0/internal/logging"
)

// Run is a no-op on platforms without a system tray implementation. The process just keeps
// running normally as a foreground/background service; the dashboard is
// still reachable at opts.DashboardURL.
func Run(opts Options) {
	logging.Infof("trayapp", "trayapp: system tray is unavailable; dashboard available at %s", opts.DashboardURL)
	select {}
}

//go:build darwin && !cgo

package trayapp

import "github.com/maistr0/maistr0/internal/logging"

func Run(opts Options) {
	logging.Infof("trayapp", "trayapp: system tray requires cgo; dashboard available at %s", opts.DashboardURL)
	select {}
}

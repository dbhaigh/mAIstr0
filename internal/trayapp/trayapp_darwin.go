//go:build darwin && cgo

package trayapp

import (
	"os/exec"

	"github.com/getlantern/systray"
	"github.com/maistr0/maistr0/internal/logging"
)

func openPath(path string) {
	if path == "" {
		logging.Errorf("trayapp", "trayapp: cannot open requested path: path is unavailable")
		return
	}
	if err := exec.Command("open", path).Run(); err != nil {
		logging.Errorf("trayapp", "trayapp: failed to open path %q: %v", path, err)
	}
}

func Run(opts Options) {
	systray.Run(func() {
		systray.SetTitle(opts.Title)
		systray.SetTooltip(opts.Title)

		openDashboard := systray.AddMenuItem("Open Dashboard", "Open the mAIstr0 dashboard in your browser")
		viewLogs := systray.AddMenuItem("View Logs", "Open the mAIstr0 application log")
		openLogs := systray.AddMenuItem("Open Logs Folder", "Open current and archived mAIstr0 logs")
		systray.AddSeparator()
		quit := systray.AddMenuItem("Quit", "Stop mAIstr0")

		go func() {
			for {
				select {
				case <-openDashboard.ClickedCh:
					openPath(opts.DashboardURL)
				case <-viewLogs.ClickedCh:
					openPath(opts.LogFilePath)
				case <-openLogs.ClickedCh:
					openPath(opts.LogDirectory)
				case <-quit.ClickedCh:
					systray.Quit()
					return
				}
			}
		}()
	}, func() {
		if opts.OnQuit != nil {
			opts.OnQuit()
		}
	})
}

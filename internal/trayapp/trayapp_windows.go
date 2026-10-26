//go:build windows

package trayapp

import (
	"github.com/maistr0/maistr0/internal/logging"
	"os/exec"
	"syscall"

	"github.com/getlantern/systray"
)

var (
	user32               = syscall.NewLazyDLL("user32.dll")
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	procShowWindow       = user32.NewProc("ShowWindow")
	procGetConsoleWindow = kernel32.NewProc("GetConsoleWindow")
)

const (
	swHide = 0
	swShow = 5
)

func consoleWindow() uintptr {
	h, _, _ := procGetConsoleWindow.Call()
	return h
}

func setConsoleVisible(visible bool) {
	hwnd := consoleWindow()
	if hwnd == 0 {
		return
	}
	cmd := uintptr(swHide)
	if visible {
		cmd = swShow
	}
	// ShowWindow returns the previous visibility state, not an error.
	_, _, _ = procShowWindow.Call(hwnd, cmd)
}

func openBrowser(url string) {
	if err := exec.Command("explorer.exe", url).Start(); err != nil {
		logging.Errorf("trayapp", "trayapp: failed to open browser: %v", err)
	}
}

// Run blocks, driving the tray icon's event loop until Quit is chosen. It
// must be called from the main goroutine.
func Run(opts Options) {
	consoleVisible := opts.Mode != "hidden"
	setConsoleVisible(consoleVisible)

	systray.Run(func() {
		systray.SetTitle(opts.Title)
		systray.SetTooltip(opts.Title)

		mOpen := systray.AddMenuItem("Open Dashboard", "Open the mAIstr0 dashboard in your browser")
		mLogs := systray.AddMenuItem("View Logs", "Open the mAIstr0 application log")
		mLogFolder := systray.AddMenuItem("Open Logs Folder", "Open current and archived mAIstr0 logs")
		mToggle := systray.AddMenuItem("Hide Console", "Toggle the console window")
		if !consoleVisible {
			mToggle.SetTitle("Show Console")
		}
		systray.AddSeparator()
		mQuit := systray.AddMenuItem("Quit", "Stop mAIstr0")

		go func() {
			for {
				select {
				case <-mOpen.ClickedCh:
					openBrowser(opts.DashboardURL)
				case <-mLogs.ClickedCh:
					openLogFile(opts.LogFilePath)
				case <-mLogFolder.ClickedCh:
					openLogDirectory(opts.LogDirectory)
				case <-mToggle.ClickedCh:
					consoleVisible = !consoleVisible
					setConsoleVisible(consoleVisible)
					if consoleVisible {
						mToggle.SetTitle("Hide Console")
					} else {
						mToggle.SetTitle("Show Console")
					}
				case <-mQuit.ClickedCh:
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

func openLogFile(path string) {
	if path == "" {
		logging.Errorf("trayapp", "trayapp: cannot open application log: log file path is unavailable")
		return
	}
	if err := exec.Command("explorer.exe", path).Start(); err != nil {
		logging.Errorf("trayapp", "trayapp: failed to open application log %q: %v", path, err)
	}
}

func openLogDirectory(path string) {
	if path == "" {
		logging.Errorf("trayapp", "trayapp: cannot open application log directory: path is unavailable")
		return
	}
	if err := exec.Command("explorer.exe", path).Start(); err != nil {
		logging.Errorf("trayapp", "trayapp: failed to open application log directory %q: %v", path, err)
	}
}

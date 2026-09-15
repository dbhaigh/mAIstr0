//go:build windows

package trayapp

import (
	"log"
	"os/exec"
	"syscall"

	"github.com/getlantern/systray"
)

var (
	user32                  = syscall.NewLazyDLL("user32.dll")
	kernel32                = syscall.NewLazyDLL("kernel32.dll")
	procShowWindow          = user32.NewProc("ShowWindow")
	procGetConsoleWindow    = kernel32.NewProc("GetConsoleWindow")
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
	procShowWindow.Call(hwnd, cmd)
}

func openBrowser(url string) {
	if err := exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start(); err != nil {
		log.Printf("trayapp: failed to open browser: %v", err)
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

// Package trayapp gives mAIstr0 a system tray presence with a right-click
// context menu, so the process can be minimized to the taskbar or hidden
// entirely behind a tray icon depending on configuration. Real tray/console
// control is only implemented on Windows (see trayapp_windows.go); other
// platforms use the no-op stub in trayapp_other.go and just keep running as
// a normal foreground/background process.
package trayapp

// Options configures the tray icon's behaviour.
type Options struct {
	Title string // tooltip / menu title, e.g. "mAIstr0 orchestrator"
	// Mode is "taskbar" (console window visible, tray icon optional extra)
	// or "hidden" (console window hidden immediately, tray icon only).
	Mode string
	// DashboardURL is opened by the "Open Dashboard" menu item.
	DashboardURL string
	// OnQuit is called when the user chooses Quit from the tray menu.
	OnQuit func()
}

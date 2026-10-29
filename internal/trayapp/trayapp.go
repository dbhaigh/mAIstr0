// Package trayapp gives mAIstr0 a system tray presence with a right-click
// context menu. Windows can also minimize or hide its console window; macOS
// runs as a menu bar application. Other platforms keep running normally.
package trayapp

// Options configures the tray icon's behaviour.
type Options struct {
	Title string // tooltip / menu title, e.g. "mAIstr0 orchestrator"
	// Mode is "taskbar" (Windows console visible) or "hidden" (Windows console
	// hidden immediately). macOS always uses a menu bar icon.
	Mode string
	// DashboardURL is opened by the "Open Dashboard" menu item.
	DashboardURL string
	// LogFilePath is opened by the "View Logs" menu item.
	LogFilePath string
	// LogDirectory is opened by the "Open Logs Folder" menu item.
	LogDirectory string
	// OnQuit is called when the user chooses Quit from the tray menu.
	OnQuit func()
}

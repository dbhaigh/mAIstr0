// Package web embeds the control-plane GUI static assets into the binary so
// the orchestrator can serve them without external files.
package web

import "embed"

//go:embed static
var StaticFiles embed.FS

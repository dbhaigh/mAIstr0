//go:build !windows

package main

import "os/exec"

func hideProbeWindow(*exec.Cmd) {}

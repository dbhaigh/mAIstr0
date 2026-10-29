package main

import "testing"

func TestRuntimeBooleanSettingsOverrideConfigAndRespectCLI(t *testing.T) {
	t.Run("saved browser setting overrides config", func(t *testing.T) {
		saved := true
		if !resolveAutoOpenBrowser(false, &saved, nil, false, false) {
			t.Fatal("saved browser setting did not override node config")
		}
	})
	t.Run("explicit no-browser wins", func(t *testing.T) {
		saved := true
		if resolveAutoOpenBrowser(true, &saved, map[string]bool{"no-browser": true}, true, true) {
			t.Fatal("--no-browser must take precedence")
		}
	})
	t.Run("explicit browser flag wins", func(t *testing.T) {
		saved := false
		if !resolveAutoOpenBrowser(false, &saved, map[string]bool{"auto-open-browser": true}, false, true) {
			t.Fatal("--auto-open-browser must take precedence")
		}
	})
	t.Run("saved discovery setting overrides config", func(t *testing.T) {
		saved := true
		if !resolveDiscoveryEnabled(false, &saved, nil, false, false) {
			t.Fatal("saved discovery setting did not override node config")
		}
	})
	t.Run("explicit no-discovery wins", func(t *testing.T) {
		saved := true
		if resolveDiscoveryEnabled(true, &saved, map[string]bool{"no-discovery": true}, true, true) {
			t.Fatal("--no-discovery must take precedence")
		}
	})
}

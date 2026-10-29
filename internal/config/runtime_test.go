package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRuntimeSettingsRoundTripAndFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config", "runtime.json")
	role := "node"
	allowUpdates := true
	requirePairingVerification := true
	settings := RuntimeSettings{
		Role: &role, AllowPeerUpdates: &allowUpdates,
		RequirePairingVerification: &requirePairingVerification,
	}
	if err := SaveRuntimeSettings(path, settings); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRuntimeSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role == nil || *got.Role != role || got.AllowPeerUpdates == nil || !*got.AllowPeerUpdates ||
		got.RequirePairingVerification == nil || !*got.RequirePairingVerification {
		t.Fatalf("unexpected settings after round trip: %+v", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("runtime settings mode = %o, want 600", info.Mode().Perm())
	}
}

func TestRuntimeSettingsRejectInvalidValues(t *testing.T) {
	invalidRole := "mystery"
	if err := (RuntimeSettings{Role: &invalidRole}).Validate(); err == nil {
		t.Fatal("invalid role was accepted")
	}
	invalidAddress := "https://node.example\n--bad"
	if err := (RuntimeSettings{OrchestratorAddr: &invalidAddress}).Validate(); err == nil {
		t.Fatal("control characters were accepted")
	}
}

func TestLoadRuntimeSettingsMissingFileIsEmpty(t *testing.T) {
	settings, err := LoadRuntimeSettings(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatal(err)
	}
	if settings.Role != nil || settings.AllowPeerUpdates != nil || settings.RequirePairingVerification != nil {
		t.Fatalf("missing file must not add overrides: %+v", settings)
	}
}

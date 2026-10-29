//go:build !windows

package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
)

func TestSignalShutdownClosesRunningNode(t *testing.T) {
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"models":[]}`))
	}))
	defer engine.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	dir := t.TempDir()
	cfg := config.DefaultNodeConfig()
	cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr = "signal-probe", address, "https://"+address
	cfg.MemoryPath = filepath.Join(dir, "node.db")
	cfg.DiscoveryEnabled, cfg.AutoOpenBrowser = false, false
	cfg.Engines = []config.EngineConfig{{Name: "ollama", URL: engine.URL}}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(dir, "maistr0-bin")
	build := exec.Command("go", "build", "-o", executable, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build node subprocess: %v\n%s", err, output)
	}
	command := exec.Command(executable, "--role=node", "--node-config="+path,
		"--no-discovery", "--no-browser", "--no-peer-updates")
	command.Env = append(os.Environ(), "HOME="+dir, "PATH=",
		"XDG_STATE_HOME="+dir, "XDG_CONFIG_HOME="+dir, "XDG_CACHE_HOME="+dir)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			_ = command.Process.Kill()
			<-done
		}
	})
	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
ready:
	for {
		select {
		case <-done:
			t.Fatalf("node exited during startup: %v\n%s", waitErr, output.String())
		case <-deadline.C:
			_ = command.Process.Kill()
			<-done
			t.Fatalf("node did not become ready\n%s", output.String())
		case <-tick.C:
			response, err := client.Get("http://" + address + "/api/version")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					break ready
				}
			}
		}
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if waitErr != nil {
			t.Fatalf("SIGTERM did not exit cleanly: %v\n%s", waitErr, output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SIGTERM did not stop the running node")
	}
}

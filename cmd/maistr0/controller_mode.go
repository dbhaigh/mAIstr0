package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/nodecontrol"
	"github.com/maistr0/maistr0/internal/selfupdate"
)

func runControllerMode(args []string) error {
	flags := flag.NewFlagSet("controller", flag.ContinueOnError)
	runtimePath := flags.String("runtime-config", config.DefaultRuntimeSettingsPath(), "path to persistent runtime settings")
	nodeConfigPath := flags.String("node-config", "", "path to node JSON config file")
	listenAddr := flags.String("listen", ":7453", "HTTPS control listener address")
	serviceManaged := flags.Bool("service-managed", false, "controller is managed by an OS service manager")
	cleanupPeerUpdate := flags.String("cleanup-peer-update", "", "internal exited replacement-helper cleanup")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected controller arguments: %v", flags.Args())
	}
	if *cleanupPeerUpdate != "" {
		if err := selfupdate.Cleanup(*cleanupPeerUpdate); err != nil {
			return fmt.Errorf("clean up peer update helper: %w", err)
		}
	}
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate controller executable: %w", err)
	}
	if err := selfupdate.CleanupPending(executablePath); err != nil {
		return fmt.Errorf("clean up previous peer update helper: %w", err)
	}
	restartArgs := append([]string{"controller"}, selfupdate.RestartArgs(args)...)
	return nodecontrol.RunService(nodecontrol.Options{
		ExecutablePath:      executablePath,
		RuntimeSettingsPath: *runtimePath,
		NodeConfigPath:      *nodeConfigPath,
		ListenAddr:          *listenAddr,
		RestartArgs:         restartArgs,
		ServiceManaged:      *serviceManaged,
	})
}

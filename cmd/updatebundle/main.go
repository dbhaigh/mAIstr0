// Command updatebundle writes checked metadata and optional central-update payloads.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/version"
)

func main() {
	root := flag.String("root", "dist", "directory containing platform build folders")
	release := flag.String("version", version.String(), "release of the binaries just built")
	windowsBinary := flag.String("windows-binary", "maistr0.exe", "Windows output name (maistr0-update.exe when installed image is in use)")
	darwinOnly := flag.Bool("darwin-only", false, "write manifests for darwin/amd64 and darwin/arm64 bundles only")
	catalogOutput := flag.String("catalog-output", "", "optional unsigned central update catalog path; requires all five platform binaries")
	flag.Parse()
	targets := []struct{ folder, name string }{
		{"windows-amd64", *windowsBinary},
		{"linux-amd64", "maistr0"},
		{"linux-arm64", "maistr0"},
		{"linux-ubuntu-amd64", "maistr0"},
	}
	if *darwinOnly {
		targets = []struct{ folder, name string }{
			{"darwin-amd64", "maistr0"},
			{"darwin-arm64", "maistr0"},
		}
	}
	for _, target := range targets {
		path := filepath.Join(*root, target.folder, target.name)
		manifest, err := selfupdate.Inspect(path, *release)
		if err == nil && manifest.OS == runtime.GOOS && manifest.Arch == runtime.GOARCH {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			command := exec.CommandContext(ctx, path, "-version")
			hideProbeWindow(command)
			output, probeErr := command.Output()
			cancel()
			if probeErr != nil {
				err = fmt.Errorf("probe bundle version: %w", probeErr)
			} else if strings.TrimSpace(string(output)) != "maistr0 "+*release {
				err = fmt.Errorf("%s does not report requested release %s", path, *release)
			}
		} else if err == nil {
			fmt.Printf("%s: foreign-platform release %s is asserted by the publisher; receiver verifies -version before installation\n", path, *release)
		}
		if err == nil {
			var data []byte
			data, err = json.MarshalIndent(manifest, "", "  ")
			if err == nil {
				err = os.WriteFile(path+".peer.json", append(data, '\n'), 0600)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if *catalogOutput != "" {
		if err := writeCentralUpdatePayload(*root, *release, *windowsBinary, *catalogOutput); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func writeCentralUpdatePayload(root, release, windowsBinary, output string) error {
	catalog, err := selfupdate.BuildReleaseCatalog(root, release, windowsBinary)
	if err != nil {
		return err
	}
	outputPath, err := filepath.Abs(output)
	if err != nil {
		return fmt.Errorf("resolve central catalog output path: %w", err)
	}
	outputDir := filepath.Dir(outputPath)
	for _, artifact := range catalog.Artifacts {
		relativePath := filepath.FromSlash(artifact.Path)
		sourcePath := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(artifact.Path, "v"+release+"/")))
		targetPath := filepath.Join(outputDir, relativePath)
		if err := copyUpdateArtifact(sourcePath, targetPath); err != nil {
			return fmt.Errorf("stage central update artifact %s/%s: %w", artifact.OS, artifact.Arch, err)
		}
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create central update payload directory: %w", err)
	}
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("encode central update catalog: %w", err)
	}
	if err := os.WriteFile(outputPath, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write unsigned central update catalog: %w", err)
	}
	return nil
}

func copyUpdateArtifact(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open release binary: %w", err)
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return fmt.Errorf("inspect release binary: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return fmt.Errorf("create staged release binary: %w", err)
	}
	_, copyErr := io.Copy(target, source)
	syncErr := target.Sync()
	closeErr := target.Close()
	return errors.Join(copyErr, syncErr, closeErr)
}

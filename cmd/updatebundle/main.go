// Command updatebundle writes checked metadata for cross-platform peer bundles.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	flag.Parse()
	for _, target := range []struct{ folder, name string }{
		{"windows-amd64", *windowsBinary},
		{"linux-amd64", "maistr0"},
		{"linux-ubuntu-amd64", "maistr0"},
	} {
		path := filepath.Join(*root, target.folder, target.name)
		manifest, err := selfupdate.Inspect(path, *release)
		if err == nil && manifest.OS == runtime.GOOS && manifest.Arch == runtime.GOARCH {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			command := exec.CommandContext(ctx, path, "-version")
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
}

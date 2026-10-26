package selfupdate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

func RestartArgs(args []string) []string {
	result := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "-cleanup-peer-update" {
			i++
			continue
		}
		if strings.HasPrefix(args[i], "-cleanup-peer-update=") {
			continue
		}
		result = append(result, args[i])
	}
	return result
}

// Cleanup removes only the exact staged helper image named by the local job.
func Cleanup(path string) error {
	target, err := os.Executable()
	if err != nil {
		return err
	}
	if !filepath.IsAbs(path) || filepath.Dir(path) != filepath.Dir(target) ||
		!strings.HasPrefix(filepath.Base(path), ".maistr0-update-") || path == target {
		return errors.New("refusing cleanup outside the staged peer image")
	}
	for range 100 {
		err = os.Remove(path)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("remove exited peer update helper: %w", err)
}

type applyJob struct {
	Parent    int      `json:"parent"`
	Target    string   `json:"target"`
	Staged    string   `json:"staged"`
	Directory string   `json:"directory"`
	Args      []string `json:"args"`
	Manifest  Manifest `json:"manifest"`
}

// LaunchHelper starts the staged image in replacement mode before its parent
// exits. Replacement waits for that exact parent process; it never kills it.
func LaunchHelper(target, staged string, manifest Manifest, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, staged, "-version")
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("probe staged executable version: %w", err)
	}
	if strings.TrimSpace(string(output)) != "maistr0 "+manifest.Version {
		return errors.New("staged executable does not report the offered version")
	}
	directory, err := os.Getwd()
	if err != nil {
		return err
	}
	job := applyJob{Parent: os.Getpid(), Target: target, Staged: staged, Directory: directory, Args: RestartArgs(args), Manifest: manifest}
	file, err := os.CreateTemp(filepath.Dir(target), ".maistr0-apply-*.json")
	if err != nil {
		return err
	}
	path := file.Name()
	if err := errors.Join(json.NewEncoder(file).Encode(job), file.Close()); err != nil {
		_ = os.Remove(path)
		return err
	}
	log, err := os.OpenFile(target+".peer-update.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	defer closeutil.Discard(log)
	helper := exec.Command(staged, "-apply-peer-update", path)
	detach(helper)
	helper.Stdout, helper.Stderr = log, log
	if err := helper.Start(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("launch update replacement helper: %w", err)
	}
	return helper.Process.Release()
}

func Apply(path string) (result error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var job applyJob
	if err := json.Unmarshal(data, &job); err != nil {
		return err
	}
	defer func() { _ = os.Remove(path) }()
	if !filepath.IsAbs(job.Target) || filepath.Dir(job.Target) != filepath.Dir(job.Staged) || job.Target == job.Staged || job.Parent <= 0 {
		return errors.New("invalid local update replacement job")
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if !strings.EqualFold(filepath.Clean(self), filepath.Clean(job.Staged)) {
		return errors.New("replacement helper is not the staged image")
	}
	manifest, err := Inspect(job.Staged, job.Manifest.Version)
	if err != nil || manifest != job.Manifest {
		return errors.New("staged image changed before replacement")
	}
	if err := waitForParent(job.Parent, 2*time.Minute); err != nil {
		return err
	}
	start := func(cleanup bool) error {
		args := append([]string(nil), job.Args...)
		if cleanup {
			args = append(args, "-cleanup-peer-update", job.Staged)
		}
		command := exec.Command(job.Target, args...)
		command.Dir = job.Directory
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		detach(command)
		if err := command.Start(); err != nil {
			return err
		}
		return command.Process.Release()
	}
	original, err := Inspect(job.Target, job.Manifest.Version)
	if err != nil {
		return fmt.Errorf("inspect installed image before replacement: %w", err)
	}
	defer func() {
		if result == nil {
			return
		}
		current, err := Inspect(job.Target, original.Version)
		if err == nil && current == original {
			result = errors.Join(result, start(false))
		} else {
			result = errors.Join(result, errors.New("original image could not be verified for automatic recovery"), err)
		}
	}()
	backup := job.Target + ".previous"
	if _, err := os.Stat(backup); err == nil {
		if err := os.Remove(backup); err != nil {
			return fmt.Errorf("remove previous backup: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Windows locks the helper's own image, so copy it to another temporary
	// file before moving that copy into the installed executable's name.
	replacement, err := os.CreateTemp(filepath.Dir(job.Target), ".maistr0-replace-*")
	if err != nil {
		return err
	}
	replacementPath := replacement.Name()
	defer func() { _ = os.Remove(replacementPath) }()
	input, err := os.Open(job.Staged)
	if err != nil {
		_ = replacement.Close()
		return err
	}
	_, copyErr := replacement.ReadFrom(input)
	if err := errors.Join(copyErr, input.Close(), replacement.Sync(), replacement.Close()); err != nil {
		return err
	}
	if err := os.Chmod(replacementPath, 0700); err != nil {
		return err
	}
	if err := os.Rename(job.Target, backup); err != nil {
		return fmt.Errorf("preserve current executable: %w", err)
	}
	if err := os.Rename(replacementPath, job.Target); err != nil {
		restoreErr := os.Rename(backup, job.Target)
		return errors.Join(fmt.Errorf("replace executable: %w", err), restoreErr)
	}

	if err := start(true); err != nil {
		removeErr := os.Remove(job.Target)
		restoreErr := os.Rename(backup, job.Target)
		return errors.Join(fmt.Errorf("launch updated executable: %w", err), removeErr, restoreErr)
	}
	// The replacement is already installed and running, so a failed status
	// line on the detached helper's stdout must not trigger recovery.
	_, _ = fmt.Fprintf(os.Stdout, "peer update installed version %s; previous executable retained at %s\n", job.Manifest.Version, backup)
	return nil
}

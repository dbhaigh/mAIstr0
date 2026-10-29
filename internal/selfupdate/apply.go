package selfupdate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/version"
)

func RestartArgs(args []string) []string {
	result := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		isCleanupFlag := args[i] == "-cleanup-peer-update" || args[i] == "--cleanup-peer-update"
		isCleanupValue := strings.HasPrefix(args[i], "-cleanup-peer-update=") ||
			strings.HasPrefix(args[i], "--cleanup-peer-update=")
		if isCleanupFlag {
			i++
			continue
		}
		if isCleanupValue {
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

func CleanupPending(executablePath string) error {
	marker := executablePath + ".peer-update-cleanup"
	data, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read peer update cleanup marker: %w", err)
	}
	staged := strings.TrimSpace(string(data))
	if staged == "" {
		return errors.New("peer update cleanup marker is empty")
	}
	if err := Cleanup(staged); err != nil {
		return err
	}
	if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove peer update cleanup marker: %w", err)
	}
	return nil
}

func writeDeferredCleanup(executablePath, staged string) error {
	marker := executablePath + ".peer-update-cleanup"
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create peer update cleanup marker: %w", err)
	}
	_, writeErr := io.WriteString(file, staged+"\n")
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(marker)
		return fmt.Errorf("write peer update cleanup marker: %w", err)
	}
	return nil
}

type applyJob struct {
	Parent                   int      `json:"parent"`
	Target                   string   `json:"target"`
	Staged                   string   `json:"staged"`
	Directory                string   `json:"directory"`
	Args                     []string `json:"args"`
	Manifest                 Manifest `json:"manifest"`
	ControllerPID            int      `json:"controller_pid,omitempty"`
	ControllerServiceManaged bool     `json:"controller_service_managed,omitempty"`
	ControllerArgs           []string `json:"controller_args,omitempty"`
	HandoffPath              string   `json:"handoff_path,omitempty"`
}

type ControllerHandoff struct {
	ControllerPID  int
	ServiceManaged bool
	RestartArgs    []string
}

type controllerHandoffRequest struct {
	ParentPID     int `json:"parent_pid"`
	ControllerPID int `json:"controller_pid"`
}

// LaunchHelper starts the staged image in replacement mode before its parent
// exits. Replacement waits for that exact parent process; it never kills it.
func LaunchHelper(target, staged string, manifest Manifest, args []string) error {
	return LaunchHelperWithControllerHandoff(target, staged, manifest, args, ControllerHandoff{})
}

func LaunchHelperWithControllerHandoff(target, staged string, manifest Manifest, args []string, handoff ControllerHandoff) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, staged, "-version")
	hideConsole(command)
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
	job := applyJob{
		Parent: os.Getpid(), Target: target, Staged: staged, Directory: directory,
		Args: RestartArgs(args), Manifest: manifest,
		ControllerPID:            handoff.ControllerPID,
		ControllerServiceManaged: handoff.ServiceManaged,
		ControllerArgs:           append([]string(nil), handoff.RestartArgs...),
	}
	if handoff.ControllerPID != 0 {
		if handoff.ControllerPID < 0 || handoff.ControllerPID == job.Parent ||
			len(handoff.RestartArgs) == 0 || handoff.RestartArgs[0] != "controller" {
			return errors.New("invalid controller update handoff")
		}
		job.HandoffPath = controllerHandoffPath(target, handoff.ControllerPID, job.Parent)
	} else if handoff.ServiceManaged || len(handoff.RestartArgs) != 0 {
		return errors.New("controller restart details require a controller process")
	}
	file, err := os.CreateTemp(filepath.Dir(target), ".maistr0-apply-*.json")
	if err != nil {
		return err
	}
	path := file.Name()
	if err := errors.Join(json.NewEncoder(file).Encode(job), file.Close()); err != nil {
		_ = os.Remove(path)
		return err
	}
	if job.HandoffPath != "" {
		if err := writeControllerHandoff(job.HandoffPath, controllerHandoffRequest{
			ParentPID: job.Parent, ControllerPID: job.ControllerPID,
		}); err != nil {
			_ = os.Remove(path)
			_ = os.Remove(job.HandoffPath)
			return fmt.Errorf("request controller update handoff: %w", err)
		}
	}
	log, err := os.OpenFile(target+".peer-update.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		_ = os.Remove(path)
		_ = os.Remove(job.HandoffPath)
		return err
	}
	defer closeutil.Discard(log)
	helper := exec.Command(staged, "-apply-peer-update", path)
	detach(helper)
	helper.Stdout, helper.Stderr = log, log
	if err := helper.Start(); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(job.HandoffPath)
		return fmt.Errorf("launch update replacement helper: %w", err)
	}
	if err := helper.Process.Release(); err != nil {
		_ = helper.Process.Kill()
		_ = helper.Wait()
		_ = os.Remove(path)
		_ = os.Remove(job.HandoffPath)
		return fmt.Errorf("release update replacement helper: %w", err)
	}
	return nil
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
	if job.ControllerPID < 0 || job.ControllerPID == job.Parent ||
		(job.ControllerPID == 0 && (job.HandoffPath != "" || job.ControllerServiceManaged || len(job.ControllerArgs) != 0)) ||
		(job.ControllerPID > 0 && (len(job.ControllerArgs) == 0 || job.ControllerArgs[0] != "controller" ||
			job.HandoffPath != controllerHandoffPath(job.Target, job.ControllerPID, job.Parent))) {
		return errors.New("invalid controller update handoff")
	}
	if job.HandoffPath != "" {
		defer func() { _ = os.Remove(job.HandoffPath) }()
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if !strings.EqualFold(filepath.Clean(self), filepath.Clean(job.Staged)) {
		return errors.New("replacement helper is not the staged image")
	}
	manifest, err := Inspect(job.Staged, job.Manifest.Version)
	if err != nil || !sameArtifact(manifest, job.Manifest) {
		return errors.New("staged image changed before replacement")
	}
	releaseLock, acquired, err := acquireReplacementLock(job.Target)
	if err != nil {
		return fmt.Errorf("lock peer update replacement: %w", err)
	}
	if !acquired {
		return nil
	}
	defer func() { result = errors.Join(result, releaseLock()) }()
	if err := waitForParent(job.Parent, 2*time.Minute); err != nil {
		return err
	}
	if job.ControllerPID > 0 {
		if err := waitForParent(job.ControllerPID, 2*time.Minute); err != nil {
			return fmt.Errorf("wait for controller to exit for update: %w", err)
		}
	}
	installedVersion, err := executableVersion(job.Target)
	if err != nil {
		return fmt.Errorf("inspect installed executable version: %w", err)
	}
	if version.Compare(installedVersion, job.Manifest.Version) >= 0 {
		if installedVersion == job.Manifest.Version {
			installed, err := Inspect(job.Target, installedVersion)
			if err != nil {
				return fmt.Errorf("inspect already-installed executable: %w", err)
			}
			if !sameArtifact(installed, job.Manifest) {
				return errors.New("installed executable has the offered version but different content")
			}
			if _, err := installArtifactManifest(job.Target, job.Manifest); err != nil {
				return fmt.Errorf("synchronize installed peer update manifest: %w", err)
			}
		}
		return nil
	}
	start := func(cleanup bool) error {
		if job.ControllerPID > 0 && job.ControllerServiceManaged {
			return writeDeferredCleanup(job.Target, job.Staged)
		}
		args, shouldStart := replacementStartArgs(job, cleanup)
		if !shouldStart {
			return nil
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
	original, err := Inspect(job.Target, installedVersion)
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
	restoreManifest, err := installArtifactManifest(job.Target, job.Manifest)
	if err != nil {
		removeErr := os.Remove(job.Target)
		restoreErr := os.Rename(backup, job.Target)
		return errors.Join(fmt.Errorf("install peer update manifest: %w", err), removeErr, restoreErr)
	}

	if err := start(true); err != nil {
		manifestErr := error(nil)
		if restoreManifest != nil {
			manifestErr = restoreManifest()
		}
		removeErr := os.Remove(job.Target)
		restoreErr := os.Rename(backup, job.Target)
		return errors.Join(fmt.Errorf("launch updated executable: %w", err), manifestErr, removeErr, restoreErr)
	}
	// The replacement is already installed and running, so a failed status
	// line on the detached helper's stdout must not trigger recovery.
	_, _ = fmt.Fprintf(os.Stdout, "peer update installed version %s; previous executable retained at %s\n", job.Manifest.Version, backup)
	return nil
}

func executableVersion(path string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "-version")
	hideConsole(command)
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	const prefix = "maistr0 "
	release := strings.TrimSpace(string(output))
	if !strings.HasPrefix(release, prefix) {
		return "", errors.New("executable returned an invalid version response")
	}
	release = strings.TrimPrefix(release, prefix)
	if !releaseVersion.MatchString(release) {
		return "", errors.New("executable returned an invalid release version")
	}
	return release, nil
}

func installArtifactManifest(target string, manifest Manifest) (func() error, error) {
	path := target + ".peer.json"
	backup := path + ".previous"
	if data, err := os.ReadFile(path); err == nil {
		var current Manifest
		if json.Unmarshal(data, &current) == nil && current == manifest {
			return nil, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read existing peer update manifest: %w", err)
	}

	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode peer update manifest: %w", err)
	}
	encoded = append(encoded, '\n')
	file, err := os.CreateTemp(filepath.Dir(path), ".maistr0-manifest-*")
	if err != nil {
		return nil, fmt.Errorf("stage peer update manifest: %w", err)
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	n, writeErr := file.Write(encoded)
	if writeErr == nil && n != len(encoded) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return nil, fmt.Errorf("write staged peer update manifest: %w", err)
	}

	if err := removeIfExists(backup); err != nil {
		return nil, fmt.Errorf("remove previous peer update manifest: %w", err)
	}
	previousExists := false
	if _, err := os.Stat(path); err == nil {
		if err := os.Rename(path, backup); err != nil {
			return nil, fmt.Errorf("preserve current peer update manifest: %w", err)
		}
		previousExists = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect current peer update manifest: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		restoreErr := error(nil)
		if previousExists {
			restoreErr = os.Rename(backup, path)
		}
		return nil, errors.Join(fmt.Errorf("replace peer update manifest: %w", err), restoreErr)
	}
	return func() error {
		removeErr := removeIfExists(path)
		restoreErr := error(nil)
		if previousExists {
			restoreErr = os.Rename(backup, path)
		}
		return errors.Join(removeErr, restoreErr)
	}, nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func replacementStartArgs(job applyJob, cleanup bool) ([]string, bool) {
	if job.ControllerPID > 0 && job.ControllerServiceManaged {
		return nil, false
	}
	args := append([]string(nil), job.Args...)
	if job.ControllerPID > 0 {
		args = append([]string(nil), job.ControllerArgs...)
	}
	if cleanup {
		args = append(args, "-cleanup-peer-update", job.Staged)
	}
	return args, true
}

func ControllerHandoffRequested(target string, controllerPID, parentPID int) (bool, error) {
	if controllerPID <= 0 || parentPID <= 0 {
		return false, nil
	}
	path := controllerHandoffPath(target, controllerPID, parentPID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read controller update handoff: %w", err)
	}
	var request controllerHandoffRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return false, fmt.Errorf("decode controller update handoff: %w", err)
	}
	if request.ControllerPID != controllerPID || request.ParentPID != parentPID {
		return false, nil
	}
	return true, nil
}

func controllerHandoffPath(target string, controllerPID, parentPID int) string {
	name := fmt.Sprintf(".maistr0-handoff-%d-%d.json", controllerPID, parentPID)
	return filepath.Join(filepath.Dir(target), name)
}

func writeControllerHandoff(path string, request controllerHandoffRequest) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	data, marshalErr := json.Marshal(request)
	if marshalErr != nil {
		return errors.Join(marshalErr, file.Close())
	}
	data = append(data, '\n')
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

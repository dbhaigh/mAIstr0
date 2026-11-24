package execution

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
	"testing"
	"time"
)

var processHelperOutput *os.File

func TestExecutionProcessHelper(t *testing.T) {
	action, args, ok := executionHelperArgs()
	if !ok {
		return
	}
	processHelperOutput = os.Stdout
	os.Stdout = os.Stderr
	switch action {
	case "echo":
		_, _ = io.Copy(processHelperOutput, os.Stdin)
	case "args":
		_ = json.NewEncoder(processHelperOutput).Encode(map[string][]string{"args": args})
	case "malformed":
		_, _ = fmt.Fprint(processHelperOutput, "{malformed")
	case "array":
		_, _ = fmt.Fprint(processHelperOutput, `[]`)
	case "large":
		_, _ = processHelperOutput.Write([]byte(strings.Repeat("x", 4096)))
	case "large-stderr":
		_, _ = fmt.Fprint(os.Stderr, strings.Repeat("x", 4096))
		_, _ = fmt.Fprint(processHelperOutput, `{}`)
	case "lines":
		_, _ = fmt.Fprintln(processHelperOutput, `{"event":"one"}`)
		_, _ = fmt.Fprintln(processHelperOutput, `{"event":"two"}`)
	case "fast-lines":
		_, _ = fmt.Fprintln(processHelperOutput, `{"event":"one"}`)
		time.Sleep(10 * time.Millisecond)
		_, _ = fmt.Fprintln(processHelperOutput, `{"event":"two"}`)
	case "sensitive-stderr":
		_, _ = fmt.Fprintln(os.Stderr, args[0])
		_, _ = fmt.Fprint(processHelperOutput, "{malformed")
	case "exit":
		os.Exit(17)
	case "sleep":
		if err := os.WriteFile(args[0], []byte("started"), 0o600); err != nil {
			os.Exit(5)
		}
		time.Sleep(30 * time.Second)
		_, _ = fmt.Fprint(processHelperOutput, `{}`)
	case "spawn":
		child := exec.Command(
			os.Args[0], "-test.run=^TestExecutionProcessHelper$", "--",
			"--execution-helper", "late-write", args[0], args[1],
		)
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		_ = child.Wait()
	case "late-write":
		if err := os.WriteFile(args[1], []byte("started"), 0o600); err != nil {
			os.Exit(5)
		}
		time.Sleep(800 * time.Millisecond)
		if err := os.WriteFile(args[0], []byte("orphaned"), 0o600); err != nil {
			os.Exit(5)
		}
	case "workspace":
		workspace, _ := os.Getwd()
		_ = json.NewEncoder(processHelperOutput).Encode(map[string]string{"workspace": workspace})
	case "environment":
		_ = json.NewEncoder(processHelperOutput).Encode(map[string][]string{"environment": os.Environ()})
	case "barrier":
		var request struct {
			Started  string `json:"started"`
			Continue string `json:"continue"`
		}
		if json.NewDecoder(os.Stdin).Decode(&request) != nil {
			os.Exit(3)
		}
		if err := os.WriteFile(request.Started, []byte("started"), 0o600); err != nil {
			os.Exit(5)
		}
		for {
			_, err := os.Stat(request.Continue)
			if err == nil {
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				os.Exit(6)
			}
			time.Sleep(10 * time.Millisecond)
		}
		_, _ = fmt.Fprint(processHelperOutput, `{}`)
	default:
		os.Exit(4)
	}
}

func executionHelperArgs() (string, []string, bool) {
	for index, arg := range os.Args {
		if arg == "--execution-helper" && index+1 < len(os.Args) {
			return os.Args[index+1], os.Args[index+2:], true
		}
	}
	return "", nil, false
}

func testRunner(t *testing.T, action string, args ...string) *Runner {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	configured := DefaultConfig()
	configured.Enabled = true
	configured.ApprovedWorkspace = t.TempDir()
	configured.Executable = executable
	configured.Args = append([]string{"-test.run=^TestExecutionProcessHelper$", "--", "--execution-helper", action}, args...)
	runner, err := NewRunner(configured)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runner.Close(); err != nil {
			t.Errorf("close runner: %v", err)
		}
	})
	return runner
}

func TestRunnerIsDisabledByDefaultAndRejectsUnsupportedIsolation(t *testing.T) {
	runner, err := NewRunner(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled runner error = %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	configured := DefaultConfig()
	configured.Enabled = true
	if _, err := NewRunner(configured); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing approval/config error = %v", err)
	}
	configured.RequireIsolation = true
	if _, err := NewRunner(configured); !errors.Is(err, ErrIsolationUnsupported) {
		t.Fatalf("unsupported isolation error = %v", err)
	}
}

func TestRunnerUsesArgvStdinAndApprovedWorkspace(t *testing.T) {
	sideEffect := filepath.Join(t.TempDir(), "should-not-exist")
	argument := ";touch " + sideEffect
	runner := testRunner(t, "args", "two words", argument)
	response, err := runner.RunJSON(context.Background(), []byte(`{"prompt":"sent on stdin"}`))
	if err != nil {
		t.Fatal(err)
	}
	var argsResponse struct {
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(response, &argsResponse); err != nil {
		t.Fatal(err)
	}
	args := argsResponse.Args
	if len(args) != 2 || args[0] != "two words" || args[1] != argument {
		t.Fatalf("argument vector changed: %#v", args)
	}
	if _, err := os.Stat(sideEffect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("argument was interpreted by a shell: %v", err)
	}

	echoRunner := testRunner(t, "echo")
	request := []byte(`{"prompt":"sent only on stdin"}`)
	echoResponse, err := echoRunner.RunJSON(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if string(echoResponse) != string(request) {
		t.Fatalf("stdin response = %s, want %s", echoResponse, request)
	}

	workspaceRunner := testRunner(t, "workspace")
	workspaceResponse, err := workspaceRunner.RunJSON(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var workspaceResult struct {
		Workspace string `json:"workspace"`
	}
	if err := json.Unmarshal(workspaceResponse, &workspaceResult); err != nil {
		t.Fatal(err)
	}
	if workspaceResult.Workspace != workspaceRunner.workspace {
		t.Fatalf("working directory = %q, want approved directory %q", workspaceResult.Workspace, workspaceRunner.workspace)
	}
}

func TestRunnerStreamsBoundedLinesAndSelectedEnvironment(t *testing.T) {
	const secret = "unselected-parent-environment-value"
	t.Setenv("MAISTR0_PROCESS_SECRET_SENTINEL", secret)
	configured := DefaultConfig()
	configured.Enabled = true
	configured.ApprovedWorkspace = t.TempDir()
	configured.Executable, _ = os.Executable()
	configured.Args = []string{
		"-test.run=^TestExecutionProcessHelper$", "--", "--execution-helper", "lines",
	}
	configured.Environment = map[string]string{"MAISTR0_RUNNER_SELECTED": "selected-value"}
	runner, err := NewRunner(configured)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("close runner: %v", err)
		}
	}()
	var lines []string
	err = runner.RunLines(context.Background(), []string{"extra", "argument"}, []byte("prompt"), func(line []byte) error {
		lines = append(lines, string(line))
		return nil
	})
	if err != nil {
		t.Fatalf("stream lines: %v", err)
	}
	if strings.Join(lines, ",") != `{"event":"one"},{"event":"two"}` {
		t.Fatalf("streamed lines = %#v", lines)
	}

	environmentConfig := configured
	environmentConfig.Args = []string{
		"-test.run=^TestExecutionProcessHelper$", "--", "--execution-helper", "environment",
	}
	environmentRunner, err := NewRunner(environmentConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = environmentRunner.Close() }()
	var environmentLine []byte
	if err := environmentRunner.RunLines(context.Background(), nil, nil, func(line []byte) error {
		environmentLine = append([]byte(nil), line...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var environmentResponse struct {
		Environment []string `json:"environment"`
	}
	if err := json.Unmarshal(environmentLine, &environmentResponse); err != nil {
		t.Fatal(err)
	}
	environmentText := strings.Join(environmentResponse.Environment, "\n")
	if !strings.Contains(environmentText, "MAISTR0_RUNNER_SELECTED=selected-value") ||
		strings.Contains(environmentText, secret) {
		t.Fatalf("child environment did not preserve the explicit allowlist: %s", environmentText)
	}
	if err := environmentRunner.Close(); err != nil {
		t.Fatal(err)
	}
	if environmentRunner.config.Environment["MAISTR0_RUNNER_SELECTED"] != "" {
		t.Fatal("runner retained configured environment data after Close")
	}

	failedCallback := testRunner(t, "lines")
	if err := failedCallback.RunLines(context.Background(), nil, nil, func([]byte) error {
		return errors.New("sensitive callback detail")
	}); !errors.Is(err, ErrOutputCallback) || strings.Contains(err.Error(), "sensitive callback detail") {
		t.Fatalf("callback failure was not generic: %v", err)
	}

	large := testRunner(t, "large")
	large.config.MaxOutputBytes = 128
	if err := large.RunLines(context.Background(), nil, nil, func([]byte) error { return nil }); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("oversized streamed output error = %v", err)
	}
}

func TestRunnerDrainsLineOutputAfterFastProcessExit(t *testing.T) {
	runner := testRunner(t, "fast-lines")
	var lines []string
	err := runner.RunLines(context.Background(), nil, nil, func(line []byte) error {
		lines = append(lines, string(line))
		if len(lines) == 1 {
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(lines, ",") != `{"event":"one"},{"event":"two"}` {
		t.Fatalf("lines drained after fast process exit = %#v", lines)
	}
}

func TestRunnerDoesNotInheritEnvironmentCredentials(t *testing.T) {
	const secret = "process-environment-secret"
	t.Setenv("MAISTR0_PROCESS_SECRET_SENTINEL", secret)
	runner := testRunner(t, "environment")
	response, err := runner.RunJSON(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var environmentResponse struct {
		Environment []string `json:"environment"`
	}
	if err := json.Unmarshal(response, &environmentResponse); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(environmentResponse.Environment, "\n"), secret) {
		t.Fatalf("runner inherited a credential-like environment value: %s", response)
	}
	for _, entry := range environmentResponse.Environment {
		if !strings.HasPrefix(strings.ToLower(entry), "systemroot=") {
			t.Fatalf("runner inherited unexpected environment entry %q", entry)
		}
	}
}

func TestRunnerBoundsInputOutputAndRedactsProcessFailures(t *testing.T) {
	runner := testRunner(t, "malformed")
	if _, err := runner.RunJSON(context.Background(), []byte(`not json`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid input error = %v", err)
	}
	if _, err := runner.RunJSON(context.Background(), []byte(`[]`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("non-object input error = %v", err)
	}
	if _, err := runner.RunJSON(context.Background(), []byte(`{"payload":"`+strings.Repeat("x", defaultMaxInputBytes)+`"}`)); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("oversized input error = %v", err)
	}
	if _, err := runner.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("malformed response error = %v", err)
	}
	array := testRunner(t, "array")
	if _, err := array.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("non-object response error = %v", err)
	}

	large := testRunner(t, "large")
	large.config.MaxOutputBytes = 128
	if _, err := large.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("oversized output error = %v", err)
	}
	largeStderr := testRunner(t, "large-stderr")
	largeStderr.config.MaxOutputBytes = 128
	if _, err := largeStderr.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("oversized stderr error = %v", err)
	}

	const secret = "sensitive-process-output"
	redacted := testRunner(t, "sensitive-stderr", secret)
	_, err := redacted.RunJSON(context.Background(), []byte(`{"prompt":"sensitive-request"}`))
	if !errors.Is(err, ErrMalformedResponse) || strings.Contains(err.Error(), secret) ||
		strings.Contains(err.Error(), "sensitive-request") {
		t.Fatalf("process failure exposed sensitive data: %v", err)
	}

	failed := testRunner(t, "exit")
	if _, err := failed.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrProcessFailed) {
		t.Fatalf("non-zero process exit error = %v", err)
	}
}

func TestRunnerFailsClosedForMissingExecutable(t *testing.T) {
	configured := DefaultConfig()
	configured.Enabled = true
	configured.ApprovedWorkspace = t.TempDir()
	configured.Executable = filepath.Join(configured.ApprovedWorkspace, "missing-binary")
	runner, err := NewRunner(configured)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runner.Close() }()
	if _, err := runner.RunJSON(context.Background(), []byte(`{}`)); !errors.Is(err, ErrProcessStart) {
		t.Fatalf("missing executable error = %v", err)
	}
}

func TestRunnerCancellationTerminatesDescendants(t *testing.T) {
	lateMarker := filepath.Join(t.TempDir(), "late-child-output")
	childStarted := filepath.Join(t.TempDir(), "child-started")
	runner := testRunner(t, "spawn", lateMarker, childStarted)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		request := []byte(`{"padding":"` + strings.Repeat("x", 256<<10) + `"}`)
		_, err := runner.RunJSON(ctx, request)
		done <- err
	}()
	waitForFile(t, childStarted)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled run error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled process did not exit")
	}
	time.Sleep(time.Second)
	if _, err := os.Stat(lateMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant survived cancellation: %v", err)
	}
}

func TestRunnerEnforcesRuntimeLimit(t *testing.T) {
	started := filepath.Join(t.TempDir(), "process-started")
	runner := testRunner(t, "sleep", started)
	runner.config.MaxRuntime = 100 * time.Millisecond
	_, err := runner.RunJSON(context.Background(), []byte(`{}`))
	if !errors.Is(err, ErrRuntimeLimit) {
		t.Fatalf("runtime limit error = %v", err)
	}
	if _, err := os.Stat(started); err != nil {
		t.Fatalf("process did not start before the runtime limit: %v", err)
	}
}

func TestRunnerCloseCancelsAndJoinsActiveProcess(t *testing.T) {
	lateMarker := filepath.Join(t.TempDir(), "late-child-output")
	childStarted := filepath.Join(t.TempDir(), "child-started")
	runner := testRunner(t, "spawn", lateMarker, childStarted)
	done := make(chan error, 1)
	go func() {
		_, err := runner.RunJSON(context.Background(), []byte(`{}`))
		done <- err
	}()
	waitForFile(t, childStarted)

	closed := make(chan error, 1)
	go func() { closed <- runner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close runner: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner Close did not join the active process")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("active run after close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active RunJSON outlived runner Close")
	}
	time.Sleep(time.Second)
	if _, err := os.Stat(lateMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant survived runner shutdown: %v", err)
	}
}

func TestRunnerBoundsConcurrentProcesses(t *testing.T) {
	runner := testRunner(t, "barrier")
	firstStarted := filepath.Join(t.TempDir(), "first-started")
	firstContinue := filepath.Join(t.TempDir(), "first-continue")
	secondStarted := filepath.Join(t.TempDir(), "second-started")
	secondContinue := filepath.Join(t.TempDir(), "second-continue")

	run := func(started, proceed string) <-chan error {
		result := make(chan error, 1)
		request, _ := json.Marshal(map[string]string{"started": started, "continue": proceed})
		go func() {
			_, err := runner.RunJSON(context.Background(), request)
			result <- err
		}()
		return result
	}
	first := run(firstStarted, firstContinue)
	waitForFile(t, firstStarted)
	second := run(secondStarted, secondContinue)
	if err := <-second; !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent process was not rejected by the limit: %v", err)
	}
	if _, err := os.Stat(secondStarted); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("concurrency limit did not hold: %v", err)
	}
	if err := os.WriteFile(firstContinue, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatalf("first process: %v", err)
	}
	second = run(secondStarted, secondContinue)
	waitForFile(t, secondStarted)
	if err := os.WriteFile(secondContinue, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second process: %v", err)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := os.Stat(path)
		if err == nil {
			return
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect %s: %v", filepath.Base(path), err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", filepath.Base(path))
}

package codexcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const fakeThreadID = "11111111-1111-4111-8111-111111111111"

var codexHelperOutput *os.File

func TestCodexProcessHelper(t *testing.T) {
	helperIndex := -1
	for index, arg := range os.Args {
		if arg == "--codex-test-helper" {
			helperIndex = index
			break
		}
	}
	if helperIndex < 0 {
		return
	}
	codexHelperOutput = os.Stdout
	os.Stdout = os.Stderr
	args := os.Args[helperIndex+1:]
	if hasArg(args, "--version") {
		version := os.Getenv("CODEX_TEST_VERSION")
		if version == "" {
			version = "codex-cli 0.171.0"
		}
		_, _ = fmt.Fprintln(codexHelperOutput, version)
		os.Exit(0)
	}
	if hasArg(args, "--help") && hasArg(args, "exec") {
		help := "--json --ask-for-approval --ignore-user-config --strict-config --config"
		if os.Getenv("CODEX_TEST_NO_SANDBOX") != "1" {
			help += " --sandbox"
		}
		_, _ = fmt.Fprintln(codexHelperOutput, help)
		os.Exit(0)
	}
	if !hasArg(args, "exec") || !hasArg(args, "--json") ||
		!hasPair(args, "--sandbox", "read-only") ||
		!hasPair(args, "--ask-for-approval", "never") ||
		!hasArg(args, "--ignore-user-config") || !hasArg(args, "--strict-config") ||
		!hasPair(args, "--config", `approval_policy="never"`) ||
		!hasPair(args, "--config", `sandbox_mode="read-only"`) ||
		!hasPair(args, "--config", "mcp_servers={}") ||
		hasArg(args, "--dangerously-bypass-approvals-and-sandbox") ||
		args[len(args)-1] != "-" {
		os.Exit(21)
	}
	prompt, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(22)
	}
	resumeIndex := indexOf(args, "resume")
	switch string(prompt) {
	case "first turn":
		if resumeIndex >= 0 {
			os.Exit(23)
		}
		writeCodexContractSuccess()
	case "resumed turn":
		if resumeIndex < 0 || resumeIndex+1 >= len(args) || args[resumeIndex+1] != fakeThreadID {
			os.Exit(24)
		}
		writeCodexLine(`{"type":"thread.started","thread_id":"` + fakeThreadID + `"}`)
		writeCodexLine(`{"type":"item.completed","item":{"id":"final","type":"agent_message","text":"Resumed"}}`)
		writeCodexLine(`{"type":"turn.completed"}`)
	case "FAIL":
		writeCodexLine(`{"type":"thread.started","thread_id":"` + fakeThreadID + `"}`)
		writeCodexLine(`{"type":"turn.failed","error":{"message":"private-turn-error"}}`)
	case "EXIT":
		_, _ = fmt.Fprintln(os.Stderr, "private-child-error")
		os.Exit(25)
	case "MALFORMED":
		_, _ = fmt.Fprintln(codexHelperOutput, "private-malformed-output")
		os.Exit(0)
	case "NO_TERMINAL":
		writeCodexLine(`{"type":"thread.started","thread_id":"` + fakeThreadID + `"}`)
		writeCodexLine(`{"type":"item.completed","item":{"id":"final","type":"agent_message","text":"unfinished"}}`)
		writeCodexLine(`{"type":"private-last-event-output"}`)
		os.Exit(0)
	case "BLOCK":
		writeCodexLine(`{"type":"thread.started","thread_id":"` + fakeThreadID + `"}`)
		writeCodexLine(`{"type":"item.updated","item":{"id":"live","type":"agent_message","text":"working"}}`)
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		writeCodexContractSuccessText("OK")
	}
	os.Exit(0)
}

func writeCodexContractSuccess() {
	writeCodexContractSuccessText("Hello")
}

func writeCodexContractSuccessText(text string) {
	line := `{"type":"thread.started","thread_id":"` + fakeThreadID + `"}` + "\n"
	writeCodexLineFragmented(line)
	prefix := text
	if len(prefix) > 3 {
		prefix = prefix[:3]
	}
	writeCodexLine(`{"type":"item.updated","item":{"id":"final","type":"agent_message","text":"` + prefix + `"}}`)
	writeCodexLine(`{"type":"item.updated","item":{"id":"final","type":"agent_message","text":"` + text + `"}}`)
	writeCodexLine(`{"type":"item.completed","item":{"id":"final","type":"agent_message","text":"` + text + `"}}`)
	writeCodexLine(`{"type":"turn.completed"}`)
}

func writeCodexLine(line string) {
	_, _ = fmt.Fprintln(codexHelperOutput, line)
}

func writeCodexLineFragmented(line string) {
	for start := 0; start < len(line); start += 7 {
		end := start + 7
		if end > len(line) {
			end = len(line)
		}
		_, _ = io.WriteString(codexHelperOutput, line[start:end])
		time.Sleep(time.Millisecond)
	}
}

func hasArg(args []string, name string) bool { return indexOf(args, name) >= 0 }

func hasPair(args []string, name, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name && args[index+1] == value {
			return true
		}
	}
	return false
}

func indexOf(args []string, value string) int {
	for index, arg := range args {
		if arg == value {
			return index
		}
	}
	return -1
}

func newTestBackend(t *testing.T, workspace, codexHome string, extraEnvironment map[string]string) *Backend {
	t.Helper()
	mappingDir := filepath.Join(codexHome, mappingDirectoryName)
	if err := os.Mkdir(mappingDir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable = executable
	runnerConfig.Args = []string{"-test.run=^TestCodexProcessHelper$", "--", "--codex-test-helper"}
	runnerConfig.Environment = map[string]string{
		"CODEX_HOME":            codexHome,
		"CODEX_TEST_VERSION":    "codex-cli 0.171.0",
		"CODEX_TEST_NO_SANDBOX": "",
	}
	for name, value := range extraEnvironment {
		runnerConfig.Environment[name] = value
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := newBackendWithRunner(runner, workspace, mappingDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Codex backend: %v", err)
		}
	})
	return backend
}

func TestSupportedCodexCLIVersionRange(t *testing.T) {
	for _, version := range []string{"0.171.0", "codex-cli 0.171.0", "v0.171.99"} {
		if err := ValidateVersion(version); err != nil {
			t.Errorf("ValidateVersion(%q): %v", version, err)
		}
	}
	for _, version := range []string{"0.170.99", "0.172.0", "1.0.0", "unknown"} {
		if err := ValidateVersion(version); err == nil {
			t.Errorf("ValidateVersion(%q) unexpectedly succeeded", version)
		}
	}
}

func TestCodexVersionOutputIsNormalized(t *testing.T) {
	backend := newTestBackend(t, t.TempDir(), t.TempDir(), map[string]string{
		"CODEX_TEST_VERSION": "codex-cli 0.171.0 private-version-output",
	})
	if backend.version != "0.171.0" {
		t.Fatalf("backend version = %q, want normalized version only", backend.version)
	}
}

func TestCodexStartupFailsClosedWithoutSandboxOption(t *testing.T) {
	workspace := t.TempDir()
	codexHome := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable = executable
	runnerConfig.Args = []string{"-test.run=^TestCodexProcessHelper$", "--", "--codex-test-helper"}
	runnerConfig.Environment = map[string]string{
		"CODEX_HOME": codexHome, "CODEX_TEST_VERSION": "codex-cli 0.171.0", "CODEX_TEST_NO_SANDBOX": "1",
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newBackendWithRunner(runner, workspace, filepath.Join(codexHome, mappingDirectoryName)); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("backend without approval support error = %v", err)
	}
}

func TestCodexSessionStreamsFragmentedJSONLAndResumesNativeThread(t *testing.T) {
	workspace := t.TempDir()
	codexHome := t.TempDir()
	backend := newTestBackend(t, workspace, codexHome, nil)
	session, err := backend.CreateSession("contract", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan agent.StreamEvent, 8)
	final, err := backend.SendMessage(context.Background(), session.ID, "first turn", events)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "Hello" || final.Role != agent.RoleAssistant {
		t.Fatalf("final message = %+v", final)
	}
	var eventTypes []agent.StreamEventType
	for len(events) > 0 {
		eventTypes = append(eventTypes, (<-events).Type)
	}
	if len(eventTypes) != 4 || eventTypes[0] != agent.EventTextDelta ||
		eventTypes[1] != agent.EventTextDelta || eventTypes[2] != agent.EventFinalMessage ||
		eventTypes[3] != agent.EventDone {
		t.Fatalf("stream events = %#v", eventTypes)
	}
	saved, exists := backend.GetSession(session.ID)
	if !exists || saved.ID != session.ID || len(saved.Messages) != 2 {
		t.Fatalf("saved session = %+v, exists=%t", saved, exists)
	}
	if _, err := os.Stat(backend.mappingPath(session.ID)); err != nil {
		t.Fatalf("native Codex thread mapping was not persisted: %v", err)
	}

	restored := newTestBackend(t, workspace, codexHome, nil)
	if err := restored.RestoreSession(saved); err != nil {
		t.Fatal(err)
	}
	final, err = restored.SendMessage(context.Background(), session.ID, "resumed turn", nil)
	if err != nil {
		t.Fatalf("resume Codex thread: %v", err)
	}
	if final.Content != "Resumed" {
		t.Fatalf("resumed message = %+v", final)
	}
}

func TestCodexMapsTerminalAndEarlyProcessFailuresWithoutOutput(t *testing.T) {
	backend := newTestBackend(t, t.TempDir(), t.TempDir(), nil)
	for _, test := range []struct {
		prompt string
		want   error
		leak   string
	}{
		{prompt: "FAIL", want: ErrTurnFailed, leak: "private-turn-error"},
		{prompt: "EXIT", want: ErrProcessFailed, leak: "private-child-error"},
		{prompt: "MALFORMED", want: ErrProtocol, leak: "private-malformed-output"},
		{prompt: "NO_TERMINAL", want: ErrProtocol, leak: "private-last-event-output"},
	} {
		session, err := backend.CreateSession("failure", agent.SessionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.SendMessage(context.Background(), session.ID, test.prompt, nil)
		if !errors.Is(err, test.want) || test.leak != "" && strings.Contains(fmt.Sprint(err), test.leak) {
			t.Errorf("prompt %q error = %v, want %v without child output", test.prompt, err, test.want)
		}
	}
}

func TestCodexCancellationPersistsThreadForResume(t *testing.T) {
	workspace := t.TempDir()
	codexHome := t.TempDir()
	backend := newTestBackend(t, workspace, codexHome, nil)
	session, err := backend.CreateSession("cancel", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan agent.StreamEvent, 4)
	done := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "BLOCK", events)
		done <- err
	}()
	select {
	case event := <-events:
		if event.Type != agent.EventTextDelta || event.Content != "working" {
			t.Fatalf("first event = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake Codex process did not start")
	}
	if !backend.InterruptSession(session.ID) {
		t.Fatal("active Codex session was not cancellable")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled turn error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Codex process did not stop")
	}
	select {
	case event := <-events:
		if event.Type != agent.EventInterrupted || event.SessionID != session.ID {
			t.Fatalf("interruption event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not report the interrupted turn")
	}
	saved, exists := backend.GetSession(session.ID)
	if !exists || !strings.Contains(saved.Messages[len(saved.Messages)-1].Content, "interrupted") {
		t.Fatalf("interrupted session was not recorded: %+v", saved)
	}
	restored := newTestBackend(t, workspace, codexHome, nil)
	if err := restored.RestoreSession(saved); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.SendMessage(context.Background(), session.ID, "resumed turn", nil); err != nil {
		t.Fatalf("resume interrupted Codex turn: %v", err)
	}
}

func TestCodexRejectsUnsupportedSessionOptionsAndTools(t *testing.T) {
	backend := newTestBackend(t, t.TempDir(), t.TempDir(), nil)
	for _, config := range []agent.SessionConfig{
		{CoordinatorModel: "model"}, {MaxSteps: 1}, {Temperature: 0.1}, {SystemPrompt: "must not be discarded"},
	} {
		if _, err := backend.CreateSession("unsupported", config); !errors.Is(err, ErrUnsupportedConfiguration) {
			t.Errorf("CreateSession(%+v) error = %v", config, err)
		}
	}
	if len(backend.ListTools()) != 0 || len(backend.ListClusterModels()) != 0 {
		t.Fatal("unsupported tools or cluster models were advertised")
	}
	if _, err := backend.ExecuteTool(context.Background(), "", "tool", nil); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("ExecuteTool error = %v", err)
	}
}

func TestCodexEnvironmentOptInIsExplicit(t *testing.T) {
	unsetCodexEnvironment(t, "MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME")
	backend, enabled, err := NewFromEnvironment()
	if backend != nil || enabled || err != nil {
		t.Fatalf("disabled Codex opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
	t.Setenv("MAISTR0_CODEX_EXECUTABLE", "configured")
	t.Setenv("MAISTR0_CODEX_WORKSPACE", "")
	t.Setenv("MAISTR0_CODEX_HOME", "")
	backend, enabled, err = NewFromEnvironment()
	if backend != nil || !enabled || err == nil ||
		!strings.Contains(err.Error(), "requires MAISTR0_CODEX_EXECUTABLE") {
		t.Fatalf("partial Codex opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
}

func unsetCodexEnvironment(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		value, exists := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		envName, oldValue, wasSet := name, value, exists
		t.Cleanup(func() {
			if wasSet {
				_ = os.Setenv(envName, oldValue)
			} else {
				_ = os.Unsetenv(envName)
			}
		})
	}
}

func TestRealCodexSmoke(t *testing.T) {
	if os.Getenv("MAISTR0_CODEX_REAL_SMOKE") != "1" {
		t.Skip("set MAISTR0_CODEX_REAL_SMOKE=1 to run against an installed Codex CLI")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || backend == nil {
		t.Fatal("real smoke requires the Codex executable, workspace, and CODEX_HOME settings")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Codex backend: %v", err)
		}
	}()
	t.Logf("Codex CLI version used by smoke test: %s", backend.version)
	session, err := backend.CreateSession("real smoke", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	message, err := backend.SendMessage(context.Background(), session.ID, "Reply with exactly READY.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message.Content, "READY") {
		t.Fatal("real smoke response did not contain the expected marker")
	}
}

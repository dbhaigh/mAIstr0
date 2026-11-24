package geminicli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const fakeGeminiAPIKey = "test-gemini-api-key-do-not-log"

var geminiHelperOutput *os.File

func TestGeminiProcessHelper(t *testing.T) {
	if !hasArg(os.Args, "--gemini-test-helper") || os.Getenv("GEMINI_CLI_SYSTEM_SETTINGS_PATH") == "" {
		return
	}
	geminiHelperOutput = os.Stdout
	os.Stdout = os.Stderr
	args := helperArgs(os.Args)
	if hasArg(args, "--version") {
		_, _ = fmt.Fprintln(geminiHelperOutput, os.Getenv("GEMINI_TEST_VERSION"))
		os.Exit(0)
	}
	if hasArg(args, "--help") {
		help := "--output-format --approval-mode --sandbox --session-id --resume --prompt"
		if os.Getenv("GEMINI_TEST_MISSING_POLICY") == "1" {
			help = "--output-format --prompt"
		}
		_, _ = fmt.Fprintln(geminiHelperOutput, help)
		os.Exit(0)
	}
	if !hasArg(args, "--output-format") || !hasPair(args, "--output-format", "stream-json") ||
		!hasArg(args, "--sandbox") || !hasArg(args, "--approval-mode=plan") ||
		!hasPair(args, "--prompt", " ") || os.Getenv("GEMINI_SANDBOX") == "" {
		os.Exit(21)
	}
	if strings.Contains(strings.Join(os.Args, " "), fakeGeminiAPIKey) {
		os.Exit(22)
	}
	if !checkGeminiPolicy(os.Getenv("GEMINI_CLI_SYSTEM_SETTINGS_PATH")) {
		os.Exit(23)
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(24)
	}
	sessionID := optionValue(args, "--session-id")
	resumeID := optionValue(args, "--resume")
	switch string(input) {
	case "first turn":
		if sessionID == "" || resumeID != "" {
			os.Exit(25)
		}
		writeGeminiSuccess(sessionID)
	case "resumed turn":
		if resumeID == "" || sessionID != "" || !validSessionID(resumeID) {
			os.Exit(26)
		}
		writeGeminiEvent(`{"type":"init","session_id":"` + resumeID + `","model":"fake-model"}`)
		writeGeminiEvent(`{"type":"message","role":"assistant","content":"Resumed","delta":true}`)
		writeGeminiEvent(`{"type":"result","status":"success"}`)
	case "MALFORMED":
		writeGeminiFragment(`{"type":"message","content":"private-malformed-output"` + "\n")
	case "NO_TERMINAL":
		writeGeminiEvent(`{"type":"init","session_id":"` + sessionID + `","model":"fake-model"}`)
		writeGeminiEvent(`{"type":"message","role":"assistant","content":"partial","delta":true}`)
	case "TURN_ERROR":
		writeGeminiEvent(`{"type":"init","session_id":"` + sessionID + `","model":"fake-model"}`)
		writeGeminiEvent(`{"type":"error","severity":"error","message":"private ` + fakeGeminiAPIKey + `"}`)
		writeGeminiEvent(`{"type":"result","status":"error","error":{"type":"private","message":"` + fakeGeminiAPIKey + `"}}`)
	case "RESULT_ERROR":
		writeGeminiEvent(`{"type":"result","status":"error","error":{"type":"private","message":"` + fakeGeminiAPIKey + `"}}`)
	case "EXIT":
		_, _ = fmt.Fprintln(os.Stderr, "private child output "+fakeGeminiAPIKey)
		os.Exit(27)
	case "BLOCK":
		writeGeminiEvent(`{"type":"init","session_id":"` + sessionID + `","model":"fake-model"}`)
		writeGeminiEvent(`{"type":"message","role":"assistant","content":"partial","delta":true}`)
		time.Sleep(30 * time.Second)
	default:
		os.Exit(28)
	}
	os.Exit(0)
}

func writeGeminiSuccess(sessionID string) {
	writeGeminiFragment(`{"type":"init","session_id":"` + sessionID + `","model":"fake-model"}` + "\n")
	writeGeminiEvent(`{"type":"message","role":"assistant","content":"Hel","delta":true}`)
	writeGeminiEvent(`{"type":"message","role":"assistant","content":"lo","delta":true}`)
	writeGeminiEvent(`{"type":"message","role":"assistant","content":"Hello","delta":false}`)
	writeGeminiEvent(`{"type":"tool_use","tool_name":"read_file","tool_id":"tool-1","parameters":{"path":"README.md"}}`)
	writeGeminiEvent(`{"type":"tool_result","tool_id":"tool-1","status":"success","output":"file contents"}`)
	writeGeminiEvent(`{"type":"message","role":"assistant","content":"!","delta":true}`)
	writeGeminiEvent(`{"type":"message","role":"assistant","content":"!","delta":false}`)
	writeGeminiEvent(`{"type":"result","status":"success"}`)
}

func writeGeminiEvent(line string) {
	_, _ = fmt.Fprintln(geminiHelperOutput, line)
}

func writeGeminiFragment(line string) {
	for start := 0; start < len(line); start += 5 {
		end := start + 5
		if end > len(line) {
			end = len(line)
		}
		_, _ = io.WriteString(geminiHelperOutput, line[start:end])
		time.Sleep(time.Millisecond)
	}
}

func helperArgs(args []string) []string {
	for index, arg := range args {
		if arg == "--gemini-test-helper" {
			return args[index+1:]
		}
	}
	return nil
}

func hasArg(args []string, name string) bool {
	for _, arg := range args {
		if arg == name {
			return true
		}
	}
	return false
}

func hasPair(args []string, name, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name && args[index+1] == value {
			return true
		}
	}
	return false
}

func optionValue(args []string, name string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name {
			return args[index+1]
		}
	}
	return ""
}

func checkGeminiPolicy(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var policy struct {
		Admin struct {
			Extensions struct {
				Enabled bool `json:"enabled"`
			} `json:"extensions"`
			MCP struct {
				Enabled bool `json:"enabled"`
			} `json:"mcp"`
		} `json:"admin"`
		HooksConfig struct {
			Enabled bool `json:"enabled"`
		} `json:"hooksConfig"`
		Skills struct {
			Enabled bool `json:"enabled"`
		} `json:"skills"`
		Tools struct {
			Sandbox              string   `json:"sandbox"`
			SandboxAllowedPaths  []string `json:"sandboxAllowedPaths"`
			SandboxNetworkAccess bool     `json:"sandboxNetworkAccess"`
		} `json:"tools"`
		General struct {
			DefaultApprovalMode string `json:"defaultApprovalMode"`
		} `json:"general"`
	}
	if json.Unmarshal(data, &policy) != nil {
		return false
	}
	return !policy.Admin.Extensions.Enabled && !policy.Admin.MCP.Enabled &&
		!policy.HooksConfig.Enabled && !policy.Skills.Enabled &&
		policy.Tools.Sandbox != "" && len(policy.Tools.SandboxAllowedPaths) == 0 &&
		policy.Tools.SandboxNetworkAccess && policy.General.DefaultApprovalMode == "plan"
}

func newTestBackend(t *testing.T, workspace, home, mode string) *Backend {
	t.Helper()
	mappingDir, err := ensurePrivateDirectory(filepath.Join(home, mappingDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	cliHome, err := ensurePrivateDirectory(filepath.Join(home, cliHomeDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	sandbox := testSandbox()
	policyPath, err := writeSystemPolicy(cliHome, sandbox)
	if err != nil {
		t.Fatal(err)
	}
	homeVariable := "HOME"
	if runtime.GOOS == "windows" {
		homeVariable = "USERPROFILE"
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig.Args = []string{"-test.run=TestGeminiProcessHelper", "--", "--gemini-test-helper"}
	runnerConfig.MaxRuntime = 10 * time.Minute
	runnerConfig.MaxOutputBytes = 8 << 20
	runnerConfig.MaxInputBytes = 8 << 20
	runnerConfig.Environment = map[string]string{
		"GEMINI_CLI_SYSTEM_SETTINGS_PATH": policyPath,
		"GEMINI_SANDBOX":                  sandbox,
		"GEMINI_API_KEY":                  fakeGeminiAPIKey,
		"GEMINI_TEST_VERSION":             "Gemini CLI 0.70.0",
		"GEMINI_TEST_MODE":                mode,
		"PATH":                            os.Getenv("PATH"),
		homeVariable:                      cliHome,
	}
	if mode == "missing-policy" {
		runnerConfig.Environment["GEMINI_TEST_MISSING_POLICY"] = "1"
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		_ = os.Remove(policyPath)
		t.Fatal(err)
	}
	backend, err := newBackendWithRunner(runner, workspace, mappingDir, policyPath)
	if err != nil {
		_ = runner.Close()
		_ = os.Remove(policyPath)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Gemini backend: %v", err)
		}
	})
	return backend
}

func testSandbox() string {
	switch runtime.GOOS {
	case "windows":
		return "windows-native"
	case "darwin":
		return "sandbox-exec"
	default:
		return "docker"
	}
}

func TestGeminiSessionStreamAndRestoreResume(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	backend := newTestBackend(t, workspace, home, "success")
	session, err := backend.CreateSession("Gemini", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan agent.StreamEvent, 16)
	final, err := backend.SendMessage(context.Background(), session.ID, "first turn", events)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "Hello!" || final.Model != "fake-model" || len(final.ToolCalls) != 1 {
		t.Fatalf("final response = %+v", final)
	}
	if got := []agent.StreamEventType{(<-events).Type, (<-events).Type, (<-events).Type, (<-events).Type, (<-events).Type, (<-events).Type}; len(got) != 6 ||
		got[0] != agent.EventTextDelta || got[1] != agent.EventTextDelta || got[2] != agent.EventToolCall ||
		got[3] != agent.EventToolResponse || got[4] != agent.EventTextDelta || got[5] != agent.EventFinalMessage {
		t.Fatalf("stream event order = %v", got)
	}
	if done := <-events; done.Type != agent.EventDone {
		t.Fatalf("terminal event = %+v", done)
	}
	snapshot, ok := backend.GetSession(session.ID)
	if !ok || len(snapshot.Messages) != 3 ||
		snapshot.Messages[1].Role != agent.RoleAssistant ||
		snapshot.Messages[2].Role != agent.RoleTool ||
		snapshot.Messages[1].Content != "Hello!" ||
		len(snapshot.Messages[1].ToolCalls) != 1 ||
		snapshot.Messages[2].ToolResponse == nil ||
		snapshot.Messages[2].ToolResponse.Output != "file contents" {
		t.Fatalf("stored session = %+v", snapshot)
	}

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	resumed := newTestBackend(t, workspace, home, "resume")
	if err := resumed.RestoreSession(snapshot); err != nil {
		t.Fatal(err)
	}
	final, err = resumed.SendMessage(context.Background(), session.ID, "resumed turn", nil)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "Resumed" {
		t.Fatalf("resumed response = %+v", final)
	}
}

func TestGeminiTurnFailuresAreGenericAndTerminalResultIsRequired(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	tests := []struct {
		prompt string
		want   error
	}{
		{prompt: "MALFORMED", want: ErrProtocol},
		{prompt: "NO_TERMINAL", want: ErrProtocol},
		{prompt: "TURN_ERROR", want: ErrTurnFailed},
		{prompt: "RESULT_ERROR", want: ErrTurnFailed},
		{prompt: "EXIT", want: ErrProcessFailed},
	}
	backend := newTestBackend(t, workspace, home, "failures")
	for _, test := range tests {
		session, err := backend.CreateSession("failure", agent.SessionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		events := make(chan agent.StreamEvent, 8)
		_, err = backend.SendMessage(context.Background(), session.ID, test.prompt, events)
		if !errors.Is(err, test.want) {
			t.Errorf("prompt %q error = %v, want %v", test.prompt, err, test.want)
		}
		if strings.Contains(fmt.Sprint(err), fakeGeminiAPIKey) ||
			strings.Contains(fmt.Sprint(err), "private-malformed-output") ||
			strings.Contains(fmt.Sprint(err), "private child output") {
			t.Errorf("prompt %q exposed private process output: %v", test.prompt, err)
		}
		errorEventSeen := false
		for len(events) > 0 {
			event := <-events
			errorEventSeen = errorEventSeen || event.Type == agent.EventError
			if strings.Contains(event.Error, fakeGeminiAPIKey) || strings.Contains(event.Content, fakeGeminiAPIKey) {
				t.Errorf("prompt %q exposed private event output: %+v", test.prompt, event)
			}
		}
		if (test.prompt == "TURN_ERROR" || test.prompt == "RESULT_ERROR") && !errorEventSeen {
			t.Errorf("prompt %q did not map the failure to a generic error event", test.prompt)
		}
	}
}

func TestGeminiCancellationTerminatesTurn(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	backend := newTestBackend(t, workspace, home, "cancel")
	session, err := backend.CreateSession("cancel", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan agent.StreamEvent, 8)
	done := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(ctx, session.ID, "BLOCK", events)
		done <- err
	}()
	select {
	case event := <-events:
		if event.Type != agent.EventTextDelta || event.Content != "partial" {
			t.Fatalf("initial cancellation event = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Gemini CLI did not emit the initial event")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Gemini CLI process did not stop after cancellation")
	}
}

func TestGeminiPolicyAndAuthenticationValidation(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	backend := newTestBackend(t, workspace, home, "policy")
	data, err := os.ReadFile(backend.policyPath)
	if err != nil {
		t.Fatal(err)
	}
	if !checkGeminiPolicy(backend.policyPath) {
		t.Fatalf("generated system settings do not enforce the required policy: %s", data)
	}
	if _, err := authenticationEnvironment(Config{AuthMode: "api-key", APIKey: fakeGeminiAPIKey}); err != nil {
		t.Fatalf("AI Studio API-key auth rejected: %v", err)
	}
	if _, err := authenticationEnvironment(Config{
		AuthMode: "vertex-api-key", APIKey: fakeGeminiAPIKey,
		VertexProject: "project", VertexLocation: "location",
	}); err != nil {
		t.Fatalf("Vertex API-key auth rejected: %v", err)
	}
	credentials := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(credentials, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticationEnvironment(Config{
		AuthMode: "vertex-service-account", GoogleApplicationCredentials: credentials,
		VertexProject: "project", VertexLocation: "location",
	}); err != nil {
		t.Fatalf("Vertex service-account auth rejected: %v", err)
	}
	if _, err := validateSandbox("not-a-sandbox"); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("invalid sandbox error = %v", err)
	}
}

func TestGeminiVersionAndEnvironmentOptIn(t *testing.T) {
	for _, output := range []string{"0.70.0", "Gemini CLI v0.70.4"} {
		if err := ValidateVersion(output); err != nil {
			t.Errorf("supported version %q rejected: %v", output, err)
		}
	}
	for _, output := range []string{"0.69.9", "0.70.-1", "0.71.0", "unknown"} {
		if err := ValidateVersion(output); err == nil {
			t.Errorf("unsupported version %q accepted", output)
		}
	}
	names := []string{
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
	}
	unsetGeminiOptIn(t, names...)
	backend, enabled, err := NewFromEnvironment()
	if backend != nil || enabled || err != nil {
		t.Fatalf("disabled Gemini opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
	t.Setenv("MAISTR0_GEMINI_EXECUTABLE", "configured")
	backend, enabled, err = NewFromEnvironment()
	if backend != nil || !enabled || err == nil ||
		!strings.Contains(err.Error(), "MAISTR0_GEMINI_WORKSPACE") {
		t.Fatalf("partial Gemini opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
}

func TestGeminiMissingPolicyFlagFailsClosed(t *testing.T) {
	workspace := t.TempDir()
	home := t.TempDir()
	mappingDir, err := ensurePrivateDirectory(filepath.Join(home, mappingDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	cliHome, err := ensurePrivateDirectory(filepath.Join(home, cliHomeDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	policyPath, err := writeSystemPolicy(cliHome, testSandbox())
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig.Args = []string{"-test.run=TestGeminiProcessHelper", "--", "--gemini-test-helper"}
	runnerConfig.Environment = map[string]string{
		"GEMINI_CLI_SYSTEM_SETTINGS_PATH": policyPath,
		"GEMINI_SANDBOX":                  testSandbox(),
		"GEMINI_TEST_VERSION":             "0.70.0",
		"GEMINI_TEST_MISSING_POLICY":      "1",
		"PATH":                            os.Getenv("PATH"),
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newBackendWithRunner(runner, workspace, mappingDir, policyPath)
	if !errors.Is(err, ErrPolicyUnavailable) {
		_ = runner.Close()
		_ = os.Remove(policyPath)
		t.Fatalf("missing policy flag error = %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(policyPath); err != nil {
		t.Fatal(err)
	}
}

func TestRealGeminiCLISmoke(t *testing.T) {
	if os.Getenv("MAISTR0_GEMINI_REAL_SMOKE") != "1" {
		t.Skip("set MAISTR0_GEMINI_REAL_SMOKE=1 to run against an installed Gemini CLI")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || backend == nil {
		t.Fatal("Gemini CLI smoke test requires the complete opt-in configuration")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Gemini CLI backend: %v", err)
		}
	}()
	t.Logf("Gemini CLI version %s", backend.version)
	session, err := backend.CreateSession("Gemini CLI smoke test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	message, err := backend.SendMessage(ctx, session.ID, "Reply with exactly GEMINI_SMOKE_OK and no other text.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(message.Content) == "" {
		t.Fatal("Gemini CLI returned an empty smoke-test response")
	}
}

func unsetGeminiOptIn(t *testing.T, names ...string) {
	t.Helper()
	type saved struct {
		value   string
		present bool
	}
	previous := make(map[string]saved, len(names))
	for _, name := range names {
		value, present := os.LookupEnv(name)
		previous[name] = saved{value: value, present: present}
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for name, state := range previous {
			if state.present {
				_ = os.Setenv(name, state.value)
			} else {
				_ = os.Unsetenv(name)
			}
		}
	})
}

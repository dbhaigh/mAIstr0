package claudecode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

var claudeHelperOutput *os.File

func TestClaudeProcessHelper(t *testing.T) {
	helperIndex := -1
	for index, arg := range os.Args {
		if arg == "--claude-test-helper" {
			helperIndex = index
			break
		}
	}
	if helperIndex < 0 {
		return
	}
	claudeHelperOutput = os.Stdout
	os.Stdout = os.Stderr
	args := os.Args[helperIndex+1:]
	if hasArg(args, "--version") {
		key := os.Getenv("ANTHROPIC_API_KEY")
		version := strings.TrimPrefix(key, "test-version-")
		if !strings.HasPrefix(version, "2.") {
			version = "2.1.318"
		}
		_, _ = fmt.Fprintln(claudeHelperOutput, version)
		os.Exit(0)
	}
	if !hasArg(args, "--restricted") || !hasArg(args, "--bare") ||
		!hasPair(args, "--tools", "") || !hasPair(args, "--disallowedTools", "mcp__*") ||
		!hasArg(args, "--strict-mcp-config") || !hasPair(args, "--mcp-config", `{"mcpServers":{}}`) ||
		!hasPair(args, "--permission-mode", "plan") ||
		!hasPair(args, "--permission-prompts", "none") ||
		!hasArg(args, "--disable-slash-commands") {
		os.Exit(21)
	}
	sessionID := argValue(args, "--session-id")
	if sessionID == "" {
		sessionID = argValue(args, "--resume")
	}
	if sessionID == "" {
		os.Exit(22)
	}
	promptBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(23)
	}
	switch string(promptBytes) {
	case "first turn":
		if argValue(args, "--session-id") == "" || argValue(args, "--resume") != "" {
			os.Exit(25)
		}
	case "resumed turn":
		if argValue(args, "--resume") == "" || argValue(args, "--session-id") != "" {
			os.Exit(26)
		}
	case "EXIT":
		_, _ = fmt.Fprintln(os.Stderr, "secret-process-detail")
		os.Exit(24)
	case "ERROR":
		writeClaudeHelperLine(`{"type":"result","subtype":"error","is_error":true,"session_id":"` + sessionID + `","result":"private-result-detail"}`)
		os.Exit(0)
	case "BROKEN":
		_, _ = fmt.Fprintln(claudeHelperOutput, "sensitive-malformed-output")
		os.Exit(0)
	case "BLOCK":
		writeClaudeHelperLine(`{"type":"system","subtype":"init","session_id":"` + sessionID + `"}`)
		writeClaudeHelperLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"running"}}}`)
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	writeClaudeHelperLine(`{"type":"system","subtype":"init","session_id":"` + sessionID + `"}`)
	writeClaudeHelperLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hel"}}}`)
	writeClaudeHelperLine(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"lo"}}}`)
	writeClaudeHelperLine(`{"type":"result","subtype":"success","is_error":false,"session_id":"` + sessionID + `","result":"Hello"}`)
	os.Exit(0)
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

func argValue(args []string, name string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name {
			return args[index+1]
		}
	}
	return ""
}

func writeClaudeHelperLine(line string) {
	_, _ = fmt.Fprintln(claudeHelperOutput, line)
}

func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = t.TempDir()
	runnerConfig.Executable = executable
	runnerConfig.Args = []string{"-test.run=^TestClaudeProcessHelper$", "--", "--claude-test-helper"}
	runnerConfig.Environment = map[string]string{"ANTHROPIC_API_KEY": "test-version-2.1.318"}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	var version string
	if err := runner.RunLines(context.Background(), []string{"--version"}, nil, func(line []byte) error {
		version = string(line)
		return nil
	}); err != nil || version != "2.1.318" {
		t.Fatalf("fake CLI version = %q, err = %v", version, err)
	}
	backend, err := newBackendWithRunner(runner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Claude Code backend: %v", err)
		}
	})
	return backend
}

func TestSupportedClaudeCodeVersionRange(t *testing.T) {
	for _, version := range []string{"2.1.286", "v2.1.318 (Claude Code)", "Claude Code v2.1.318", "2.1.999"} {
		if err := ValidateVersion(version); err != nil {
			t.Errorf("ValidateVersion(%q): %v", version, err)
		}
	}
	for _, version := range []string{"2.1.285", "2.2.0", "3.0.0", "unknown"} {
		if err := ValidateVersion(version); err == nil {
			t.Errorf("ValidateVersion(%q) unexpectedly succeeded", version)
		}
	}
}

func TestClaudeCodeEnvironmentOptInIsExplicit(t *testing.T) {
	t.Setenv("MAISTR0_CLAUDE_CODE_EXECUTABLE", "")
	t.Setenv("MAISTR0_CLAUDE_CODE_WORKSPACE", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	backend, enabled, err := NewFromEnvironment()
	if backend != nil || !enabled || err == nil ||
		!strings.Contains(err.Error(), "requires both MAISTR0_CLAUDE_CODE_EXECUTABLE") {
		t.Fatalf("partial opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}

	t.Setenv("MAISTR0_CLAUDE_CODE_EXECUTABLE", "configured")
	t.Setenv("MAISTR0_CLAUDE_CODE_WORKSPACE", "configured")
	backend, enabled, err = NewFromEnvironment()
	if backend != nil || !enabled || err == nil ||
		!strings.Contains(err.Error(), "requires ANTHROPIC_API_KEY") {
		t.Fatalf("opt-in without credentials = backend %v, enabled %t, error %v", backend, enabled, err)
	}
}

func TestClaudeCodeSessionStreamsAndResumesCLIIdentity(t *testing.T) {
	backend := newTestBackend(t)
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
	if err := backend.RestoreSession(session); err == nil {
		t.Fatal("duplicate session restore succeeded")
	}
	saved, exists := backend.GetSession(session.ID)
	if !exists || saved.ID != session.ID || len(saved.Messages) != 2 {
		t.Fatalf("saved session = %+v, exists=%t", saved, exists)
	}

	restored := newTestBackend(t)
	if err := restored.RestoreSession(saved); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.SendMessage(context.Background(), session.ID, "resumed turn", nil); err != nil {
		t.Fatalf("resume Claude Code session: %v", err)
	}
	if _, got := restored.GetSession(session.ID); !got {
		t.Fatal("resumed session disappeared")
	}
}

func TestClaudeCodeMapsFailuresWithoutReturningProcessOutput(t *testing.T) {
	backend := newTestBackend(t)
	for _, test := range []struct {
		prompt string
		want   error
		leak   string
	}{
		{prompt: "ERROR", want: ErrTurnFailed, leak: "private-result-detail"},
		{prompt: "EXIT", want: ErrProcessFailed, leak: "secret-process-detail"},
		{prompt: "BROKEN", want: ErrProtocol, leak: "sensitive-malformed-output"},
	} {
		session, err := backend.CreateSession("failure", agent.SessionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.SendMessage(context.Background(), session.ID, test.prompt, nil)
		if !errors.Is(err, test.want) || strings.Contains(err.Error(), test.leak) {
			t.Errorf("prompt %q error = %v, want %v without child output", test.prompt, err, test.want)
		}
	}
}

func TestClaudeCodeCancellationStopsOwnedProcess(t *testing.T) {
	backend := newTestBackend(t)
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
		if event.Type != agent.EventTextDelta || event.Content != "running" {
			t.Fatalf("first event = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake Claude process did not start")
	}
	if !backend.InterruptSession(session.ID) {
		t.Fatal("active Claude Code session was not cancellable")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled turn error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Claude process did not stop")
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
	if _, err := backend.SendMessage(context.Background(), session.ID, "resumed turn", nil); err != nil {
		t.Fatalf("resume after interrupted CLI turn: %v", err)
	}
}

func TestClaudeCodeRejectsUnsupportedSessionOptionsAndTools(t *testing.T) {
	backend := newTestBackend(t)
	for _, config := range []agent.SessionConfig{
		{MaxSteps: 1}, {Temperature: 0.1}, {SystemPrompt: "must not be discarded"},
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

func TestRealClaudeCodeSmoke(t *testing.T) {
	if os.Getenv("MAISTR0_CLAUDE_CODE_REAL_SMOKE") != "1" {
		t.Skip("set MAISTR0_CLAUDE_CODE_REAL_SMOKE=1 to run against an installed Claude Code CLI")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || backend == nil {
		t.Fatal("real smoke requires the Claude Code executable and workspace environment settings")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Claude Code backend: %v", err)
		}
	}()
	session, err := backend.CreateSession("real smoke", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	message, err := backend.SendMessage(context.Background(), session.ID, "Reply with exactly READY.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message.Content, "READY") {
		t.Fatalf("real smoke response did not contain the expected marker")
	}
}

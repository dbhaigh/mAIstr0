package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	ErrDisabled                 = errors.New("external execution is disabled")
	ErrClosed                   = errors.New("external execution runner is closed")
	ErrBusy                     = errors.New("external execution concurrency limit is reached")
	ErrInvalidConfig            = errors.New("external execution configuration is invalid")
	ErrInvalidRequest           = errors.New("external execution request must be one JSON object")
	ErrIsolationUnsupported     = errors.New("requested process isolation is unsupported")
	ErrProcessTreeUnsupported   = errors.New("process-tree termination is unsupported on this platform")
	ErrInputTooLarge            = errors.New("external execution input exceeds its limit")
	ErrOutputTooLarge           = errors.New("external execution output exceeds its limit")
	ErrMalformedResponse        = errors.New("external execution returned malformed JSON")
	ErrProcessStart             = errors.New("external process could not be started")
	ErrProcessFailed            = errors.New("external process failed")
	ErrProcessTerminationFailed = errors.New("external process tree could not be terminated")
	ErrRuntimeLimit             = errors.New("external process exceeded its runtime limit")
)

const (
	defaultMaxRuntime     = 2 * time.Minute
	defaultMaxInputBytes  = 1 << 20
	defaultMaxOutputBytes = 1 << 20
	defaultMaxConcurrency = 1
	maximumMaxRuntime     = 10 * time.Minute
	maximumMaxInputBytes  = 8 << 20
	maximumMaxOutputBytes = 8 << 20
	maximumMaxConcurrency = 16
)

// Config is an opt-in, single-request stdio process policy. ApprovedWorkspace
// is the fixed working directory, not a filesystem or operating-system sandbox.
type Config struct {
	Enabled           bool
	ApprovedWorkspace string
	Executable        string
	Args              []string
	RequireIsolation  bool
	MaxRuntime        time.Duration
	MaxInputBytes     int
	MaxOutputBytes    int
	MaxConcurrency    int
}

// DefaultConfig disables external execution. Limits are applied when enabled.
func DefaultConfig() Config {
	return Config{
		MaxRuntime:     defaultMaxRuntime,
		MaxInputBytes:  defaultMaxInputBytes,
		MaxOutputBytes: defaultMaxOutputBytes,
		MaxConcurrency: defaultMaxConcurrency,
	}
}

// Runner starts one configured executable per request. It owns every started
// process tree and joins active runs during Close.
type Runner struct {
	config    Config
	workspace string
	ctx       context.Context
	cancel    context.CancelFunc
	slots     chan struct{}

	mu        sync.Mutex
	closed    bool
	closeErr  error
	active    sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
}

// NewRunner validates an explicitly enabled executable and approved workspace.
// The zero configuration returns a disabled runner.
func NewRunner(configured Config) (*Runner, error) {
	if configured.RequireIsolation {
		return nil, ErrIsolationUnsupported
	}
	if !configured.Enabled {
		return newRunner(configured, ""), nil
	}
	if !processTreeSupported() {
		return nil, ErrProcessTreeUnsupported
	}
	workspace, err := canonicalWorkspace(configured.ApprovedWorkspace)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(configured.Executable) ||
		strings.ContainsRune(configured.Executable, '\x00') {
		return nil, ErrInvalidConfig
	}
	if configured.MaxRuntime == 0 {
		configured.MaxRuntime = defaultMaxRuntime
	}
	if configured.MaxInputBytes == 0 {
		configured.MaxInputBytes = defaultMaxInputBytes
	}
	if configured.MaxOutputBytes == 0 {
		configured.MaxOutputBytes = defaultMaxOutputBytes
	}
	if configured.MaxConcurrency == 0 {
		configured.MaxConcurrency = defaultMaxConcurrency
	}
	if configured.MaxRuntime <= 0 || configured.MaxRuntime > maximumMaxRuntime ||
		configured.MaxInputBytes < 1 || configured.MaxInputBytes > maximumMaxInputBytes ||
		configured.MaxOutputBytes < 1 || configured.MaxOutputBytes > maximumMaxOutputBytes ||
		configured.MaxConcurrency < 1 || configured.MaxConcurrency > maximumMaxConcurrency {
		return nil, ErrInvalidConfig
	}
	for _, arg := range configured.Args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, ErrInvalidConfig
		}
	}
	configured.ApprovedWorkspace = workspace
	configured.Executable = filepath.Clean(configured.Executable)
	configured.Args = append([]string(nil), configured.Args...)
	return newRunner(configured, workspace), nil
}

func newRunner(configured Config, workspace string) *Runner {
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{
		config: configured, workspace: workspace,
		ctx: ctx, cancel: cancel,
		slots:     make(chan struct{}, configured.MaxConcurrency),
		closeDone: make(chan struct{}),
	}
}

// RunJSON sends one JSON object through stdin and accepts exactly one JSON
// response from stdout. Prompts and request data are never placed in argv.
func (r *Runner) RunJSON(ctx context.Context, request []byte) ([]byte, error) {
	if !r.config.Enabled {
		return nil, ErrDisabled
	}
	if ctx == nil {
		return nil, ErrInvalidRequest
	}
	if len(request) > r.config.MaxInputBytes {
		return nil, ErrInputTooLarge
	}
	if !validJSONObject(request) {
		return nil, ErrInvalidRequest
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	r.active.Add(1)
	r.mu.Unlock()
	defer r.active.Done()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.ctx.Err() != nil {
		return nil, ErrClosed
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		return nil, ErrBusy
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.ctx.Err() != nil {
		return nil, ErrClosed
	}

	runCtx, cancel := context.WithTimeout(ctx, r.config.MaxRuntime)
	stopClose := context.AfterFunc(r.ctx, cancel)
	defer func() {
		stopClose()
		cancel()
	}()

	response, processErr := r.runProcess(runCtx, request)
	if errors.Is(processErr, ErrProcessTerminationFailed) {
		r.mu.Lock()
		r.closeErr = errors.Join(r.closeErr, processErr)
		r.mu.Unlock()
	}
	if r.ctx.Err() != nil {
		return nil, errors.Join(ErrClosed, terminationError(processErr))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, terminationError(processErr))
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return nil, errors.Join(ErrRuntimeLimit, terminationError(processErr))
	}
	if processErr != nil {
		return nil, processErr
	}
	return response, nil
}

// Close cancels all active process trees and waits for every RunJSON call.
func (r *Runner) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.active.Wait()
		close(r.closeDone)
	})
	<-r.closeDone
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeErr
}

func terminationError(err error) error {
	if errors.Is(err, ErrProcessTerminationFailed) {
		return err
	}
	return nil
}

func (r *Runner) runProcess(ctx context.Context, request []byte) ([]byte, error) {
	workspace, err := canonicalWorkspace(r.workspace)
	if err != nil || workspace != r.workspace {
		return nil, ErrInvalidConfig
	}
	executable, err := filepath.EvalSymlinks(r.config.Executable)
	if err != nil {
		return nil, ErrProcessStart
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrProcessStart
	}

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return nil, ErrProcessStart
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		return nil, ErrProcessStart
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		return nil, ErrProcessStart
	}

	command := exec.Command(executable, r.config.Args...)
	command.Dir = workspace
	command.Env = processEnvironment()
	command.Stdin = stdinReader
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	if ctx.Err() != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
		return nil, ctx.Err()
	}
	tree, err := startProcessTree(command)
	_ = stdinReader.Close()
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	if err != nil {
		_ = stdinWriter.Close()
		_ = stdoutReader.Close()
		_ = stderrReader.Close()
		return nil, ErrProcessStart
	}

	output := &outputCollector{limit: r.config.MaxOutputBytes}
	inputDone := make(chan error, 1)
	go func() {
		_, writeErr := io.Copy(stdinWriter, bytes.NewReader(request))
		closeErr := stdinWriter.Close()
		inputDone <- errors.Join(writeErr, closeErr)
	}()
	var readers sync.WaitGroup
	readers.Add(2)
	go func() {
		defer readers.Done()
		_, _ = io.Copy(output.stdoutWriter(), stdoutReader)
	}()
	go func() {
		defer readers.Done()
		_, _ = io.Copy(output.stderrWriter(), stderrReader)
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	var waitErr error
	var terminateErr error
	select {
	case waitErr = <-waited:
	case <-ctx.Done():
		terminateErr = tree.Kill()
		if terminateErr != nil && command.Process != nil {
			_ = command.Process.Kill()
		}
		waitErr = <-waited
	}
	closeErr := tree.Close()
	_ = stdinWriter.Close()
	inputErr := <-inputDone
	_ = stdoutReader.Close()
	_ = stderrReader.Close()
	readers.Wait()

	if closeErr != nil {
		return nil, ErrProcessTerminationFailed
	}
	if output.exceeded {
		return nil, ErrOutputTooLarge
	}
	if inputErr != nil {
		return nil, ErrProcessFailed
	}
	if waitErr != nil {
		return nil, ErrProcessFailed
	}
	if !validJSONObject(output.stdout) {
		return nil, ErrMalformedResponse
	}
	return append([]byte(nil), output.stdout...), nil
}

func validJSONObject(data []byte) bool {
	if !json.Valid(data) {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(data, &object) == nil && object != nil
}

func canonicalWorkspace(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", ErrInvalidConfig
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", ErrInvalidConfig
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", ErrInvalidConfig
	}
	return filepath.Clean(resolved), nil
}

func processEnvironment() []string {
	if runtime.GOOS == "windows" {
		if systemRoot := os.Getenv("SystemRoot"); systemRoot != "" {
			return []string{"SystemRoot=" + systemRoot}
		}
	}
	return []string{}
}

type outputCollector struct {
	mu       sync.Mutex
	limit    int
	used     int
	stdout   []byte
	exceeded bool
}

type outputWriter struct {
	collector *outputCollector
	capture   bool
}

func (c *outputCollector) stdoutWriter() io.Writer {
	return outputWriter{collector: c, capture: true}
}

func (c *outputCollector) stderrWriter() io.Writer {
	return outputWriter{collector: c}
}

func (w outputWriter) Write(data []byte) (int, error) {
	c := w.collector
	c.mu.Lock()
	defer c.mu.Unlock()
	remaining := c.limit - c.used
	if remaining < 0 {
		remaining = 0
	}
	accepted := len(data)
	if accepted > remaining {
		accepted = remaining
		c.exceeded = true
	}
	c.used += accepted
	if w.capture && accepted > 0 {
		c.stdout = append(c.stdout, data[:accepted]...)
	}
	return len(data), nil
}

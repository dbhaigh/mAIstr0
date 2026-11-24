package execution

import (
	"bufio"
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
	ErrOutputCallback           = errors.New("external execution output could not be consumed")
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
	Environment       map[string]string
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
	for name, value := range configured.Environment {
		if !validEnvironmentName(name) || strings.ContainsRune(value, '\x00') {
			return nil, ErrInvalidConfig
		}
		if runtime.GOOS == "windows" && strings.EqualFold(name, "SystemRoot") {
			return nil, ErrInvalidConfig
		}
	}
	configured.ApprovedWorkspace = workspace
	configured.Executable = filepath.Clean(configured.Executable)
	configured.Args = append([]string(nil), configured.Args...)
	configured.Environment = cloneEnvironment(configured.Environment)
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
	return r.run(ctx, nil, request, nil)
}

// RunLines sends input on stdin and calls consume once for every stdout line.
// Arguments are appended to the configured argv; neither arguments nor output
// are included in errors returned by the runner.
func (r *Runner) RunLines(ctx context.Context, args []string, input []byte, consume func([]byte) error) error {
	if !r.config.Enabled {
		return ErrDisabled
	}
	if ctx == nil || consume == nil {
		return ErrInvalidRequest
	}
	if len(input) > r.config.MaxInputBytes {
		return ErrInputTooLarge
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return ErrInvalidRequest
		}
	}
	_, err := r.run(ctx, args, input, consume)
	return err
}

func (r *Runner) run(ctx context.Context, args []string, request []byte, consume func([]byte) error) ([]byte, error) {
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

	response, processErr := r.runProcess(runCtx, args, request, consume)
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
		r.mu.Lock()
		for name := range r.config.Environment {
			r.config.Environment[name] = ""
		}
		r.mu.Unlock()
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

func (r *Runner) runProcess(ctx context.Context, args []string, request []byte, consume func([]byte) error) ([]byte, error) {
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

	command := exec.Command(executable)
	command.Args = append(command.Args, r.config.Args...)
	command.Args = append(command.Args, args...)
	command.Dir = workspace
	command.Env = processEnvironment(r.config.Environment)
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

	output := &outputCollector{limit: r.config.MaxOutputBytes, overflow: make(chan struct{})}
	inputDone := make(chan error, 1)
	go func() {
		_, writeErr := io.Copy(stdinWriter, bytes.NewReader(request))
		closeErr := stdinWriter.Close()
		inputDone <- errors.Join(writeErr, closeErr)
	}()
	var readers sync.WaitGroup
	readers.Add(2)
	readersDone := make(chan struct{})
	go func() {
		readers.Wait()
		close(readersDone)
	}()
	stdoutDone := make(chan error, 1)
	go func() {
		defer readers.Done()
		if consume == nil {
			_, err := io.Copy(output.stdoutWriter(), stdoutReader)
			stdoutDone <- err
			return
		}
		scanner := bufio.NewScanner(io.TeeReader(stdoutReader, output.meterWriter()))
		scanner.Buffer(make([]byte, 64<<10), r.config.MaxOutputBytes)
		for scanner.Scan() {
			if err := consume(append([]byte(nil), scanner.Bytes()...)); err != nil {
				stdoutDone <- ErrOutputCallback
				return
			}
		}
		if err := scanner.Err(); err != nil {
			stdoutDone <- ErrOutputTooLarge
			return
		}
		stdoutDone <- nil
	}()
	go func() {
		defer readers.Done()
		_, _ = io.Copy(output.stderrWriter(), stderrReader)
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	var waitErr error
	var terminateErr error
	var consumeErr error
	waitDone := (<-chan error)(waited)
	var stdoutReadDone <-chan error = stdoutDone
	for waitDone != nil {
		select {
		case waitErr = <-waitDone:
			waitDone = nil
		case readErr := <-stdoutReadDone:
			stdoutReadDone = nil
			if readErr != nil {
				consumeErr = readErr
				terminateErr = tree.Kill()
				if terminateErr != nil && command.Process != nil {
					_ = command.Process.Kill()
				}
				waitErr = <-waited
				waitDone = nil
			}
		case <-output.overflow:
			terminateErr = tree.Kill()
			if terminateErr != nil && command.Process != nil {
				_ = command.Process.Kill()
			}
			waitErr = <-waited
			waitDone = nil
		case <-ctx.Done():
			terminateErr = tree.Kill()
			if terminateErr != nil && command.Process != nil {
				_ = command.Process.Kill()
			}
			waitErr = <-waited
			waitDone = nil
		}
	}
	if ctx.Err() != nil && terminateErr == nil {
		terminateErr = tree.Kill()
	}
	closeErr := tree.Close()
	_ = stdinWriter.Close()
	inputErr := <-inputDone
	drainTimer := time.NewTimer(processTreeTerminationWait)
	select {
	case <-readersDone:
	case <-drainTimer.C:
		_ = stdoutReader.Close()
		_ = stderrReader.Close()
		if closeErr == nil {
			closeErr = errors.New("process output pipes did not close")
		}
		<-readersDone
	}
	if !drainTimer.Stop() {
		select {
		case <-drainTimer.C:
		default:
		}
	}
	if stdoutReadDone != nil {
		readErr := <-stdoutReadDone
		if consumeErr == nil && readErr != nil {
			consumeErr = readErr
		}
	}
	_ = stdoutReader.Close()
	_ = stderrReader.Close()

	if closeErr != nil {
		return nil, ErrProcessTerminationFailed
	}
	if output.exceeded {
		return nil, ErrOutputTooLarge
	}
	if consumeErr != nil {
		return nil, consumeErr
	}
	if inputErr != nil {
		return nil, ErrProcessFailed
	}
	if waitErr != nil {
		return nil, ErrProcessFailed
	}
	if consume == nil && !validJSONObject(output.stdout) {
		return nil, ErrMalformedResponse
	}
	if consume == nil {
		return append([]byte(nil), output.stdout...), nil
	}
	return nil, nil
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

func processEnvironment(configured map[string]string) []string {
	environment := make([]string, 0, len(configured)+1)
	systemRootPresent := false
	if runtime.GOOS == "windows" {
		if systemRoot := os.Getenv("SystemRoot"); systemRoot != "" {
			environment = append(environment, "SystemRoot="+systemRoot)
			systemRootPresent = true
		}
	}
	for name, value := range configured {
		if systemRootPresent && runtime.GOOS == "windows" && strings.EqualFold(name, "SystemRoot") {
			continue
		}
		environment = append(environment, name+"="+value)
	}
	return environment
}

type outputCollector struct {
	mu       sync.Mutex
	limit    int
	used     int
	stdout   []byte
	exceeded bool
	overflow chan struct{}
	once     sync.Once
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

func (c *outputCollector) meterWriter() io.Writer {
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
		c.once.Do(func() { close(c.overflow) })
	}
	c.used += accepted
	if w.capture && accepted > 0 {
		c.stdout = append(c.stdout, data[:accepted]...)
	}
	return len(data), nil
}

func validEnvironmentName(name string) bool {
	if name == "" || strings.ContainsAny(name, "=\x00") {
		return false
	}
	for index, char := range name {
		if char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			index > 0 && char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return true
}

func cloneEnvironment(environment map[string]string) map[string]string {
	if environment == nil {
		return nil
	}
	cloned := make(map[string]string, len(environment))
	for name, value := range environment {
		cloned[name] = value
	}
	return cloned
}

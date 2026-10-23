package supervisor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/logging"
)

var (
	ErrNotFound            = errors.New("managed engine not found")
	ErrAlreadyRunning      = errors.New("managed engine is already running")
	ErrUnsupportedPlatform = errors.New("managed engine is unsupported on this platform")
	ErrInvalidShardPlan    = errors.New("invalid managed engine shard plan")
	ErrReadinessTimeout    = errors.New("managed engine readiness timed out")
	ErrProcessExited       = errors.New("managed engine exited before becoming ready")
	ErrStart               = errors.New("managed engine could not be started")
	ErrStop                = errors.New("managed engine could not be stopped")
	ErrRestartLimit        = errors.New("managed engine restart limit exceeded")
	ErrClosed              = errors.New("managed engine supervisor is closed")
)

const (
	stateStopped    = "stopped"
	stateStarting   = "starting"
	stateReady      = "ready"
	stateStopping   = "stopping"
	stateRestarting = "restarting"
	stateFailed     = "failed"
)

const maxLoggedOutputLine = 16 * 1024

var (
	processSecretAssignment = regexp.MustCompile(`(?i)([A-Za-z0-9_.-]*(?:api[_-]?key|access[_-]?token|token|password|secret|credential)[A-Za-z0-9_.-]*["']?\s*[:=]\s*["']?)[^"' \t,;}&]+`)
	processAuthorization    = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(?:bearer|basic)\s+[^\s,;]+`)
	processBearer           = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
	processURLUserInfo      = regexp.MustCompile(`(?i)(https?://)[^/\s:@]+:[^/@\s]+@`)
)

type ShardPlan struct {
	NNodes               int    `json:"nnodes"`
	NodeRank             int    `json:"node_rank"`
	MasterAddr           string `json:"master_addr"`
	RPC                  string `json:"rpc,omitempty"`
	TensorSplit          string `json:"tensor_split,omitempty"`
	TensorParallelSize   int    `json:"tensor_parallel_size,omitempty"`
	PipelineParallelSize int    `json:"pipeline_parallel_size,omitempty"`
}

type Status struct {
	ID           string     `json:"id"`
	Kind         string     `json:"kind"`
	State        string     `json:"state"`
	PID          int        `json:"pid,omitempty"`
	Endpoint     string     `json:"endpoint,omitempty"`
	RestartCount int        `json:"restart_count"`
	LastError    string     `json:"last_error,omitempty"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	Warnings     []string   `json:"warnings,omitempty"`
}

type RestartPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Options struct {
	ReadinessTimeout time.Duration
	ProbeInterval    time.Duration
	StopTimeout      time.Duration
	Restart          RestartPolicy
}

type Manager struct {
	ctx       context.Context
	cancel    context.CancelFunc
	options   Options
	engines   map[string]*managedEngine
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type managedEngine struct {
	mu                sync.Mutex
	config            config.ManagedEngineConfig
	binaryAllowlist   []string
	modelAllowlist    []string
	status            Status
	command           *exec.Cmd
	tree              processTree
	done              chan struct{}
	runGeneration     uint64
	controlGeneration uint64
	ready             bool
	userStopped       bool
	shardPlan         *ShardPlan
}

type processTree interface {
	Kill() error
	Close() error
}

func New(cfg config.NodeConfig, options Options) (*Manager, error) {
	if err := config.ValidateManagedEngines(cfg.ManagedEngines, cfg.ManagedEngineBinaryAllowlist, cfg.ManagedEngineModelAllowlist); err != nil {
		return nil, fmt.Errorf("managed engine configuration: %w", err)
	}
	options = normalizeOptions(options)
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		ctx:       ctx,
		cancel:    cancel,
		options:   options,
		engines:   make(map[string]*managedEngine, len(cfg.ManagedEngines)),
		closeDone: make(chan struct{}),
	}
	for _, configured := range cfg.ManagedEngines {
		entry := cloneManagedEngineConfig(configured)
		status := Status{ID: entry.ID, Kind: string(entry.Kind), State: stateStopped, Endpoint: managedEndpoint(entry)}
		if warning := nonLoopbackRPCWarning(entry); warning != "" {
			status.Warnings = []string{warning}
			logging.FileWarnf("managed_engine", "%s", warning)
		}
		manager.engines[entry.ID] = &managedEngine{
			config:          entry,
			binaryAllowlist: append([]string(nil), cfg.ManagedEngineBinaryAllowlist...),
			modelAllowlist:  append([]string(nil), cfg.ManagedEngineModelAllowlist...),
			status:          status,
		}
	}
	return manager, nil
}

func normalizeOptions(options Options) Options {
	if options.ReadinessTimeout <= 0 {
		options.ReadinessTimeout = 30 * time.Second
	}
	if options.ProbeInterval <= 0 {
		options.ProbeInterval = 200 * time.Millisecond
	}
	if options.StopTimeout <= 0 {
		options.StopTimeout = 10 * time.Second
	}
	if options.Restart.MaxAttempts <= 0 {
		options.Restart.MaxAttempts = 5
	}
	if options.Restart.InitialBackoff <= 0 {
		options.Restart.InitialBackoff = time.Second
	}
	if options.Restart.MaxBackoff <= 0 {
		options.Restart.MaxBackoff = 30 * time.Second
	}
	if options.Restart.MaxBackoff < options.Restart.InitialBackoff {
		options.Restart.MaxBackoff = options.Restart.InitialBackoff
	}
	return options
}

func (m *Manager) Statuses() []Status {
	engines := make([]*managedEngine, 0, len(m.engines))
	for _, engine := range m.engines {
		engines = append(engines, engine)
	}
	sort.Slice(engines, func(i, j int) bool { return engines[i].config.ID < engines[j].config.ID })
	statuses := make([]Status, 0, len(engines))
	for _, engine := range engines {
		statuses = append(statuses, engine.snapshot())
	}
	return statuses
}

func (m *Manager) Status(id string) (Status, error) {
	engine, err := m.engine(id)
	if err != nil {
		return Status{}, err
	}
	return engine.snapshot(), nil
}

func (m *Manager) Start(ctx context.Context, id string, plan *ShardPlan) error {
	if m.isClosed() {
		return ErrClosed
	}
	engine, err := m.engine(id)
	if err != nil {
		return err
	}
	if err := validatePlatformAndPlan(engine.config, plan); err != nil {
		return err
	}

	engine.mu.Lock()
	if engine.command != nil || engine.status.State == stateStarting || engine.status.State == stateReady || engine.status.State == stateRestarting {
		engine.mu.Unlock()
		return ErrAlreadyRunning
	}
	engine.controlGeneration++
	controlGeneration := engine.controlGeneration
	engine.userStopped = false
	engine.ready = false
	engine.shardPlan = cloneShardPlan(plan)
	engine.status.State = stateStarting
	engine.status.LastError = ""
	engine.status.StartedAt = nil
	engine.status.RestartCount = 0
	engine.mu.Unlock()

	runGeneration, done, err := m.launch(engine, controlGeneration, stateStarting)
	if err != nil {
		return err
	}
	startupCtx, cancel := context.WithTimeout(ctx, m.options.ReadinessTimeout)
	defer cancel()
	if err := m.waitForReadiness(startupCtx, engine, runGeneration, done, plan); err != nil {
		if stopErr := m.abortStartup(engine, runGeneration, err); stopErr != nil {
			return errors.Join(err, stopErr)
		}
		return err
	}
	if !m.markReady(engine, runGeneration) {
		return ErrProcessExited
	}
	return nil
}

func (m *Manager) Stop(ctx context.Context, id string) error {
	engine, err := m.engine(id)
	if err != nil {
		return err
	}
	return m.stop(engine, ctx)
}

func (m *Manager) Restart(ctx context.Context, id string, plan *ShardPlan) error {
	if m.isClosed() {
		return ErrClosed
	}
	engine, err := m.engine(id)
	if err != nil {
		return err
	}
	if err := validatePlatformAndPlan(engine.config, plan); err != nil {
		return err
	}
	if err := m.stop(engine, ctx); err != nil {
		return err
	}

	for attempt := 1; attempt <= m.options.Restart.MaxAttempts; attempt++ {
		if attempt > 1 {
			if err := waitBackoff(ctx, m.backoff(attempt-1)); err != nil {
				return err
			}
		}
		engine.mu.Lock()
		engine.status.RestartCount = attempt
		engine.shardPlan = cloneShardPlan(plan)
		engine.userStopped = false
		engine.ready = false
		engine.mu.Unlock()
		if err := m.startAttempt(ctx, engine, stateStarting); err == nil {
			return nil
		} else if !isRetryable(err) {
			return err
		}
	}
	engine.mu.Lock()
	engine.status.State = stateFailed
	engine.status.LastError = "restart limit exceeded"
	engine.mu.Unlock()
	return ErrRestartLimit
}

func (m *Manager) startAttempt(ctx context.Context, engine *managedEngine, state string) error {
	engine.mu.Lock()
	engine.controlGeneration++
	controlGeneration := engine.controlGeneration
	engine.status.State = state
	engine.status.LastError = ""
	engine.status.StartedAt = nil
	engine.mu.Unlock()

	runGeneration, done, err := m.launch(engine, controlGeneration, state)
	if err != nil {
		return err
	}
	startupCtx, cancel := context.WithTimeout(ctx, m.options.ReadinessTimeout)
	defer cancel()
	engine.mu.Lock()
	plan := cloneShardPlan(engine.shardPlan)
	engine.mu.Unlock()
	if err := m.waitForReadiness(startupCtx, engine, runGeneration, done, plan); err != nil {
		if stopErr := m.abortStartup(engine, runGeneration, err); stopErr != nil {
			return errors.Join(err, stopErr)
		}
		return err
	}
	if !m.markReady(engine, runGeneration) {
		return ErrProcessExited
	}
	return nil
}

func (m *Manager) launch(engine *managedEngine, controlGeneration uint64, state string) (uint64, <-chan struct{}, error) {
	if m.isClosed() {
		return 0, nil, ErrClosed
	}
	if err := config.ValidateManagedEngines(
		[]config.ManagedEngineConfig{engine.config},
		engine.binaryAllowlist,
		engine.modelAllowlist,
	); err != nil {
		engine.mu.Lock()
		if engine.controlGeneration == controlGeneration {
			engine.status.State = stateFailed
			engine.status.LastError = "managed engine configuration is no longer valid"
		}
		engine.mu.Unlock()
		return 0, nil, fmt.Errorf("%w: managed engine configuration is no longer valid", ErrStart)
	}
	engine.mu.Lock()
	if engine.controlGeneration != controlGeneration || engine.userStopped {
		engine.mu.Unlock()
		return 0, nil, ErrClosed
	}
	plan := cloneShardPlan(engine.shardPlan)
	engine.mu.Unlock()
	args, err := buildArgs(engine.config, plan)
	if err != nil {
		return 0, nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return 0, nil, ErrStart
	}
	cmd := exec.Command(engine.config.BinaryPath, args...)
	cmd.Stdout = writer
	cmd.Stderr = writer

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = reader.Close()
		_ = writer.Close()
		return 0, nil, ErrClosed
	}
	engine.mu.Lock()
	if engine.controlGeneration != controlGeneration || engine.userStopped {
		engine.mu.Unlock()
		m.mu.Unlock()
		_ = reader.Close()
		_ = writer.Close()
		return 0, nil, ErrClosed
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.logProcessOutput(reader, engine.config.ID)
	}()
	tree, err := startProcessTree(cmd)
	if err != nil {
		engine.status.State = stateFailed
		engine.status.LastError = "process could not be started"
		engine.status.PID = 0
		engine.mu.Unlock()
		m.mu.Unlock()
		_ = reader.Close()
		_ = writer.Close()
		logging.FileWarnf("managed_engine", "managed engine %s could not be started", engine.config.ID)
		return 0, nil, ErrStart
	}
	_ = writer.Close()
	engine.runGeneration++
	runGeneration := engine.runGeneration
	done := make(chan struct{})
	engine.command = cmd
	engine.tree = tree
	engine.done = done
	engine.ready = false
	engine.status.State = state
	engine.status.PID = cmd.Process.Pid
	engine.status.LastError = ""
	engine.status.StartedAt = nil
	engine.mu.Unlock()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.waitForProcess(engine, runGeneration, controlGeneration, cmd, tree, done)
	}()
	m.mu.Unlock()
	logging.FileInfof("managed_engine", "managed engine %s started (pid %d)", engine.config.ID, cmd.Process.Pid)
	return runGeneration, done, nil
}

func (m *Manager) waitForProcess(engine *managedEngine, runGeneration, controlGeneration uint64, cmd *exec.Cmd, tree processTree, done chan struct{}) {
	waitErr := cmd.Wait()
	_ = tree.Kill()
	_ = tree.Close()

	engine.mu.Lock()
	if engine.runGeneration != runGeneration {
		engine.mu.Unlock()
		close(done)
		return
	}
	engine.command = nil
	engine.tree = nil
	engine.status.PID = 0
	wasReady := engine.ready
	engine.ready = false
	intentional := engine.userStopped || engine.controlGeneration != controlGeneration || m.ctx.Err() != nil
	if intentional {
		engine.status.State = stateStopped
		engine.status.LastError = ""
	} else {
		engine.status.State = stateFailed
		engine.status.LastError = processExitMessage(waitErr, cmd)
	}
	plan := cloneShardPlan(engine.shardPlan)
	currentControlGeneration := engine.controlGeneration
	close(done)
	engine.mu.Unlock()

	if !intentional && wasReady {
		m.mu.Lock()
		if !m.closed {
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				m.restartAfterCrash(engine, currentControlGeneration, plan)
			}()
		}
		m.mu.Unlock()
	}
	if intentional {
		logging.FileInfof("managed_engine", "managed engine %s exited", engine.config.ID)
	} else {
		logging.FileWarnf("managed_engine", "managed engine %s exited", engine.config.ID)
	}
}

func (m *Manager) restartAfterCrash(engine *managedEngine, controlGeneration uint64, plan *ShardPlan) {
	for {
		engine.mu.Lock()
		if engine.controlGeneration != controlGeneration || engine.userStopped || m.ctx.Err() != nil {
			engine.mu.Unlock()
			return
		}
		if engine.status.RestartCount >= m.options.Restart.MaxAttempts {
			engine.status.State = stateFailed
			engine.status.LastError = "restart limit exceeded"
			engine.mu.Unlock()
			logging.FileErrorf("managed_engine", "managed engine %s exceeded its restart limit", engine.config.ID)
			return
		}
		attempt := engine.status.RestartCount + 1
		engine.status.RestartCount = attempt
		engine.status.State = stateRestarting
		engine.mu.Unlock()

		if err := waitBackoff(m.ctx, m.backoff(attempt)); err != nil {
			return
		}
		runGeneration, done, err := m.launch(engine, controlGeneration, stateRestarting)
		if err == nil {
			startupCtx, cancel := context.WithTimeout(m.ctx, m.options.ReadinessTimeout)
			err = m.waitForReadiness(startupCtx, engine, runGeneration, done, plan)
			cancel()
			if err == nil && m.markReady(engine, runGeneration) {
				logging.FileInfof("managed_engine", "managed engine %s restarted and is ready", engine.config.ID)
				return
			}
			if err == nil {
				err = ErrProcessExited
			}
			if stopErr := m.abortRestartAttempt(engine, runGeneration, controlGeneration, err); stopErr != nil {
				engine.mu.Lock()
				if engine.controlGeneration == controlGeneration {
					engine.status.State = stateFailed
					engine.status.LastError = "process did not stop after failed restart"
				}
				engine.mu.Unlock()
				return
			}
		}
		if !isRetryable(err) {
			engine.mu.Lock()
			if engine.controlGeneration == controlGeneration {
				engine.status.State = stateFailed
				engine.status.LastError = "restart failed"
			}
			engine.mu.Unlock()
			return
		}
	}
}

func (m *Manager) waitForReadiness(
	ctx context.Context,
	engine *managedEngine,
	runGeneration uint64,
	done <-chan struct{},
	plan *ShardPlan,
) error {
	if engine.config.Kind == config.ManagedEngineVLLM && plan != nil && plan.NodeRank > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-done:
			return ErrProcessExited
		default:
			engine.mu.Lock()
			stillCurrent := engine.runGeneration == runGeneration && engine.command != nil
			engine.mu.Unlock()
			if stillCurrent {
				return nil
			}
			return ErrProcessExited
		}
	}
	ticker := time.NewTicker(m.options.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return ErrProcessExited
		default:
		}
		ready, err := probe(ctx, engine.config)
		if err == nil && ready {
			select {
			case <-done:
				return ErrProcessExited
			default:
				engine.mu.Lock()
				stillCurrent := engine.runGeneration == runGeneration && engine.command != nil
				engine.mu.Unlock()
				if stillCurrent {
					return nil
				}
				return ErrProcessExited
			}
		}
		select {
		case <-done:
			return ErrProcessExited
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrReadinessTimeout
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) markReady(engine *managedEngine, runGeneration uint64) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.runGeneration != runGeneration || engine.command == nil || engine.userStopped {
		return false
	}
	engine.ready = true
	engine.status.State = stateReady
	now := time.Now().UTC()
	engine.status.StartedAt = &now
	return true
}

func (m *Manager) abortStartup(engine *managedEngine, runGeneration uint64, cause error) error {
	engine.mu.Lock()
	if engine.runGeneration != runGeneration {
		engine.mu.Unlock()
		return nil
	}
	engine.controlGeneration++
	controlGeneration := engine.controlGeneration
	engine.userStopped = true
	engine.ready = false
	engine.status.State = stateStopping
	tree, done := engine.tree, engine.done
	engine.mu.Unlock()

	if tree != nil {
		_ = tree.Kill()
	}
	stopErr := m.waitForStopContext(context.Background(), done)
	engine.mu.Lock()
	if engine.runGeneration == runGeneration && engine.controlGeneration == controlGeneration {
		engine.status.State = stateFailed
		if stopErr == nil {
			engine.status.PID = 0
			engine.status.LastError = startupErrorMessage(cause)
			engine.command = nil
			engine.tree = nil
		} else {
			engine.status.LastError = "process did not stop after startup failure"
		}
	}
	engine.mu.Unlock()
	return stopErr
}

func (m *Manager) abortRestartAttempt(engine *managedEngine, runGeneration, controlGeneration uint64, cause error) error {
	engine.mu.Lock()
	if engine.runGeneration != runGeneration || engine.controlGeneration != controlGeneration {
		engine.mu.Unlock()
		return nil
	}
	engine.ready = false
	engine.status.State = stateStopping
	tree, done := engine.tree, engine.done
	engine.mu.Unlock()

	if tree != nil {
		_ = tree.Kill()
	}
	stopErr := m.waitForStopContext(context.Background(), done)
	engine.mu.Lock()
	if engine.runGeneration == runGeneration && engine.controlGeneration == controlGeneration {
		engine.status.State = stateFailed
		if stopErr == nil {
			engine.status.PID = 0
			engine.status.LastError = startupErrorMessage(cause)
			engine.command = nil
			engine.tree = nil
		} else {
			engine.status.LastError = "process did not stop after failed restart"
		}
	}
	engine.mu.Unlock()
	return stopErr
}

func (m *Manager) stop(engine *managedEngine, ctx context.Context) error {
	engine.mu.Lock()
	engine.controlGeneration++
	engine.userStopped = true
	engine.ready = false
	if engine.command == nil {
		engine.status.State = stateStopped
		engine.status.PID = 0
		engine.status.LastError = ""
		engine.mu.Unlock()
		return nil
	}
	engine.status.State = stateStopping
	tree, done := engine.tree, engine.done
	engine.mu.Unlock()

	if tree != nil {
		if err := tree.Kill(); err != nil && !isClosed(done) {
			logging.FileWarnf("managed_engine", "managed engine %s process-tree termination was unsuccessful", engine.config.ID)
		}
	}
	if err := m.waitForStopContext(ctx, done); err != nil {
		return err
	}
	engine.mu.Lock()
	if engine.command == nil {
		engine.status.State = stateStopped
		engine.status.PID = 0
		engine.status.LastError = ""
	}
	engine.mu.Unlock()
	logging.FileInfof("managed_engine", "managed engine %s stopped", engine.config.ID)
	return nil
}

func (m *Manager) waitForStopContext(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	timeout := m.options.StopTimeout
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrStop
		}
		return ctx.Err()
	}
}

func (m *Manager) logProcessOutput(reader *os.File, id string) {
	defer reader.Close()
	buffered := bufio.NewReaderSize(reader, 4096)
	line := make([]byte, 0, 4096)
	truncated := false
	for {
		part, err := buffered.ReadSlice('\n')
		if len(line) < maxLoggedOutputLine {
			available := maxLoggedOutputLine - len(line)
			if len(part) > available {
				line = append(line, part[:available]...)
				truncated = true
			} else {
				line = append(line, part...)
			}
		} else {
			truncated = true
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if len(line) > 0 {
			m.logOutputLine(id, line, truncated)
			line = line[:0]
			truncated = false
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			logging.FileWarnf("managed_engine", "managed engine %s output stream ended unexpectedly", id)
			return
		}
	}
}

func (m *Manager) logOutputLine(id string, line []byte, truncated bool) {
	message := sanitizedProcessOutput(line, truncated)
	if message != "" {
		logging.FileInfof("managed_engine", "managed engine %s: %s", id, message)
	}
}

func sanitizedProcessOutput(line []byte, truncated bool) string {
	message := strings.TrimSpace(string(line))
	message = strings.Map(func(char rune) rune {
		if unicode.IsControl(char) && char != '\t' {
			return -1
		}
		return char
	}, strings.ToValidUTF8(message, "\uFFFD"))
	message = processSecretAssignment.ReplaceAllString(message, "$1[redacted]")
	message = processAuthorization.ReplaceAllString(message, "$1[redacted]")
	message = processBearer.ReplaceAllString(message, "Bearer [redacted]")
	message = processURLUserInfo.ReplaceAllString(message, "${1}[redacted]@")
	if truncated {
		message += " [truncated]"
	}
	return message
}

func (m *Manager) backoff(attempt int) time.Duration {
	delay := m.options.Restart.InitialBackoff
	for i := 1; i < attempt && delay < m.options.Restart.MaxBackoff; i++ {
		if delay > m.options.Restart.MaxBackoff/2 {
			return m.options.Restart.MaxBackoff
		}
		delay *= 2
	}
	if delay > m.options.Restart.MaxBackoff {
		return m.options.Restart.MaxBackoff
	}
	return delay
}

func (m *Manager) engine(id string) (*managedEngine, error) {
	engine, ok := m.engines[id]
	if !ok {
		return nil, ErrNotFound
	}
	return engine, nil
}

func (m *Manager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.cancel()
		m.mu.Unlock()
		for _, engine := range m.engines {
			ctx, cancel := context.WithTimeout(context.Background(), m.options.StopTimeout)
			if err := m.stop(engine, ctx); err != nil {
				m.closeErr = errors.Join(m.closeErr, err)
			}
			cancel()
		}
		m.wg.Wait()
		close(m.closeDone)
	})
	<-m.closeDone
	return m.closeErr
}

func (engine *managedEngine) snapshot() Status {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	status := engine.status
	status.Warnings = append([]string(nil), engine.status.Warnings...)
	if engine.status.StartedAt != nil {
		startedAt := *engine.status.StartedAt
		status.StartedAt = &startedAt
	}
	return status
}

func cloneManagedEngineConfig(configured config.ManagedEngineConfig) config.ManagedEngineConfig {
	cloned := configured
	if configured.Options.LlamaServer != nil {
		options := *configured.Options.LlamaServer
		cloned.Options.LlamaServer = &options
	}
	if configured.Options.GGMLRPCServer != nil {
		options := *configured.Options.GGMLRPCServer
		cloned.Options.GGMLRPCServer = &options
	}
	if configured.Options.VLLM != nil {
		options := *configured.Options.VLLM
		cloned.Options.VLLM = &options
	}
	return cloned
}

func cloneShardPlan(plan *ShardPlan) *ShardPlan {
	if plan == nil {
		return nil
	}
	copy := *plan
	return &copy
}

func nonLoopbackRPCWarning(configured config.ManagedEngineConfig) string {
	if configured.Kind != config.ManagedEngineGGMLRPCServer || configured.Options.GGMLRPCServer == nil {
		return ""
	}
	address := configured.Options.GGMLRPCServer.BindAddress
	if address == "" {
		address = "127.0.0.1"
	}
	if isLoopback(address) {
		return ""
	}
	return fmt.Sprintf("managed engine %s ggml-rpc-server binds to non-loopback address %s; verify network access is restricted", configured.ID, address)
}

func isLoopback(address string) bool {
	if strings.EqualFold(address, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(address, "[]"))
	return ip != nil && ip.IsLoopback()
}

func processExitMessage(waitErr error, cmd *exec.Cmd) string {
	if cmd.ProcessState != nil {
		return fmt.Sprintf("process exited with code %d", cmd.ProcessState.ExitCode())
	}
	if waitErr == nil {
		return "process exited"
	}
	return "process failed"
}

func startupErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrReadinessTimeout):
		return "readiness probe timed out"
	case errors.Is(err, context.Canceled):
		return "startup was canceled"
	case errors.Is(err, ErrProcessExited):
		return "process exited before readiness"
	default:
		return "managed engine startup failed"
	}
}

func validatePlatformAndPlan(configured config.ManagedEngineConfig, plan *ShardPlan) error {
	if configured.Kind == config.ManagedEngineVLLM {
		if nativeWindows() {
			return fmt.Errorf("%w: vllm managed engines are not supported on native Windows", ErrUnsupportedPlatform)
		}
		if plan == nil || plan.NNodes < 1 || plan.NNodes > 1024 || plan.NodeRank < 0 || plan.NodeRank >= plan.NNodes || !validMasterAddress(plan.MasterAddr) {
			return ErrInvalidShardPlan
		}
		if plan.TensorParallelSize < 0 || plan.PipelineParallelSize < 0 ||
			(plan.TensorParallelSize == 0) != (plan.PipelineParallelSize == 0) {
			return ErrInvalidShardPlan
		}
		return nil
	}
	if configured.Kind == config.ManagedEngineLlamaServer && plan != nil {
		if plan.RPC == "" || plan.TensorSplit == "" || strings.ContainsAny(plan.RPC, " \t\r\n\x00") {
			return ErrInvalidShardPlan
		}
		for _, address := range strings.Split(plan.RPC, ",") {
			host, port, err := net.SplitHostPort(address)
			if err != nil || host == "" {
				return ErrInvalidShardPlan
			}
			if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
				return ErrInvalidShardPlan
			}
		}
		for _, part := range strings.Split(plan.TensorSplit, ",") {
			value, err := strconv.ParseFloat(part, 64)
			if err != nil || value <= 0 {
				return ErrInvalidShardPlan
			}
		}
	}
	if plan != nil {
		if configured.Kind != config.ManagedEngineLlamaServer {
			return ErrInvalidShardPlan
		}
	}
	return nil
}

func validMasterAddress(address string) bool {
	if address == "" || strings.TrimSpace(address) != address || strings.ContainsAny(address, "/\x00\r\n\t ") {
		return false
	}
	if ip := net.ParseIP(strings.Trim(address, "[]")); ip != nil {
		return !ip.IsUnspecified()
	}
	if strings.Contains(address, ":") || len(address) > 253 {
		return false
	}
	for _, label := range strings.Split(address, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
				return false
			}
		}
	}
	return true
}

func isRetryable(err error) bool {
	return !errors.Is(err, ErrStop) &&
		(errors.Is(err, ErrStart) || errors.Is(err, ErrReadinessTimeout) || errors.Is(err, ErrProcessExited))
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isClosed(done <-chan struct{}) bool {
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func probe(ctx context.Context, configured config.ManagedEngineConfig) (bool, error) {
	host, port, tcpOnly := probeAddress(configured)
	address := net.JoinHostPort(host, strconv.Itoa(port))
	if tcpOnly {
		connection, err := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(ctx, "tcp", address)
		if err != nil {
			return false, err
		}
		return true, connection.Close()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/health", nil)
	if err != nil {
		return false, err
	}
	client := &http.Client{Timeout: 750 * time.Millisecond}
	response, err := client.Do(request)
	if err != nil {
		return false, err
	}
	_, copyErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	closeErr := response.Body.Close()
	if copyErr != nil {
		return false, copyErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	return response.StatusCode >= 200 && response.StatusCode < 300, nil
}

func probeAddress(configured config.ManagedEngineConfig) (host string, port int, tcpOnly bool) {
	switch configured.Kind {
	case config.ManagedEngineLlamaServer:
		host, port = "127.0.0.1", 8080
		if options := configured.Options.LlamaServer; options != nil {
			if options.Host != "" {
				host = loopbackProbeHost(options.Host)
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
	case config.ManagedEngineGGMLRPCServer:
		host, port, tcpOnly = "127.0.0.1", 50052, true
		if options := configured.Options.GGMLRPCServer; options != nil {
			if options.BindAddress != "" {
				host = loopbackProbeHost(options.BindAddress)
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
	case config.ManagedEngineVLLM:
		host, port = "127.0.0.1", 8000
		if options := configured.Options.VLLM; options != nil {
			if options.Host != "" {
				host = loopbackProbeHost(options.Host)
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
	}
	return host, port, tcpOnly
}

func loopbackProbeHost(host string) string {
	switch strings.Trim(host, "[]") {
	case "0.0.0.0":
		return "127.0.0.1"
	case "::":
		return "::1"
	default:
		return strings.Trim(host, "[]")
	}
}

func buildArgs(configured config.ManagedEngineConfig, plan *ShardPlan) ([]string, error) {
	if err := validatePlatformAndPlan(configured, plan); err != nil {
		return nil, err
	}
	model := configured.ModelPath
	if model == "" {
		model = configured.ModelID
	}
	switch configured.Kind {
	case config.ManagedEngineLlamaServer:
		host, port := "127.0.0.1", 8080
		args := []string{"--model", model}
		if options := configured.Options.LlamaServer; options != nil {
			if options.Host != "" {
				host = options.Host
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		args = append(args, "--host", host, "--port", strconv.Itoa(port))
		if options := configured.Options.LlamaServer; options != nil {
			if options.ContextSize > 0 {
				args = append(args, "--ctx-size", strconv.Itoa(options.ContextSize))
			}
			if options.GPULayers > 0 {
				args = append(args, "--n-gpu-layers", strconv.Itoa(options.GPULayers))
			}
			if options.Threads > 0 {
				args = append(args, "--threads", strconv.Itoa(options.Threads))
			}
		}
		if plan != nil {
			if plan.RPC != "" {
				args = append(args, "--rpc", plan.RPC)
			}
			if plan.TensorSplit != "" {
				args = append(args, "--tensor-split", plan.TensorSplit)
			}
		}
		return args, nil
	case config.ManagedEngineGGMLRPCServer:
		host, port := "127.0.0.1", 50052
		if options := configured.Options.GGMLRPCServer; options != nil {
			if options.BindAddress != "" {
				host = options.BindAddress
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		return []string{"--host", host, "--port", strconv.Itoa(port)}, nil
	case config.ManagedEngineVLLM:
		host, port := "127.0.0.1", 8000
		args := []string{"serve", model}
		if plan.NodeRank > 0 {
			args = append(args, "--headless")
		}
		args = append(args, "--nnodes", strconv.Itoa(plan.NNodes), "--node-rank", strconv.Itoa(plan.NodeRank), "--master-addr", plan.MasterAddr)
		if options := configured.Options.VLLM; options != nil {
			if options.Host != "" {
				host = options.Host
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		args = append(args, "--host", host, "--port", strconv.Itoa(port))
		tensorParallelSize := plan.TensorParallelSize
		if tensorParallelSize == 0 && configured.Options.VLLM != nil {
			tensorParallelSize = configured.Options.VLLM.TensorParallelSize
		}
		if tensorParallelSize > 0 {
			args = append(args, "--tensor-parallel-size", strconv.Itoa(tensorParallelSize))
		}
		if plan.PipelineParallelSize > 0 {
			args = append(args, "--pipeline-parallel-size", strconv.Itoa(plan.PipelineParallelSize))
		}
		return args, nil
	default:
		return nil, fmt.Errorf("%w: unsupported managed engine kind", ErrStart)
	}
}

func managedEndpoint(configured config.ManagedEngineConfig) string {
	host, port := "127.0.0.1", 0
	switch configured.Kind {
	case config.ManagedEngineLlamaServer:
		port = 8080
		if options := configured.Options.LlamaServer; options != nil {
			if options.Host != "" {
				host = loopbackProbeHost(options.Host)
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
	case config.ManagedEngineVLLM:
		port = 8000
		if options := configured.Options.VLLM; options != nil {
			if options.Host != "" {
				host = loopbackProbeHost(options.Host)
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
	case config.ManagedEngineGGMLRPCServer:
		host, port = "127.0.0.1", 50052
		if options := configured.Options.GGMLRPCServer; options != nil {
			if options.BindAddress != "" {
				host = options.BindAddress
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		return net.JoinHostPort(host, strconv.Itoa(port))
	default:
		return ""
	}
}

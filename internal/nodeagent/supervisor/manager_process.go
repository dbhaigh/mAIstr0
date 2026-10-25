package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/maistr0/maistr0/internal/config"
	enginepkg "github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/logging"
)

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
	plan := cloneShardPlan(engine.launchPlan)
	engine.mu.Unlock()
	args, err := buildArgs(engine.config, plan)
	if err != nil {
		return 0, nil, err
	}
	if engine.modelName != "" {
		args = append(args, "--alias", engine.modelName)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return 0, nil, ErrStart
	}
	cmd := exec.Command(engine.config.BinaryPath, args...)
	if engine.config.Template != "" {
		template, ok := enginepkg.ManagedEngineTemplateFor(engine.config.Template)
		if !ok {
			_ = reader.Close()
			_ = writer.Close()
			return 0, nil, fmt.Errorf("%w: managed engine template is not available", ErrStart)
		}
		cmd.Env = applyTemplateEnvironment(os.Environ(), template.Environment)
	}
	if engine.config.Kind == config.ManagedEngineVLLM {
		environment, insecure, err := vllmChildEnvironment(
			engine.config,
			m.trustedNetworkInterfaces,
			m.allowInsecureShardNetwork,
			m.networkAddresses,
			os.Environ(),
		)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %w", ErrUntrustedShardNetwork, err)
		}
		cmd.Env = environment
		if insecure {
			warning := "INSECURE SHARD NETWORK: vLLM distributed traffic is unencrypted and is using an untrusted interface because allow_insecure_shard_network is enabled"
			engine.mu.Lock()
			if !containsWarning(engine.status.Warnings, warning) {
				engine.status.Warnings = append(engine.status.Warnings, warning)
			}
			engine.mu.Unlock()
			logging.FileWarnf("managed_engine", "%s", warning)
		}
	}
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
		engine.launchPlan = nil
		engine.mu.Unlock()
		return m.stopRelay(ctx, engine.config.ID)
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
	if err := m.stopRelay(ctx, engine.config.ID); err != nil {
		return err
	}
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

func processExitMessage(waitErr error, cmd *exec.Cmd) string {
	if cmd.ProcessState != nil {
		return fmt.Sprintf("process exited with code %d", cmd.ProcessState.ExitCode())
	}
	if waitErr == nil {
		return "process exited"
	}
	return "process failed"
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

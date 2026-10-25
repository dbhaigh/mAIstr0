package supervisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/logging"
)

func (m *Manager) markStartFailed(engine *managedEngine, err error) {
	engine.mu.Lock()
	engine.status.State = stateFailed
	engine.status.LastError = startupErrorMessage(err)
	engine.status.PID = 0
	engine.launchPlan = nil
	engine.mu.Unlock()
}

func (m *Manager) Start(ctx context.Context, id string, plan *ShardPlan) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.installingWorker {
		m.mu.Unlock()
		return ErrWorkerBusy
	}
	engine, ok := m.engines[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	if engine.config.Kind == config.ManagedEngineGGMLRPCServer && m.relayController == nil {
		m.mu.Unlock()
		return ErrRelayUnavailable
	}
	if err := validatePlatformAndPlan(engine.config, plan); err != nil {
		m.mu.Unlock()
		return err
	}

	engine.mu.Lock()
	if engine.command != nil || engine.status.State == stateStarting || engine.status.State == stateReady || engine.status.State == stateRestarting {
		engine.mu.Unlock()
		m.mu.Unlock()
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
	m.workerControls++
	m.mu.Unlock()
	defer m.finishWorkerControl()

	launchPlan, err := m.prepareLaunchPlan(ctx, engine, plan)
	if err != nil {
		m.markStartFailed(engine, err)
		return err
	}
	engine.mu.Lock()
	engine.launchPlan = launchPlan
	engine.mu.Unlock()

	runGeneration, done, err := m.launch(engine, controlGeneration, stateStarting)
	if err != nil {
		_ = m.stopRelay(context.Background(), id)
		m.markStartFailed(engine, err)
		return err
	}
	startupCtx, cancel := context.WithTimeout(ctx, m.readinessTimeout(engine))
	defer cancel()
	if err := m.waitForReadiness(startupCtx, engine, runGeneration, done, plan); err != nil {
		if stopErr := m.abortStartup(engine, runGeneration, err); stopErr != nil {
			_ = m.stopRelay(context.Background(), id)
			return errors.Join(err, stopErr)
		}
		_ = m.stopRelay(context.Background(), id)
		return err
	}
	if engine.config.Kind == config.ManagedEngineGGMLRPCServer {
		controller := m.rpcRelayController()
		if controller == nil {
			err := ErrRelayUnavailable
			if stopErr := m.abortStartup(engine, runGeneration, err); stopErr != nil {
				return errors.Join(err, stopErr)
			}
			return err
		}
		relayEndpoint, relayErr := controller.StartWorker(ctx, id, managedEndpoint(engine.config))
		if relayErr != nil {
			_ = m.stopRelay(context.Background(), id)
			if stopErr := m.abortStartup(engine, runGeneration, relayErr); stopErr != nil {
				return errors.Join(relayErr, stopErr)
			}
			return fmt.Errorf("%w: %w", ErrRelayUnavailable, relayErr)
		}
		engine.mu.Lock()
		engine.status.RelayEndpoint = relayEndpoint
		engine.mu.Unlock()
	}
	if !m.markReady(engine, runGeneration) {
		_ = m.stopRelay(context.Background(), id)
		return ErrProcessExited
	}
	return nil
}

func (m *Manager) Stop(ctx context.Context, id string) error {
	if err := m.beginWorkerControl(); err != nil {
		return err
	}
	defer m.finishWorkerControl()
	engine, err := m.engine(id)
	if err != nil {
		return err
	}
	return m.stop(engine, ctx)
}

func (m *Manager) Restart(ctx context.Context, id string, plan *ShardPlan) error {
	if err := m.beginWorkerControl(); err != nil {
		return err
	}
	defer m.finishWorkerControl()
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

	launchPlan, err := m.prepareLaunchPlan(ctx, engine, plan)
	if err != nil {
		m.markStartFailed(engine, err)
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
		engine.launchPlan = cloneShardPlan(launchPlan)
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
	_ = m.stopRelay(context.Background(), id)
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
	startupCtx, cancel := context.WithTimeout(ctx, m.readinessTimeout(engine))
	defer cancel()
	engine.mu.Lock()
	plan := cloneShardPlan(engine.launchPlan)
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
			startupCtx, cancel := context.WithTimeout(m.ctx, m.readinessTimeout(engine))
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

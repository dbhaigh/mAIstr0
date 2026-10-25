package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/httpx"
)

func (m *Manager) readinessTimeout(engine *managedEngine) time.Duration {
	if engine.config.Kind == config.ManagedEngineLlamaServer || engine.config.Kind == config.ManagedEngineVLLM {
		return m.options.ModelReadinessTimeout
	}
	return m.options.ReadinessTimeout
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
				return fmt.Errorf("%w: startup deadline exceeded; check the node's managed_engine log entries for model loading progress or runtime errors", ErrReadinessTimeout)
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
	client := httpx.NewClient(httpx.ClientOptions{Timeout: 750 * time.Millisecond})
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

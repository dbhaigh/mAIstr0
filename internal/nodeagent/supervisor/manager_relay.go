package supervisor

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/config"
)

func (m *Manager) SetRPCRelayController(controller RPCRelayController) {
	m.mu.Lock()
	m.relayController = controller
	m.mu.Unlock()
}

func (m *Manager) rpcRelayController() RPCRelayController {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.relayController
}

func (m *Manager) prepareLaunchPlan(ctx context.Context, engine *managedEngine, plan *ShardPlan) (*ShardPlan, error) {
	launchPlan := cloneShardPlan(plan)
	if engine.config.Kind != config.ManagedEngineLlamaServer || plan == nil || len(plan.RPCRelays) == 0 {
		return launchPlan, nil
	}
	controller := m.rpcRelayController()
	if controller == nil {
		return nil, ErrRelayUnavailable
	}
	addresses, err := controller.StartHead(ctx, engine.config.ID, plan.RPCRelays)
	if err != nil {
		return nil, fmt.Errorf("%w: start local mTLS relay endpoints: %w", ErrRelayUnavailable, err)
	}
	if len(addresses) != len(plan.RPCRelays) {
		_ = controller.Stop(context.Background(), engine.config.ID)
		return nil, fmt.Errorf("%w: relay endpoint count does not match the shard plan", ErrRelayUnavailable)
	}
	for _, address := range addresses {
		host, port, ok := splitEndpointHostPort(address)
		if !ok || !isLoopback(host) {
			_ = controller.Stop(context.Background(), engine.config.ID)
			return nil, fmt.Errorf("%w: local relay must bind to loopback", ErrRelayUnavailable)
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			_ = controller.Stop(context.Background(), engine.config.ID)
			return nil, fmt.Errorf("%w: local relay returned an invalid port", ErrRelayUnavailable)
		}
	}
	launchPlan.RPC = strings.Join(addresses, ",")
	launchPlan.RPCRelays = nil
	return launchPlan, nil
}

func (m *Manager) stopRelay(ctx context.Context, id string) error {
	controller := m.rpcRelayController()
	if controller == nil {
		return nil
	}
	if err := controller.Stop(ctx, id); err != nil {
		return fmt.Errorf("stop managed engine RPC relay: %w", err)
	}
	return nil
}

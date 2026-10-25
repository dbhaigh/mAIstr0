package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/logging"
)

var (
	ErrNotFound              = errors.New("managed engine not found")
	ErrAlreadyRunning        = errors.New("managed engine is already running")
	ErrUnsupportedPlatform   = errors.New("managed engine is unsupported on this platform")
	ErrInvalidShardPlan      = errors.New("invalid managed engine shard plan")
	ErrUntrustedShardNetwork = errors.New("managed engine shard network is not protected or trusted")
	ErrRelayUnavailable      = errors.New("managed engine RPC relay is unavailable")
	ErrReadinessTimeout      = errors.New("managed engine readiness timed out")
	ErrProcessExited         = errors.New("managed engine exited before becoming ready")
	ErrStart                 = errors.New("managed engine could not be started")
	ErrStop                  = errors.New("managed engine could not be stopped")
	ErrRestartLimit          = errors.New("managed engine restart limit exceeded")
	ErrClosed                = errors.New("managed engine supervisor is closed")
)

const (
	stateStopped    = "stopped"
	stateStarting   = "starting"
	stateReady      = "ready"
	stateStopping   = "stopping"
	stateRestarting = "restarting"
	stateFailed     = "failed"
)

type ShardPlan struct {
	NNodes               int              `json:"nnodes"`
	NodeRank             int              `json:"node_rank"`
	MasterAddr           string           `json:"master_addr"`
	RPC                  string           `json:"rpc,omitempty"`
	RPCRelays            []RPCRelayTarget `json:"rpc_relays,omitempty"`
	TensorSplit          string           `json:"tensor_split,omitempty"`
	TensorParallelSize   int              `json:"tensor_parallel_size,omitempty"`
	PipelineParallelSize int              `json:"pipeline_parallel_size,omitempty"`
}

type RPCRelayTarget struct {
	PeerID  string `json:"peer_id"`
	Address string `json:"address"`
}

type Status struct {
	ID            string     `json:"id"`
	Kind          string     `json:"kind"`
	State         string     `json:"state"`
	PID           int        `json:"pid,omitempty"`
	Endpoint      string     `json:"endpoint,omitempty"`
	RelayEndpoint string     `json:"relay_endpoint,omitempty"`
	RestartCount  int        `json:"restart_count"`
	LastError     string     `json:"last_error,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	Warnings      []string   `json:"warnings,omitempty"`
}

type RestartPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Options struct {
	ReadinessTimeout      time.Duration
	ModelReadinessTimeout time.Duration
	ProbeInterval         time.Duration
	StopTimeout           time.Duration
	Restart               RestartPolicy
	NetworkAddresses      NetworkAddressSource
}

type RPCRelayController interface {
	StartWorker(context.Context, string, string) (string, error)
	StartHead(context.Context, string, []RPCRelayTarget) ([]string, error)
	Stop(context.Context, string) error
}

type Manager struct {
	ctx                       context.Context
	cancel                    context.CancelFunc
	options                   Options
	engines                   map[string]*managedEngine
	mu                        sync.Mutex
	closed                    bool
	wg                        sync.WaitGroup
	closeOnce                 sync.Once
	closeDone                 chan struct{}
	closeErr                  error
	relayController           RPCRelayController
	trustedNetworkInterfaces  []string
	allowInsecureShardNetwork bool
	networkAddresses          NetworkAddressSource
	installMu                 sync.Mutex
	workerRoot                string
	workerConfig              config.NodeConfig
	workerInstallation        *workerInstallation
	installingWorker          bool
	workerControls            int
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
	launchPlan        *ShardPlan
	runGeneration     uint64
	controlGeneration uint64
	ready             bool
	userStopped       bool
	shardPlan         *ShardPlan
	modelName         string
}

type processTree interface {
	Kill() error
	Close() error
}

func New(cfg config.NodeConfig, options Options) (*Manager, error) {
	workerRoot, err := installedWorkerRoot(cfg)
	if err != nil {
		return nil, err
	}
	installation, err := loadWorkerInstallation(workerRoot)
	if err != nil {
		return nil, err
	}
	if installation != nil {
		cfg = withInstalledWorker(cfg, workerRoot, *installation)
	}
	if err := config.ValidateManagedEngines(cfg.ManagedEngines, cfg.ManagedEngineBinaryAllowlist, cfg.ManagedEngineModelAllowlist); err != nil {
		return nil, fmt.Errorf("managed engine configuration: %w", err)
	}
	options = normalizeOptions(options)
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		ctx:                       ctx,
		cancel:                    cancel,
		options:                   options,
		engines:                   make(map[string]*managedEngine, len(cfg.ManagedEngines)),
		closeDone:                 make(chan struct{}),
		trustedNetworkInterfaces:  append([]string(nil), cfg.TrustedNetworkInterfaces...),
		allowInsecureShardNetwork: cfg.AllowInsecureShardNetwork,
		networkAddresses:          options.NetworkAddresses,
		workerRoot:                workerRoot,
		workerConfig:              cfg,
		workerInstallation:        installation,
	}
	for _, configured := range cfg.ManagedEngines {
		entry := cloneManagedEngineConfig(configured)
		status := Status{ID: entry.ID, Kind: string(entry.Kind), State: stateStopped, Endpoint: managedEndpoint(entry)}
		if entry.Kind == config.ManagedEngineGGMLRPCServer {
			status.RelayEndpoint = configuredRPCRelayEndpoint(cfg)
		}
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
		if entry.ID == installedHeadID && installation != nil && installation.Head != nil {
			manager.engines[entry.ID].modelName = installation.Head.Name
		}
	}
	return manager, nil
}

func normalizeOptions(options Options) Options {
	if options.ModelReadinessTimeout <= 0 {
		options.ModelReadinessTimeout = options.ReadinessTimeout
		if options.ModelReadinessTimeout <= 0 {
			options.ModelReadinessTimeout = 10 * time.Minute
		}
	}
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
	if options.NetworkAddresses == nil {
		options.NetworkAddresses = systemNetworkAddressSource{}
	}
	return options
}

func (m *Manager) Statuses() []Status {
	m.mu.Lock()
	engines := make([]*managedEngine, 0, len(m.engines))
	for _, engine := range m.engines {
		engines = append(engines, engine)
	}
	m.mu.Unlock()
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

func (m *Manager) engine(id string) (*managedEngine, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
		engines := make([]*managedEngine, 0, len(m.engines))
		for _, engine := range m.engines {
			engines = append(engines, engine)
		}
		m.mu.Unlock()
		for _, engine := range engines {
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
	copy.RPCRelays = append([]RPCRelayTarget(nil), plan.RPCRelays...)
	return &copy
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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
)

type shardFixtureNode struct {
	http    *httptest.Server
	manager *supervisor.Manager
}

// The fixture replaces only node HTTP/authentication and relay transport. The
// orchestrator, planner, capacity gates and process supervisor are production
// implementations. Relay authentication is tested in internal/nodeagent.
func newShardFixtureNodes(t *testing.T, mode shard.Mode, binaries map[string]string) []*shardFixtureNode {
	t.Helper()
	rpcAddresses := []string{freeTCPAddress(t), freeTCPAddress(t)}
	headAddresses := []string{freeTCPAddress(t), freeTCPAddress(t)}
	t.Setenv("MAISTR0_FAKE_VLLM_WORKER", "http://"+headAddresses[1])
	nodes := make([]*shardFixtureNode, 2)
	for i := range nodes {
		id := fmt.Sprintf("node-%d", i)
		cfg := config.NodeConfig{
			AdvertiseAddr:            fmt.Sprintf("https://10.20.0.%d:7451", i+1),
			TrustedNetworkInterfaces: []string{"fixture"},
		}
		if mode == shard.ModeLlamaCPPRPC {
			cfg.ManagedEngines = []config.ManagedEngineConfig{{
				ID: "rpc", Kind: config.ManagedEngineGGMLRPCServer, BinaryPath: binaries["fakerpc"],
				Options: config.ManagedEngineOptions{GGMLRPCServer: &config.GGMLRPCServerOptions{
					Port: fixturePort(t, rpcAddresses[i]),
				}},
			}}
			if i == 0 {
				cfg.ManagedEngines = append(cfg.ManagedEngines, config.ManagedEngineConfig{
					ID: "head", Kind: config.ManagedEngineLlamaServer, BinaryPath: binaries["fakellama"], ModelID: "fake-sharded",
					Options: config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{Port: fixturePort(t, headAddresses[i])}},
				})
			}
		} else {
			cfg.ManagedEngines = []config.ManagedEngineConfig{{
				ID: "vllm", Kind: config.ManagedEngineVLLM, BinaryPath: binaries["fakevllm"], ModelID: "fake-sharded",
				Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: fixturePort(t, headAddresses[i])}},
			}}
		}
		for _, entry := range cfg.ManagedEngines {
			cfg.ManagedEngineBinaryAllowlist = append(cfg.ManagedEngineBinaryAllowlist, filepath.Dir(entry.BinaryPath))
		}
		manager, err := supervisor.New(cfg, supervisor.Options{
			ReadinessTimeout: 5 * time.Second, ProbeInterval: 10 * time.Millisecond, StopTimeout: 2 * time.Second,
			Restart:          supervisor.RestartPolicy{MaxAttempts: 1, InitialBackoff: time.Minute, MaxBackoff: time.Minute},
			NetworkAddresses: fixtureNetworkAddresses{},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := manager.Close(); err != nil {
				t.Errorf("close supervisor: %v", err)
			}
		})
		manager.SetRPCRelayController(fixtureRPCRelay{addresses: rpcAddresses, nodeID: id})
		node := &shardFixtureNode{manager: manager}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/engines/managed", func(w http.ResponseWriter, _ *http.Request) {
			fixtureJSON(w, manager.Statuses())
		})
		mux.HandleFunc("POST /api/engines/managed/{id}/start", func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				Plan *supervisor.ShardPlan `json:"shard_plan"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := manager.Start(r.Context(), r.PathValue("id"), request.Plan); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			status, err := manager.Status(r.PathValue("id"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			fixtureJSON(w, status)
		})
		mux.HandleFunc("POST /api/engines/managed/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
			if err := manager.Stop(r.Context(), r.PathValue("id")); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			status, err := manager.Status(r.PathValue("id"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			fixtureJSON(w, status)
		})
		mux.HandleFunc("POST /api/engines/managed/{id}/proxy/{path...}", func(w http.ResponseWriter, r *http.Request) {
			status, err := manager.Status(r.PathValue("id"))
			if err != nil || status.State != "ready" {
				http.Error(w, "head is not ready", http.StatusServiceUnavailable)
				return
			}
			target, err := url.Parse(status.Endpoint)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			r.URL.Path = "/" + r.PathValue("path")
			httputil.NewSingleHostReverseProxy(target).ServeHTTP(w, r)
		})
		node.http = httptest.NewServer(mux)
		t.Cleanup(node.http.Close)
		nodes[i] = node
	}
	return nodes
}

func fixturePort(t *testing.T, address string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func fixtureJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		panic(err)
	}
}

type fixtureNetworkAddresses struct{}

func (fixtureNetworkAddresses) List() ([]supervisor.NetworkAddress, error) {
	return []supervisor.NetworkAddress{{Interface: "fixture", IP: net.ParseIP("10.20.0.1")}}, nil
}

type fixtureRPCRelay struct {
	addresses []string
	nodeID    string
}

func (r fixtureRPCRelay) StartWorker(context.Context, string, string) (string, error) {
	if r.nodeID == "node-0" {
		return "10.20.0.1:7452", nil
	}
	return "10.20.0.2:7452", nil
}

func (r fixtureRPCRelay) StartHead(_ context.Context, _ string, targets []supervisor.RPCRelayTarget) ([]string, error) {
	addresses := make([]string, 0, len(targets))
	for _, target := range targets {
		switch target.PeerID {
		case "node-0":
			addresses = append(addresses, r.addresses[0])
		case "node-1":
			addresses = append(addresses, r.addresses[1])
		default:
			return nil, fmt.Errorf("unknown fixture relay peer %q", target.PeerID)
		}
	}
	return addresses, nil
}

func (fixtureRPCRelay) Stop(context.Context, string) error { return nil }

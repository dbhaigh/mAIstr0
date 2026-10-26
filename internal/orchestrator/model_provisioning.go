package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
)

const modelMemoryReserve = 1.2

var modelParameterCount = regexp.MustCompile(`(?i)(?:^|[^0-9.])(\d+(?:\.\d+)?)b(?:$|[^a-z])`)

type modelProvisionNodeClient interface {
	DoModelPull(*http.Request) (*http.Response, error)
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
}

type modelPullAttempt struct {
	done chan struct{}
	err  error
}

type autoModelProvisioner struct {
	shards   *ShardManager
	ggufGate chan struct{}
	registry *cluster.Registry
	client   modelProvisionNodeClient

	mu       sync.Mutex
	inFlight map[string]*modelPullAttempt
}

func newAutoModelProvisioner(registry *cluster.Registry, client modelProvisionNodeClient) *autoModelProvisioner {
	return &autoModelProvisioner{
		registry: registry,
		client:   client,
		inFlight: make(map[string]*modelPullAttempt),
		ggufGate: make(chan struct{}, 1),
	}
}

func (p *autoModelProvisioner) provision(ctx context.Context, subtasks []scheduler.Subtask) error {
	for _, subtask := range subtasks {
		nodes := p.provisioningNodes()
		if _, err := scheduler.Assign([]scheduler.Subtask{subtask}, nodes); err == nil {
			continue
		}
		var ggufErr error
		if p.shards != nil {
			ggufErr = p.provisionGGUF(ctx, subtask)
			if ggufErr == nil {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}

		candidates := modelCandidates(subtask, nodes)
		if len(candidates) == 0 {
			return fmt.Errorf("no compatible local or recommended model fits subtask %s (required capabilities: %v); GGUF recovery: %v; no curated Ollama model fits the reported node capabilities", subtask.ID, subtask.RequiredCapabilities, ggufErr)
		}

		triedNodes := make(map[string]bool)
		var pullErrors []string
		provisioned := false
		for _, candidate := range candidates {
			if triedNodes[candidate.node.ID] {
				continue
			}
			triedNodes[candidate.node.ID] = true
			if err := p.pull(ctx, candidate.node, candidate.model); err != nil {
				pullErrors = append(pullErrors, fmt.Sprintf("%s on %s: %v", candidate.model.Name, candidate.node.ID, err))
				continue
			}
			provisioned = true
			break
		}
		if !provisioned {
			return fmt.Errorf("could not provision a model for subtask %s: %s", subtask.ID, strings.Join(pullErrors, "; "))
		}
	}
	return nil
}

type modelCandidate struct {
	node  cluster.NodeStatus
	model engine.Model
	score float64
}

func modelCandidates(subtask scheduler.Subtask, nodes []cluster.NodeStatus) []modelCandidate {
	var candidates []modelCandidate
	catalog := engine.ModelCatalog("ollama", "")
	for _, node := range nodes {
		if !node.Healthy || !node.EngineReady("ollama") {
			continue
		}
		for _, catalogModel := range catalog {
			sizeGB, ok := estimatedModelSizeGB(catalogModel.Name)
			if !ok || !nodeCanFitModel(node, sizeGB) || nodeHasModel(node, catalogModel.Name) {
				continue
			}
			model := engine.Model{
				Name: catalogModel.Name, Engine: catalogModel.Engine,
				Tags: append([]string(nil), catalogModel.Tags...), SizeGB: sizeGB,
			}
			candidateNode := node
			candidateNode.Models = []engine.Model{model}
			assignments, err := scheduler.Assign([]scheduler.Subtask{subtask}, []cluster.NodeStatus{candidateNode})
			if err != nil || len(assignments) != 1 {
				continue
			}
			candidates = append(candidates, modelCandidate{
				node: node, model: model, score: assignments[0].Score,
			})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if candidates[i].model.SizeGB != candidates[j].model.SizeGB {
			return candidates[i].model.SizeGB < candidates[j].model.SizeGB
		}
		if candidates[i].node.ID != candidates[j].node.ID {
			return candidates[i].node.ID < candidates[j].node.ID
		}
		return candidates[i].model.Name < candidates[j].model.Name
	})
	return candidates
}

func estimatedModelSizeGB(name string) (float64, bool) {
	if strings.EqualFold(name, "phi4-mini") {
		return 2.8, true
	}
	match := modelParameterCount.FindStringSubmatch(name)
	if len(match) != 2 {
		return 0, false
	}
	parameters, err := strconv.ParseFloat(match[1], 64)
	if err != nil || parameters <= 0 {
		return 0, false
	}
	return parameters*0.65 + 0.3, true
}

func nodeCanFitModel(node cluster.NodeStatus, sizeGB float64) bool {
	if sizeGB <= 0 {
		return false
	}
	now := time.Now()
	telemetryFresh := !node.TelemetryAt.IsZero() &&
		!node.TelemetryAt.After(now) &&
		now.Sub(node.TelemetryAt) <= 30*time.Second
	if node.Hardware.HasGPU && telemetryFresh &&
		float64(node.Hardware.GPUMemoryFreeMB) >= sizeGB*1024*modelMemoryReserve {
		return true
	}

	// Fall back to host CPU memory when fresh GPU memory stats are unavailable or insufficient.
	totalRAM := node.Hardware.TotalRAMMB
	if totalRAM == 0 {
		return false
	}
	availableRAM := float64(totalRAM) * 0.7
	if telemetryFresh && node.Hardware.MemoryStatsAvailable && node.Hardware.MemoryUsage >= 0 && node.Hardware.MemoryUsage <= 100 {
		availableRAM = float64(totalRAM) * float64(100-node.Hardware.MemoryUsage) / 100
	}
	return availableRAM >= sizeGB*1024*modelMemoryReserve
}

func nodeHasModel(node cluster.NodeStatus, name string) bool {
	for _, model := range node.Models {
		if strings.EqualFold(model.Engine, "ollama") && strings.EqualFold(model.Name, name) {
			return true
		}
	}
	return false
}

func (p *autoModelProvisioner) pull(ctx context.Context, node cluster.NodeStatus, model engine.Model) error {
	key := node.ID + "\x00" + model.Name
	p.mu.Lock()
	if attempt, ok := p.inFlight[key]; ok {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &modelPullAttempt{done: make(chan struct{})}
	p.inFlight[key] = attempt
	p.mu.Unlock()

	err := p.pullAndRefresh(ctx, node, model)

	p.mu.Lock()
	attempt.err = err
	delete(p.inFlight, key)
	close(attempt.done)
	p.mu.Unlock()
	return err
}

func (p *autoModelProvisioner) pullAndRefresh(ctx context.Context, node cluster.NodeStatus, model engine.Model) error {
	logging.Infof("orchestrator", "orchestrator: auto-pulling model %s on node %s", model.Name, node.ID)
	body, err := json.Marshal(map[string]string{"model": model.Name, "engine": model.Engine})
	if err != nil {
		return fmt.Errorf("encode model pull request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, node.Address+"/models/pull", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create model pull request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.DoModelPull(request)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return fmt.Errorf("request model pull: %w", err)
	}
	if response == nil {
		return errors.New("model pull returned no response")
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("model pull returned %s and its error body could not be read: %w", response.Status, readErr)
		}
		return fmt.Errorf("model pull returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}

	status, err := p.client.discoveredNodeStatusContext(ctx, node.Address)
	if err != nil {
		return fmt.Errorf("refresh node inventory after pulling %s: %w", model.Name, err)
	}
	if status.ID != node.ID {
		return fmt.Errorf("refreshed node identity %q does not match target %q", status.ID, node.ID)
	}
	if !nodeHasModel(status, model.Name) {
		return fmt.Errorf("node %s completed the pull but did not report model %s as available", node.ID, model.Name)
	}
	p.registry.Upsert(status)
	return nil
}

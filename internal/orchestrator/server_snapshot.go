package orchestrator

import (
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
)

func (s *Server) healthSweepLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !s.isElectedOrchestrator() {
			continue
		}
		for _, node := range s.registry.All() {
			if node.Healthy && time.Since(node.LastSeen) > 30*time.Second {
				s.registry.MarkUnhealthy(node.ID)
				log.Printf("orchestrator: node %s is offline after missing heartbeats", node.ID)
			}
		}
		s.publishSnapshot()
	}
}

// clusterSnapshot is the payload pushed over /api/events; the GUI applies
// it directly instead of polling the individual REST endpoints.
type clusterSnapshot struct {
	Nodes          []cluster.NodeStatus    `json:"nodes"`
	Tasks          []*taskmgr.Task         `json:"tasks"`
	Discovery      []discovery.Peer        `json:"discovery"`
	BuildVersion   string                  `json:"build_version"`
	LeaderID       string                  `json:"leader_id,omitempty"`
	MemberCount    int                     `json:"member_count"`
	AgentStats     *agent.AgentStats       `json:"agent_stats,omitempty"`
	Memory         *memory.Insights        `json:"memory,omitempty"`
	Dispatch       *dispatchSnapshot       `json:"dispatch"`
	ClusterHealth  ClusterHealth           `json:"cluster_health"`
	RouterEvents   []routerRequestEvent    `json:"router_events,omitempty"`
	ModelInventory []dashboardModelEntry   `json:"model_inventory"`
	PipelinePlans  []dashboardPipelinePlan `json:"pipeline_plans"`
}

type dashboardModelEntry struct {
	ModelID string   `json:"model_id"`
	Kind    string   `json:"kind"`
	Nodes   []string `json:"nodes,omitempty"`
}

type dashboardPipelinePlan struct {
	ModelID string                   `json:"model_id"`
	Status  string                   `json:"status"`
	Plan    *config.PipelinePlan     `json:"plan,omitempty"`
	Stages  []dashboardPipelineStage `json:"stages"`
}

type dashboardPipelineStage struct {
	Stage                int      `json:"stage"`
	NodeID               string   `json:"node_id"`
	StartLayer           int      `json:"start_layer"`
	EndLayer             int      `json:"end_layer"`
	Engine               string   `json:"engine"`
	Status               string   `json:"status"`
	Active               int      `json:"active"`
	Capacity             int      `json:"capacity,omitempty"`
	UtilizationPercent   *float64 `json:"utilization_percent,omitempty"`
	ActivationTransferMS *float64 `json:"activation_transfer_ms,omitempty"`
	ActivationBytes      int      `json:"activation_bytes,omitempty"`
}

func (s *Server) snapshot() clusterSnapshot {
	nodes := s.registry.All()
	activeNodes := 0
	leaderID := ""
	for _, node := range nodes {
		if node.Healthy {
			activeNodes++
		}
		if node.Leader {
			leaderID = node.ID
			break
		}
	}
	s.electionPort().recordLeader(leaderID)
	dispatch := s.dispatcher.snapshot()
	snapshot := clusterSnapshot{
		Nodes:          nodes,
		Tasks:          s.tasks.Recent(50),
		Discovery:      s.electionPort().discoverySnapshot(),
		BuildVersion:   version.String(),
		LeaderID:       leaderID,
		MemberCount:    activeNodes,
		Dispatch:       dispatch,
		ClusterHealth:  s.clusterHealthFor(nodes, dispatch),
		RouterEvents:   s.routerEvents.snapshot(),
		ModelInventory: s.dashboardModelInventory(nodes),
		PipelinePlans:  s.dashboardPipelinePlans(nodes),
	}
	if s.harness != nil {
		stats := s.agentStats()
		snapshot.AgentStats = &stats
	}
	if s.mem != nil {
		insights := s.mem.Insights()
		snapshot.Memory = &insights
	}
	return snapshot
}

func (s *Server) dashboardModelInventory(nodes []cluster.NodeStatus) []dashboardModelEntry {
	shardedSources := make(map[string]struct{}, len(s.shardedModels))
	shardedLogicalIDs := make(map[string]struct{}, len(s.shardedModels))
	entries := make(map[string]*dashboardModelEntry)
	for logicalID, model := range s.shardedModels {
		key := strings.ToLower(logicalID)
		shardedLogicalIDs[key] = struct{}{}
		shardedSources[strings.ToLower(model.SourceModelID)] = struct{}{}
		entries[key] = &dashboardModelEntry{ModelID: logicalID, Kind: "sharded"}
	}

	wholeNodes := make(map[string]map[string]struct{})
	wholeNames := make(map[string]string)
	for _, node := range nodes {
		for _, model := range node.Models {
			modelID := strings.TrimSpace(model.Name)
			key := strings.ToLower(modelID)
			if key == "" {
				continue
			}
			if _, isLogicalShardedModel := shardedLogicalIDs[key]; isLogicalShardedModel {
				continue
			}
			if _, isShardedSource := shardedSources[key]; isShardedSource {
				continue
			}
			if wholeNodes[key] == nil {
				wholeNodes[key] = make(map[string]struct{})
				wholeNames[key] = modelID
			}
			wholeNodes[key][node.ID] = struct{}{}
		}
	}
	for key, nodeIDs := range wholeNodes {
		nodes := make([]string, 0, len(nodeIDs))
		for nodeID := range nodeIDs {
			nodes = append(nodes, nodeID)
		}
		sort.Strings(nodes)
		entries[key] = &dashboardModelEntry{ModelID: wholeNames[key], Kind: "whole", Nodes: nodes}
	}

	inventory := make([]dashboardModelEntry, 0, len(entries))
	for _, entry := range entries {
		inventory = append(inventory, *entry)
	}
	sort.Slice(inventory, func(i, j int) bool {
		if inventory[i].ModelID != inventory[j].ModelID {
			return strings.ToLower(inventory[i].ModelID) < strings.ToLower(inventory[j].ModelID)
		}
		return inventory[i].Kind < inventory[j].Kind
	})
	return inventory
}

func (s *Server) dashboardPipelinePlans(nodes []cluster.NodeStatus) []dashboardPipelinePlan {
	byNode := make(map[string]cluster.NodeStatus, len(nodes))
	for _, node := range nodes {
		byNode[node.ID] = node
	}
	logicalIDs := make([]string, 0, len(s.shardedModels))
	for logicalID := range s.shardedModels {
		logicalIDs = append(logicalIDs, logicalID)
	}
	sort.Strings(logicalIDs)

	pipelines := make([]dashboardPipelinePlan, 0, len(logicalIDs))
	for _, logicalID := range logicalIDs {
		pipeline := dashboardPipelinePlan{
			ModelID: logicalID,
			Status:  "not_placed",
			Stages:  []dashboardPipelineStage{},
		}
		plan, exists := s.pipelinePlans.get(logicalID)
		if !exists {
			pipelines = append(pipelines, pipeline)
			continue
		}
		planCopy := plan
		pipeline.Plan = &planCopy
		pipeline.Status = "ready"
		for i, spec := range plan.Stages {
			stage := dashboardPipelineStage{
				Stage: i, NodeID: spec.NodeID, StartLayer: spec.StartLayer,
				EndLayer: spec.EndLayer, Engine: spec.Engine, Status: "unavailable",
			}
			node, found := byNode[spec.NodeID]
			if !found || !node.Healthy {
				stage.Status = "offline"
			} else if node.MaxLayersItCanHost >= spec.EndLayer-spec.StartLayer &&
				nodeSupportsShardedModel(node, plan.Model) &&
				nodeHasReadyStageEngine(node, spec.Engine, plan.Model.SourceModelID, spec.EndLayer-spec.StartLayer) {
				stage.Status = "ready"
			}
			if stage.Status != "ready" {
				pipeline.Status = "degraded"
			}
			if found {
				stage.Capacity = node.MaxConcurrentTasks
				if stage.Capacity > 0 {
					utilization := float64(stage.Active) / float64(stage.Capacity) * 100
					stage.UtilizationPercent = &utilization
				}
			}
			metricKey := pipelineStageMetricKey{
				logicalID: logicalID, stageIndex: i, nodeID: spec.NodeID,
				startLayer: spec.StartLayer, endLayer: spec.EndLayer,
			}
			if s.pipelineMetrics != nil {
				metric := s.pipelineMetrics.snapshot(metricKey)
				stage.Active = metric.active
				if stage.Capacity > 0 {
					utilization := float64(stage.Active) / float64(stage.Capacity) * 100
					stage.UtilizationPercent = &utilization
				}
				if metric.hasActivation {
					latency := float64(metric.activationLatency) / float64(time.Millisecond)
					stage.ActivationTransferMS = &latency
					stage.ActivationBytes = metric.activationBytes
				}
			}
			pipeline.Stages = append(pipeline.Stages, stage)
		}
		pipelines = append(pipelines, pipeline)
	}
	return pipelines
}

func (s *Server) publishSnapshot() {
	data, err := json.Marshal(s.snapshot())
	if err != nil {
		return
	}
	s.events.Broadcast(data)
}

// broadcastLoop pushes cluster state to every connected dashboard on a
// short interval so nodes coming online, load changes, and task progress
// all show up in real time without the browser needing to poll.
func (s *Server) broadcastLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.publishSnapshot()
	}
}

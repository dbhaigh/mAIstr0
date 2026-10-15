package orchestrator

import (
	"encoding/json"
	"log"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
)

func (s *Server) healthSweepLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
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
	Nodes        []cluster.NodeStatus `json:"nodes"`
	Tasks        []*taskmgr.Task      `json:"tasks"`
	Discovery    []discovery.Peer     `json:"discovery"`
	BuildVersion string               `json:"build_version"`
	LeaderID     string               `json:"leader_id,omitempty"`
	MemberCount  int                  `json:"member_count"`
	AgentStats   *agent.AgentStats    `json:"agent_stats,omitempty"`
	Memory       *memory.Insights     `json:"memory,omitempty"`
	Dispatch     *dispatchSnapshot    `json:"dispatch"`
	RouterEvents []routerRequestEvent `json:"router_events,omitempty"`
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
	s.election.recordLeader(leaderID)
	stats := s.agentStats()
	snapshot := clusterSnapshot{
		Nodes:        nodes,
		Tasks:        s.tasks.Recent(50),
		Discovery:    s.election.discoverySnapshot(),
		BuildVersion: version.String(),
		LeaderID:     leaderID,
		MemberCount:  activeNodes,
		AgentStats:   &stats,
		Dispatch:     s.dispatcher.snapshot(),
		RouterEvents: s.routerEvents.snapshot(),
	}
	if s.mem != nil {
		insights := s.mem.Insights()
		snapshot.Memory = &insights
	}
	return snapshot
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

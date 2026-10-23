package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func (c *peerComponent) peerSnapshot() []cluster.NodeStatus {
	return c.state.peersSnapshot()
}

func (c *peerComponent) currentTasks() []*taskmgr.Task {
	if c.isMostCapable() {
		return c.tasks.All()
	}
	leader := c.mostCapableNode()
	resp, err := c.peerHTTP().Get(leader.Address + "/tasks")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var tasks []*taskmgr.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil
	}
	return tasks
}

func (c *peerComponent) dispatchToPeer(ctx context.Context, taskID string, asg scheduler.Assignment) {
	cfg := c.state.configSnapshot()
	if !c.tasks.WaitForDependencies(ctx, taskID, asg.Subtask.ID) {
		if ctx.Err() == nil {
			c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("a required subtask did not complete"))
		}
		return
	}
	c.tasks.StartSubtask(taskID, asg.Subtask.ID)
	available := false
	for _, peer := range c.peerSnapshot() {
		if peer.ID == asg.NodeID && peer.Healthy && peer.Address == asg.Address {
			available = true
			break
		}
	}
	if !available && asg.NodeID != cfg.NodeID {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("node was ejected before dispatch"))
		return
	}
	prompt, ok := c.tasks.InputForSubtask(taskID, asg.Subtask.ID)
	if !ok {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("subtask prerequisites are not available"))
		return
	}
	body, _ := json.Marshal(executeRequest{
		SubtaskID: asg.Subtask.ID,
		Model:     asg.Model,
		Prompt:    prompt,
	})

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, asg.Address+"/execute", bytes.NewReader(body))
	if err != nil {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", err)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := c.dispatchClient.Do(request)
	if err != nil {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("node returned "+resp.Status+": "+string(errBody)))
		return
	}

	var out executeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", err)
		return
	}
	if out.Error != "" {
		c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New(out.Error))
		return
	}
	c.tasks.CompleteSubtask(taskID, asg.Subtask.ID, out.Output, nil)
}

// mostCapableNode returns the cluster's elected leader. The orchestrator's
// registry enforces the election and marks the winner on every snapshot, so
// a peer with the leader flag is authoritative; local scoring is only the
// bootstrap fallback before the first snapshot arrives.
func (c *peerComponent) mostCapableNode() cluster.NodeStatus {
	cfg := c.state.configSnapshot()
	hw := c.telemetry.hardwareSnapshot()
	self := cluster.NodeStatus{
		ID:           cfg.NodeID,
		Address:      cfg.AdvertiseAddr,
		Hardware:     hw,
		FastScore:    fastScore(hw),
		DefaultModel: c.models.defaultModelName(),
		ActiveTasks:  c.executor.activeTaskCount(),
		Healthy:      true,
	}
	best := self
	for _, node := range c.peerSnapshot() {
		if !node.Healthy {
			continue
		}
		if node.Leader {
			return node
		}
		if node.ID == self.ID {
			continue
		}
		if cluster.CapabilityScore(node) > cluster.CapabilityScore(best) ||
			(cluster.CapabilityScore(node) == cluster.CapabilityScore(best) && node.ID < best.ID) {
			best = node
		}
	}
	return best
}

func (c *peerComponent) fastestNode() cluster.NodeStatus {
	cfg := c.state.configSnapshot()
	hw := c.telemetry.hardwareSnapshot()
	self := cluster.NodeStatus{
		ID:           cfg.NodeID,
		Address:      cfg.AdvertiseAddr,
		Hardware:     hw,
		FastScore:    fastScore(hw),
		DefaultModel: c.models.defaultModelName(),
		ActiveTasks:  c.executor.activeTaskCount(),
		Healthy:      true,
	}
	best := self
	for _, node := range c.peerSnapshot() {
		if !node.Healthy || node.ID == self.ID {
			continue
		}
		if node.FastScore > best.FastScore ||
			(node.FastScore == best.FastScore && node.ID < best.ID) {
			best = node
		}
	}
	return best
}

func (c *peerComponent) isMostCapable() bool {
	return c.mostCapableNode().ID == c.state.configSnapshot().NodeID
}

func (c *peerComponent) isFastest() bool {
	return c.fastestNode().ID == c.state.configSnapshot().NodeID
}

func (c *peerComponent) peerAddress(id string) (string, bool) {
	cfg := c.state.configSnapshot()
	if id == cfg.NodeID {
		return cfg.AdvertiseAddr, true
	}
	for _, node := range c.peerSnapshot() {
		if node.ID == id {
			return node.Address, true
		}
	}
	return "", false
}

// proxy forwards the incoming request's method/body to addr+path and
// copies the response straight back to the caller.
func (c *peerComponent) proxy(w http.ResponseWriter, r *http.Request, addr, path string) {
	bodyLimit := httpx.ControlJSONBodyBytes
	if path == "/api/memory/sync" {
		bodyLimit = c.state.configSnapshot().SyncMaxBytes
		if bodyLimit <= 0 {
			bodyLimit = config.DefaultSyncMaxBytes
		}
	} else if strings.Contains(path, "/chat") ||
		strings.Contains(path, "/dialogues") ||
		strings.Contains(path, "/peer/converse") ||
		strings.HasSuffix(path, "/execute") {
		bodyLimit = httpx.InferenceJSONBodyBytes
	}
	body, err := httpx.ReadBody(w, r, bodyLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(r.Method, addr+path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for key, values := range r.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := c.peerHTTP()
	taskSubmission := r.Method == http.MethodPost && path == "/api/tasks"
	if taskSubmission || strings.Contains(r.Header.Get("Accept"), "text/event-stream") || r.URL.Query().Get("stream") == "true" {
		client = c.dispatchClient
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "peer unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if err := httpx.NewStreamWriter(w).Copy(resp.Body, nil); err != nil {
		logging.Errorf("nodeagent", "nodeagent: peer proxy response stream failed: %v", err)
	}
}

func (s *agentState) peersSnapshot() []cluster.NodeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cluster.NodeStatus, len(s.peers))
	copy(out, s.peers)
	return out
}

func (s *agentState) replacePeers(peers []cluster.NodeStatus) {
	s.mu.Lock()
	s.peers = append([]cluster.NodeStatus(nil), peers...)
	s.mu.Unlock()
}

func (a *Agent) currentTasks() []*taskmgr.Task {
	return a.peerComponent.currentTasks()
}

func (a *Agent) Peers() []cluster.NodeStatus {
	return a.peerComponent.peerSnapshot()
}

func (a *Agent) dispatchToPeer(ctx context.Context, taskID string, assignment scheduler.Assignment) {
	a.peerComponent.dispatchToPeer(ctx, taskID, assignment)
}

func (a *Agent) mostCapableNode() cluster.NodeStatus {
	return a.peerComponent.mostCapableNode()
}

func (a *Agent) fastestNode() cluster.NodeStatus {
	return a.peerComponent.fastestNode()
}

func (a *Agent) isMostCapable() bool { return a.peerComponent.isMostCapable() }
func (a *Agent) isFastest() bool     { return a.peerComponent.isFastest() }
func (a *Agent) peerAddress(id string) (string, bool) {
	return a.peerComponent.peerAddress(id)
}

func (a *Agent) proxy(w http.ResponseWriter, r *http.Request, addr, path string) {
	a.peerComponent.proxy(w, r, addr, path)
}

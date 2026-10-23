package deepseek

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
)

// Harness is the interactive DeepSeek agent execution coordinator.
// It manages multi-turn conversational sessions, plans and reasons with tools,
// and spreads generation and subtask workloads evenly across the cluster.
type Harness struct {
	registry   *cluster.Registry
	dispatcher *http.Client
	tools      *ToolRegistry
	loop       *agent.AgentLoop[deepSeekRequest]

	sessions map[string]*Session
	// sessionsMu protects the session map. Stats may nest session.mu beneath
	// this lock; session code must not acquire sessionsMu while holding session.mu.
	sessionsMu sync.RWMutex

	events           *hub.Hub
	mem              memory.Store
	style            string
	workflowExecutor func(context.Context, string, []scheduler.Subtask, bool) (string, error)
}

// SetWorkflowExecutor installs the shared durable workflow execution path.
func (h *Harness) SetWorkflowExecutor(executor func(context.Context, string, []scheduler.Subtask, bool) (string, error)) {
	h.workflowExecutor = executor
}

func New(registry *cluster.Registry, dispatcher *http.Client) *Harness {
	return newWithStyle(registry, dispatcher, "deepseek")
}

func newWithStyle(registry *cluster.Registry, dispatcher *http.Client, style string) *Harness {
	if dispatcher == nil {
		dispatcher = &http.Client{Timeout: 10 * time.Minute}
	}
	h := &Harness{
		registry:   registry,
		dispatcher: dispatcher,
		tools:      NewToolRegistry(),
		sessions:   make(map[string]*Session),
		events:     hub.New(),
		style:      style,
	}

	for _, registration := range defaultToolRegistrations() {
		h.tools.register(registration.tool, registration.argumentType)
	}
	h.loop = h.newAgentLoop()
	return h
}

// RegisterTool adds a tool to the harness catalog.
func (h *Harness) RegisterTool(t Tool) {
	h.tools.Register(t)
}

// SetEventsHub attaches an SSE hub to broadcast agent events cluster-wide.
func (h *Harness) SetEventsHub(events *hub.Hub) {
	h.events = events
}

// SetMemory attaches the persistent cluster memory the harness recalls from
// and learns into. Without it the harness still works, just statelessly.
func (h *Harness) SetMemory(store memory.Store) {
	h.mem = store
	if store != nil {
		registerTypedTool[recallMemoryArgs](h.tools, &RecallMemoryTool{})
		registerTypedTool[rememberFactArgs](h.tools, &RememberFactTool{})
		registerTypedTool[memoryInsightsArgs](h.tools, &MemoryInsightsTool{})
		registerTypedTool[nodeToNodeConversationArgs](h.tools, &NodeToNodeConversationTool{})
	}
}

// Memory returns the attached memory store (may be nil).
func (h *Harness) Memory() memory.Store { return h.mem }

// remember persists one agent action so future sessions can recall it and
// the scheduler can learn which node/model pairings actually work.
func (h *Harness) remember(e memory.Experience) {
	if h.mem == nil {
		return
	}
	if _, err := h.mem.Record(e); err != nil {
		logging.Errorf("deepseek", "deepseek: memory write failed: %v", err)
	}
}

// ListTools returns definitions for all available tools in the harness.
func (h *Harness) ListTools() []ToolDef {
	return h.tools.List()
}

// ExecuteTool runs a registered tool outside the harness-owned agent loop.
// It is used by other agent backends that share mAIstr0's cluster tools.
func (h *Harness) ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (*ToolResponse, error) {
	s, ok := h.GetSession(sessionID)
	if !ok {
		return nil, errors.New("session not found: " + sessionID)
	}
	s.toolMu.Lock()
	defer s.toolMu.Unlock()
	tool, ok := h.tools.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
	start := time.Now()
	output, nodeID, nodeAddr, model, err := tool.Execute(ctx, h, s, args)
	response := &ToolResponse{
		ToolCallID: generateID("call_"), Name: name, Output: output,
		NodeID: nodeID, NodeAddr: nodeAddr, Model: model,
		DurationMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		response.Error = err.Error()
	}
	argsJSON, _ := json.Marshal(args)
	call := ToolCall{Name: name, Arguments: args, RawArgs: string(argsJSON)}
	h.remember(memory.Experience{
		Kind: "tool_call", TaskType: toolTaskType(call), Description: describeToolCall(call), Prompt: string(argsJSON),
		Output: renderToolOutput(response.Output), NodeID: nodeID, Model: model,
		Tool: name, SessionID: sessionID, Success: err == nil, DurationMs: response.DurationMs,
		Error: response.Error, Tags: []string{"agent", name},
	})
	return response, err
}

// ModelInfo describes an LLM model available on a specific cluster node.
type ModelInfo struct {
	NodeID   string   `json:"node_id"`
	NodeAddr string   `json:"node_address"`
	Name     string   `json:"name"`
	Engine   string   `json:"engine"`
	Tags     []string `json:"tags"`
	SizeGB   float64  `json:"size_gb,omitempty"`
	Default  bool     `json:"default"`
	Healthy  bool     `json:"healthy"`
}

// ListClusterModels returns every available model across all healthy nodes.
func (h *Harness) ListClusterModels() []ModelInfo {
	nodes := h.registry.Active()
	var models []ModelInfo
	for _, n := range nodes {
		for _, m := range n.Models {
			models = append(models, ModelInfo{
				NodeID:   n.ID,
				NodeAddr: n.Address,
				Name:     m.Name,
				Engine:   m.Engine,
				Tags:     m.Tags,
				SizeGB:   m.SizeGB,
				Default:  m.Name == n.DefaultModel,
				Healthy:  n.Healthy,
			})
		}
	}
	return models
}

// CreateSession initializes a new interactive conversation session.
func (h *Harness) CreateSession(title string, cfg SessionConfig) *Session {
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()

	id := generateID("ses_")
	if title == "" {
		title = "Interactive Session " + time.Now().Format("Jan 02 15:04")
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 8
	}

	s := &Session{
		ID:        id,
		Title:     title,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Messages:  make([]Message, 0),
		Memory:    make(map[string]string),
		Config:    cfg,
		Active:    false,
	}
	h.sessions[id] = s
	return s
}

// RestoreSession adds a saved session to the harness without replacing an
// existing session with the same ID.
func (h *Harness) RestoreSession(snapshot SessionSnapshot) error {
	if snapshot.ID == "" {
		return errors.New("session ID is required")
	}
	if snapshot.Config.MaxSteps <= 0 {
		snapshot.Config.MaxSteps = 8
	}
	if snapshot.Memory == nil {
		snapshot.Memory = make(map[string]string)
	}
	if snapshot.Messages == nil {
		snapshot.Messages = make([]Message, 0)
	}
	snapshot.Messages = append([]Message(nil), snapshot.Messages...)
	memory := make(map[string]string, len(snapshot.Memory))
	for key, value := range snapshot.Memory {
		memory[key] = value
	}
	session := &Session{
		ID: snapshot.ID, Title: snapshot.Title, CreatedAt: snapshot.CreatedAt,
		UpdatedAt: snapshot.UpdatedAt, Messages: snapshot.Messages, Memory: memory,
		Config: snapshot.Config, Active: false,
	}

	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	if _, exists := h.sessions[session.ID]; exists {
		return fmt.Errorf("session %q already exists", session.ID)
	}
	h.sessions[session.ID] = session
	return nil
}

// GetSession fetches a session by its unique ID.
func (h *Harness) GetSession(id string) (*Session, bool) {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	s, ok := h.sessions[id]
	return s, ok
}

// ListSessions returns all active and past sessions ordered by last update.
func (h *Harness) ListSessions() []*Session {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	list := make([]*Session, 0, len(h.sessions))
	for _, s := range h.sessions {
		list = append(list, s)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})
	return list
}

// DeleteSession removes a session from memory.
func (h *Harness) DeleteSession(id string) bool {
	h.InterruptSession(id)
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	if _, ok := h.sessions[id]; ok {
		delete(h.sessions, id)
		return true
	}
	return false
}

// InterruptSession aborts in-flight reasoning and tools for a session.
func (h *Harness) InterruptSession(sessionID string) bool {
	return h.loop.InterruptSession(sessionID)
}

// Stats returns runtime statistics for the interactive agent harness.
func (h *Harness) Stats() AgentStats {
	h.sessionsMu.RLock()
	totalSessions := len(h.sessions)
	activeSessions := 0
	totalMessages := 0
	totalToolCalls := 0
	for _, s := range h.sessions {
		s.mu.RLock()
		if s.Active {
			activeSessions++
		}
		totalMessages += len(s.Messages)
		for _, m := range s.Messages {
			totalToolCalls += len(m.ToolCalls)
		}
		s.mu.RUnlock()
	}
	h.sessionsMu.RUnlock()

	nodes := h.registry.Active()
	loadMap := make(map[string]int, len(nodes))
	for _, n := range nodes {
		loadMap[n.ID] = n.ActiveTasks
	}

	return AgentStats{
		TotalSessions:  totalSessions,
		ActiveSessions: activeSessions,
		TotalMessages:  totalMessages,
		TotalToolCalls: totalToolCalls,
		DispatchedLoad: loadMap,
	}
}

// SendMessage runs the full interactive DeepSeek agent loop for an incoming user message.
func (h *Harness) SendMessage(ctx context.Context, sessionID string, userText string, streamChan chan<- StreamEvent) (*Message, error) {
	s, ok := h.GetSession(sessionID)
	if !ok {
		return nil, errors.New("session not found: " + sessionID)
	}
	var agentEvents chan agent.StreamEvent
	var relayDone chan struct{}
	if streamChan != nil {
		agentEvents = make(chan agent.StreamEvent, cap(streamChan))
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			for event := range agentEvents {
				select {
				case streamChan <- FromAgentStreamEvent(event):
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	message, err := h.loop.Run(ctx, deepSeekConversation{session: s}, userText, agentEvents)
	if agentEvents != nil {
		close(agentEvents)
		<-relayDone
	}
	if message == nil {
		return nil, err
	}
	converted := FromAgentMessage(*message)
	return &converted, err
}

func toolTaskType(tc ToolCall) string {
	if tc.Arguments == nil {
		return ""
	}
	if v, ok := tc.Arguments["task_type"].(string); ok {
		return v
	}
	return ""
}

func describeToolCall(tc ToolCall) string {
	for _, key := range []string{"message", "prompt", "task", "topic", "expression", "query"} {
		if v, ok := tc.Arguments[key].(string); ok && v != "" {
			return tc.Name + ": " + v
		}
	}
	return tc.Name
}

func renderToolOutput(out any) string {
	if out == nil {
		return ""
	}
	if s, ok := out.(string); ok {
		return s
	}
	buf, err := json.Marshal(out)
	if err != nil {
		return fmt.Sprintf("%v", out)
	}
	return string(buf)
}

// lastUserMessage returns the most recent user turn, used as the recall key.
func lastUserMessage(s *Session) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.Messages) - 1; i >= 0; i-- {
		if s.Messages[i].Role == RoleUser {
			return s.Messages[i].Content
		}
	}
	return ""
}

// selectCoordinator chooses the optimal node and model to orchestrate reasoning.
func (h *Harness) selectCoordinator(s *Session) (*cluster.NodeStatus, string, error) {
	nodes := h.registry.Active()
	if len(nodes) == 0 {
		return nil, "", errors.New("no active nodes in cluster")
	}

	// The elected leader is the coordinator role. A requested model may tune
	// which model runs there, but must never move coordinator work to a worker
	// node that does not hold that role.
	for _, n := range nodes {
		if n.Leader && len(n.Models) > 0 {
			if s.Config.CoordinatorModel != "" {
				for _, m := range n.Models {
					if m.Name == s.Config.CoordinatorModel {
						return &n, m.Name, nil
					}
				}
			}
			model := pickBestCoordinatorModel(n.Models, n.DefaultModel)
			if model != "" {
				return &n, model, nil
			}
		}
	}

	// Fallback for a single-node cluster before leadership is established.
	var bestNode *cluster.NodeStatus
	bestScore := -1.0
	bestModel := ""

	for i := range nodes {
		n := &nodes[i]
		if len(n.Models) == 0 {
			continue
		}
		m := pickBestCoordinatorModel(n.Models, n.DefaultModel)
		if m == "" {
			continue
		}
		score := n.FastScore - float64(n.ActiveTasks)*8.0 -
			cluster.GPUUtilizationPenalty(n.Hardware) - cluster.NPUUtilizationPenalty(n.Hardware)
		if score > bestScore {
			bestScore = score
			bestNode = n
			bestModel = m
		}
	}

	if bestNode != nil && bestModel != "" {
		return bestNode, bestModel, nil
	}

	// 4. Default to first node's first model or simulated
	firstNode := &nodes[0]
	if len(firstNode.Models) > 0 {
		return firstNode, firstNode.Models[0].Name, nil
	}

	return firstNode, "simulated-general", nil
}

func pickBestCoordinatorModel(models []engine.Model, defaultModel string) string {
	bestName := ""
	bestScore := -1.0
	for _, m := range models {
		score := 0.0
		if m.Name == defaultModel {
			score += 5.0
		}
		for _, tag := range m.Tags {
			switch tag {
			case "reasoning":
				score += 10.0
			case "chat":
				score += 6.0
			case "general":
				score += 4.0
			case "code":
				score += 3.0
			case "fast":
				score += 2.0
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = m.Name
		}
	}
	return bestName
}

// buildPrompt constructs the selected harness prompt incorporating tools and history.
func (h *Harness) buildPrompt(s *Session) string {
	return h.buildPromptWithTools(s, h.ListTools())
}

func (h *Harness) buildPromptWithTools(s *Session, tools []ToolDef) string {
	var sb strings.Builder

	// System prompt
	sb.WriteString("<|im_start|>system\n")
	if s.Config.SystemPrompt != "" {
		sb.WriteString(s.Config.SystemPrompt + "\n\n")
	} else if h.style == "deepseek" {
		sb.WriteString("You are DeepSeek, an intelligent reasoning coordinator running on the mAIstr0 multi-node LLM cluster orchestrator.\n")
		sb.WriteString("Use deliberate reasoning, and use the available tools whenever cluster work is required.\n\n")
	} else {
		sb.WriteString("You are DeepSeek, an intelligent interactive AI coordinator running on the mAIstr0 multi-node LLM orchestrator.\n")
		sb.WriteString("You have access to distributed worker nodes across the cluster to execute LLM queries, evaluate math, and fan out parallel subtasks.\n\n")
	}

	// Tools schema
	toolsJSON, _ := json.MarshalIndent(tools, "", "  ")
	sb.WriteString("You have access to the following tools:\n<tools>\n")
	sb.Write(toolsJSON)
	sb.WriteString("\n</tools>\n\n")

	if h.style == "deepseek" {
		sb.WriteString("Reason inside <think>...</think> when useful. When you need to execute a tool, output ONLY this tool call block:\n")
	} else {
		sb.WriteString("When you need to execute a tool, output ONLY the tool call block in this format:\n")
	}
	sb.WriteString("<tool_call>\n{\"name\": \"tool_name\", \"arguments\": {\"arg_name\": \"value\"}}\n</tool_call>\n\n")
	sb.WriteString("Guidelines:\n")
	sb.WriteString("- To check cluster status and compute resources, call `cluster_status`.\n")
	sb.WriteString("- To discover models hosted on nodes, call `list_node_models`.\n")
	sb.WriteString("- To hold an interactive multi-turn conversation with a specific node's LLM, call `node_converse` with node_id, message, and dialogue_id.\n")
	sb.WriteString("- To collaboratively solve and iteratively refine a complex task with a worker node's LLM across multiple rounds, call `node_collaborate`.\n")
	sb.WriteString("- To converse with the least-loaded matching node in the cluster, call `cluster_converse`.\n")
	sb.WriteString("- To directly query a node's LLM once, call `node_llm_query` or `cluster_llm_query`.\n")
	sb.WriteString("- To fan out parallel tasks simultaneously across multiple nodes, call `cluster_parallel_dispatch`.\n")
	sb.WriteString("- For multi-step work that needs durable progress, dependencies, validation, or review, call `execute_workflow` instead of coordinating worker calls manually.\n")
	sb.WriteString("- To perform arithmetic or math formulas, call `eval_expression`.\n")
	sb.WriteString("- To have two worker node LLMs debate or design something together, call `node_to_node_conversation`.\n")
	sb.WriteString("- To look up what the cluster learned from past runs, call `recall_memory` or `memory_insights`.\n")
	sb.WriteString("- To durably save something worth remembering next session, call `remember_fact`.\n")
	sb.WriteString("- If no tool call is needed, provide your direct response to the user.\n")

	// Recalled experience is injected so the agent starts each step already
	// knowing what worked, what failed, and which node handled it best.
	if h.mem != nil {
		if brief := h.mem.ContextBrief(lastUserMessage(s), "", 5); brief != "" {
			sb.WriteString("\n")
			sb.WriteString(brief)
			sb.WriteString("Use this recalled experience: repeat what worked, avoid what failed, and do not re-derive answers you already have.\n")
		}
	}

	sb.WriteString("<|im_end|>\n")

	// Message history
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, msg := range s.Messages {
		switch msg.Role {
		case RoleUser:
			sb.WriteString(fmt.Sprintf("<|im_start|>user\n%s<|im_end|>\n", msg.Content))
		case RoleAssistant:
			sb.WriteString("<|im_start|>assistant\n")
			if msg.Thought != "" {
				sb.WriteString(msg.Thought + "\n")
			}
			if len(msg.ToolCalls) > 0 {
				for _, tc := range msg.ToolCalls {
					callJSON, _ := json.Marshal(map[string]any{
						"name":      tc.Name,
						"arguments": tc.Arguments,
					})
					sb.WriteString(fmt.Sprintf("<tool_call>\n%s\n</tool_call>\n", string(callJSON)))
				}
			} else if msg.Content != "" {
				sb.WriteString(msg.Content + "\n")
			}
			sb.WriteString("<|im_end|>\n")
		case RoleTool:
			if msg.ToolResponse != nil {
				respJSON, _ := json.Marshal(map[string]any{
					"name":   msg.ToolResponse.Name,
					"output": msg.ToolResponse.Output,
					"error":  msg.ToolResponse.Error,
					"node":   msg.ToolResponse.NodeID,
				})
				sb.WriteString(fmt.Sprintf("<tool_response>\n%s\n</tool_response>\n", string(respJSON)))
			}
		}
	}

	sb.WriteString("<|im_start|>assistant\n")
	return sb.String()
}

// pickBestNodeForQuery picks the least-loaded matching node in the cluster.
func (h *Harness) pickBestNodeForQuery(taskType, modelPref string, nodes []cluster.NodeStatus) (*cluster.NodeStatus, string, float64) {
	simulatedLoad := make(map[string]int, len(nodes))
	for _, n := range nodes {
		simulatedLoad[n.ID] = n.ActiveTasks
	}
	st := scheduler.Subtask{
		ID:          "query",
		Description: "llm_query",
		TaskType:    taskType,
	}
	return h.pickBestNodeForSubtask(st, modelPref, nodes, simulatedLoad)
}

// pickBestNodeForSubtask scores nodes considering model match, hardware score, and load penalty.
func (h *Harness) pickBestNodeForSubtask(st scheduler.Subtask, modelPref string, nodes []cluster.NodeStatus, load map[string]int) (*cluster.NodeStatus, string, float64) {
	var best *cluster.NodeStatus
	bestModel := ""
	bestScore := -99999.0

	for i := range nodes {
		n := &nodes[i]
		if !n.Healthy || len(n.Models) == 0 {
			continue
		}
		model, matchScore := matchModel(st, modelPref, n.Models, n.DefaultModel)
		if model == "" {
			continue
		}
		hwScore := n.Hardware.Score
		loadPenalty := float64(load[n.ID]) * 8.0
		acceleratorPenalty := cluster.GPUUtilizationPenalty(n.Hardware) + cluster.NPUUtilizationPenalty(n.Hardware)

		// Past results shift routing: pairings that historically succeeded
		// here get promoted, ones that failed or ran slow get demoted.
		learned := 0.0
		if h.mem != nil {
			learned = h.mem.LearnedBias(n.ID, model, st.TaskType)
		}

		totalScore := matchScore*10.0 + hwScore*0.3 - loadPenalty - acceleratorPenalty + learned
		if totalScore > bestScore {
			bestScore = totalScore
			best = n
			bestModel = model
		}
	}
	return best, bestModel, bestScore
}

func matchModel(st scheduler.Subtask, modelPref string, models []engine.Model, defaultModel string) (string, float64) {
	bestName := ""
	bestScore := -1.0

	for _, m := range models {
		score := 0.0
		if modelPref != "" && strings.EqualFold(m.Name, modelPref) {
			score += 15.0
		}
		if m.Name == defaultModel {
			score += 1.0
		}
		for _, tag := range m.Tags {
			if tag == st.TaskType {
				score += 3.0
			} else if tag == "general" {
				score += 0.5
			} else if tag == "fast" {
				score += 0.25
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = m.Name
		}
	}
	if bestName == "" && len(models) > 0 {
		return models[0].Name, 0.1
	}
	return bestName, bestScore
}

// dispatchToNode sends an execution payload to a cluster node's /execute endpoint.
func (h *Harness) dispatchToNode(ctx context.Context, address, model, prompt string) (string, error) {
	// If the prompt is formatted as ChatML, try /chat or /execute
	body, err := json.Marshal(map[string]string{
		"model":  model,
		"prompt": prompt,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/execute", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.dispatcher.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return "", fmt.Errorf("node returned %s: %s", resp.Status, string(errBody))
	}

	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	return out.Output, nil
}

// DispatchChatToNode sends structured multi-turn messages to a node's /chat endpoint.
func (h *Harness) DispatchChatToNode(ctx context.Context, address, model string, messages []engine.ChatMessage) (string, error) {
	body, err := json.Marshal(map[string]any{
		"model":    model,
		"messages": messages,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.dispatcher.Do(req)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		var out struct {
			Message engine.ChatMessage `json:"message"`
			Error   string             `json:"error,omitempty"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&out); decodeErr == nil {
			if out.Error != "" {
				return "", errors.New(out.Error)
			}
			return out.Message.Content, nil
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Fallback to formatted prompt on /execute
	var sb strings.Builder
	for _, m := range messages {
		sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
	}
	sb.WriteString("<|im_start|>assistant\n")
	return h.dispatchToNode(ctx, address, model, sb.String())
}

// NodeDialogueResponse holds the outcome of a conversational turn with a node.
type NodeDialogueResponse struct {
	DialogueID string               `json:"dialogue_id"`
	NodeID     string               `json:"node_id"`
	Model      string               `json:"model"`
	Turn       int                  `json:"turn"`
	Reply      string               `json:"reply"`
	Messages   []engine.ChatMessage `json:"messages"`
	DurationMs int64                `json:"duration_ms"`
	Error      string               `json:"error,omitempty"`
}

// SendNodeDialogueMessage sends a message in a dialogue with a node's LLM, continuing the conversation.
func (h *Harness) SendNodeDialogueMessage(ctx context.Context, node cluster.NodeStatus, dialogueID, model, content, role string) (NodeDialogueResponse, error) {
	if dialogueID == "" {
		dialogueID = fmt.Sprintf("dlg-%s-%d", node.ID, time.Now().UnixNano()%1000000)
	}
	if role == "" {
		role = "user"
	}

	body, err := json.Marshal(map[string]string{
		"role":    role,
		"content": content,
	})
	if err != nil {
		return NodeDialogueResponse{}, err
	}

	url := fmt.Sprintf("%s/dialogues/%s/messages", node.Address, dialogueID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return NodeDialogueResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	h.registry.IncrementLoad(node.ID, 1)
	defer h.registry.IncrementLoad(node.ID, -1)

	resp, err := h.dispatcher.Do(req)
	if err != nil {
		return NodeDialogueResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var rawMap map[string]any
		if decodeErr := json.NewDecoder(resp.Body).Decode(&rawMap); decodeErr == nil {
			reply := ""
			if r, ok := rawMap["reply"].(string); ok && r != "" {
				reply = r
			} else if o, ok := rawMap["output"].(string); ok && o != "" {
				reply = o
			}
			errStr := ""
			if e, ok := rawMap["error"].(string); ok {
				errStr = e
			}
			if errStr != "" {
				return NodeDialogueResponse{}, errors.New(errStr)
			}
			turn := 1
			if t, ok := rawMap["turn"].(float64); ok && t > 0 {
				turn = int(t)
			}
			did := dialogueID
			if d, ok := rawMap["dialogue_id"].(string); ok && d != "" {
				did = d
			}
			mod := model
			if m, ok := rawMap["model"].(string); ok && m != "" {
				mod = m
			}
			return NodeDialogueResponse{
				DialogueID: did,
				NodeID:     node.ID,
				Model:      mod,
				Turn:       turn,
				Reply:      reply,
				Messages: []engine.ChatMessage{
					{Role: role, Content: content},
					{Role: "assistant", Content: reply},
				},
			}, nil
		}
	}

	// Fallback to direct chat if node dialogue endpoint failed
	start := time.Now()
	reply, chatErr := h.DispatchChatToNode(ctx, node.Address, model, []engine.ChatMessage{
		{Role: role, Content: content},
	})
	elapsed := time.Since(start).Milliseconds()
	if chatErr != nil {
		return NodeDialogueResponse{}, chatErr
	}

	return NodeDialogueResponse{
		DialogueID: dialogueID,
		NodeID:     node.ID,
		Model:      model,
		Turn:       1,
		Reply:      reply,
		Messages: []engine.ChatMessage{
			{Role: role, Content: content},
			{Role: "assistant", Content: reply},
		},
		DurationMs: elapsed,
	}, nil
}

func generateID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

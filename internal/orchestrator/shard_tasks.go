package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/workpool"
)

func shardSchedulerNodes(nodes []cluster.NodeStatus, groups []ShardGroup) []cluster.NodeStatus {
	if len(groups) == 0 {
		return nodes
	}
	names := make(map[string]bool, len(groups))
	for _, group := range groups {
		names[strings.ToLower(group.Model.Name)] = true
	}
	for i := range nodes {
		models := make([]engine.Model, 0, len(nodes[i].Models))
		for _, model := range nodes[i].Models {
			if !names[strings.ToLower(model.Name)] {
				models = append(models, model)
			}
		}
		nodes[i].Models = models
	}
	for _, group := range groups {
		if group.State != ShardGroupReady {
			continue
		}
		groupCapabilities := shardGroupHardwareCapabilities(group)
		for _, head := range group.Members {
			if head.Role != "head" {
				continue
			}
			for i := range nodes {
				if nodes[i].ID != head.NodeID || nodes[i].Address != head.Address || !nodes[i].Healthy {
					continue
				}
				name := "shard-group:" + group.ID
				weights := group.Model.FileSizeBytes
				if group.Model.WeightsSizeBytes != 0 {
					weights = group.Model.WeightsSizeBytes
				}
				nodes[i].Models = append(nodes[i].Models, engine.Model{
					Name: group.Model.Name, Engine: name, Tags: shardTextTags(group.Model.Name),
					SizeGB:    float64(weights) / (1 << 30),
					Protocols: []string{"openai"}, ContextLength: group.Model.RequestedContext,
					Quantization: group.Model.Quantization,
				})
				nodes[i].Engines = append(append([]engine.Status(nil), nodes[i].Engines...), engine.Status{Name: name, Ready: true, Protocols: []string{"openai"}})
				derived := cluster.NodeStatus{Models: []engine.Model{nodes[i].Models[len(nodes[i].Models)-1]}}.EffectiveCapabilities()
				capabilities := append([]string(nil), nodes[i].EffectiveCapabilities()...)
				for _, capability := range append(derived, groupCapabilities...) {
					if !containsCapability(capabilities, capability) {
						capabilities = append(capabilities, capability)
					}
				}
				nodes[i].Capabilities = capabilities
			}

		}
	}
	return nodes
}

func shardGroupHardwareCapabilities(group ShardGroup) []string {
	var capabilities []string
	for _, node := range group.Plan.Nodes {
		for _, device := range node.Devices {
			switch strings.ToLower(device.Kind) {
			case "gpu":
				capabilities = appendUniqueString(capabilities, "gpu")
				capabilities = appendUniqueString(capabilities, "fast")
			case "npu":
				capabilities = appendUniqueString(capabilities, "npu")
				capabilities = appendUniqueString(capabilities, "fast")
			}
		}
	}
	return capabilities
}

func shardTextTags(name string) []string {
	tags := []string{"general", "sharded", "creative", "summarize", "translate"}
	lower := strings.ToLower(name)
	if strings.Contains(lower, "qwen") || strings.Contains(lower, "coder") || strings.Contains(lower, "phi4") {
		tags = append(tags, "code", "math")
	}
	return tags
}

func containsCapability(capabilities []string, target string) bool {
	for _, capability := range capabilities {
		if strings.EqualFold(capability, target) {
			return true
		}
	}
	return false
}

func (c *nodeClient) generateShardGroup(ctx context.Context, group ShardGroup, address, prompt string, temperature float64, maxTokens int) (string, error) {
	return c.generateShardGroupWithIDs(ctx, group, address, prompt, temperature, maxTokens, "", "")
}

func (c *nodeClient) generateShardGroupWithIDs(ctx context.Context, group ShardGroup, address, prompt string, temperature float64, maxTokens int, requestID, attemptID string) (output string, resultErr error) {
	started := time.Now()
	defer func() {
		var timeout interface{ Timeout() bool }
		if resultErr != nil && (errors.Is(resultErr, context.DeadlineExceeded) || (errors.As(resultErr, &timeout) && timeout.Timeout())) {
			resultErr = fmt.Errorf("shard group %s model %s did not complete generation within %s; check head engine logs for stalled or slow inference and increase dispatch_timeout_seconds if needed: %w",
				group.ID, group.Model.Name, time.Since(started).Round(time.Millisecond), resultErr)
		}
	}()
	// Include the group queue in the generation budget, not just the HTTP call.
	if c.dispatcher.Timeout > 0 {
		bounded, cancel := context.WithTimeout(ctx, c.dispatcher.Timeout)
		defer cancel()
		ctx = bounded
	}
	if group.State != ShardGroupReady {
		return "", fmt.Errorf("shard group %s is %s; a ready group is required", group.ID, group.State)
	}
	var head ShardGroupMember
	for _, member := range group.Members {
		if member.Role == "head" {
			head = member
			break
		}
	}
	node, ok := c.shards.registry.Get(head.NodeID)
	if !ok || !node.Healthy || node.Address != head.Address || head.Address != address || head.EngineID == "" {
		return "", errors.New("shard group head is no longer available")
	}
	release, err := c.shards.AcquireRequest(ctx, group.ID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("shard group %s model %s timed out waiting for its request slot after %s (dispatch_timeout_seconds controls the generation budget): %w",
				group.ID, group.Model.Name, time.Since(started).Round(time.Millisecond), err)
		}
		return "", fmt.Errorf("acquire shard group request: %w", err)
	}
	defer release()
	if err := c.VerifyModel(ctx, head.Address, head.EngineID, group.Model, false); err != nil {
		return "", fmt.Errorf("shard head model does not match saved group; stop and start the group to restore its model: %w", err)
	}
	payload := map[string]any{
		"model": group.Model.Name, "messages": []map[string]string{{"role": "user", "content": prompt}},
		"stream": true, "temperature": temperature,
	}
	if maxTokens > 0 {
		payload["max_tokens"] = maxTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode shard generation: %w", err)
	}
	endpoint := strings.TrimRight(head.Address, "/") + "/api/engines/managed/" + url.PathEscape(head.EngineID) + "/proxy/v1/chat/completions"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create shard generation: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	if requestID != "" {
		request.Header.Set(httpx.RequestIDHeader, requestID)
		request.Header.Set(inferroute.HeaderRequestID, requestID)
	}
	if attemptID != "" {
		request.Header.Set(inferroute.HeaderAttemptID, attemptID)
	}
	request, err = withRequestIDHeaders(request)
	if err != nil {
		return "", fmt.Errorf("prepare shard request identity: %w", err)
	}
	response, err := c.dispatcher.Do(request)
	if err != nil {
		return "", fmt.Errorf("shard generation request: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if err != nil {
			return "", fmt.Errorf("read shard generation error: %w", err)
		}
		return "", &nodeResponseError{statusCode: response.StatusCode, body: strings.TrimSpace(string(body))}
	}
	if strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		lastProgress := time.Time{}
		return readShardGenerationStream(response.Body, func(chunks, outputBytes, reasoningBytes int, done bool) {
			now := time.Now()
			if done || lastProgress.IsZero() || now.Sub(lastProgress) >= 15*time.Second {
				slog.InfoContext(ctx, "shard generation progress", "component", "orchestrator",
					"request_id", request.Header.Get(httpx.RequestIDHeader), "group_id", group.ID,
					"model", group.Model.Name, "node_id", head.NodeID,
					"elapsed_ms", now.Sub(started).Milliseconds(), "chunks", chunks,
					"output_bytes", outputBytes, "reasoning_bytes", reasoningBytes, "completed", done)
				lastProgress = now
			}
		})
	}
	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, httpx.InferenceJSONBodyBytes)).Decode(&result); err != nil {
		return "", fmt.Errorf("decode shard generation: %w", err)
	}
	if len(result.Choices) == 0 || strings.TrimSpace(result.Choices[0].Message.Content) == "" {
		return "", errors.New("shard generation returned no assistant content")
	}
	return result.Choices[0].Message.Content, nil
}

func readShardGenerationStream(body io.Reader, progress func(chunks, outputBytes, reasoningBytes int, done bool)) (string, error) {
	limited := &io.LimitedReader{R: body, N: httpx.InferenceJSONBodyBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var output strings.Builder
	var data strings.Builder
	chunks, reasoningBytes := 0, 0
	done := false
	consume := func() error {
		payload := strings.TrimSpace(data.String())
		defer data.Reset()
		if payload == "" {
			return nil
		}
		if payload == "[DONE]" {
			done = true
			return nil
		}
		var event struct {
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return fmt.Errorf("decode shard stream event: %w", err)
		}
		if len(event.Error) > 0 && string(event.Error) != "null" {
			return errors.New("shard head reported a generation stream error; check head engine logs")
		}
		for _, choice := range event.Choices {
			if choice.FinishReason == "length" {
				return errors.New("shard generation reached its token limit before completing; reduce the requested output or use a model with more suitable output limits")
			}
			output.WriteString(choice.Delta.Content)
			reasoningBytes += len(choice.Delta.ReasoningContent)
		}
		chunks++
		progress(chunks, output.Len(), reasoningBytes, false)
		return nil
	}
	for scanner.Scan() {
		if limited.N <= 0 {
			return "", errors.New("shard generation stream exceeds the response size limit")
		}
		line := scanner.Text()
		if line == "" {
			if err := consume(); err != nil {
				return "", err
			}
			if done {
				break
			}
		} else if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read shard generation stream: %w", err)
	}
	if !done && data.Len() > 0 {
		if err := consume(); err != nil {
			return "", err
		}
	}
	if !done {
		return "", errors.New("shard generation stream ended before completion")
	}
	if strings.TrimSpace(output.String()) == "" {
		return "", errors.New("shard generation returned no assistant content")
	}
	progress(chunks, output.Len(), reasoningBytes, true)
	return output.String(), nil
}

func (d *dispatcherComponent) reserveTaskCapacity(ctx context.Context, assignment scheduler.Assignment) (*dispatchReservation, error) {
	if !strings.HasPrefix(assignment.Engine, "shard-group:") {
		return reserveDispatchCapacity(ctx, d.capacityGate, d.registry, assignment)
	}
	gate, ok := d.capacityGate.(*dispatchCapacityGate)
	if !ok {
		return nil, errors.New("shard task capacity gate is unavailable")
	}
	// The group already holds its member slots. Only acquire a global task slot;
	// generation serializes against other group requests through AcquireRequest.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case gate.slots <- struct{}{}:
	}
	return &dispatchReservation{
		capacity: staticRouterReservation{target: workpool.CapacityTarget{NodeID: assignment.NodeID, Address: assignment.Address}, release: func() { <-gate.slots }},
		registry: d.registry, nodeID: assignment.NodeID,
	}, nil
}

package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
)

func (c *nodeClient) NodeStatus(ctx context.Context, address string) (cluster.NodeStatus, error) {
	return c.discoveredNodeStatusContext(ctx, address)
}

// Automatic heads can be reconfigured while stopped. Operator-defined engine
// configurations are immutable for the lifetime of their supervisor.
func (c *nodeClient) VerifyModel(ctx context.Context, address, id string, expected shard.ModelProfile, restore bool) error {
	if id != "maistr0-auto-head" {
		return nil
	}
	readProfile := func() (shard.ModelProfile, error) {
		endpoint := managedEngineURL(address, id, "profile") + fmt.Sprintf("?requested_context=%d", expected.RequestedContext)
		response, err := c.managedRequest(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return shard.ModelProfile{}, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return shard.ModelProfile{}, managedEngineResponseError(response)
		}
		var profile shard.ModelProfile
		err = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&profile)
		return profile, err
	}
	actual, err := readProfile()
	if err != nil {
		return fmt.Errorf("read automatic head model: %w", err)
	}
	if matchingShardModel(expected, actual) {
		return nil
	}
	if !restore {
		return fmt.Errorf("expected %q (%s, %d layers), head is configured for %q (%s, %d layers)",
			expected.Name, expected.Architecture, expected.LayerCount, actual.Name, actual.Architecture, actual.LayerCount)
	}
	endpoint := strings.TrimRight(address, "/") + fmt.Sprintf("/api/engines/managed/head-models?requested_context=%d", expected.RequestedContext)
	response, err := c.managedRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("discover saved head model: %w", err)
	}
	var inventory supervisor.LocalHeadModels
	decodeErr := func() error {
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return managedEngineResponseError(response)
		}
		return json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&inventory)
	}()
	if decodeErr != nil {
		return fmt.Errorf("discover saved head model: %w", decodeErr)
	}
	for _, model := range inventory.Models {
		if !matchingShardModel(expected, model.Profile) {
			continue
		}
		payload, err := json.Marshal(map[string]any{"model_id": model.ID, "requested_context": expected.RequestedContext})
		if err != nil {
			return fmt.Errorf("encode saved head configuration: %w", err)
		}
		response, err := c.managedRequest(ctx, http.MethodPost, strings.TrimRight(address, "/")+"/api/engines/managed/configure-head", bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("restore saved head model: %w", err)
		}
		if _, err := decodeManagedEngineStatus(response); err != nil {
			return fmt.Errorf("restore saved head model: %w", err)
		}
		actual, err = readProfile()
		if err != nil {
			return fmt.Errorf("verify restored head model: %w", err)
		}
		if !matchingShardModel(expected, actual) {
			return fmt.Errorf("restored head still does not match saved model %q", expected.Name)
		}
		return nil
	}
	return fmt.Errorf("saved model %q is no longer available on the head; restore its local GGUF weights or recreate the group through the model-selection wizard", expected.Name)
}

func matchingShardModel(expected, actual shard.ModelProfile) bool {
	if !strings.EqualFold(expected.Name, actual.Name) ||
		!strings.EqualFold(expected.Architecture, actual.Architecture) ||
		expected.LayerCount != actual.LayerCount || expected.HiddenSize != actual.HiddenSize ||
		expected.KVHeads != actual.KVHeads || expected.HeadDim != actual.HeadDim {
		return false
	}
	if expected.FileSizeBytes != 0 && expected.FileSizeBytes != actual.FileSizeBytes {
		return false
	}
	if expected.WeightsSizeBytes != 0 && expected.WeightsSizeBytes != actual.WeightsSizeBytes {
		return false
	}
	if expected.Quantization != "" && !strings.EqualFold(expected.Quantization, actual.Quantization) {
		return false
	}
	if expected.VocabSize != 0 && expected.VocabSize != actual.VocabSize {
		return false
	}
	return true
}

func (c *nodeClient) List(ctx context.Context, address string) ([]supervisor.Status, error) {
	response, err := c.managedRequest(ctx, http.MethodGet, address+"/api/engines/managed", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, managedEngineResponseError(response)
	}
	var statuses []supervisor.Status
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&statuses); err != nil {
		return nil, fmt.Errorf("decode managed engine status: %w", err)
	}
	return statuses, nil
}

func (c *nodeClient) Start(
	ctx context.Context,
	address, id string,
	plan *supervisor.ShardPlan,
) (supervisor.Status, error) {
	payload, err := json.Marshal(struct {
		ShardPlan *supervisor.ShardPlan `json:"shard_plan,omitempty"`
	}{ShardPlan: plan})
	if err != nil {
		return supervisor.Status{}, fmt.Errorf("encode managed engine start request: %w", err)
	}
	endpoint := managedEngineURL(address, id, "start")
	response, err := c.managedRequest(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return supervisor.Status{}, err
	}
	return decodeManagedEngineStatus(response)
}

func (c *nodeClient) Stop(ctx context.Context, address, id string) (supervisor.Status, error) {
	endpoint := managedEngineURL(address, id, "stop")
	response, err := c.managedRequest(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return supervisor.Status{}, err
	}
	return decodeManagedEngineStatus(response)
}

func (c *nodeClient) managedRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("prepare managed engine request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := c.managed
	if method == http.MethodPost && (strings.HasSuffix(request.URL.Path, "/download-model") || strings.HasSuffix(request.URL.Path, "/install-worker")) {
		client = c.modelPull
	}
	if method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start") {
		startClient := *client
		startClient.Timeout = 11 * time.Minute
		client = &startClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("managed engine request failed: %w", err)
	}
	return response, nil
}

func managedEngineURL(address, id, operation string) string {
	return strings.TrimRight(address, "/") + "/api/engines/managed/" + url.PathEscape(id) + "/" + operation
}

func decodeManagedEngineStatus(response *http.Response) (supervisor.Status, error) {
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return supervisor.Status{}, managedEngineResponseError(response)
	}
	var status supervisor.Status
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&status); err != nil {
		return supervisor.Status{}, fmt.Errorf("decode managed engine response: %w", err)
	}
	return status, nil
}

func managedEngineResponseError(response *http.Response) error {
	message, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if err != nil {
		return fmt.Errorf("managed engine returned %s (read response: %w)", response.Status, err)
	}
	return fmt.Errorf("managed engine returned %s: %s", response.Status, strings.TrimSpace(string(message)))
}

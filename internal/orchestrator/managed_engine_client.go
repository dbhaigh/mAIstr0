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

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
)

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
	response, err := c.managed.Do(request)
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

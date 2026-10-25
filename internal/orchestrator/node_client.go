package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
)

type nodeClient struct {
	shards     *ShardManager
	client     *http.Client
	dispatcher *http.Client
	status     *http.Client
	managed    *http.Client
	modelPull  *http.Client
	sync       *http.Client
}

type nodeClientAPI interface {
	setTransport(func() *http.Transport)
	discoveredNodeStatus(string) (cluster.NodeStatus, error)
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
	streamNodeGenerate(http.ResponseWriter, *http.Request, string, string, generateAPIRequest, string)
	callNodeGenerate(string, string, string) (string, error)
	callNodeGenerateWithOptions(string, string, string, float64, int) (string, error)
	callNodeGenerateWithOptionsContext(context.Context, string, string, string, float64, int) (string, error)
	callNodeGenerateWithOptionsContextAndIdempotency(context.Context, string, string, string, float64, int, string) (string, error)
	Do(*http.Request) (*http.Response, error)
	DoDispatch(*http.Request) (*http.Response, error)
	Get(string) (*http.Response, error)
	PostDispatch(string, string, io.Reader) (*http.Response, error)
}

func newNodeClient(client, dispatcher, status *http.Client) *nodeClient {
	return &nodeClient{
		client: clientWithRequestID(client), dispatcher: clientWithRequestID(dispatcher),
		status: clientWithRequestID(status), managed: clientWithRequestID(httpx.NewClient(httpx.ClientOptions{Timeout: 2 * time.Minute})),
		modelPull: clientWithRequestID(httpx.NewClient(httpx.ClientOptions{Timeout: httpx.DownloadTimeout})),
		sync:      clientWithRequestID(httpx.NewClient(httpx.ClientOptions{Timeout: httpx.InferenceTimeout})),
	}
}

func newNodeClientWithSyncTimeout(client, dispatcher, status *http.Client, timeout time.Duration) *nodeClient {
	node := newNodeClient(client, dispatcher, status)
	node.sync = clientWithRequestID(httpx.NewClient(httpx.ClientOptions{Timeout: timeout}))
	return node
}

type requestIDTransport struct {
	next http.RoundTripper
}

func (t *requestIDTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var err error
	request, err = withRequestIDHeaders(request)
	if err != nil {
		return nil, err
	}
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(request)
}

func clientWithRequestID(client *http.Client) *http.Client {
	if client == nil {
		client = httpx.NewClient(httpx.ClientOptions{})
	}
	clone := *client
	if _, alreadyWrapped := clone.Transport.(*requestIDTransport); !alreadyWrapped {
		clone.Transport = &requestIDTransport{next: clone.Transport}
	}
	return &clone
}

func withRequestIDHeaders(request *http.Request) (*http.Request, error) {
	requestID := request.Header.Get(httpx.RequestIDHeader)
	if requestID == "" {
		requestID = request.Header.Get(inferroute.HeaderRequestID)
	}
	if requestID == "" {
		requestID = httpx.RequestIDFromContext(request.Context())
	}
	if requestID == "" {
		var err error
		requestID, err = httpx.NewRequestID()
		if err != nil {
			return nil, fmt.Errorf("generate node request ID: %w", err)
		}
	}
	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	if request.Header.Get(httpx.RequestIDHeader) == "" {
		request.Header.Set(httpx.RequestIDHeader, requestID)
	}
	if request.Header.Get(inferroute.HeaderRequestID) == "" {
		request.Header.Set(inferroute.HeaderRequestID, requestID)
	}
	return request, nil
}

func (c *nodeClient) Do(request *http.Request) (*http.Response, error) {
	return c.client.Do(request)
}

func (c *nodeClient) DoDispatch(request *http.Request) (*http.Response, error) {
	return c.dispatcher.Do(request)
}

func (c *nodeClient) DoModelPull(request *http.Request) (*http.Response, error) {
	if c.modelPull == nil {
		return nil, errors.New("model pull client is unavailable")
	}
	return c.modelPull.Do(request)
}

func (c *nodeClient) Get(url string) (*http.Response, error) {
	return c.client.Get(url)
}

func (c *nodeClient) PostDispatch(url, contentType string, body io.Reader) (*http.Response, error) {
	return c.dispatcher.Post(url, contentType, body)
}

func (c *nodeClient) setTransport(newTransport func() *http.Transport) {
	transport := &requestIDTransport{next: newTransport()}
	c.client.Transport = transport
	c.dispatcher.Transport = transport
	c.status.Transport = transport
	c.managed.Transport = transport
	if c.modelPull != nil {
		c.modelPull.Transport = transport
	}
	if c.sync != nil {
		c.sync.Transport = transport
	}
}

func (c *nodeClient) discoveredNodeStatus(addr string) (cluster.NodeStatus, error) {
	return c.discoveredNodeStatusContext(context.Background(), addr)
}

func (c *nodeClient) discoveredNodeStatusContext(ctx context.Context, addr string) (cluster.NodeStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/status", nil)
	if err != nil {
		return cluster.NodeStatus{}, err
	}
	resp, err := c.status.Do(req)
	if err != nil {
		return cluster.NodeStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusForbidden {
			return cluster.NodeStatus{}, fmt.Errorf(
				"node status returned %s: peer rejected authorization; verify mutual pairing and the advertised HTTPS address",
				resp.Status,
			)
		}
		return cluster.NodeStatus{}, errors.New("node status returned " + resp.Status)
	}
	var status cluster.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return cluster.NodeStatus{}, err
	}
	return status, nil
}

func (c *nodeClient) streamNodeGenerate(w http.ResponseWriter, r *http.Request, address, model string, genReq generateAPIRequest, prompt string) {
	if c.shards != nil {
		if group, found := c.shards.GroupForModel(model); found {
			output, err := c.generateShardGroup(r.Context(), group, address, prompt, genReq.Temperature, genReq.MaxTokens)
			if err != nil {
				writeJSON(w, http.StatusBadGateway, generateAPIResponse{Error: err.Error()})
				return
			}
			data, err := json.Marshal(map[string]string{"output": output})
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			if err := httpx.NewStreamWriter(w).WriteSSEData(data); err != nil {
				logging.Errorf("orchestrator", "orchestrator: shard generation stream failed: %v", err)
			}
			return
		}
	}
	body, err := marshalGeneratePayload(model, prompt, genReq.Temperature, genReq.MaxTokens)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
		return
	}
	nodeReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, address+"/generate?stream=true", bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
		return
	}
	nodeReq.Header.Set("Content-Type", "application/json")
	resp, err := c.dispatcher.Do(nodeReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, generateAPIResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		writeJSON(w, resp.StatusCode, generateAPIResponse{Error: strings.TrimSpace(string(errBody))})
		return
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		var out generateAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			writeJSON(w, http.StatusBadGateway, generateAPIResponse{Error: "invalid response from node: " + err.Error()})
			return
		}
		data, err := json.Marshal(map[string]string{"output": out.Output})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		stream := httpx.NewStreamWriter(w)
		if err := stream.WriteSSEData(data); err != nil {
			return
		}
		if out.Error != "" {
			errData, _ := json.Marshal(map[string]string{"error": out.Error})
			if err := stream.WriteSSEData(errData); err != nil {
				return
			}
		}
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	stream := httpx.NewStreamWriter(w)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		if err := stream.WriteLine(scanner.Text()); err != nil {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		logging.Errorf("orchestrator", "orchestrator: stream from node %s failed: %v", address, err)
	}
}

func (c *nodeClient) callNodeGenerate(nodeAddr, model, prompt string) (string, error) {
	return c.callNodeGenerateWithOptions(nodeAddr, model, prompt, 0, 0)
}

func (c *nodeClient) callNodeGenerateWithOptions(nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return c.callNodeGenerateWithOptionsContext(context.Background(), nodeAddr, model, prompt, temperature, maxTokens)
}

func (c *nodeClient) callNodeGenerateWithOptionsContext(ctx context.Context, nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return c.callNodeGenerateWithOptionsContextAndIdempotency(ctx, nodeAddr, model, prompt, temperature, maxTokens, "")
}

func (c *nodeClient) callNodeGenerateWithOptionsContextAndIdempotency(
	ctx context.Context,
	nodeAddr, model, prompt string,
	temperature float64,
	maxTokens int,
	idempotencyKey string,
) (string, error) {
	if c.shards != nil {
		if group, found := c.shards.GroupForModel(model); found {
			if idempotencyKey != "" {
				return "", errors.New("idempotency keys are not supported for managed shard generation")
			}
			return c.generateShardGroup(ctx, group, nodeAddr, prompt, temperature, maxTokens)
		}
	}
	genPayload, err := marshalGeneratePayload(model, prompt, temperature, maxTokens)
	if err != nil {
		return "", err
	}

	genReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeAddr+"/generate", bytes.NewReader(genPayload))
	if err != nil {
		return "", err
	}
	genReq.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		genReq.Header.Set(inferroute.HeaderIdempotencyKey, idempotencyKey)
	}
	resp, err := c.dispatcher.Do(genReq)
	if idempotencyKey != "" {
		if err != nil {
			if resp != nil {
				_ = resp.Body.Close()
			}
			return "", fmt.Errorf("node generate request failed: %w", err)
		}
		if resp == nil {
			return "", errors.New("node generate request returned no response")
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
			if readErr != nil {
				return "", fmt.Errorf("read node error response failed: %w", readErr)
			}
			return "", &nodeResponseError{
				statusCode: resp.StatusCode, status: resp.Status, body: strings.TrimSpace(string(errBody)),
			}
		}
		var result struct {
			Output string `json:"output"`
			Error  string `json:"error,omitempty"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", fmt.Errorf("invalid response from node: %w", err)
		}
		if result.Error != "" {
			return "", errors.New(result.Error)
		}
		return result.Output, nil
	}
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		var res struct {
			Output string `json:"output"`
			Error  string `json:"error,omitempty"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&res); decodeErr == nil {
			if res.Error != "" {
				return "", errors.New(res.Error)
			}
			return res.Output, nil
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	execPayload, _ := json.Marshal(map[string]string{
		"subtask_id": "direct-gen",
		"model":      model,
		"prompt":     prompt,
	})
	execReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeAddr+"/execute", bytes.NewReader(execPayload))
	if err != nil {
		return "", err
	}
	execReq.Header.Set("Content-Type", "application/json")
	resp, err = c.dispatcher.Do(execReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
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

type nodeResponseError struct {
	statusCode int
	status     string
	body       string
}

func (e *nodeResponseError) Error() string {
	if e.body == "" {
		return "node returned " + e.status
	}
	return "node returned " + e.status + ": " + e.body
}

func marshalGeneratePayload(model, prompt string, temperature float64, maxTokens int) ([]byte, error) {
	payload := map[string]any{"model": model, "prompt": prompt}
	if temperature != 0 {
		payload["temperature"] = temperature
	}
	if maxTokens > 0 {
		payload["max_tokens"] = maxTokens
	}
	return json.Marshal(payload)
}

type generateAPIRequest struct {
	NodeID      string  `json:"node_id,omitempty"`
	Model       string  `json:"model,omitempty"`
	Prompt      string  `json:"prompt"`
	System      string  `json:"system,omitempty"`
	TaskType    string  `json:"task_type,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

type generateAPIResponse struct {
	NodeID      string `json:"node_id"`
	NodeAddress string `json:"node_address"`
	Model       string `json:"model"`
	Output      string `json:"output"`
	DurationMs  int64  `json:"duration_ms"`
	Error       string `json:"error,omitempty"`
}

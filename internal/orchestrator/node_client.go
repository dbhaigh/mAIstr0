package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/cluster"
)

type nodeClient struct {
	client     *http.Client
	dispatcher *http.Client
	status     *http.Client
}

type nodeClientAPI interface {
	setTransport(func() *http.Transport)
	discoveredNodeStatus(string) (cluster.NodeStatus, error)
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
	streamNodeGenerate(http.ResponseWriter, *http.Request, string, string, generateAPIRequest, string)
	callNodeGenerate(string, string, string) (string, error)
	callNodeGenerateWithOptions(string, string, string, float64, int) (string, error)
	callNodeGenerateWithOptionsContext(context.Context, string, string, string, float64, int) (string, error)
	Do(*http.Request) (*http.Response, error)
	DoDispatch(*http.Request) (*http.Response, error)
	Get(string) (*http.Response, error)
	PostDispatch(string, string, io.Reader) (*http.Response, error)
}

func newNodeClient(client, dispatcher, status *http.Client) *nodeClient {
	return &nodeClient{
		client: client, dispatcher: dispatcher, status: status,
	}
}

func (c *nodeClient) Do(request *http.Request) (*http.Response, error) {
	return c.client.Do(request)
}

func (c *nodeClient) DoDispatch(request *http.Request) (*http.Response, error) {
	return c.dispatcher.Do(request)
}

func (c *nodeClient) Get(url string) (*http.Response, error) {
	return c.client.Get(url)
}

func (c *nodeClient) PostDispatch(url, contentType string, body io.Reader) (*http.Response, error) {
	return c.dispatcher.Post(url, contentType, body)
}

func (c *nodeClient) setTransport(newTransport func() *http.Transport) {
	c.client.Transport = newTransport()
	c.dispatcher.Transport = newTransport()
	c.status.Transport = newTransport()
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
		return cluster.NodeStatus{}, errors.New("node status returned " + resp.Status)
	}
	var status cluster.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return cluster.NodeStatus{}, err
	}
	return status, nil
}

func (c *nodeClient) streamNodeGenerate(w http.ResponseWriter, r *http.Request, address, model string, genReq generateAPIRequest, prompt string) {
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
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		if out.Error != "" {
			errData, _ := json.Marshal(map[string]string{"error": out.Error})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", errData)
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		if _, err := fmt.Fprintln(w, scanner.Text()); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("orchestrator: stream from node %s failed: %v", address, err)
	}
}

func (c *nodeClient) callNodeGenerate(nodeAddr, model, prompt string) (string, error) {
	return c.callNodeGenerateWithOptions(nodeAddr, model, prompt, 0, 0)
}

func (c *nodeClient) callNodeGenerateWithOptions(nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return c.callNodeGenerateWithOptionsContext(context.Background(), nodeAddr, model, prompt, temperature, maxTokens)
}

func (c *nodeClient) callNodeGenerateWithOptionsContext(ctx context.Context, nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	genPayload, err := marshalGeneratePayload(model, prompt, temperature, maxTokens)
	if err != nil {
		return "", err
	}

	genReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeAddr+"/generate", bytes.NewReader(genPayload))
	if err != nil {
		return "", err
	}
	genReq.Header.Set("Content-Type", "application/json")
	resp, err := c.dispatcher.Do(genReq)
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

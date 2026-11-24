package hermes

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	SupportedVersion     = "0.23.3"
	maximumResponseBytes = 8 << 20
	maximumEventLine     = 1 << 20
)

type httpStatusError struct {
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%v: HTTP %d", ErrServerRequest, e.status)
}

func (e *httpStatusError) Unwrap() error { return ErrServerRequest }

type capabilities struct {
	Object   string `json:"object"`
	Platform string `json:"platform"`
	Auth     struct {
		Type     string `json:"type"`
		Required bool   `json:"required"`
	} `json:"auth"`
	Features struct {
		RunSubmission   bool `json:"run_submission"`
		RunStatus       bool `json:"run_status"`
		RunEventsSSE    bool `json:"run_events_sse"`
		RunStop         bool `json:"run_stop"`
		RunsIdempotency struct {
			Supported bool `json:"supported"`
			Durable   bool `json:"durable"`
		} `json:"runs_idempotency"`
	} `json:"features"`
}

type runSubmission struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type runStatus struct {
	Object    string `json:"object"`
	RunID     string `json:"run_id"`
	Status    string `json:"status"`
	SessionID string `json:"session_id"`
	Output    string `json:"output"`
}

type runEvent struct {
	Event      string          `json:"event"`
	RunID      string          `json:"run_id"`
	Seq        *int64          `json:"seq"`
	Delta      string          `json:"delta"`
	Text       string          `json:"text"`
	Tool       string          `json:"tool"`
	ToolCallID string          `json:"tool_call_id"`
	Error      json.RawMessage `json:"error"`
}

func validateServerURL(rawURL, apiKey string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		strings.TrimSpace(apiKey) != apiKey || apiKey == "" ||
		len(apiKey) > 4096 || strings.ContainsAny(apiKey, "\r\n\x00") {
		return "", ErrInvalidConfiguration
	}
	if parsed.Scheme != "https" && !isLoopbackHost(parsed.Hostname()) {
		return "", ErrInvalidConfiguration
	}
	if parsed.RawPath != "" || !validProfilePrefix(parsed.Path) {
		return "", ErrInvalidConfiguration
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return strings.TrimRight(parsed.String(), "/"), nil
}

func validProfilePrefix(value string) bool {
	if value == "" || value == "/" {
		return true
	}
	parts := strings.Split(strings.Trim(value, "/"), "/")
	if len(parts) != 2 || parts[0] != "p" || parts[1] == "" {
		return false
	}
	for _, char := range parts[1] {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return parts[1] != "." && parts[1] != ".."
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (b *Backend) newRequest(ctx context.Context, method, endpoint string, body any) (*http.Request, error) {
	if ctx == nil || !strings.HasPrefix(endpoint, "/") || strings.Contains(endpoint, "\x00") {
		return nil, ErrInvalidRequest
	}
	target, err := url.Parse(b.baseURL + endpoint)
	if err != nil {
		return nil, ErrServerUnavailable
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, ErrProtocol
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return nil, ErrServerUnavailable
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+b.apiKey)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func (b *Backend) do(ctx context.Context, method, endpoint string, body any) (*http.Response, error) {
	request, err := b.newRequest(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrServerUnavailable
	}
	return response, nil
}

func (b *Backend) requestJSON(ctx context.Context, method, endpoint string, body, result any) error {
	response, err := b.do(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		return statusError(response.StatusCode)
	}
	if result == nil || response.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err := decoder.Decode(result); err != nil {
		return ErrProtocol
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return ErrProtocol
	}
	return nil
}

func statusError(status int) error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrAuthentication
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return ErrServerUnavailable
	default:
		return &httpStatusError{status: status}
	}
}

func (b *Backend) verifyServer(ctx context.Context) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/health", nil)
	if err != nil {
		return "", ErrServerUnavailable
	}
	request.Header.Set("Accept", "application/json")
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrServerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		return "", statusError(response.StatusCode)
	}
	var health struct {
		Status   string `json:"status"`
		Platform string `json:"platform"`
		Version  string `json:"version"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maximumResponseBytes+1))
	if decoder.Decode(&health) != nil || health.Status != "ok" ||
		health.Platform != "hermes-agent" || health.Version != SupportedVersion {
		return "", ErrUnsupportedVersion
	}
	return health.Version, nil
}

func (b *Backend) verifyCapabilities(ctx context.Context) error {
	var value capabilities
	if err := b.requestJSON(ctx, http.MethodGet, "/v1/capabilities", nil, &value); err != nil {
		return err
	}
	if value.Object != "hermes.api_server.capabilities" || value.Platform != "hermes-agent" ||
		value.Auth.Type != "bearer" || !value.Auth.Required ||
		!value.Features.RunSubmission || !value.Features.RunStatus ||
		!value.Features.RunEventsSSE || !value.Features.RunStop ||
		!value.Features.RunsIdempotency.Supported || !value.Features.RunsIdempotency.Durable {
		return ErrUnsupportedCapabilities
	}
	return nil
}

func (b *Backend) submitRun(ctx context.Context, key string, body map[string]any) (runSubmission, error) {
	request, err := b.newRequest(ctx, http.MethodPost, "/v1/runs", body)
	if err != nil {
		return runSubmission{}, err
	}
	request.Header.Set("Idempotency-Key", key)
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return runSubmission{}, ctx.Err()
		}
		return runSubmission{}, ErrOutcomeUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		if response.StatusCode >= http.StatusInternalServerError || response.StatusCode == http.StatusConflict {
			return runSubmission{}, ErrOutcomeUnknown
		}
		return runSubmission{}, statusError(response.StatusCode)
	}
	var result runSubmission
	decoder := json.NewDecoder(io.LimitReader(response.Body, maximumResponseBytes+1))
	if decoder.Decode(&result) != nil || !validRunID(result.RunID) || result.Status == "" {
		return runSubmission{}, ErrOutcomeUnknown
	}
	return result, nil
}

func (b *Backend) getRun(ctx context.Context, runID string) (runStatus, error) {
	var result runStatus
	err := b.requestJSON(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID), nil, &result)
	if err != nil {
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.status == http.StatusNotFound {
			return runStatus{}, ErrOutcomeUnknown
		}
		return runStatus{}, err
	}
	return result, nil
}

func (b *Backend) stopRun(ctx context.Context, runID string) (runStatus, error) {
	var result runStatus
	err := b.requestJSON(
		ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/stop", map[string]any{}, &result)
	if err != nil {
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.status == http.StatusNotFound {
			return runStatus{}, ErrOutcomeUnknown
		}
		return runStatus{}, err
	}
	return result, nil
}

func (b *Backend) deleteRemoteSession(ctx context.Context, sessionID string) error {
	err := b.requestJSON(
		ctx, http.MethodDelete, "/api/sessions/"+url.PathEscape(sessionID), nil, nil)
	if err == nil {
		return nil
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) && statusErr.status == http.StatusNotFound {
		return nil
	}
	return err
}

func (b *Backend) readEvents(
	ctx context.Context,
	runID string,
	lastSeq int64,
	handle func(runEvent, int64) error,
) error {
	request, err := b.newRequest(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID)+"/events", nil)
	if err != nil {
		return err
	}
	if lastSeq >= 0 {
		request.Header.Set("Last-Event-ID", strconv.FormatInt(lastSeq, 10))
	}
	request.Header.Set("Accept", "text/event-stream")
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrServerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		return statusError(response.StatusCode)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), maximumEventLine)
	var eventID string
	var data strings.Builder
	dispatch := func() error {
		if data.Len() == 0 {
			eventID = ""
			return nil
		}
		var event runEvent
		if json.Unmarshal([]byte(data.String()), &event) != nil ||
			event.Event == "" || event.RunID != runID {
			return ErrProtocol
		}
		seq := int64(-1)
		if event.Seq != nil {
			seq = *event.Seq
		} else if eventID != "" {
			parsed, parseErr := strconv.ParseInt(eventID, 10, 64)
			if parseErr != nil || parsed < 0 {
				return ErrProtocol
			}
			seq = parsed
		}
		if err := handle(event, seq); err != nil {
			return err
		}
		eventID = ""
		data.Reset()
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		switch {
		case line == "":
			if err := dispatch(); err != nil {
				return err
			}
		case strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "id:"):
			eventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() != 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrServerUnavailable
	}
	return dispatch()
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

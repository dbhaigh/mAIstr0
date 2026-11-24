package openhands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
)

const (
	SupportedVersion     = "1.63.1"
	maximumResponseBytes = 8 << 20
	maximumSocketFrame   = 4 << 20
)

type conversationInfo struct {
	ID              string `json:"id"`
	ExecutionStatus string `json:"execution_status"`
	Workspace       struct {
		Kind       string `json:"kind"`
		WorkingDir string `json:"working_dir"`
	} `json:"workspace"`
}

type finalResponse struct {
	Response string `json:"response"`
}

func validateServerURL(rawURL, apiKey string) (string, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		strings.TrimSpace(apiKey) == "" || strings.TrimSpace(apiKey) != apiKey ||
		len(apiKey) > 4096 || strings.ContainsAny(apiKey, "\r\n\x00") {
		return "", "", ErrInvalidConfiguration
	}
	if !isLoopbackHost(parsed.Hostname()) && parsed.Scheme != "https" {
		return "", "", ErrInvalidConfiguration
	}
	parsed.Path = ""
	baseURL := strings.TrimRight(parsed.String(), "/")
	if parsed.Scheme == "https" {
		parsed.Scheme = "wss"
	} else {
		parsed.Scheme = "ws"
	}
	return baseURL, strings.TrimRight(parsed.String(), "/"), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateWorkspace(value string) (string, error) {
	if strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
		return "", ErrInvalidConfiguration
	}
	cleaned := path.Clean(value)
	if path.IsAbs(value) {
		if cleaned == "/" || cleaned != value {
			return "", ErrInvalidConfiguration
		}
		return cleaned, nil
	}
	windowsPath := strings.ReplaceAll(value, `\`, "/")
	if len(windowsPath) < 4 || windowsPath[1] != ':' || windowsPath[2] != '/' ||
		!isASCIILetter(windowsPath[0]) || path.Clean(windowsPath) != windowsPath {
		return "", ErrInvalidConfiguration
	}
	return value, nil
}

func isASCIILetter(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func (b *Backend) request(
	ctx context.Context,
	method string,
	endpoint string,
	query url.Values,
	body any,
	result any,
) error {
	if ctx == nil {
		return ErrInvalidRequest
	}
	target, err := url.Parse(b.baseURL + endpoint)
	if err != nil {
		return ErrServerUnavailable
	}
	if query != nil {
		target.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return ErrProtocol
		}
		reader = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return ErrServerUnavailable
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Session-API-Key", b.apiKey)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
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
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return ErrAuthentication
		case http.StatusNotFound:
			return ErrConversationUnavailable
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return ErrServerUnavailable
		default:
			return fmt.Errorf("%w: HTTP %d", ErrServerRequest, response.StatusCode)
		}
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
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrProtocol
	}
	return nil
}

func (b *Backend) verifyAuthentication(ctx context.Context) error {
	if err := b.request(ctx, http.MethodGet, "/api/conversations/count", nil, nil, nil); err != nil {
		if errors.Is(err, ErrAuthentication) {
			return ErrAuthentication
		}
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/api/conversations/count", nil)
	if err != nil {
		return ErrServerUnavailable
	}
	request.Header.Set("Accept", "application/json")
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrServerUnavailable
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
	if response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusForbidden {
		return ErrAuthenticationRequired
	}
	return nil
}

func (b *Backend) serverVersion(ctx context.Context) (string, error) {
	var info struct {
		Version string `json:"version"`
	}
	if err := b.request(ctx, http.MethodGet, "/server_info", nil, nil, &info); err != nil {
		return "", err
	}
	if info.Version != SupportedVersion {
		return "", ErrUnsupportedVersion
	}
	return info.Version, nil
}

func (b *Backend) getConversation(ctx context.Context, conversationID string) (conversationInfo, error) {
	if !validConversationID(conversationID) {
		return conversationInfo{}, ErrSessionMapping
	}
	var info conversationInfo
	if err := b.request(ctx, http.MethodGet, "/api/conversations/"+conversationID, nil, nil, &info); err != nil {
		return conversationInfo{}, err
	}
	if info.ID != conversationID || info.Workspace.Kind != "LocalWorkspace" ||
		info.Workspace.WorkingDir != b.workspace {
		return conversationInfo{}, ErrSessionOwnership
	}
	return info, nil
}

func (b *Backend) createConversation(ctx context.Context) (conversationInfo, error) {
	request := map[string]any{
		"agent": map[string]any{
			"llm": map[string]any{"model": b.model},
			"tools": []map[string]any{
				{"name": "TerminalTool", "params": map[string]any{}},
				{"name": "FileEditorTool", "params": map[string]any{}},
			},
			"mcp_config": map[string]any{"mcpServers": map[string]any{}},
		},
		"workspace": map[string]any{
			"kind": "LocalWorkspace", "working_dir": b.workspace,
		},
		"conversation_runtime": "local",
		"confirmation_policy":  map[string]string{"kind": "NeverConfirm"},
		"max_iterations":       50,
		"stuck_detection":      true,
		"plugins":              []string{},
		"hook_config":          nil,
	}
	var info conversationInfo
	if err := b.request(ctx, http.MethodPost, "/api/conversations", nil, request, &info); err != nil {
		return conversationInfo{}, err
	}
	if !validConversationID(info.ID) || info.Workspace.Kind != "LocalWorkspace" ||
		info.Workspace.WorkingDir != b.workspace {
		return conversationInfo{}, ErrProtocol
	}
	return info, nil
}

func (b *Backend) finalResponse(ctx context.Context, conversationID string) (string, error) {
	var result finalResponse
	if err := b.request(
		ctx, http.MethodGet,
		"/api/conversations/"+conversationID+"/agent_final_response",
		nil, nil, &result,
	); err != nil {
		return "", err
	}
	return strings.TrimSpace(result.Response), nil
}

func (b *Backend) socketURL(conversationID string, afterSeq int64) (string, error) {
	if !validConversationID(conversationID) {
		return "", ErrSessionMapping
	}
	target, err := url.Parse(b.socketBase + "/sockets/session/" + conversationID)
	if err != nil {
		return "", ErrServerUnavailable
	}
	query := target.Query()
	query.Set("after_seq", fmt.Sprintf("%d", afterSeq))
	target.RawQuery = query.Encode()
	return target.String(), nil
}

func (b *Backend) pollConversation(ctx context.Context, conversationID string) (conversationInfo, error) {
	return b.getConversation(ctx, conversationID)
}

package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	MinimumVersion       = "2.3.2"
	MaximumVersion       = "2.4.0"
	maximumResponseBytes = 8 << 20
)

var versionPattern = regexp.MustCompile(`^v?([0-9]+)\.([0-9]+)\.([0-9]+)$`)

type permissionRule struct {
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Effect   string `json:"effect"`
}

type sessionInfo struct {
	ID       string `json:"id"`
	Location struct {
		Directory string `json:"directory"`
	} `json:"location"`
}

type sessionEnvelope struct {
	Data sessionInfo `json:"data"`
}

type sessionMessagesEnvelope struct {
	Data   []nativeMessage `json:"data"`
	Cursor struct {
		Next string `json:"next"`
	} `json:"cursor"`
}

type nativeMessage struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Text    string `json:"text"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Outcome string `json:"outcome"`
}

func supportedVersion(value string) (string, error) {
	match := versionPattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return "", ErrUnsupportedVersion
	}
	major, err := strconv.Atoi(match[1])
	if err != nil {
		return "", ErrUnsupportedVersion
	}
	minor, err := strconv.Atoi(match[2])
	if err != nil {
		return "", ErrUnsupportedVersion
	}
	patch, err := strconv.Atoi(match[3])
	if err != nil || major != 2 || minor != 3 || patch < 2 {
		return "", ErrUnsupportedVersion
	}
	return fmt.Sprintf("%d.%d.%d", major, minor, patch), nil
}

func newHTTPClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func validateServerURL(rawURL, password string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" ||
		parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", ErrInvalidConfiguration
	}
	if strings.TrimSpace(password) == "" {
		return "", ErrInvalidConfiguration
	}
	if !isLoopbackHost(parsed.Hostname()) && parsed.Scheme != "https" {
		return "", ErrInvalidConfiguration
	}
	parsed.Path = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (b *Backend) request(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body any,
	result any,
) error {
	if ctx == nil {
		return ErrInvalidRequest
	}
	target, err := url.Parse(b.baseURL + path)
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
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.SetBasicAuth("opencode", b.password)
	response, err := b.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %s %s: %v", ErrServerUnavailable, method, path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		if response.StatusCode == http.StatusNotFound {
			return ErrSessionUnavailable
		}
		if response.StatusCode == http.StatusServiceUnavailable {
			return ErrServerUnavailable
		}
		return fmt.Errorf("%w: HTTP %d", ErrServerRequest, response.StatusCode)
	}
	if result == nil || response.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseBytes))
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err := decoder.Decode(result); err != nil {
		return ErrProtocol
	}
	return nil
}

func (b *Backend) serverInfo(ctx context.Context) (string, error) {
	var info struct {
		Version string `json:"version"`
	}
	if err := b.request(ctx, http.MethodGet, "/api/info", nil, nil, &info); err != nil {
		return "", err
	}
	return supportedVersion(info.Version)
}

func (b *Backend) verifyPolicy(ctx context.Context) error {
	query := url.Values{
		"location[directory]": {b.workspace},
	}
	for _, path := range []string{"/api/plugin", "/api/mcp"} {
		var response struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := b.request(ctx, http.MethodGet, path, query, nil, &response); err != nil {
			return ErrPolicyUnavailable
		}
		if response.Data == nil {
			return ErrPolicyUnavailable
		}
		if len(response.Data) != 0 {
			return ErrPolicyUnavailable
		}
	}
	return nil
}

func sessionPermissions(workspace string) []permissionRule {
	resource := filepath.ToSlash(filepath.Clean(workspace)) + "/**"
	return []permissionRule{
		{Action: "*", Resource: "*", Effect: "deny"},
		{Action: "read", Resource: resource, Effect: "allow"},
		{Action: "edit", Resource: resource, Effect: "allow"},
		{Action: "glob", Resource: resource, Effect: "allow"},
		{Action: "grep", Resource: resource, Effect: "allow"},
		{Action: "shell", Resource: "*", Effect: "deny"},
		{Action: "external_directory", Resource: "*", Effect: "deny"},
	}
}

func (b *Backend) waitForServer(ctx context.Context, processStatus func() (bool, error)) (string, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		version, err := b.serverInfo(requestCtx)
		cancel()
		if err == nil {
			return version, nil
		}
		if errors.Is(err, ErrUnsupportedVersion) {
			return "", err
		}
		if processStatus != nil {
			exited, statusErr := processStatus()
			if exited {
				if statusErr != nil {
					return "", ErrServerUnavailable
				}
				return "", ErrServerUnavailable
			}
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return "", ErrServerUnavailable
		}
		time.Sleep(100 * time.Millisecond)
	}
}

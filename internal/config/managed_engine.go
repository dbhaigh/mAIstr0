package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
)

type ManagedEngineKind string

const (
	ManagedEngineLlamaServer   ManagedEngineKind = "llama-server"
	ManagedEngineGGMLRPCServer ManagedEngineKind = "ggml-rpc-server"
	ManagedEngineVLLM          ManagedEngineKind = "vllm"
)

type ManagedEngineConfig struct {
	ID         string               `json:"id"`
	Kind       ManagedEngineKind    `json:"kind"`
	BinaryPath string               `json:"binary_path"`
	ModelPath  string               `json:"model_path,omitempty"`
	ModelID    string               `json:"model_id,omitempty"`
	Template   string               `json:"template,omitempty"`
	Options    ManagedEngineOptions `json:"options,omitempty"`
}

type ManagedEngineOptions struct {
	LlamaServer   *LlamaServerOptions   `json:"llama_server,omitempty"`
	GGMLRPCServer *GGMLRPCServerOptions `json:"ggml_rpc_server,omitempty"`
	VLLM          *VLLMOptions          `json:"vllm,omitempty"`
}

type LlamaServerOptions struct {
	Host        string `json:"host,omitempty"`
	Port        int    `json:"port,omitempty"`
	ContextSize int    `json:"context_size,omitempty"`
	GPULayers   int    `json:"gpu_layers,omitempty"`
	Threads     int    `json:"threads,omitempty"`
}

type GGMLRPCServerOptions struct {
	BindAddress     string `json:"bind_address,omitempty"`
	Port            int    `json:"port,omitempty"`
	AllowRemoteBind bool   `json:"allow_remote_bind,omitempty"`
}

type VLLMOptions struct {
	Host               string `json:"host,omitempty"`
	HostIP             string `json:"host_ip,omitempty"`
	Port               int    `json:"port,omitempty"`
	TensorParallelSize int    `json:"tensor_parallel_size,omitempty"`
}

var managedEngineIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidateManagedEngines checks that configured executable and local model
// paths resolve inside the operator-approved directories.
func ValidateManagedEngines(engines []ManagedEngineConfig, binaryAllowlist, modelAllowlist []string) error {
	binaryRoots, binaryErr := resolveManagedAllowlist("managed_engine_binary_allowlist", binaryAllowlist)
	modelRoots, modelErr := resolveManagedAllowlist("managed_engine_model_allowlist", modelAllowlist)
	violations := make([]error, 0, len(engines)+2)
	if binaryErr != nil {
		violations = append(violations, binaryErr)
	}
	if modelErr != nil {
		violations = append(violations, modelErr)
	}
	if len(engines) > 0 && len(binaryRoots) == 0 && binaryErr == nil {
		violations = append(violations, errors.New("managed_engine_binary_allowlist must contain a directory when managed_engines is set"))
	}

	seenIDs := make(map[string]struct{}, len(engines))
	for index, configured := range engines {
		field := fmt.Sprintf("managed_engines[%d]", index)
		if !managedEngineIDPattern.MatchString(configured.ID) {
			violations = append(violations, fmt.Errorf("%s.id must be 1-64 characters using letters, digits, dot, underscore, or hyphen", field))
		} else {
			id := strings.ToLower(configured.ID)
			if _, exists := seenIDs[id]; exists {
				violations = append(violations, fmt.Errorf("%s.id duplicates another managed engine", field))
			}
			seenIDs[id] = struct{}{}
		}
		switch configured.Kind {
		case ManagedEngineLlamaServer, ManagedEngineGGMLRPCServer, ManagedEngineVLLM:
		default:
			violations = append(violations, fmt.Errorf("%s.kind must be llama-server, ggml-rpc-server, or vllm", field))
		}
		if configured.Template != "" {
			template, ok := engine.ManagedEngineTemplateFor(configured.Template)
			if !ok || template.ManagedEngineKind == "" {
				violations = append(violations, fmt.Errorf("%s.template %q is not a managed process template", field, configured.Template))
			} else if template.ManagedEngineKind != string(configured.Kind) {
				violations = append(violations, fmt.Errorf("%s.template %q requires kind %s", field, configured.Template, template.ManagedEngineKind))
			} else if template.Capabilities.RequiresContextCap &&
				(configured.Options.LlamaServer == nil || configured.Options.LlamaServer.ContextSize <= 0) {
				violations = append(violations, fmt.Errorf("%s.template %q requires options.llama_server.context_size to be set", field, configured.Template))
			}
		}
		if err := validateAllowedManagedPath(configured.BinaryPath, binaryRoots, true); err != nil {
			violations = append(violations, fmt.Errorf("%s.binary_path: %w", field, err))
		}

		hasModelPath := strings.TrimSpace(configured.ModelPath) != ""
		hasModelID := strings.TrimSpace(configured.ModelID) != ""
		if configured.ModelPath != "" && !hasModelPath {
			violations = append(violations, fmt.Errorf("%s.model_path must not be blank", field))
		}
		if configured.ModelID != "" && !hasModelID {
			violations = append(violations, fmt.Errorf("%s.model_id must not be blank", field))
		}
		if hasModelPath && hasModelID {
			violations = append(violations, fmt.Errorf("%s must set only one of model_path or model_id", field))
		}
		switch configured.Kind {
		case ManagedEngineLlamaServer, ManagedEngineVLLM:
			if hasModelPath == hasModelID {
				violations = append(violations, fmt.Errorf("%s must set exactly one of model_path or model_id", field))
			}
		}
		if hasModelPath {
			if len(modelRoots) == 0 && modelErr == nil {
				violations = append(violations, errors.New("managed_engine_model_allowlist must contain a directory when model_path is set"))
			} else if err := validateAllowedManagedPath(configured.ModelPath, modelRoots, false); err != nil {
				violations = append(violations, fmt.Errorf("%s.model_path: %w", field, err))
			}
		}
		if hasModelID && (strings.ContainsAny(configured.ModelID, "\x00\r\n") || len(configured.ModelID) > 512) {
			violations = append(violations, fmt.Errorf("%s.model_id must be a non-empty single-line identifier of at most 512 characters", field))
		}
		if err := validateManagedEngineOptions(configured, field); err != nil {
			violations = append(violations, err)
		}
	}
	return errors.Join(violations...)
}

func resolveManagedAllowlist(field string, paths []string) ([]string, error) {
	roots := make([]string, 0, len(paths))
	var violations []error
	for index, path := range paths {
		if !filepath.IsAbs(path) {
			violations = append(violations, fmt.Errorf("%s[%d] must be an absolute directory path", field, index))
			continue
		}
		resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
		if err != nil {
			violations = append(violations, fmt.Errorf("%s[%d] must resolve to an existing directory", field, index))
			continue
		}
		info, err := os.Stat(resolved)
		if err != nil || !info.IsDir() {
			violations = append(violations, fmt.Errorf("%s[%d] must resolve to an existing directory", field, index))
			continue
		}
		roots = append(roots, filepath.Clean(resolved))
	}
	return roots, errors.Join(violations...)
}

func validateAllowedManagedPath(path string, roots []string, executable bool) error {
	if strings.TrimSpace(path) == "" || !filepath.IsAbs(path) {
		return errors.New("must be an absolute path")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return errors.New("must resolve to an existing allowed path")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return errors.New("must resolve to an existing allowed path")
	}
	if executable {
		if !info.Mode().IsRegular() {
			return errors.New("must be a regular executable file")
		}
		if runtime.GOOS == "windows" {
			if !strings.EqualFold(filepath.Ext(resolved), ".exe") {
				return errors.New("must be a native .exe file; command scripts are not supported")
			}
		} else if info.Mode().Perm()&0o111 == 0 {
			return errors.New("must be executable")
		}
	} else if !info.Mode().IsRegular() && !info.IsDir() {
		return errors.New("must be a regular model file or directory")
	}
	for _, root := range roots {
		if pathWithin(root, filepath.Clean(resolved)) {
			return nil
		}
	}
	return errors.New("is outside the configured operator allow-list")
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || filepath.IsAbs(relative) || relative == ".." {
		return false
	}
	return !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateManagedEngineOptions(configured ManagedEngineConfig, field string) error {
	options := configured.Options
	count := 0
	if options.LlamaServer != nil {
		count++
	}
	if options.GGMLRPCServer != nil {
		count++
	}
	if options.VLLM != nil {
		count++
	}
	if count > 1 {
		return fmt.Errorf("%s.options must contain options for only one engine kind", field)
	}
	if count == 0 {
		return nil
	}
	switch configured.Kind {
	case ManagedEngineLlamaServer:
		if options.LlamaServer == nil {
			return fmt.Errorf("%s.options must use llama_server for kind llama-server", field)
		}
		if err := validateHostPort(options.LlamaServer.Host, options.LlamaServer.Port, "llama_server"); err != nil {
			return fmt.Errorf("%s.options: %w", field, err)
		}
		if options.LlamaServer.ContextSize < 0 || options.LlamaServer.GPULayers < 0 || options.LlamaServer.Threads < 0 {
			return fmt.Errorf("%s.options llama_server numeric values must not be negative", field)
		}
	case ManagedEngineGGMLRPCServer:
		if options.GGMLRPCServer == nil {
			return fmt.Errorf("%s.options must use ggml_rpc_server for kind ggml-rpc-server", field)
		}
		o := options.GGMLRPCServer
		if err := validateHostPort(o.BindAddress, o.Port, "ggml_rpc_server"); err != nil {
			return fmt.Errorf("%s.options: %w", field, err)
		}
		bindAddress := o.BindAddress
		if bindAddress == "" {
			bindAddress = "127.0.0.1"
		}
		if !managedAddressIsLoopback(bindAddress) {
			return fmt.Errorf("%s.options.ggml_rpc_server.bind_address must be loopback; use the authenticated RPC relay for remote access", field)
		}
	case ManagedEngineVLLM:
		if options.VLLM == nil {
			return fmt.Errorf("%s.options must use vllm for kind vllm", field)
		}
		if err := validateHostPort(options.VLLM.Host, options.VLLM.Port, "vllm"); err != nil {
			return fmt.Errorf("%s.options: %w", field, err)
		}
		if options.VLLM.HostIP != "" && net.ParseIP(strings.Trim(options.VLLM.HostIP, "[]")) == nil {
			return fmt.Errorf("%s.options.vllm.host_ip must be an IP address", field)
		}
		if options.VLLM.TensorParallelSize < 0 {
			return fmt.Errorf("%s.options.vllm.tensor_parallel_size must not be negative", field)
		}
	default:
		return fmt.Errorf("%s.options cannot be used with an unsupported engine kind", field)
	}
	return nil
}

func validateHostPort(host string, port int, option string) error {
	if host != "" && !validManagedHost(host) {
		return fmt.Errorf("options.%s.host must be a hostname or IP address", option)
	}
	if port < 0 || port > 65535 {
		return fmt.Errorf("options.%s.port must be between 1 and 65535 when set", option)
	}
	return nil
}

func validManagedHost(host string) bool {
	if host == "" || strings.TrimSpace(host) != host || strings.ContainsAny(host, "/\x00\r\n\t ") {
		return false
	}
	if net.ParseIP(strings.Trim(host, "[]")) != nil || strings.EqualFold(host, "localhost") {
		return true
	}
	if len(host) > 253 || strings.Contains(host, ":") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func managedAddressIsLoopback(address string) bool {
	if strings.EqualFold(address, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(address, "[]"))
	return ip != nil && ip.IsLoopback()
}

package supervisor

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/config"
	enginepkg "github.com/maistr0/maistr0/internal/engine"
)

func nonLoopbackRPCWarning(configured config.ManagedEngineConfig) string {
	if configured.Kind != config.ManagedEngineGGMLRPCServer || configured.Options.GGMLRPCServer == nil {
		return ""
	}
	address := configured.Options.GGMLRPCServer.BindAddress
	if address == "" {
		address = "127.0.0.1"
	}
	if isLoopback(address) {
		return ""
	}
	return fmt.Sprintf("managed engine %s ggml-rpc-server binds to non-loopback address %s; verify network access is restricted", configured.ID, address)
}

func isLoopback(address string) bool {
	if strings.EqualFold(address, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(address, "[]"))
	return ip != nil && ip.IsLoopback()
}

func validatePlatformAndPlan(configured config.ManagedEngineConfig, plan *ShardPlan) error {
	if configured.Kind == config.ManagedEngineVLLM {
		if nativeWindows() {
			return fmt.Errorf("%w: vllm managed engines are not supported on native Windows", ErrUnsupportedPlatform)
		}
		if plan == nil || plan.NNodes < 1 || plan.NNodes > 1024 || plan.NodeRank < 0 || plan.NodeRank >= plan.NNodes || !validMasterAddress(plan.MasterAddr) {
			return ErrInvalidShardPlan
		}
		if plan.TensorParallelSize < 0 || plan.PipelineParallelSize < 0 ||
			(plan.TensorParallelSize == 0) != (plan.PipelineParallelSize == 0) {
			return ErrInvalidShardPlan
		}
		return nil
	}
	if configured.Kind == config.ManagedEngineLlamaServer && plan != nil {
		if plan.TensorSplit == "" || strings.ContainsAny(plan.RPC, " \t\r\n\x00") ||
			(plan.RPC == "") == (len(plan.RPCRelays) == 0) {
			return ErrInvalidShardPlan
		}
		if plan.RPC != "" {
			for _, address := range strings.Split(plan.RPC, ",") {
				host, port, err := net.SplitHostPort(address)
				if err != nil || host == "" || !isLoopback(host) {
					return ErrInvalidShardPlan
				}
				if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
					return ErrInvalidShardPlan
				}
			}
		} else {
			seenPeers := make(map[string]struct{}, len(plan.RPCRelays))
			for _, target := range plan.RPCRelays {
				if strings.TrimSpace(target.PeerID) == "" {
					return ErrInvalidShardPlan
				}
				if _, exists := seenPeers[target.PeerID]; exists {
					return ErrInvalidShardPlan
				}
				seenPeers[target.PeerID] = struct{}{}
				host, port, err := net.SplitHostPort(target.Address)
				if err != nil || host == "" {
					return ErrInvalidShardPlan
				}
				if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
					return ErrInvalidShardPlan
				}
				if strings.ContainsAny(target.PeerID+target.Address, "\x00\r\n\t ") {
					return ErrInvalidShardPlan
				}
			}
		}
		for _, part := range strings.Split(plan.TensorSplit, ",") {
			value, err := strconv.ParseFloat(part, 64)
			if err != nil || value <= 0 {
				return ErrInvalidShardPlan
			}
		}
	}
	if plan != nil {
		if configured.Kind != config.ManagedEngineLlamaServer {
			return ErrInvalidShardPlan
		}
	}
	return nil
}

func validMasterAddress(address string) bool {
	if address == "" || strings.TrimSpace(address) != address || strings.ContainsAny(address, "/\x00\r\n\t ") {
		return false
	}
	if ip := net.ParseIP(strings.Trim(address, "[]")); ip != nil {
		return !ip.IsUnspecified()
	}
	if strings.Contains(address, ":") || len(address) > 253 {
		return false
	}
	for _, label := range strings.Split(address, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
				return false
			}
		}
	}
	return true
}

func applyTemplateEnvironment(base []string, overrides []enginepkg.TemplateEnvironment) []string {
	environment := append([]string(nil), base...)
	for _, override := range overrides {
		prefix := override.Name + "="
		replaced := false
		for index, variable := range environment {
			name, _, found := strings.Cut(variable, "=")
			if found && strings.EqualFold(name, override.Name) {
				environment[index] = prefix + override.Value
				replaced = true
				break
			}
		}
		if !replaced {
			environment = append(environment, prefix+override.Value)
		}
	}
	return environment
}

func buildArgs(configured config.ManagedEngineConfig, plan *ShardPlan) ([]string, error) {
	if err := validatePlatformAndPlan(configured, plan); err != nil {
		return nil, err
	}
	model := configured.ModelPath
	if model == "" {
		model = configured.ModelID
	}
	switch configured.Kind {
	case config.ManagedEngineLlamaServer:
		host, port := "127.0.0.1", 8080
		args := []string{"--model", model}
		if options := configured.Options.LlamaServer; options != nil {
			if options.Host != "" {
				host = options.Host
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		args = append(args, "--host", host, "--port", strconv.Itoa(port))
		if options := configured.Options.LlamaServer; options != nil {
			if options.ContextSize > 0 {
				args = append(args, "--ctx-size", strconv.Itoa(options.ContextSize))
			}
			if options.GPULayers > 0 {
				args = append(args, "--n-gpu-layers", strconv.Itoa(options.GPULayers))
			}
			if options.Threads > 0 {
				args = append(args, "--threads", strconv.Itoa(options.Threads))
			}
		}
		if template, ok := enginepkg.ManagedEngineTemplateFor(configured.Template); ok {
			if template.LlamaServerDevice != "" {
				args = append(args, "--device", template.LlamaServerDevice)
			}
			if template.LlamaServerParallelSeqs > 0 {
				args = append(args, "--parallel", strconv.Itoa(template.LlamaServerParallelSeqs))
			}
			if template.LlamaServerNoKVOffload {
				args = append(args, "-nkvo")
			}
		}
		if plan != nil {
			if plan.RPC != "" {
				args = append(args, "--rpc", plan.RPC)
			}

			if plan.TensorSplit != "" {
				args = append(args, "--tensor-split", plan.TensorSplit)
			}
		}
		return args, nil
	case config.ManagedEngineGGMLRPCServer:
		host, port := "127.0.0.1", 50052
		if options := configured.Options.GGMLRPCServer; options != nil {
			if options.BindAddress != "" {
				host = options.BindAddress
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		return []string{"--host", host, "--port", strconv.Itoa(port)}, nil
	case config.ManagedEngineVLLM:
		host, port := "127.0.0.1", 8000
		args := []string{"serve", model}
		if plan.NodeRank > 0 {
			args = append(args, "--headless")
		}
		args = append(args, "--nnodes", strconv.Itoa(plan.NNodes), "--node-rank", strconv.Itoa(plan.NodeRank), "--master-addr", plan.MasterAddr)
		if options := configured.Options.VLLM; options != nil {
			if options.Host != "" {
				host = options.Host
			}
			if options.Port != 0 {
				port = options.Port
			}
		}
		args = append(args, "--host", host, "--port", strconv.Itoa(port))
		tensorParallelSize := plan.TensorParallelSize
		if tensorParallelSize == 0 && configured.Options.VLLM != nil {
			tensorParallelSize = configured.Options.VLLM.TensorParallelSize
		}
		if tensorParallelSize > 0 {
			args = append(args, "--tensor-parallel-size", strconv.Itoa(tensorParallelSize))
		}
		if plan.PipelineParallelSize > 0 {
			args = append(args, "--pipeline-parallel-size", strconv.Itoa(plan.PipelineParallelSize))
		}
		return args, nil
	default:
		return nil, fmt.Errorf("%w: unsupported managed engine kind", ErrStart)
	}
}

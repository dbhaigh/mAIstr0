package orchestrator

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/shard"
)

func shardPlanNetworkWarnings(plan shard.ShardPlan, candidates []shard.CandidateNode) ([]string, error) {
	byID := make(map[string]shard.CandidateNode, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.ID] = candidate
	}
	var warnings []string
	for _, planned := range plan.Nodes {
		candidate, exists := byID[planned.NodeID]
		if !exists {
			return nil, &shard.Refusal{
				Code:      shard.RefusalUntrustedNetwork,
				Message:   fmt.Sprintf("shard plan selected unknown node %q; network security cannot be verified", planned.NodeID),
				Rationale: []string{fmt.Sprintf("node %q is absent from the validated candidate snapshot", planned.NodeID)},
			}
		}
		switch plan.Mode {
		case shard.ModeLlamaCPPRPC:
			if !candidate.RPCProtected || !validRemoteRPCRelayAddress(candidate.RPCAddress) {
				return nil, &shard.Refusal{
					Code:      shard.RefusalUntrustedNetwork,
					Message:   fmt.Sprintf("node %q has no authenticated mTLS RPC relay", candidate.ID),
					Rationale: []string{fmt.Sprintf("node %q was selected without a reachable mTLS-protected RPC relay", candidate.ID)},
				}
			}
		case shard.ModeVLLMMP:
			if !trustedNetworkLink(candidate) {
				if !candidate.AllowInsecureShardNetwork {
					return nil, &shard.Refusal{
						Code:      shard.RefusalUntrustedNetwork,
						Message:   fmt.Sprintf("node %q has no operator-trusted network interface for vllm-mp", candidate.ID),
						Rationale: []string{fmt.Sprintf("node %q does not mark any distributed-traffic interface trusted", candidate.ID)},
					}
				}
				warnings = append(warnings, fmt.Sprintf(
					"WARNING: node %q permits unencrypted vllm-mp traffic on an untrusted network because allow_insecure_shard_network is enabled",
					candidate.ID,
				))
			}
		}
	}
	return warnings, nil
}

func trustedNetworkLink(candidate shard.CandidateNode) bool {
	for _, link := range candidate.NetworkLinks {
		if link.Trusted {
			return true
		}
	}
	return false
}

func validRemoteRPCRelayAddress(endpoint string) bool {
	host, port, ok := endpointHostPort(endpoint)
	if !ok {
		return false
	}
	if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast()
	}
	return true
}

func advertisedEndpointAddress(endpoint, nodeAddress string) string {
	_, port, ok := endpointHostPort(endpoint)
	if !ok {
		return ""
	}
	advertised, err := url.Parse(nodeAddress)
	if err != nil || advertised.Hostname() == "" {
		return ""
	}
	return net.JoinHostPort(advertised.Hostname(), port)
}

func advertisedNodeHost(nodeAddress string) string {
	advertised, err := url.Parse(nodeAddress)
	if err != nil {
		return ""
	}
	return advertised.Hostname()
}

func endpointHostPort(endpoint string) (string, string, bool) {
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Scheme != "" {
		host, port := parsed.Hostname(), parsed.Port()
		return host, port, host != "" && port != ""
	}
	host, port, err := net.SplitHostPort(endpoint)
	return host, port, err == nil && host != "" && port != ""
}

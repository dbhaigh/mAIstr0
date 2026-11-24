package orchestrator

import (
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/maistr0/maistr0/internal/agent"
)

func localExecutionProfile(workspace, isolation, approvalLimitations string) agent.ExecutionProfile {
	return agent.ExecutionProfile{
		Location: agent.ExecutionLocal, Workspace: workspace,
		Isolation: isolation, ApprovalLimitations: approvalLimitations,
	}
}

func serverExecutionProfile(serverURL, workspace, isolation, approvalLimitations string) agent.ExecutionProfile {
	return agent.ExecutionProfile{
		Location: serverExecutionLocation(serverURL), Workspace: workspace,
		Isolation: isolation, ApprovalLimitations: approvalLimitations,
	}
}

func configuredPath(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func opencodeExecutionProfile() agent.ExecutionProfile {
	workspace := configuredPath("MAISTR0_OPENCODE_WORKSPACE")
	isolation := "Workspace-scoped tool permissions; not OS-level isolation."
	approvals := "No mAIstr0 approval interaction."
	if configuredPath("MAISTR0_OPENCODE_MODE") == "managed" {
		return localExecutionProfile(workspace, isolation, approvals)
	}
	return serverExecutionProfile(
		configuredPath("MAISTR0_OPENCODE_SERVER_URL"), workspace, isolation, approvals,
	)
}

func serverExecutionLocation(serverURL string) agent.ExecutionLocation {
	parsed, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil || parsed.Hostname() == "" {
		return agent.ExecutionUnknown
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return agent.ExecutionLocal
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return agent.ExecutionLocal
		}
		return agent.ExecutionRemote
	}
	return agent.ExecutionRemote
}

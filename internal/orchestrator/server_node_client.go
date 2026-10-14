package orchestrator

import (
	"context"
	"net/http"

	"github.com/maistr0/maistr0/internal/cluster"
)

func (s *Server) discoveredNodeStatus(addr string) (cluster.NodeStatus, error) {
	return s.nodeClient.discoveredNodeStatus(addr)
}

func (s *Server) discoveredNodeStatusContext(ctx context.Context, addr string) (cluster.NodeStatus, error) {
	return s.nodeClient.discoveredNodeStatusContext(ctx, addr)
}

func (s *Server) streamNodeGenerate(w http.ResponseWriter, r *http.Request, address, model string, request generateAPIRequest, prompt string) {
	s.nodeClient.streamNodeGenerate(w, r, address, model, request, prompt)
}

func (s *Server) callNodeGenerate(nodeAddr, model, prompt string) (string, error) {
	return s.nodeClient.callNodeGenerate(nodeAddr, model, prompt)
}

func (s *Server) callNodeGenerateWithOptions(nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return s.nodeClient.callNodeGenerateWithOptions(nodeAddr, model, prompt, temperature, maxTokens)
}

func (s *Server) callNodeGenerateWithOptionsContext(ctx context.Context, nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return s.nodeClient.callNodeGenerateWithOptionsContext(ctx, nodeAddr, model, prompt, temperature, maxTokens)
}

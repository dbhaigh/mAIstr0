package orchestrator

import (
	"errors"
	"net/http"
)

func (s *Server) handlePipelineReshard(w http.ResponseWriter, r *http.Request) {
	model, exists := s.shardedModel(r.PathValue("model_id"))
	if !exists {
		http.NotFound(w, r)
		return
	}
	plan, err := s.reshardPipeline(r.Context(), model)
	if err != nil {
		status := pipelineHTTPStatus(err)
		if errors.Is(err, errPipelinePlanPersistence) {
			status = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), status)
		return
	}
	s.publishSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{"plan": plan})
}

package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type assignmentExplanationResponse struct {
	TaskID      string                        `json:"task_id"`
	Assignments []assignmentExplanationDetail `json:"assignments"`
}

type assignmentExplanationDetail struct {
	SubtaskID    string                 `json:"subtask_id"`
	Description  string                 `json:"description"`
	NodeID       string                 `json:"node_id"`
	Model        string                 `json:"model"`
	Engine       string                 `json:"engine,omitempty"`
	Score        float64                `json:"score"`
	ScoreFactors scheduler.ScoreFactors `json:"score_factors"`
	LearnedBias  float64                `json:"learned_bias"`
	Summary      string                 `json:"summary"`
}

type taskDependenciesResponse struct {
	Nodes []taskDependencyNode `json:"nodes"`
	Edges []taskDependencyEdge `json:"edges"`
}

type taskDependencyNode struct {
	ID          string         `json:"id"`
	Status      taskmgr.Status `json:"status"`
	Description string         `json:"description"`
}

type taskDependencyEdge struct {
	SourceID string `json:"source_id"`
	DestID   string `json:"dest_id"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	s.recoverTaskHistory()
	task, err := s.createTask(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	s.recoverTaskHistory()
	version, watermarks, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		http.Error(w, "invalid task sync headers: "+err.Error(), http.StatusBadRequest)
		return
	}
	cursor, upper, err := memory.ReadSyncPageRequestHeaders(r)
	if err != nil {
		http.Error(w, "invalid task sync page headers: "+err.Error(), http.StatusBadRequest)
		return
	}
	if cursor != nil && !memory.SupportsSyncPages(version) {
		http.Error(w, "task sync pagination requires protocol version 2", http.StatusBadRequest)
		return
	}
	if !isTaskSyncRequest(r) {
		pageRequest, err := parseHistoryPageRequest(r, "tasks", "q", "status")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if pageRequest.Active {
			var pageCursor *taskmgr.PageCursor
			if r.URL.Query().Has("cursor") {
				if pageRequest.Cursor.Key == "" {
					http.Error(w, "cursor is missing its task position", http.StatusBadRequest)
					return
				}
				pageCursor = &taskmgr.PageCursor{
					UpdatedAt: pageRequest.Cursor.UpdatedAt,
					ID:        pageRequest.Cursor.Key,
				}
			}
			status := r.URL.Query().Get("status")
			if status == "all" {
				status = ""
			}
			page, err := s.tasks.PageRecent(
				pageCursor, r.URL.Query().Get("q"), taskmgr.Status(status),
				pageRequest.Limit, maxHistoryPageBytes-historyPageEnvelopeRoom,
			)
			if err != nil {
				if errors.Is(err, taskmgr.ErrPageRecordTooLarge) {
					http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, "could not read task history page: "+err.Error(), http.StatusInternalServerError)
				return
			}
			writeHistoryPage(w, pageRequest, page.Items, memory.HistoryCursor{
				UpdatedAt: page.NextCursor.UpdatedAt,
				Key:       page.NextCursor.ID,
			}, page.HasMore, s.maxSyncBytes())
			return
		}
	}
	tasks, currentWatermarks := s.tasks.SnapshotAndWatermarksForPeer(version, taskmgr.FilterTaskWatermarks(watermarks))
	if memory.SupportsSyncPages(version) {
		pageStarted := time.Now()
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = currentWatermarks
		}
		page, nextCursor, more, err := taskmgr.PageTasks(tasks, cursor, upper, s.maxSyncBytes())
		if err != nil {
			http.Error(w, "could not page task history: "+err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		tasks = page
		currentWatermarks = upper
		if more {
			if err := memory.SetSyncPageResponseHeader(w, nextCursor); err != nil {
				http.Error(w, "could not encode task sync page cursor: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		s.metrics.recordSyncPage()
		log.Printf("FILE_ONLY: orchestrator: task history sync event=page_served %s records=%d more=%t",
			syncLogFields(syncLogRequestID(r), syncLogNodeID(r), time.Since(pageStarted), "none"), len(tasks), more)
	}
	if err := memory.SetSyncResponseHeaders(w, currentWatermarks); err != nil {
		http.Error(w, "could not encode task sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleTaskDependencies(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}

	graph := task.Graph
	if graph == nil || len(graph.Nodes) == 0 && len(task.Subtasks) > 0 {
		subtasks := make([]scheduler.Subtask, 0, len(task.Subtasks))
		for _, result := range task.Subtasks {
			subtasks = append(subtasks, result.Subtask)
		}
		if len(subtasks) > 0 {
			var err error
			graph, err = scheduler.TaskGraphFromSubtasks(subtasks)
			if err != nil {
				writeError(w, apperr.InternalError(fmt.Errorf("could not reconstruct task dependency graph: %w", err)))
				return
			}
		}
	}
	if graph == nil {
		graph = scheduler.NewTaskGraph()
	}
	if len(graph.Nodes) > 0 {
		if err := graph.Validate(); err != nil {
			writeError(w, apperr.InternalError(fmt.Errorf("invalid task dependency graph: %w", err)))
			return
		}
	}

	statuses := make(map[string]taskmgr.Status, len(task.Subtasks))
	for _, result := range task.Subtasks {
		statuses[result.Subtask.ID] = result.Status
	}
	response := taskDependenciesResponse{
		Nodes: make([]taskDependencyNode, 0, len(graph.Nodes)),
		Edges: make([]taskDependencyEdge, 0, len(graph.Edges)),
	}
	for id, subtask := range graph.Nodes {
		status, ok := statuses[id]
		if !ok {
			status = taskmgr.StatusPending
		}
		response.Nodes = append(response.Nodes, taskDependencyNode{
			ID: id, Status: status, Description: subtask.Description,
		})
	}
	edges := make(map[scheduler.Edge]struct{}, len(graph.Edges))
	for _, edge := range graph.Edges {
		edges[edge] = struct{}{}
	}
	for id, subtask := range graph.Nodes {
		for _, prerequisite := range subtask.DependsOn {
			edges[scheduler.Edge{From: prerequisite, To: id}] = struct{}{}
		}
	}
	for edge := range edges {
		response.Edges = append(response.Edges, taskDependencyEdge{
			SourceID: edge.From, DestID: edge.To,
		})
	}
	sort.Slice(response.Nodes, func(i, j int) bool {
		return response.Nodes[i].ID < response.Nodes[j].ID
	})
	sort.Slice(response.Edges, func(i, j int) bool {
		if response.Edges[i].SourceID != response.Edges[j].SourceID {
			return response.Edges[i].SourceID < response.Edges[j].SourceID
		}
		return response.Edges[i].DestID < response.Edges[j].DestID
	})
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleTaskAssignmentExplanation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	response := assignmentExplanationResponse{
		TaskID:      task.ID,
		Assignments: make([]assignmentExplanationDetail, 0, len(task.Subtasks)),
	}
	for _, result := range task.Subtasks {
		assignment := result.Assignment
		factors := assignment.Factors
		telemetrySummary := "stale or unavailable telemetry was ignored"
		if factors.TelemetryFresh {
			telemetrySummary = fmt.Sprintf("telemetry was fresh (%d ms old)", factors.TelemetryAgeMs)
		}
		summary := fmt.Sprintf(
			"Score %.1f assigned to %s using %s: model match %+.1f, hardware %+.1f, capabilities %+.1f, load and capacity %+.1f, learned bias %+.1f; %s.",
			assignment.Score, assignment.NodeID, assignment.Model,
			factors.ModelMatch, factors.Hardware, factors.CapabilityScore,
			-(factors.LoadPenalty + factors.CapacityPenalty), factors.LearnedBias, telemetrySummary,
		)
		response.Assignments = append(response.Assignments, assignmentExplanationDetail{
			SubtaskID:    assignment.Subtask.ID,
			Description:  assignment.Subtask.Description,
			NodeID:       assignment.NodeID,
			Model:        assignment.Model,
			Engine:       assignment.Engine,
			Score:        assignment.Score,
			ScoreFactors: factors,
			LearnedBias:  factors.LearnedBias,
			Summary:      summary,
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	cancelled, cancelledOK, err := s.tasks.CancelAndSnapshotChecked(id)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist cancellation: %w", err)))
		return
	}
	if !cancelledOK {
		writeError(w, apperr.ConflictError(errors.New("task is already complete")))
		return
	}
	writeJSON(w, http.StatusOK, cancelled)
}

func (s *Server) handleApproveTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	taskBeforeApproval, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	if taskBeforeApproval.Status != taskmgr.StatusAwaitingApproval {
		writeError(w, apperr.ConflictError(errors.New("task is not awaiting approval")))
		return
	}
	if err := s.tasks.ApproveChecked(id); err != nil {
		if errors.Is(err, taskmgr.ErrNotAwaitingApproval) {
			writeError(w, apperr.ConflictError(err))
			return
		}
		writeError(w, apperr.InternalError(fmt.Errorf("could not approve task: %w", err)))
		return
	}
	task, _ := s.tasks.Get(id)
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleListResumableTasks(w http.ResponseWriter, r *http.Request) {
	resumable := s.tasks.ResumableTasks()
	if resumable == nil {
		resumable = []*taskmgr.Task{}
	}
	writeJSON(w, http.StatusOK, resumable)
}

func (s *Server) handleResumeTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	if !task.Resumable {
		writeError(w, apperr.ConflictError(errors.New("task is not resumable")))
		return
	}
	resumed, err := s.tasks.ResumeTaskChecked(id)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not resume task: %w", err)))
		return
	}
	for _, subtask := range resumed.Subtasks {
		if subtask.Status == taskmgr.StatusPending {
			if err := s.submitDispatch(resumed.ID, subtask.Assignment); err != nil {
				log.Printf("orchestrator: could not queue resumed subtask %s: %v", subtask.Subtask.ID, err)
				s.tasks.CompleteSubtask(resumed.ID, subtask.Subtask.ID, "", err)
			}
		}
	}
	writeJSON(w, http.StatusOK, resumed)
}

func (s *Server) handleTaskCheckpoints(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.tasks.Get(id); !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	cps, err := s.tasks.GetCheckpoints(id)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not load checkpoints: %w", err)))
		return
	}
	if cps == nil {
		cps = []taskmgr.Checkpoint{}
	}
	writeJSON(w, http.StatusOK, cps)
}

package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

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
	tasks, currentWatermarks := s.tasks.SnapshotAndWatermarksForPeer(version, taskmgr.FilterTaskWatermarks(watermarks))
	if memory.SupportsSyncPages(version) {
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = currentWatermarks
		}
		page, nextCursor, more, err := taskmgr.PageTasks(tasks, cursor, upper, maxReplicatedTaskHistoryBytes)
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

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, ok := s.tasks.Get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	cancelledOK, err := s.tasks.CancelChecked(id)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist cancellation: %w", err)))
		return
	}
	if !cancelledOK {
		writeError(w, apperr.ConflictError(errors.New("task is already complete")))
		return
	}
	cancelled, _ := s.tasks.Get(id)
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

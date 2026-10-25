package nodeagent

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"net/http"
	"net/url"
)

// handleSubmit lets this node accept a task directly and share the work
// across every other node it knows about (via the peer cache), dispatching
// each subtask straight to the chosen peer's /execute endpoint rather than
// routing everything back through the orchestrator.
func (a *Agent) handleSubmit(w http.ResponseWriter, r *http.Request) {
	req, err := httpx.DecodeJSON[submitRequest](
		w, r, httpx.ControlJSONBodyBytes,
	)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}

	subtasks := scheduler.Decompose(req.Description)
	if len(subtasks) == 0 {
		writeError(w, apperr.Invalid(errors.New("description produced no subtasks")))
		return
	}

	assignments, err := scheduler.AssignWithExperience(subtasks, a.Peers(), a.experience())
	if err != nil {
		if errors.Is(err, scheduler.ErrNoHealthyNodes) {
			writeError(w, apperr.UnavailableError(err))
			return
		}
		var unassignable *scheduler.UnassignableSubtasksError
		if errors.As(err, &unassignable) {
			writeError(w, apperr.UnavailableError(err))
			return
		}
		writeError(w, apperr.InternalError(err))
		return
	}
	if len(assignments) != len(subtasks) {
		writeError(w, apperr.UnavailableError(errors.New("scheduler returned an incomplete assignment")))
		return
	}

	task, err := a.orchestratorLink.tasks.CreateChecked(req.Description, assignments)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist task: %w", err)))
		return
	}
	taskCtx, ok := a.orchestratorLink.tasks.Context(task.ID)
	if !ok {
		writeError(w, apperr.InternalError(errors.New("created task is no longer available")))
		return
	}
	for _, asg := range assignments {
		if err := a.peerComponent.dispatchPool.Submit(taskCtx, peerDispatchJob{taskID: task.ID, assignment: asg}); err != nil {
			a.orchestratorLink.tasks.CompleteSubtask(task.ID, asg.Subtask.ID, "", err)
		}
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *Agent) handleTasksList(w http.ResponseWriter, r *http.Request) {
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
	tasks, currentWatermarks := a.orchestratorLink.tasks.SnapshotAndWatermarksForPeer(version, taskmgr.FilterTaskWatermarks(watermarks))
	if memory.SupportsSyncPages(version) {
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = currentWatermarks
		}
		page, nextCursor, more, err := taskmgr.PageTasks(tasks, cursor, upper, a.cfg.SyncMaxBytes)
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
	if err := memory.SetSyncResponseHeadersForPeer(w, currentWatermarks, version); err != nil {
		http.Error(w, "could not encode task sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if memory.SupportsEmptySync(version) && r.Header.Get(memory.SyncPageCursorHeader) == "" && len(tasks) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, err := memory.WriteSyncJSON(w, http.StatusOK, tasks, version, memory.AcceptsSyncCompression(r)); err != nil {
		logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: task sync response encoding failed: %v", err)
	}
}

func (a *Agent) handleTaskGet(w http.ResponseWriter, r *http.Request) {
	t, ok := a.orchestratorLink.tasks.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// handleAPITasksPost sends joined-cluster submissions to the persistent
// orchestrator task manager. Standalone nodes fall back to their elected peer.
func (a *Agent) handleAPITasksPost(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks")
		return
	}
	if a.isMostCapable() {
		a.handleSubmit(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/submit")
}

func (a *Agent) handleAPITasksList(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, withRawQuery("/api/tasks", r))
		return
	}
	if a.isMostCapable() {
		a.handleTasksList(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks")
}

func (a *Agent) handleAPITaskGet(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks/"+url.PathEscape(r.PathValue("id")))
		return
	}
	if a.isMostCapable() {
		a.handleTaskGet(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks/"+r.PathValue("id"))
}

func (a *Agent) handleAPITaskAssignmentExplanation(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		path := "/api/tasks/" + url.PathEscape(r.PathValue("id")) + "/assignment-explanation"
		a.proxy(w, r, addr, path)
		return
	}
	writeError(w, apperr.UnavailableError(errors.New("assignment explanations are unavailable without an orchestrator")))
}

func (a *Agent) handleAPITaskCancel(w http.ResponseWriter, r *http.Request) {
	taskPath := "/api/tasks/" + url.PathEscape(r.PathValue("id")) + "/cancel"
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, taskPath)
		return
	}
	if a.isMostCapable() {
		a.handleTaskCancel(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks/"+url.PathEscape(r.PathValue("id"))+"/cancel")
}

func (a *Agent) handleAPITaskApprove(w http.ResponseWriter, r *http.Request) {
	a.proxyTaskAction(w, r, "/approve")
}

func (a *Agent) handleAPITaskResume(w http.ResponseWriter, r *http.Request) {
	a.proxyTaskAction(w, r, "/resume")
}

func (a *Agent) handleAPITaskCheckpoints(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks/"+url.PathEscape(r.PathValue("id"))+"/checkpoints")
		return
	}
	writeError(w, apperr.UnavailableError(errors.New("task checkpoints are unavailable without an orchestrator")))
}

func (a *Agent) proxyTaskAction(w http.ResponseWriter, r *http.Request, action string) {
	taskPath := "/api/tasks/" + url.PathEscape(r.PathValue("id")) + action
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, taskPath)
		return
	}
	writeError(w, apperr.UnavailableError(errors.New("task action is unavailable without an orchestrator")))
}

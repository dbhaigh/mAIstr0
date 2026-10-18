function renderTaskHistoryTable(container, tasks, idPrefix, linkNodes = false) {
  const expandedTaskIds = new Set(
    Array.from(container.querySelectorAll?.(".task-details-row:not([hidden])") || [])
      .map((item) => item.dataset.taskId)
      .filter(Boolean)
  );
  container.innerHTML = "";
  const validStatuses = new Set(["pending", "running", "synthesizing", "awaiting_approval", "completed", "failed", "cancelled"]);
  const formatTimestamp = (label, value) => {
    if (!value) return `<span class="task-time-unavailable">${escapeHtml(label)} unavailable</span>`;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return `<span class="task-time-unavailable">${escapeHtml(label)} unavailable</span>`;
    }
    return `<time datetime="${escapeHtml(value)}">${escapeHtml(label)} ${escapeHtml(date.toLocaleString())}</time>`;
  };
  const table = document.createElement("table");
  table.className = "task-history-table";
  table.innerHTML = `
    <thead><tr>
      <th scope="col"><span class="sr-only">Expand</span></th>
      <th scope="col">Task</th>
      <th scope="col">Task ID</th>
      <th scope="col">Steps</th>
      <th scope="col">Status</th>
    </tr></thead>
  `;
  const tbody = document.createElement("tbody");
  table.appendChild(tbody);
  container.appendChild(table);
  for (const [index, t] of tasks.entries()) {
    const status = validStatuses.has(t.status) ? t.status : "unknown";
    const isExpanded = expandedTaskIds.has(t.id);
    const detailsId = `${idPrefix}-details-${index}`;
    const subtasks = t.subtasks || [];
    const completedCount = subtasks.filter((subtask) => subtask.status === "completed").length;
    const subtaskHTML = subtasks.map((s) => {
      const subtaskStatus = validStatuses.has(s.status) ? s.status : "unknown";
      const subtask = s.subtask || {};
      const node = s.node_id
        ? linkNodes
          ? `<button class="activity-assignment" type="button" data-target-node="${escapeHtml(s.node_id)}">${escapeHtml(s.node_id)}</button>`
          : escapeHtml(s.node_id)
        : "Unassigned";
      const attempts = Array.isArray(s.attempts) ? s.attempts : [];
      const attemptTimestamps = attempts.length
        ? attempts.map((attempt, attemptIndex) => {
          const attemptNumber = Number(attempt.number) || attemptIndex + 1;
          const attemptStatus = typeof attempt.status === "string" ? attempt.status : "unknown";
          const durationMs = Number(attempt.duration_ms);
          const details = [
            `<strong>Attempt ${attemptNumber} · ${escapeHtml(attemptStatus)}</strong>`,
            attempt.id ? `<span>Attempt ID: ${escapeHtml(attempt.id)}</span>` : "",
            attempt.request_id ? `<span>Request ID: ${escapeHtml(attempt.request_id)}</span>` : "",
            attempt.node_id ? `<span>Node: ${escapeHtml(attempt.node_id)}</span>` : "",
            attempt.model ? `<span>Model: ${escapeHtml(attempt.model)}</span>` : "",
            attempt.engine ? `<span>Engine: ${escapeHtml(attempt.engine)}</span>` : "",
            attempt.failure_class ? `<span>Failure: ${escapeHtml(attempt.failure_class)}</span>` : "",
            attempt.http_status ? `<span>HTTP ${escapeHtml(attempt.http_status)}</span>` : "",
            Number.isFinite(durationMs) && durationMs >= 0 ? `<span>Duration: ${durationMs} ms</span>` : "",
            attempt.error ? `<span>Error: ${escapeHtml(attempt.error)}</span>` : "",
          ].join("");
          return `<div class="task-step-attempt">${details}${formatTimestamp("Started", attempt.started_at)}${formatTimestamp("Finished", attempt.finished_at)}</div>`;
        }).join("")
        : `<div class="task-step-attempt"><span>Not dispatched</span>${formatTimestamp("Task created", t.created_at)}</div>`;
      return `
        <article class="task-step">
          <div class="task-step-heading"><strong>${escapeHtml(subtask.id || "Subtask")}</strong><span class="badge ${subtaskStatus}">${escapeHtml(subtaskStatus.replaceAll("_", " "))}</span></div>
          <div class="task-step-target">${escapeHtml(subtask.role || subtask.task_type || "Task step")} · ${escapeHtml(subtask.task_type || "general")} · ${node} / ${escapeHtml(s.model || "No model")}</div>
          <div class="task-step-timestamps">${attemptTimestamps}</div>
          ${subtask.description ? `<p>${escapeHtml(subtask.description)}</p>` : ""}
          ${s.error ? `<p class="task-error-reason"><strong>Failure reason:</strong> ${escapeHtml(s.error)}</p>` : ""}
          ${s.score_factors ? `<details class="routing-score-details"><summary>Routing score ${Number(s.score || 0).toFixed(1)}</summary><div class="catalog-tags">Model match +${Number(s.score_factors.model_match || 0).toFixed(1)} · capability +${Number(s.score_factors.capability_score || 0).toFixed(1)} · quality +${Number(s.score_factors.model_quality_score || 0).toFixed(1)} · hardware +${Number(s.score_factors.hardware || 0).toFixed(1)} · GPU preference +${Number(s.score_factors.gpu_compatibility_score || 0).toFixed(1)} · load −${Number(s.score_factors.load_penalty || 0).toFixed(1)} (${s.score_factors.active_tasks || 0} active + ${s.score_factors.queued_tasks || 0} queued) · topology −${Number(s.score_factors.topology_score || 0).toFixed(1)} · GPU −${Number(s.score_factors.gpu_utilization_penalty || 0).toFixed(1)} · NPU −${Number(s.score_factors.npu_utilization_penalty || 0).toFixed(1)} · learned ${Number(s.score_factors.learned_bias || 0) >= 0 ? "+" : ""}${Number(s.score_factors.learned_bias || 0).toFixed(1)} · telemetry ${s.score_factors.telemetry_fresh ? `fresh (${s.score_factors.telemetry_age_ms || 0}ms)` : "stale/unavailable, ignored"}</div></details>` : ""}
          ${s.output ? `<div class="subtask-output-wrap"><button class="copy-output-btn" type="button" data-copy-output="${encodeURIComponent(s.output)}">Copy output</button><pre class="subtask-output">${escapeHtml(s.output)}</pre></div>` : ""}
        </article>
      `;
    }).join("");
    const item = document.createElement("tr");
    item.className = `task-history-row task-history-row-${status}`;
    item.dataset.taskId = t.id;
    item.innerHTML = `
      <td><button class="task-expand-btn" type="button" aria-expanded="${isExpanded}" aria-controls="${detailsId}" aria-label="Show details for ${escapeHtml(t.description || t.id)}"><span class="task-expand-icon" aria-hidden="true">›</span></button></td>
      <td><span class="task-history-title" title="${escapeHtml(t.description || t.id)}">${escapeHtml(t.description || t.id)}</span><span class="task-history-timestamp">${formatTimestamp("Created", t.created_at)}</span></td>
      <td><span class="task-history-id" title="${escapeHtml(t.id)}">${escapeHtml(t.id)}</span></td>
      <td>${subtasks.length ? `${completedCount} / ${subtasks.length}` : "—"}</td>
      <td><span class="badge ${status}">${escapeHtml(status.replaceAll("_", " "))}</span></td>
    `;
    const detailsRow = document.createElement("tr");
    detailsRow.id = detailsId;
    detailsRow.className = "task-details-row";
    detailsRow.dataset.taskId = t.id;
    detailsRow.hidden = !isExpanded;
    detailsRow.innerHTML = `
      <td colspan="5"><div class="task-details-content">
      <div class="task-menu-meta">
        ${formatTimestamp("Created", t.created_at)}
        ${t.updated_at ? formatTimestamp("Updated", t.updated_at) : ""}
      </div>
      <div class="task-actions">
        ${status === "running" || status === "awaiting_approval" ? `<button class="cancel-task-btn" type="button" data-task-id="${escapeHtml(t.id)}">Cancel</button>` : ""}
        ${status === "awaiting_approval" ? `<button class="approve-task-btn primary-btn" type="button" data-task-id="${escapeHtml(t.id)}">Approve and synthesize</button>` : ""}
        ${t.resumable && (status === "failed" || status === "cancelled") ? `<button class="resume-task-btn primary-btn" type="button" data-task-id="${escapeHtml(t.id)}">Resume</button>` : ""}
      </div>
      ${t.error ? `<p class="task-error-reason"><strong>Task failure reason:</strong> ${escapeHtml(t.error)}</p>` : ""}
      ${t.last_checkpoint ? `<div class="task-checkpoint">Last checkpoint: ${escapeHtml(t.last_checkpoint)}</div>` : ""}
      ${subtasks.length ? `<div class="task-subtasks">${subtaskHTML}</div>` : '<p class="task-history-empty">No subtasks recorded.</p>'}
      ${t.result ? `<section class="task-result"><h4>Final result</h4><pre class="subtask-output">${escapeHtml(t.result)}</pre></section>` : ""}
      </div></td>
    `;
    tbody.appendChild(item);
    tbody.appendChild(detailsRow);
    const expandButton = item.querySelector(".task-expand-btn");
    expandButton?.addEventListener("click", () => {
      detailsRow.hidden = !detailsRow.hidden;
      expandButton.setAttribute("aria-expanded", String(!detailsRow.hidden));
      expandButton.setAttribute("aria-label", `${detailsRow.hidden ? "Show" : "Hide"} details for ${t.description || t.id}`);
    });
    detailsRow.querySelector(".resume-task-btn")?.addEventListener("click", async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      try {
        await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/resume`, { method: "POST" });
      } catch (err) {
        button.disabled = false;
        if (submitStatus) submitStatus.textContent = `Unable to resume ${t.id}: ${err.message}`;
      }
    });
    detailsRow.querySelector(".cancel-task-btn")?.addEventListener("click", async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      try {
        await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/cancel`, { method: "POST" });
      } catch (err) {
        if (submitStatus) submitStatus.textContent = `Unable to cancel ${t.id}: ${err.message}`;
      }
    });
    detailsRow.querySelector(".approve-task-btn")?.addEventListener("click", async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      try {
        await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/approve`, { method: "POST" });
      } catch (err) {
        button.disabled = false;
        if (submitStatus) submitStatus.textContent = `Unable to approve ${t.id}: ${err.message}`;
      }
    });
    detailsRow.querySelectorAll("[data-copy-output]").forEach((button) => {
      button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copyOutput), button));
    });
  }
}

function renderTasks(tasks, hasMore = false, loading = false) {
  if (!tasksList) return;
  if (!tasks || tasks.length === 0) {
    tasksList.innerHTML = '<p class="task-history-empty">No tasks submitted yet.</p>';
    if (taskHistoryCount) taskHistoryCount.textContent = dashboardViewModel.taskHistoryCountLabel(0, 0);
    appendTaskHistoryLoadMore(hasMore, loading);
    return;
  }
  const query = (taskHistorySearch?.value || "").trim().toLowerCase();
  const statusFilter = taskHistoryStatus?.value || "all";
  const orderedTasks = [...tasks]
    .filter((task) => {
      const status = task.status || "unknown";
      if (statusFilter !== "all" && status !== statusFilter) return false;
      if (!query) return true;
      const searchable = [
        task.id, task.description, status, task.result, task.error,
        ...(task.subtasks || []).flatMap((item) => [
          item.subtask?.id, item.subtask?.description, item.subtask?.role,
          item.subtask?.task_type, item.node_id, item.model, item.output, item.error,
        ]),
      ].filter(Boolean).join(" ").toLowerCase();
      return searchable.includes(query);
    })
    .sort((a, b) =>
      new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
  if (taskHistoryCount) {
    taskHistoryCount.textContent = dashboardViewModel.taskHistoryCountLabel(orderedTasks.length, tasks.length);
  }
  if (orderedTasks.length === 0) {
    tasksList.innerHTML = '<p class="task-history-empty">No tasks match the selected filters.</p>';
    appendTaskHistoryLoadMore(hasMore, loading);
    return;
  }
  renderTaskHistoryTable(tasksList, orderedTasks, "task-history");
  appendTaskHistoryLoadMore(hasMore, loading);
}

function appendTaskHistoryLoadMore(hasMore, loading) {
  if (!hasMore || !tasksList) return;
  const button = document.createElement("button");
  button.className = "icon-button history-load-more";
  button.type = "button";
  button.textContent = loading ? "Loading…" : "Load more tasks";
  button.disabled = loading;
  button.addEventListener("click", loadMoreTaskHistory);
  tasksList.appendChild(button);
}

let taskHistoryFilterTimer = null;
taskHistorySearch?.addEventListener("input", () => {
  clearTimeout(taskHistoryFilterTimer);
  taskHistoryFilterTimer = setTimeout(() => refreshTaskHistory(true), 300);
});
taskHistoryStatus?.addEventListener("change", () => refreshTaskHistory(true));

async function copyText(text, button) {
  try {
    await navigator.clipboard.writeText(text);
    const original = button.textContent;
    button.textContent = "Copied";
    setTimeout(() => { button.textContent = original; }, 1200);
  } catch (err) {
    button.textContent = "Copy failed";
  }
}

function taskText(tasks) {
  return (tasks || []).map((t) => [
    `Task ${t.id}: ${t.description}`,
    ...(t.subtasks || []).map((s) => `${s.subtask.id} (${s.status}) on ${s.node_id}:\n${s.output || s.error || ""}`),
  ].join("\n")).join("\n\n");
}

copyTasksBtn?.addEventListener("click", () => copyAllTaskResults());

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

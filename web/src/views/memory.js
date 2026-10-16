// --- Persistent memory & learning ---

async function refreshMemory() {
  try {
    const query = memorySearch?.value.trim() || "";
    const experienceURL = query
      ? `/api/memory/experiences?q=${encodeURIComponent(query)}`
      : "/api/memory/experiences";
    const [insights, facts, experiences, dialogues] = await Promise.all([
      fetchJSON("/api/memory/insights"),
      fetchMemoryHistoryPage("facts", "/api/memory/facts", true),
      fetchMemoryHistoryPage("experiences", experienceURL, true),
      fetchMemoryHistoryPage("dialogues", "/api/memory/dialogues", true),
    ]);
    renderLearning(insights);
    renderFacts(facts, memoryHistoryPages.facts.hasMore, memoryHistoryPages.facts.loading);
    renderExperiences(experiences, memoryHistoryPages.experiences.hasMore, memoryHistoryPages.experiences.loading);
    renderTranscripts(dialogues, memoryHistoryPages.dialogues.hasMore, memoryHistoryPages.dialogues.loading);
  } catch (err) {
    console.error("memory refresh failed", err);
  }
}

async function fetchMemoryHistoryPage(name, baseURL, reset = false) {
  const state = memoryHistoryPages[name];
  if (!state) throw new Error(`unknown memory history page: ${name}`);
  if (reset) {
    state.generation++;
    state.items = [];
    state.nextCursor = "";
    state.hasMore = false;
    state.loading = false;
    state.url = baseURL;
  }
  if (state.loading || (!reset && !state.hasMore)) return state.items;
  const generation = state.generation;
  state.loading = true;
  const pageSize = name === "dialogues" ? 20 : name === "experiences" ? 30 : 50;
  const separator = state.url.includes("?") ? "&" : "?";
  let url = `${state.url}${separator}page_size=${pageSize}`;
  if (!reset && state.nextCursor) url += `&cursor=${encodeURIComponent(state.nextCursor)}`;
  try {
    const response = await fetchJSON(url);
    const page = Array.isArray(response)
      ? { items: response, next_cursor: "", has_more: false }
      : response;
    if (generation !== state.generation) return state.items;
    const items = Array.isArray(page?.items) ? page.items : [];
    if (reset) {
      state.items = items;
    } else {
      const itemKey = (item) => name === "facts" ? `${item.scope || "cluster"}|${item.key}` : item.id;
      const merged = new Map(state.items.map((item) => [itemKey(item), item]));
      for (const item of items) merged.set(itemKey(item), item);
      state.items = [...merged.values()];
    }
    state.nextCursor = page?.next_cursor || "";
    state.hasMore = Boolean(page?.has_more);
    return state.items;
  } finally {
    if (generation === state.generation) state.loading = false;
  }
}

async function loadMoreMemoryHistory(name) {
  const state = memoryHistoryPages[name];
  if (!state || state.loading || !state.hasMore || !state.nextCursor) return;
  try {
    await fetchMemoryHistoryPage(name, state.url);
  } catch (err) {
    console.error(`loading more ${name} failed`, err);
  }
  if (name === "facts") renderFacts(state.items, state.hasMore, state.loading);
  if (name === "experiences") renderExperiences(state.items, state.hasMore, state.loading);
  if (name === "dialogues") renderTranscripts(state.items, state.hasMore, state.loading);
}

function appendMemoryLoadMore(container, name, hasMore, loading) {
  if (!container || !hasMore) return;
  const button = document.createElement("button");
  button.className = "icon-button history-load-more";
  button.type = "button";
  button.textContent = loading ? "Loading…" : "Load more";
  button.disabled = loading;
  button.addEventListener("click", () => loadMoreMemoryHistory(name));
  container.appendChild(button);
}

function renderLearning(insights) {
  if (!learningSummary) return;
  if (!insights || !insights.total_experiences) {
    learningSummary.innerHTML = '<p style="color:var(--muted)">No experience recorded yet. The cluster starts learning as soon as it runs work.</p>';
    return;
  }

  const pairings = (insights.top_pairings || [])
    .map((p) => `<div class="row"><span>${escapeHtml(p.model)} on ${escapeHtml(p.node_id)} (${escapeHtml(p.task_type)})</span><span class="badge ${p.success_rate >= 0.7 ? "completed" : "failed"}">${Math.round(p.success_rate * 100)}% · ${p.avg_duration_ms}ms</span></div>`)
    .join("");

  const lessons = (insights.lessons || [])
    .map((l) => `<div class="lesson${/Avoid/i.test(l) ? " negative" : ""}">${escapeHtml(l)}</div>`)
    .join("") || '<p style="color:var(--muted);font-size:0.82rem">Not enough evidence for firm lessons yet.</p>';

  const taskTypes = Object.entries(insights.task_types || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `<div class="row"><span>${escapeHtml(k)}</span><span>${v}</span></div>`)
    .join("");

  learningSummary.innerHTML = `
    <div class="card">
      <div class="memory-stat-grid">
        <div class="memory-stat"><span class="memory-stat-value">${insights.total_experiences}</span><span class="memory-stat-label">Experiences</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${Math.round((insights.success_rate || 0) * 100)}%</span><span class="memory-stat-label">Success rate</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${insights.avg_duration_ms || 0}ms</span><span class="memory-stat-label">Avg duration</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${insights.tracked_pairings || 0}</span><span class="memory-stat-label">Learned pairings</span></div>
      </div>
      <h4>Lessons applied to routing</h4>
      ${lessons}
      <h4>Best node + model pairings</h4>
      ${pairings || '<p style="color:var(--muted);font-size:0.8rem">None yet.</p>'}
      <h4>Work seen by type</h4>
      ${taskTypes || '<p style="color:var(--muted);font-size:0.8rem">None yet.</p>'}
      <div class="row"><span>Database</span><span>${escapeHtml(insights.database_path || "")}</span></div>
    </div>`;
}

function renderFacts(facts, hasMore = false, loading = false) {
  if (!factsList) return;
  factsList.innerHTML = "";
  if (!facts || facts.length === 0) {
    factsList.innerHTML = '<p style="color:var(--muted)">Nothing remembered yet.</p>';
    appendMemoryLoadMore(factsList, "facts", hasMore, loading);
    return;
  }
  for (const f of facts) {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h3>${escapeHtml(f.key)} <span class="badge healthy">${escapeHtml(f.scope || "cluster")}</span></h3>
      <div class="row"><span>${escapeHtml(f.value)}</span></div>
      <div class="row"><span>Source</span><span>${escapeHtml(f.source || "unknown")}</span></div>
      <div class="row"><span>Recalled</span><span>${f.hits || 0}×</span></div>
      <button class="icon-button" data-forget="${escapeHtml(f.key)}" data-scope="${escapeHtml(f.scope || "cluster")}">Forget</button>
    `;
    card.querySelector("[data-forget]")?.addEventListener("click", async (e) => {
      const key = e.target.dataset.forget;
      const scope = e.target.dataset.scope;
      await fetch(`/api/memory/facts/${encodeURIComponent(key)}?scope=${encodeURIComponent(scope)}`, { method: "DELETE" });
      refreshMemory();
    });
    factsList.appendChild(card);
  }
  appendMemoryLoadMore(factsList, "facts", hasMore, loading);
}

function renderExperiences(experiences, hasMore = false, loading = false) {
  if (!experiencesList) return;
  experiencesList.innerHTML = "";
  if (!experiences || experiences.length === 0) {
    experiencesList.innerHTML = '<p style="color:var(--muted)">No matching experience recorded.</p>';
    appendMemoryLoadMore(experiencesList, "experiences", hasMore, loading);
    return;
  }
  for (const e of experiences) {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h3>${escapeHtml(e.kind || "task")} <span class="badge ${e.success ? "completed" : "failed"}">${e.success ? "success" : "failed"}</span></h3>
      <div class="row"><span>${escapeHtml(e.description || "")}</span></div>
      <div class="row"><span>${escapeHtml(e.node_id || "-")} / ${escapeHtml(e.model || "-")}</span><span>${e.duration_ms || 0}ms</span></div>
      <div class="row"><span>When</span><span>${new Date(e.created_at).toLocaleString()}</span></div>
      ${e.output ? `<div class="subtask-output">${escapeHtml(e.output.slice(0, 600))}</div>` : ""}
      ${e.error ? `<div class="subtask-output" style="border-color:var(--bad)">${escapeHtml(e.error)}</div>` : ""}
    `;
    experiencesList.appendChild(card);
  }
  appendMemoryLoadMore(experiencesList, "experiences", hasMore, loading);
}

function renderTranscripts(dialogues, hasMore = false, loading = false) {
  if (!transcriptsList) return;
  transcriptsList.innerHTML = "";
  if (!dialogues || dialogues.length === 0) {
    transcriptsList.innerHTML = '<p style="color:var(--muted)">No stored conversations yet.</p>';
    appendMemoryLoadMore(transcriptsList, "dialogues", hasMore, loading);
    return;
  }
  for (const d of dialogues) {
    const card = document.createElement("div");
    card.className = "card";
    const turns = (d.turns || []).slice(-6)
      .map((t) => `<div class="node-msg"><strong>${escapeHtml(t.speaker)}:</strong> ${escapeHtml((t.content || "").slice(0, 400))}</div>`)
      .join("");
    card.innerHTML = `
      <h3>${escapeHtml(d.id)} <span class="badge healthy">${(d.turns || []).length} turns</span></h3>
      <div class="row"><span>Participants</span><span>${escapeHtml((d.participants || []).join(", "))}</span></div>
      ${d.topic ? `<div class="row"><span>Topic</span><span>${escapeHtml(d.topic)}</span></div>` : ""}
      ${turns}
    `;
    transcriptsList.appendChild(card);
  }
  appendMemoryLoadMore(transcriptsList, "dialogues", hasMore, loading);
}

factForm?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const key = factKey.value.trim();
  const value = factValue.value.trim();
  if (!key || !value) return;
  try {
    await fetchJSON("/api/memory/facts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key, value, scope: "cluster" }),
    });
    factKey.value = "";
    factValue.value = "";
    refreshMemory();
  } catch (err) {
    console.error("failed to save fact", err);
  }
});

let memorySearchTimer = null;
memorySearch?.addEventListener("input", () => {
  clearTimeout(memorySearchTimer);
  memorySearchTimer = setTimeout(refreshMemory, 300);
});

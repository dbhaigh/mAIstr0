// --- Interactive Orchestrator Functions ---

async function loadAgentTools() {
  try {
    const tools = await fetchJSON("/api/agent/tools");
    if (!agentToolsList) return;
    agentToolsList.innerHTML = "";
    for (const t of tools) {
      const div = document.createElement("div");
      div.className = "tool-pill";
      div.innerHTML = `
        <div class="tool-pill-name">⚡ ${escapeHtml(t.name)}</div>
        <div class="tool-pill-desc">${escapeHtml(t.description)}</div>
      `;
      agentToolsList.appendChild(div);
    }
  } catch (err) {
    console.error("failed to load agent tools", err);
  }
}

async function loadAgentBackend() {
  try {
    applyAgentBackend(await fetchJSON("/api/agent/backend"));
  } catch (err) {
    console.error("failed to load agent backend", err);
    if (agentHarnessStatus) agentHarnessStatus.textContent = `Could not load harness: ${err.message}`;
  }
}

function applyAgentBackend(backend) {
  currentAgentHarness = backend.name;
  agentSupportsCoordinatorModel = Boolean(backend.supports_coordinator_model);
  const mode = activeChatMode === "direct" ? "direct" : backend.name;
  activeChatMode = mode;
  agentTabs.forEach((tab) => {
    const active = tab.dataset.agent === mode;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  if (agentHarnessSelect) agentHarnessSelect.value = backend.name;
  if (agentSettingsTitle) {
    agentSettingsTitle.textContent = `${backend.name === "pi" ? "Pi" : "DeepSeek"} Agent Settings`;
  }
  const modelLabel = agentModelSelect?.closest("label");
  const stepsLabel = agentStepsSelect?.closest("label");
  if (modelLabel) modelLabel.hidden = !backend.supports_coordinator_model;
  if (stepsLabel) stepsLabel.hidden = !backend.supports_coordinator_model;
  if (agentHarnessStatus) {
    agentHarnessStatus.textContent = `New conversations use ${backend.name === "pi" ? "Pi" : "DeepSeek"}; existing conversations stay on their harness.`;
  }
  setAgentConsoleMode(mode);
}

agentHarnessSelect?.addEventListener("change", async () => {
  const requested = agentHarnessSelect.value;
  await activateAgentTab(requested);
});

let clusterModelsLoaded = false;

async function loadClusterModels() {
  if (clusterModelsLoaded) return;
  try {
    const models = await fetchJSON("/api/agent/models");
    if (!agentModelSelect) return;
    const currentVal = agentModelSelect.value;
    agentModelSelect.innerHTML = '<option value="">Auto (Fastest / Cluster Leader)</option>';
    const seen = new Set();
    for (const m of models) {
      if (seen.has(m.name)) continue;
      seen.add(m.name);
      const opt = document.createElement("option");
      opt.value = m.name;
      opt.textContent = `${m.name} (${(m.tags || []).join(", ")}) · on ${m.node_id}`;
      if (m.name === currentVal) opt.selected = true;
      agentModelSelect.appendChild(opt);
    }
    clusterModelsLoaded = true;
  } catch (err) {
    console.error("failed to load cluster models", err);
  }
}

async function loadSessions() {
  try {
    dashboardState.sessions = await fetchJSON("/api/agent/sessions");
    renderSessionList();
    if (activeChatMode === "direct") return;
    const sessions = sessionsForHarness(currentAgentHarness);
    const preferredID = activeSessionIds.get(currentAgentHarness);
    const sessionID = sessions.some((session) => session.id === preferredID)
      ? preferredID
      : sessions[0]?.id;
    if (sessionID) {
      await selectSession(sessionID);
    } else {
      await createNewSession("New Interactive Session");
    }
  } catch (err) {
    console.error("failed to load sessions", err);
  }
}

function sessionsForHarness(harness) {
  return dashboardState.sessions.filter((session) => (session.harness || "pi") === harness);
}

function renderSessionList() {
  if (!sessionListEl) return;
  sessionListEl.innerHTML = "";
  for (const s of sessionsForHarness(currentAgentHarness)) {
    const div = document.createElement("div");
    div.className = "session-item" + (s.id === activeSessionId ? " active" : "");
    div.setAttribute("role", "button");
    div.tabIndex = 0;
    div.setAttribute("aria-current", String(s.id === activeSessionId));
    div.innerHTML = `
      <span class="session-info">
        <span class="session-title">${escapeHtml(s.title || "Session " + s.id.slice(-4))}</span>
        <small class="session-harness">${s.harness === "deepseek" ? "DeepSeek" : "Pi"}</small>
      </span>
      <button class="session-del-btn" data-del="${s.id}" title="Delete session">&times;</button>
    `;
    div.addEventListener("click", (e) => {
      if (isSending || e.target.dataset.del) return;
      selectSession(s.id);
    });
    div.addEventListener("keydown", (e) => {
      if (!isSending && (e.key === "Enter" || e.key === " ")) {
        e.preventDefault();
        selectSession(s.id);
      }
    });
    div.querySelector(".session-del-btn")?.addEventListener("click", async (e) => {
      e.stopPropagation();
      if (!isSending) await deleteSession(s.id);
    });
    sessionListEl.appendChild(div);
  }
}

async function selectSession(id) {
  activeSessionId = id;
  const sessionInfo = dashboardState.sessions.find((session) => session.id === id);
  if (sessionInfo) activeSessionIds.set(sessionInfo.harness || "pi", id);
  renderSessionList();
  try {
    const session = await fetchJSON(`/api/agent/sessions/${encodeURIComponent(id)}`);
    if (activeSessionId === id) renderChatMessages(session);
  } catch (err) {
    console.error("failed to load session", id, err);
  }
}

async function createNewSession(title = "") {
  if (isSending) return;
  try {
    const session = await fetchJSON("/api/agent/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title: title || `Session ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`,
        coordinator_model: agentModelSelect?.value || "",
        max_steps: parseInt(agentStepsSelect?.value || "8", 10),
      }),
    });
    dashboardState.sessions.unshift(session);
    activeSessionIds.set(currentAgentHarness, session.id);
    await selectSession(session.id);
  } catch (err) {
    console.error("failed to create session", err);
  }
}

async function deleteSession(id) {
  try {
    await fetch(`/api/agent/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
    dashboardState.sessions = dashboardState.sessions.filter((s) => s.id !== id);
    if (activeSessionId === id) {
      const sessions = sessionsForHarness(currentAgentHarness);
      activeSessionId = sessions[0]?.id || null;
      if (activeSessionId) activeSessionIds.set(currentAgentHarness, activeSessionId);
      else activeSessionIds.delete(currentAgentHarness);
      if (activeSessionId) {
        await selectSession(activeSessionId);
      } else {
        await createNewSession();
      }
    } else {
      renderSessionList();
    }
  } catch (err) {
    console.error("failed to delete session", err);
  }
}

function renderChatMessages(session) {
  if (!chatMessagesEl) return;
  dashboardDOM.resetVirtualList(chatMessagesEl, { estimateHeight: 140, threshold: 50, overscan: 6 });
  if (!session || !session.messages || session.messages.length === 0) {
    const welcome = document.createElement("div");
    welcome.className = "welcome-card";
    welcome.innerHTML = "<h2>What would you like to orchestrate?</h2><p>Ask a question or give a task to your local cluster.</p>";
    dashboardDOM.appendVirtualListItem(chatMessagesEl, welcome);
    return;
  }

  for (const msg of session.messages) {
    appendMessageToChat(msg);
  }
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}

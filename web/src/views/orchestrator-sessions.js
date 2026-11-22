import { fetchJSON } from "../shared/api.js";
import { dashboardDOM } from "../shared/dom.js";
import { escapeHtml } from "../shared/format.js";
import {
  activeChatMode,
  activeSessionId,
  activeSessionIds,
  agentHarnessSelect,
  agentHarnessStatus,
  agentModelSelect,
  agentSettingsTitle,
  chatInput,
  agentStepsSelect,
  agentSupportsCoordinatorModel,
  agentTabs,
  agentToolsList,
  chatMessagesEl,
  currentAgentHarness,
  dashboardState,
  isSending,
  newSessionBtn,
  sendMsgBtn,
  sessionListEl,
  setActiveChatMode,
  setActiveSessionId,
  setAgentSupportsCoordinatorModel,
  setCurrentAgentHarness,
} from "../shared/state.js";
import { appendMessageToChat } from "./orchestrator-chat.js";
import { activateAgentTab, setAgentConsoleMode } from "./orchestrator-controls.js";

// --- Interactive Orchestrator Functions ---

let backendAvailability = new Map();
let activeBackendAvailable = true;

function backendLabel(name) {
  if (name === "pi") return "Pi";
  if (name === "deepseek") return "DeepSeek";
  return name.replace(/[-_]+/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function sessionIsAvailable(session) {
  const backend = backendAvailability.get(session.harness || "pi");
  return session.available !== false && backend?.available !== false;
}

function updateNewSessionAvailability() {
  if (newSessionBtn) newSessionBtn.disabled = isSending || !activeBackendAvailable;
}

export async function loadAgentTools() {
  if (dashboardState.agentCapabilities?.tool_access === false) {
    if (agentToolsList) agentToolsList.innerHTML = "";
    return;
  }
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

export async function loadAgentBackend() {
  try {
    applyAgentBackend(await fetchJSON("/api/agent/backend"));
  } catch (err) {
    console.error("failed to load agent backend", err);
    if (agentHarnessStatus) agentHarnessStatus.textContent = `Could not load harness: ${err.message}`;
  }
}

export function applyAgentBackend(backend) {
  setCurrentAgentHarness(backend.name);
  const backends = Array.isArray(backend.backends)
    ? backend.backends
    : (backend.harnesses || []).map((name) => ({
      name,
      available: true,
      capabilities: { model_selection: Boolean(backend.supports_coordinator_model), tool_access: true },
    }));
  dashboardState.agentBackends = backends;
  backendAvailability = new Map(backends.map((item) => [item.name, item]));
  dashboardState.agentCapabilities = backend.capabilities || null;
  activeBackendAvailable = backend.available !== false;
  dashboardState.activeBackendAvailable = activeBackendAvailable;
  const capabilities = backend.capabilities || {};
  const supportsModelSelection = backend.capabilities
    ? Boolean(capabilities.model_selection)
    : Boolean(backend.supports_coordinator_model);
  setAgentSupportsCoordinatorModel(supportsModelSelection);
  const mode = activeChatMode === "direct" ? "direct" : backend.name;
  setActiveChatMode(mode);
  agentTabs.forEach((tab) => {
    const active = tab.dataset.agent === mode;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  if (agentHarnessSelect) {
    agentHarnessSelect.replaceChildren();
    for (const item of backends.filter((candidate) => candidate.available)) {
      const option = document.createElement("option");
      option.value = item.name;
      option.textContent = backendLabel(item.name);
      agentHarnessSelect.appendChild(option);
    }
    if (!activeBackendAvailable) {
      const option = document.createElement("option");
      option.value = backend.name;
      option.textContent = `${backendLabel(backend.name)} (unavailable)`;
      option.disabled = true;
      agentHarnessSelect.appendChild(option);
    }
    agentHarnessSelect.value = backend.name;
  }
  if (agentSettingsTitle) {
    agentSettingsTitle.textContent = `${backendLabel(backend.name)} Agent Settings`;
  }
  const modelLabel = agentModelSelect?.closest("label");
  const stepsLabel = agentStepsSelect?.closest("label");
  if (modelLabel) modelLabel.hidden = !supportsModelSelection;
  if (stepsLabel) stepsLabel.hidden = !supportsModelSelection;
  if (agentHarnessStatus) {
    agentHarnessStatus.textContent = activeBackendAvailable
      ? `New conversations use ${backendLabel(backend.name)}; existing conversations stay on their harness.`
      : `${backendLabel(backend.name)} is unavailable. Existing session history is preserved; no other harness will be substituted.`;
  }
  updateNewSessionAvailability();
  setAgentConsoleMode(mode);
}

agentHarnessSelect?.addEventListener("change", async () => {
  const requested = agentHarnessSelect.value;
  await activateAgentTab(requested);
});

let clusterModelsLoaded = false;

export async function loadClusterModels() {
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

export async function loadSessions() {
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
    } else if (activeBackendAvailable) {
      await createNewSession("New Interactive Session");
    } else {
      renderSessionList();
    }
  } catch (err) {
    console.error("failed to load sessions", err);
  }
}

export function sessionsForHarness(harness) {
  return dashboardState.sessions.filter((session) => (session.harness || "pi") === harness);
}

export function renderSessionList() {
  if (!sessionListEl) return;
  sessionListEl.innerHTML = "";
  const selectedSessions = sessionsForHarness(currentAgentHarness);
  const unavailableSessions = dashboardState.sessions.filter((session) =>
    !sessionIsAvailable(session) && !selectedSessions.some((selected) => selected.id === session.id)
  );
  for (const s of [...selectedSessions, ...unavailableSessions]) {
    const available = sessionIsAvailable(s);
    const harnessLabel = available
      ? backendLabel(s.harness || "pi")
      : `Unavailable · ${backendLabel(s.harness || "pi")}`;
    const div = document.createElement("div");
    div.className = "session-item" + (s.id === activeSessionId ? " active" : "");
    div.setAttribute("role", "button");
    div.tabIndex = 0;
    div.setAttribute("aria-current", String(s.id === activeSessionId));
    div.innerHTML = `
      <span class="session-info">
        <span class="session-title">${escapeHtml(s.title || "Session " + s.id.slice(-4))}</span>
        <small class="session-harness">${escapeHtml(harnessLabel)}</small>
      </span>
      <button class="session-del-btn" data-del="${s.id}" title="Delete session">&times;</button>
    `;
    div.addEventListener("click", (e) => {
      if (isSending || e.target?.dataset?.del) return;
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

export async function selectSession(id) {
  setActiveSessionId(id);
  const sessionInfo = dashboardState.sessions.find((session) => session.id === id);
  if (sessionInfo) {
    activeSessionIds.set(sessionInfo.harness || "pi", id);
    const available = sessionIsAvailable(sessionInfo);
    if (chatInput) chatInput.disabled = !available;
    if (sendMsgBtn) sendMsgBtn.disabled = !available;
    if (!available && agentHarnessStatus) {
      agentHarnessStatus.textContent = `This ${backendLabel(sessionInfo.harness || "pi")} session is unavailable. Its stored history is preserved; select its backend when it is available to continue.`;
    } else if (agentHarnessStatus && activeBackendAvailable) {
      agentHarnessStatus.textContent = `New conversations use ${backendLabel(currentAgentHarness)}; existing conversations stay on their harness.`;
    }
  }
  renderSessionList();
  try {
    const session = await fetchJSON(`/api/agent/sessions/${encodeURIComponent(id)}`);
    if (activeSessionId === id) renderChatMessages(session);
  } catch (err) {
    console.error("failed to load session", id, err);
  }
}

export async function createNewSession(title = "") {
  if (isSending || !activeBackendAvailable) return;
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
      setActiveSessionId(sessions[0]?.id || null);
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

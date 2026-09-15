// DOM Elements
const nodesList = document.getElementById("nodes-list");
const routingList = document.getElementById("routing-list");
const discoveryList = document.getElementById("discovery-list");
const tasksList = document.getElementById("tasks-list");
const taskInput = document.getElementById("task-input");
const submitBtn = document.getElementById("submit-btn");
const submitStatus = document.getElementById("submit-status");
const copyTasksBtn = document.getElementById("copy-tasks-btn");
const refreshAllModelsBtn = document.getElementById("refresh-all-models-btn");
const clusterMap = document.getElementById("cluster-map");
const clusterLinks = document.getElementById("cluster-links");
const clusterNodes = document.getElementById("cluster-nodes");
const clusterMapEmpty = document.getElementById("cluster-map-empty");
const clusterMapStatus = document.getElementById("cluster-map-status");
const clusterPeers = document.getElementById("cluster-peers");

// Header stats & navigation
const statNodes = document.getElementById("stat-nodes");
const statModels = document.getElementById("stat-models");
const statLoad = document.getElementById("stat-load");
const statMemory = document.getElementById("stat-memory");
const statVersion = document.getElementById("stat-version");
const navTabs = document.querySelectorAll(".nav-tab");
const tabContents = document.querySelectorAll(".tab-content");

// Memory & learning elements
const learningSummary = document.getElementById("learning-summary");
const factsList = document.getElementById("facts-list");
const factForm = document.getElementById("fact-form");
const factKey = document.getElementById("fact-key");
const factValue = document.getElementById("fact-value");
const experiencesList = document.getElementById("experiences-list");
const transcriptsList = document.getElementById("transcripts-list");
const memorySearch = document.getElementById("memory-search");

// Agent Console Elements
const sessionListEl = document.getElementById("session-list");
const newSessionBtn = document.getElementById("new-session-btn");
const agentModelSelect = document.getElementById("agent-model-select");
const agentStepsSelect = document.getElementById("agent-steps-select");
const agentToolsList = document.getElementById("agent-tools-list");
const chatMessagesEl = document.getElementById("chat-messages");
const chatInput = document.getElementById("chat-input");
const sendMsgBtn = document.getElementById("send-msg-btn");
const chatStatus = document.getElementById("chat-status");
const promptChips = document.querySelectorAll(".prompt-chip");

// State
let activeSessionId = null;
let currentSessions = [];
let isSending = false;
const modelPanelOpen = new Set();
const pendingModelState = new Map();
const pendingDefaultModel = new Map();
const lastFetchedModels = new Map();
let contextNode = null;
let currentNodes = [];
let propertyNodeId = null;
const nodeProperties = new Map();
let terminalNode = null;
let terminalDialogueId = null;

// Tab Switching
navTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    navTabs.forEach((t) => t.classList.remove("active"));
    tabContents.forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    const target = document.getElementById(tab.dataset.tab);
    if (target) target.classList.add("active");
    if (tab.dataset.tab === "memory-tab") refreshMemory();
  });
});

async function fetchJSON(url, opts) {
  const resp = await fetch(url, opts);
  if (!resp.ok) {
    throw new Error(await resp.text());
  }
  if (resp.status === 204) return null;
  return resp.json();
}

function updateClusterTitle(count) {
  const title = document.querySelector("header h1");
  if (title) title.textContent = `mAIstr0 · ${count} ${count === 1 ? "Node" : "Nodes"}`;
}

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

async function loadClusterModels() {
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
  } catch (err) {
    console.error("failed to load cluster models", err);
  }
}

async function loadSessions() {
  try {
    currentSessions = await fetchJSON("/api/agent/sessions");
    renderSessionList();
    if (!activeSessionId && currentSessions.length > 0) {
      selectSession(currentSessions[0].id);
    } else if (currentSessions.length === 0) {
      await createNewSession("New Interactive Session");
    }
  } catch (err) {
    console.error("failed to load sessions", err);
  }
}

function renderSessionList() {
  if (!sessionListEl) return;
  sessionListEl.innerHTML = "";
  for (const s of currentSessions) {
    const div = document.createElement("div");
    div.className = "session-item" + (s.id === activeSessionId ? " active" : "");
    div.innerHTML = `
      <span class="session-title">${escapeHtml(s.title || "Session " + s.id.slice(-4))}</span>
      <button class="session-del-btn" data-del="${s.id}" title="Delete session">&times;</button>
    `;
    div.addEventListener("click", (e) => {
      if (e.target.dataset.del) return;
      selectSession(s.id);
    });
    div.querySelector(".session-del-btn")?.addEventListener("click", async (e) => {
      e.stopPropagation();
      await deleteSession(s.id);
    });
    sessionListEl.appendChild(div);
  }
}

async function selectSession(id) {
  activeSessionId = id;
  renderSessionList();
  try {
    const session = await fetchJSON(`/api/agent/sessions/${encodeURIComponent(id)}`);
    renderChatMessages(session);
  } catch (err) {
    console.error("failed to load session", id, err);
  }
}

async function createNewSession(title = "") {
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
    currentSessions.unshift(session);
    await selectSession(session.id);
  } catch (err) {
    console.error("failed to create session", err);
  }
}

async function deleteSession(id) {
  try {
    await fetch(`/api/agent/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
    currentSessions = currentSessions.filter((s) => s.id !== id);
    if (activeSessionId === id) {
      activeSessionId = currentSessions[0]?.id || null;
      if (activeSessionId) {
        selectSession(activeSessionId);
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
  chatMessagesEl.innerHTML = "";
  if (!session || !session.messages || session.messages.length === 0) {
    chatMessagesEl.innerHTML = `
      <div class="welcome-card">
        <h2>Welcome to the mAIstr0 Orchestrator</h2>
        <p>The orchestrator plans and reasons visibly, then spreads LLM workloads across worker nodes while preserving every node response.</p>
        <ul>
          <li><strong>Dynamic Workload Balancing:</strong> LLM queries and subtasks are routed to the least-loaded nodes.</li>
          <li><strong>Parallel Multi-Node Dispatch:</strong> Subtasks can be executed simultaneously across distinct cluster nodes.</li>
          <li><strong>Interactive ReAct Loop:</strong> Real-time streaming of thoughts, tool calls, and node execution metrics.</li>
        </ul>
      </div>
    `;
    return;
  }

  for (const msg of session.messages) {
    appendMessageToChat(msg);
  }
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}

function appendMessageToChat(msg) {
  if (!chatMessagesEl) return;
  const isUser = msg.role === "user";
  const isTool = msg.role === "tool";

  if (isTool && msg.tool_response) {
    const tr = msg.tool_response;
    const card = document.createElement("div");
    card.className = "tool-card";

    let bodyHTML = "";
    if (tr.output && typeof tr.output === "object" && tr.output.dialogue_flow) {
      // Multi-round collaboration flow
      bodyHTML = `<div class="dialogue-flow-container">`;
      for (const ex of tr.output.dialogue_flow) {
        bodyHTML += `
          <div class="dialogue-exchange">
            <div class="exchange-header"><strong>Round ${ex.round}</strong> · ${ex.duration_ms || 0}ms</div>
            <div class="orch-msg"><strong>Orchestrator:</strong> ${escapeHtml(ex.orchestrator_prompt)}</div>
            <div class="node-msg"><strong>Node LLM:</strong> ${escapeHtml(ex.node_reply)}</div>
          </div>`;
      }
      bodyHTML += `</div>`;
    } else if (tr.output && typeof tr.output === "object" && tr.output.node_reply) {
      // Single turn node converse
      bodyHTML = `
        <div class="dialogue-single-turn">
          <div class="node-msg"><strong>Node LLM (Turn ${tr.output.turn || 1}):</strong>\n${escapeHtml(tr.output.node_reply)}</div>
        </div>`;
    } else {
      const outStr = typeof tr.output === "object" ? JSON.stringify(tr.output, null, 2) : String(tr.output || tr.error || "");
      bodyHTML = `<div class="tool-body">${escapeHtml(outStr)}</div>`;
    }

    card.innerHTML = `
      <div class="tool-card-header">
        <span class="tool-title">⚡ ${escapeHtml(tr.name)}</span>
        ${tr.node_id ? `<span class="node-tag">Node: ${escapeHtml(tr.node_id)}</span>` : ""}
        ${tr.model ? `<span class="node-tag">${escapeHtml(tr.model)}</span>` : ""}
        <span class="badge ${tr.error ? "failed" : "completed"}">${tr.duration_ms || 0}ms</span>
      </div>
      ${bodyHTML}
    `;
    chatMessagesEl.appendChild(card);
    return;
  }

  const msgDiv = document.createElement("div");
  msgDiv.className = `chat-msg ${isUser ? "user" : "assistant"}`;

  let metaHTML = "";
  if (!isUser) {
    metaHTML = `<div class="msg-meta"><span>🤖 ${escapeHtml(msg.coordinator || "Orchestrator Coordinator")}</span>${msg.node_id ? `<span class="node-tag">Execution node: ${escapeHtml(msg.node_id)}</span>` : ""}${msg.model ? `<span class="node-tag">${escapeHtml(msg.model)}</span>` : ""}${msg.duration_ms ? `<span>${msg.duration_ms}ms</span>` : ""}</div>`;
  }

  let thoughtHTML = "";
  if (msg.thought) {
    thoughtHTML = `<div class="thought-box">💭 <strong>Reasoning:</strong> ${escapeHtml(msg.thought)}</div>`;
  }

  if (msg.raw_output) {
    thoughtHTML += `<div class="node-output-box"><strong>Node output:</strong><pre>${escapeHtml(msg.raw_output)}</pre></div>`;
  }

  let toolsHTML = "";
  if (msg.tool_calls && msg.tool_calls.length > 0) {
    for (const tc of msg.tool_calls) {
      toolsHTML += `
        <div class="tool-card">
          <div class="tool-card-header">
            <span class="tool-title">⚡ Calling tool: ${escapeHtml(tc.name)}</span>
          </div>
          <div class="tool-body">${escapeHtml(JSON.stringify(tc.arguments || {}, null, 2))}</div>
        </div>`;
    }
  }

  let contentHTML = "";
  if (msg.content) {
    contentHTML = `<div class="msg-bubble">${escapeHtml(msg.content)}</div>`;
  }

  msgDiv.innerHTML = `
    ${metaHTML}
    ${thoughtHTML}
    ${toolsHTML}
    ${contentHTML}
  `;

  chatMessagesEl.appendChild(msgDiv);
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}

async function sendInteractiveMessage(text) {
  if (!text || !activeSessionId || isSending) return;
  isSending = true;
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (chatStatus) chatStatus.textContent = "⚡ Reasoning & dispatching across cluster...";

  // Optimistically append user message
  appendMessageToChat({ role: "user", content: text });
  chatInput.value = "";

  try {
    const url = `/api/agent/sessions/${encodeURIComponent(activeSessionId)}/messages?stream=true`;
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "text/event-stream" },
      body: JSON.stringify({ content: text }),
    });

    if (!response.ok) {
      throw new Error(await response.text());
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const jsonStr = line.slice(6).trim();
        if (!jsonStr) continue;
        try {
          const event = JSON.parse(jsonStr);
          handleStreamEvent(event);
        } catch (e) {
          console.error("stream parse error", e, jsonStr);
        }
      }
    }

    if (chatStatus) chatStatus.textContent = "";
    await selectSession(activeSessionId);
  } catch (err) {
    if (chatStatus) chatStatus.textContent = "Error: " + err.message;
    appendMessageToChat({
      role: "assistant",
      content: "⚠️ Error during execution: " + err.message,
    });
  } finally {
    isSending = false;
    if (sendMsgBtn) sendMsgBtn.disabled = false;
  }
}

function handleStreamEvent(event) {
  if (event.type === "step_started") {
    if (chatStatus) chatStatus.textContent = `⚡ Step ${event.step}: Reasoning on coordinator LLM...`;
  } else if (event.type === "thought" && event.content) {
    const thoughtBox = document.createElement("div");
    thoughtBox.className = "thought-box";
    thoughtBox.innerHTML = `💭 <strong>Step ${event.step || 1}:</strong> ${escapeHtml(event.content)}`;
    chatMessagesEl.appendChild(thoughtBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "node_output" && event.output) {
    const outputBox = document.createElement("div");
    outputBox.className = "node-output-box";
    outputBox.innerHTML = `<strong>Orchestrator coordinator output · ${escapeHtml(event.node_id || "node")} · ${escapeHtml(event.model || "")}</strong><pre>${escapeHtml(event.output)}</pre>`;
    chatMessagesEl.appendChild(outputBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "tool_call" && event.tool_call) {
    if (chatStatus) chatStatus.textContent = `⚡ Spreading work: Calling tool ${event.tool_call.name}...`;
  } else if (event.type === "tool_response" && event.tool_resp) {
    appendMessageToChat({ role: "tool", tool_response: event.tool_resp });
  } else if (event.type === "final_message" && event.message) {
    appendMessageToChat(event.message);
  } else if (event.type === "error") {
    if (chatStatus) chatStatus.textContent = `Error: ${event.error}`;
  }
}

// Event Listeners for Chat
sendMsgBtn?.addEventListener("click", () => {
  sendInteractiveMessage(chatInput.value.trim());
});

chatInput?.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendInteractiveMessage(chatInput.value.trim());
  }
});

newSessionBtn?.addEventListener("click", () => {
  createNewSession();
});

promptChips.forEach((chip) => {
  chip.addEventListener("click", () => {
    const p = chip.dataset.prompt;
    if (p) {
      if (chatInput) chatInput.value = p;
      sendInteractiveMessage(p);
    }
  });
});

// --- Persistent memory & learning ---

async function refreshMemory() {
  try {
    const [insights, facts, experiences, dialogues] = await Promise.all([
      fetchJSON("/api/memory/insights"),
      fetchJSON("/api/memory/facts"),
      memorySearch && memorySearch.value.trim()
        ? fetchJSON(`/api/memory/experiences?q=${encodeURIComponent(memorySearch.value.trim())}&limit=30`)
        : fetchJSON("/api/memory/experiences?limit=30"),
      fetchJSON("/api/memory/dialogues?limit=20"),
    ]);
    renderLearning(insights);
    renderFacts(facts);
    renderExperiences(experiences);
    renderTranscripts(dialogues);
  } catch (err) {
    console.error("memory refresh failed", err);
  }
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

function renderFacts(facts) {
  if (!factsList) return;
  factsList.innerHTML = "";
  if (!facts || facts.length === 0) {
    factsList.innerHTML = '<p style="color:var(--muted)">Nothing remembered yet.</p>';
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
}

function renderExperiences(experiences) {
  if (!experiencesList) return;
  experiencesList.innerHTML = "";
  if (!experiences || experiences.length === 0) {
    experiencesList.innerHTML = '<p style="color:var(--muted)">No matching experience recorded.</p>';
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
}

function renderTranscripts(dialogues) {
  if (!transcriptsList) return;
  transcriptsList.innerHTML = "";
  if (!dialogues || dialogues.length === 0) {
    transcriptsList.innerHTML = '<p style="color:var(--muted)">No stored conversations yet.</p>';
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

// --- Existing Cluster & Routing Views ---

function staticNodePosition(nodeId) {
  const hash = [...nodeId].reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  const x = 18 + ((hash % 7) * 11) + 10;
  const y = 20 + ((hash % 5) * 16) + 10;
  return { x: Math.min(Math.max(x, 18), 82), y: Math.min(Math.max(y, 18), 82) };
}

function sortNodesByCapability(nodes) {
  return [...nodes].sort((a, b) => {
    if ((b.active_tasks || 0) !== (a.active_tasks || 0)) return (b.active_tasks || 0) - (a.active_tasks || 0);
    if (Boolean(b.leader) !== Boolean(a.leader)) return Number(b.leader) - Number(a.leader);
    const aScore = (a.hardware?.score || 0) + (a.fast_score || 0) * 0.75 + (a.hardware?.total_ram_mb || 0) / 1024 * 0.5 + (a.hardware?.cpu_cores || 0) * 0.25 + (a.hardware?.has_gpu ? 25 : 0);
    const bScore = (b.hardware?.score || 0) + (b.fast_score || 0) * 0.75 + (b.hardware?.total_ram_mb || 0) / 1024 * 0.5 + (b.hardware?.cpu_cores || 0) * 0.25 + (b.hardware?.has_gpu ? 25 : 0);
    if (b.healthy !== a.healthy) return Number(b.healthy) - Number(a.healthy);
    if (bScore !== aScore) return bScore - aScore;
    return (b.id || "").localeCompare(a.id || "");
  });
}

function renderClusterMap(nodes, tasks, peers) {
  if (!clusterMap || !clusterLinks || !clusterNodes) return;
  clusterLinks.innerHTML = "";
  clusterLinks.innerHTML = '<defs><marker id="cluster-arrow" markerWidth="4" markerHeight="4" refX="3.5" refY="2" orient="auto"><path d="M0,0 L4,2 L0,4 z" fill="#5ec2ff"></path></marker></defs>';
  clusterNodes.innerHTML = "";
  if (clusterPeers) clusterPeers.innerHTML = "";
  const ordered = sortNodesByCapability(nodes || []);
  if (clusterMapEmpty) clusterMapEmpty.hidden = ordered.length > 0;
  if (clusterMapStatus) clusterMapStatus.textContent = `${ordered.length} nodes · ${(tasks || []).length} jobs`;
  if (!ordered.length) return;

  const positions = new Map();
  ordered.forEach((node, index) => {
    const angle = (index / Math.max(ordered.length, 1)) * Math.PI * 2 - Math.PI / 2;
    const radius = ordered.length === 1 ? 0 : Math.min(31, 12 + ordered.length * 2.2);
    positions.set(node.id, { x: 50 + Math.cos(angle) * radius, y: 50 + Math.sin(angle) * radius });
  });

  const links = [];
  for (const task of tasks || []) {
    const subtasks = (task.subtasks || []).filter((item) => positions.has(item.node_id));
    for (let index = 1; index < subtasks.length; index++) {
      const from = subtasks[index - 1];
      const to = subtasks[index];
      if (from.node_id === to.node_id) continue;
      const typeText = `${task.description || ""} ${from.subtask?.task_type || ""} ${to.subtask?.task_type || ""}`.toLowerCase();
      links.push({ from: from.node_id, to: to.node_id, label: task.id, conversation: /conversation|collaborat|dialog|debate|chat/.test(typeText) });
    }
  }
  for (const link of links) {
    const from = positions.get(link.from);
    const to = positions.get(link.to);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", from.x); line.setAttribute("y1", from.y);
    line.setAttribute("x2", to.x); line.setAttribute("y2", to.y);
    line.setAttribute("class", link.conversation ? "cluster-link conversation" : "cluster-link job");
    clusterLinks.appendChild(line);
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", (from.x + to.x) / 2); label.setAttribute("y", (from.y + to.y) / 2 - 1.5);
    label.setAttribute("class", "cluster-link-label"); label.textContent = link.conversation ? "conversation" : link.label;
    clusterLinks.appendChild(label);
  }

  const leaderID = ordered.find((node) => node.leader)?.id;
  ordered.forEach((node, index) => {
    const position = positions.get(node.id);
    const load = Math.min(100, (node.active_tasks || 0) * 25);
    const entity = document.createElement("button");
    entity.type = "button";
    entity.className = `cluster-node-entity ${node.healthy ? "online" : "offline"}${node.id === leaderID ? " leader" : ""}`;
    entity.style.left = `${position.x}%`; entity.style.top = `${position.y}%`;
    entity.innerHTML = `<span class="cluster-node-orbit"></span><strong>${escapeHtml(node.id)}</strong><span class="cluster-node-role">${node.id === leaderID ? "coordinator" : node.healthy ? "worker" : "offline"}</span><span class="cluster-node-load"><i style="width:${load}%"></i></span><small>${node.active_tasks || 0} active · ${(node.models || []).length} models</small>`;
    entity.title = "Right-click to edit node properties";
    entity.addEventListener("click", () => openNodeTerminal(node));
    entity.addEventListener("contextmenu", (event) => {
      event.preventDefault(); contextNode = node; showNodeMenu(event.clientX, event.clientY);
    });
    clusterNodes.appendChild(entity);
  });

  const peerCount = (peers || []).filter((peer) => peer.role && peer.role !== "node").length;
  if (clusterMapStatus && peerCount) clusterMapStatus.textContent += ` · ${peerCount} discovered peers`;
  if (clusterPeers) {
    const discovered = (peers || []).filter((peer) => peer.role && peer.role !== "node");
    clusterPeers.innerHTML = discovered.length
      ? `<span class="cluster-peers-label">Local network</span>${discovered.map((peer) => `<span class="cluster-peer"><i class="legend-dot peer"></i><strong>${escapeHtml(peer.id || "unknown")}</strong><small>${escapeHtml(peer.role)} · ${escapeHtml(peer.http_addr || "address unavailable")}</small></span>`).join("")}`
      : '<span class="cluster-peers-label">Local network</span><span class="panel-caption">No additional peers discovered</span>';
  }
}

function renderNodes(nodes) {
  currentNodes = nodes || [];
  if (!nodesList) return;
  nodesList.innerHTML = "";
  if (!nodes || nodes.length === 0) {
    nodesList.innerHTML = '<p style="color:var(--muted)">No nodes registered yet.</p>';
    return;
  }

  const ordered = sortNodesByCapability(nodes);
  const leaderID = ordered.find((n) => n.leader)?.id;
  const fastestID = ordered.reduce((best, node) => !best || node.fast_score > best.fast_score || (node.fast_score === best.fast_score && node.id < best.id) ? node : best, null)?.id;

  for (const n of ordered) {
    const card = document.createElement("div");
    card.className = "card" + (n.id === leaderID ? " leader" : "");
    const modelTags = (n.models || [])
      .map((m) => `<button class="model-tag-btn" data-node="${escapeHtml(n.id)}" data-model="${escapeHtml(m.name)}" title="Directly test ${escapeHtml(m.name)} on ${escapeHtml(n.id)}">⚡ ${escapeHtml(m.name)}</button>`)
      .join("");
    const propOpen = propertyNodeId === n.id;
    card.innerHTML = `
      <h3>${escapeHtml(n.id)} <span class="badge ${n.healthy ? "healthy" : "unhealthy"}">${n.healthy ? "online" : "offline"}</span></h3>
      ${n.version_error ? `<div class="node-version-error">⚠ ${escapeHtml(n.version_error)}</div>` : ""}
      <div class="role-tags">${n.id === leaderID ? '<span class="role-tag coordinator">cluster leader · orchestrator</span>' : ''}${n.id === fastestID && n.id !== leaderID ? '<span class="role-tag worker">fastest</span>' : ''}</div>
      <div class="row"><span>Address</span><span>${escapeHtml(n.address)}</span></div>
      <div class="row"><span>OS / Arch</span><span>${escapeHtml(n.hardware.os)}/${escapeHtml(n.hardware.arch)}</span></div>
      <div class="row"><span>CPU cores</span><span>${n.hardware.cpu_cores}</span></div>
      <div class="row"><span>RAM</span><span>${(n.hardware.total_ram_mb / 1024).toFixed(1)} GB</span></div>
      <div class="row"><span>GPU</span><span>${n.hardware.has_gpu ? escapeHtml(n.hardware.gpu_vendor || "enabled") : "none"}</span></div>
      <div class="row"><span>Active tasks</span><span>${n.active_tasks}</span></div>
      <div class="row"><span>Score</span><span>${n.hardware.score.toFixed(1)}</span></div>
      <div class="row"><span>Fast score</span><span>${(n.fast_score || 0).toFixed(1)}</span></div>
      <div class="row"><span>Default model</span><span>${escapeHtml(n.default_model || "automatic")}</span></div>
      <div>${modelTags || '<span style="color:var(--muted);font-size:0.75rem">No models installed</span>'}</div>
      <button class="model-config-toggle" data-node="${escapeHtml(n.id)}">⚙ Configure models</button>
      <button class="node-model-refresh" data-node="${escapeHtml(n.id)}">↻ Refresh local registry</button>
      <button class="node-terminal-open" data-node="${escapeHtml(n.id)}">▸ Open node terminal</button>
      <div class="model-config-panel" id="model-config-${cssId(n.id)}" style="display:${modelPanelOpen.has(n.id) ? "block" : "none"}"></div>
      ${propOpen ? `<div class="node-property-panel"><label>Advertised address<input value="${escapeHtml(n.address)}" data-prop="address" /></label><label>Default model<input value="${escapeHtml(n.default_model || "")}" data-prop="default_model" /></label><label>Model server<select data-prop="engine_name"><option value="ollama">Ollama</option><option value="vllm">vLLM</option></select></label><label>Server URL<input value="http://127.0.0.1:11434" data-prop="engine_url" /></label><div class="node-property-actions"><button data-node="${escapeHtml(n.id)}" class="node-property-save">Save node properties</button><input placeholder="model name to pull" data-prop="pull_model" /><button data-node="${escapeHtml(n.id)}" class="node-model-pull">Pull model</button></div><span class="node-property-status"></span></div>` : ""}
    `;
    nodesList.appendChild(card);
    card.querySelector(".node-terminal-open")?.addEventListener("click", () => openNodeTerminal(n));
    card.querySelector(".node-model-refresh")?.addEventListener("click", () => refreshNodeModels(n.id));
    if (propOpen) {
      const props = nodeProperties.get(n.id);
      if (props) {
        const server = card.querySelector('[data-prop="engine_name"]');
        const url = card.querySelector('[data-prop="engine_url"]');
        if (server && props.engine_name) server.value = props.engine_name;
        if (url && props.engine_url) url.value = props.engine_url;
      }
      card.querySelector(".node-property-save")?.addEventListener("click", () => saveNodeProperties(n.id, card));
      card.querySelector(".node-model-pull")?.addEventListener("click", () => pullNodeModel(n.id, card));
      loadNodeProperties(n.id, card);
    }
    card.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      contextNode = n;
      showNodeMenu(event.clientX, event.clientY);
    });

    // Wire model direct test buttons
    card.querySelectorAll(".model-tag-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const nodeId = btn.dataset.node;
        const model = btn.dataset.model;
        btn.textContent = "⏳ Testing…";
        try {
          const res = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/${encodeURIComponent(model)}/test`, { method: "POST" });
          if (res.healthy) {
            btn.textContent = `✓ ${model} (${res.duration_ms}ms)`;
          } else {
            btn.textContent = `✗ ${model} (err)`;
          }
        } catch (e) {
          btn.textContent = `✗ ${model}`;
        }
        setTimeout(() => { btn.textContent = `⚡ ${model}`; }, 3500);
      });
    });
    if (modelPanelOpen.has(n.id)) {
      if (lastFetchedModels.has(n.id)) {
        renderModelPanel(n.id, lastFetchedModels.get(n.id));
      } else {
        loadModelPanel(n.id);
      }
    }
  }

  nodesList.querySelectorAll(".model-config-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nodeId = btn.dataset.node;
      if (modelPanelOpen.has(nodeId)) {
        modelPanelOpen.delete(nodeId);
        lastFetchedModels.delete(nodeId);
      } else {
        modelPanelOpen.add(nodeId);
      }
      renderNodes(nodes);
    });
  });
}

async function openNodeTerminal(node) {
  terminalNode = node;
  terminalDialogueId = null;
  const modal = document.getElementById("node-terminal");
  const title = document.getElementById("node-terminal-title");
  const models = document.getElementById("node-terminal-model");
  const messages = document.getElementById("node-terminal-messages");
  const status = document.getElementById("node-terminal-status");
  title.textContent = `${node.id} terminal`;
  messages.innerHTML = "";
  status.textContent = "Loading local models...";
  modal.hidden = false;
  try {
    const available = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models`);
    models.innerHTML = available.map((m) => `<option value="${escapeHtml(m.name)}">${escapeHtml(m.name)} · ${escapeHtml(m.engine)}</option>`).join("");
    status.textContent = available.length ? "Ready" : "No locally served models";
  } catch (err) {
    status.textContent = "Model discovery failed: " + err.message;
  }
  document.getElementById("node-terminal-input")?.focus();
}

function closeNodeTerminal() {
  document.getElementById("node-terminal").hidden = true;
  terminalNode = null;
  terminalDialogueId = null;
}

function appendTerminalMessage(role, content) {
  const messages = document.getElementById("node-terminal-messages");
  const item = document.createElement("div");
  item.className = `node-terminal-message ${role}`;
  item.innerHTML = `<strong>${role === "user" ? "You" : escapeHtml(terminalNode?.id || "Node")}</strong><div>${escapeHtml(content)}</div>`;
  messages.appendChild(item);
  messages.scrollTop = messages.scrollHeight;
}

document.getElementById("node-terminal-close")?.addEventListener("click", closeNodeTerminal);
document.getElementById("node-terminal-form")?.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!terminalNode) return;
  const input = document.getElementById("node-terminal-input");
  const content = input.value.trim();
  const model = document.getElementById("node-terminal-model").value;
  const status = document.getElementById("node-terminal-status");
  if (!content || !model) return;
  input.value = "";
  appendTerminalMessage("user", content);
  status.textContent = "Generating...";
  try {
    if (!terminalDialogueId) {
      const dialogue = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }) });
      terminalDialogueId = dialogue.id;
    }
    const response = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/messages`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ content }) });
    appendTerminalMessage("assistant", response.reply || response.error || "No response");
    status.textContent = `${response.model || model} · ${response.duration_ms || 0}ms`;
  } catch (err) {
    appendTerminalMessage("assistant", "Error: " + err.message);
    status.textContent = "Request failed";
  }
});

async function loadNodeProperties(nodeId, card) {
  try {
    const props = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/properties`);
    nodeProperties.set(nodeId, props);
    const server = card.querySelector('[data-prop="engine_name"]');
    const url = card.querySelector('[data-prop="engine_url"]');
    if (server && props.engine_name) server.value = props.engine_name;
    if (url && props.engine_url) url.value = props.engine_url;
  } catch (err) { card.querySelector(".node-property-status").textContent = err.message; }
}

async function saveNodeProperties(nodeId, card) {
  const value = (name) => card.querySelector(`[data-prop="${name}"]`)?.value.trim() || "";
  const status = card.querySelector(".node-property-status");
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/properties`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ advertise_addr: value("address"), default_model: value("default_model"), engine_name: value("engine_name"), engine_url: value("engine_url") }) });
    status.textContent = "Saved";
    refresh();
  } catch (err) { status.textContent = "Error: " + err.message; }
}

async function pullNodeModel(nodeId, card) {
  const model = card.querySelector('[data-prop="pull_model"]').value.trim();
  const status = card.querySelector(".node-property-status");
  if (!model) { status.textContent = "Enter a model name"; return; }
  status.textContent = "Pulling...";
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/pull`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }) });
    status.textContent = `Pulled ${model}`;
    lastFetchedModels.delete(nodeId);
    refresh();
  } catch (err) { status.textContent = "Pull failed: " + err.message; }
}

async function refreshNodeModels(nodeId) {
  const button = [...(nodesList?.querySelectorAll(".node-model-refresh") || [])]
    .find((candidate) => candidate.dataset.node === nodeId);
  if (button) button.textContent = "Refreshing...";
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" });
    lastFetchedModels.delete(nodeId);
    await refresh();
  } catch (err) {
    console.error("model refresh failed", err);
    if (button) button.textContent = "Refresh failed";
  }
}

refreshAllModelsBtn?.addEventListener("click", async () => {
  refreshAllModelsBtn.disabled = true;
  refreshAllModelsBtn.textContent = "Refreshing...";
  try {
    await fetchJSON("/api/nodes/models/refresh", { method: "POST" });
    await refresh();
    refreshAllModelsBtn.textContent = "Registries refreshed";
  } catch (err) {
    refreshAllModelsBtn.textContent = "Refresh failed";
    console.error("all-node model refresh failed", err);
  } finally {
    setTimeout(() => {
      refreshAllModelsBtn.disabled = false;
      refreshAllModelsBtn.textContent = "↻ Refresh all model registries";
    }, 1800);
  }
});

function showNodeMenu(x, y) {
  const menu = document.getElementById("node-context-menu");
  if (!menu || !contextNode) return;
  menu.querySelector("[data-action=properties]").textContent = `Edit ${contextNode.id} properties`;
  menu.style.left = `${Math.min(x, window.innerWidth - menu.offsetWidth - 8)}px`;
  menu.style.top = `${Math.min(y, window.innerHeight - menu.offsetHeight - 8)}px`;
  menu.hidden = false;
}

function hideNodeMenu() {
  const menu = document.getElementById("node-context-menu");
  if (menu) menu.hidden = true;
}

document.addEventListener("click", hideNodeMenu);
document.addEventListener("scroll", hideNodeMenu, true);
document.getElementById("node-context-menu")?.addEventListener("click", async (event) => {
  const action = event.target.closest("[data-action]")?.dataset.action;
  if (!contextNode || !action) return;
  const node = contextNode;
  hideNodeMenu();
  if (action === "properties") {
    propertyNodeId = node.id;
    renderNodes(currentNodes);
  } else if (action === "open") {
    window.open(node.address, "_blank", "noopener");
  } else if (action === "copy") {
    await navigator.clipboard?.writeText(node.address);
  }
});

function cssId(nodeId) {
  return nodeId.replace(/[^a-zA-Z0-9_-]/g, "_");
}

async function loadModelPanel(nodeId) {
  const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
  if (!panel) return;
  panel.innerHTML = '<p style="color:var(--muted)">Loading models…</p>';
  try {
    const models = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`);
    const state = new Map(models.map((m) => [m.name, m.enabled]));
    pendingModelState.set(nodeId, state);
    pendingDefaultModel.set(nodeId, (models.find((m) => m.default) || {}).name || "");
    lastFetchedModels.set(nodeId, models);
    renderModelPanel(nodeId, models);
  } catch (err) {
    panel.innerHTML = `<p style="color:var(--bad)">Failed to load models: ${escapeHtml(err.message)}</p>`;
  }
}

function renderModelPanel(nodeId, models) {
  const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
  if (!panel) return;
  if (!models || models.length === 0) {
    panel.innerHTML = '<p style="color:var(--muted)">No models discovered on this node.</p>';
    return;
  }
  const state = pendingModelState.get(nodeId);
  const defaultModel = pendingDefaultModel.get(nodeId) || "";
  const rows = models
    .map((m) => {
      const checked = state.get(m.name) ? "checked" : "";
      return `
        <label class="model-config-row">
          <input type="checkbox" data-model="${escapeHtml(m.name)}" ${checked} />
          <span class="model-config-name">${escapeHtml(m.name)}</span>
          <span class="model-config-meta">${(m.tags || []).join(", ")} · ${m.engine}${m.size_gb ? ` · ${m.size_gb.toFixed(1)} GB` : ""}</span>
        </label>`;
    })
    .join("");
  panel.innerHTML = `
    <label class="model-default-row">
      <span>Default model</span>
      <select class="model-default-select">
        <option value="">Automatic best match</option>
        ${models.map((m) => `<option value="${escapeHtml(m.name)}" ${m.name === defaultModel ? "selected" : ""} ${state.get(m.name) ? "" : "disabled"}>${escapeHtml(m.name)}</option>`).join("")}
      </select>
    </label>
    ${rows}
    <button class="model-config-save" data-node="${escapeHtml(nodeId)}">Save</button>
    <span class="model-config-status" id="model-config-status-${cssId(nodeId)}"></span>
  `;
  panel.querySelectorAll("input[type=checkbox]").forEach((cb) => {
    cb.addEventListener("change", () => {
      state.set(cb.dataset.model, cb.checked);
    });
  });
  panel.querySelector(".model-default-select").addEventListener("change", (event) => {
    pendingDefaultModel.set(nodeId, event.target.value);
  });
  panel.querySelector(".model-config-save").addEventListener("click", () => saveModelPanel(nodeId));
}

async function saveModelPanel(nodeId) {
  const statusEl = document.getElementById(`model-config-status-${cssId(nodeId)}`);
  const state = pendingModelState.get(nodeId);
  if (!state) return;
  const disabled = [...state.entries()].filter(([, enabled]) => !enabled).map(([name]) => name);
  const defaultModel = pendingDefaultModel.get(nodeId) || "";
  if (statusEl) statusEl.textContent = "Saving…";
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ disabled_models: disabled, default_model: defaultModel }),
    });
    if (statusEl) statusEl.textContent = "Saved";
    lastFetchedModels.delete(nodeId);
    refresh();
  } catch (err) {
    if (statusEl) statusEl.textContent = "Error: " + err.message;
  }
}

function renderRouting(nodes, tasks) {
  if (!routingList) return;
  routingList.innerHTML = "";
  if (!nodes || nodes.length === 0) {
    routingList.innerHTML = '<p style="color:var(--muted)">No nodes registered yet.</p>';
    return;
  }

  const byNode = new Map(nodes.map((n) => [n.id, { node: n, completed: 0, failed: 0, running: 0, recent: [] }]));
  const sortedTasks = [...(tasks || [])].sort((a, b) => (a.id < b.id ? 1 : -1));
  for (const t of sortedTasks) {
    for (const s of t.subtasks || []) {
      const bucket = byNode.get(s.node_id);
      if (!bucket) continue;
      if (s.status === "completed") bucket.completed++;
      else if (s.status === "failed") bucket.failed++;
      else bucket.running++;
      if (bucket.recent.length < 5) {
        bucket.recent.push({ taskId: t.id, subtaskId: s.subtask.id, model: s.model, status: s.status, taskType: s.subtask.task_type });
      }
    }
  }

  const workload = [...byNode.values()].sort((a, b) => {
    if ((b.node.active_tasks || 0) !== (a.node.active_tasks || 0)) return (b.node.active_tasks || 0) - (a.node.active_tasks || 0);
    return (b.running + b.completed + b.failed) - (a.running + a.completed + a.failed);
  });
  for (const { node, completed, failed, running, recent } of workload) {
    const card = document.createElement("div");
    card.className = "card";
    const total = completed + failed + running;
    const pct = (n) => (total === 0 ? 0 : Math.round((n / total) * 100));
    const recentHTML = recent.length
      ? recent
          .map(
            (r) => `<div class="row"><span>${r.taskId}/${r.subtaskId} (${r.taskType}) &rarr; ${escapeHtml(r.model)}</span><span class="badge ${r.status}">${r.status}</span></div>`
          )
          .join("")
      : '<p style="color:var(--muted);font-size:0.8rem">No jobs routed yet.</p>';
    card.innerHTML = `
      <h3>${escapeHtml(node.id)} <span class="badge ${node.healthy ? "healthy" : "unhealthy"}">${node.active_tasks} active</span></h3>
      <div class="workload-bar">
        <div class="workload-seg workload-running" style="width:${pct(running)}%"></div>
        <div class="workload-seg workload-completed" style="width:${pct(completed)}%"></div>
        <div class="workload-seg workload-failed" style="width:${pct(failed)}%"></div>
      </div>
      <div class="row"><span>In-flight</span><span>${running}</span></div>
      <div class="row"><span>Completed</span><span>${completed}</span></div>
      <div class="row"><span>Failed</span><span>${failed}</span></div>
      <h4>Recent jobs</h4>
      ${recentHTML}
    `;
    routingList.appendChild(card);
  }
}

function renderDiscovery(peers) {
  if (!discoveryList) return;
  discoveryList.innerHTML = "";
  if (!peers || peers.length === 0) {
    discoveryList.innerHTML = '<p style="color:var(--muted)">No other instances found on the local network yet.</p>';
    return;
  }

  const orchestrator = peers.filter((p) => p.role === "orchestrator").sort((a, b) => (a.id || "").localeCompare(b.id || ""));
  const ordered = [...peers].sort((a, b) => (a.id || "").localeCompare(b.id || ""));
  for (const p of ordered) {
    const card = document.createElement("div");
    card.className = "card";
    const associated = p.role === "node" ? (orchestrator[0]?.id || "none") : "self";
    card.innerHTML = `
      <h3>${escapeHtml(p.id)} <span class="badge healthy">${escapeHtml(p.role)}</span></h3>
      <div class="row"><span>Address</span><span>${escapeHtml(p.http_addr)}</span></div>
      <div class="row"><span>Associated orchestrator</span><span>${escapeHtml(associated)}</span></div>
      <div class="row"><span>Last seen</span><span>${new Date(p.last_seen).toLocaleTimeString()}</span></div>
    `;
    discoveryList.appendChild(card);
  }
}

function renderTasks(tasks) {
  if (!tasksList) return;
  tasksList.innerHTML = "";
  if (!tasks || tasks.length === 0) {
    tasksList.innerHTML = '<p style="color:var(--muted)">No tasks submitted yet.</p>';
    return;
  }
  tasks.sort((a, b) => (a.id < b.id ? 1 : -1));
  for (const t of tasks) {
    const card = document.createElement("div");
    card.className = "card";
    const subtaskHTML = t.subtasks
      .map(
        (s) => `
        <div class="row"><span>${s.subtask.id} (${s.subtask.task_type}) &rarr; ${escapeHtml(s.node_id)}</span>
        <span class="badge ${s.status}">${s.status}</span></div>
        ${s.output ? `<div class="subtask-output-wrap"><button class="copy-output-btn" type="button" data-copy-output="${encodeURIComponent(s.output)}">Copy</button><div class="subtask-output">${escapeHtml(s.output)}</div></div>` : ""}
        ${s.error ? `<div class="subtask-output" style="border-color:var(--bad)">${escapeHtml(s.error)}</div>` : ""}
      `
      )
      .join("");
    card.innerHTML = `
      <h3>${escapeHtml(t.id)} <span class="badge ${t.status}">${t.status}</span></h3>
      <div class="row"><span colspan="2">${escapeHtml(t.description)}</span></div>
      ${subtaskHTML}
    `;
    tasksList.appendChild(card);
    card.querySelectorAll("[data-copy-output]").forEach((button) => {
      button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copyOutput), button));
    });
  }
}

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

copyTasksBtn?.addEventListener("click", () => copyText(taskText(window.latestTasks || []), copyTasksBtn));

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

async function refresh() {
  try {
    const [nodes, tasks, peers, build] = await Promise.all([
      fetchJSON("/api/nodes"),
      fetchJSON("/api/tasks"),
      fetchJSON("/api/discovery"),
      fetchJSON("/api/version"),
    ]);
    applySnapshot({ nodes, tasks, discovery: peers, build_version: build.version });
  } catch (err) {
    console.error(err);
  }
}

function applySnapshot(snap) {
  window.latestTasks = snap.tasks || [];
  const nodeCount = snap.member_count ?? (snap.nodes || []).length;
  updateClusterTitle(nodeCount);

  // Update header stats pills
  if (statNodes) statNodes.textContent = `Nodes: ${nodeCount}`;
  let modelCount = 0;
  let activeLoad = 0;
  for (const n of snap.nodes || []) {
    modelCount += (n.models || []).length;
    activeLoad += (n.active_tasks || 0);
  }
  if (statModels) statModels.textContent = `Models: ${modelCount}`;
  if (statLoad) statLoad.textContent = `Active Load: ${activeLoad}`;
  if (statMemory && snap.memory) {
    statMemory.textContent = `Memory: ${snap.memory.total_experiences || 0} · ${Math.round((snap.memory.success_rate || 0) * 100)}%`;
  }
  if (statVersion && snap.build_version) statVersion.textContent = snap.build_version;

  renderClusterMap(snap.nodes || [], snap.tasks || [], snap.discovery || []);
  renderNodes(snap.nodes || []);
  renderRouting(snap.nodes || [], snap.tasks || []);
  renderDiscovery(snap.discovery || []);
  renderTasks(snap.tasks || []);
}

function connectEvents() {
  const es = new EventSource("/api/events");
  es.onmessage = (evt) => {
    try {
      applySnapshot(JSON.parse(evt.data));
    } catch (err) {
      console.error(err);
    }
  };
}

submitBtn?.addEventListener("click", async () => {
  const description = taskInput.value.trim();
  if (!description) return;
  submitStatus.textContent = "Submitting...";
  try {
    const task = await fetchJSON("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ description }),
    });
    submitStatus.textContent = `Submitted as ${task.id}`;
    taskInput.value = "";
    refresh();
  } catch (err) {
    submitStatus.textContent = "Error: " + err.message;
  }
});

// Initialization
loadAgentTools();
loadClusterModels();
loadSessions();
refresh();
connectEvents();

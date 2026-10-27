import { refresh } from "../app.js";
import { engineStatusDetailsMarkup, engineStatusSummaryMarkup } from "../components/node-card.js";
import { openPairingDialog } from "../components/pairing-dialog.js";
import { fetchJSON } from "../shared/api.js";
import { dashboardDOM } from "../shared/dom.js";
import { escapeHtml, formatRelativeTime } from "../shared/format.js";
import {
  contextNode,
  dashboardState,
  lastFetchedModels,
  modelPanelOpen,
  nodeProperties,
  nodeTerminalInterruptBtn,
  nodesList,
  pendingDefaultModel,
  pendingModelState,
  refreshAllModelsBtn,
  setContextNode,
  setTerminalAbortController,
  setTerminalDialogueId,
  setTerminalNode,
  setupNodeList,
  setupNodeSummary,
  terminalAbortController,
  terminalDialogueId,
  terminalNode,
} from "../shared/state.js";
import { nodeAcceleratorMarkup, resourceHistoryMarkup, sortNodesByCapability } from "./cluster.js";
import { openShardGroupWizard } from "./shard-setup.js";

export function renderNodes(nodes) {
  dashboardState.nodes = nodes || [];
  if (!nodesList) return;
  if (!nodes || nodes.length === 0) {
    dashboardDOM.patchKeyedChildren(nodesList, [{ id: "__empty" }], {
      keyOf: (item) => item.id,
      signatureOf: () => "empty",
      renderItem: () => {
        const empty = document.createElement("p");
        empty.className = "task-history-empty";
        empty.textContent = "No nodes registered yet.";
        return empty;
      },
    });
    return;
  }

  const ordered = sortNodesByCapability(nodes);
  const leaderID = ordered.find((n) => n.leader)?.id;
  const fastestID = ordered.reduce((best, node) => !best || node.fast_score > best.fast_score || (node.fast_score === best.fast_score && node.id < best.id) ? node : best, null)?.id;

  dashboardDOM.patchKeyedChildren(nodesList, ordered, {
    keyOf: (node) => node.id,
    signatureOf: (node) => JSON.stringify({
      node,
      modelPanelOpen: modelPanelOpen.has(node.id),
      modelPanel: lastFetchedModels.get(node.id),
    }),
    renderItem: (n) => {
      const card = document.createElement("div");
      card.className = "card" + (n.id === leaderID ? " leader" : "");
      const modelTags = (n.models || [])
        .map((m) => `<button class="model-tag-btn" data-node="${escapeHtml(n.id)}" data-model="${escapeHtml(m.name)}" title="Directly test ${escapeHtml(m.name)} on ${escapeHtml(n.id)}" ${n.healthy ? "" : "disabled"}>⚡ ${escapeHtml(m.name)}</button>`)
        .join("");
      const nodeActions = n.healthy
        ? `<button class="model-config-toggle" data-node="${escapeHtml(n.id)}">⚙ Configure models</button>
           <button class="node-model-refresh" data-node="${escapeHtml(n.id)}">↻ Rescan engines &amp; models</button>
           <button class="node-terminal-open" data-node="${escapeHtml(n.id)}">▸ Open node terminal</button>`
        : `<small class="node-offline-note">Offline · last seen ${formatRelativeTime(n.last_seen)}</small>`;
      card.innerHTML = `
        <div class="node-card-heading"><h3>${escapeHtml(n.id)} <span class="badge ${n.healthy ? "healthy" : "unhealthy"}">${n.healthy ? "online" : "offline"}</span></h3></div>
        ${n.version_error ? `<div class="node-version-error">⚠ ${escapeHtml(n.version_error)}</div>` : ""}
        <div class="role-tags">${n.id === leaderID ? '<span class="role-tag coordinator">cluster leader · orchestrator</span>' : ''}${n.id === fastestID && n.id !== leaderID ? '<span class="role-tag worker">fastest</span>' : ''}</div>
        ${engineStatusSummaryMarkup(n.engines)}
        <div class="row"><span>Address</span><span>${escapeHtml(n.address)}</span></div>
        <div class="row"><span>OS / Arch</span><span>${escapeHtml(n.hardware.os)}/${escapeHtml(n.hardware.arch)}</span></div>
        <div class="row"><span>CPU cores</span><span>${n.hardware.cpu_cores}</span></div>
        <div class="row"><span>RAM</span><span>${(n.hardware.total_ram_mb / 1024).toFixed(1)} GB</span></div>
        ${nodeAcceleratorMarkup(n)}
        ${resourceHistoryMarkup(n)}
        ${!n.healthy ? `<div class="row"><span>Last seen</span><span>${escapeHtml(n.last_seen ? new Date(n.last_seen).toLocaleString() : "unknown")}</span></div>` : ""}
        <div class="row"><span>Active tasks</span><span>${n.active_tasks}</span></div>
        <div class="row"><span>Score</span><span>${n.hardware.score.toFixed(1)}</span></div>
        <div class="row"><span>Fast score</span><span>${(n.fast_score || 0).toFixed(1)}</span></div>
        <div class="row"><span>Default model</span><span>${escapeHtml(n.default_model || "automatic")}</span></div>
        <div>${modelTags || '<span style="color:var(--muted);font-size:0.75rem">No models installed</span>'}</div>
        ${nodeActions}
        <div class="model-config-panel" id="model-config-${cssId(n.id)}" style="display:${modelPanelOpen.has(n.id) ? "block" : "none"}"></div>
      `;
      card.querySelector(".node-terminal-open")?.addEventListener("click", () => openNodeTerminal(n));
      card.querySelector(".node-model-refresh")?.addEventListener("click", () => refreshNodeModels(n.id));
      card.querySelector(".model-config-toggle")?.addEventListener("click", () => {
        if (modelPanelOpen.has(n.id)) {
          modelPanelOpen.delete(n.id);
          lastFetchedModels.delete(n.id);
        } else {
          modelPanelOpen.add(n.id);
        }
        renderNodes(nodes);
      });
      card.querySelectorAll(".model-tag-btn").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const nodeId = btn.dataset.node;
          const model = btn.dataset.model;
          btn.textContent = "⏳ Testing…";
          try {
            const res = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/${encodeURIComponent(model)}/test`, { method: "POST" });
            btn.textContent = res.healthy ? `✓ ${model} (${res.duration_ms}ms)` : `✗ ${model} (err)`;
          } catch {
            btn.textContent = `✗ ${model}`;
          }
          setTimeout(() => { btn.textContent = `⚡ ${model}`; }, 3500);
        });
      });
      attachNodeContextMenu(card, n);
      if (modelPanelOpen.has(n.id)) {
        if (lastFetchedModels.has(n.id)) renderModelPanel(n.id, lastFetchedModels.get(n.id), n.engines);
        else loadModelPanel(n.id);
      }
      return card;
    },
  });
}

export function renderSetupNodes(nodes) {
  if (!setupNodeList) return;
  const ordered = sortNodesByCapability(nodes || []);
  if (setupNodeSummary) {
    const online = ordered.filter((node) => node.healthy).length;
    setupNodeSummary.textContent = `${online} online · ${ordered.length - online} offline · ${ordered.length} total`;
  }
  if (!ordered.length) {
    dashboardDOM.patchKeyedChildren(setupNodeList, [{ id: "__empty" }], {
      keyOf: (item) => item.id,
      signatureOf: () => "empty",
      renderItem: () => {
        const empty = document.createElement("p");
        empty.className = "setup-empty";
        empty.textContent = "No cluster members are registered yet. Pair a node to add it to the cluster.";
        return empty;
      },
    });
    return;
  }
  dashboardDOM.patchKeyedChildren(setupNodeList, ordered, {
    keyOf: (node) => node.id,
    signatureOf: JSON.stringify,
    renderItem: (node) => {
    const online = Boolean(node.healthy);
    const card = document.createElement("article");
    card.className = `setup-node-card ${online ? "online" : "offline"}`;
    card.innerHTML = `
      <div class="setup-node-heading">
        <div><h4>${escapeHtml(node.id)}</h4><span class="panel-caption">${escapeHtml(node.address || "Address unavailable")}</span></div>
        <span class="setup-node-status ${online ? "online" : "offline"}"><span class="legend-dot ${online ? "online" : "offline"}"></span>${online ? "Online" : "Offline"}</span>
      </div>
      <div class="setup-node-details">
        <span>${(node.models || []).length} models</span>
        <span>${node.active_tasks || 0} active tasks</span>
        ${online ? "" : `<span>Last seen ${escapeHtml(node.last_seen ? formatRelativeTime(node.last_seen) : "unknown")}</span>`}
      </div>`;
    card.insertAdjacentHTML("beforeend", engineStatusSummaryMarkup(node.engines));
    attachNodeContextMenu(card, node);
    return card;
    },
  });
}

async function openNodeTerminal(node) {
  setTerminalNode(node);
  setTerminalDialogueId(null);
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
  setTerminalNode(null);
  setTerminalDialogueId(null);
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
  setTerminalAbortController(new AbortController());
  if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = false;
  try {
    if (!terminalDialogueId) {
      const dialogue = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }), signal: terminalAbortController.signal });
      setTerminalDialogueId(dialogue.id);
    }
    const response = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/messages`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ content }), signal: terminalAbortController.signal });
    appendTerminalMessage("assistant", response.reply || response.error || "No response");
    status.textContent = `${response.model || model} · ${response.duration_ms || 0}ms`;
  } catch (err) {
    if (err.name === "AbortError") {
      appendTerminalMessage("assistant", "⚠️ Dialogue turn interrupted.");
      status.textContent = "Interrupted";
    } else {
      appendTerminalMessage("assistant", "Error: " + err.message);
      status.textContent = "Request failed";
    }
  } finally {
    setTerminalAbortController(null);
    if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = true;
  }
});

nodeTerminalInterruptBtn?.addEventListener("click", async () => {
  if (terminalNode && terminalDialogueId) {
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/interrupt`, {
        method: "POST",
      });
    } catch (err) {
      console.error("interrupt node dialogue failed", err);
    }
  }
  if (terminalAbortController) {
    terminalAbortController.abort();
  }
});

async function openNodeProperties(node) {
  const modal = document.getElementById("node-properties-modal");
  const form = document.getElementById("node-properties-form");
  const status = document.getElementById("node-properties-status");
  if (!modal || !form) return;
  setContextNode(node);
  status.textContent = "Loading properties...";
  modal.hidden = false;
  try {
    const props = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/properties`);
    nodeProperties.set(node.id, props);
    for (const [name, value] of Object.entries(props)) {
      const field = form.elements.namedItem(name);
      if (!field) continue;
      if (field.type === "checkbox") field.checked = Boolean(value);
      else if (name === "disabled_models") field.value = (value || []).join("\n");
      else field.value = value ?? "";
    }
    document.getElementById("node-properties-title").textContent = `${node.id} properties`;
    document.getElementById("node-properties-subtitle").textContent = "Edit runtime settings. Identity and listen address apply after restart.";
    status.textContent = "";
    form.elements.namedItem("advertise_addr")?.focus();
  } catch (err) {
    status.textContent = "Unable to load properties: " + err.message;
  }
}

function closeNodeProperties() {
  const modal = document.getElementById("node-properties-modal");
  if (modal) modal.hidden = true;
}

async function saveNodeProperties(nodeId, form) {
  const status = document.getElementById("node-properties-status");
  const value = (name) => form.elements.namedItem(name)?.value.trim() || "";
  status.textContent = "Saving...";
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/properties`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        advertise_addr: value("advertise_addr"),
        orchestrator_addr: value("orchestrator_addr"),
        default_model: value("default_model"),
        engine_name: value("engine_name"),
        engine_url: value("engine_url"),
        disabled_models: value("disabled_models").split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean),
        discovery_enabled: form.elements.namedItem("discovery_enabled").checked,
        auto_open_browser: form.elements.namedItem("auto_open_browser").checked,
        tray_mode: value("tray_mode"),
        memory_path: value("memory_path"),
      }),
    });
    status.textContent = "Saved";
    await refresh();
    setTimeout(closeNodeProperties, 500);
  } catch (err) { status.textContent = "Error: " + err.message; }
}

async function refreshNodeModels(nodeId) {
  const button = [...(nodesList?.querySelectorAll(".node-model-refresh") || [])]
    .find((candidate) => candidate.dataset.node === nodeId);
  if (button) button.textContent = "Rescanning...";
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
  refreshAllModelsBtn.textContent = "Rescanning...";
  try {
    await fetchJSON("/api/nodes/models/refresh", { method: "POST" });
    await refresh();
    refreshAllModelsBtn.textContent = "Rescan complete";
  } catch (err) {
    refreshAllModelsBtn.textContent = "Refresh failed";
    console.error("all-node model refresh failed", err);
  } finally {
    setTimeout(() => {
      refreshAllModelsBtn.disabled = false;
      refreshAllModelsBtn.textContent = "↻ Rescan all nodes";
    }, 1800);
  }
});

function showNodeMenu(x, y) {
  const menu = document.getElementById("node-context-menu");
  if (!menu || !contextNode) return;
  const properties = menu.querySelector("[data-action=properties]");
  const pairing = menu.querySelector("[data-action=pair]");
  const nodeIsDiscovered = Boolean(contextNode.discovered);
  const joiningOrchestrator = Boolean(contextNode.joinOrchestrator);
  if (properties) {
    properties.textContent = joiningOrchestrator ? "Configure host properties" : `Configure ${contextNode.id}`;
    properties.disabled = !contextNode.healthy || nodeIsDiscovered || joiningOrchestrator;
  }
  if (pairing) {
    const currentOrchestrator = dashboardState.pairing.status?.orchestrator_id === contextNode.id;
    pairing.textContent = joiningOrchestrator
      ? dashboardState.pairing.status?.joined && currentOrchestrator
        ? `Re-pair with ${contextNode.id}`
        : `Join ${contextNode.id}'s cluster`
      : nodeIsDiscovered ? "Pair node to this cluster" : "Re-pair node with this cluster";
    pairing.disabled = !contextNode.healthy;
  }
  menu.querySelector("[data-action=open]").disabled = !contextNode.healthy || nodeIsDiscovered || joiningOrchestrator;
  menu.hidden = false;
  const bounds = menu.getBoundingClientRect();
  menu.style.left = `${Math.max(8, Math.min(x, window.innerWidth - bounds.width - 8))}px`;
  menu.style.top = `${Math.max(8, Math.min(y, window.innerHeight - bounds.height - 8))}px`;
}

export function attachNodeContextMenu(element, node) {
  element.addEventListener("click", (event) => {
    if (!event.ctrlKey || !node.healthy || event.target.closest("button, input, select, textarea, a")) return;
    event.preventDefault();
    openShardGroupWizard(node.id);
  });
  element.addEventListener("contextmenu", (event) => {
    if (event.ctrlKey && node.healthy) {
      event.preventDefault();
      if (!document.getElementById("shard-group-setup-dialog")?.open) openShardGroupWizard(node.id);
      return;
    }
    event.preventDefault();
    setContextNode(node);
    showNodeMenu(event.clientX, event.clientY);
  });
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
    openNodeProperties(node);
  } else if (action === "pair") {
    openPairingDialog(node);
  } else if (action === "open") {
    window.open(node.address, "_blank", "noopener");
  } else if (action === "copy") {
    await navigator.clipboard?.writeText(node.address);
  }
});

document.getElementById("node-properties-close")?.addEventListener("click", closeNodeProperties);
document.getElementById("node-properties-cancel")?.addEventListener("click", closeNodeProperties);
document.getElementById("node-properties-form")?.addEventListener("submit", (event) => {
  event.preventDefault();
  if (contextNode) saveNodeProperties(contextNode.id, event.currentTarget);
});
document.getElementById("node-properties-modal")?.addEventListener("click", (event) => {
  if (event.target.id === "node-properties-modal") closeNodeProperties();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeNodeProperties();
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
    const node = dashboardState.nodes.find((item) => item.id === nodeId);
    renderModelPanel(nodeId, models, node?.engines || []);
  } catch (err) {
    panel.innerHTML = `<p style="color:var(--bad)">Failed to load models: ${escapeHtml(err.message)}</p>`;
  }
}

function renderModelPanel(nodeId, models, engines = []) {
  const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
  if (!panel) return;
  const engineDetails = engineStatusDetailsMarkup(engines);
  if (!models || models.length === 0) {
    panel.innerHTML = `${engineDetails}<p style="color:var(--muted)">No models discovered on this node.</p>`;
    return;
  }
  const state = pendingModelState.get(nodeId);
  const defaultModel = pendingDefaultModel.get(nodeId) || "";
  const modelsByEngine = new Map();
  for (const model of models) {
    const engineName = model.engine || "Unknown engine";
    if (!modelsByEngine.has(engineName)) modelsByEngine.set(engineName, []);
    modelsByEngine.get(engineName).push(model);
  }
  const rows = [...modelsByEngine.entries()].map(([engineName, engineModels]) => `
      <section class="model-config-engine-group">
        <h5>${escapeHtml(engineName)} · ${engineModels.length} model${engineModels.length === 1 ? "" : "s"}</h5>
        ${engineModels.map((m) => {
      const checked = state.get(m.name) ? "checked" : "";
      return `
        <label class="model-config-row">
          <input type="checkbox" data-model="${escapeHtml(m.name)}" ${checked} />
          <span class="model-config-name">${escapeHtml(m.name)}</span>
          <span class="model-config-meta">${escapeHtml((m.tags || []).join(", "))} · ${escapeHtml(m.engine || "Unknown engine")}${m.size_gb ? ` · ${Number(m.size_gb).toFixed(1)} GB` : ""}</span>
        </label>`;
    }).join("")}
      </section>
    `).join("");
  panel.innerHTML = `
    ${engineDetails}
    <h4>Installed models</h4>
    <label class="model-default-row">
      <span>Default model</span>
      <select class="model-default-select">
        <option value="">Automatic best match</option>
        ${models.map((m) => `<option value="${escapeHtml(m.name)}" ${m.name === defaultModel ? "selected" : ""} ${state.get(m.name) ? "" : "disabled"}>${escapeHtml(m.name)}</option>`).join("")}
      </select>
    </label>
    <div class="model-config-engine-groups">${rows}</div>
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

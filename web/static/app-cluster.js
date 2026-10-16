async function checkSetupReadiness() {
  if (setupCheckBtn) {
    setupCheckBtn.disabled = true;
    setupCheckBtn.textContent = "Checking…";
  }
  setSetupStatus("Checking node engines and refreshing empty model registries…", "checking");
  setupResults?.setAttribute("aria-busy", "true");
  try {
    const allNodes = await fetchJSON("/api/nodes/all");
    const nodes = allNodes.filter((node) => node.healthy);
    if (!nodes.length) {
      setSetupStatus(
        allNodes.length ? "No cluster nodes are online. Offline nodes remain listed above." : "No nodes are registered. Start a node and pair it with this orchestrator first.",
        allNodes.length ? "attention" : "empty"
      );
      if (setupResults) {
        setupResults.innerHTML = '<p class="setup-empty">An online node is required to check engine and model readiness.</p>';
      }
      return;
    }
    const results = await Promise.all(nodes.map(async (node) => {
      try {
        const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/engines`);
        let refreshed = false;
        if (engines.length && !(node.models || []).length) {
          await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models/refresh`, { method: "POST" });
          refreshed = true;
        }
        const models = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models`);
        return { node, engines, models, refreshed, error: "" };
      } catch (err) {
        return { node, engines: [], models: [], refreshed: false, error: err.message };
      }
    }));
    let readyCount = 0;
    if (setupResults) {
      setupResults.innerHTML = results.map(({ node, engines, models, refreshed, error }) => {
        const installedCount = models.length;
        const usableCount = models.filter((model) => model.enabled).length;
        const ready = !error && engines.length > 0 && usableCount > 0;
        if (ready) readyCount += 1;
        const state = error ? `Could not inspect node: ${error}` : !engines.length ? "No inference engine detected. Configure an engine in node settings." : usableCount ? `${usableCount} usable model${usableCount === 1 ? "" : "s"}${installedCount > usableCount ? ` · ${installedCount - usableCount} disabled` : ""}${refreshed ? " (registry refreshed)" : ""}.` : installedCount ? "Models are installed, but all are disabled for scheduling." : "Engine detected, but no models are available yet.";
        const stateName = ready ? "ready" : error ? "error" : "attention";
        const badge = ready ? "Ready" : error ? "Unavailable" : "Needs setup";
        return `<article class="setup-card" data-state="${stateName}">
            <div class="setup-card-heading"><h3>${escapeHtml(node.id)}</h3><span class="setup-badge">${badge}</span></div>
            <dl class="setup-details">
              <div><dt>Engines</dt><dd>${engines.length ? engines.map((engine) => escapeHtml(engine.name)).join(", ") : "None detected"}</dd></div>
              <div><dt>Installed</dt><dd>${installedCount}</dd></div>
              <div><dt>Usable</dt><dd>${usableCount}</dd></div>
            </dl>
            ${models.length ? `<div class="catalog-tags">${models.map((model) => `${escapeHtml(model.name)} · ${escapeHtml(model.engine)} — ${model.enabled ? "usable" : "installed, disabled"}`).join("<br>")}</div>` : ""}
            <p class="setup-card-message">${escapeHtml(state)}</p>
          </article>`;
      }).join("");
    }
    setSetupStatus(
      readyCount === nodes.length ? `All ${nodes.length} node${nodes.length === 1 ? "" : "s"} are ready for inference.` : `${readyCount} of ${nodes.length} nodes are ready. Use node settings to configure engines or select models.`,
      readyCount === nodes.length ? "ready" : "attention"
    );
    await refresh();
  } catch (err) {
    setSetupStatus(`Readiness check failed: ${err.message}`, "error");
  } finally {
    if (setupCheckBtn) {
      setupCheckBtn.disabled = false;
      setupCheckBtn.textContent = "Check readiness";
    }
    setupResults?.removeAttribute("aria-busy");
  }
}
async function loadModelCatalog() {
  if (!catalogResults) return;
  try {
    [modelCatalogEntries] = await Promise.all([
      fetchJSON("/api/models/catalog"),
      loadCatalogNodes()
    ]);
    renderModelCatalog();
  } catch (err) {
    catalogStatus.textContent = `Could not load model catalog: ${err.message}`;
    catalogResults.innerHTML = "";
  }
}
async function loadCatalogNodes() {
  const nodes = await fetchJSON("/api/nodes/all");
  const online = nodes.filter((node) => node.healthy);
  const selected = catalogNodeSelect.value;
  catalogNodeSelect.innerHTML = online.map((node) => `<option value="${escapeHtml(node.id)}">${escapeHtml(node.id)}</option>`).join("");
  if (online.some((node) => node.id === selected)) catalogNodeSelect.value = selected;
  if (!online.length) {
    catalogEngineSelect.innerHTML = "";
    catalogModels = [];
    catalogStatus.textContent = "An online node is required to inspect installed models or install a catalog model.";
    catalogResults.innerHTML = '<p class="setup-empty">No online target nodes.</p>';
    return;
  }
  await loadCatalogTarget();
}
async function loadCatalogTarget() {
  const nodeId = catalogNodeSelect?.value;
  if (!nodeId) return;
  catalogStatus.textContent = `Loading engines and installed models for ${nodeId}…`;
  try {
    const [allEngines, models] = await Promise.all([
      fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/engines`),
      fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`)
    ]);
    catalogEngines = allEngines.filter((item) => item.can_pull);
    catalogModels = models;
    const currentEngine = catalogEngineSelect.value;
    catalogEngineSelect.innerHTML = catalogEngines.map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)} · ${escapeHtml(item.url || "configured engine")}</option>`).join("");
    if (catalogEngines.some((item) => item.name === currentEngine)) catalogEngineSelect.value = currentEngine;
    catalogStatus.textContent = catalogEngines.length ? `Target: ${nodeId} · ${catalogEngineSelect.value || "Select an engine"}. Installed models are marked usable or disabled for scheduling.` : `No configured engine on ${nodeId} supports model downloads.`;
    renderModelCatalog();
  } catch (err) {
    catalogStatus.textContent = `Could not inspect ${nodeId}: ${err.message}`;
    catalogResults.innerHTML = "";
  }
}
function renderModelCatalog() {
  if (!catalogResults) return;
  const engineName = catalogEngineSelect?.value || "";
  const query = (catalogSearch?.value || "").trim().toLowerCase();
  const entries = modelCatalogEntries.filter(
    (model) => model.engine === engineName && (!query || `${model.name} ${model.description} ${(model.tags || []).join(" ")}`.toLowerCase().includes(query))
  );
  if (!engineName) {
    catalogResults.innerHTML = '<p class="setup-empty">Choose an online node with a download-capable engine to browse matching models.</p>';
    return;
  }
  if (!entries.length) {
    catalogResults.innerHTML = '<p class="setup-empty">No catalog entries match this engine and search.</p>';
    return;
  }
  catalogResults.innerHTML = entries.map((model) => {
    const installed = catalogModels.find((item) => item.name === model.name && item.engine === model.engine);
    const state = installed ? installed.enabled ? "Installed · usable" : "Installed · disabled for scheduling" : "Available to download";
    const button = installed ? `<button class="secondary-button" type="button" disabled>${installed.enabled ? "Installed" : "Installed (disabled)"}</button>` : `<button class="primary-btn catalog-install" type="button" data-model="${escapeHtml(model.name)}">Install on ${escapeHtml(catalogNodeSelect.value)} · ${escapeHtml(engineName)}</button>`;
    return `<article class="setup-card catalog-card">
      <div class="setup-card-heading"><h3>${escapeHtml(model.name)}</h3><span class="setup-badge">${escapeHtml(state)}</span></div>
      <p>${escapeHtml(model.description)}</p>
      <div class="catalog-tags">Engine: ${escapeHtml(model.engine)}${model.tags?.length ? ` · ${model.tags.map(escapeHtml).join(", ")}` : ""}</div>
      ${button}
    </article>`;
  }).join("");
  catalogResults.querySelectorAll(".catalog-install").forEach((button) => {
    button.addEventListener("click", () => installCatalogModel(button.dataset.model, button));
  });
}
async function installCatalogModel(model, button) {
  const nodeId = catalogNodeSelect.value;
  const engineName = catalogEngineSelect.value;
  if (!nodeId || !engineName || !model) return;
  button.disabled = true;
  button.textContent = "Installing…";
  catalogStatus.textContent = `Installing ${model} with ${engineName} on ${nodeId}…`;
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/pull`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, engine: engineName })
    });
    catalogModels = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" });
    catalogStatus.textContent = `Installed ${model} on ${nodeId} using ${engineName}.`;
    renderModelCatalog();
    await refresh();
  } catch (err) {
    catalogStatus.textContent = `Could not install ${model} on ${nodeId} using ${engineName}: ${err.message}`;
    button.disabled = false;
    button.textContent = `Retry on ${nodeId} · ${engineName}`;
  }
}
function setSetupStatus(message, state) {
  if (!setupStatus) return;
  setupStatus.textContent = message;
  setupStatus.dataset.state = state;
}
async function loadClusterModels() {
  try {
    const models = await fetchJSON("/api/agent/models");
    if (!agentModelSelect) return;
    const currentVal = agentModelSelect.value;
    agentModelSelect.innerHTML = '<option value="">Auto (Fastest / Cluster Leader)</option>';
    const seen = /* @__PURE__ */ new Set();
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
function formatNodeHardware(node) {
  const hardware = node.hardware || {};
  const parts = [];
  if (hardware.os || hardware.arch) parts.push([hardware.os, hardware.arch].filter(Boolean).join("/"));
  if (hardware.cpu_cores) parts.push(`${hardware.cpu_cores} CPU cores`);
  if (hardware.total_ram_mb) parts.push(`${(hardware.total_ram_mb / 1024).toFixed(1)} GB RAM`);
  if (hardware.has_gpu) {
    parts.push(`${hardware.gpu_vendor || "GPU"}${hardware.gpu_stats_available ? ` · ${hardware.gpu_utilization_percent}% busy` : ""}`);
  }
  if (hardware.has_npu) {
    parts.push(`${hardware.npu_vendor || "NPU"}${hardware.npu_stats_available ? ` · ${hardware.npu_utilization_percent}% busy` : ""}`);
  }
  return parts.length ? parts.join(" · ") : "Hardware details unavailable";
}
function engineInventoryState(engine) {
  const knownStates = ["unknown", "available", "empty", "error"];
  if (knownStates.includes(engine?.inventory_state)) return engine.inventory_state;
  if (!engine?.last_checked_at) return "unknown";
  if (!engine.ready) return "error";
  return Number(engine.model_count) > 0 ? "available" : "empty";
}
function engineInventoryLabel(state) {
  switch (state) {
    case "available":
      return "Inventory available";
    case "empty":
      return "Reachable · no models";
    case "error":
      return "Check failed";
    default:
      return "Not checked";
  }
}
function engineStatusSummaryMarkup(engines) {
  if (!Array.isArray(engines) || !engines.length) {
    return '<span class="engine-status-empty">No engines reported</span>';
  }
  return `<div class="engine-status-summary" aria-label="Inference engine status">${engines.map((engine) => {
    const state = engineInventoryState(engine);
    const count = Math.max(0, Number(engine.model_count) || 0);
    const countLabel = state === "error" ? `${count} known` : `${count} model${count === 1 ? "" : "s"}`;
    const title = engine.last_error ? `${engine.name}: ${engine.last_error}` : `${engine.name}: ${engineInventoryLabel(state)}`;
    return `<span class="engine-status-chip ${state}" title="${escapeHtml(title)}">
      <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
      <span>${escapeHtml(engineInventoryLabel(state))} · ${countLabel}</span>
    </span>`;
  }).join("")}</div>`;
}
function engineStatusDetailsMarkup(engines) {
  if (!Array.isArray(engines) || !engines.length) {
    return '<p class="engine-inventory-empty">No engine inventory status has been reported.</p>';
  }
  return `<section class="engine-inventory-details" aria-label="Inference engine inventory">
    <h4>Inference engine inventory</h4>
    ${engines.map((engine) => {
    const state = engineInventoryState(engine);
    const checkedAt = engine.last_checked_at ? new Date(engine.last_checked_at) : null;
    const successfulAt = engine.last_successful_at ? new Date(engine.last_successful_at) : null;
    const checkedLabel = checkedAt && Number.isFinite(checkedAt.getTime()) ? checkedAt.toLocaleString() : "Not checked";
    const successfulLabel = successfulAt && Number.isFinite(successfulAt.getTime()) ? successfulAt.toLocaleString() : "No successful inventory check";
    const count = Math.max(0, Number(engine.model_count) || 0);
    const error = state === "error" ? `<p class="engine-inventory-error">Inventory check failed: ${escapeHtml(engine.last_error || "No error details were reported.")}</p>` : "";
    return `<article class="engine-inventory-row ${state}">
        <div class="engine-inventory-heading">
          <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
          <span class="engine-inventory-state">${escapeHtml(engineInventoryLabel(state))}</span>
          <span>${count} known model${count === 1 ? "" : "s"}</span>
        </div>
        <small>Last checked: ${escapeHtml(checkedLabel)} · Last successful inventory: ${escapeHtml(successfulLabel)}</small>
        ${error}
      </article>`;
  }).join("")}
  </section>`;
}
function nodeContextDetailsMarkup(node, expanded) {
  const models = (node.models || []).map((model) => {
    const tags = Array.isArray(model.tags) && model.tags.length ? model.tags.join(", ") : "general";
    const engine = model.engine ? ` · ${escapeHtml(model.engine)}` : "";
    return `<div class="overview-node-model">
      <strong>${escapeHtml(model.name || "Unknown model")}</strong>
      <span>${escapeHtml(tags)}${engine}</span>
    </div>`;
  }).join("") || '<div class="overview-node-model"><span>No models reported</span></div>';
  const score = Number.isFinite(node.hardware?.score) ? ` · capability score ${node.hardware.score.toFixed(1)}` : "";
  return `<details class="overview-node-context"${expanded ? " open" : ""}>
    <summary>Hardware, engines &amp; models</summary>
    <div class="overview-node-context-body">
      <div>${escapeHtml(formatNodeHardware(node))}</div>
      <div>Active tasks: ${Number(node.active_tasks) || 0}${score}</div>
      ${engineStatusSummaryMarkup(node.engines)}
      ${engineStatusDetailsMarkup(node.engines)}
      <div class="overview-node-models">${models}</div>
    </div>
  </details>`;
}
function sortNodesByCapability(nodes) {
  return [...nodes].sort((a, b) => {
    if ((b.active_tasks || 0) !== (a.active_tasks || 0)) return (b.active_tasks || 0) - (a.active_tasks || 0);
    if (Boolean(b.leader) !== Boolean(a.leader)) return Number(b.leader) - Number(a.leader);
    const aScore = (a.hardware?.score || 0) + (a.fast_score || 0) * 0.75 + (a.hardware?.total_ram_mb || 0) / 1024 * 0.5 + (a.hardware?.cpu_cores || 0) * 0.25 + (a.hardware?.has_gpu ? 25 : 0) + (a.hardware?.has_npu ? 30 : 0);
    const bScore = (b.hardware?.score || 0) + (b.fast_score || 0) * 0.75 + (b.hardware?.total_ram_mb || 0) / 1024 * 0.5 + (b.hardware?.cpu_cores || 0) * 0.25 + (b.hardware?.has_gpu ? 25 : 0) + (b.hardware?.has_npu ? 30 : 0);
    if (b.healthy !== a.healthy) return Number(b.healthy) - Number(a.healthy);
    if (bScore !== aScore) return bScore - aScore;
    return (b.id || "").localeCompare(a.id || "");
  });
}
function gpuGaugeMarkup(node, size = 40) {
  const hardware = node.hardware || {};
  if (!hardware.has_gpu) return "";
  const utilization = Number(hardware.gpu_utilization_percent);
  if (!hardware.gpu_stats_available || !Number.isFinite(utilization)) {
    return `<span class="gpu-gauge-unavailable" role="img" aria-label="GPU utilization unavailable">GPU n/a</span>`;
  }
  const value = Math.max(0, Math.min(100, utilization));
  const radius = 15;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference * (1 - value / 100);
  return `
    <span class="node-gpu-gauge" role="img" aria-label="GPU utilization ${value}%">
      <svg width="${size}" height="${size}" viewBox="0 0 36 36" aria-hidden="true">
        <circle class="node-gpu-gauge-track" cx="18" cy="18" r="${radius}"></circle>
        <circle class="node-gpu-gauge-value" cx="18" cy="18" r="${radius}"
          stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"></circle>
      </svg>
      <span>${value}%</span>
    </span>`;
}
function npuGaugeMarkup(node, size = 40) {
  const hardware = node.hardware || {};
  if (!hardware.has_npu) return "";
  const utilization = Number(hardware.npu_utilization_percent);
  if (!hardware.npu_stats_available || !Number.isFinite(utilization)) {
    return `<span class="npu-gauge-unavailable" role="img" aria-label="NPU utilization unavailable">NPU n/a</span>`;
  }
  const value = Math.max(0, Math.min(100, utilization));
  const radius = 15;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference * (1 - value / 100);
  return `
    <span class="node-npu-gauge" role="img" aria-label="NPU utilization ${value}%">
      <svg width="${size}" height="${size}" viewBox="0 0 36 36" aria-hidden="true">
        <circle class="node-npu-gauge-track" cx="18" cy="18" r="${radius}"></circle>
        <circle class="node-npu-gauge-value" cx="18" cy="18" r="${radius}"
          stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"></circle>
      </svg>
      <span>${value}%</span>
    </span>`;
}
function nodeAcceleratorMarkup(node, size = 46) {
  const hardware = node.hardware || {};
  const gpuGauge = gpuGaugeMarkup(node, size);
  const npuGauge = npuGaugeMarkup(node, size);
  const gauges = gpuGauge || npuGauge ? `<div class="node-accelerator-gauges">${gpuGauge}${npuGauge}</div>` : "";
  return `${gauges}
    ${hardware.has_gpu ? `<div class="row"><span>GPU</span><span>${escapeHtml(hardware.gpu_vendor || "enabled")}</span></div>` : ""}
    ${hardware.has_npu ? `<div class="row"><span>NPU</span><span>${escapeHtml(hardware.npu_vendor || "enabled")}</span></div>` : ""}`;
}
function resourceHistoryMarkup(node) {
  const hardware = node.hardware || {};
  const systemHistory = Array.isArray(node.system_history) ? node.system_history.slice(-60) : [];
  const series = [
    ...hardware.has_gpu ? [{
      key: "gpu",
      label: "GPU",
      samples: Array.isArray(node.gpu_history) ? node.gpu_history.slice(-60) : [],
      availableKey: "available",
      valueKey: "utilization_percent",
      currentAvailable: hardware.gpu_stats_available,
      currentValue: hardware.gpu_utilization_percent
    }] : [],
    ...hardware.has_npu ? [{
      key: "npu",
      label: "NPU",
      samples: Array.isArray(node.npu_history) ? node.npu_history.slice(-60) : [],
      availableKey: "available",
      valueKey: "utilization_percent",
      currentAvailable: hardware.npu_stats_available,
      currentValue: hardware.npu_utilization_percent
    }] : [],
    {
      key: "cpu",
      label: "CPU",
      samples: systemHistory,
      availableKey: "cpu_available",
      valueKey: "cpu_utilization_percent",
      currentAvailable: hardware.cpu_stats_available,
      currentValue: hardware.cpu_utilization_percent
    },
    {
      key: "ram",
      label: "RAM",
      samples: systemHistory,
      availableKey: "memory_available",
      valueKey: "memory_usage_percent",
      currentAvailable: hardware.memory_stats_available,
      currentValue: hardware.memory_usage_percent
    }
  ];
  const sampleCount = Math.max(1, ...series.map((item) => item.samples.length));
  const left = 8;
  const right = 172;
  const top = 7;
  const bottom = 39;
  const x = (index) => left + index / Math.max(sampleCount - 1, 1) * (right - left);
  const y = (value) => bottom - Math.max(0, Math.min(100, value)) / 100 * (bottom - top);
  const paths = series.map((item) => {
    const offset = sampleCount - item.samples.length;
    const segments = [];
    let segment = [];
    item.samples.forEach((sample, index) => {
      const value = Number(sample[item.valueKey]);
      if (!sample[item.availableKey] || !Number.isFinite(value)) {
        if (segment.length) segments.push(segment);
        segment = [];
        return;
      }
      segment.push({ x: x(offset + index), y: y(value) });
    });
    if (segment.length) segments.push(segment);
    return segments.map((points) => points.length === 1 ? `<circle class="resource-history-point resource-history-${item.key}" cx="${points[0].x.toFixed(1)}" cy="${points[0].y.toFixed(1)}" r="2.5"></circle>` : `<path class="resource-history-line resource-history-${item.key}" d="${points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ")}"></path>`).join("");
  }).join("");
  const legend = series.map((item) => {
    const value = Number(item.currentValue);
    const current = item.currentAvailable && Number.isFinite(value) ? `${Math.round(Math.max(0, Math.min(100, value)))}%` : "n/a";
    return `<span class="resource-history-item"><i class="resource-history-swatch resource-history-${item.key}"></i>${item.label} ${current}</span>`;
  }).join("");
  const labels = series.map((item) => item.label);
  const historyLabel = labels.length > 1 ? `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}` : labels[0];
  const hasSamples = series.some((item) => item.samples.some(
    (sample) => sample[item.availableKey] && Number.isFinite(Number(sample[item.valueKey]))
  ));
  return `<div class="resource-history">
    <div class="resource-history-heading"><strong>Resource utilization</strong><span>Recent · up to 5 minutes</span></div>
    <div class="resource-history-legend">${legend}</div>
    <svg viewBox="0 0 180 48" role="img" aria-label="${historyLabel} utilization history for ${escapeHtml(node.id)}">
      <path class="resource-history-grid" d="M${left},${top}H${right} M${left},${(top + bottom) / 2}H${right} M${left},${bottom}H${right}"></path>
      ${paths}
    </svg>
    ${hasSamples ? "" : '<small class="resource-history-empty">Resource telemetry history is warming up.</small>'}
  </div>`;
}
const TASK_ACTIVE_STATES = /* @__PURE__ */ new Set(["pending", "running", "synthesizing", "awaiting_approval"]);
const TASK_STATES = /* @__PURE__ */ new Set([...TASK_ACTIVE_STATES, "completed", "failed", "cancelled"]);
const SUBTASK_STATES = /* @__PURE__ */ new Set(["pending", "running", "completed", "failed", "cancelled"]);
function knownStatus(status, states) {
  return states.has(status) ? status : "unknown";
}
function nodeAssignments(tasks) {
  const assigned = /* @__PURE__ */ new Map();
  for (const task of tasks || []) {
    if (!TASK_ACTIVE_STATES.has(task.status)) continue;
    for (const subtask of task.subtasks || []) {
      if (!["pending", "running"].includes(subtask.status) || !subtask.node_id) continue;
      const entries = assigned.get(subtask.node_id) || [];
      entries.push({ taskID: task.id, status: subtask.status });
      assigned.set(subtask.node_id, entries);
    }
  }
  return assigned;
}
function renderOverview(nodes, tasks, peers, parts = {}) {
  const nodeList = sortNodesByCapability(nodes || []);
  const capacityByNode = new Map(
    (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node])
  );
  const memberIDs = new Set(nodeList.map((node) => node.id));
  const discoveredNodes = (peers || []).filter((peer) => peer.role === "node" && peer.id && !memberIDs.has(peer.id)).map((peer) => ({
    id: peer.id,
    address: peer.peer_addr || peer.http_addr,
    healthy: true,
    discovered: true,
    last_seen: peer.last_seen,
    models: [],
    active_tasks: 0,
    hardware: {}
  }));
  const displayNodes = sortNodesByCapability([...nodeList, ...discoveredNodes]);
  const taskList = [...tasks || []].sort(
    (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
  );
  const activeTasks = taskList.filter((task) => TASK_ACTIVE_STATES.has(task.status));
  const recentTasks = taskList.filter((task) => !TASK_ACTIVE_STATES.has(task.status)).slice(0, 8);
  const assignments = nodeAssignments(activeTasks);
  const online = nodeList.filter((node) => node.healthy).length;
  const offline = nodeList.length - online;
  if (clusterOverviewStatus) {
    clusterOverviewStatus.textContent = `${online} online · ${offline} offline · ${discoveredNodes.length} available to pair · ${activeTasks.length} active tasks`;
  }
  if (parts.activity !== false && overviewActivity) {
    const overviewTasks = [...activeTasks, ...recentTasks].sort(
      (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
    if (overviewTasks.length) {
      renderTaskHistoryTable(overviewActivity, overviewTasks, "overview-task", true);
    } else {
      overviewActivity.innerHTML = '<p class="overview-empty">No task activity yet.</p>';
    }
  }
  if (parts.nodes !== false && overviewNodes) {
    const leaderID = nodeList.find((node) => node.leader)?.id;
    const expandedNodeIDs = new Set(
      [...overviewNodes.querySelectorAll(".overview-node-context[open]")].map((details) => details.closest("[data-node-id]")?.dataset.nodeId).filter(Boolean)
    );
    overviewNodes.innerHTML = displayNodes.length ? displayNodes.map((node) => {
      const capacity = capacityByNode.get(node.id);
      const active = capacity?.active ?? node.active_tasks ?? 0;
      const capacityLabel = capacity ? ` · ${capacity.remaining}/${capacity.max} slots free` : "";
      const nodeTasks = assignments.get(node.id) || [];
      const state = node.discovered ? "discovered" : node.healthy ? "online" : "offline";
      const status = node.discovered ? "available · not paired" : state;
      const lastSeen = node.last_seen ? `${node.discovered ? "Seen" : "Last seen"} ${formatRelativeTime(node.last_seen)}` : "Last seen unavailable";
      return `<article class="overview-node ${state}" data-node-id="${escapeHtml(node.id)}">
            <div class="overview-node-heading"><span class="legend-dot ${node.discovered ? "peer" : state}"></span><strong>${escapeHtml(node.id)}</strong><span class="badge ${state}">${status}</span></div>
            <div class="overview-node-address">${escapeHtml(node.address || "Address unavailable")}</div>
            <div class="overview-node-meta">${node.discovered ? "Discovered on the local network" : `${node.id === leaderID ? "Cluster leader · " : ""}${active} active tasks${capacityLabel} · ${(node.models || []).length} models`}</div>
            <div class="overview-node-metrics">${gpuGaugeMarkup(node, 36)}${npuGaugeMarkup(node, 36)}${node.hardware?.cpu_stats_available ? `<span>CPU ${node.hardware.cpu_utilization_percent}%</span>` : ""}${node.hardware?.memory_stats_available ? `<span>RAM ${node.hardware.memory_usage_percent}%</span>` : ""}</div>
            <small data-last-seen="${escapeHtml(node.last_seen || "")}">${lastSeen}</small>
            ${nodeTasks.length ? `<div class="overview-node-assignments">${nodeTasks.map((item) => `<span class="assignment-chip ${item.status}">${escapeHtml(item.taskID)} · ${item.status}</span>`).join("")}</div>` : ""}
            ${node.discovered ? "" : nodeContextDetailsMarkup(node, expandedNodeIDs.has(node.id))}
          </article>`;
    }).join("") : '<p class="overview-empty">No known cluster members.</p>';
    const nodesByID = new Map(displayNodes.map((node) => [node.id, node]));
    overviewNodes.querySelectorAll("[data-node-id]").forEach((card) => {
      const node = nodesByID.get(card.dataset.nodeId);
      if (node) attachNodeContextMenu(card, node);
    });
  }
  if (parts.peers !== false && clusterPeers) {
    const discovered = (peers || []).filter((peer) => peer.role && peer.role !== "node");
    clusterPeers.innerHTML = discovered.length ? `<span class="cluster-peers-label">Discovered peers</span>${discovered.map((peer) => `<span class="cluster-peer"><i class="legend-dot peer"></i><strong>${escapeHtml(peer.id || "unknown")}</strong><small>${escapeHtml(peer.role)} · ${escapeHtml(peer.http_addr || "address unavailable")}</small></span>`).join("")}` : "";
  }
}
overviewActivity?.addEventListener("click", (event) => {
  const target = event.target.closest("[data-target-node]");
  if (!target || !overviewNodes) return;
  const node = [...overviewNodes.querySelectorAll("[data-node-id]")].find(
    (item) => item.dataset.nodeId === target.dataset.targetNode
  );
  if (!node) return;
  const membersTab = document.getElementById("overview-members-tab");
  membersTab?.click();
  membersTab?.focus({ preventScroll: true });
  node.scrollIntoView({ behavior: "smooth", block: "nearest" });
  node.classList.add("highlighted");
  setTimeout(() => node.classList.remove("highlighted"), 1200);
});
function renderNodes(nodes) {
  dashboardState.nodes = nodes || [];
  if (!nodesList) return;
  nodesList.innerHTML = "";
  if (!nodes || nodes.length === 0) {
    nodesList.innerHTML = '<p style="color:var(--muted)">No nodes registered yet.</p>';
    return;
  }
  const ordered = sortNodesByCapability(nodes);
  const leaderID = ordered.find((n) => n.leader)?.id;
  const fastestID = ordered.reduce((best, node) => !best || node.fast_score > best.fast_score || node.fast_score === best.fast_score && node.id < best.id ? node : best, null)?.id;
  for (const n of ordered) {
    const card = document.createElement("div");
    card.className = "card" + (n.id === leaderID ? " leader" : "");
    const modelTags = (n.models || []).map((m) => `<button class="model-tag-btn" data-node="${escapeHtml(n.id)}" data-model="${escapeHtml(m.name)}" title="Directly test ${escapeHtml(m.name)} on ${escapeHtml(n.id)}" ${n.healthy ? "" : "disabled"}>⚡ ${escapeHtml(m.name)}</button>`).join("");
    const nodeActions = n.healthy ? `<button class="model-config-toggle" data-node="${escapeHtml(n.id)}">⚙ Configure models</button>
         <button class="node-model-refresh" data-node="${escapeHtml(n.id)}">↻ Rescan engines &amp; models</button>
         <button class="node-terminal-open" data-node="${escapeHtml(n.id)}">▸ Open node terminal</button>` : `<small class="node-offline-note">Offline · last seen ${formatRelativeTime(n.last_seen)}</small>`;
    card.innerHTML = `
      <div class="node-card-heading"><h3>${escapeHtml(n.id)} <span class="badge ${n.healthy ? "healthy" : "unhealthy"}">${n.healthy ? "online" : "offline"}</span></h3></div>
      ${n.version_error ? `<div class="node-version-error">⚠ ${escapeHtml(n.version_error)}</div>` : ""}
      <div class="role-tags">${n.id === leaderID ? '<span class="role-tag coordinator">cluster leader · orchestrator</span>' : ""}${n.id === fastestID && n.id !== leaderID ? '<span class="role-tag worker">fastest</span>' : ""}</div>
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
    nodesList.appendChild(card);
    card.querySelector(".node-terminal-open")?.addEventListener("click", () => openNodeTerminal(n));
    card.querySelector(".node-model-refresh")?.addEventListener("click", () => refreshNodeModels(n.id));
    attachNodeContextMenu(card, n);
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
        setTimeout(() => {
          btn.textContent = `⚡ ${model}`;
        }, 3500);
      });
    });
    if (modelPanelOpen.has(n.id)) {
      if (lastFetchedModels.has(n.id)) {
        renderModelPanel(n.id, lastFetchedModels.get(n.id), n.engines);
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
function renderSetupNodes(nodes) {
  if (!setupNodeList) return;
  const ordered = sortNodesByCapability(nodes || []);
  if (setupNodeSummary) {
    const online = ordered.filter((node) => node.healthy).length;
    setupNodeSummary.textContent = `${online} online · ${ordered.length - online} offline · ${ordered.length} total`;
  }
  if (!ordered.length) {
    setupNodeList.innerHTML = '<p class="setup-empty">No cluster members are registered yet. Pair a node to add it to the cluster.</p>';
    return;
  }
  setupNodeList.innerHTML = "";
  for (const node of ordered) {
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
    setupNodeList.appendChild(card);
  }
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
  terminalAbortController = new AbortController();
  if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = false;
  try {
    if (!terminalDialogueId) {
      const dialogue = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }), signal: terminalAbortController.signal });
      terminalDialogueId = dialogue.id;
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
    terminalAbortController = null;
    if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = true;
  }
});
nodeTerminalInterruptBtn?.addEventListener("click", async () => {
  if (terminalNode && terminalDialogueId) {
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/interrupt`, {
        method: "POST"
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
  contextNode = node;
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
        memory_path: value("memory_path")
      })
    });
    status.textContent = "Saved";
    await refresh();
    setTimeout(closeNodeProperties, 500);
  } catch (err) {
    status.textContent = "Error: " + err.message;
  }
}
async function refreshNodeModels(nodeId) {
  const button = [...nodesList?.querySelectorAll(".node-model-refresh") || []].find((candidate) => candidate.dataset.node === nodeId);
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
  if (properties) {
    properties.textContent = `Configure ${contextNode.id}`;
    properties.disabled = !contextNode.healthy || nodeIsDiscovered;
  }
  if (pairing) {
    pairing.textContent = nodeIsDiscovered ? "Pair node to this cluster" : "Re-pair node with this cluster";
    pairing.disabled = !contextNode.healthy;
  }
  menu.querySelector("[data-action=open]").disabled = !contextNode.healthy || nodeIsDiscovered;
  menu.hidden = false;
  const bounds = menu.getBoundingClientRect();
  menu.style.left = `${Math.max(8, Math.min(x, window.innerWidth - bounds.width - 8))}px`;
  menu.style.top = `${Math.max(8, Math.min(y, window.innerHeight - bounds.height - 8))}px`;
}
function attachNodeContextMenu(element, node) {
  element.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    contextNode = node;
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
  const modelsByEngine = /* @__PURE__ */ new Map();
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
      body: JSON.stringify({ disabled_models: disabled, default_model: defaultModel })
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
  const byNode = new Map(nodes.map((n) => [n.id, {
    node: n,
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    unknown: 0,
    recent: []
  }]));
  const sortedTasks = [...tasks || []].sort(
    (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
  );
  for (const t of sortedTasks) {
    for (const s of t.subtasks || []) {
      const bucket = byNode.get(s.node_id);
      if (!bucket) continue;
      const status = knownStatus(s.status, SUBTASK_STATES);
      bucket[status]++;
      if (bucket.recent.length < 5) {
        bucket.recent.push({ taskId: t.id, subtaskId: s.subtask?.id || "subtask", model: s.model, status, taskType: s.subtask?.task_type || "work" });
      }
    }
  }
  const workload = [...byNode.values()].sort((a, b) => {
    if ((b.node.active_tasks || 0) !== (a.node.active_tasks || 0)) return (b.node.active_tasks || 0) - (a.node.active_tasks || 0);
    return b.running + b.pending - (a.running + a.pending);
  });
  for (const { node, pending, running, completed, failed, cancelled, unknown, recent } of workload) {
    const card = document.createElement("div");
    card.className = "card";
    const total = pending + running + completed + failed + cancelled + unknown;
    const pct = (n) => total === 0 ? 0 : Math.round(n / total * 100);
    const recentHTML = recent.length ? recent.map(
      (r) => `<div class="row"><span>${r.taskId}/${r.subtaskId} (${r.taskType}) &rarr; ${escapeHtml(r.model)}</span><span class="badge ${r.status}">${r.status}</span></div>`
    ).join("") : '<p style="color:var(--muted);font-size:0.8rem">No jobs routed yet.</p>';
    card.innerHTML = `
      <h3>${escapeHtml(node.id)} <span class="badge ${node.healthy ? "healthy" : "unhealthy"}">${node.active_tasks} active</span></h3>
      <div class="workload-bar" role="img" aria-label="${pending} pending, ${running} running, ${completed} completed, ${failed} failed, ${cancelled} cancelled">
        <div class="workload-seg workload-pending" style="width:${pct(pending)}%"></div>
        <div class="workload-seg workload-running" style="width:${pct(running)}%"></div>
        <div class="workload-seg workload-completed" style="width:${pct(completed)}%"></div>
        <div class="workload-seg workload-failed" style="width:${pct(failed)}%"></div>
        <div class="workload-seg workload-cancelled" style="width:${pct(cancelled)}%"></div>
      </div>
      <div class="row"><span>Pending</span><span>${pending}</span></div>
      <div class="row"><span>Running</span><span>${running}</span></div>
      <div class="row"><span>Completed</span><span>${completed}</span></div>
      <div class="row"><span>Failed</span><span>${failed}</span></div>
      <div class="row"><span>Cancelled${unknown ? ` / unknown` : ""}</span><span>${cancelled}${unknown ? ` / ${unknown}` : ""}</span></div>
      <h4>Recent jobs</h4>
      ${recentHTML}
    `;
    routingList.appendChild(card);
  }
}
function visibleNetworkPeers(peers, nodes, pairingStatus2) {
  const visible = new Map((peers || []).map((peer) => [peer.id, peer]));
  for (const node of nodes || []) {
    if (!node.healthy || visible.has(node.id)) continue;
    visible.set(node.id, {
      id: node.id,
      role: "node",
      peer_addr: node.address,
      last_seen: node.last_seen,
      orchestrator_id: pairingStatus2?.orchestrator_id
    });
  }
  if (pairingStatus2?.role === "node" && pairingStatus2.node_id) {
    visible.set(pairingStatus2.node_id, {
      ...visible.get(pairingStatus2.node_id),
      id: pairingStatus2.node_id,
      role: "node",
      peer_addr: pairingStatus2.node_addr,
      orchestrator_id: pairingStatus2.joined ? pairingStatus2.orchestrator_id : "",
      local: true
    });
  }
  return [...visible.values()].sort((a, b) => (a.id || "").localeCompare(b.id || ""));
}
function renderDiscovery(peers) {
  if (!discoveryList) return;
  discoveryList.innerHTML = "";
  const ordered = visibleNetworkPeers(peers, dashboardState.nodes, dashboardState.pairing.status);
  if (ordered.length === 0) {
    discoveryList.innerHTML = '<p style="color:var(--muted)">No visible nodes found on the local network yet.</p>';
    return;
  }
  for (const p of ordered) {
    const card = document.createElement("div");
    card.className = "card";
    const associated = p.role === "node" ? p.orchestrator_id || "Not announced" : p.role === "orchestrator" ? "self" : "Not elected";
    const member = p.role === "node" ? dashboardState.nodes.find((node) => node.id === p.id) : null;
    const localMember = p.id === dashboardState.pairing.status?.node_id && dashboardState.pairing.status?.joined;
    card.innerHTML = `
      <h3>${escapeHtml(p.id)} <span class="badge healthy">${escapeHtml(p.role)}</span></h3>
      <div class="row"><span>Address</span><span>${escapeHtml(p.peer_addr || p.http_addr || "Address unavailable")}</span></div>
      ${p.role === "node" ? `<div class="row"><span>Status</span><span>${member || localMember ? "Visible · cluster member" : "Visible · not a member of this cluster"}${p.local ? " · local node" : ""}</span></div>` : ""}
      <div class="row"><span>Associated orchestrator</span><span>${escapeHtml(associated)}</span></div>
      <div class="row"><span>Last seen</span><span>${p.local ? "This device" : p.last_seen ? new Date(p.last_seen).toLocaleTimeString() : "Online in cluster"}</span></div>
    `;
    discoveryList.appendChild(card);
    if (p.role === "node") {
      attachNodeContextMenu(card, member || {
        id: p.id,
        address: p.peer_addr || p.http_addr,
        healthy: true,
        discovered: !p.local,
        last_seen: p.last_seen,
        models: [],
        active_tasks: 0,
        hardware: {}
      });
    }
  }
}
function renderTaskHistoryTable(container, tasks, idPrefix, linkNodes = false) {
  const expandedTaskIds = new Set(
    Array.from(container.querySelectorAll?.(".task-details-row:not([hidden])") || []).map((item) => item.dataset.taskId).filter(Boolean)
  );
  container.innerHTML = "";
  const validStatuses = /* @__PURE__ */ new Set(["pending", "running", "synthesizing", "awaiting_approval", "completed", "failed", "cancelled"]);
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
      const node = s.node_id ? linkNodes ? `<button class="activity-assignment" type="button" data-target-node="${escapeHtml(s.node_id)}">${escapeHtml(s.node_id)}</button>` : escapeHtml(s.node_id) : "Unassigned";
      const attempts = Array.isArray(s.attempts) ? s.attempts : [];
      const attemptTimestamps = attempts.length ? attempts.map((attempt, attemptIndex) => {
        const attemptNumber = Number(attempt.number) || attemptIndex + 1;
        return `<div class="task-step-attempt"><strong>Attempt ${attemptNumber}</strong>${formatTimestamp("Started", attempt.started_at)}${attempt.finished_at ? formatTimestamp("Finished", attempt.finished_at) : ""}</div>`;
      }).join("") : `<div class="task-step-attempt"><span>Not dispatched</span>${formatTimestamp("Task created", t.created_at)}</div>`;
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
    if (taskHistoryCount) taskHistoryCount.textContent = "0 tasks";
    appendTaskHistoryLoadMore(hasMore, loading);
    return;
  }
  const query = (taskHistorySearch?.value || "").trim().toLowerCase();
  const statusFilter = taskHistoryStatus?.value || "all";
  const orderedTasks = [...tasks].filter((task) => {
    const status = task.status || "unknown";
    if (statusFilter !== "all" && status !== statusFilter) return false;
    if (!query) return true;
    const searchable = [
      task.id,
      task.description,
      status,
      task.result,
      task.error,
      ...(task.subtasks || []).flatMap((item) => [
        item.subtask?.id,
        item.subtask?.description,
        item.subtask?.role,
        item.subtask?.task_type,
        item.node_id,
        item.model,
        item.output,
        item.error
      ])
    ].filter(Boolean).join(" ").toLowerCase();
    return searchable.includes(query);
  }).sort(
    (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
  );
  if (taskHistoryCount) {
    taskHistoryCount.textContent = `Showing ${orderedTasks.length} of ${tasks.length} ${tasks.length === 1 ? "task" : "tasks"}`;
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
    setTimeout(() => {
      button.textContent = original;
    }, 1200);
  } catch (err) {
    button.textContent = "Copy failed";
  }
}
function taskText(tasks) {
  return (tasks || []).map((t) => [
    `Task ${t.id}: ${t.description}`,
    ...(t.subtasks || []).map((s) => `${s.subtask.id} (${s.status}) on ${s.node_id}:
${s.output || s.error || ""}`)
  ].join("\n")).join("\n\n");
}
copyTasksBtn?.addEventListener("click", () => copyAllTaskResults());

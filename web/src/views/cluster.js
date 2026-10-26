// --- Cluster Overview & Routing ---

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
  const utilization = hardware.gpu_utilization_percent;
  if (!hardware.gpu_stats_available || typeof utilization !== "number" || !Number.isFinite(utilization)) {
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
  const utilization = hardware.npu_utilization_percent;
  if (!hardware.npu_stats_available || typeof utilization !== "number" || !Number.isFinite(utilization)) {
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
  const gauges = gpuGauge || npuGauge
    ? `<div class="node-accelerator-gauges">${gpuGauge}${npuGauge}</div>`
    : "";
  return `${gauges}
    ${hardware.has_gpu ? `<div class="row"><span>GPU</span><span>${escapeHtml(hardware.gpu_vendor || "enabled")}</span></div>` : ""}
    ${hardware.has_npu ? `<div class="row"><span>NPU</span><span>${escapeHtml(hardware.npu_vendor || "enabled")}</span></div>` : ""}`;
}

function resourceTelemetryLabel(node, now = Date.now()) {
  const capacity = typeof dashboardState === "undefined" ? null : dashboardState.capacity;
  const state = capacity?.nodes?.find((item) => item.node_id === node.id)?.telemetry_state;
  const timestamp = Date.parse(node.telemetry_at || "");
  if (state === "unavailable" || !Number.isFinite(timestamp) || timestamp <= 0) return "Resource telemetry unavailable";
  const age = now - timestamp;
  const window = capacity?.telemetry_freshness?.freshness_window_ms ?? 30000;
  return state === "stale" || age < 0 || age > window ? "Stale resource telemetry" : "Fresh resource telemetry";
}

function resourceHistoryMarkup(node) {
  const hardware = node.hardware || {};
  const systemHistory = Array.isArray(node.system_history) ? node.system_history.slice(-60) : [];
  const series = [
    ...(hardware.has_gpu ? [{
      key: "gpu",
      label: "GPU",
      samples: Array.isArray(node.gpu_history) ? node.gpu_history.slice(-60) : [],
      availableKey: "available",
      valueKey: "utilization_percent",
      currentAvailable: hardware.gpu_stats_available,
      currentValue: hardware.gpu_utilization_percent,
    }] : []),
    ...(hardware.has_npu ? [{
      key: "npu",
      label: "NPU",
      samples: Array.isArray(node.npu_history) ? node.npu_history.slice(-60) : [],
      availableKey: "available",
      valueKey: "utilization_percent",
      currentAvailable: hardware.npu_stats_available,
      currentValue: hardware.npu_utilization_percent,
    }] : []),
    {
      key: "cpu",
      label: "CPU",
      samples: systemHistory,
      availableKey: "cpu_available",
      valueKey: "cpu_utilization_percent",
      currentAvailable: hardware.cpu_stats_available,
      currentValue: hardware.cpu_utilization_percent,
    },
    {
      key: "ram",
      label: "RAM",
      samples: systemHistory,
      availableKey: "memory_available",
      valueKey: "memory_usage_percent",
      currentAvailable: hardware.memory_stats_available,
      currentValue: hardware.memory_usage_percent,
    },
  ];
  const sampleCount = Math.max(1, ...series.map((item) => item.samples.length));
  const left = 8;
  const right = 172;
  const top = 7;
  const bottom = 39;
  const x = (index) => left + (index / Math.max(sampleCount - 1, 1)) * (right - left);
  const y = (value) => bottom - (Math.max(0, Math.min(100, value)) / 100) * (bottom - top);
  const paths = series.map((item) => {
    const offset = sampleCount - item.samples.length;
    const segments = [];
    let segment = [];
    item.samples.forEach((sample, index) => {
      const value = sample[item.valueKey];
      if (!sample[item.availableKey] || typeof value !== "number" || !Number.isFinite(value)) {
        if (segment.length) segments.push(segment);
        segment = [];
        return;
      }
      segment.push({ x: x(offset + index), y: y(value) });
    });
    if (segment.length) segments.push(segment);
    return segments.map((points) => points.length === 1
      ? `<circle class="resource-history-point resource-history-${item.key}" cx="${points[0].x.toFixed(1)}" cy="${points[0].y.toFixed(1)}" r="2.5"></circle>`
      : `<path class="resource-history-line resource-history-${item.key}" d="${points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ")}"></path>`)
      .join("");
  }).join("");
  const legend = series.map((item) => {
    const value = item.currentValue;
    const current = item.currentAvailable && typeof value === "number" && Number.isFinite(value)
      ? `${Math.round(Math.max(0, Math.min(100, value)))}%`
      : "n/a";
    return `<span class="resource-history-item"><i class="resource-history-swatch resource-history-${item.key}"></i>${item.label} ${current}</span>`;
  }).join("");
  const labels = series.map((item) => item.label);
  const historyLabel = labels.length > 1
    ? `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`
    : labels[0];
  const hasSamples = series.some((item) => item.samples.some((sample) =>
    sample[item.availableKey] && typeof sample[item.valueKey] === "number" && Number.isFinite(sample[item.valueKey])
  ));
  return `<div class="resource-history">
    <div class="resource-history-heading"><strong>Resource utilization</strong><span>${resourceTelemetryLabel(node)}</span></div>
    <div class="resource-history-legend">${legend}</div>
    <svg viewBox="0 0 180 48" role="img" aria-label="${historyLabel} utilization history for ${escapeHtml(node.id)}">
      <path class="resource-history-grid" d="M${left},${top}H${right} M${left},${(top + bottom) / 2}H${right} M${left},${bottom}H${right}"></path>
      ${paths}
    </svg>
    ${hasSamples ? "" : '<small class="resource-history-empty">Resource telemetry history is warming up.</small>'}
  </div>`;
}

const TASK_ACTIVE_STATES = new Set(["pending", "running", "synthesizing", "awaiting_approval"]);
const TASK_STATES = new Set([...TASK_ACTIVE_STATES, "completed", "failed", "cancelled"]);
const SUBTASK_STATES = new Set(["pending", "running", "completed", "failed", "cancelled"]);

function knownStatus(status, states) {
  return states.has(status) ? status : "unknown";
}

function nodeAssignments(tasks) {
  const assigned = new Map();
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
    (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node]),
  );
  const memberIDs = new Set(nodeList.map((node) => node.id));
  const discoveredNodes = (peers || [])
    .filter((peer) => peer.role === "node" && peer.id && !memberIDs.has(peer.id))
    .map((peer) => ({
      id: peer.id,
      address: peer.peer_addr || peer.http_addr,
      healthy: true,
      discovered: true,
      last_seen: peer.last_seen,
      models: [],
      active_tasks: 0,
      hardware: {},
    }));
  const displayNodes = sortNodesByCapability([...nodeList, ...discoveredNodes]);
  const taskList = [...(tasks || [])].sort((a, b) =>
    new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
    const overviewTasks = [...activeTasks, ...recentTasks].sort((a, b) =>
      new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
      [...overviewNodes.querySelectorAll(".overview-node-context[open]")]
        .map((details) => details.closest("[data-node-id]")?.dataset.nodeId)
        .filter(Boolean),
    );
    const nodeItems = displayNodes.length ? displayNodes : [{ id: "__empty", empty: true }];
    dashboardDOM.patchKeyedChildren(overviewNodes, nodeItems, {
      keyOf: (item) => item.id,
      signatureOf: (item) => item.empty ? "empty" : JSON.stringify({
        node: item,
        capacity: capacityByNode.get(item.id),
        assignments: assignments.get(item.id),
        leaderID,
        expanded: expandedNodeIDs.has(item.id),
      }),
      renderItem: (node) => {
        if (node.empty) {
          const empty = document.createElement("p");
          empty.className = "overview-empty";
          empty.textContent = "No known cluster members.";
          return empty;
        }
        const capacity = capacityByNode.get(node.id);
        const active = capacity?.active ?? node.active_tasks ?? 0;
        const capacityLabel = capacity ? ` · ${capacity.remaining}/${capacity.max} slots free` : "";
        const nodeTasks = assignments.get(node.id) || [];
        const state = node.discovered ? "discovered" : node.healthy ? "online" : "offline";
        const status = node.discovered ? "available · not paired" : state;
        const lastSeen = node.last_seen
          ? `${node.discovered ? "Seen" : "Last seen"} ${formatRelativeTime(node.last_seen)}`
          : "Last seen unavailable";
        const card = document.createElement("article");
        card.className = `overview-node ${state}`;
        card.dataset.nodeId = node.id;
        card.innerHTML = `
          <div class="overview-node-heading"><span class="legend-dot ${node.discovered ? "peer" : state}"></span><strong>${escapeHtml(node.id)}</strong><span class="badge ${state}">${status}</span></div>
          <div class="overview-node-address">${escapeHtml(node.address || "Address unavailable")}</div>
          <div class="overview-node-meta">${node.discovered ? "Discovered on the local network" : `${node.id === leaderID ? "Cluster leader · " : ""}${active} active tasks${capacityLabel} · ${(node.models || []).length} models`}</div>
          <div class="overview-node-metrics">${gpuGaugeMarkup(node, 36)}${npuGaugeMarkup(node, 36)}${node.hardware?.cpu_stats_available ? `<span>CPU ${node.hardware.cpu_utilization_percent}%</span>` : ""}${node.hardware?.memory_stats_available ? `<span>RAM ${node.hardware.memory_usage_percent}%</span>` : ""}</div>
          <small data-last-seen="${escapeHtml(node.last_seen || "")}">${lastSeen}</small>
          ${nodeTasks.length ? `<div class="overview-node-assignments">${nodeTasks.map((item) => `<span class="assignment-chip ${item.status}">${escapeHtml(item.taskID)} · ${item.status}</span>`).join("")}</div>` : ""}
          ${node.discovered ? "" : nodeContextDetailsMarkup(node, expandedNodeIDs.has(node.id))}
        `;
        if (!node.discovered) attachNodeContextMenu(card, node);
        return card;
      },
    });
  }

  if (parts.peers !== false && clusterPeers) {
    const discovered = (peers || []).filter((peer) => peer.role && peer.role !== "node");
    clusterPeers.innerHTML = discovered.length
      ? `<span class="cluster-peers-label">Discovered peers</span>${discovered.map((peer) => `<span class="cluster-peer"><i class="legend-dot peer"></i><strong>${escapeHtml(peer.id || "unknown")}</strong><small>${escapeHtml(peer.role)} · ${escapeHtml(peer.http_addr || "address unavailable")}</small></span>`).join("")}`
      : "";
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

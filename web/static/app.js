async function refresh() {
  try {
    const [nodes, peers, build] = await Promise.all([
      fetchJSON("/api/nodes/all"),
      fetchJSON("/api/discovery"),
      fetchJSON("/api/version")
    ]);
    await refreshTaskHistory();
    applySnapshot({ nodes, tasks: dashboardState.tasks || [], discovery: peers, build_version: build.version });
  } catch (err) {
    console.error(err);
  }
}
function taskHistoryURL(cursor = "") {
  const params = new URLSearchParams({ page_size: "50" });
  const query = (taskHistorySearch?.value || "").trim();
  const status = taskHistoryStatus?.value || "all";
  if (query) params.set("q", query);
  if (status !== "all") params.set("status", status);
  if (cursor) params.set("cursor", cursor);
  return `/api/tasks?${params}`;
}
function normalizeHistoryPage(response) {
  return Array.isArray(response) ? { items: response, next_cursor: "", has_more: false } : {
    items: Array.isArray(response?.items) ? response.items : [],
    next_cursor: response?.next_cursor || "",
    has_more: Boolean(response?.has_more)
  };
}
function sortTaskHistory(items) {
  return items.sort(
    (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
  );
}
async function refreshTaskHistory(reset = false) {
  const query = (taskHistorySearch?.value || "").trim();
  const status = taskHistoryStatus?.value || "all";
  const filterKey = JSON.stringify([query, status]);
  if (reset || !taskHistoryPage.initialized || filterKey !== taskHistoryPage.filterKey) {
    taskHistoryPage.generation++;
    taskHistoryPage.items = [];
    taskHistoryPage.nextCursor = "";
    taskHistoryPage.hasMore = false;
    taskHistoryPage.loading = false;
    taskHistoryPage.initialized = false;
    taskHistoryPage.filterKey = filterKey;
    reset = true;
  }
  if (taskHistoryPage.loading) return;
  const generation = taskHistoryPage.generation;
  taskHistoryPage.loading = true;
  renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, true);
  try {
    const page = normalizeHistoryPage(await fetchJSON(taskHistoryURL()));
    if (generation !== taskHistoryPage.generation) return;
    if (reset) {
      taskHistoryPage.items = page.items;
      taskHistoryPage.nextCursor = page.next_cursor;
      taskHistoryPage.hasMore = page.has_more;
    } else {
      const updated = new Map(taskHistoryPage.items.map((task) => [task.id, task]));
      for (const task of page.items) updated.set(task.id, task);
      taskHistoryPage.items = sortTaskHistory([...updated.values()]);
      taskHistoryPage.hasMore ||= page.has_more;
    }
    taskHistoryPage.initialized = true;
  } catch (err) {
    console.error("task history refresh failed", err);
  } finally {
    if (generation === taskHistoryPage.generation) {
      taskHistoryPage.loading = false;
      renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, false);
    }
  }
}
async function loadMoreTaskHistory() {
  const filterKey = JSON.stringify([
    (taskHistorySearch?.value || "").trim(),
    taskHistoryStatus?.value || "all"
  ]);
  if (filterKey !== taskHistoryPage.filterKey) {
    await refreshTaskHistory(true);
    return;
  }
  if (taskHistoryPage.loading || !taskHistoryPage.hasMore || !taskHistoryPage.nextCursor) return;
  const generation = taskHistoryPage.generation;
  taskHistoryPage.loading = true;
  renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, true);
  try {
    const page = normalizeHistoryPage(await fetchJSON(taskHistoryURL(taskHistoryPage.nextCursor)));
    if (generation !== taskHistoryPage.generation) return;
    const updated = new Map(taskHistoryPage.items.map((task) => [task.id, task]));
    for (const task of page.items) updated.set(task.id, task);
    taskHistoryPage.items = sortTaskHistory([...updated.values()]);
    taskHistoryPage.nextCursor = page.next_cursor;
    taskHistoryPage.hasMore = page.has_more;
  } catch (err) {
    console.error("loading more task history failed", err);
  } finally {
    if (generation === taskHistoryPage.generation) {
      taskHistoryPage.loading = false;
      renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, false);
    }
  }
}
async function copyAllTaskResults() {
  try {
    const tasks = [];
    let cursor = "";
    do {
      const params = new URLSearchParams({ page_size: "100" });
      if (cursor) params.set("cursor", cursor);
      const page = normalizeHistoryPage(await fetchJSON(`/api/tasks?${params}`));
      tasks.push(...page.items);
      if (page.has_more && !page.next_cursor) throw new Error("task history page omitted its continuation cursor");
      cursor = page.next_cursor;
    } while (cursor);
    await copyText(taskText(tasks), copyTasksBtn);
  } catch (err) {
    console.error("copying task history failed", err);
    if (submitStatus) submitStatus.textContent = `Unable to copy task history: ${err.message}`;
  }
}
function mergeLiveTaskHistory(tasks) {
  if (!taskHistoryPage.initialized || !taskHistoryPage.items.length) return;
  const updates = new Map((tasks || []).map((task) => [task.id, task]));
  taskHistoryPage.items = sortTaskHistory(taskHistoryPage.items.map((task) => updates.get(task.id) || task));
}
async function refreshDiscovery() {
  try {
    const discovery = await fetchJSON("/api/discovery");
    applySnapshot({
      nodes: dashboardState.nodes || [],
      tasks: dashboardState.tasks || [],
      discovery
    });
  } catch (err) {
    console.error("local network refresh failed", err);
  }
}
setInterval(refreshDiscovery, 5e3);
function applySnapshot(snap) {
  if (snap.complete_tasks) {
    dashboardState.tasks = snap.tasks || [];
    dashboardState.fullTaskHistoryLoaded = true;
  } else if (dashboardState.fullTaskHistoryLoaded) {
    const merged = new Map((dashboardState.tasks || []).map((task) => [task.id, task]));
    for (const task of snap.tasks || []) merged.set(task.id, task);
    dashboardState.tasks = [...merged.values()].sort(
      (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
  } else {
    dashboardState.tasks = snap.tasks || [];
  }
  mergeLiveTaskHistory(snap.tasks || []);
  dashboardState.nodes = snap.nodes || [];
  if (snap.cluster_health) dashboardState.capacity = snap.cluster_health;
  const capacityByNode = new Map(
    (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node])
  );
  const nodeCount = snap.member_count ?? dashboardState.nodes.filter((node) => node.healthy).length;
  updateClusterTitle(nodeCount);
  if (statNodes) statNodes.textContent = `Nodes: ${nodeCount}`;
  let modelCount = 0;
  let activeLoad = 0;
  if (Array.isArray(dashboardState.capacity?.nodes)) {
    for (const node of dashboardState.capacity.nodes) {
      if (!node.healthy) continue;
      activeLoad += Number(node.active) || 0;
    }
  } else {
    for (const n of dashboardState.nodes) {
      if (!n.healthy) continue;
      activeLoad += n.active_tasks || 0;
    }
  }
  for (const n of dashboardState.nodes) {
    if (!n.healthy) continue;
    modelCount += (n.models || []).length;
  }
  if (statModels) statModels.textContent = `Models: ${modelCount}`;
  if (statLoad) {
    const dispatch = dashboardState.capacity?.global_dispatch;
    statLoad.textContent = dispatch ? `Active Load: ${activeLoad} · Dispatch ${dispatch.active_slots}/${dispatch.global_limit} · Queue ${dispatch.queue_depth}` : `Active Load: ${activeLoad}`;
  }
  if (statMemory && snap.memory) {
    statMemory.textContent = `Memory: ${snap.memory.total_experiences || 0} · ${Math.round((snap.memory.success_rate || 0) * 100)}%`;
  }
  if (statVersion && snap.build_version) statVersion.textContent = snap.build_version;
  const nodeState = dashboardState.nodes.map((node) => ({
    id: node.id,
    version: node.version,
    address: node.address,
    hardware: node.hardware,
    models: node.models,
    default_model: node.default_model,
    fast_score: node.fast_score,
    active_tasks: node.active_tasks,
    healthy: node.healthy,
    leader: node.leader,
    version_error: node.version_error,
    last_seen: node.healthy ? void 0 : node.last_seen,
    gpu_sample: node.gpu_history?.at(-1)?.timestamp,
    npu_sample: node.npu_history?.at(-1)?.timestamp,
    system_sample: node.system_history?.at(-1)?.timestamp
  }));
  const nodeSignature = JSON.stringify(nodeState);
  const overviewNodeState = dashboardState.nodes.map((node) => {
    const capacity = capacityByNode.get(node.id);
    return {
      id: node.id,
      address: node.address,
      healthy: node.healthy,
      leader: node.leader,
      active_tasks: node.active_tasks,
      capacity: capacity && {
        active: capacity.active,
        max: capacity.max,
        remaining: capacity.remaining,
        oversubscribed: capacity.oversubscribed,
        telemetry_state: capacity.telemetry_state
      },
      model_count: node.models?.length || 0,
      last_seen: node.healthy ? void 0 : node.last_seen,
      gpu_stats_available: node.hardware?.gpu_stats_available,
      gpu_utilization_percent: node.hardware?.gpu_utilization_percent,
      npu_stats_available: node.hardware?.npu_stats_available,
      npu_utilization_percent: node.hardware?.npu_utilization_percent,
      cpu_stats_available: node.hardware?.cpu_stats_available,
      cpu_utilization_percent: node.hardware?.cpu_utilization_percent,
      memory_stats_available: node.hardware?.memory_stats_available,
      memory_usage_percent: node.hardware?.memory_usage_percent,
      hardware_context: {
        os: node.hardware?.os,
        arch: node.hardware?.arch,
        cpu_cores: node.hardware?.cpu_cores,
        total_ram_mb: node.hardware?.total_ram_mb,
        score: node.hardware?.score,
        has_gpu: node.hardware?.has_gpu,
        gpu_vendor: node.hardware?.gpu_vendor,
        has_npu: node.hardware?.has_npu,
        npu_vendor: node.hardware?.npu_vendor
      },
      engine_context: (node.engines || []).map((engine) => ({
        name: engine.name,
        ready: engine.ready,
        inventory_state: engine.inventory_state,
        model_count: engine.model_count,
        last_checked_at: engine.last_checked_at,
        last_successful_at: engine.last_successful_at,
        last_error: engine.last_error
      })),
      model_context: (node.models || []).map((model) => ({
        name: model.name,
        tags: model.tags,
        engine: model.engine
      }))
    };
  });
  const taskState = dashboardState.tasks.map((task) => ({
    id: task.id,
    description: task.description,
    status: task.status,
    updated_at: task.updated_at,
    subtasks: (task.subtasks || []).map((item) => ({
      id: item.subtask?.id,
      type: item.subtask?.task_type,
      status: item.status,
      node_id: item.node_id,
      model: item.model
    }))
  }));
  const taskSignature = JSON.stringify(dashboardState.tasks.map((task) => [task.id, task.updated_at]));
  const activitySignature = JSON.stringify(taskState);
  const peerState = (snap.discovery || []).map((peer) => ({
    id: peer.id,
    role: peer.role,
    address: peer.http_addr,
    peer_addr: peer.peer_addr,
    last_seen: peer.last_seen,
    orchestrator_id: peer.orchestrator_id
  }));
  const peerSignature = JSON.stringify({ peers: peerState, nodes: nodeState.map((node) => ({
    id: node.id,
    address: node.address,
    healthy: node.healthy,
    last_seen: node.last_seen
  })), pairing: dashboardState.pairing.status });
  const discoveredNodeState = (snap.discovery || []).filter((peer) => peer.role === "node").map((peer) => ({ id: peer.id, address: peer.peer_addr || peer.http_addr, last_seen: peer.last_seen }));
  const overviewNodeSignature = JSON.stringify({ members: overviewNodeState, discovered: discoveredNodeState });
  let overviewChanged = false;
  const overviewParts = {
    activity: activitySignature !== window.lastActivitySignature,
    nodes: overviewNodeSignature !== window.lastOverviewNodeSignature,
    peers: peerSignature !== window.lastOverviewPeerSignature
  };
  if (overviewParts.activity || overviewParts.nodes || overviewParts.peers) {
    renderOverview(dashboardState.nodes, dashboardState.tasks, snap.discovery || [], overviewParts);
    window.lastActivitySignature = activitySignature;
    window.lastOverviewNodeSignature = overviewNodeSignature;
    window.lastOverviewPeerSignature = peerSignature;
    overviewChanged = true;
  }
  if (nodeSignature !== window.lastNodeSignature) {
    renderNodes(dashboardState.nodes);
    renderSetupNodes(dashboardState.nodes);
    window.lastNodeSignature = nodeSignature;
  }
  const routingSignature = JSON.stringify({
    nodes: nodeState.map((node) => ({ id: node.id, healthy: node.healthy, active_tasks: node.active_tasks })),
    tasks: taskState.map((task) => ({
      id: task.id,
      updated_at: task.updated_at,
      subtasks: task.subtasks
    }))
  });
  if (routingSignature !== window.lastRoutingSignature) {
    renderRouting(dashboardState.nodes, dashboardState.tasks);
    window.lastRoutingSignature = routingSignature;
  }
  if (peerSignature !== window.lastPeerSignature) {
    renderDiscovery(snap.discovery || []);
    window.lastPeerSignature = peerSignature;
  }
  if (taskSignature !== window.lastTaskSignature) {
    renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, taskHistoryPage.loading);
    window.lastTaskSignature = taskSignature;
  }
  if (overviewChanged) refreshOverviewTimes();
}
function refreshOverviewTimes() {
  overviewNodes?.querySelectorAll("[data-last-seen]").forEach((label) => {
    label.textContent = `Last seen ${formatRelativeTime(label.dataset.lastSeen)}`;
  });
}
setInterval(refreshOverviewTimes, 15e3);
submitBtn?.addEventListener("click", async () => {
  const description = taskInput.value.trim();
  if (!description) return;
  submitStatus.textContent = "Submitting...";
  try {
    const task = await fetchJSON("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        description,
        plan_mode: taskAutoPlan?.checked ? "auto" : "heuristic",
        require_review: taskRequireReview?.checked || false,
        require_approval: taskRequireApproval?.checked || false
      })
    });
    submitStatus.textContent = `Submitted as ${task.id}`;
    taskInput.value = "";
    refresh();
  } catch (err) {
    submitStatus.textContent = "Error: " + err.message;
  }
});
loadPairingStatus();
refreshPersistenceStatus();
setInterval(loadPairingStatus, 5e3);
setInterval(refreshPersistenceStatus, 3e4);
setInterval(() => {
  if (document.getElementById("cluster-tab")?.classList.contains("active")) {
    void refresh();
  }
}, 15e3);
loadClusterModels();
refresh();
checkSetupReadiness();
loadModelCatalog();
connectEvents();

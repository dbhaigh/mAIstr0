import { loadPairingStatus } from "./components/pairing-dialog.js";
import { copyText, renderTasks, taskText } from "./components/task-list.js";
import { fetchJSON, refreshPersistenceStatus, updateClusterTitle } from "./shared/api.js";
import { formatRelativeTime } from "./shared/format.js";
import { dashboardSnapshot } from "./shared/snapshot.js";
import {
  clusterWorkspaceTabs,
  connectionStatus,
  copyTasksBtn,
  dashboardState as rawDashboardState,
  overviewNodes,
  statLoad,
  statMemory,
  statModels,
  statNodes,
  statVersion,
  submitBtn,
  submitStatus,
  taskAutoPlan,
  taskHistoryPage as rawTaskHistoryPage,
  taskHistorySearch as rawTaskHistorySearch,
  taskHistoryStatus as rawTaskHistoryStatus,
  taskInput,
  taskRequireApproval,
  taskRequireReview,
} from "./shared/state.js";
import { dashboardViewModel } from "./shared/view-model.js";
import { checkSetupReadiness, loadModelCatalog } from "./views/cluster-setup.js";
import { renderOverview } from "./views/cluster.js";
import { refreshMemory } from "./views/memory.js";
import { renderNodes, renderSetupNodes } from "./views/nodes.js";
import { renderPipelineDashboard } from "./views/pipelines.js";
import { renderDiscovery, renderRouting } from "./views/routing.js";
import { refreshShardGroups } from "./views/shards.js";
import type {
  DashboardActionHandlers,
  DashboardSnapshot,
  DashboardState,
  DashboardTabID,
  DiscoveryAuthentication,
  NormalizedTaskHistoryPage,
  Task,
  TaskHistoryPageState,
  TaskHistoryResponse,
} from "./shared/types";

const dashboardState = rawDashboardState as DashboardState;
const taskHistoryPage = rawTaskHistoryPage as TaskHistoryPageState<Task>;
const taskHistorySearch = rawTaskHistorySearch as HTMLInputElement | null;
const taskHistoryStatus = rawTaskHistoryStatus as HTMLSelectElement | null;
const taskInputElement = taskInput as HTMLTextAreaElement | null;
const taskAutoPlanCheckbox = taskAutoPlan as HTMLInputElement | null;
const taskRequireReviewCheckbox = taskRequireReview as HTMLInputElement | null;
const taskRequireApprovalCheckbox = taskRequireApproval as HTMLInputElement | null;
let refreshInFlight = false;
let connectionGeneration = 0;
let syncGeneration = 0;
let pollingFallbackActive = false;

function clusterTabIsActive(): boolean {
  return Boolean(document.getElementById("cluster-tab")?.classList.contains("active"));
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error &&
      typeof error.message === "string") {
    return error.message;
  }
  return String(error);
}

function updateConnectionStatus(): void {
  if (!connectionStatus) return;
  const state = dashboardState.connection;
  const lastSync = state.lastSyncAt
    ? new Date(state.lastSyncAt).toLocaleTimeString()
    : "not yet";
  const mode = state.connected ? "Connected" : pollingFallbackActive ? "Polling fallback" : "Reconnecting";
  connectionStatus.dataset.connected = String(state.connected);
  connectionStatus.dataset.mode = state.connected ? "live" : pollingFallbackActive ? "polling" : "reconnecting";
  connectionStatus.textContent = `${mode} · Last sync ${lastSync}`;
}

const loadedDashboardPanels = new Set(["agent-tab"]);

export function resetDashboardSignatures(): void {
  dashboardState.renderSignatures = {};
}

export function markDashboardSynced(): void {
  syncGeneration += 1;
  dashboardState.connection.lastSyncAt = Date.now();
  updateConnectionStatus();
}

export async function refresh(): Promise<void> {
  if (!clusterTabIsActive() || refreshInFlight) return;
  refreshInFlight = true;
  const requestedConnectionGeneration = connectionGeneration;
  const requestedSyncGeneration = syncGeneration;
  try {
    const [nodes, peers, build, taskResponse] = await Promise.all([
      fetchJSON("/api/nodes/all"),
      fetchJSON("/api/discovery"),
      fetchJSON("/api/version"),
      fetchJSON("/api/tasks?page_size=50"),
    ]);
    if (requestedConnectionGeneration !== connectionGeneration ||
        requestedSyncGeneration !== syncGeneration) return;
    const tasks = normalizeHistoryPage(taskResponse).items;
    applySnapshot({ nodes, tasks, discovery: peers, build_version: build.version });
    markDashboardSynced();
  } catch (err) {
    console.error(err);
  } finally {
    refreshInFlight = false;
  }
}

function taskHistoryURL(cursor = ""): string {
  const params = new URLSearchParams({ page_size: "50" });
  const query = (taskHistorySearch?.value || "").trim();
  const status = taskHistoryStatus?.value || "all";
  if (query) params.set("q", query);
  if (status !== "all") params.set("status", status);
  if (cursor) params.set("cursor", cursor);
  return `/api/tasks?${params}`;
}

function normalizeHistoryPage(response: TaskHistoryResponse): NormalizedTaskHistoryPage {
  return Array.isArray(response)
    ? { items: response, next_cursor: "", has_more: false }
    : {
      items: Array.isArray(response?.items) ? response.items : [],
      next_cursor: response?.next_cursor || "",
      has_more: Boolean(response?.has_more),
    };
}

function sortTaskHistory(items: Task[]): Task[] {
  return items.sort((a, b) =>
    new Date(b.updated_at || b.created_at || 0).getTime() -
    new Date(a.updated_at || a.created_at || 0).getTime()
  );
}

export async function refreshTaskHistory(reset = false): Promise<void> {
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
      const updated = new Map(taskHistoryPage.items.map((task): [string, Task] => [task.id, task]));
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

export async function loadMoreTaskHistory(): Promise<void> {
  const filterKey = JSON.stringify([
    (taskHistorySearch?.value || "").trim(),
    taskHistoryStatus?.value || "all",
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
    const updated = new Map(taskHistoryPage.items.map((task): [string, Task] => [task.id, task]));
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

export async function copyAllTaskResults(): Promise<void> {
  try {
    const tasks: Task[] = [];
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
  } catch (error: unknown) {
    console.error("copying task history failed", error);
    if (submitStatus) submitStatus.textContent = `Unable to copy task history: ${errorMessage(error)}`;
  }
}

function mergeLiveTaskHistory(tasks: Task[]): void {
  if (!taskHistoryPage.initialized || !taskHistoryPage.items.length) return;
  const updates = new Map((tasks || []).map((task): [string, Task] => [task.id, task]));
  taskHistoryPage.items = sortTaskHistory(taskHistoryPage.items.map((task) => updates.get(task.id) || task));
}

export async function refreshDiscovery(): Promise<void> {
  try {
    const discovery: DashboardSnapshot["discovery"] = await fetchJSON("/api/discovery");
    let discoveryAuthentication: DiscoveryAuthentication = "unavailable";
    try {
      const status: { discovery_authentication?: string } = await fetchJSON("/api/security/status");
      if (status.discovery_authentication === "unauthenticated" ||
          status.discovery_authentication === "hmac-sha256") {
        discoveryAuthentication = status.discovery_authentication;
      } else {
        throw new Error("security status response omitted a recognized discovery authentication mode");
      }
    } catch (err) {
      console.error("local network security status refresh failed", err);
    }
    applySnapshot({
      nodes: dashboardState.nodes || [],
      tasks: dashboardState.tasks || [],
      discovery,
      discovery_authentication: discoveryAuthentication,
    });
  } catch (err) {
    console.error("local network refresh failed", err);
  }
}

setInterval(() => {
  if (loadedDashboardPanels.has("discovery-panel") &&
      document.getElementById("cluster-tab")?.classList.contains("active") &&
      document.getElementById("discovery-panel")?.classList.contains("active")) {
    void refreshDiscovery();
  }
}, 5000);

function applySnapshot(snap: DashboardSnapshot): void {
  const signatures = dashboardState.renderSignatures || (dashboardState.renderSignatures = {});
  const taskSnapshot = dashboardSnapshot.mergeTaskSnapshot(
    dashboardState.tasks, snap.tasks, snap.complete_tasks, dashboardState.fullTaskHistoryLoaded,
  );
  dashboardState.tasks = taskSnapshot.tasks;
  dashboardState.fullTaskHistoryLoaded = taskSnapshot.fullTaskHistoryLoaded;
  mergeLiveTaskHistory(snap.tasks || []);
  dashboardState.nodes = snap.nodes || [];
  if (Array.isArray(snap.discovery)) dashboardState.discovery = snap.discovery;
  if (typeof snap.discovery_authentication === "string") {
    dashboardState.discoveryAuthentication = snap.discovery_authentication;
  }
  if (snap.build_version) dashboardState.buildVersion = snap.build_version;
  if (Array.isArray(snap.model_inventory)) dashboardState.modelInventory = snap.model_inventory;
  if (Array.isArray(snap.pipeline_plans)) dashboardState.pipelinePlans = snap.pipeline_plans;
  if (snap.cluster_health) dashboardState.capacity = snap.cluster_health;
  const capacityByNode = new Map(
    (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node] as const),
  );
  const status = dashboardViewModel.dashboardStatusViewModel(snap, dashboardState.nodes, dashboardState.capacity);
  const statusSignature = JSON.stringify(status);
  if (statusSignature !== signatures.status) {
    updateClusterTitle(status.nodeCount);
    if (statNodes) statNodes.textContent = `Nodes: ${status.nodeCount}`;
    if (statModels) statModels.textContent = `Models: ${status.modelCount}`;
    if (statLoad) statLoad.textContent = status.loadLabel;
    if (statMemory && status.memoryLabel) statMemory.textContent = status.memoryLabel;
    if (statVersion && status.buildVersion) statVersion.textContent = status.buildVersion;
    signatures.status = statusSignature;
  }

  const nodeState = dashboardState.nodes.map((node) => ({
    id: node.id, version: node.version, address: node.address, hardware: node.hardware,
    models: node.models, default_model: node.default_model, fast_score: node.fast_score,
    active_tasks: node.active_tasks, healthy: node.healthy, leader: node.leader,
    version_error: node.version_error, last_seen: node.healthy ? undefined : node.last_seen,
    gpu_sample: node.gpu_history?.at(-1)?.timestamp,
    npu_sample: node.npu_history?.at(-1)?.timestamp,
    system_sample: node.system_history?.at(-1)?.timestamp,
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
        telemetry_state: capacity.telemetry_state,
      },
      model_count: node.models?.length || 0,
      last_seen: node.healthy ? undefined : node.last_seen,
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
        npu_vendor: node.hardware?.npu_vendor,
      },
      engine_context: (node.engines || []).map((engine) => ({
        name: engine.name,
        ready: engine.ready,
        inventory_state: engine.inventory_state,
        model_count: engine.model_count,
        last_checked_at: engine.last_checked_at,
        last_successful_at: engine.last_successful_at,
        last_error: engine.last_error,
      })),
      model_context: (node.models || []).map((model) => ({
        name: model.name,
        tags: model.tags,
        engine: model.engine,
      })),
    };
  });
  const taskState = dashboardState.tasks.map((task) => ({
    id: task.id, description: task.description, status: task.status, updated_at: task.updated_at,
    subtasks: (task.subtasks || []).map((item) => ({
      id: item.subtask?.id, type: item.subtask?.task_type, status: item.status, node_id: item.node_id,
      model: item.model,
    })),
  }));
  const taskSignature = JSON.stringify(dashboardState.tasks.map((task) => [task.id, task.updated_at]));
  const activitySignature = JSON.stringify(taskState);
  const peerState = (snap.discovery || []).map((peer) => ({
    id: peer.id, role: peer.role, address: peer.http_addr, peer_addr: peer.peer_addr, last_seen: peer.last_seen,
    orchestrator_id: peer.orchestrator_id,
  }));
  const peerSignature = JSON.stringify({ peers: peerState, discoveryAuthentication: dashboardState.discoveryAuthentication, nodes: nodeState.map((node) => ({
    id: node.id, address: node.address, healthy: node.healthy, last_seen: node.last_seen,
  })), pairing: dashboardState.pairing.status });
  const discoveredNodeState = (snap.discovery || [])
    .filter((peer) => peer.role === "node")
    .map((peer) => ({ id: peer.id, address: peer.peer_addr || peer.http_addr, last_seen: peer.last_seen }));
  const overviewNodeSignature = JSON.stringify({ members: overviewNodeState, discovered: discoveredNodeState });
  let overviewChanged = false;
  const overviewParts = {
    activity: activitySignature !== signatures.activity,
    nodes: overviewNodeSignature !== signatures.overviewNodes,
    peers: peerSignature !== signatures.overviewPeers,
  };
  if (loadedDashboardPanels.has("cluster-overview-panel") &&
      (overviewParts.activity || overviewParts.nodes || overviewParts.peers)) {
    renderOverview(dashboardState.nodes, dashboardState.tasks, snap.discovery || [], overviewParts);
    signatures.activity = activitySignature;
    signatures.overviewNodes = overviewNodeSignature;
    signatures.overviewPeers = peerSignature;
    overviewChanged = true;
  }
  if (loadedDashboardPanels.has("nodes-panel") && nodeSignature !== signatures.nodes) {
    renderNodes(dashboardState.nodes);
    signatures.nodes = nodeSignature;
  }
  if (loadedDashboardPanels.has("setup-nodes-panel") && nodeSignature !== signatures.setupNodes) {
    renderSetupNodes(dashboardState.nodes);
    signatures.setupNodes = nodeSignature;
  }
  const routingSignature = JSON.stringify({
    nodes: nodeState.map((node) => ({ id: node.id, healthy: node.healthy, active_tasks: node.active_tasks })),
    tasks: taskState.map((task) => ({
      id: task.id, updated_at: task.updated_at,
      subtasks: task.subtasks,
    })),
  });
  if (loadedDashboardPanels.has("routing-panel") && routingSignature !== signatures.routing) {
    renderRouting(dashboardState.nodes, dashboardState.tasks);
    signatures.routing = routingSignature;
  }
  const pipelineSignature = JSON.stringify({
    models: dashboardState.modelInventory,
    plans: dashboardState.pipelinePlans,
  });
  if (loadedDashboardPanels.has("routing-panel") && pipelineSignature !== signatures.pipeline) {
    renderPipelineDashboard(dashboardState.modelInventory, dashboardState.pipelinePlans);
    signatures.pipeline = pipelineSignature;
  }
  if (loadedDashboardPanels.has("discovery-panel") && peerSignature !== signatures.discovery) {
    renderDiscovery(snap.discovery || [], dashboardState.discoveryAuthentication);
    signatures.discovery = peerSignature;
  }
  if (loadedDashboardPanels.has("tasks-panel") && taskSignature !== signatures.tasks) {
    renderTasks(taskHistoryPage.items, taskHistoryPage.hasMore, taskHistoryPage.loading);
    signatures.tasks = taskSignature;
  }
  if (overviewChanged) refreshOverviewTimes();
}

export function activateDashboardTab(tabID: DashboardTabID): void {
  syncPollingFallback();
  if (tabID === "memory-tab") {
    loadedDashboardPanels.add(tabID);
    void refreshMemory();
    return;
  }
  if (tabID !== "cluster-tab") return;
  const selected = Array.from(clusterWorkspaceTabs)
    .find((tab) => tab.getAttribute("aria-selected") === "true");
  activateWorkspacePanel(selected?.getAttribute("aria-controls") || "cluster-overview-panel");
  void refresh();
}

export function activateWorkspacePanel(panelID: string): void {
  if (panelID.startsWith("overview-")) loadedDashboardPanels.add("cluster-overview-panel");
  if (panelID.startsWith("setup-")) loadedDashboardPanels.add("setup-panel");
  loadedDashboardPanels.add(panelID);
  applySnapshot({
    nodes: dashboardState.nodes,
    tasks: dashboardState.tasks,
    discovery: dashboardState.discovery,
    discovery_authentication: dashboardState.discoveryAuthentication,
    model_inventory: dashboardState.modelInventory,
    pipeline_plans: dashboardState.pipelinePlans,
    cluster_health: dashboardState.capacity,
    build_version: dashboardState.buildVersion,
  });
  if (panelID === "setup-readiness-panel") void checkSetupReadiness();
  if (panelID === "setup-catalog-panel") void loadModelCatalog();
  if (panelID === "tasks-panel") void refreshTaskHistory(true);
  if (panelID === "discovery-panel") void refreshDiscovery();
  if (panelID === "routing-panel") void refreshShardGroups();
}

function refreshOverviewTimes(): void {
  overviewNodes?.querySelectorAll<HTMLElement>("[data-last-seen]").forEach((label) => {
    label.textContent = `Last seen ${formatRelativeTime(label.dataset.lastSeen)}`;
  });
}

setInterval(refreshOverviewTimes, 15000);

function updateConnectionState(connected: boolean): void {
  if (dashboardState.connection.connected !== connected) connectionGeneration += 1;
  dashboardState.connection.connected = connected;
  if (connected) {
    pollingFallbackActive = false;
    updateConnectionStatus();
  } else {
    syncPollingFallback();
  }
}

function syncPollingFallback(): void {
  const shouldPoll = !dashboardState.connection.connected && clusterTabIsActive();
  const becameActive = shouldPoll && !pollingFallbackActive;
  pollingFallbackActive = shouldPoll;
  updateConnectionStatus();
  if (becameActive) void refresh();
}

async function submitTask(): Promise<void> {
  const description = taskInputElement?.value.trim() || "";
  if (!description) return;
  if (submitStatus) submitStatus.textContent = "Submitting...";
  try {
    const task: { id: string } = await fetchJSON("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        description,
        plan_mode: taskAutoPlanCheckbox?.checked ? "auto" : "heuristic",
        require_review: taskRequireReviewCheckbox?.checked || false,
        require_approval: taskRequireApprovalCheckbox?.checked || false,
      }),
    });
    if (submitStatus) submitStatus.textContent = `Submitted as ${task.id}`;
    if (taskInputElement) taskInputElement.value = "";
    void refresh();
  } catch (error: unknown) {
    if (submitStatus) submitStatus.textContent = `Error: ${errorMessage(error)}`;
  }
}

const dashboardActions: DashboardActionHandlers = {
  refresh,
  refreshTaskHistory,
  loadMoreTaskHistory,
  applySnapshot,
  connectionChange: updateConnectionState,
  activateDashboardTab,
  activateWorkspacePanel,
  submitTask,
};

function connectEvents() {
  dashboardSnapshot.connectDashboardEvents((snapshot: DashboardSnapshot) => {
    markDashboardSynced();
    applySnapshot(snapshot);
  }, {
    onConnectionChange: dashboardActions.connectionChange,
  });
  syncPollingFallback();
}

submitBtn?.addEventListener("click", dashboardActions.submitTask);

// Initialization
loadPairingStatus();
refreshPersistenceStatus();
setInterval(loadPairingStatus, 5000);
setInterval(refreshPersistenceStatus, 30000);
setInterval(() => {
  if (clusterTabIsActive()) {
    if (!dashboardState.connection.connected) void refresh();
    if (document.getElementById("routing-panel")?.classList.contains("active")) {
      void refreshShardGroups();
    }
  }
}, 15000);
connectEvents();

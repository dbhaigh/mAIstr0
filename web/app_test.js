const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const indexSource = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
const appScriptPaths = [...indexSource.matchAll(/<script\s+src="([^"]+)"\s*><\/script>/g)].map((match) => match[1]);
const appSources = new Map(appScriptPaths.map((scriptPath) => [
  path.basename(scriptPath),
  fs.readFileSync(path.join(__dirname, "static", path.basename(scriptPath)), "utf8"),
]));
const source = [...appSources.values()].join("\n");
const sharedSource = appSources.get("app-shared.js");
const clusterSource = appSources.get("app-cluster.js");
const discoverySource = source.slice(source.indexOf("function visibleNetworkPeers("), source.indexOf("function renderTasks("));
const taskSource = source.slice(source.indexOf("function renderTaskHistoryTable("), source.indexOf("async function copyText("));
const resourceHistorySource = source.slice(source.indexOf("function gpuGaugeMarkup("), source.indexOf("const TASK_ACTIVE_STATES"));
const engineStatusSource = clusterSource.slice(clusterSource.indexOf("function formatNodeHardware("), clusterSource.indexOf("function gpuGaugeMarkup("));
const sessionSource = source.slice(source.indexOf("async function loadSessions("), source.indexOf("function sessionsForHarness("));
const optionalFeaturesSource = source.slice(source.indexOf("let optionalFeaturesInitialized = false;"), source.indexOf("async function refreshPersistenceStatus()"));
const sendInteractiveStart = source.indexOf("async function sendInteractiveMessage(text) {");
const sendInteractiveEnd = source.indexOf("\nfunction handleStreamEvent(", sendInteractiveStart);
const sendInteractiveSource = source.slice(sendInteractiveStart, sendInteractiveEnd);

test("Dashboard loads shared utilities and each view without a build step", () => {
  assert.deepEqual(appScriptPaths, [
    "/app-shared.js",
    "/app-cluster.js",
    "/app-memory.js",
    "/app-orchestrator.js",
    "/app.js",
  ]);
  for (const scriptPath of appScriptPaths) {
    assert.ok(fs.existsSync(path.join(__dirname, "static", path.basename(scriptPath))), `${scriptPath} is missing`);
  }
});

test("Dashboard view scripts execute in HTML load order", () => {
  const element = {
    addEventListener() {},
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    dataset: {},
    style: {},
  };
  const context = vm.createContext({
    document: {
      getElementById: () => element,
      querySelector: () => element,
      querySelectorAll: () => [],
      addEventListener() {},
    },
  });
  for (const scriptPath of appScriptPaths.slice(0, -1)) {
    vm.runInContext(appSources.get(path.basename(scriptPath)), context, { filename: scriptPath });
  }
});

test("Node properties allow configuring llama.cpp", () => {
  const index = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  assert.match(index, /<option value="llamacpp">llama\.cpp<\/option>/);
});

test("Live node context is expandable from cluster overview node cards", () => {
  const index = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const overviewStart = index.indexOf('<section id="cluster-overview-panel"');
  const nodesPanelPosition = index.indexOf('<section id="nodes-panel"');
  const sidebarStart = index.indexOf('<aside class="agent-sidebar"');
  const sidebarEnd = index.indexOf("</aside>", sidebarStart);
  const sidebarMarkup = index.slice(sidebarStart, sidebarEnd);

  assert.ok(overviewStart >= 0 && nodesPanelPosition > overviewStart);
  assert.doesNotMatch(index, /direct-cluster-context|Live Cluster Context/);
  assert.doesNotMatch(sidebarMarkup, /id="direct-cluster-context"/);
  assert.ok(sidebarEnd > sidebarStart);
  assert.match(source, /<details class="overview-node-context"/);
  assert.match(source, /nodeContextDetailsMarkup\(node, expandedNodeIDs\.has\(node\.id\)\)/);
  assert.doesNotMatch(source, /refreshDirectClusterContext/);
  assert.match(source, /if \(agentSidebar\) agentSidebar\.hidden = direct;/);
});

test("Dashboard capacity display uses the shared cluster health view", () => {
  assert.match(source, /if \(snap\.cluster_health\) dashboardState\.capacity = snap\.cluster_health/);
  assert.match(source, /Dispatch \$\{dispatch\.active_slots\}\/\$\{dispatch\.global_limit\} · Queue \$\{dispatch\.queue_depth\}/);
  assert.match(source, /\$\{capacity\.remaining\}\/\$\{capacity\.max\} slots free/);
});

test("mAIstr0 is the default chat mode and does not create an agent session on startup", async () => {
  const defaultMode = source.match(/let activeChatMode = "([^"]+)";/);
  const index = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  assert.equal(defaultMode?.[1], "direct");
  assert.match(index, /class="agent-tab active"[^>]*data-agent="direct">mAIstr0<\/button>/);

  let selected = 0;
  let created = 0;
  const context = vm.createContext({
    activeChatMode: "direct",
    dashboardState: { sessions: [] },
    fetchJSON: async (url) => {
      assert.equal(url, "/api/agent/sessions");
      return [{ id: "existing-pi-session", harness: "pi" }];
    },
    renderSessionList: () => {},
    sessionsForHarness: () => assert.fail("direct mode should not select an agent session"),
    activeSessionIds: new Map(),
    selectSession: async () => { selected++; },
    createNewSession: async () => { created++; },
    console: { error: () => {} },
  });
  vm.runInContext(sessionSource, context);
  await context.loadSessions();

  assert.equal(context.dashboardState.sessions.length, 1);
  assert.equal(selected, 0);
  assert.equal(created, 0);
});

test("minimal mode hides harness and learning controls without loading optional APIs", () => {
  let optionalCalls = 0;
  const memoryTab = {
    hidden: false,
    classList: { contains: () => false },
  };
  const directTab = { dataset: { agent: "direct" }, hidden: false, disabled: false };
  const harnessTab = { dataset: { agent: "pi" }, hidden: false, disabled: false };
  const context = vm.createContext({
    agentTabs: [directTab, harnessTab],
    document: {
      querySelector: (selector) => selector.includes("memory-tab") ? memoryTab : null,
    },
    activeChatMode: "direct",
    activateDirectChat: () => assert.fail("direct mode should remain selected"),
    loadAgentTools: () => { optionalCalls++; },
    loadAgentBackend: () => { optionalCalls++; return Promise.resolve(); },
    loadSessions: () => { optionalCalls++; },
  });
  vm.runInContext(optionalFeaturesSource, context);
  context.initializeOptionalFeatures(false);

  assert.equal(optionalCalls, 0);
  assert.equal(directTab.hidden, false);
  assert.equal(harnessTab.hidden, true);
  assert.equal(harnessTab.disabled, true);
  assert.equal(memoryTab.hidden, true);
});

test("mAIstr0 sends questions and work through one task submission action", async () => {
  const index = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  assert.doesNotMatch(index, /dispatch-direct-task-btn/);
  assert.match(index, /id="send-msg-btn"[^>]*>Ask mAIstr0<\/button>/);

  const submitted = [];
  const context = vm.createContext({
    activeChatMode: "direct",
    dispatchDirectTask: async (description) => { submitted.push(description); },
  });
  vm.runInContext(sendInteractiveSource, context);
  await context.sendInteractiveMessage("Which node has the most available memory?");
  assert.deepEqual(submitted, ["Which node has the most available memory?"]);
});

test("Nodes page combines GPU, NPU, CPU, and RAM histories in one graph", () => {
  const context = vm.createContext({
    escapeHtml: (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
  });
  vm.runInContext(resourceHistorySource, context);
  const html = context.resourceHistoryMarkup({
    id: "worker-1",
    hardware: {
      gpu_stats_available: true, gpu_utilization_percent: 45, has_gpu: true,
      npu_stats_available: true, npu_utilization_percent: 25, has_npu: true,
      cpu_stats_available: true, cpu_utilization_percent: 35,
      memory_stats_available: true, memory_usage_percent: 65,
    },
    gpu_history: [
      { available: true, utilization_percent: 40 },
      { available: true, utilization_percent: 45 },
    ],
    npu_history: [
      { available: true, utilization_percent: 20 },
      { available: true, utilization_percent: 25 },
    ],
    system_history: [
      { cpu_available: true, cpu_utilization_percent: 30, memory_available: true, memory_usage_percent: 60 },
      { cpu_available: true, cpu_utilization_percent: 35, memory_available: true, memory_usage_percent: 65 },
    ],
  });

  assert.equal((html.match(/<svg /g) || []).length, 1);
  assert.match(html, /GPU, NPU, CPU and RAM utilization history for worker-1/);
  for (const metric of ["gpu", "npu", "cpu", "ram"]) {
    assert.match(html, new RegExp(`class="resource-history-line resource-history-${metric}"`));
    assert.match(html, new RegExp(`resource-history-swatch resource-history-${metric}`));
  }
});

test("Nodes page omits GPU and NPU details when the hardware is absent", () => {
  const context = vm.createContext({
    escapeHtml: (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
  });
  vm.runInContext(resourceHistorySource, context);
  const node = {
    id: "cpu-worker",
    hardware: {
      has_gpu: false, has_npu: false,
      cpu_stats_available: true, cpu_utilization_percent: 35,
      memory_stats_available: true, memory_usage_percent: 65,
    },
    gpu_history: [{ available: true, utilization_percent: 90 }],
    npu_history: [{ available: true, utilization_percent: 80 }],
    system_history: [
      { cpu_available: true, cpu_utilization_percent: 30, memory_available: true, memory_usage_percent: 60 },
      { cpu_available: true, cpu_utilization_percent: 35, memory_available: true, memory_usage_percent: 65 },
    ],
  };

  assert.equal(context.nodeAcceleratorMarkup(node).trim(), "");
  const history = context.resourceHistoryMarkup(node);
  assert.doesNotMatch(history, /GPU|NPU|none/);
  assert.match(history, /CPU and RAM utilization history for cpu-worker/);
  assert.match(history, /resource-history-swatch resource-history-cpu/);
  assert.match(history, /resource-history-swatch resource-history-ram/);
  assert.doesNotMatch(history, /resource-history-(gpu|npu)/);
});

test("Node engine inventory indicators distinguish available, empty, failed, and unchecked states", () => {
  const context = vm.createContext({
    escapeHtml: (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
  });
  vm.runInContext(engineStatusSource, context);

  const html = context.engineStatusSummaryMarkup([
    { name: "ollama", ready: true, inventory_state: "available", model_count: 2 },
    { name: "vllm", ready: true, inventory_state: "empty", model_count: 0 },
    { name: "lmstudio", ready: false, inventory_state: "error", model_count: 1, last_error: "<timeout>" },
    { name: "fastflowlm", ready: false, inventory_state: "unknown", model_count: 0 },
  ]);
  assert.match(html, /engine-status-chip available/);
  assert.match(html, /ollama<\/strong>\s*<span>Inventory available · 2 models/);
  assert.match(html, /engine-status-chip empty/);
  assert.match(html, /Reachable · no models · 0 models/);
  assert.match(html, /engine-status-chip error/);
  assert.match(html, /lmstudio: &lt;timeout&gt;/);
  assert.match(html, /1 known/);
  assert.match(html, /engine-status-chip unknown/);
  assert.match(html, /Not checked · 0 models/);

  const details = context.engineStatusDetailsMarkup([
    {
      name: "lmstudio", ready: false, inventory_state: "error", model_count: 1,
      last_checked_at: "2026-10-14T12:00:00Z", last_successful_at: "2026-10-14T11:00:00Z",
      last_error: "<timeout>",
    },
  ]);
  assert.match(details, /Last checked:/);
  assert.match(details, /Last successful inventory:/);
  assert.match(details, /Inventory check failed: &lt;timeout&gt;/);
  assert.doesNotMatch(details, /<timeout>/);
});

test("Expandable node context includes hardware, engine status, and model capabilities", () => {
  const context = vm.createContext({
    escapeHtml: (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
  });
  vm.runInContext(engineStatusSource, context);

  const html = context.nodeContextDetailsMarkup({
    active_tasks: 2,
    hardware: {
      os: "windows", arch: "amd64", cpu_cores: 8, total_ram_mb: 16384,
      has_gpu: true, gpu_vendor: "NVIDIA", gpu_stats_available: true, gpu_utilization_percent: 42,
      score: 87,
    },
    engines: [{ name: "ollama", ready: true, inventory_state: "available", model_count: 1 }],
    models: [{ name: "qwen", tags: ["chat", "<reasoning>"], engine: "ollama" }],
  }, false);

  assert.match(html, /<details class="overview-node-context">/);
  assert.match(html, /windows\/amd64 · 8 CPU cores · 16\.0 GB RAM · NVIDIA · 42% busy/);
  assert.match(html, /Active tasks: 2 · capability score 87\.0/);
  assert.match(html, /Inventory available/);
  assert.match(html, /qwen<\/strong>/);
  assert.match(html, /chat, &lt;reasoning&gt; · ollama/);
  assert.doesNotMatch(html, /<reasoning>/);
});

function discoveryContext(nodes = [], pairing = null) {
  const list = {
    innerHTML: "",
    cards: [],
    appendChild(card) { this.cards.push(card); },
  };
  const context = vm.createContext({
    discoveryList: list,
    dashboardState: { nodes, pairing: { status: pairing } },
    document: { createElement: () => ({ innerHTML: "", className: "" }) },
    escapeHtml: (value) => String(value),
    attachNodeContextMenu: () => {},
  });
  vm.runInContext(discoverySource, context);
  return { context, list };
}

test("Local Network merges discovered nonmembers with online members without duplicates", () => {
  const nodes = [
    { id: "local-worker", address: "https://local:7451", healthy: true },
    { id: "paired-worker", address: "https://paired:7451", healthy: true },
    { id: "offline-worker", address: "https://offline:7451", healthy: false },
  ];
  const peers = [
    { id: "paired-worker", role: "node", peer_addr: "https://paired:7451" },
    { id: "unpaired-worker", role: "node", peer_addr: "https://unpaired:7451" },
    { id: "other-cluster-worker", role: "node", orchestrator_id: "other-orchestrator" },
    { id: "orchestrator", role: "orchestrator" },
    { id: "candidate", role: "candidate" },
  ];
  const { context, list } = discoveryContext(nodes, { role: "orchestrator", orchestrator_id: "local-orchestrator" });
  context.renderDiscovery(peers);
  assert.equal(list.cards.length, 6);
  const html = list.cards.map((card) => card.innerHTML).join("\n");
  assert.match(html, /local-worker/);
  assert.doesNotMatch(html, /offline-worker/);
  assert.match(list.cards.find((card) => card.innerHTML.includes("paired-worker <")).innerHTML, /Visible \u00b7 cluster member/);
  assert.match(list.cards.find((card) => card.innerHTML.includes("unpaired-worker <")).innerHTML, /Visible \u00b7 not a member of this cluster/);
  assert.match(html, /other-orchestrator/);
});

test("Local Network shows the unjoined local worker when discovery excludes self", () => {
  const { context, list } = discoveryContext([], {
    role: "node", node_id: "self-worker", node_addr: "https://self:7451", joined: false,
    orchestrator_id: "old-orchestrator",
  });
  context.renderDiscovery([]);
  assert.equal(list.cards.length, 1);
  assert.match(list.cards[0].innerHTML, /self-worker/);
  assert.match(list.cards[0].innerHTML, /not a member of this cluster \u00b7 local node/);
  assert.doesNotMatch(list.cards[0].innerHTML, /old-orchestrator/);
});

test("A local member stays deduplicated and identified as local", () => {
  const { context, list } = discoveryContext([{ id: "self-worker", healthy: true }], {
    role: "node", node_id: "self-worker", node_addr: "https://self:7451", joined: true,
    orchestrator_id: "leader",
  });
  context.renderDiscovery([{ id: "self-worker", role: "node" }]);
  assert.equal(list.cards.length, 1);
  assert.match(list.cards[0].innerHTML, /cluster member \u00b7 local node/);
  assert.match(list.cards[0].innerHTML, /leader/);
});

test("Visible paired nodes are not presented as offline because registry health lags", () => {
  const { context, list } = discoveryContext([{ id: "worker", healthy: false }]);
  context.renderDiscovery([{ id: "worker", role: "node" }]);
  assert.match(list.cards[0].innerHTML, /Visible \u00b7 cluster member/);
  assert.doesNotMatch(list.cards[0].innerHTML, /Offline/);
});

test("Tasks are searchable and filterable in an expandable table", () => {
  const rows = [];
  const tables = [];
  const createElement = (tagName) => ({
    tagName,
    className: "",
    innerHTML: "",
    dataset: {},
    hidden: false,
    children: [],
    listeners: {},
    attributes: {},
    appendChild(child) {
      this.children.push(child);
      if (child.tagName === "tr") rows.push(child);
    },
    querySelector(selector) {
      if (selector === ".task-expand-btn") {
        this.expandButton ||= {
          listeners: {},
          attributes: {},
          addEventListener(type, listener) { this.listeners[type] = listener; },
          setAttribute(name, value) { this.attributes[name] = value; },
        };
        return this.expandButton;
      }
      return null;
    },
    querySelectorAll: () => [],
    addEventListener(type, listener) { this.listeners[type] = listener; },
  });
  const tasksList = {
    _innerHTML: "",
    set innerHTML(value) {
      this._innerHTML = value;
      if (value === "") {
        rows.length = 0;
        tables.length = 0;
      }
    },
    get innerHTML() { return this._innerHTML; },
    appendChild(table) { tables.push(table); },
    querySelectorAll(selector) {
      return selector === ".task-details-row:not([hidden])"
        ? rows.filter((row) => row.className === "task-details-row" && !row.hidden)
        : [];
    },
  };
  const taskHistorySearch = { value: "compiler", addEventListener() {} };
  const taskHistoryStatus = { value: "completed", addEventListener() {} };
  const taskHistoryCount = { textContent: "" };
  const context = vm.createContext({
    tasksList,
    taskHistorySearch,
    taskHistoryStatus,
    taskHistoryCount,
    document: {
      createElement,
    },
    escapeHtml: (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
    dashboardState: { tasks: [] },
    fetchJSON: async () => {},
    submitStatus: null,
    copyText: async () => {},
  });
  vm.runInContext(taskSource, context);

  context.renderTasks([
    {
      id: "task-2",
      description: "Review the <compiler> changes",
      status: "completed",
      created_at: "2026-10-13T10:00:00Z",
      updated_at: "2026-10-13T10:05:00Z",
      subtasks: [{
        subtask: { id: "review", description: "Check output", role: "reviewer", task_type: "analysis" },
        status: "completed",
        node_id: "node-a",
        model: "model-a",
        attempts: [{
          number: 1,
          started_at: "2026-10-13T10:01:00Z",
          finished_at: "2026-10-13T10:04:00Z",
        }],
        output: "review complete",
      }],
      result: "All good",
    },
    {
      id: "task-1",
      description: "Unrelated failed task",
      status: "failed",
      subtasks: [{
        subtask: { id: "compile", description: "Compile output", role: "coder", task_type: "code" },
        status: "failed",
        error: "result is not valid JSON",
      }],
    },
  ]);

  assert.equal(tables.length, 1);
  assert.match(tables[0].className, /task-history-table/);
  assert.match(tables[0].innerHTML, /<th scope="col">Task<\/th>/);
  assert.equal(rows.length, 2);
  assert.match(rows[0].innerHTML, /Review the &lt;compiler&gt; changes/);
  assert.match(rows[0].innerHTML, /aria-expanded="false"/);
  assert.match(rows[0].innerHTML, /1 \/ 1/);
  assert.match(rows[0].innerHTML, /<span class="badge completed">completed<\/span>/);
  assert.match(rows[0].innerHTML, /datetime="2026-10-13T10:00:00Z"/);
  assert.match(rows[0].innerHTML, /Created/);
  assert.match(rows[1].innerHTML, /Created/);
  assert.match(rows[1].innerHTML, /datetime="2026-10-13T10:01:00Z"/);
  assert.match(rows[1].innerHTML, /datetime="2026-10-13T10:04:00Z"/);
  assert.match(rows[1].innerHTML, /Started/);
  assert.match(rows[1].innerHTML, /Finished/);
  assert.match(rows[1].innerHTML, /review complete/);
  assert.equal(taskHistoryCount.textContent, "Showing 1 of 2 tasks");

  rows[0].expandButton.listeners.click();
  assert.equal(rows[1].hidden, false);
  assert.equal(rows[0].expandButton.attributes["aria-expanded"], "true");
  context.renderTasks([
    {
      id: "task-2",
      description: "Review the <compiler> changes",
      status: "completed",
      created_at: "2026-10-13T10:00:00Z",
      updated_at: "2026-10-13T10:05:00Z",
      subtasks: [{
        subtask: { id: "review", description: "Check output", role: "reviewer", task_type: "analysis" },
        status: "completed",
        node_id: "node-a",
        model: "model-a",
        output: "review complete",
      }],
      result: "All good",
    },
  ]);
  assert.equal(rows.length, 2);
  assert.equal(rows[1].hidden, false);
  assert.match(rows[1].innerHTML, /Not dispatched/);
  assert.match(rows[1].innerHTML, /datetime="2026-10-13T10:00:00Z"/);

  taskHistorySearch.value = "task-1";
  taskHistoryStatus.value = "all";
  context.renderTasks([
    {
      id: "task-2",
      description: "Review the <compiler> changes",
      status: "completed",
      subtasks: [],
    },
    {
      id: "task-1",
      description: "Unrelated failed task",
      status: "failed",
      subtasks: [{
        subtask: { id: "compile", description: "Compile output", role: "coder", task_type: "code" },
        status: "failed",
        error: "result is not valid JSON",
      }],
    },
  ]);
  assert.equal(rows.length, 2);
  assert.match(rows[1].innerHTML, /<strong>Failure reason:<\/strong> result is not valid JSON/);
  assert.match(rows[0].innerHTML, /<span class="badge failed">failed<\/span>/);
});

test("Task history loads cursor pages and restarts when filters change", async () => {
  const start = source.indexOf("function taskHistoryURL(");
  const end = source.indexOf("async function refreshDiscovery(", start);
  const pagingSource = source.slice(start, end);
  const responses = [
    { items: [{ id: "task-new", updated_at: "2026-01-03" }], next_cursor: "next-1", has_more: true },
    { items: [{ id: "task-old", updated_at: "2026-01-02" }], next_cursor: "", has_more: false },
    { items: [{ id: "filtered-task", updated_at: "2026-01-01" }], next_cursor: "", has_more: false },
  ];
  const requests = [];
  const pageState = {
    items: [], nextCursor: "", hasMore: false, loading: false,
    generation: 0, initialized: false, filterKey: "",
  };
  const context = vm.createContext({
    taskHistorySearch: { value: "compiler" },
    taskHistoryStatus: { value: "completed" },
    taskHistoryPage: pageState,
    URLSearchParams,
    fetchJSON: async (url) => { requests.push(url); return responses.shift(); },
    renderTasks: () => {},
    console: { error: () => {} },
  });
  vm.runInContext(pagingSource, context);

  await context.refreshTaskHistory(true);
  await context.loadMoreTaskHistory();
  assert.match(requests[0], /page_size=50/);
  assert.match(requests[0], /q=compiler/);
  assert.match(requests[0], /status=completed/);
  assert.match(requests[1], /cursor=next-1/);
  assert.deepEqual(Array.from(pageState.items, (task) => task.id), ["task-new", "task-old"]);
  assert.equal(pageState.hasMore, false);

  context.taskHistorySearch.value = "different";
  await context.refreshTaskHistory(true);
  assert.doesNotMatch(requests[2], /cursor=/);
  assert.match(requests[2], /q=different/);
  assert.deepEqual(Array.from(pageState.items, (task) => task.id), ["filtered-task"]);
});

test("Memory history requests cursor pages for the load-more controls", async () => {
  const start = source.indexOf("async function fetchMemoryHistoryPage(");
  const end = source.indexOf("function renderLearning(", start);
  const pagingSource = source.slice(start, end);
  const responses = [
    { items: [{ id: 1 }], next_cursor: "memory-next", has_more: true },
    { items: [{ id: 2 }], next_cursor: "", has_more: false },
  ];
  const requests = [];
  const context = vm.createContext({
    memoryHistoryPages: {
      experiences: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
    },
    fetchJSON: async (url) => { requests.push(url); return responses.shift(); },
    document: { createElement: () => ({ addEventListener() {} }) },
    renderFacts: () => {},
    renderExperiences: () => {},
    renderTranscripts: () => {},
    console: { error: () => {} },
  });
  vm.runInContext(pagingSource, context);

  await context.fetchMemoryHistoryPage("experiences", "/api/memory/experiences?q=cache", true);
  await context.fetchMemoryHistoryPage("experiences", "/api/memory/experiences?q=cache");
  assert.equal(requests[0], "/api/memory/experiences?q=cache&page_size=30");
  assert.match(requests[1], /cursor=memory-next/);
  assert.deepEqual(
    Array.from(context.memoryHistoryPages.experiences.items, (item) => item.id),
    [1, 2],
  );
  assert.equal(context.memoryHistoryPages.experiences.hasMore, false);
});

test("Overview recent activity uses the shared expandable task history table", () => {
  assert.match(source, /renderTaskHistoryTable\(overviewActivity, overviewTasks, "overview-task", true\)/);
  assert.doesNotMatch(source, /activity-card/);
});

test("An unknown association is not guessed from the first discovered orchestrator", () => {
  const { context, list } = discoveryContext();
  context.renderDiscovery([
    { id: "worker", role: "node" },
    { id: "arbitrary-orchestrator", role: "orchestrator" },
  ]);
  const worker = list.cards.find((card) => card.innerHTML.includes("worker <")).innerHTML;
  assert.match(worker, /Not announced/);
  assert.doesNotMatch(worker, /arbitrary-orchestrator/);
});

test("Persistence warning is displayed only when the API reports degraded storage", async () => {
  const sourceBlock = sharedSource.slice(sharedSource.indexOf("async function refreshPersistenceStatus("));
  const persistenceWarning = { hidden: true, textContent: "" };
  let persistence = { available: true };
  let mode = "full";
  const initializedModes = [];
  const context = vm.createContext({
    persistenceWarning,
    fetchJSON: async () => ({ persistence, mode }),
    initializeOptionalFeatures: (enabled) => initializedModes.push(enabled),
    console: { error: () => {} },
  });
  vm.runInContext(sourceBlock, context);

  await context.refreshPersistenceStatus();
  assert.equal(persistenceWarning.hidden, true);
  assert.deepEqual(initializedModes, [true]);

  persistence = { available: false, warning: "Memory will not survive restart." };
  await context.refreshPersistenceStatus();
  assert.equal(persistenceWarning.hidden, false);
  assert.equal(persistenceWarning.textContent, "Memory will not survive restart.");

  mode = "minimal";
  persistence = { available: false };
  await context.refreshPersistenceStatus();
  assert.equal(persistenceWarning.hidden, true);
  assert.deepEqual(initializedModes, [true, true, false]);
});

test("An open yielded dashboard navigates to the local worker before using pairing controls", async () => {
  const pairingSource = source.slice(source.indexOf("async function loadPairingStatus("), source.indexOf("async function activateAgentTab("));
  let destination;
  const context = vm.createContext({
    fetchJSON: async () => ({ role: "node", dashboard_url: "http://localhost:7451" }),
    window: { location: { replace: (url) => { destination = url; } } },
  });
  vm.runInContext(pairingSource, context);
  await context.loadPairingStatus();
  assert.equal(destination, "http://localhost:7451");
});

test("Same-subnet auto-join suppresses the automatic pairing dialog", async () => {
  const pairingSource = source.slice(source.indexOf("async function loadPairingStatus("), source.indexOf("async function activateAgentTab("));
  async function pairingContext(autoJoinAvailable, promptAlreadyOpen = false) {
    let dialogsShown = 0;
    let dialogsClosed = 0;
    const context = vm.createContext({
      fetchJSON: async (path) => path === "/api/pairing/status"
        ? { role: "node", joined: false, auto_join_available: autoJoinAvailable, orchestrator_addr: "https://192.168.1.10:7450" }
        : { pending: false },
      dashboardState: {
        nodes: [],
        tasks: [],
        sessions: [],
        pairing: {
          status: null,
          targetNode: null,
          setupFocused: true,
          promptShown: promptAlreadyOpen,
          dialogAutoOpened: promptAlreadyOpen,
        },
      },
      pairingPinView: {},
      pairingNodeConnect: {},
      pairingConnectOpenBtn: {},
      pairingInstructions: {},
      pairingAddress: { value: "" },
      pairingState: {},
      pairingConnectDialog: { close() { dialogsClosed++; } },
      pairingInvitationDialog: { open: false, close() {}, addEventListener() {} },
      pairingInvitationAcceptBtn: { addEventListener() {} },
      pairingInvitationDeclineBtn: { addEventListener() {} },
      openPairingDialog: () => { dialogsShown++; },
      document: { querySelector: () => null },
    });
    vm.runInContext(pairingSource, context);
    await context.loadPairingStatus();
    return { context, dialogsShown, dialogsClosed };
  }

  const automatic = await pairingContext(true);
  assert.equal(automatic.dialogsShown, 0);
  assert.match(automatic.context.pairingState.textContent, /Connecting automatically/);

  const automaticAfterPrompt = await pairingContext(true, true);
  assert.equal(automaticAfterPrompt.dialogsClosed, 1);

  const manual = await pairingContext(false);
  assert.equal(manual.dialogsShown, 1);
});

test("Pairing the local worker uses its join form, not an invitation to itself", () => {
  const dialogSource = source.slice(source.indexOf("function openPairingDialog("), source.indexOf("pairingPinRegenerateBtn?.addEventListener("));
  let shown = false;
  const context = vm.createContext({
    dashboardState: {
      pairing: {
        status: { role: "node", node_id: "self-worker", joined: false, orchestrator_addr: "https://leader:7450" },
        targetNode: null,
      },
    },
    pairingConnectDialog: { open: false, showModal: () => { shown = true; } },
    pairingStatus: {},
    pairingAddressField: {},
    pairingAddress: { value: "", focus: () => {} },
    pairingCode: {},
    pairingConnectButton: {},
    document: { getElementById: () => ({}) },
  });
  vm.runInContext(dialogSource, context);
  context.openPairingDialog({ id: "self-worker", discovered: true });
  assert.equal(shown, true);
  assert.equal(context.dashboardState.pairing.targetNode, null);
  assert.equal(context.pairingConnectButton.textContent, "Join cluster");
  assert.equal(context.pairingAddress.value, "https://leader:7450");
  assert.equal(context.pairingAddressField.hidden, false);
});

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(path.join(__dirname, "static", "app.js"), "utf8");
const discoverySource = source.slice(source.indexOf("function visibleNetworkPeers("), source.indexOf("function renderTasks("));
const taskSource = source.slice(source.indexOf("function renderTasks("), source.indexOf("async function copyText("));
const resourceHistorySource = source.slice(source.indexOf("function gpuGaugeMarkup("), source.indexOf("const TASK_ACTIVE_STATES"));

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

function discoveryContext(nodes = [], pairing = null) {
  const list = {
    innerHTML: "",
    cards: [],
    appendChild(card) { this.cards.push(card); },
  };
  const context = vm.createContext({
    discoveryList: list,
    currentNodes: nodes,
    currentPairingStatus: pairing,
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
    window: { latestTasks: [] },
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
  assert.match(rows[1].innerHTML, /Created/);
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
  const sourceBlock = source.slice(source.indexOf("async function refreshPersistenceStatus("), source.indexOf("function updateClusterTitle("));
  const persistenceWarning = { hidden: true, textContent: "" };
  let persistence = { available: true };
  const context = vm.createContext({
    persistenceWarning,
    fetchJSON: async () => ({ persistence }),
    console: { error: () => {} },
  });
  vm.runInContext(sourceBlock, context);

  await context.refreshPersistenceStatus();
  assert.equal(persistenceWarning.hidden, true);

  persistence = { available: false, warning: "Memory will not survive restart." };
  await context.refreshPersistenceStatus();
  assert.equal(persistenceWarning.hidden, false);
  assert.equal(persistenceWarning.textContent, "Memory will not survive restart.");
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
      currentPairingStatus: null,
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
      pairingSetupFocused: true,
      pairingPromptShown: promptAlreadyOpen,
      pairingDialogAutoOpened: promptAlreadyOpen,
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
    currentPairingStatus: { role: "node", node_id: "self-worker", joined: false, orchestrator_addr: "https://leader:7450" },
    pairingConnectDialog: { open: false, showModal: () => { shown = true; } },
    pairingTargetNode: null,
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
  assert.equal(context.pairingTargetNode, null);
  assert.equal(context.pairingConnectButton.textContent, "Join cluster");
  assert.equal(context.pairingAddress.value, "https://leader:7450");
  assert.equal(context.pairingAddressField.hidden, false);
});

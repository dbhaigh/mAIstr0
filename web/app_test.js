import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { dashboardDOM } from "./src/shared/dom.js";
import { dashboardFormat } from "./src/shared/format.js";
import { dashboardSnapshot } from "./src/shared/snapshot.js";
import { dashboardViewModel } from "./src/shared/view-model.js";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const stripModuleSyntax = (source) =>
  source
    .replace(/^\s*import\s*\{[\s\S]*?\}\s*from\s*["'][^"']+["'];?\s*/gm, "")
    .replace(/^\s*import\s*["'][^"']+["'];?\s*/gm, "")
    .replace(/^\s*export\s*\{[^}]*\};?\s*$/gm, "")
    .replace(/^export\s+/gm, "");
const readModuleSource = (...parts) =>
  stripModuleSyntax(fs.readFileSync(path.join(__dirname, ...parts), "utf8"));
const writableStateSetters = [
  ["modelCatalogEntries", "setModelCatalogEntries"],
  ["catalogEngines", "setCatalogEngines"],
  ["catalogModels", "setCatalogModels"],
  ["activeSessionId", "setActiveSessionId"],
  ["isSending", "setIsSending"],
  ["activeAbortController", "setActiveAbortController"],
  ["terminalAbortController", "setTerminalAbortController"],
  ["streamingAssistantEl", "setStreamingAssistantEl"],
  ["currentAgentHarness", "setCurrentAgentHarness"],
  ["agentSupportsCoordinatorModel", "setAgentSupportsCoordinatorModel"],
  ["activeChatMode", "setActiveChatMode"],
  ["directMessages", "setDirectMessages"],
  ["contextNode", "setContextNode"],
  ["terminalNode", "setTerminalNode"],
  ["terminalDialogueId", "setTerminalDialogueId"],
];
const createTestContext = (sandbox) => {
  const context = vm.createContext(sandbox);
  for (const [name, setter] of writableStateSetters) {
    context[setter] ??= (value) => { context[name] = value; };
  }
  context.resetDashboardSignatures ??= () => {};
  return context;
};

const indexSource = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
const appScriptPaths = [...indexSource.matchAll(/<script\s+src="([^"]+)"\s*><\/script>/g)].map((match) => match[1]);
const bundlePath = path.join(__dirname, "static", path.basename(appScriptPaths[0]));
const bundleSource = fs.readFileSync(bundlePath, "utf8");
const entrySource = fs.readFileSync(path.join(__dirname, "src", "entry.js"), "utf8");
const moduleSources = [...entrySource.matchAll(/^import\s+"([^"]+)";$/gm)]
  .map((match) => readModuleSource("src", match[1]));
const source = moduleSources.join("\n");
const pipelineViewSource = readModuleSource("src", "views", "pipelines.js");
const shardViewSource = readModuleSource("src", "views", "shards.js");
const shardSetupSource = readModuleSource("src", "views", "shard-setup.js");
const statusQuestionSource = readModuleSource("src", "views", "status-questions.js");
const directChatSource = readModuleSource("src", "views", "orchestrator-chat.js");
const directDispatchSource = directChatSource.slice(directChatSource.indexOf("function formatDirectTaskDispatch("), directChatSource.indexOf("async function sendInteractiveMessage("));
const sharedSource = readModuleSource("src", "shared", "api.js");
const applySnapshotStart = source.indexOf("function applySnapshot(snap) {");
const applySnapshotSource = source.slice(applySnapshotStart, source.indexOf("\nfunction refreshOverviewTimes(", applySnapshotStart));
const discoverySource = source.slice(source.indexOf("function visibleNetworkPeers("), source.indexOf("function renderTasks("));
const taskSourceStart = source.lastIndexOf("function renderTaskHistoryTable(");
const taskSource = source.slice(taskSourceStart, source.indexOf("async function copyText(", taskSourceStart));
const resourceHistorySource = source.slice(source.indexOf("function gpuGaugeMarkup("), source.indexOf("const TASK_ACTIVE_STATES"));
const engineStatusSource = readModuleSource("src", "components", "node-card.js");
const sessionSource = source.slice(source.indexOf("async function loadSessions("), source.indexOf("function sessionsForHarness("));
const optionalFeaturesSource = source.slice(source.indexOf("let optionalFeaturesInitialized = false;"), source.indexOf("async function refreshPersistenceStatus()"));
const sendInteractiveStart = source.indexOf("async function sendInteractiveMessage(text) {");
const sendInteractiveEnd = source.indexOf("\nfunction handleStreamEvent(", sendInteractiveStart);
const sendInteractiveSource = source.slice(sendInteractiveStart, sendInteractiveEnd);
test("Hardware summaries distinguish unavailable utilization from measured zero", () => {
  const hardware = { has_gpu: true, has_npu: true, gpu_stats_available: true, npu_stats_available: true };
  assert.doesNotMatch(dashboardViewModel.formatNodeHardware({ hardware }), /busy|null|undefined/);
  hardware.gpu_utilization_percent = null;
  hardware.npu_utilization_percent = null;
  assert.doesNotMatch(dashboardViewModel.formatNodeHardware({ hardware }), /busy|null/);
  hardware.gpu_utilization_percent = 0;
  hardware.npu_utilization_percent = 0;
  assert.match(dashboardViewModel.formatNodeHardware({ hardware }), /0% busy/);
});

test("Dashboard loads its generated bundle through the Go embedded static path", () => {
  assert.equal(appScriptPaths.length, 1);
  assert.match(appScriptPaths[0], /^\/app\.[a-f0-9]{12}\.js$/);
  assert.match(indexSource, /href="\/style\.[a-f0-9]{12}\.css"/);
  assert.match(indexSource, /id="connection-status"[^>]*role="status"[^>]*aria-live="polite"/);
  assert.match(indexSource, /id="pipeline-model-list"/);
  assert.match(indexSource, /id="pipeline-action-status"/);
  assert.match(indexSource, /id="shard-group-list"/);
  assert.match(indexSource, /id="shard-group-status"/);
  for (const scriptPath of appScriptPaths) {
    assert.ok(fs.existsSync(path.join(__dirname, "static", path.basename(scriptPath))), `${scriptPath} is missing`);
    assert.ok(fs.existsSync(path.join(__dirname, "static", `${path.basename(scriptPath)}.br`)));
    assert.ok(!fs.existsSync(path.join(__dirname, "static", `${path.basename(scriptPath)}.gz`)));
  }
  const stylePath = indexSource.match(/href="(\/style\.[a-f0-9]{12}\.css)"/)?.[1];
  assert.ok(stylePath);
  assert.ok(fs.existsSync(path.join(__dirname, "static", `${path.basename(stylePath)}.br`)));
  assert.ok(!fs.existsSync(path.join(__dirname, "static", `${path.basename(stylePath)}.gz`)));
  assert.ok(!fs.existsSync(path.join(__dirname, "static", "app.js")));
  assert.ok(!fs.existsSync(path.join(__dirname, "static", "style.css")));
  const embedSource = fs.readFileSync(path.join(__dirname, "embed.go"), "utf8");
  assert.match(embedSource, /\/\/go:embed static/);
});

test("Dashboard bundle parses as a plain JavaScript browser script", () => {
  assert.doesNotThrow(() => new vm.Script(bundleSource, { filename: bundlePath }));
});

test("Dashboard compatibility aliases do not duplicate generated assets", () => {
  for (const alias of ["app.js", "style.css"]) {
    assert.ok(!fs.existsSync(path.join(__dirname, "static", alias)), `${alias} should resolve in the handler`);
  }
});

test("Dashboard hashed assets and compressed page match their embedded contents", () => {
  const crypto = require("node:crypto");
  const zlib = require("node:zlib");
  const stylePath = indexSource.match(/href="(\/style\.[a-f0-9]{12}\.css)"/)[1];
  for (const assetPath of [appScriptPaths[0], stylePath, "/index.html"]) {
    const filename = path.join(__dirname, "static", path.basename(assetPath));
    const contents = fs.readFileSync(filename);
    const hash = crypto.createHash("sha256").update(contents).digest("hex").slice(0, 12);
    if (assetPath !== "/index.html") assert.ok(assetPath.includes(`.${hash}.`), assetPath);
    assert.deepEqual(zlib.brotliDecompressSync(fs.readFileSync(`${filename}.br`)), contents);
    assert.ok(!fs.existsSync(`${filename}.gz`));
  }
  assert.deepEqual(
    fs.readFileSync(path.join(__dirname, "static", path.basename(stylePath))),
    Buffer.from(fs.readFileSync(path.join(__dirname, "src", "style.css"), "utf8").replace(/\r\n/g, "\n")),
  );
});

test("Shard dashboard renders group state, members, and escaped rationale", () => {
  const elements = {
    "shard-group-list": { innerHTML: "", addEventListener() {} },
    "shard-group-status": { textContent: "" },
    "refresh-shard-groups": { addEventListener() {} },
  };
  const context = vm.createContext({
    document: { getElementById: (id) => elements[id] || null },
    dashboardState: { shardGroups: [], shardGroupsError: "" },
    escapeHtml: dashboardFormat.escapeHtml,
  });
  vm.runInContext(shardViewSource, context);
  const groups = [{
    id: "shard-1",
    model: { name: "model<&>" },
    mode: "llamacpp-rpc",
    state: "ready",
    members: [{
      role: "head",
      node_id: "node<&>",
      engine_id: "llama-head",
      engine_kind: "llama-server",
      started: true,
    }],
    plan: { rationale: ["GPU <memory> fit"] },
  }];
  context.renderShardGroups(groups);

  assert.match(elements["shard-group-list"].innerHTML, /model&lt;&amp;&gt;/);
  assert.match(elements["shard-group-list"].innerHTML, /node&lt;&amp;&gt;/);
  assert.match(elements["shard-group-list"].innerHTML, /GPU &lt;memory&gt; fit/);
  assert.match(elements["shard-group-list"].innerHTML, /badge healthy/);
  assert.equal(elements["shard-group-status"].textContent, "1 shard group");
  groups[0].state = "degraded";
  context.renderShardGroups(groups);
  assert.match(elements["shard-group-list"].innerHTML, /started, group not ready/);
  assert.doesNotMatch(elements["shard-group-list"].innerHTML, /badge healthy/);
});

test("Failed shard groups can be deleted without deleting ready groups or hiding errors", async () => {
  const handlers = {};
  const elements = {
    "shard-group-list": { innerHTML: "", addEventListener(event, handler) { handlers[event] = handler; } },
    "shard-group-status": { textContent: "" },
    "refresh-shard-groups": { addEventListener() {} },
  };
  const state = {
    shardGroups: [
      { id: "failed/<group>", model: { name: "large" }, state: "failed" },
      { id: "ready", model: { name: "small" }, state: "ready" },
    ],
    shardGroupsError: "",
  };
  const requests = [];
  let finish;
  const context = vm.createContext({
    document: { getElementById: (id) => elements[id] || null },
    dashboardState: state,
    escapeHtml: dashboardFormat.escapeHtml,
    console: { error() {} },
    fetchJSON: (url, options) => {
      requests.push({ url, options });
      return new Promise((resolve, reject) => { finish = { resolve, reject }; });
    },
  });
  vm.runInContext(shardViewSource, context);
  context.renderShardGroups();
  assert.match(elements["shard-group-list"].innerHTML, /data-delete-shard-group="failed\/&lt;group&gt;"/);
  assert.doesNotMatch(elements["shard-group-list"].innerHTML, /data-delete-shard-group="ready"/);
  assert.equal(typeof handlers.click, "function");
  const failed = context.deleteFailedShardGroup("failed/<group>");
  assert.match(elements["shard-group-list"].innerHTML, /disabled>Deleting/);
  await context.deleteFailedShardGroup("failed/<group>");
  await context.deleteFailedShardGroup("ready");
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/api/shards/failed%2F%3Cgroup%3E");
  assert.equal(requests[0].options.method, "DELETE");
  finish.reject(new Error("worker could not be stopped"));
  await failed;
  assert.equal(state.shardGroups.length, 2);
  assert.match(elements["shard-group-status"].textContent, /worker could not be stopped/);
  const success = context.deleteFailedShardGroup("failed/<group>");
  finish.resolve(null);
  await success;
  assert.equal(state.shardGroups.length, 1);
  assert.equal(state.shardGroups[0].id, "ready");
  assert.equal(state.shardGroupsError, "");
});

test("Shard member editor retains the head and applies selected worker membership", async () => {
  const head = { node_id: "head", role: "head" };
  const state = { nodes: [{ id: "head", healthy: true }, { id: "new-worker", healthy: true }],
    shardGroups: [{ id: "g/<id>", state: "ready", mode: "llamacpp-rpc", members: [head], pinned_nodes: ["head"] }], shardGroupsError: "" };
  const editor = { dataset: { shardMembersEditor: "g/<id>" },
    querySelectorAll: () => ["head", "new-worker"].map((id) => ({ dataset: { shardMember: id } })) };
  const elements = {
    "shard-group-list": { innerHTML: "", addEventListener() {}, querySelectorAll: () => [editor] },
    "shard-group-status": { textContent: "" },
  };
  const requests = [];
  const context = vm.createContext({ document: { getElementById: (id) => elements[id] || null },
    dashboardState: state, escapeHtml: dashboardFormat.escapeHtml, console,
    fetchJSON: async (url, options) => {
      requests.push({ url, options });
      if (url.endsWith("/engines/managed")) return url.includes("/head/") ? [{ kind: "GGML-RPC-SERVER" }] : [];
      if (url.endsWith("/install-worker")) return { engines: [] };
      return { ...state.shardGroups[0], pinned_nodes: ["head", "new-worker"] };
    } });
  vm.runInContext(shardViewSource, context);
  const markup = context.shardMemberEditorMarkup(state.shardGroups[0], false);
  assert.match(markup, /data-shard-member="head" checked disabled/);
  assert.match(markup, /data-shard-member="new-worker"/);
  assert.match(markup, /drains inference and reloads/);
  await context.applyShardMembers("g/<id>");
  const mutation = requests.find((entry) => entry.url.endsWith("/members"));
  assert.equal(mutation.url, "/api/shards/g%2F%3Cid%3E/members");
  assert.deepEqual(JSON.parse(mutation.options.body), { node_ids: ["head", "new-worker"] });
  assert.equal(requests.filter((entry) => entry.url.endsWith("/install-worker")).length, 1);
  assert.equal(state.shardGroupsError, "");
});

test("Shard lifecycle controls follow state, block duplicate actions, and preserve API errors", async () => {
  const elements = {
    "shard-group-list": { innerHTML: "", addEventListener() {} },
    "shard-group-status": { textContent: "" },
    "refresh-shard-groups": { addEventListener() {} },
  };
  const state = {
    shardGroups: [{ id: "group/<id>", state: "ready", model: { name: "model" } }],
    shardGroupsError: "",
  };
  const requests = [];
  let finish;
  let remoteState = "ready";
  const context = vm.createContext({
    document: { getElementById: (id) => elements[id] || null },
    dashboardState: state,
    escapeHtml: dashboardFormat.escapeHtml,
    console: { error() {} },
    fetchJSON: (url, options) => {
      requests.push({ url, options });
      if (!options) return Promise.resolve({ groups: [{ ...state.shardGroups[0], state: remoteState }] });
      return new Promise((resolve, reject) => { finish = { resolve, reject }; });
    },
  });
  vm.runInContext(shardViewSource, context);
  context.renderShardGroups();
  assert.match(elements["shard-group-list"].innerHTML, /data-shard-action="stop"/);
  assert.match(elements["shard-group-list"].innerHTML, /data-shard-action="restart"/);
  assert.doesNotMatch(elements["shard-group-list"].innerHTML, /data-shard-action="start"/);
  const stop = context.runShardGroupAction("group/<id>", "stop");
  assert.match(elements["shard-group-list"].innerHTML, /disabled>Stop/);
  await context.runShardGroupAction("group/<id>", "restart");
  await context.deleteFailedShardGroup("group/<id>");
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/api/shards/group%2F%3Cid%3E/stop");
  assert.equal(requests[0].options.method, "POST");
  finish.resolve({ ...state.shardGroups[0], state: "stopped" });
  await stop;
  assert.match(elements["shard-group-list"].innerHTML, /data-shard-action="start"/);
  assert.doesNotMatch(elements["shard-group-list"].innerHTML, /data-shard-action="stop"/);
  const start = context.runShardGroupAction("group/<id>", "start");
  remoteState = "failed";
  finish.reject(new Error("head startup timed out"));
  await start;
  assert.equal(state.shardGroups[0].state, "failed");
  assert.match(elements["shard-group-status"].textContent, /head startup timed out/);
  assert.match(elements["shard-group-list"].innerHTML, /Delete failed group/);
  const restart = context.runShardGroupAction("group/<id>", "restart");
  finish.resolve({ ...state.shardGroups[0], state: "ready" });
  await restart;
  assert.equal(state.shardGroups[0].state, "ready");
  assert.equal(state.shardGroupsError, "");
  for (const transitional of ["planning", "starting_workers", "starting_head", "stopping"]) {
    state.shardGroups[0].state = transitional;
    context.renderShardGroups();
    assert.doesNotMatch(elements["shard-group-list"].innerHTML, /data-shard-action=/);
  }
});

test("Node Ctrl-click opens a shard setup wizard with verified model profiles", () => {
  assert.match(indexSource, /id="shard-group-setup-dialog"/);
  assert.match(indexSource, /id="shard-group-setup-create"/);
  assert.match(source, /if \(!event\.ctrlKey \|\| !node\.healthy/);
  assert.match(shardSetupSource, /\/api\/nodes\/\$\{encodeURIComponent\(nodeID\)\}\/engines\/managed/);
  assert.match(shardSetupSource, /\/profile/);
  assert.match(shardSetupSource, /pinned_nodes: nodeIDs/);
  const element = { addEventListener() {}, querySelectorAll() { return []; } };
  const context = vm.createContext({
    document: { getElementById: () => element },
    dashboardState: { nodes: [], shardGroups: [], shardGroupsError: "" },
    escapeHtml: dashboardFormat.escapeHtml,
    formatBytes: dashboardFormat.formatBytes,
    fetchJSON: async () => [],
  });
  vm.runInContext(shardSetupSource, context);
  const valid = {
    name: "org/model",
    layer_count: 32,
    hidden_size: 4096,
    kv_heads: 8,
    head_dim: 128,
    file_size_bytes: 1024,
  };
  assert.equal(context.validShardModelProfile(valid), true);
  assert.equal(context.validShardModelProfile({ ...valid, layer_count: 0 }), false);
  assert.equal(context.sameShardModelProfile(valid, { ...valid }), true);
  assert.equal(context.sameShardModelProfile(valid, { ...valid, file_size_bytes: 2048 }), false);
  const engineInventory = new Map([
    ["rpc-ready", { engines: [{ kind: "ggml-rpc-server" }] }],
    ["not-configured", { engines: [] }],
    ["unreachable", { error: "request failed", engines: [] }],
  ]);
  assert.equal(context.shardSetupNodeEligibility("rpc-ready", engineInventory, "llamacpp-rpc").eligible, true);
  assert.equal(context.shardSetupNodeEligibility("not-configured", engineInventory, "llamacpp-rpc").eligible, true);
  assert.match(context.shardSetupNodeEligibility("not-configured", engineInventory, "llamacpp-rpc").reason, /install automatically/);
  assert.equal(context.shardSetupNodeEligibility("not-configured", engineInventory, "vllm-mp").reason, "No configured vllm");
  assert.equal(context.shardSetupNodeEligibility("unreachable", engineInventory, "llamacpp-rpc").reason, "Managed engine check failed: request failed");
  assert.equal(context.shardSetupNodeEligibility("pending", engineInventory, "llamacpp-rpc", true).reason, "Checking managed engines…");
});

test("Shard wizard automatically installs selected missing workers before loading model profiles", async () => {
  const elements = {};
  let inputs = [];
  for (const id of ["dialog", "nodes", "mode", "source", "head", "model", "context", "status", "create"]) {
    const element = {
      value: "",
      disabled: false,
      textContent: "",
      addEventListener() {},
      showModal() {},
      querySelectorAll(selector) {
        return selector.includes(":checked") ? inputs.filter((input) => input.checked) : inputs;
      },
    };
    Object.defineProperty(element, "innerHTML", {
      set(html) {
        this.html = html;
        if (id === "nodes") {
          inputs = [...html.matchAll(/<input[^>]+data-node-id="([^"]+)"[^>]*>/g)].map(([tag, nodeID]) => ({
            dataset: { nodeId: nodeID }, checked: tag.includes(" checked"), addEventListener() {},
          }));
        }
        if (id === "model" || id === "head") this.value = html.match(/value="([^"]*)"/)?.[1] || "";
      },
      get() { return this.html || ""; },
    });
    elements[`shard-group-setup-${id}`] = element;
  }
  const requests = [];
  let installFails = false;
  let createBody;
  const engines = [{ id: "maistr0-auto-rpc", kind: "ggml-rpc-server", state: "stopped" }, { id: "head", kind: "llama-server" }];
  const profile = { name: "model", layer_count: 32, hidden_size: 4096, kv_heads: 8, head_dim: 128, file_size_bytes: 1024 };
  const context = vm.createContext({
    document: { getElementById: (id) => elements[id] },
    dashboardState: { nodes: [{ id: "selected", healthy: true }, { id: "other", healthy: true }], shardGroups: [] },
    escapeHtml: dashboardFormat.escapeHtml,
    formatBytes: dashboardFormat.formatBytes,
    renderShardGroups() {},
    refreshShardGroups: async () => {},
    fetchJSON: async (url, options) => {
      requests.push(url);
      if (url.endsWith("/install-worker")) {
        if (installFails) throw new Error("SHA-256 verification failed");
        assert.equal(JSON.parse(options.body).source, "official");
        return { engines, source: "official", version: "test", updated: true };
      }
      if (url.includes("/profile?")) return profile;
      if (url.includes("/head-models?")) return {
        models: [{ id: "local-model", name: "model", source: "ollama", profile }], warnings: [],
      };
      if (url.endsWith("/configure-head")) {
        assert.equal(JSON.parse(options.body).model_id, "local-model");
        return { id: "maistr0-auto-head", kind: "llama-server", state: "stopped" };
      }
      if (url === "/api/shards") {
        createBody = JSON.parse(options.body);
        return { id: "group", state: "ready" };
      }
      return [];
    },
  });
  vm.runInContext(shardSetupSource, context);
  await context.openShardGroupWizard("selected");
  assert.ok(requests.includes("/api/nodes/selected/engines/managed/install-worker"));
  assert.ok(!requests.includes("/api/nodes/other/engines/managed/install-worker"));
  assert.ok(requests.indexOf("/api/nodes/selected/engines/managed/install-worker")
    < requests.indexOf("/api/nodes/selected/engines/managed/head/profile?requested_context=4096"));
  assert.equal(elements["shard-group-setup-create"].disabled, false);
  await context.createShardGroupFromWizard({ preventDefault() {} });
  assert.deepEqual(createBody.pinned_nodes, ["selected"]);
  assert.equal(createBody.model.name, "model");
  assert.equal(createBody.head_node_id, "selected");
  assert.equal(createBody.head_engine_id, "head");
  engines.splice(1, 1, { id: "maistr0-auto-head", kind: "llama-server", state: "stopped" });
  requests.length = 0;
  await context.openShardGroupWizard("selected");
  assert.equal(elements["shard-group-setup-create"].disabled, false);
  await context.createShardGroupFromWizard({ preventDefault() {} });
  assert.equal(createBody.head_engine_id, "maistr0-auto-head");
  assert.equal(createBody.head_node_id, "selected");
  assert.ok(requests.indexOf("/api/nodes/selected/engines/managed/configure-head") < requests.indexOf("/api/shards"));
  installFails = true;
  await context.openShardGroupWizard("selected");
  assert.equal(elements["shard-group-setup-create"].disabled, true);
  assert.match(elements["shard-group-setup-status"].textContent, /SHA-256 verification failed/);
});

test("Dashboard status view model renders node, model, load, memory, and version counts", () => {
  const capacityStatus = dashboardViewModel.dashboardStatusViewModel(
    {
      member_count: 3,
      memory: { total_experiences: 5, success_rate: 0.8 },
      build_version: "v1.2.3",
    },
    [
      { healthy: true, models: [{}, {}], active_tasks: 2 },
      { healthy: false, models: [{}], active_tasks: 9 },
      { healthy: true, models: [{}], active_tasks: 1 },
    ],
    {
      nodes: [{ healthy: true, active: 2 }, { healthy: false, active: 8 }],
      global_dispatch: { active_slots: 2, global_limit: 6, queue_depth: 1 },
    },
  );

  assert.equal(capacityStatus.nodeCount, 3);
  assert.equal(capacityStatus.modelCount, 3);
  assert.equal(capacityStatus.activeLoad, 2);
  assert.equal(capacityStatus.loadLabel, "Active Load: 2 · Dispatch 2/6 · Queue 1");
  assert.equal(capacityStatus.memoryLabel, "Memory: 5 · 80%");
  assert.equal(capacityStatus.buildVersion, "v1.2.3");

  const fallbackStatus = dashboardViewModel.dashboardStatusViewModel(
    {},
    [{ healthy: true, models: [], active_tasks: 2 }, { healthy: false, active_tasks: 8 }],
    null,
  );
  assert.equal(fallbackStatus.nodeCount, 1);
  assert.equal(fallbackStatus.activeLoad, 2);
  assert.equal(fallbackStatus.loadLabel, "Active Load: 2");
});

test("SSE snapshot application updates status pills and preserves loaded task history", () => {
  const dashboardState = {
    nodes: [],
    tasks: [{ id: "old", updated_at: "2026-10-16T00:00:00Z" }],
    pairing: { status: null },
    fullTaskHistoryLoaded: true,
    capacity: null,
    modelInventory: [],
    pipelinePlans: [],
  };
  const statNodes = { textContent: "" };
  const statModels = { textContent: "" };
  const statLoad = { textContent: "" };
  const statMemory = { textContent: "Memory: 1 · 50%" };
  const statVersion = { textContent: "" };
  const pipelineRenderCalls = [];
  let titleCount;
  let titleCalls = 0;
  const context = vm.createContext({
    dashboardState,
    dashboardSnapshot,
    dashboardViewModel,
    taskHistoryPage: { items: [], hasMore: false, loading: false },
    window: {},
    dashboardDOM,
    loadedDashboardPanels: new Set([
      "cluster-overview-panel", "nodes-panel", "setup-nodes-panel",
      "routing-panel", "discovery-panel", "tasks-panel",
    ]),
    statNodes,
    statModels,
    statLoad,
    statMemory,
    statVersion,
    updateClusterTitle: (count) => { titleCount = count; titleCalls += 1; },
    mergeLiveTaskHistory() {},
    renderOverview() {},
    renderNodes() {},
    renderSetupNodes() {},
    renderRouting() {},
    renderPipelineDashboard: (models, plans) => pipelineRenderCalls.push({ models, plans }),
    renderDiscovery() {},
    renderTasks() {},
    refreshOverviewTimes() {},
  });
  vm.runInContext(applySnapshotSource, context);
  const snapshot = {
    member_count: 3,
    build_version: "v1.2.3",
    cluster_health: {
      nodes: [{ node_id: "healthy", healthy: true, active: 2, max: 4, remaining: 2 }],
      global_dispatch: { active_slots: 2, global_limit: 8, queue_depth: 1 },
    },
    memory: { total_experiences: 5, success_rate: 0.8 },
    nodes: [
      { id: "healthy", healthy: true, active_tasks: 2, models: [{}, {}] },
      { id: "offline", healthy: false, active_tasks: 9, models: [{}] },
    ],
    tasks: [
      { id: "old", status: "completed", updated_at: "2026-10-16T01:00:00Z" },
      { id: "new", status: "running", updated_at: "2026-10-16T02:00:00Z" },
    ],
    discovery: [],
    model_inventory: [{ model_id: "logical-model", kind: "sharded" }],
    pipeline_plans: [{ model_id: "logical-model", status: "ready" }],
  };
  context.applySnapshot(snapshot);

  assert.equal(titleCount, 3);
  assert.equal(statNodes.textContent, "Nodes: 3");
  assert.equal(statModels.textContent, "Models: 2");
  assert.equal(statLoad.textContent, "Active Load: 2 · Dispatch 2/8 · Queue 1");
  assert.equal(statMemory.textContent, "Memory: 5 · 80%");
  assert.equal(statVersion.textContent, "v1.2.3");
  assert.deepEqual(dashboardState.modelInventory, [{ model_id: "logical-model", kind: "sharded" }]);
  assert.deepEqual(dashboardState.pipelinePlans, [{ model_id: "logical-model", status: "ready" }]);
  assert.deepEqual(pipelineRenderCalls, [{
    models: [{ model_id: "logical-model", kind: "sharded" }],
    plans: [{ model_id: "logical-model", status: "ready" }],
  }]);
  assert.deepEqual(dashboardState.tasks.map((task) => task.id), ["new", "old"]);
  assert.equal(dashboardState.fullTaskHistoryLoaded, true);
  context.applySnapshot(snapshot);
  assert.equal(titleCalls, 1, "unchanged status section should not update the title");
  assert.equal(pipelineRenderCalls.length, 1, "unchanged pipeline section should not rerender");
});

test("Pipeline dashboard distinguishes model distribution and renders live stage details", () => {
  const modelList = { innerHTML: "", contains: () => false, querySelectorAll: () => [] };
  const actionStatus = { textContent: "" };
  const context = vm.createContext({
    document: {
      activeElement: null,
      getElementById(id) {
        return id === "pipeline-model-list" ? modelList : actionStatus;
      },
    },
    dashboardState: { modelInventory: [], pipelinePlans: [] },
    escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, (character) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
      })[character]);
    },
    fetchJSON: async () => ({}),
    formatBytes: dashboardFormat.formatBytes,
  });
  vm.runInContext(pipelineViewSource, context);
  context.renderPipelineDashboard(
    [
      { model_id: "logical-model", kind: "sharded" },
      { model_id: "whole-model", kind: "whole", nodes: ["node-a"] },
    ],
    [{
      model_id: "logical-model",
      status: "ready",
      plan: { plan_version: 3 },
      stages: [
        { stage: 0, node_id: "node-a", start_layer: 0, end_layer: 8, engine: "simulated", status: "ready", active: 1, capacity: 2, utilization_percent: 50 },
        { stage: 1, node_id: "node-b", start_layer: 8, end_layer: 16, engine: "simulated", status: "ready", active: 0, capacity: 2, utilization_percent: 0, activation_transfer_ms: 2.4, activation_bytes: 4096 },
      ],
    }],
  );

  assert.match(modelList.innerHTML, /logical-model.*Sharded/s);
  assert.match(modelList.innerHTML, /whole-model.*Whole/s);
  assert.match(modelList.innerHTML, /Stage 2 · node-b/);
  assert.match(modelList.innerHTML, /8–16/);
  assert.match(modelList.innerHTML, /50%/);
  assert.match(modelList.innerHTML, /2\.4 ms · 4\.0 KiB/);
  assert.match(modelList.innerHTML, /Reshard \/ Reload stages/);
});

test("Reshard button requests a new placement and reports completion", async () => {
  const clickHandlers = [];
  const button = {
    dataset: { reshardModel: "logical model" },
    addEventListener: (_event, handler) => clickHandlers.push(handler),
    focus() {},
  };
  const modelList = {
    innerHTML: "",
    contains: () => false,
    querySelectorAll: () => [button],
  };
  const actionStatus = { textContent: "" };
  const requests = [];
  const context = vm.createContext({
    document: {
      activeElement: null,
      getElementById(id) {
        return id === "pipeline-model-list" ? modelList : actionStatus;
      },
    },
    dashboardState: {
      modelInventory: [{ model_id: "logical model", kind: "sharded" }],
      pipelinePlans: [],
    },
    escapeHtml: (value) => String(value),
    fetchJSON: async (url, options) => {
      requests.push({ url, options });
      return { plan: { plan_version: 4 } };
    },
  });
  vm.runInContext(pipelineViewSource, context);
  context.renderPipelineDashboard(context.dashboardState.modelInventory, []);
  await clickHandlers[0]();

  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/api/pipelines/logical%20model/reshard");
  assert.equal(requests[0].options.method, "POST");
  assert.match(actionStatus.textContent, /Placement 4 .* is loaded/);
  assert.match(modelList.innerHTML, /Placement 4 .* is loaded/);
});

test("Task history count labels preserve empty, singular, and plural forms", () => {
  assert.equal(dashboardViewModel.taskHistoryCountLabel(0, 0), "0 tasks");
  assert.equal(dashboardViewModel.taskHistoryCountLabel(1, 1), "Showing 1 of 1 task");
  assert.equal(dashboardViewModel.taskHistoryCountLabel(2, 4), "Showing 2 of 4 tasks");
});

test("Pairing PIN view model covers active, missing, and node-side states", () => {
  const active = dashboardViewModel.pairingPINViewModel({
    role: "orchestrator",
    pin: "123456",
    expires_at: "2026-10-18T00:00:00Z",
  });
  assert.equal(active.visible, true);
  assert.equal(active.hasPIN, true);
  assert.equal(active.pinText, "123456");
  assert.equal(active.regenerateText, "Generate / replace pairing PIN");
  assert.match(active.expiryText, /^Expires .+ The PIN can be used once\.$/);
  assert.match(active.stateText, /Share the PIN/);

  const missing = dashboardViewModel.pairingPINViewModel({ role: "orchestrator" });
  assert.equal(missing.visible, true);
  assert.equal(missing.hasPIN, false);
  assert.equal(missing.pinText, "Expired");
  assert.equal(missing.regenerateText, "Generate pairing PIN");
  assert.equal(missing.expiryText, "No active PIN. Generate a new PIN to pair another node.");
  assert.equal(missing.stateText, "No active PIN. Generate one to add or re-pair a node.");

  assert.equal(dashboardViewModel.pairingPINViewModel({ role: "node" }).visible, false);
});

test("SSE snapshots parse safely and merge live task updates without losing history", () => {
  const snapshots = [];
  const errors = [];
  const connectionChanges = [];
  const frames = [];
  class FakeEventSource {
    constructor(url) {
      this.url = url;
    }
  }
  const eventSource = dashboardSnapshot.connectDashboardEvents(
    (snapshot) => snapshots.push(snapshot),
    {
      EventSourceImpl: FakeEventSource,
      onError: (error) => errors.push(error),
      onConnectionChange: (connected) => connectionChanges.push(connected),
      requestAnimationFrameImpl: (callback) => {
        frames.push(callback);
        return frames.length;
      },
    },
  );
  assert.equal(eventSource.url, "/api/events");
  eventSource.onopen();
  eventSource.onerror();
  assert.deepEqual(connectionChanges, [true, false]);
  eventSource.onmessage({ data: '{"nodes":[{"id":"node-a"}]}' });
  eventSource.onmessage({ data: "{" });
  frames.shift()();
  assert.deepEqual(snapshots, [{ nodes: [{ id: "node-a" }] }]);
  assert.equal(errors.length, 1);

  const merged = dashboardSnapshot.mergeTaskSnapshot(
    [
      { id: "old", updated_at: "2026-10-16T00:00:00Z" },
      { id: "same", updated_at: "2026-10-16T01:00:00Z", status: "running" },
    ],
    [
      { id: "same", updated_at: "2026-10-16T02:00:00Z", status: "completed" },
      { id: "new", updated_at: "2026-10-16T03:00:00Z" },
    ],
    false,
    true,
  );
  assert.deepEqual(merged.tasks.map((task) => task.id), ["new", "same", "old"]);
  assert.equal(merged.tasks[1].status, "completed");
  assert.equal(merged.fullTaskHistoryLoaded, true);
});

test("Snapshot scheduling coalesces frames and skips identical snapshots", () => {
  const frames = [];
  const snapshots = [];
  const scheduler = dashboardSnapshot.createSnapshotScheduler(
    (snapshot) => snapshots.push(snapshot),
    { requestAnimationFrameImpl: (callback) => { frames.push(callback); return frames.length; } },
  );
  assert.equal(scheduler.enqueue({ revision: 1 }), true);
  assert.equal(scheduler.enqueue({ revision: 2 }), true);
  assert.equal(scheduler.enqueue({ revision: 2 }), false);
  assert.equal(frames.length, 1);
  frames.shift()();
  assert.deepEqual(snapshots, [{ revision: 2 }]);
  assert.equal(scheduler.enqueue({ revision: 2 }), false);
  assert.equal(scheduler.enqueue({ revision: 3 }), true);
  frames.shift()();
  assert.deepEqual(snapshots, [{ revision: 2 }, { revision: 3 }]);
});

test("Keyed DOM diff reuses stable nodes and replaces only changed items", () => {
  class Element {
    constructor(value) {
      this.value = value;
      this.dataset = {};
      this.children = [];
      this.parentNode = null;
    }
    get firstElementChild() { return this.children[0] || null; }
    get nextElementSibling() {
      if (!this.parentNode) return null;
      return this.parentNode.children[this.parentNode.children.indexOf(this) + 1] || null;
    }
    insertBefore(node, anchor) {
      if (node.parentNode) {
        const oldIndex = node.parentNode.children.indexOf(node);
        node.parentNode.children.splice(oldIndex, 1);
      }
      if (anchor && anchor.parentNode !== this) {
        throw new Error("NotFoundError: anchor is not a child of this node");
      }
      const index = anchor ? this.children.indexOf(anchor) : this.children.length;
      this.children.splice(index, 0, node);
      node.parentNode = this;
    }
    remove() {
      if (!this.parentNode) return;
      const index = this.parentNode.children.indexOf(this);
      if (index >= 0) this.parentNode.children.splice(index, 1);
      this.parentNode = null;
    }
  }
  const container = new Element("list");
  let rendered = 0;
  const options = {
    keyOf: (item) => item.id,
    signatureOf: (item) => item.value,
    renderItem: (item) => {
      rendered++;
      return new Element(item.value);
    },
  };
  const first = [{ id: "a", value: "one" }, { id: "b", value: "two" }];
  dashboardDOM.patchKeyedChildren(container, first, options);
  const originalA = container.children[0];
  const originalB = container.children[1];
  dashboardDOM.patchKeyedChildren(container, first, options);
  assert.equal(rendered, 2);
  assert.equal(container.children[0], originalA);
  assert.equal(container.children[1], originalB);

  dashboardDOM.patchKeyedChildren(container, [
    { id: "b", value: "two" },
    { id: "a", value: "updated" },
  ], options);
  assert.equal(rendered, 3);
  assert.equal(container.children[0], originalB);
  assert.equal(container.children[1].value, "updated");
  assert.notEqual(container.children[1], originalA);

  // Live telemetry changes the first rendered item in place on most snapshots.
  dashboardDOM.patchKeyedChildren(container, [
    { id: "b", value: "two-live" },
    { id: "a", value: "updated-live" },
    { id: "c", value: "three" },
  ], options);
  assert.deepEqual(container.children.map((child) => child.value), ["two-live", "updated-live", "three"]);
  assert.deepEqual(container.children.map((child) => child.dataset.renderKey), ["b", "a", "c"]);
});

test("Node properties allow configuring supported inference engines", () => {
  const index = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  for (const [value, label] of [
    ["llamacpp", "llama\\.cpp"],
    ["tgi", "TGI"],
    ["localai", "LocalAI"],
    ["mlx", "MLX-LM"],
    ["sglang", "SGLang"],
    ["openai", "OpenAI-compatible"],
  ]) {
    assert.match(index, new RegExp(`<option value="${value}">${label}<\\/option>`));
  }
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

test("mAIstr0 uses one submission action for status questions and work", async () => {
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

test("Cluster status questions are read-only while work stays on the task path", () => {
  const context = vm.createContext({});
  vm.runInContext(statusQuestionSource, context);
  for (const question of [
    "what common llms are on the cluster - Ie. is the same LLM model available on all cluster nodes?",
    "Is the same model available on all nodes?",
    "Which node has the most available memory?",
    "How busy is the cluster?",
    "Are any workers offline?",
    "Show all GPU and NPU hardware",
    "List models on each node",
    "What engines are running?",
    "What version of mAIstr0 is running?",
    "Who is the cluster leader?",
    "Why is the shard group degraded?",
    "What tasks are queued?",
    "Cluster status",
    "Please check the cluster health",
    "How fast is the network?",
    "Can the cluster run this model?",
    "Does the cluster have enough memory to run llama?",
  ]) assert.equal(context.isClusterStatusQuestion(question), true, question);
  for (const task of [
    "Write a report about LLM models",
    "Install a common model on all cluster nodes",
    "Which model should I install on the cluster?",
    "Check cluster health and restart offline workers",
    "Run a benchmark on all GPUs",
    "Can you run llama on the cluster?",
    "Create a shard group",
    "Research the history of AI",
    "What is the capital of France?",
  ]) assert.equal(context.isClusterStatusQuestion(task), false, task);
});

test("Common model inventory uses exact names, every node, and honest readiness", () => {
  const context = vm.createContext({ dashboardViewModel });
  vm.runInContext(statusQuestionSource, context);
  const node = (id, names, healthy = true) => ({
    id, healthy, models: names.map((name) => ({ name })),
    engines: [{ name: "ollama", ready: true, inventory_state: "available" }],
  });
  const cases = [
    {
      nodes: [node("a", ["qwen:7b", "qwen:7b", "llama:8b"]), node("b", ["qwen:7b"])],
      expected: /Models reported on ALL 2 registered nodes: qwen:7b\./,
    },
    {
      nodes: [node("a", ["qwen:7b"]), node("b", ["qwen:14b"])],
      expected: /Models reported on ALL 2 registered nodes: none\./,
    },
    {
      nodes: [node("a", ["qwen"]), node("b", ["Qwen"])],
      expected: /Models reported on ALL 2 registered nodes: none\./,
    },
    {
      nodes: [node("a", ["qwen"]), node("b", [])],
      expected: /Models reported on ALL 2 registered nodes: none\./,
    },
    {
      nodes: [node("a", ["qwen"]), node("b", ["qwen"], false)],
      expected: /Models available on ALL registered nodes: unknown/,
    },
    { nodes: [], expected: /No registered nodes/ },
  ];
  for (const { nodes, expected } of cases) {
    assert.match(context.formatClusterStatusAnswer(nodes, {}), expected);
  }
  const unknown = node("unavailable", ["qwen"]);
  unknown.engines[0].inventory_state = "error";
  const answer = context.formatClusterStatusAnswer([node("online", ["qwen"]), unknown], {});
  assert.match(answer, /availability unconfirmed/);
  assert.match(answer, /Online model inventory is incomplete/);
  assert.doesNotMatch(answer, /Models reported on ALL 2/);
  assert.match(context.formatClusterStatusAnswer([node("single", ["qwen"])], {}), /Only one node is online/);
});

test("Status reports include health, hardware, shards and task states without fabricating counters", () => {
  const context = vm.createContext({ dashboardViewModel });
  vm.runInContext(statusQuestionSource, context);
  const nodes = [{
    id: "worker", healthy: true, leader: true, version: "test-version",
    hardware: {
      cpu_cores: 8, cpu_stats_available: true, memory_stats_available: false,
      has_gpu: true, gpu_vendor: "NVIDIA", gpu_stats_available: true, gpu_utilization_percent: 42,
      has_npu: true, npu_stats_available: false,
    },
  }];
  const answer = context.formatClusterStatusAnswer(nodes, {
    "/api/cluster/health": {
      nodes: [{ node_id: "worker", remaining: 0, max: 2, telemetry_state: "stale" }],
      global_dispatch: { active_slots: 2, global_limit: 2, queue_depth: 3, active_workers: 1 },
      warnings: ["Telemetry is stale"],
    },
    "/api/shards": { groups: [{ id: "group-1", model: { name: "llama" }, mode: "llamacpp-rpc", state: "degraded" }] },
    "/api/tasks?page_size=50": { items: [{ id: "task-1", status: "running" }], has_more: true },
  });
  assert.match(answer, /CPU: 8 cores; utilization: 0%/);
  assert.match(answer, /RAM: unknown; utilization: unknown/);
  assert.match(answer, /GPU: NVIDIA; utilization: 42%; free memory: unknown/);
  assert.match(answer, /NPU: detected; utilization: unknown/);
  assert.match(answer, /capacity: 0 remaining \/ 2 slots/);
  assert.match(answer, /telemetry: stale/);
  assert.match(answer, /3 queued/);
  assert.match(answer, /group-1: llama, llamacpp-rpc, degraded/);
  assert.match(answer, /task-1: running/);
  assert.match(answer, /more history available in Tasks/);
  assert.match(answer, /Warning: Telemetry is stale/);
});

test("Direct cluster status reads live APIs without planning, even with no healthy nodes", async () => {
  const calls = [];
  const messages = [];
  const context = createTestContext({
    dashboardViewModel, isSending: false, sendMsgBtn: {}, clearDirectChatBtn: {},
    agentTabs: [], chatStatus: {}, directMessages: [], chatInput: { value: "question" },
    appendMessageToChat: (message) => messages.push(message),
    fetchJSON: async (url, opts) => {
      calls.push([url, opts]);
      if (url === "/api/nodes/all") return [{ id: "offline", healthy: false, models: [] }];
      if (url === "/api/cluster/health") return { nodes: [], warnings: ["offline"] };
      if (url === "/api/status") return { role: "orchestrator", persistence: { available: true } };
      if (url === "/api/shards") return { groups: [] };
      if (url === "/api/tasks?page_size=50") return { items: [], has_more: false };
      if (url === "/api/version") return { version: "test-version" };
      throw new Error(`Unexpected API call: ${url}`);
    },
  });
  vm.runInContext(statusQuestionSource + "\n" + directDispatchSource, context);
  await context.dispatchDirectTask("what common llms are on the cluster - Ie. is the same LLM model available on all cluster nodes?");
  assert.equal(calls.length, 6);
  assert.ok(calls.every(([, opts]) => opts === undefined));
  assert.ok(calls.every(([url]) => url !== "/api/tasks"));
  assert.match(messages.at(-1).content, /0 online, 1 offline/);
  assert.match(messages.at(-1).content, /Models available on ALL registered nodes: unknown/);
  assert.match(messages.at(-1).content, /test-version/);
  assert.equal(context.isSending, false);
  assert.equal(context.sendMsgBtn.disabled, false);
});

test("Cluster status API failures are explicit and never fall through to planning", async () => {
  const messages = [];
  const context = createTestContext({
    dashboardViewModel, isSending: false, sendMsgBtn: {}, clearDirectChatBtn: {},
    agentTabs: [], chatStatus: {}, directMessages: [], chatInput: {},
    appendMessageToChat: (message) => messages.push(message),
    fetchJSON: async (url) => {
      if (url === "/api/nodes/all") return [];
      throw new Error("status service unavailable");
    },
  });
  vm.runInContext(statusQuestionSource + "\n" + directDispatchSource, context);
  await context.dispatchDirectTask("Show cluster status");
  assert.match(messages.at(-1).content, /Incomplete status:/);
  assert.match(messages.at(-1).content, /status service unavailable/);
  context.fetchJSON = async () => { throw new Error("inventory unavailable"); };
  await context.dispatchDirectTask("Show cluster status");
  assert.match(messages.at(-1).content, /Cluster status failed: inventory unavailable/);
  assert.equal(context.isSending, false);
});

test("Malformed optional status responses are reported as incomplete", async () => {
  const context = vm.createContext({
    dashboardViewModel,
    fetchJSON: async (url) => {
      if (url === "/api/shards" || url === "/api/tasks?page_size=50") return {};
      return [];
    },
  });
  vm.runInContext(statusQuestionSource, context);
  const answer = await context.answerClusterStatusQuestion([]);
  assert.match(answer, /Incomplete status:/);
  for (const label of ["Capacity and telemetry", "Orchestrator status", "Shard groups", "Recent tasks", "Build version"]) {
    assert.ok(answer.includes(`${label} unavailable: invalid API response`));
  }
  assert.doesNotMatch(answer, /Shard groups: 0|Recent tasks: 0/);
});

test("Status answers accept unpaged task arrays and label nodes without engines", async () => {
  const tasks = Array.from({ length: 51 }, (_, index) => ({ id: `task-${index}`, status: "completed" }));
  const context = vm.createContext({
    dashboardViewModel,
    fetchJSON: async (url) => (url === "/api/tasks?page_size=50" ? tasks : {}),
  });
  vm.runInContext(statusQuestionSource, context);
  const answer = await context.answerClusterStatusQuestion([
    { id: "current", healthy: true, version: "0.0.23" },
    { id: "legacy", healthy: true },
  ]);
  assert.match(answer, /Recent tasks: 50 \(more history available in Tasks\)/);
  assert.doesNotMatch(answer, /Recent tasks unavailable/);
  assert.match(answer, /current: online[\s\S]*Engines: none configured or detected\./);
  assert.match(answer, /legacy: online[\s\S]*Engines: readiness not reported \(legacy node\)\./);
});

test("Non-status work still submits an auto-planned task", async () => {
  const calls = [];
  const context = createTestContext({
    isSending: false, sendMsgBtn: {}, clearDirectChatBtn: {}, agentTabs: [],
    chatStatus: {}, directMessages: [], chatInput: {}, appendMessageToChat() {}, refresh: async () => {},
    fetchJSON: async (url, opts) => {
      calls.push([url, opts]);
      return url === "/api/nodes/all" ? [{ id: "worker", healthy: true }] : { id: "task-1", status: "pending", subtasks: [] };
    },
  });
  vm.runInContext(statusQuestionSource + "\n" + directDispatchSource, context);
  await context.dispatchDirectTask("Write a report about LLM models");
  assert.equal(calls[1][0], "/api/tasks");
  assert.equal(JSON.parse(calls[1][1].body).plan_mode, "auto");
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

test("Resource telemetry distinguishes missing values, genuine zero, stale and fresh samples", () => {
  const context = vm.createContext({ escapeHtml: String });
  vm.runInContext(resourceHistorySource, context);
  const now = Date.parse("2026-10-26T12:00:00Z");
  assert.equal(context.resourceTelemetryLabel({}, now), "Resource telemetry unavailable");
  assert.equal(context.resourceTelemetryLabel({ telemetry_at: "0001-01-01T00:00:00Z" }, now), "Resource telemetry unavailable");
  assert.equal(context.resourceTelemetryLabel({ telemetry_at: new Date(now - 30000).toISOString() }, now), "Fresh resource telemetry");
  assert.equal(context.resourceTelemetryLabel({ telemetry_at: new Date(now - 30001).toISOString() }, now), "Stale resource telemetry");
  assert.equal(context.resourceTelemetryLabel({ telemetry_at: new Date(now + 1).toISOString() }, now), "Stale resource telemetry");
  for (const kind of ["gpu", "npu"]) {
    const gauge = kind === "gpu" ? context.gpuGaugeMarkup : context.npuGaugeMarkup;
    const hardware = { [`has_${kind}`]: true, [`${kind}_stats_available`]: true };
    assert.match(gauge({ hardware }), /n\/a/);
    hardware[`${kind}_utilization_percent`] = null;
    assert.match(gauge({ hardware }), /n\/a/);
    hardware[`${kind}_utilization_percent`] = 0;
    assert.match(gauge({ hardware }), /utilization 0%/);
  }
  const html = context.resourceHistoryMarkup({ id: "missing", hardware: { cpu_stats_available: true, cpu_utilization_percent: null } });
  assert.match(html, /CPU n\/a/);
  assert.doesNotMatch(html, /CPU 0%/);
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
    dashboardViewModel,
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
    dashboardViewModel,
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
    cards: [],
    get firstElementChild() { return this.cards[0] || null; },
    insertBefore(card, anchor) {
      if (card.parentNode) {
        const previousIndex = card.parentNode.cards.indexOf(card);
        if (previousIndex >= 0) card.parentNode.cards.splice(previousIndex, 1);
      }
      const index = anchor ? this.cards.indexOf(anchor) : this.cards.length;
      this.cards.splice(index < 0 ? this.cards.length : index, 0, card);
      card.parentNode = this;
    },
  };
  const contextMenus = [];
  const openedPairingTargets = [];
  const securityStatus = { textContent: "" };
  const context = vm.createContext({
    discoveryList: list,
    dashboardState: { nodes, pairing: { status: pairing } },
    document: {
      getElementById: (id) => id === "discovery-security-status" ? securityStatus : null,
      createElement: () => {
        const joinButton = { addEventListener(_event, handler) { this.handler = handler; } };
        const card = {
          innerHTML: "",
          className: "",
          dataset: {},
          parentNode: null,
          get nextElementSibling() {
            if (!this.parentNode) return null;
            return this.parentNode.cards[this.parentNode.cards.indexOf(this) + 1] || null;
          },
          remove() {
            if (!this.parentNode) return;
            const index = this.parentNode.cards.indexOf(this);
            if (index >= 0) this.parentNode.cards.splice(index, 1);
            this.parentNode = null;
          },
          joinButton,
          querySelector: (selector) => selector === "[data-action=join-orchestrator]" ? joinButton : null,
        };
        return card;
      },
    },
    dashboardDOM,
    escapeHtml: (value) => String(value),
    attachNodeContextMenu: (element, node) => contextMenus.push({ element, node }),
    openPairingDialog: (node) => openedPairingTargets.push(node),
  });
  vm.runInContext(discoverySource, context);
  return { context, list, contextMenus, openedPairingTargets, securityStatus };
}

test("Local Network labels whether LAN discovery announcements are authenticated", () => {
  const { context, securityStatus } = discoveryContext();
  context.renderDiscovery([], "hmac-sha256");
  assert.match(securityStatus.textContent, /HMAC-SHA256 enabled/);
  context.renderDiscovery([], "unauthenticated");
  assert.match(securityStatus.textContent, /Unauthenticated \(default\)/);
  assert.match(securityStatus.textContent, /discovery_hmac_secret/);
});

test("Local Network refresh reads the privacy-safe discovery security status", async () => {
  const start = source.indexOf("async function refreshDiscovery() {");
  const end = source.indexOf("\nsetInterval(", start);
  const refreshSource = source.slice(start, end);
  const requests = [];
  let snapshot;
  const context = vm.createContext({
    fetchJSON: async (url) => {
      requests.push(url);
      return url === "/api/discovery"
        ? [{ id: "peer-1" }]
        : { discovery_authentication: "hmac-sha256" };
    },
    dashboardState: { nodes: [], tasks: [] },
    applySnapshot: (value) => { snapshot = value; },
    console: { error: () => {} },
  });
  vm.runInContext(refreshSource, context);
  await context.refreshDiscovery();
  assert.deepEqual(requests, ["/api/discovery", "/api/security/status"]);
  assert.equal(snapshot.discovery_authentication, "hmac-sha256");
  assert.deepEqual(Array.from(snapshot.discovery, (peer) => peer.id), ["peer-1"]);
});

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

test("An unjoined node can join a selected discovered orchestrator from its Local Network card", () => {
  const { context, list, contextMenus, openedPairingTargets } = discoveryContext([], {
    role: "node", node_id: "self-worker", node_addr: "https://self:7451", joined: false,
  });
  context.renderDiscovery([{
    id: "selected-orchestrator",
    role: "orchestrator",
    peer_addr: "https://192.168.1.20:7450",
  }]);
  const target = contextMenus.find(({ node }) => node.id === "selected-orchestrator")?.node;
  assert.ok(target);
  assert.equal(target.role, "orchestrator");
  assert.equal(target.joinOrchestrator, true);
  assert.equal(target.address, "https://192.168.1.20:7450");
  const card = list.cards.find((item) => item.innerHTML.includes("selected-orchestrator"));
  assert.match(card.innerHTML, /Join selected-orchestrator's cluster/);
  card.joinButton.handler();
  assert.equal(openedPairingTargets[0].id, "selected-orchestrator");
  assert.equal(openedPairingTargets[0].address, "https://192.168.1.20:7450");
});

test("Selected orchestrator context action offers join or re-pair by current membership", () => {
  const contextMenuSource = source.slice(source.indexOf("function showNodeMenu("), source.indexOf("function attachNodeContextMenu("));
  const pairing = {};
  const properties = {};
  const open = {};
  const menu = {
    hidden: true,
    style: {},
    querySelector: (selector) => ({
      "[data-action=properties]": properties,
      "[data-action=pair]": pairing,
      "[data-action=open]": open,
    })[selector],
    getBoundingClientRect: () => ({ width: 180, height: 100 }),
  };
  const selected = {
    id: "selected-orchestrator",
    joinOrchestrator: true,
    healthy: true,
    discovered: true,
  };
  const context = vm.createContext({
    contextNode: selected,
    dashboardState: { pairing: { status: { role: "node", joined: false } } },
    document: { getElementById: () => menu },
    window: { innerWidth: 800, innerHeight: 600 },
  });
  vm.runInContext(contextMenuSource, context);
  context.showNodeMenu(100, 100);
  assert.equal(menu.hidden, false);
  assert.equal(pairing.textContent, "Join selected-orchestrator's cluster");
  assert.equal(pairing.disabled, false);
  assert.equal(properties.disabled, true);
  assert.equal(open.disabled, true);

  context.dashboardState.pairing.status = {
    role: "node", joined: true, orchestrator_id: selected.id,
  };
  context.showNodeMenu(100, 100);
  assert.equal(pairing.textContent, "Re-pair with selected-orchestrator");
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
  const createElement = (tagName) => {
    const element = {
      tagName,
      className: "",
      innerHTML: "",
      dataset: {},
      hidden: false,
      children: [],
      listeners: {},
      attributes: {},
      style: {},
      get firstElementChild() { return this.children[0] || null; },
      get nextElementSibling() {
        if (!this.parentNode) return null;
        return this.parentNode.children[this.parentNode.children.indexOf(this) + 1] || null;
      },
      appendChild(child) { this.insertBefore(child, null); },
      insertBefore(child, anchor) {
        if (child.parentNode) {
          const previous = child.parentNode;
          const index = previous.children.indexOf(child);
          if (index >= 0) previous.children.splice(index, 1);
        } else if (child.tagName === "tr") {
          rows.push(child);
        }
        const index = anchor ? this.children.indexOf(anchor) : this.children.length;
        this.children.splice(index < 0 ? this.children.length : index, 0, child);
        child.parentNode = this;
      },
      remove() {
        if (!this.parentNode) return;
        const index = this.parentNode.children.indexOf(this);
        if (index >= 0) this.parentNode.children.splice(index, 1);
        this.parentNode = null;
        const rowIndex = rows.indexOf(this);
        if (rowIndex >= 0) rows.splice(rowIndex, 1);
      },
      setAttribute(name, value) { this.attributes[name] = value; },
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
    };
    return element;
  };
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
    appendChild(table) { tables.push(table); table.parentNode = this; },
    querySelector() { return null; },
    setAttribute() {},
    addEventListener() {},
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
    dashboardViewModel,
    dashboardDOM,
    formatTimestamp: dashboardFormat.formatTimestamp,
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
          id: "task-2:review:1",
          request_id: "task-2:review",
          node_id: "node-old",
          model: "model-old",
          engine: "test-engine",
          status: "failed",
          failure_class: "upstream_5xx",
          http_status: 503,
          error: "temporary <failure>",
          started_at: "2026-10-13T10:01:00Z",
          finished_at: "2026-10-13T10:04:00Z",
          duration_ms: 180000,
        }, {
          number: 2,
          id: "task-2:review:2",
          request_id: "task-2:review",
          node_id: "node-a",
          model: "model-a",
          engine: "test-engine",
          status: "succeeded",
          started_at: "2026-10-13T10:04:00Z",
          finished_at: "2026-10-13T10:04:30Z",
          duration_ms: 30000,
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
  assert.match(rows[1].innerHTML, /Attempt ID: task-2:review:1/);
  assert.match(rows[1].innerHTML, /Request ID: task-2:review/);
  assert.match(rows[1].innerHTML, /Node: node-old/);
  assert.match(rows[1].innerHTML, /Model: model-old/);
  assert.match(rows[1].innerHTML, /Failure: upstream_5xx/);
  assert.match(rows[1].innerHTML, /HTTP 503/);
  assert.match(rows[1].innerHTML, /temporary &lt;failure&gt;/);
  assert.match(rows[1].innerHTML, /Attempt 2 · succeeded/);
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
  const pairingSource = source.slice(source.indexOf("async function loadPairingStatus("), source.indexOf("async function checkPairingInvitation("));
  let destination;
  const context = vm.createContext({
    fetchJSON: async () => ({ role: "node", dashboard_url: "http://localhost:7451" }),
    window: { location: { replace: (url) => { destination = url; } } },
  });
  vm.runInContext(pairingSource, context);
  await context.loadPairingStatus();
  assert.equal(destination, "http://localhost:7451");
});

test("Legacy auto-join status remains displayable; current manual status prompts for verified pairing", async () => {
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
      pairingFingerprintDisplay: {},
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
  assert.equal(automatic.context.pairingPinView.hidden, true);
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
    pairingFingerprint: { value: "" },
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

test("Pairing a selected orchestrator never trusts a discovery fingerprint", () => {
  const dialogSource = source.slice(source.indexOf("function openPairingDialog("), source.indexOf("pairingPinRegenerateBtn?.addEventListener("));
  let shown = false;
  const title = {};
  const description = {};
  const context = vm.createContext({
    dashboardState: {
      pairing: {
        status: { role: "node", node_id: "self-worker", joined: false },
        targetNode: null,
      },
    },
    pairingConnectDialog: { open: false, showModal: () => { shown = true; } },
    pairingStatus: {},
    pairingAddressField: {},
    pairingAddress: { value: "", focus: () => {} },
    pairingCode: { value: "old-pin" },
    pairingFingerprint: { value: "old-fingerprint" },
    pairingConnectButton: {},
    document: {
      getElementById: (id) => id === "pairing-title-node" ? title : description,
    },
  });
  vm.runInContext(dialogSource, context);
  context.openPairingDialog({
    id: "selected-orchestrator",
    role: "orchestrator",
    address: "https://192.168.1.20:7450",
    peer_addr: "https://192.168.1.20:7450",
    discovered: true,
    joinOrchestrator: true,
    fingerprint: "untrusted-discovery-fingerprint",
  });
  assert.equal(shown, true);
  assert.equal(title.textContent, "Join selected-orchestrator's cluster");
  assert.match(description.textContent, /fingerprint out of band/);
  assert.equal(context.pairingFingerprint.value, "");
  assert.equal(context.pairingAddress.value, "https://192.168.1.20:7450");
  assert.equal(context.pairingAddress.required, true);
  assert.equal(context.pairingAddressField.hidden, false);
  assert.equal(context.pairingCode.value, "");
  assert.equal(context.pairingConnectButton.textContent, "Join cluster");
});

test("Pairing a selected orchestrator sends its expected identity to the local node API", async () => {
  const start = source.indexOf("pairingConnectForm?.addEventListener(");
  const end = source.indexOf("\n});", start) + 4;
  const submissionSource = source.slice(start, end);
  const requests = [];
  let submit;
  const targetNode = {
    id: "selected-orchestrator",
    joinOrchestrator: true,
  };
  const context = vm.createContext({
    dashboardState: { pairing: { targetNode } },
    pairingConnectForm: { addEventListener: (_event, handler) => { submit = handler; } },
    pairingConnectButton: { disabled: false },
    pairingAddress: { value: "https://192.168.1.20:7450" },
    pairingCode: { value: "" },
    pairingFingerprint: { value: "a".repeat(64) },
    pairingStatus: { textContent: "" },
    pairingState: { textContent: "" },
    pairingConnectDialog: { close() {} },
    fetchJSON: async (endpoint, options) => {
      requests.push({ endpoint, options });
      return { orchestrator_id: targetNode.id, orchestrator_addr: "https://192.168.1.20:7450" };
    },
    loadPairingStatus: async () => {},
    refresh: async () => {},
  });
  vm.runInContext(submissionSource, context);
  await submit({ preventDefault() {} });
  assert.equal(requests.length, 1);
  assert.equal(requests[0].endpoint, "/api/pairing/connect");
  assert.deepEqual(JSON.parse(requests[0].options.body), {
    address: "https://192.168.1.20:7450",
    pin: "",
    fingerprint: "a".repeat(64),
    orchestrator_id: "selected-orchestrator",
  });
  assert.match(context.pairingStatus.textContent, /Connected to selected-orchestrator/);
});

test("Invitation polling preserves typed verification and acceptance never copies the offered fingerprint", async () => {
  const invitationSource = source.slice(source.indexOf("async function checkPairingInvitation("), source.indexOf("async function activateAgentTab("));
  const requests = [];
  const listeners = [];
  const fingerprint = { value: "old value" };
  const dialog = { open: false, showModal() { this.open = true; }, close() { this.open = false; }, addEventListener() {} };
  const context = vm.createContext({
    fetchJSON: async (path, options) => {
      if (path === "/api/pairing/invitation") return { pending: true, orchestrator_id: "leader", address: "https://leader:7450", fingerprint: "a".repeat(64), expires_at: "2030-01-01" };
      requests.push({ path, options });
      return { orchestrator_id: "leader", orchestrator_addr: "https://leader:7450" };
    },
    dashboardState: { pairing: {} },
    pairingInvitationFingerprint: fingerprint,
    pairingInvitationDetails: {},
    pairingInvitationStatus: {},
    pairingInvitationDialog: dialog,
    pairingConnectDialog: { close() {} },
    pairingInvitationAcceptBtn: { addEventListener: () => listeners.push("accept") },
    pairingInvitationDeclineBtn: { addEventListener: () => listeners.push("decline") },
    loadPairingStatus: async () => {},
    refresh: async () => {},
  });
  vm.runInContext(invitationSource, context);
  await context.checkPairingInvitation();
  assert.equal(fingerprint.value, "");
  await context.respondToPairingInvitation(true);
  assert.equal(requests.length, 0);
  assert.match(context.pairingInvitationStatus.textContent, /verified out of band/);
  fingerprint.value = "b".repeat(64);
  await context.checkPairingInvitation();
  assert.equal(fingerprint.value, "b".repeat(64));
  assert.deepEqual(listeners, ["accept", "decline"]);
  await context.respondToPairingInvitation(true);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].path, "/api/pairing/invitation/accept");
  assert.deepEqual(JSON.parse(requests[0].options.body), { fingerprint: "b".repeat(64) });
});

test("Direct chat removes the welcome card before showing the first message", () => {
  const removed = [];
  const appended = [];
  const welcome = { className: "welcome-card" };
  const chatMessagesEl = {
    querySelectorAll: (selector) => (selector === ".welcome-card" && !removed.includes(welcome) ? [welcome] : []),
  };
  const helperSource = directChatSource.slice(
    directChatSource.indexOf("function appendChatItem("),
    directChatSource.indexOf("function appendMessageToChat("),
  );
  const context = vm.createContext({
    chatMessagesEl,
    dashboardDOM: {
      removeVirtualListItem: (_container, node) => removed.push(node),
      appendVirtualListItem: (_container, node) => appended.push(node),
    },
  });
  vm.runInContext(helperSource, context);
  const reply = { className: "chat-msg assistant" };
  context.appendChatItem(reply);
  context.appendChatItem({ className: "chat-msg user" });
  assert.deepEqual(removed, [welcome]);
  assert.equal(appended[0], reply);
  assert.equal(appended.length, 2);
});

test("Light theme gives chat replies a readable light surface", () => {
  const css = fs.readFileSync(path.join(__dirname, "src", "style.css"), "utf8");
  const light = css.slice(css.indexOf("@media (prefers-color-scheme: light)"));
  const rule = light.match(/\.chat-msg\.assistant \.msg-bubble[^{]*\{([^}]*)\}/);
  assert.ok(rule, "light theme must override the dark assistant bubble");
  assert.match(rule[1], /background:\s*var\(--surface\)/);
  assert.match(rule[1], /color:\s*var\(--text\)/);
});

function viewStateHarness({ stored = {}, chatMode = "direct" } = {}) {
  const storage = new Map(Object.entries(stored));
  const tabs = new Map();
  const clicks = [];
  const makeTab = (id, cls, selected, extra = {}) => {
    const tab = {
      id, className: cls, dataset: extra.dataset || {}, attrs: { role: "tab", "aria-selected": String(selected), ...extra.attrs },
      getAttribute(name) { return this.attrs[name] ?? null; },
      click() { clicks.push(id); },
    };
    tabs.set(id, tab);
    return tab;
  };
  makeTab("agent-tab-button", "nav-tab active", true, { dataset: { tab: "agent-tab" } });
  makeTab("cluster-tab-button", "nav-tab", false, { dataset: { tab: "cluster-tab" } });
  makeTab("cluster-overview-tab", "cluster-workspace-tab", true, { attrs: { "aria-controls": "cluster-overview-panel" } });
  makeTab("cluster-tasks-tab", "cluster-workspace-tab", false, { attrs: { "aria-controls": "tasks-panel" } });
  const calls = [];
  let pagehide;
  const context = createTestContext({
    window: { addEventListener: (name, fn) => { if (name === "pagehide") pagehide = fn; } },
    sessionStorage: { getItem: (k) => storage.get(k) ?? null, setItem: (k, v) => storage.set(k, v) },
    CSS: { escape: (value) => value },
    document: {
      getElementById: (id) => tabs.get(id) || null,
      querySelector: (selector) => {
        if (selector === ".nav-tab.active") return [...tabs.values()].find((tab) => tab.className === "nav-tab active") || null;
        if (selector === ".agent-tab.active") return { dataset: { agent: chatMode } };
        if (selector === '.cluster-workspace-tab[aria-selected="true"]') {
          return [...tabs.values()].find((tab) => tab.className === "cluster-workspace-tab" && tab.attrs["aria-selected"] === "true") || null;
        }
        return null;
      },
      querySelectorAll: () => [...tabs.values()].filter((tab) => tab.className === "cluster-workspace-tab"),
    },
    directMessages: [],
    activeChatMode: chatMode,
    activeSessionId: "",
    renderDirectMessages: () => calls.push("renderDirectMessages"),
    refresh: async () => calls.push("refresh"),
    refreshTaskHistory: async (reset) => calls.push(`refreshTaskHistory:${reset}`),
    refreshDiscovery: async () => calls.push("refreshDiscovery"),
    refreshShardGroups: async () => calls.push("refreshShardGroups"),
    refreshMemory: async () => calls.push("refreshMemory"),
    selectSession: async (id) => calls.push(`selectSession:${id}`),
    loadPairingStatus: async () => calls.push("loadPairingStatus"),
    checkSetupReadiness: async () => calls.push("checkSetupReadiness"),
    loadModelCatalog: async () => calls.push("loadModelCatalog"),
    markDashboardSynced: () => {},
    console,
  });
  vm.runInContext(readModuleSource("src", "views", "view-state.js"), context);
  return { context, storage, tabs, clicks, calls, pagehide: () => pagehide() };
}

test("Reloading restores the selected tabs and the direct chat transcript", () => {
  const first = viewStateHarness();
  first.context.directMessages = [
    { role: "user", content: "what models are available?" },
    { role: "assistant", content: "Live cluster status" },
    { role: "tool", content: "ignored" },
  ];
  first.tabs.get("cluster-tab-button").className = "nav-tab active";
  first.tabs.get("agent-tab-button").className = "nav-tab";
  first.tabs.get("cluster-overview-tab").attrs["aria-selected"] = "false";
  first.tabs.get("cluster-tasks-tab").attrs["aria-selected"] = "true";
  first.pagehide();

  const reloaded = viewStateHarness({ stored: Object.fromEntries(first.storage) });
  assert.deepEqual(JSON.parse(JSON.stringify(reloaded.context.directMessages)), [
    { role: "user", content: "what models are available?" },
    { role: "assistant", content: "Live cluster status" },
  ]);
  assert.ok(reloaded.calls.includes("renderDirectMessages"));
  assert.deepEqual(reloaded.clicks, ["cluster-tasks-tab", "cluster-tab-button"]);
});

test("Refresh view reloads only the active tab's data", async () => {
  const harness = viewStateHarness();
  harness.tabs.get("cluster-tab-button").className = "nav-tab active";
  harness.tabs.get("agent-tab-button").className = "nav-tab";
  harness.tabs.get("cluster-overview-tab").attrs["aria-selected"] = "false";
  harness.tabs.get("cluster-tasks-tab").attrs["aria-selected"] = "true";
  harness.calls.length = 0;
  await harness.context.refreshActiveView();
  assert.deepEqual(harness.calls, ["refresh", "refreshTaskHistory:true"]);

  harness.tabs.get("cluster-tab-button").className = "nav-tab";
  harness.tabs.get("agent-tab-button").className = "nav-tab active";
  harness.calls.length = 0;
  await harness.context.refreshActiveView();
  assert.deepEqual(harness.calls, ["renderDirectMessages"]);
});

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(path.join(__dirname, "static", "app.js"), "utf8");
const discoverySource = source.slice(source.indexOf("function visibleNetworkPeers("), source.indexOf("function renderTasks("));

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

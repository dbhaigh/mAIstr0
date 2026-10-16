const nodesList = document.getElementById("nodes-list");
const routingList = document.getElementById("routing-list");
const discoveryList = document.getElementById("discovery-list");
const tasksList = document.getElementById("tasks-list");
const taskHistorySearch = document.getElementById("task-history-search");
const taskHistoryStatus = document.getElementById("task-history-status");
const taskHistoryCount = document.getElementById("task-history-count");
const taskInput = document.getElementById("task-input");
const taskAutoPlan = document.getElementById("task-auto-plan");
const taskRequireReview = document.getElementById("task-require-review");
const taskRequireApproval = document.getElementById("task-require-approval");
const submitBtn = document.getElementById("submit-btn");
const submitStatus = document.getElementById("submit-status");
const copyTasksBtn = document.getElementById("copy-tasks-btn");
const refreshAllModelsBtn = document.getElementById("refresh-all-models-btn");
const overviewActivity = document.getElementById("overview-activity");
const overviewNodes = document.getElementById("overview-nodes");
const clusterOverviewStatus = document.getElementById("cluster-overview-status");
const clusterPeers = document.getElementById("cluster-peers");
const setupCheckBtn = document.getElementById("setup-check-btn");
const setupStatus = document.getElementById("setup-status");
const setupResults = document.getElementById("setup-results");
const setupNodeList = document.getElementById("setup-node-list");
const setupNodeSummary = document.getElementById("setup-node-summary");
const catalogNodeSelect = document.getElementById("catalog-node");
const catalogEngineSelect = document.getElementById("catalog-engine");
const catalogSearch = document.getElementById("catalog-search");
const catalogStatus = document.getElementById("catalog-status");
const catalogResults = document.getElementById("catalog-results");
let modelCatalogEntries = [];
let catalogEngines = [];
let catalogModels = [];
const pairingPinView = document.getElementById("pairing-pin-view");
const pairingPin = document.getElementById("pairing-pin");
const pairingPinExpiry = document.getElementById("pairing-pin-expiry");
const pairingPinRegenerateBtn = document.getElementById("pairing-pin-regenerate-btn");
const pairingAddressDisplay = document.getElementById("pairing-address-display");
const pairingInstructions = document.getElementById("pairing-instructions");
const pairingNodeConnect = document.getElementById("pairing-node-connect");
const pairingConnectForm = document.getElementById("pairing-connect-form");
const pairingConnectDialog = document.getElementById("pairing-connect-dialog");
const pairingConnectOpenBtn = document.getElementById("pairing-connect-open");
const pairingConnectCancelBtn = document.getElementById("pairing-connect-cancel");
const pairingAddress = document.getElementById("pairing-address");
const pairingAddressField = document.getElementById("pairing-address-field");
const pairingCode = document.getElementById("pairing-code");
const pairingConnectButton = document.getElementById("pairing-connect-button");
const pairingStatus = document.getElementById("pairing-status");
const pairingState = document.getElementById("pairing-state");
const pairingInvitationDialog = document.getElementById("pairing-invitation-dialog");
const pairingInvitationDetails = document.getElementById("pairing-invitation-details");
const pairingInvitationStatus = document.getElementById("pairing-invitation-status");
const pairingInvitationAcceptBtn = document.getElementById("pairing-invitation-accept");
const pairingInvitationDeclineBtn = document.getElementById("pairing-invitation-decline");
const statNodes = document.getElementById("stat-nodes");
const statModels = document.getElementById("stat-models");
const statLoad = document.getElementById("stat-load");
const statMemory = document.getElementById("stat-memory");
const statVersion = document.getElementById("stat-version");
const persistenceWarning = document.getElementById("persistence-warning");
const navTabs = document.querySelectorAll(".nav-tab");
const tabContents = document.querySelectorAll(".tab-content");
const clusterWorkspaceTabs = document.querySelectorAll(".cluster-workspace-tab");
const clusterWindows = document.querySelectorAll(".cluster-window");
const overviewTabs = document.querySelectorAll(".overview-tab");
const overviewPanels = document.querySelectorAll("#cluster-overview-panel > .overview-column");
const setupTabs = document.querySelectorAll(".setup-tab");
const setupPanels = document.querySelectorAll("#setup-panel > .setup-panel-section");
const learningSummary = document.getElementById("learning-summary");
const factsList = document.getElementById("facts-list");
const factForm = document.getElementById("fact-form");
const factKey = document.getElementById("fact-key");
const factValue = document.getElementById("fact-value");
const experiencesList = document.getElementById("experiences-list");
const transcriptsList = document.getElementById("transcripts-list");
const memorySearch = document.getElementById("memory-search");
const sessionListEl = document.getElementById("session-list");
const newSessionBtn = document.getElementById("new-session-btn");
const agentHarnessSelect = document.getElementById("agent-harness-select");
const agentHarnessStatus = document.getElementById("agent-harness-status");
const agentSidebar = document.querySelector(".agent-sidebar");
const agentTabs = document.querySelectorAll(".agent-tab");
const agentModelSelect = document.getElementById("agent-model-select");
const agentStepsSelect = document.getElementById("agent-steps-select");
const agentSettingsTitle = document.getElementById("agent-settings-title");
const agentToolsList = document.getElementById("agent-tools-list");
const clearDirectChatBtn = document.getElementById("clear-direct-chat-btn");
const interruptMsgBtn = document.getElementById("interrupt-msg-btn");
const nodeTerminalInterruptBtn = document.getElementById("node-terminal-interrupt");
const chatMessagesEl = document.getElementById("chat-messages");
const chatInput = document.getElementById("chat-input");
const sendMsgBtn = document.getElementById("send-msg-btn");
const chatStatus = document.getElementById("chat-status");
const promptChips = document.querySelectorAll(".prompt-chip");
const dashboardState = {
  nodes: [],
  tasks: [],
  sessions: [],
  pairing: {
    status: null,
    targetNode: null,
    setupFocused: false,
    promptShown: false,
    dialogAutoOpened: false
  },
  capacity: null,
  fullTaskHistoryLoaded: false
};
const taskHistoryPage = {
  items: [],
  nextCursor: "",
  hasMore: false,
  loading: false,
  generation: 0,
  initialized: false,
  filterKey: ""
};
const memoryHistoryPages = {
  facts: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  experiences: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  dialogues: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" }
};
let activeSessionId = null;
const activeSessionIds = /* @__PURE__ */ new Map();
let isSending = false;
let activeAbortController = null;
let terminalAbortController = null;
let streamingAssistantEl = null;
let currentAgentHarness = "pi";
let agentSupportsCoordinatorModel = true;
let activeChatMode = "direct";
let directMessages = [];
const modelPanelOpen = /* @__PURE__ */ new Set();
const pendingModelState = /* @__PURE__ */ new Map();
const pendingDefaultModel = /* @__PURE__ */ new Map();
const lastFetchedModels = /* @__PURE__ */ new Map();
let contextNode = null;
const nodeProperties = /* @__PURE__ */ new Map();
let terminalNode = null;
let terminalDialogueId = null;
navTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    navTabs.forEach((t) => t.classList.remove("active"));
    tabContents.forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    const target = document.getElementById(tab.dataset.tab);
    if (target) target.classList.add("active");
    if (tab.dataset.tab === "memory-tab") refreshMemory();
    if (tab.dataset.tab === "cluster-tab") void refresh();
  });
});
function wireWorkspaceTabs(tabs, panels) {
  const selectTab = (tab) => {
    const targetID = tab.getAttribute("aria-controls");
    tabs.forEach((item) => {
      const active = item === tab;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
      item.tabIndex = active ? 0 : -1;
    });
    panels.forEach((panel) => {
      const active = panel.id === targetID;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
  };
  tabs.forEach((tab, index) => {
    tab.tabIndex = tab.getAttribute("aria-selected") === "true" ? 0 : -1;
    tab.addEventListener("click", () => selectTab(tab));
    tab.addEventListener("keydown", (event) => {
      let nextIndex;
      if (event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
      else if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = tabs.length - 1;
      else return;
      event.preventDefault();
      selectTab(tabs[nextIndex]);
      tabs[nextIndex].focus();
    });
  });
}
wireWorkspaceTabs(clusterWorkspaceTabs, clusterWindows);
wireWorkspaceTabs(overviewTabs, overviewPanels);
wireWorkspaceTabs(setupTabs, setupPanels);
pairingConnectCancelBtn?.addEventListener("click", () => pairingConnectDialog?.close());
pairingConnectOpenBtn?.addEventListener("click", () => openPairingDialog());
setupCheckBtn?.addEventListener("click", () => checkSetupReadiness());
catalogNodeSelect?.addEventListener("change", () => loadCatalogTarget());
catalogEngineSelect?.addEventListener("change", () => renderModelCatalog());
catalogSearch?.addEventListener("input", () => renderModelCatalog());
async function fetchJSON(url, opts) {
  const resp = await fetch(url, opts);
  if (!resp.ok) {
    throw new Error(await resp.text());
  }
  if (resp.status === 204) return null;
  return resp.json();
}
let optionalFeaturesInitialized = false;
function initializeOptionalFeatures(enabled) {
  if (optionalFeaturesInitialized) return;
  optionalFeaturesInitialized = true;
  if (!enabled) {
    agentTabs.forEach((tab) => {
      if (tab.dataset.agent !== "direct") {
        tab.hidden = true;
        tab.disabled = true;
      }
    });
    const memoryTab = document.querySelector('.nav-tab[data-tab="memory-tab"]');
    if (memoryTab?.classList.contains("active")) {
      document.querySelector('.nav-tab[data-tab="cluster-tab"]')?.click();
    }
    if (memoryTab) memoryTab.hidden = true;
    if (activeChatMode !== "direct") activateDirectChat();
    return;
  }
  loadAgentTools();
  loadAgentBackend().then(loadSessions);
}
async function refreshPersistenceStatus() {
  if (!persistenceWarning) return;
  try {
    const status = await fetchJSON("/api/status");
    const persistence = status.persistence;
    persistenceWarning.hidden = status.mode === "minimal" || persistence?.available !== false;
    persistenceWarning.textContent = persistence?.warning || "Persistent memory is unavailable.";
    initializeOptionalFeatures(status.mode !== "minimal");
  } catch (error) {
    console.error("Could not load persistence status:", error);
    initializeOptionalFeatures(true);
  }
}
function formatRelativeTime(value) {
  if (!value) return "time unavailable";
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "time unavailable";
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1e3));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(timestamp).toLocaleDateString();
}
function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
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

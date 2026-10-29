// DOM Elements
export const nodesList = document.getElementById("nodes-list");
export const routingList = document.getElementById("routing-list");
export const discoveryList = document.getElementById("discovery-list");
export const tasksList = document.getElementById("tasks-list");
export const taskHistorySearch = document.getElementById("task-history-search");
export const taskHistoryStatus = document.getElementById("task-history-status");
export const taskHistoryCount = document.getElementById("task-history-count");
export const taskInput = document.getElementById("task-input");
export const taskAutoPlan = document.getElementById("task-auto-plan");
export const taskRequireReview = document.getElementById("task-require-review");
export const taskRequireApproval = document.getElementById("task-require-approval");
export const submitBtn = document.getElementById("submit-btn");
export const submitStatus = document.getElementById("submit-status");
export const copyTasksBtn = document.getElementById("copy-tasks-btn");
export const refreshAllModelsBtn = document.getElementById("refresh-all-models-btn");
export const overviewActivity = document.getElementById("overview-activity");
export const overviewNodes = document.getElementById("overview-nodes");
export const clusterOverviewStatus = document.getElementById("cluster-overview-status");
export const clusterPeers = document.getElementById("cluster-peers");
export const setupCheckBtn = document.getElementById("setup-check-btn");
export const setupStatus = document.getElementById("setup-status");
export const setupResults = document.getElementById("setup-results");
export const setupNodeList = document.getElementById("setup-node-list");
export const setupNodeSummary = document.getElementById("setup-node-summary");
export const catalogNodeSelect = document.getElementById("catalog-node");
export const catalogEngineSelect = document.getElementById("catalog-engine");
export const catalogSearch = document.getElementById("catalog-search");
export const catalogStatus = document.getElementById("catalog-status");
export const catalogResults = document.getElementById("catalog-results");
export let modelCatalogEntries = [];
export let catalogEngines = [];
export let catalogModels = [];
export const pairingPinView = document.getElementById("pairing-pin-view");
export const pairingPin = document.getElementById("pairing-pin");
export const pairingPinExpiry = document.getElementById("pairing-pin-expiry");
export const pairingPinRegenerateBtn = document.getElementById("pairing-pin-regenerate-btn");
export const pairingRequireVerification = document.getElementById("pairing-require-verification");
export const pairingPolicyState = document.getElementById("pairing-policy-state");
export const pairingAddressDisplay = document.getElementById("pairing-address-display");
export const pairingInstructions = document.getElementById("pairing-instructions");
export const pairingNodeConnect = document.getElementById("pairing-node-connect");
export const pairingConnectForm = document.getElementById("pairing-connect-form");
export const pairingConnectDialog = document.getElementById("pairing-connect-dialog");
export const pairingConnectOpenBtn = document.getElementById("pairing-connect-open");
export const pairingConnectCancelBtn = document.getElementById("pairing-connect-cancel");
export const pairingAddress = document.getElementById("pairing-address");
export const pairingAddressField = document.getElementById("pairing-address-field");
export const pairingCode = document.getElementById("pairing-code");
export const pairingFingerprint = document.getElementById("pairing-fingerprint");
export const pairingFingerprintDisplay = document.getElementById("pairing-fingerprint-display");
export const pairingInvitationFingerprint = document.getElementById("pairing-invitation-fingerprint");
export const pairingConnectButton = document.getElementById("pairing-connect-button");
export const pairingStatus = document.getElementById("pairing-status");
export const pairingState = document.getElementById("pairing-state");
export const pairingInvitationDialog = document.getElementById("pairing-invitation-dialog");
export const pairingInvitationDetails = document.getElementById("pairing-invitation-details");
export const pairingInvitationStatus = document.getElementById("pairing-invitation-status");
export const pairingInvitationAcceptBtn = document.getElementById("pairing-invitation-accept");
export const pairingInvitationDeclineBtn = document.getElementById("pairing-invitation-decline");

// Header stats & navigation
export const statNodes = document.getElementById("stat-nodes");
export const statModels = document.getElementById("stat-models");
export const statLoad = document.getElementById("stat-load");
export const statMemory = document.getElementById("stat-memory");
export const statVersion = document.getElementById("stat-version");
export const connectionStatus = document.getElementById("connection-status");
export const persistenceWarning = document.getElementById("persistence-warning");
export const navTabs = document.querySelectorAll(".nav-tab");
export const tabContents = document.querySelectorAll(".tab-content");
export const clusterWorkspaceTabs = document.querySelectorAll(".cluster-workspace-tab");
export const clusterWindows = document.querySelectorAll(".cluster-window");
export const overviewTabs = document.querySelectorAll(".overview-tab");
export const overviewPanels = document.querySelectorAll("#cluster-overview-panel > .overview-column");
export const setupTabs = document.querySelectorAll(".setup-tab");
export const setupPanels = document.querySelectorAll("#setup-panel > .setup-panel-section");

// Memory & learning elements
export const learningSummary = document.getElementById("learning-summary");
export const factsList = document.getElementById("facts-list");
export const factForm = document.getElementById("fact-form");
export const factKey = document.getElementById("fact-key");
export const factValue = document.getElementById("fact-value");
export const experiencesList = document.getElementById("experiences-list");
export const transcriptsList = document.getElementById("transcripts-list");
export const memorySearch = document.getElementById("memory-search");

// Agent Console Elements
export const sessionListEl = document.getElementById("session-list");
export const newSessionBtn = document.getElementById("new-session-btn");
export const agentHarnessSelect = document.getElementById("agent-harness-select");
export const agentHarnessStatus = document.getElementById("agent-harness-status");
export const agentSidebar = document.querySelector(".agent-sidebar");
export const agentTabs = document.querySelectorAll(".agent-tab");
export const agentModelSelect = document.getElementById("agent-model-select");
export const agentStepsSelect = document.getElementById("agent-steps-select");
export const agentSettingsTitle = document.getElementById("agent-settings-title");
export const agentToolsList = document.getElementById("agent-tools-list");
export const clearDirectChatBtn = document.getElementById("clear-direct-chat-btn");
export const interruptMsgBtn = document.getElementById("interrupt-msg-btn");
export const nodeTerminalInterruptBtn = document.getElementById("node-terminal-interrupt");
export const chatMessagesEl = document.getElementById("chat-messages");
export const chatInput = document.getElementById("chat-input");
export const sendMsgBtn = document.getElementById("send-msg-btn");
export const chatStatus = document.getElementById("chat-status");
export const promptChips = document.querySelectorAll(".prompt-chip");

// State
export const dashboardState = {
  nodes: [],
  tasks: [],
  sessions: [],
  pairing: {
    status: null,
    targetNode: null,
    setupFocused: false,
    promptShown: false,
    dialogAutoOpened: false,
  },
  connection: {
    connected: false,
    lastSyncAt: null,
  },
  capacity: null,
  modelInventory: [],
  pipelinePlans: [],
  shardGroups: [],
  shardGroupsError: "",
  discovery: [],
  discoveryAuthentication: "unavailable",
  buildVersion: "",
  fullTaskHistoryLoaded: false,
  renderSignatures: {},
};
export const taskHistoryPage = {
  items: [],
  nextCursor: "",
  hasMore: false,
  loading: false,
  generation: 0,
  initialized: false,
  filterKey: "",
};
export const memoryHistoryPages = {
  facts: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  experiences: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  dialogues: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
};
export let activeSessionId = null;
export const activeSessionIds = new Map();
export let isSending = false;
export let activeAbortController = null;
export let terminalAbortController = null;
export let streamingAssistantEl = null;
export let currentAgentHarness = "pi";
export let agentSupportsCoordinatorModel = true;
export let activeChatMode = "direct";
export let directMessages = [];
export const modelPanelOpen = new Set();
export const pendingModelState = new Map();
export const pendingDefaultModel = new Map();
export const lastFetchedModels = new Map();
export let contextNode = null;
export const nodeProperties = new Map();
export let terminalNode = null;
export let terminalDialogueId = null;

export function setModelCatalogEntries(value) {
  modelCatalogEntries = value;
}

export function setCatalogEngines(value) {
  catalogEngines = value;
}

export function setCatalogModels(value) {
  catalogModels = value;
}

export function setActiveSessionId(value) {
  activeSessionId = value;
}

export function setIsSending(value) {
  isSending = value;
}

export function setActiveAbortController(value) {
  activeAbortController = value;
}

export function setTerminalAbortController(value) {
  terminalAbortController = value;
}

export function setStreamingAssistantEl(value) {
  streamingAssistantEl = value;
}

export function setCurrentAgentHarness(value) {
  currentAgentHarness = value;
}

export function setAgentSupportsCoordinatorModel(value) {
  agentSupportsCoordinatorModel = value;
}

export function setActiveChatMode(value) {
  activeChatMode = value;
}

export function setDirectMessages(value) {
  directMessages = value;
}

export function setContextNode(value) {
  contextNode = value;
}

export function setTerminalNode(value) {
  terminalNode = value;
}

export function setTerminalDialogueId(value) {
  terminalDialogueId = value;
}

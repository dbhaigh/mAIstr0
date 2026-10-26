// DOM Elements
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
const pairingFingerprint = document.getElementById("pairing-fingerprint");
const pairingFingerprintDisplay = document.getElementById("pairing-fingerprint-display");
const pairingInvitationFingerprint = document.getElementById("pairing-invitation-fingerprint");
const pairingConnectButton = document.getElementById("pairing-connect-button");
const pairingStatus = document.getElementById("pairing-status");
const pairingState = document.getElementById("pairing-state");
const pairingInvitationDialog = document.getElementById("pairing-invitation-dialog");
const pairingInvitationDetails = document.getElementById("pairing-invitation-details");
const pairingInvitationStatus = document.getElementById("pairing-invitation-status");
const pairingInvitationAcceptBtn = document.getElementById("pairing-invitation-accept");
const pairingInvitationDeclineBtn = document.getElementById("pairing-invitation-decline");

// Header stats & navigation
const statNodes = document.getElementById("stat-nodes");
const statModels = document.getElementById("stat-models");
const statLoad = document.getElementById("stat-load");
const statMemory = document.getElementById("stat-memory");
const statVersion = document.getElementById("stat-version");
const connectionStatus = document.getElementById("connection-status");
const persistenceWarning = document.getElementById("persistence-warning");
const navTabs = document.querySelectorAll(".nav-tab");
const tabContents = document.querySelectorAll(".tab-content");
const clusterWorkspaceTabs = document.querySelectorAll(".cluster-workspace-tab");
const clusterWindows = document.querySelectorAll(".cluster-window");
const overviewTabs = document.querySelectorAll(".overview-tab");
const overviewPanels = document.querySelectorAll("#cluster-overview-panel > .overview-column");
const setupTabs = document.querySelectorAll(".setup-tab");
const setupPanels = document.querySelectorAll("#setup-panel > .setup-panel-section");

// Memory & learning elements
const learningSummary = document.getElementById("learning-summary");
const factsList = document.getElementById("facts-list");
const factForm = document.getElementById("fact-form");
const factKey = document.getElementById("fact-key");
const factValue = document.getElementById("fact-value");
const experiencesList = document.getElementById("experiences-list");
const transcriptsList = document.getElementById("transcripts-list");
const memorySearch = document.getElementById("memory-search");

// Agent Console Elements
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

// State
const dashboardState = {
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
  buildVersion: "",
  fullTaskHistoryLoaded: false,
};
const taskHistoryPage = {
  items: [],
  nextCursor: "",
  hasMore: false,
  loading: false,
  generation: 0,
  initialized: false,
  filterKey: "",
};
const memoryHistoryPages = {
  facts: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  experiences: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
  dialogues: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
};
let activeSessionId = null;
const activeSessionIds = new Map();
let isSending = false;
let activeAbortController = null;
let terminalAbortController = null;
let streamingAssistantEl = null;
let currentAgentHarness = "pi";
let agentSupportsCoordinatorModel = true;
let activeChatMode = "direct";
let directMessages = [];
const modelPanelOpen = new Set();
const pendingModelState = new Map();
const pendingDefaultModel = new Map();
const lastFetchedModels = new Map();
let contextNode = null;
const nodeProperties = new Map();
let terminalNode = null;
let terminalDialogueId = null;

(() => {
  // src/shared/state.js
  var nodesList = document.getElementById("nodes-list");
  var routingList = document.getElementById("routing-list");
  var discoveryList = document.getElementById("discovery-list");
  var tasksList = document.getElementById("tasks-list");
  var taskHistorySearch = document.getElementById("task-history-search");
  var taskHistoryStatus = document.getElementById("task-history-status");
  var taskHistoryCount = document.getElementById("task-history-count");
  var taskInput = document.getElementById("task-input");
  var taskAutoPlan = document.getElementById("task-auto-plan");
  var taskRequireReview = document.getElementById("task-require-review");
  var taskRequireApproval = document.getElementById("task-require-approval");
  var submitBtn = document.getElementById("submit-btn");
  var submitStatus = document.getElementById("submit-status");
  var copyTasksBtn = document.getElementById("copy-tasks-btn");
  var refreshAllModelsBtn = document.getElementById("refresh-all-models-btn");
  var overviewActivity = document.getElementById("overview-activity");
  var overviewNodes = document.getElementById("overview-nodes");
  var clusterOverviewStatus = document.getElementById("cluster-overview-status");
  var clusterPeers = document.getElementById("cluster-peers");
  var setupCheckBtn = document.getElementById("setup-check-btn");
  var setupStatus = document.getElementById("setup-status");
  var setupResults = document.getElementById("setup-results");
  var setupNodeList = document.getElementById("setup-node-list");
  var setupNodeSummary = document.getElementById("setup-node-summary");
  var catalogNodeSelect = document.getElementById("catalog-node");
  var catalogEngineSelect = document.getElementById("catalog-engine");
  var catalogSearch = document.getElementById("catalog-search");
  var catalogStatus = document.getElementById("catalog-status");
  var catalogResults = document.getElementById("catalog-results");
  var modelCatalogEntries = [];
  var catalogEngines = [];
  var catalogModels = [];
  var pairingPinView = document.getElementById("pairing-pin-view");
  var pairingPin = document.getElementById("pairing-pin");
  var pairingPinExpiry = document.getElementById("pairing-pin-expiry");
  var pairingPinRegenerateBtn = document.getElementById("pairing-pin-regenerate-btn");
  var pairingRequireVerification = document.getElementById("pairing-require-verification");
  var pairingPolicyState = document.getElementById("pairing-policy-state");
  var pairingAddressDisplay = document.getElementById("pairing-address-display");
  var pairingInstructions = document.getElementById("pairing-instructions");
  var pairingNodeConnect = document.getElementById("pairing-node-connect");
  var pairingConnectForm = document.getElementById("pairing-connect-form");
  var pairingConnectDialog = document.getElementById("pairing-connect-dialog");
  var pairingConnectOpenBtn = document.getElementById("pairing-connect-open");
  var pairingConnectCancelBtn = document.getElementById("pairing-connect-cancel");
  var pairingAddress = document.getElementById("pairing-address");
  var pairingAddressField = document.getElementById("pairing-address-field");
  var pairingCode = document.getElementById("pairing-code");
  var pairingFingerprint = document.getElementById("pairing-fingerprint");
  var pairingFingerprintDisplay = document.getElementById("pairing-fingerprint-display");
  var pairingInvitationFingerprint = document.getElementById("pairing-invitation-fingerprint");
  var pairingConnectButton = document.getElementById("pairing-connect-button");
  var pairingStatus = document.getElementById("pairing-status");
  var pairingState = document.getElementById("pairing-state");
  var pairingInvitationDialog = document.getElementById("pairing-invitation-dialog");
  var pairingInvitationDetails = document.getElementById("pairing-invitation-details");
  var pairingInvitationStatus = document.getElementById("pairing-invitation-status");
  var pairingInvitationAcceptBtn = document.getElementById("pairing-invitation-accept");
  var pairingInvitationDeclineBtn = document.getElementById("pairing-invitation-decline");
  var statNodes = document.getElementById("stat-nodes");
  var statModels = document.getElementById("stat-models");
  var statLoad = document.getElementById("stat-load");
  var statMemory = document.getElementById("stat-memory");
  var statVersion = document.getElementById("stat-version");
  var connectionStatus = document.getElementById("connection-status");
  var persistenceWarning = document.getElementById("persistence-warning");
  var navTabs = document.querySelectorAll(".nav-tab");
  var tabContents = document.querySelectorAll(".tab-content");
  var clusterWorkspaceTabs = document.querySelectorAll(".cluster-workspace-tab");
  var clusterWindows = document.querySelectorAll(".cluster-window");
  var overviewTabs = document.querySelectorAll(".overview-tab");
  var overviewPanels = document.querySelectorAll("#cluster-overview-panel > .overview-column");
  var setupTabs = document.querySelectorAll(".setup-tab");
  var setupPanels = document.querySelectorAll("#setup-panel > .setup-panel-section");
  var learningSummary = document.getElementById("learning-summary");
  var factsList = document.getElementById("facts-list");
  var factForm = document.getElementById("fact-form");
  var factKey = document.getElementById("fact-key");
  var factValue = document.getElementById("fact-value");
  var experiencesList = document.getElementById("experiences-list");
  var transcriptsList = document.getElementById("transcripts-list");
  var memorySearch = document.getElementById("memory-search");
  var sessionListEl = document.getElementById("session-list");
  var newSessionBtn = document.getElementById("new-session-btn");
  var agentHarnessSelect = document.getElementById("agent-harness-select");
  var agentHarnessStatus = document.getElementById("agent-harness-status");
  var agentSidebar = document.querySelector(".agent-sidebar");
  var agentTabs = document.querySelectorAll(".agent-tab");
  var agentModelSelect = document.getElementById("agent-model-select");
  var agentStepsSelect = document.getElementById("agent-steps-select");
  var agentSettingsTitle = document.getElementById("agent-settings-title");
  var agentToolsList = document.getElementById("agent-tools-list");
  var clearDirectChatBtn = document.getElementById("clear-direct-chat-btn");
  var interruptMsgBtn = document.getElementById("interrupt-msg-btn");
  var nodeTerminalInterruptBtn = document.getElementById("node-terminal-interrupt");
  var chatMessagesEl = document.getElementById("chat-messages");
  var chatInput = document.getElementById("chat-input");
  var sendMsgBtn = document.getElementById("send-msg-btn");
  var chatStatus = document.getElementById("chat-status");
  var promptChips = document.querySelectorAll(".prompt-chip");
  var dashboardState = {
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
    connection: {
      connected: false,
      lastSyncAt: null
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
    renderSignatures: {}
  };
  var taskHistoryPage = {
    items: [],
    nextCursor: "",
    hasMore: false,
    loading: false,
    generation: 0,
    initialized: false,
    filterKey: ""
  };
  var memoryHistoryPages = {
    facts: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
    experiences: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" },
    dialogues: { items: [], nextCursor: "", hasMore: false, loading: false, generation: 0, url: "" }
  };
  var activeSessionId = null;
  var activeSessionIds = /* @__PURE__ */ new Map();
  var isSending = false;
  var activeAbortController = null;
  var terminalAbortController = null;
  var streamingAssistantEl = null;
  var currentAgentHarness = "pi";
  var agentSupportsCoordinatorModel = true;
  var activeChatMode = "direct";
  var directMessages = [];
  var modelPanelOpen = /* @__PURE__ */ new Set();
  var pendingModelState = /* @__PURE__ */ new Map();
  var pendingDefaultModel = /* @__PURE__ */ new Map();
  var lastFetchedModels = /* @__PURE__ */ new Map();
  var contextNode = null;
  var nodeProperties = /* @__PURE__ */ new Map();
  var terminalNode = null;
  var terminalDialogueId = null;
  function setModelCatalogEntries(value) {
    modelCatalogEntries = value;
  }
  function setCatalogEngines(value) {
    catalogEngines = value;
  }
  function setCatalogModels(value) {
    catalogModels = value;
  }
  function setActiveSessionId(value) {
    activeSessionId = value;
  }
  function setIsSending(value) {
    isSending = value;
  }
  function setActiveAbortController(value) {
    activeAbortController = value;
  }
  function setTerminalAbortController(value) {
    terminalAbortController = value;
  }
  function setStreamingAssistantEl(value) {
    streamingAssistantEl = value;
  }
  function setCurrentAgentHarness(value) {
    currentAgentHarness = value;
  }
  function setAgentSupportsCoordinatorModel(value) {
    agentSupportsCoordinatorModel = value;
  }
  function setActiveChatMode(value) {
    activeChatMode = value;
  }
  function setDirectMessages(value) {
    directMessages = value;
  }
  function setContextNode(value) {
    contextNode = value;
  }
  function setTerminalNode(value) {
    terminalNode = value;
  }
  function setTerminalDialogueId(value) {
    terminalDialogueId = value;
  }

  // src/shared/theme.js
  var THEME_STORAGE_KEY = "maistr0-theme";
  var themeToggle = document.getElementById("theme-toggle");
  var root = document.documentElement;
  function setDashboardTheme(theme, persist = false) {
    const selectedTheme = theme === "light" ? "light" : "dark";
    root.dataset.theme = selectedTheme;
    if (themeToggle) {
      const nextTheme = selectedTheme === "light" ? "dark" : "light";
      themeToggle.textContent = `${nextTheme[0].toUpperCase()}${nextTheme.slice(1)} mode`;
      themeToggle.setAttribute("aria-label", `Switch to ${nextTheme} mode`);
      themeToggle.setAttribute("aria-pressed", String(selectedTheme === "light"));
      themeToggle.title = `Switch to ${nextTheme} mode`;
    }
    if (persist) {
      try {
        localStorage.setItem(THEME_STORAGE_KEY, selectedTheme);
      } catch (error) {
        console.warn("Could not save dashboard theme preference.", error);
      }
    }
  }
  var savedTheme = "dark";
  try {
    if (localStorage.getItem(THEME_STORAGE_KEY) === "light") savedTheme = "light";
  } catch (error) {
    console.warn("Could not read dashboard theme preference.", error);
  }
  setDashboardTheme(savedTheme);
  themeToggle?.addEventListener("click", () => {
    setDashboardTheme(root.dataset.theme === "light" ? "dark" : "light", true);
  });

  // src/shared/view-model.js
  var dashboardViewModel = (() => {
    function engineInventoryState(engine) {
      const knownStates = ["unknown", "available", "empty", "error"];
      if (knownStates.includes(engine?.inventory_state)) return engine.inventory_state;
      if (!engine?.last_checked_at) return "unknown";
      if (!engine.ready) return "error";
      return Number(engine.model_count) > 0 ? "available" : "empty";
    }
    function engineInventoryLabel(state) {
      switch (state) {
        case "available":
          return "Inventory available";
        case "empty":
          return "Reachable · no models";
        case "error":
          return "Check failed";
        default:
          return "Not checked";
      }
    }
    function formatNodeHardware(node) {
      const hardware = node.hardware || {};
      const parts = [];
      if (hardware.os || hardware.arch) parts.push([hardware.os, hardware.arch].filter(Boolean).join("/"));
      if (hardware.cpu_cores) parts.push(`${hardware.cpu_cores} CPU cores`);
      if (hardware.total_ram_mb) parts.push(`${(hardware.total_ram_mb / 1024).toFixed(1)} GB RAM`);
      if (hardware.has_gpu) {
        parts.push(`${hardware.gpu_vendor || "GPU"}${hardware.gpu_stats_available && Number.isFinite(hardware.gpu_utilization_percent) ? ` · ${hardware.gpu_utilization_percent}% busy` : ""}`);
      }
      if (hardware.has_npu) {
        parts.push(`${hardware.npu_vendor || "NPU"}${hardware.npu_stats_available && Number.isFinite(hardware.npu_utilization_percent) ? ` · ${hardware.npu_utilization_percent}% busy` : ""}`);
      }
      return parts.length ? parts.join(" · ") : "Hardware details unavailable";
    }
    function dashboardStatusViewModel(snapshot, nodes, capacity) {
      const nodeCount = snapshot.member_count ?? nodes.filter((node) => node.healthy).length;
      let modelCount = 0;
      let activeLoad = 0;
      if (Array.isArray(capacity?.nodes)) {
        for (const node of capacity.nodes) {
          if (!node.healthy) continue;
          activeLoad += Number(node.active) || 0;
        }
      } else {
        for (const node of nodes) {
          if (!node.healthy) continue;
          activeLoad += node.active_tasks || 0;
        }
      }
      for (const node of nodes) {
        if (!node.healthy) continue;
        modelCount += (node.models || []).length;
      }
      const dispatch = capacity?.global_dispatch;
      return {
        nodeCount,
        modelCount,
        activeLoad,
        loadLabel: dispatch ? `Active Load: ${activeLoad} · Dispatch ${dispatch.active_slots}/${dispatch.global_limit} · Queue ${dispatch.queue_depth}` : `Active Load: ${activeLoad}`,
        memoryLabel: snapshot.memory ? `Memory: ${snapshot.memory.total_experiences || 0} · ${Math.round((snapshot.memory.success_rate || 0) * 100)}%` : null,
        buildVersion: snapshot.build_version || null
      };
    }
    function taskHistoryCountLabel(visibleCount, totalCount) {
      if (totalCount === 0) return "0 tasks";
      return `Showing ${visibleCount} of ${totalCount} ${totalCount === 1 ? "task" : "tasks"}`;
    }
    function pairingPINViewModel(status) {
      const hasPIN = Boolean(status?.pin);
      const hasActivePIN = hasPIN || Boolean(status?.expires_at);
      return {
        visible: status?.role === "orchestrator",
        hasPIN,
        hasActivePIN,
        pinText: status?.pin || (hasActivePIN ? "Hidden on this connection" : "Expired"),
        regenerateText: hasPIN ? "Generate / replace pairing PIN" : hasActivePIN ? "Generate / reveal pairing PIN" : "Generate pairing PIN",
        expiryText: status?.expires_at ? `Expires ${new Date(status.expires_at).toLocaleTimeString()}. The PIN can be used once.` : "No active PIN. Generate a new PIN to pair another node.",
        stateText: hasPIN ? "Share the PIN and HTTPS address only with the node you intend to add." : hasActivePIN ? "An active PIN is hidden on this connection. Generate a replacement to reveal it locally." : "No active PIN. Generate one to add or re-pair a node."
      };
    }
    return Object.freeze({
      dashboardStatusViewModel,
      engineInventoryLabel,
      engineInventoryState,
      formatNodeHardware,
      pairingPINViewModel,
      taskHistoryCountLabel
    });
  })();

  // src/shared/snapshot.js
  var dashboardSnapshot = ((root2) => {
    function hashSnapshot(serialized) {
      let hash = 2166136261;
      for (let i = 0; i < serialized.length; i++) {
        hash ^= serialized.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
      }
      return hash >>> 0;
    }
    function createSnapshotScheduler(onSnapshot, options = {}) {
      const requestFrame = options.requestAnimationFrameImpl || root2.requestAnimationFrame?.bind(root2) || ((callback) => root2.setTimeout(callback, 0));
      const cancelFrame = options.cancelAnimationFrameImpl || root2.cancelAnimationFrame?.bind(root2) || root2.clearTimeout?.bind(root2);
      let pending = null;
      let lastHash = null;
      let lastSerialized = null;
      let scheduled = false;
      let frameID = null;
      const flush = () => {
        scheduled = false;
        frameID = null;
        const update = pending;
        pending = null;
        if (!update || update.hash === lastHash && update.serialized === lastSerialized) return;
        lastHash = update.hash;
        lastSerialized = update.serialized;
        onSnapshot(update.snapshot);
      };
      return Object.freeze({
        enqueue(snapshot) {
          const serialized = JSON.stringify(snapshot);
          const hash = hashSnapshot(serialized);
          if (hash === lastHash && serialized === lastSerialized || pending && hash === pending.hash && serialized === pending.serialized) return false;
          pending = { snapshot, serialized, hash };
          if (!scheduled) {
            scheduled = true;
            frameID = requestFrame(flush);
          }
          return true;
        },
        cancel() {
          if (scheduled && cancelFrame && frameID !== null) cancelFrame(frameID);
          pending = null;
          scheduled = false;
          frameID = null;
        },
        flush
      });
    }
    function mergeTaskSnapshot(currentTasks, incomingTasks, complete, fullTaskHistoryLoaded) {
      const tasks = incomingTasks || [];
      if (complete) return { tasks, fullTaskHistoryLoaded: true };
      if (!fullTaskHistoryLoaded) return { tasks, fullTaskHistoryLoaded: false };
      const merged = new Map((currentTasks || []).map((task) => [task.id, task]));
      for (const task of tasks) merged.set(task.id, task);
      return {
        tasks: [...merged.values()].sort(
          (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
        ),
        fullTaskHistoryLoaded: true
      };
    }
    function connectDashboardEvents(onSnapshot, options = {}) {
      const EventSourceImpl = options.EventSourceImpl || root2.EventSource;
      const onError = options.onError || ((error) => root2.console.error(error));
      const onConnectionChange = options.onConnectionChange || (() => {
      });
      const scheduler = createSnapshotScheduler((snapshot) => {
        try {
          onSnapshot(snapshot);
        } catch (error) {
          onError(error);
        }
      }, options);
      const eventSource = new EventSourceImpl("/api/events");
      eventSource.onopen = () => onConnectionChange(true);
      eventSource.onerror = () => onConnectionChange(false);
      eventSource.onmessage = (event) => {
        try {
          scheduler.enqueue(JSON.parse(event.data));
        } catch (error) {
          onError(error);
        }
      };
      return eventSource;
    }
    return Object.freeze({ connectDashboardEvents, createSnapshotScheduler, hashSnapshot, mergeTaskSnapshot });
  })(globalThis);

  // src/shared/dom.js
  var dashboardDOM = ((root2) => {
    const keyedState = /* @__PURE__ */ new WeakMap();
    const virtualState = /* @__PURE__ */ new WeakMap();
    function patchKeyedChildren(container, items, options) {
      if (!container || !options || typeof options.renderItem !== "function") return;
      const keyOf = options.keyOf || ((item) => item.id);
      const signatureOf = options.signatureOf || JSON.stringify;
      const previous = keyedState.get(container) || /* @__PURE__ */ new Map();
      const desired = new Set(items.map((item, index) => String(keyOf(item, index))));
      for (const [key, entry] of previous) {
        if (!desired.has(key)) {
          entry.nodes.forEach((node) => node.remove());
          previous.delete(key);
        }
      }
      let cursor = container.firstElementChild;
      const next = /* @__PURE__ */ new Map();
      for (const [index, item] of items.entries()) {
        const key = String(keyOf(item, index));
        const signature = signatureOf(item, index);
        let entry = previous.get(key);
        if (entry && entry.signature !== signature) {
          while (cursor && entry.nodes.includes(cursor)) cursor = cursor.nextElementSibling;
          entry.nodes.forEach((node) => node.remove());
          entry = null;
        }
        if (!entry) {
          const rendered = options.renderItem(item, index);
          const nodes = Array.isArray(rendered) ? rendered : [rendered];
          for (const node of nodes) {
            node.dataset.renderKey = key;
            container.insertBefore(node, cursor);
          }
          entry = { signature, nodes };
        } else {
          for (const node of entry.nodes) {
            if (node !== cursor) container.insertBefore(node, cursor);
            cursor = node.nextElementSibling;
          }
        }
        if (!entry.nodes.length) continue;
        if (entry.nodes[0].parentNode === container) {
          cursor = entry.nodes[entry.nodes.length - 1].nextElementSibling;
        }
        next.set(key, entry);
      }
      keyedState.set(container, next);
    }
    function getVirtualRange(items, scrollTop, viewportHeight, options = {}) {
      const estimate = Math.max(1, options.estimateHeight || 96);
      const overscan = Math.max(0, options.overscan ?? 6);
      const heightOf = options.heightOf || (() => estimate);
      if (!items.length) return { start: 0, end: 0, top: 0, bottom: 0 };
      const offsets = new Array(items.length + 1);
      offsets[0] = 0;
      for (let i = 0; i < items.length; i++) {
        offsets[i + 1] = offsets[i] + Math.max(1, heightOf(items[i], i) || estimate);
      }
      const findIndex = (offset) => {
        let low = 0;
        let high = items.length;
        while (low < high) {
          const middle = low + high >>> 1;
          if (offsets[middle + 1] <= offset) low = middle + 1;
          else high = middle;
        }
        return low;
      };
      const first = findIndex(Math.max(0, scrollTop));
      const last = findIndex(Math.max(0, scrollTop) + Math.max(1, viewportHeight));
      const start = Math.max(0, first - overscan);
      const end = Math.min(items.length, last + overscan + 1);
      return {
        start,
        end,
        top: offsets[start],
        bottom: offsets[items.length] - offsets[end]
      };
    }
    function renderVirtualList(container, items, options) {
      if (!container || !options || typeof options.renderItem !== "function") return;
      let state = virtualState.get(container);
      if (!state) {
        const top = document.createElement("div");
        const content = document.createElement("div");
        const bottom = document.createElement("div");
        top.className = "virtual-list-spacer virtual-list-spacer-top";
        bottom.className = "virtual-list-spacer virtual-list-spacer-bottom";
        content.className = "virtual-list-items";
        top.setAttribute("aria-hidden", "true");
        bottom.setAttribute("aria-hidden", "true");
        container.replaceChildren(top, content, bottom);
        container.classList.add("virtual-list-container");
        state = { top, content, bottom, items: [], options, frame: 0, heights: /* @__PURE__ */ new Map(), observer: null };
        state.onScroll = () => {
          if (state.frame) return;
          const raf = root2.requestAnimationFrame || ((callback) => root2.setTimeout(callback, 16));
          state.frame = raf(() => {
            state.frame = 0;
            renderVirtualList(container, state.items, state.options);
          });
        };
        container.addEventListener("scroll", state.onScroll, { passive: true });
        if (typeof root2.ResizeObserver === "function") {
          state.observer = new root2.ResizeObserver((entries) => {
            let changed = false;
            for (const entry of entries) {
              const key = entry.target.dataset.renderKey;
              const height = entry.contentRect.height;
              if (key && Number.isFinite(height) && height > 0 && Math.abs((state.heights.get(key) || 0) - height) > 1) {
                state.heights.set(key, height);
                changed = true;
              }
            }
            if (changed) state.onScroll();
          });
        }
        virtualState.set(container, state);
      }
      container.setAttribute("aria-busy", "false");
      state.items = items;
      state.options = options;
      const keyOf = options.keyOf || ((item, index) => item.id ?? index);
      const heightOf = (item, index) => state.heights.get(String(keyOf(item, index))) || options.estimateHeight || 96;
      const threshold = options.threshold || 40;
      const range = items.length > threshold ? getVirtualRange(items, container.scrollTop, container.clientHeight || 600, {
        estimateHeight: options.estimateHeight,
        overscan: options.overscan,
        heightOf
      }) : { start: 0, end: items.length, top: 0, bottom: 0 };
      const visible = items.slice(range.start, range.end);
      const signatureOf = options.signatureOf || JSON.stringify;
      state.top.style.height = `${range.top}px`;
      state.bottom.style.height = `${range.bottom}px`;
      state.top.hidden = range.top === 0;
      state.bottom.hidden = range.bottom === 0;
      patchKeyedChildren(state.content, visible, {
        keyOf: (item, index) => keyOf(item, range.start + index),
        signatureOf,
        renderItem: (item, index) => options.renderItem(item, range.start + index)
      });
      state.observer?.disconnect();
      state.content.querySelectorAll(":scope > [data-render-key]").forEach((node) => state.observer?.observe(node));
    }
    function resetVirtualList(container, options = {}) {
      if (!container) return;
      const state = virtualState.get(container);
      if (state) {
        state.items = [];
        state.options = { ...options, keyOf: (node) => node.dataset.virtualKey, renderItem: (node) => node };
        state.heights.clear();
        container.scrollTop = 0;
        renderVirtualList(container, [], state.options);
        return;
      }
      container.scrollTop = 0;
      renderVirtualList(container, [], {
        ...options,
        keyOf: (node) => node.dataset.virtualKey,
        signatureOf: (node) => node.dataset.virtualSignature || "",
        renderItem: (node) => node
      });
    }
    function appendVirtualListItem(container, node) {
      let state = virtualState.get(container);
      if (!state) {
        resetVirtualList(container);
        state = virtualState.get(container);
      }
      if (!node.dataset.virtualKey) node.dataset.virtualKey = `virtual-${state.items.length}`;
      state.items = [...state.items, node];
      const wasAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 48;
      renderVirtualList(container, state.items, state.options);
      if (wasAtBottom) container.scrollTop = container.scrollHeight;
    }
    function removeVirtualListItem(container, node) {
      const state = virtualState.get(container);
      if (!state) {
        node.remove();
        return;
      }
      state.items = state.items.filter((item) => item !== node);
      state.heights.delete(node.dataset.virtualKey);
      renderVirtualList(container, state.items, state.options);
    }
    function renderSkeleton(container, count = 3) {
      if (!container) return;
      const state = virtualState.get(container);
      if (state) {
        state.observer?.disconnect();
        if (state.frame) {
          const cancel = root2.cancelAnimationFrame || root2.clearTimeout;
          cancel?.call(root2, state.frame);
        }
        container.removeEventListener("scroll", state.onScroll);
        virtualState.delete(container);
        keyedState.delete(state.content);
      }
      container.setAttribute("aria-busy", "true");
      const placeholders = Array.from({ length: count }, (_, index) => {
        const card = document.createElement("div");
        card.className = "skeleton-card";
        card.setAttribute("aria-hidden", "true");
        card.innerHTML = `<span class="skeleton skeleton-heading"></span><span class="skeleton"></span><span class="skeleton skeleton-short"></span>`;
        card.dataset.skeletonIndex = String(index);
        return card;
      });
      container.replaceChildren(...placeholders);
    }
    return Object.freeze({
      appendVirtualListItem,
      getVirtualRange,
      patchKeyedChildren,
      renderSkeleton,
      removeVirtualListItem,
      renderVirtualList,
      resetVirtualList
    });
  })(globalThis);

  // src/shared/format.js
  var dashboardFormat = (() => {
    function escapeHtml2(value) {
      return String(value ?? "").replace(/[&<>"']/g, (character) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      })[character]);
    }
    function formatBytes2(bytes) {
      const value = Number(bytes);
      if (!Number.isFinite(value) || value < 0) return "0 B";
      if (value < 1024) return `${value} B`;
      if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
      return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
    }
    function formatRelativeTime2(value) {
      if (!value) return "time unavailable";
      const timestamp = new Date(value).getTime();
      if (!Number.isFinite(timestamp)) return "time unavailable";
      const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1e3));
      if (seconds < 60) return `${seconds}s ago`;
      if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
      if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
      return new Date(timestamp).toLocaleDateString();
    }
    function formatTimestamp2(label, value) {
      if (!value) return `<span class="task-time-unavailable">${escapeHtml2(label)} unavailable</span>`;
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return `<span class="task-time-unavailable">${escapeHtml2(label)} unavailable</span>`;
      }
      return `<time datetime="${escapeHtml2(value)}">${escapeHtml2(label)} ${escapeHtml2(date.toLocaleString())}</time>`;
    }
    return Object.freeze({ escapeHtml: escapeHtml2, formatBytes: formatBytes2, formatRelativeTime: formatRelativeTime2, formatTimestamp: formatTimestamp2 });
  })();
  var { escapeHtml, formatBytes, formatRelativeTime, formatTimestamp } = dashboardFormat;

  // src/views/orchestrator-sessions.js
  async function loadAgentTools() {
    try {
      const tools = await fetchJSON("/api/agent/tools");
      if (!agentToolsList) return;
      agentToolsList.innerHTML = "";
      for (const t of tools) {
        const div = document.createElement("div");
        div.className = "tool-pill";
        div.innerHTML = `
        <div class="tool-pill-name">⚡ ${escapeHtml(t.name)}</div>
        <div class="tool-pill-desc">${escapeHtml(t.description)}</div>
      `;
        agentToolsList.appendChild(div);
      }
    } catch (err) {
      console.error("failed to load agent tools", err);
    }
  }
  async function loadAgentBackend() {
    try {
      applyAgentBackend(await fetchJSON("/api/agent/backend"));
    } catch (err) {
      console.error("failed to load agent backend", err);
      if (agentHarnessStatus) agentHarnessStatus.textContent = `Could not load harness: ${err.message}`;
    }
  }
  function applyAgentBackend(backend) {
    setCurrentAgentHarness(backend.name);
    setAgentSupportsCoordinatorModel(Boolean(backend.supports_coordinator_model));
    const mode = activeChatMode === "direct" ? "direct" : backend.name;
    setActiveChatMode(mode);
    agentTabs.forEach((tab) => {
      const active = tab.dataset.agent === mode;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", String(active));
    });
    if (agentHarnessSelect) agentHarnessSelect.value = backend.name;
    if (agentSettingsTitle) {
      agentSettingsTitle.textContent = `${backend.name === "pi" ? "Pi" : "DeepSeek"} Agent Settings`;
    }
    const modelLabel = agentModelSelect?.closest("label");
    const stepsLabel = agentStepsSelect?.closest("label");
    if (modelLabel) modelLabel.hidden = !backend.supports_coordinator_model;
    if (stepsLabel) stepsLabel.hidden = !backend.supports_coordinator_model;
    if (agentHarnessStatus) {
      agentHarnessStatus.textContent = `New conversations use ${backend.name === "pi" ? "Pi" : "DeepSeek"}; existing conversations stay on their harness.`;
    }
    setAgentConsoleMode(mode);
  }
  agentHarnessSelect?.addEventListener("change", async () => {
    const requested = agentHarnessSelect.value;
    await activateAgentTab(requested);
  });
  var clusterModelsLoaded = false;
  async function loadClusterModels() {
    if (clusterModelsLoaded) return;
    try {
      const models = await fetchJSON("/api/agent/models");
      if (!agentModelSelect) return;
      const currentVal = agentModelSelect.value;
      agentModelSelect.innerHTML = '<option value="">Auto (Fastest / Cluster Leader)</option>';
      const seen = /* @__PURE__ */ new Set();
      for (const m of models) {
        if (seen.has(m.name)) continue;
        seen.add(m.name);
        const opt = document.createElement("option");
        opt.value = m.name;
        opt.textContent = `${m.name} (${(m.tags || []).join(", ")}) · on ${m.node_id}`;
        if (m.name === currentVal) opt.selected = true;
        agentModelSelect.appendChild(opt);
      }
      clusterModelsLoaded = true;
    } catch (err) {
      console.error("failed to load cluster models", err);
    }
  }
  async function loadSessions() {
    try {
      dashboardState.sessions = await fetchJSON("/api/agent/sessions");
      renderSessionList();
      if (activeChatMode === "direct") return;
      const sessions = sessionsForHarness(currentAgentHarness);
      const preferredID = activeSessionIds.get(currentAgentHarness);
      const sessionID = sessions.some((session) => session.id === preferredID) ? preferredID : sessions[0]?.id;
      if (sessionID) {
        await selectSession(sessionID);
      } else {
        await createNewSession("New Interactive Session");
      }
    } catch (err) {
      console.error("failed to load sessions", err);
    }
  }
  function sessionsForHarness(harness) {
    return dashboardState.sessions.filter((session) => (session.harness || "pi") === harness);
  }
  function renderSessionList() {
    if (!sessionListEl) return;
    sessionListEl.innerHTML = "";
    for (const s of sessionsForHarness(currentAgentHarness)) {
      const div = document.createElement("div");
      div.className = "session-item" + (s.id === activeSessionId ? " active" : "");
      div.setAttribute("role", "button");
      div.tabIndex = 0;
      div.setAttribute("aria-current", String(s.id === activeSessionId));
      div.innerHTML = `
      <span class="session-info">
        <span class="session-title">${escapeHtml(s.title || "Session " + s.id.slice(-4))}</span>
        <small class="session-harness">${s.harness === "deepseek" ? "DeepSeek" : "Pi"}</small>
      </span>
      <button class="session-del-btn" data-del="${s.id}" title="Delete session">&times;</button>
    `;
      div.addEventListener("click", (e) => {
        if (isSending || e.target.dataset.del) return;
        selectSession(s.id);
      });
      div.addEventListener("keydown", (e) => {
        if (!isSending && (e.key === "Enter" || e.key === " ")) {
          e.preventDefault();
          selectSession(s.id);
        }
      });
      div.querySelector(".session-del-btn")?.addEventListener("click", async (e) => {
        e.stopPropagation();
        if (!isSending) await deleteSession(s.id);
      });
      sessionListEl.appendChild(div);
    }
  }
  async function selectSession(id) {
    setActiveSessionId(id);
    const sessionInfo = dashboardState.sessions.find((session) => session.id === id);
    if (sessionInfo) activeSessionIds.set(sessionInfo.harness || "pi", id);
    renderSessionList();
    try {
      const session = await fetchJSON(`/api/agent/sessions/${encodeURIComponent(id)}`);
      if (activeSessionId === id) renderChatMessages(session);
    } catch (err) {
      console.error("failed to load session", id, err);
    }
  }
  async function createNewSession(title = "") {
    if (isSending) return;
    try {
      const session = await fetchJSON("/api/agent/sessions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: title || `Session ${(/* @__PURE__ */ new Date()).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`,
          coordinator_model: agentModelSelect?.value || "",
          max_steps: parseInt(agentStepsSelect?.value || "8", 10)
        })
      });
      dashboardState.sessions.unshift(session);
      activeSessionIds.set(currentAgentHarness, session.id);
      await selectSession(session.id);
    } catch (err) {
      console.error("failed to create session", err);
    }
  }
  async function deleteSession(id) {
    try {
      await fetch(`/api/agent/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
      dashboardState.sessions = dashboardState.sessions.filter((s) => s.id !== id);
      if (activeSessionId === id) {
        const sessions = sessionsForHarness(currentAgentHarness);
        setActiveSessionId(sessions[0]?.id || null);
        if (activeSessionId) activeSessionIds.set(currentAgentHarness, activeSessionId);
        else activeSessionIds.delete(currentAgentHarness);
        if (activeSessionId) {
          await selectSession(activeSessionId);
        } else {
          await createNewSession();
        }
      } else {
        renderSessionList();
      }
    } catch (err) {
      console.error("failed to delete session", err);
    }
  }
  function renderChatMessages(session) {
    if (!chatMessagesEl) return;
    dashboardDOM.resetVirtualList(chatMessagesEl, { estimateHeight: 140, threshold: 50, overscan: 6 });
    if (!session || !session.messages || session.messages.length === 0) {
      const welcome = document.createElement("div");
      welcome.className = "welcome-card";
      welcome.innerHTML = "<h2>What would you like to orchestrate?</h2><p>Ask a question or give a task to your local cluster.</p>";
      dashboardDOM.appendVirtualListItem(chatMessagesEl, welcome);
      return;
    }
    for (const msg of session.messages) {
      appendMessageToChat(msg);
    }
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  }

  // src/views/status-questions.js
  function isClusterStatusQuestion(text) {
    const query = text.toLowerCase();
    const subject = /\b(cluster|nodes?|workers?|orchestrators?|peers?|engines?|models?|llms?|gpus?|npus?|cpus?|ram|vram|memory|hardware|capacity|telemetry|shards?|pipelines?|tasks?|queues?|versions?|leader|leadership|maistr0|network)\b/.test(query);
    const read = /^(?:(?:hi|hello|please)[,.! ]+)*(?:what|which|who|where|when|why|how|is|are|do|does|can|could|would|has|have|show|list|tell|check|inspect|describe|compare|report|status|cluster)\b/.test(query.trim()) || /\b(status|health|inventory|available|online|offline|running|common|shared|same)\b/.test(query);
    const capability = /^(?:what|which|can|could|does|do|is|are)\b/.test(query.trim()) && (/\b(enough|support|capable|able)\b/.test(query) || /\b(?:can|could)\s+(?:(?:the|this|my|our)\s+)?(?:cluster|nodes?|workers?|hardware|gpus?|npus?|cpus?|i)\b/.test(query));
    const mutation = /\b(install|uninstall|download|pull|delete|remove|restart|reboot|stop|start|kill|deploy|configure|change|update|upgrade|enable|disable|create|build|write|implement|schedule|dispatch|benchmark|test|fix|repair|optimize|optimise)\b/.test(query) || /\brun\b/.test(query) && !capability;
    return subject && read && !mutation;
  }
  async function answerClusterStatusQuestion(nodes) {
    const sources = [
      ["/api/cluster/health", "Capacity and telemetry"],
      ["/api/status", "Orchestrator status"],
      ["/api/shards", "Shard groups"],
      ["/api/tasks?page_size=50", "Recent tasks"],
      ["/api/version", "Build version"]
    ];
    const results = await Promise.allSettled(sources.map(([url]) => fetchJSON(url)));
    const data = {};
    const errors = [];
    results.forEach((result, index) => {
      const [url, label] = sources[index];
      if (result.status === "fulfilled") {
        if (url.startsWith("/api/tasks") && Array.isArray(result.value)) {
          data[url] = { items: result.value.slice(0, 50), has_more: result.value.length > 50 };
        } else if (!result.value || typeof result.value !== "object" || Array.isArray(result.value)) {
          errors.push(`${label} unavailable: invalid API response`);
        } else {
          data[url] = result.value;
        }
      } else {
        errors.push(`${label} unavailable: ${result.reason.message}`);
      }
    });
    return formatClusterStatusAnswer(nodes, data, errors);
  }
  function formatClusterStatusAnswer(nodes, data, errors = []) {
    const health = data["/api/cluster/health"];
    const status = data["/api/status"];
    const shardList = data["/api/shards"];
    const taskPage = data["/api/tasks?page_size=50"];
    const online = nodes.filter((node) => node.healthy);
    const offline = nodes.filter((node) => !node.healthy);
    const sorted = [...nodes].sort((a, b) => a.id.localeCompare(b.id));
    const number = (value, suffix = "") => typeof value === "number" && Number.isFinite(value) ? `${value}${suffix}` : "unknown";
    const utilization = (available, value) => available ? number(value ?? 0, "%") : "unknown";
    const lines = [
      "Live cluster status (read-only; no LLM planner or task dispatch).",
      "These are reported snapshots, not fresh engine probes. Model names identify advertised models, not verified identical weights or revisions.",
      `Nodes: ${online.length} online, ${offline.length} offline, ${nodes.length} registered.`
    ];
    const modelNames = (node) => new Set((node.models || []).map((model) => model.name).filter(Boolean));
    const inventoryUnknown = (node) => !node.healthy || (node.engines || []).some((engine) => !engine.ready || !["available", "empty"].includes(dashboardViewModel.engineInventoryState(engine)));
    const intersection = (members) => {
      if (!members.length) return [];
      return [...modelNames(members[0])].filter((name) => members.every((node) => modelNames(node).has(name))).sort();
    };
    const common = intersection(online);
    if (!nodes.length) {
      lines.push("No registered nodes; common models cannot be determined.");
    } else if (nodes.some(inventoryUnknown)) {
      lines.push("Models available on ALL registered nodes: unknown; at least one node is offline or has an unavailable engine/inventory.");
    } else {
      lines.push(`Models reported on ALL ${nodes.length} registered nodes: ${common.length ? common.join(", ") : "none"}.`);
    }
    if (online.length) {
      lines.push(`Common advertised model names across ${online.length} online node(s): ${common.length ? common.join(", ") : "none"}.`);
      if (online.some(inventoryUnknown)) lines.push("Online model inventory is incomplete; common names above are not a readiness guarantee.");
      if (online.length === 1) lines.push("Only one node is online; this does not establish multi-node model availability.");
    }
    const capacity = new Map((health?.nodes || []).map((node) => [node.node_id, node]));
    for (const node of sorted) {
      const hardware = node.hardware || {};
      const load = capacity.get(node.id);
      lines.push("", `${node.id}: ${node.healthy ? "online" : "OFFLINE"}${node.leader ? ", leader" : ""}; version ${node.version || "unknown"}.`);
      lines.push(`  Address: ${node.address || "unknown"}; OS: ${hardware.os || "unknown"}/${hardware.arch || "unknown"}.`);
      lines.push(`  Last seen: ${node.last_seen || "unknown"}; telemetry: ${load?.telemetry_state || "unknown"}; sampled at ${node.telemetry_at || "unknown"}.`);
      lines.push(`  CPU: ${number(hardware.cpu_cores)} cores; utilization: ${utilization(hardware.cpu_stats_available, hardware.cpu_utilization_percent)}.`);
      lines.push(`  RAM: ${number(hardware.total_ram_mb, " MiB")}; utilization: ${utilization(hardware.memory_stats_available, hardware.memory_usage_percent)}.`);
      lines.push(`  GPU: ${hardware.has_gpu ? hardware.gpu_vendor || "detected" : "not reported"}; utilization: ${utilization(hardware.gpu_stats_available, hardware.gpu_utilization_percent)}; free memory: ${number(hardware.gpu_memory_free_mb, " MiB")}.`);
      lines.push(`  NPU: ${hardware.has_npu ? hardware.npu_vendor || "detected" : "not reported"}; utilization: ${utilization(hardware.npu_stats_available, hardware.npu_utilization_percent)}.`);
      for (const device of hardware.devices || []) {
        lines.push(`  Device ${device.kind}:${device.index} ${device.name || device.vendor || ""}: memory ${number(device.memory_mb, " MiB")}, free ${number(device.free_memory_mb, " MiB")} (omitted memory is unknown).`);
      }
      for (const link of hardware.network_links || []) {
        lines.push(`  Network ${link.name}: ${number(link.speed_mbps, " Mbps")}; trusted ${link.trusted === true ? "yes" : "not reported"}; RDMA ${link.rdma === true ? "yes" : "not reported"}.`);
      }
      lines.push(`  Tasks: ${number(node.active_tasks)} active; capacity: ${number(load?.remaining)} remaining / ${number(load?.max)} slots.`);
      lines.push(`  Default model: ${node.default_model || "not set"}.`);
      const models = [...modelNames(node)].sort();
      lines.push(`  ${inventoryUnknown(node) ? "Last reported models (availability unconfirmed)" : "Reported models"}: ${models.length ? models.join(", ") : "none reported"}.`);
      const engines = node.engines || [];
      lines.push(`  Engines: ${engines.length ? engines.map((engine) => `${engine.name}: ${engine.ready ? "ready" : "not ready"}, inventory ${dashboardViewModel.engineInventoryState(engine)}${engine.last_error ? `, error ${engine.last_error}` : ""}`).join("; ") : node.version ? "none configured or detected" : "readiness not reported (legacy node)"}.`);
      if (node.version_error) lines.push(`  Version warning: ${node.version_error}`);
    }
    if (status) {
      lines.push("", `Orchestrator: ${status.role || "unknown role"}; mode ${status.mode || "full"}; version ${data["/api/version"]?.version || "unknown"}.`);
      lines.push(`Persistence: ${status.persistence?.available === true ? "available" : status.persistence?.available === false ? "unavailable" : "not reported"}${status.persistence?.warning ? `; ${status.persistence.warning}` : ""}.`);
    }
    if (health) {
      const dispatch = health.global_dispatch || {};
      lines.push("", `Dispatch: ${number(dispatch.active_slots)} active slots / ${number(dispatch.global_limit)} limit; ${number(dispatch.queue_depth)} queued; ${number(dispatch.active_workers)} active workers.`);
      const engines = health.engine_readiness || {};
      lines.push(`Engine readiness: ${number(engines.ready_engines)} / ${number(engines.total_engines)} ready.`);
      const telemetry = health.telemetry_freshness || {};
      lines.push(`Telemetry: ${number(telemetry.fresh_nodes)} fresh, ${number(telemetry.stale_nodes)} stale, ${number(telemetry.unavailable_nodes)} unavailable.`);
      for (const warning of health.warnings || []) lines.push(`Warning: ${warning}`);
    }
    if (Array.isArray(shardList?.groups)) {
      lines.push("", `Shard groups: ${shardList.groups.length}.`);
      for (const group of shardList.groups) {
        lines.push(`  ${group.id}: ${group.model?.name || "unknown model"}, ${group.mode}, ${group.state}${group.error ? `; ${group.error}` : ""}.`);
        lines.push(`  Members: ${(group.members || []).map((member) => `${member.node_id}/${member.engine_id} (${member.role})`).join(", ") || "none"}.`);
      }
    } else if (shardList) {
      errors = [...errors, "Shard groups unavailable: invalid API response"];
    }
    if (Array.isArray(taskPage?.items)) {
      lines.push("", `Recent tasks: ${taskPage.items.length}${taskPage.has_more ? " (more history available in Tasks)" : ""}.`);
      for (const task of taskPage.items) lines.push(`  ${task.id}: ${task.status}; updated ${task.updated_at || "unknown"}.`);
    } else if (taskPage) {
      errors = [...errors, "Recent tasks unavailable: invalid API response"];
    }
    if (errors.length) lines.push("", "Incomplete status:", ...errors);
    return lines.join("\n");
  }

  // src/views/orchestrator-chat.js
  function renderDirectMessages() {
    if (!chatMessagesEl) return;
    dashboardDOM.resetVirtualList(chatMessagesEl, { estimateHeight: 140, threshold: 50, overscan: 6 });
    if (!directMessages.length) {
      const welcome = document.createElement("div");
      welcome.className = "welcome-card";
      welcome.innerHTML = "<h2>The mAIstr0 awaits your command</h2>";
      dashboardDOM.appendVirtualListItem(chatMessagesEl, welcome);
      return;
    }
    for (const message of directMessages) {
      appendMessageToChat({ ...message, coordinator: "mAIstr0" });
    }
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  }
  function appendChatItem(node) {
    for (const welcome of chatMessagesEl.querySelectorAll(".welcome-card")) {
      dashboardDOM.removeVirtualListItem(chatMessagesEl, welcome);
    }
    dashboardDOM.appendVirtualListItem(chatMessagesEl, node);
  }
  function appendMessageToChat(msg) {
    if (!chatMessagesEl) return;
    const isUser = msg.role === "user";
    const isTool = msg.role === "tool";
    if (isTool && msg.tool_response) {
      const tr = msg.tool_response;
      const card = document.createElement("div");
      card.className = "tool-card";
      let bodyHTML = "";
      if (tr.output && typeof tr.output === "object" && tr.output.dialogue_flow) {
        bodyHTML = `<div class="dialogue-flow-container">`;
        for (const ex of tr.output.dialogue_flow) {
          bodyHTML += `
          <div class="dialogue-exchange">
            <div class="exchange-header"><strong>Round ${ex.round}</strong> · ${ex.duration_ms || 0}ms</div>
            <div class="orch-msg"><strong>Orchestrator:</strong> ${escapeHtml(ex.orchestrator_prompt)}</div>
            <div class="node-msg"><strong>Node LLM:</strong> ${escapeHtml(ex.node_reply)}</div>
          </div>`;
        }
        bodyHTML += `</div>`;
      } else if (tr.output && typeof tr.output === "object" && tr.output.node_reply) {
        bodyHTML = `
        <div class="dialogue-single-turn">
          <div class="node-msg"><strong>Node LLM (Turn ${tr.output.turn || 1}):</strong>
${escapeHtml(tr.output.node_reply)}</div>
        </div>`;
      } else {
        const outStr = typeof tr.output === "object" ? JSON.stringify(tr.output, null, 2) : String(tr.output || tr.error || "");
        bodyHTML = `<div class="tool-body">${escapeHtml(outStr)}</div>`;
      }
      card.innerHTML = `
      <div class="tool-card-header">
        <span class="tool-title">⚡ ${escapeHtml(tr.name)}</span>
        ${tr.node_id ? `<span class="node-tag">Node: ${escapeHtml(tr.node_id)}</span>` : ""}
        ${tr.model ? `<span class="node-tag">${escapeHtml(tr.model)}</span>` : ""}
        <span class="badge ${tr.error ? "failed" : "completed"}">${tr.duration_ms || 0}ms</span>
      </div>
      ${bodyHTML}
    `;
      appendChatItem(card);
      return;
    }
    const msgDiv = document.createElement("div");
    msgDiv.className = `chat-msg ${isUser ? "user" : "assistant"}`;
    let metaHTML = "";
    if (!isUser) {
      metaHTML = `<div class="msg-meta"><span>🤖 ${escapeHtml(msg.coordinator || "Orchestrator Coordinator")}</span>${msg.node_id ? `<span class="node-tag">Execution node: ${escapeHtml(msg.node_id)}</span>` : ""}${msg.model ? `<span class="node-tag">${escapeHtml(msg.model)}</span>` : ""}${msg.duration_ms ? `<span>${msg.duration_ms}ms</span>` : ""}</div>`;
    }
    let thoughtHTML = "";
    if (msg.thought) {
      thoughtHTML = `<div class="thought-box">💭 <strong>Reasoning:</strong> ${escapeHtml(msg.thought)}</div>`;
    }
    if (msg.raw_output) {
      thoughtHTML += `<div class="node-output-box"><strong>Node output:</strong><pre>${escapeHtml(msg.raw_output)}</pre></div>`;
    }
    let toolsHTML = "";
    if (msg.tool_calls && msg.tool_calls.length > 0) {
      for (const tc of msg.tool_calls) {
        toolsHTML += `
        <div class="tool-card">
          <div class="tool-card-header">
            <span class="tool-title">⚡ Calling tool: ${escapeHtml(tc.name)}</span>
          </div>
          <div class="tool-body">${escapeHtml(JSON.stringify(tc.arguments || {}, null, 2))}</div>
        </div>`;
      }
    }
    let contentHTML = "";
    if (msg.content) {
      contentHTML = `<div class="msg-bubble">${escapeHtml(msg.content)}</div>`;
    }
    msgDiv.innerHTML = `
    ${metaHTML}
    ${thoughtHTML}
    ${toolsHTML}
    ${contentHTML}
  `;
    appendChatItem(msgDiv);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  }
  function formatDirectTaskDispatch(task) {
    const assignments = (task.subtasks || []).map((item) => {
      const subtask = item.subtask || {};
      const description = subtask.description || subtask.id || "Subtask";
      const category = subtask.task_type || "general";
      return `• ${description} [${category}] → ${item.node_id} / ${item.model} (${item.status})`;
    });
    return [
      `Task ${task.id} submitted to the orchestrator's native planner and capability-aware scheduler.`,
      `Status: ${task.status}`,
      ...assignments,
      "Live progress and results are available in the Tasks workspace."
    ].join("\n");
  }
  async function dispatchDirectTask(description) {
    if (!description || isSending) return;
    const statusQuestion = isClusterStatusQuestion(description);
    setIsSending(true);
    if (sendMsgBtn) sendMsgBtn.disabled = true;
    if (clearDirectChatBtn) clearDirectChatBtn.disabled = true;
    agentTabs.forEach((tab) => {
      tab.disabled = true;
    });
    if (chatStatus) chatStatus.textContent = statusQuestion ? "Reading live cluster status…" : "Planning against live capabilities and routing to suitable nodes…";
    directMessages.push({ role: "user", content: description });
    appendMessageToChat({ role: "user", content: description });
    chatInput.value = "";
    try {
      const nodes = await fetchJSON("/api/nodes/all");
      if (!Array.isArray(nodes)) throw new Error("Could not load the current cluster inventory.");
      if (statusQuestion) {
        const summary2 = await answerClusterStatusQuestion(nodes);
        directMessages.push({ role: "assistant", content: summary2 });
        appendMessageToChat({ role: "assistant", coordinator: "mAIstr0", content: summary2 });
        if (chatStatus) chatStatus.textContent = "";
        return;
      }
      if (!nodes.some((node) => node.healthy)) {
        throw new Error("No healthy cluster nodes are available to plan or run this task.");
      }
      const task = await fetchJSON("/api/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ description, plan_mode: "auto" })
      });
      const summary = formatDirectTaskDispatch(task);
      directMessages.push({ role: "assistant", content: summary });
      appendMessageToChat({ role: "assistant", coordinator: "mAIstr0", content: summary });
      if (chatStatus) chatStatus.textContent = "";
      void refresh();
    } catch (err) {
      const message = `${statusQuestion ? "Cluster status failed" : "Task dispatch failed"}: ${err.message}`;
      directMessages.push({ role: "assistant", content: message });
      appendMessageToChat({ role: "assistant", coordinator: "mAIstr0", content: message });
      if (chatStatus) chatStatus.textContent = message;
    } finally {
      setIsSending(false);
      if (sendMsgBtn) sendMsgBtn.disabled = false;
      if (clearDirectChatBtn) clearDirectChatBtn.disabled = false;
      agentTabs.forEach((tab) => {
        tab.disabled = false;
      });
    }
  }
  async function sendInteractiveMessage(text) {
    if (activeChatMode === "direct") return dispatchDirectTask(text);
    if (!text || !activeSessionId || isSending) return;
    setIsSending(true);
    setStreamingAssistantEl(null);
    setActiveAbortController(new AbortController());
    if (sendMsgBtn) sendMsgBtn.disabled = true;
    if (interruptMsgBtn) interruptMsgBtn.hidden = false;
    if (newSessionBtn) newSessionBtn.disabled = true;
    if (agentHarnessSelect) agentHarnessSelect.disabled = true;
    agentTabs.forEach((tab) => {
      tab.disabled = true;
    });
    if (chatStatus) chatStatus.textContent = "⚡ Reasoning & dispatching across cluster...";
    appendMessageToChat({ role: "user", content: text });
    chatInput.value = "";
    try {
      const url = `/api/agent/sessions/${encodeURIComponent(activeSessionId)}/messages?stream=true`;
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Accept": "text/event-stream" },
        body: JSON.stringify({ content: text }),
        signal: activeAbortController.signal
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let streamError = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n\n");
        buffer = lines.pop() || "";
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const jsonStr = line.slice(6).trim();
          if (!jsonStr) continue;
          try {
            const event = JSON.parse(jsonStr);
            handleStreamEvent(event);
            if (event.type === "error") streamError = event.error || "Agent request failed";
          } catch (e) {
            console.error("stream parse error", e, jsonStr);
          }
        }
      }
      await selectSession(activeSessionId);
      if (streamError) throw new Error(streamError);
      if (chatStatus) chatStatus.textContent = "";
    } catch (err) {
      if (err.name === "AbortError") {
        if (chatStatus) chatStatus.textContent = "Interrupted.";
        try {
          await selectSession(activeSessionId);
        } catch (e) {
        }
      } else {
        if (chatStatus) chatStatus.textContent = "Error: " + err.message;
        appendMessageToChat({
          role: "assistant",
          content: "⚠️ Error during execution: " + err.message
        });
      }
    } finally {
      setIsSending(false);
      setActiveAbortController(null);
      if (interruptMsgBtn) interruptMsgBtn.hidden = true;
      if (sendMsgBtn) sendMsgBtn.disabled = false;
      if (newSessionBtn) newSessionBtn.disabled = false;
      if (agentHarnessSelect) agentHarnessSelect.disabled = false;
      agentTabs.forEach((tab) => {
        tab.disabled = false;
      });
    }
  }
  function handleStreamEvent(event) {
    if (event.type === "step_started") {
      if (chatStatus) chatStatus.textContent = `⚡ Step ${event.step}: Reasoning on coordinator LLM...`;
    } else if (event.type === "thought" && event.content) {
      const thoughtBox = document.createElement("div");
      thoughtBox.className = "thought-box";
      thoughtBox.innerHTML = `💭 <strong>Step ${event.step || 1}:</strong> ${escapeHtml(event.content)}`;
      appendChatItem(thoughtBox);
      chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
    } else if (event.type === "node_output" && event.output) {
      const outputBox = document.createElement("div");
      outputBox.className = "node-output-box";
      outputBox.innerHTML = `<strong>Orchestrator coordinator output · ${escapeHtml(event.node_id || "node")} · ${escapeHtml(event.model || "")}</strong><pre>${escapeHtml(event.output)}</pre>`;
      appendChatItem(outputBox);
      chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
    } else if (event.type === "tool_call" && event.tool_call) {
      if (chatStatus) chatStatus.textContent = `⚡ Spreading work: Calling tool ${event.tool_call.name}...`;
    } else if (event.type === "tool_response" && event.tool_resp) {
      appendMessageToChat({ role: "tool", tool_response: event.tool_resp });
    } else if (event.type === "final_message" && event.message) {
      if (streamingAssistantEl) {
        dashboardDOM.removeVirtualListItem(chatMessagesEl, streamingAssistantEl);
        setStreamingAssistantEl(null);
      }
      appendMessageToChat(event.message);
    } else if (event.type === "text_delta" && event.content) {
      if (!streamingAssistantEl) {
        setStreamingAssistantEl(document.createElement("div"));
        streamingAssistantEl.className = "chat-msg assistant";
        const meta = document.createElement("div");
        meta.className = "msg-meta";
        meta.textContent = "🤖 Pi";
        const bubble2 = document.createElement("div");
        bubble2.className = "msg-bubble";
        streamingAssistantEl.append(meta, bubble2);
        appendChatItem(streamingAssistantEl);
      }
      const bubble = streamingAssistantEl.querySelector(".msg-bubble");
      if (bubble) bubble.textContent += event.content;
      chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
    } else if (event.type === "interrupted") {
      if (chatStatus) chatStatus.textContent = "Session interrupted.";
    } else if (event.type === "error") {
      if (chatStatus) chatStatus.textContent = `Error: ${event.error}`;
    }
  }
  interruptMsgBtn?.addEventListener("click", async () => {
    if (!isSending) return;
    if (chatStatus) chatStatus.textContent = "Interrupting...";
    if (activeChatMode !== "direct" && activeSessionId) {
      try {
        await fetchJSON(`/api/agent/sessions/${encodeURIComponent(activeSessionId)}/interrupt`, {
          method: "POST"
        });
      } catch (err) {
        console.error("interrupt agent session failed", err);
      }
    }
    if (activeAbortController) {
      activeAbortController.abort();
    }
  });
  sendMsgBtn?.addEventListener("click", () => {
    sendInteractiveMessage(chatInput.value.trim());
  });
  chatInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendInteractiveMessage(chatInput.value.trim());
    }
  });
  newSessionBtn?.addEventListener("click", () => {
    createNewSession();
  });
  clearDirectChatBtn?.addEventListener("click", () => {
    if (isSending) return;
    setDirectMessages([]);
    if (chatStatus) chatStatus.textContent = "";
    renderDirectMessages();
  });
  promptChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      const p = chip.dataset.prompt;
      if (p) {
        if (chatInput) chatInput.value = p;
        sendInteractiveMessage(p);
      }
    });
  });

  // src/views/orchestrator-controls.js
  async function activateAgentTab(harness) {
    if (isSending) return;
    const previousMode = activeChatMode;
    setActiveChatMode(harness);
    if (newSessionBtn) newSessionBtn.disabled = true;
    if (agentHarnessSelect) agentHarnessSelect.disabled = true;
    agentTabs.forEach((tab) => {
      const active = tab.dataset.agent === harness;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", String(active));
      tab.tabIndex = active ? 0 : -1;
      tab.disabled = true;
    });
    setAgentConsoleMode(harness);
    try {
      if (currentAgentHarness !== harness) {
        const backend = await fetchJSON("/api/agent/backend", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ harness })
        });
        applyAgentBackend(backend);
        await loadAgentTools();
      }
      await loadClusterModels();
      const sessions = sessionsForHarness(harness);
      const rememberedID = activeSessionIds.get(harness);
      const sessionID = sessions.some((session) => session.id === rememberedID) ? rememberedID : sessions[0]?.id;
      if (sessionID) await selectSession(sessionID);
      else await createNewSession();
    } catch (err) {
      console.error("failed to switch agent tab", err);
      if (agentHarnessStatus) agentHarnessStatus.textContent = `Could not switch agent: ${err.message}`;
      setActiveChatMode(previousMode);
      setAgentConsoleMode(previousMode);
      agentTabs.forEach((tab) => {
        const active = tab.dataset.agent === previousMode;
        tab.classList.toggle("active", active);
        tab.setAttribute("aria-selected", String(active));
        tab.tabIndex = active ? 0 : -1;
      });
    } finally {
      agentTabs.forEach((tab) => {
        tab.disabled = false;
      });
      if (newSessionBtn) newSessionBtn.disabled = false;
      if (agentHarnessSelect) agentHarnessSelect.disabled = false;
      renderSessionList();
    }
  }
  function setAgentConsoleMode(mode) {
    const direct = mode === "direct";
    const sessionSection = sessionListEl?.closest(".sidebar-section");
    const settingsSection = agentSettingsTitle?.closest(".sidebar-section");
    const toolsSection = agentToolsList?.closest(".sidebar-section");
    const harnessLabel = agentHarnessSelect?.closest("label");
    const modelLabel = agentModelSelect?.closest("label");
    const stepsLabel = agentStepsSelect?.closest("label");
    const modelLabelText = modelLabel?.querySelector("span");
    const quickPrompts = document.querySelector(".quick-prompts");
    const promptSuggestions = quickPrompts?.closest(".prompt-suggestions");
    if (sessionSection) sessionSection.hidden = direct;
    if (settingsSection) settingsSection.hidden = direct;
    if (toolsSection) toolsSection.hidden = direct;
    if (modelLabel) modelLabel.hidden = direct || !agentSupportsCoordinatorModel;
    if (agentSidebar) agentSidebar.hidden = direct;
    if (harnessLabel) harnessLabel.hidden = direct;
    if (stepsLabel) stepsLabel.hidden = direct || !agentSupportsCoordinatorModel;
    if (quickPrompts) quickPrompts.hidden = direct;
    if (promptSuggestions) promptSuggestions.hidden = direct;
    if (clearDirectChatBtn) clearDirectChatBtn.hidden = !direct;
    if (sendMsgBtn) sendMsgBtn.textContent = direct ? "Ask mAIstr0" : "Send";
    if (agentSettingsTitle) {
      agentSettingsTitle.textContent = direct ? "mAIstr0 Settings" : `${mode === "pi" ? "Pi" : "DeepSeek"} Agent Settings`;
    }
    if (modelLabelText) {
      modelLabelText.textContent = direct ? "Cluster Model" : "Coordinator Model";
      if (agentModelSelect?.options.length) {
        agentModelSelect.options[0].textContent = direct ? "Auto (Orchestrator scheduler)" : "Auto (Fastest / Cluster Leader)";
      }
    }
    if (chatInput) {
      chatInput.placeholder = direct ? "Ask about the cluster, or enter work to dispatch as a task..." : "Message the orchestrator... (Enter to send, Shift+Enter for new line)";
    }
    if (direct) {
      renderDirectMessages();
      void refresh();
    }
  }
  function activateDirectChat() {
    if (isSending) return;
    setActiveChatMode("direct");
    agentTabs.forEach((tab) => {
      const active = tab.dataset.agent === "direct";
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", String(active));
      tab.tabIndex = active ? 0 : -1;
    });
    setAgentConsoleMode("direct");
    chatInput?.focus();
  }
  agentTabs.forEach((tab) => {
    tab.tabIndex = tab.getAttribute("aria-selected") === "true" ? 0 : -1;
    tab.addEventListener("click", () => {
      if (tab.dataset.agent === "direct") activateDirectChat();
      else activateAgentTab(tab.dataset.agent);
    });
    tab.addEventListener("keydown", (event) => {
      const tabs = Array.from(agentTabs);
      const index = tabs.indexOf(tab);
      let nextIndex;
      if (event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
      else if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = tabs.length - 1;
      else return;
      event.preventDefault();
      const next = tabs[nextIndex];
      next.focus();
      if (next.dataset.agent === "direct") activateDirectChat();
      else void activateAgentTab(next.dataset.agent);
    });
  });
  pairingConnectForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!pairingConnectButton || !pairingAddress || !pairingCode) return;
    pairingConnectButton.disabled = true;
    pairingStatus.textContent = "Connecting securely…";
    try {
      const node = dashboardState.pairing.targetNode;
      const joiningOrchestrator = Boolean(node?.joinOrchestrator);
      const invitation = Boolean(node?.discovered && !joiningOrchestrator);
      const endpoint = invitation ? `/api/discovery/${encodeURIComponent(node.id)}/pairing/invite` : joiningOrchestrator ? "/api/pairing/connect" : node ? `/api/nodes/${encodeURIComponent(node.id)}/pairing/connect` : "/api/pairing/connect";
      const result = await fetchJSON(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(invitation ? { pin: pairingCode.value.trim(), fingerprint: pairingFingerprint?.value.trim() || "" } : {
          address: pairingAddress.value.trim(),
          pin: pairingCode.value.trim(),
          fingerprint: pairingFingerprint?.value.trim() || "",
          ...joiningOrchestrator ? { orchestrator_id: node.id } : {}
        })
      });
      pairingCode.value = "";
      pairingStatus.textContent = invitation ? `Invitation sent to ${node.id}. Confirm it on the node to complete pairing.` : joiningOrchestrator ? `Connected to ${result.orchestrator_id}. Waiting for cluster registration…` : node ? `${node.id} paired with this cluster.` : `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}. Connecting to the cluster…`;
      if (invitation && pairingState) {
        pairingState.textContent = `Invitation sent to ${node.id}; waiting for confirmation on that node.`;
      }
      dashboardState.pairing.targetNode = null;
      pairingConnectDialog?.close();
      await loadPairingStatus();
      await refresh();
    } catch (err) {
      pairingStatus.textContent = `Pairing failed: ${err.message}`;
    } finally {
      pairingConnectButton.disabled = false;
    }
  });

  // src/shared/api.js
  async function fetchJSON(url, opts) {
    const resp = await fetch(url, opts);
    if (!resp.ok) {
      throw new Error(await resp.text());
    }
    if (resp.status === 204) return null;
    return resp.json();
  }
  var optionalFeaturesInitialized = false;
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
  function updateClusterTitle(count) {
    const title = document.querySelector("header h1");
    if (title) title.textContent = `mAIstr0 · ${count} ${count === 1 ? "Node" : "Nodes"}`;
  }

  // src/components/pairing-dialog.js
  var generatedPairingPIN = null;
  var pairingPolicyPending = false;
  function pairingStatusWithGeneratedPIN(status) {
    if (status.pin) {
      generatedPairingPIN = { pin: status.pin, expires_at: status.expires_at };
      return status;
    }
    if (!generatedPairingPIN) return status;
    if (!status.expires_at || status.expires_at !== generatedPairingPIN.expires_at || Date.parse(status.expires_at) <= Date.now()) {
      generatedPairingPIN = null;
      return status;
    }
    return { ...status, pin: generatedPairingPIN.pin };
  }
  function openPairingDialog(node = null) {
    if (!pairingConnectDialog || pairingConnectDialog.open) return;
    if (dashboardState.pairing.status?.role === "node" && node?.id === dashboardState.pairing.status.node_id) {
      node = null;
    }
    dashboardState.pairing.targetNode = node;
    if (pairingFingerprint) pairingFingerprint.value = node && !node.joinOrchestrator && !node.discovered ? dashboardState.pairing.status?.fingerprint || "" : "";
    if (pairingStatus) pairingStatus.textContent = "";
    const title = document.getElementById("pairing-title-node");
    const description = document.getElementById("pairing-dialog-description");
    if (node?.joinOrchestrator) {
      const pairingStatus2 = dashboardState.pairing.status;
      const alreadyPairedWithTarget = pairingStatus2?.joined && pairingStatus2.orchestrator_id === node.id;
      if (title) title.textContent = alreadyPairedWithTarget ? `Re-pair with ${node.id}` : pairingStatus2?.joined ? `Switch to ${node.id}'s cluster` : `Join ${node.id}'s cluster`;
      if (description) description.textContent = `Connect securely to ${node.id} at ${node.peer_addr || node.address}. Discovery does not authenticate its certificate. With strict verification off, same-subnet pairing skips the fingerprint and PIN; otherwise verify the fingerprint out of band and enter the PIN.`;
      if (pairingAddressField) pairingAddressField.hidden = false;
      if (pairingAddress) {
        pairingAddress.required = true;
        pairingAddress.value = node.peer_addr || node.address || "";
      }
      if (pairingCode) pairingCode.value = "";
      if (pairingConnectButton) pairingConnectButton.textContent = alreadyPairedWithTarget ? `Re-pair with ${node.id}` : pairingStatus2?.joined ? `Switch to ${node.id}` : "Join cluster";
    } else if (node) {
      const invitation = Boolean(node.discovered);
      if (title) title.textContent = invitation ? `Pair ${node.id} with this cluster` : `Re-pair ${node.id} with this cluster`;
      if (description) description.textContent = invitation ? `Send a pairing invitation to ${node.address}. The node must confirm locally before it joins.` : "Enter this cluster's HTTPS address and an active one-use PIN. The selected node must be online and already trusted by this cluster.";
      if (pairingAddressField) pairingAddressField.hidden = invitation;
      if (pairingAddress) pairingAddress.required = !invitation;
      if (pairingAddress) pairingAddress.value = dashboardState.pairing.status?.address || dashboardState.pairing.status?.orchestrator_addr || "";
      if (pairingCode) pairingCode.value = dashboardState.pairing.status?.role === "orchestrator" ? dashboardState.pairing.status.pin || "" : "";
      if (pairingConnectButton) pairingConnectButton.textContent = invitation ? "Send invitation" : "Re-pair node";
    } else {
      if (pairingAddressField) pairingAddressField.hidden = false;
      if (pairingAddress) pairingAddress.required = true;
      const joiningNode = dashboardState.pairing.status?.role === "node";
      if (title) title.textContent = joiningNode ? dashboardState.pairing.status.joined ? "Re-pair node" : "Join cluster" : "Connect to orchestrator";
      if (description) description.textContent = joiningNode ? "Enter the orchestrator HTTPS address, its out-of-band verified certificate fingerprint, and the one-use PIN shown on its local Setup page." : "Enter the HTTPS address and one-use PIN displayed on the orchestrator dashboard.";
      if (joiningNode) {
        pairingAddress.value = dashboardState.pairing.status.orchestrator_addr || pairingAddress.value;
        pairingCode.value = "";
      }
      if (pairingConnectButton) pairingConnectButton.textContent = joiningNode ? dashboardState.pairing.status.joined ? "Re-pair node" : "Join cluster" : "Connect node";
    }
    pairingConnectDialog.showModal();
    pairingAddress?.focus();
  }
  pairingPinRegenerateBtn?.addEventListener("click", async () => {
    pairingPinRegenerateBtn.disabled = true;
    pairingPinRegenerateBtn.textContent = "Generating PIN…";
    if (pairingState) pairingState.textContent = "Generating a new one-use PIN…";
    try {
      const generated = await fetchJSON("/api/pairing/pin", { method: "POST" });
      if (!generated.pin || !generated.expires_at) throw new Error("The server did not return an active PIN.");
      generatedPairingPIN = { pin: generated.pin, expires_at: generated.expires_at };
      if (pairingPin) pairingPin.textContent = generated.pin;
      if (pairingPinExpiry) {
        pairingPinExpiry.textContent = `Expires ${new Date(generated.expires_at).toLocaleTimeString()}. The PIN can be used once.`;
      }
      await loadPairingStatus();
      if (pairingState) pairingState.textContent = "New PIN generated. Enter it only on the node you intend to pair.";
    } catch (err) {
      if (pairingState) pairingState.textContent = `Could not generate a new PIN: ${err.message}`;
    } finally {
      pairingPinRegenerateBtn.disabled = false;
      pairingPinRegenerateBtn.textContent = "Generate / replace pairing PIN";
    }
  });
  pairingRequireVerification?.addEventListener("change", async () => {
    const required = pairingRequireVerification.checked;
    pairingPolicyPending = true;
    pairingRequireVerification.disabled = true;
    if (pairingPolicyState) pairingPolicyState.textContent = "Saving cluster-wide pairing policy…";
    try {
      const saved = await fetchJSON("/api/pairing/policy", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ require_pairing_verification: required })
      });
      if (typeof saved.require_pairing_verification !== "boolean") {
        throw new Error("The server did not confirm the pairing policy.");
      }
      dashboardState.pairing.status = {
        ...dashboardState.pairing.status,
        require_pairing_verification: saved.require_pairing_verification
      };
      pairingRequireVerification.checked = saved.require_pairing_verification;
      if (pairingPolicyState) {
        pairingPolicyState.textContent = saved.require_pairing_verification ? "Strict mode is on. New joins and re-pairs require both checks; existing trust is unchanged." : "Strict mode is off. Nodes on a connected local subnet may auto-join; existing trust is unchanged.";
      }
    } catch (err) {
      pairingRequireVerification.checked = !required;
      if (pairingPolicyState) pairingPolicyState.textContent = `Could not save pairing policy: ${err.message}`;
    } finally {
      pairingPolicyPending = false;
      pairingRequireVerification.disabled = false;
    }
  });
  async function loadPairingStatus() {
    try {
      const status = pairingStatusWithGeneratedPIN(await fetchJSON("/api/pairing/status"));
      if (status.dashboard_url) {
        window.location.replace(status.dashboard_url);
        return;
      }
      dashboardState.pairing.status = status;
      if (pairingFingerprintDisplay) pairingFingerprintDisplay.textContent = status.fingerprint || "Unavailable";
      if (status.role === "orchestrator") {
        const pinViewModel = dashboardViewModel.pairingPINViewModel(status);
        if (pairingPinView) pairingPinView.hidden = !pinViewModel.visible;
        if (pairingNodeConnect) pairingNodeConnect.hidden = true;
        if (pairingInstructions) pairingInstructions.textContent = "Strict mode requires an out-of-band verified fingerprint and one-use PIN. With strict mode off, nodes on a connected local subnet can auto-join.";
        if (pairingRequireVerification && !pairingPolicyPending) {
          pairingRequireVerification.checked = status.require_pairing_verification === true;
        }
        if (pairingPolicyState && !pairingPolicyPending) {
          pairingPolicyState.textContent = status.require_pairing_verification ? "Strict mode is on. New joins and re-pairs require both checks; existing trust is unchanged." : "Strict mode is off. Nodes on a connected local subnet may auto-join; existing trust is unchanged.";
        }
        if (pairingPinRegenerateBtn) {
          pairingPinRegenerateBtn.textContent = pinViewModel.regenerateText;
        }
        if (pairingAddressDisplay) pairingAddressDisplay.textContent = status.address || "HTTPS address unavailable";
        if (pairingPin) pairingPin.textContent = pinViewModel.pinText;
        if (pairingPinExpiry) pairingPinExpiry.textContent = pinViewModel.expiryText;
        if (pairingState) pairingState.textContent = pinViewModel.stateText;
        return;
      }
      if (status.role === "node") {
        if (pairingPinView) pairingPinView.hidden = true;
        if (pairingNodeConnect) pairingNodeConnect.hidden = false;
        if (pairingConnectOpenBtn) pairingConnectOpenBtn.textContent = status.joined ? "Re-pair / change orchestrator" : "Enter address and pairing PIN";
        if (pairingInstructions) pairingInstructions.textContent = status.auto_join_available ? "This orchestrator is on a connected local subnet. If cluster strict mode is off, the node joins automatically without an out-of-band fingerprint or PIN." : status.require_pairing_verification ? "Cluster strict mode is on. New joins and re-pairs require the orchestrator's verified certificate fingerprint and one-use PIN." : "For non-local connections, verify the orchestrator's certificate fingerprint out of band and enter its one-use PIN. Discovery alone does not establish trust.";
        if (status.orchestrator_addr && pairingAddress && !pairingAddress.value) {
          pairingAddress.value = status.orchestrator_addr;
        }
        if (pairingState) {
          pairingState.textContent = status.joined ? `Connected to ${status.orchestrator_addr || "the orchestrator"}.` : status.auto_join_available ? `Connecting automatically to ${status.orchestrator_addr || "a discovered cluster"} on this subnet.` : status.trusted ? "Pairing succeeded. Waiting for the node to register with the orchestrator." : "Not paired. Enter the orchestrator HTTPS address and its one-use PIN.";
        }
        if (!status.joined && !dashboardState.pairing.setupFocused) {
          dashboardState.pairing.setupFocused = true;
          document.querySelector('.nav-tab[data-tab="cluster-tab"]')?.click();
          document.querySelector('[data-window="setup-panel"]')?.click();
          document.getElementById("setup-pairing-tab")?.click();
        }
        const invitationPending = await checkPairingInvitation();
        if ((status.auto_join_available || status.joined || status.trusted) && dashboardState.pairing.dialogAutoOpened) {
          pairingConnectDialog?.close();
          dashboardState.pairing.dialogAutoOpened = false;
          dashboardState.pairing.promptShown = false;
        }
        if (!invitationPending && !status.joined && !status.trusted && status.pairing_required !== false && !status.auto_join_available && !dashboardState.pairing.promptShown) {
          dashboardState.pairing.promptShown = true;
          dashboardState.pairing.dialogAutoOpened = true;
          openPairingDialog();
        } else if (status.joined) {
          dashboardState.pairing.promptShown = false;
          dashboardState.pairing.dialogAutoOpened = false;
        }
      }
    } catch (err) {
      if (pairingState) pairingState.textContent = `Could not load pairing status: ${err.message}`;
      if (pairingNodeConnect) pairingNodeConnect.hidden = false;
      if (pairingConnectOpenBtn) pairingConnectOpenBtn.hidden = false;
    }
  }
  async function checkPairingInvitation() {
    const invitation = await fetchJSON("/api/pairing/invitation");
    if (!invitation.pending) {
      if (pairingInvitationDialog?.open) pairingInvitationDialog.close();
      return false;
    }
    pairingConnectDialog?.close();
    if (pairingInvitationDetails) {
      pairingInvitationDetails.textContent = `${invitation.orchestrator_id} at ${invitation.address} wants to add this node to its cluster. Verify SHA-256 ${invitation.fingerprint} out of band before accepting. The invitation expires ${new Date(invitation.expires_at).toLocaleTimeString()}.`;
    }
    if (pairingInvitationDialog && !pairingInvitationDialog.open) {
      if (pairingInvitationFingerprint) pairingInvitationFingerprint.value = "";
      pairingInvitationDialog.showModal();
    }
    return true;
  }
  async function respondToPairingInvitation(accept) {
    if (!pairingInvitationAcceptBtn || !pairingInvitationDeclineBtn) return;
    pairingInvitationAcceptBtn.disabled = true;
    pairingInvitationDeclineBtn.disabled = true;
    if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept ? "Connecting to the cluster securely…" : "Declining invitation…";
    try {
      const fingerprint = pairingInvitationFingerprint?.value.trim() || "";
      if (accept && !/^[a-fA-F0-9]{64}$/.test(fingerprint)) throw new Error("Enter the orchestrator fingerprint verified out of band.");
      const result = await fetchJSON(`/api/pairing/invitation/${accept ? "accept" : "reject"}`, {
        method: "POST",
        ...accept ? { headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fingerprint }) } : {}
      });
      dashboardState.pairing.promptShown = true;
      pairingInvitationDialog?.close();
      if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept ? `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}.` : "Invitation declined.";
      await loadPairingStatus();
      await refresh();
    } catch (err) {
      if (pairingInvitationStatus) pairingInvitationStatus.textContent = `${accept ? "Could not join cluster" : "Could not decline invitation"}: ${err.message}`;
    } finally {
      pairingInvitationAcceptBtn.disabled = false;
      pairingInvitationDeclineBtn.disabled = false;
    }
  }
  pairingInvitationAcceptBtn?.addEventListener("click", () => respondToPairingInvitation(true));
  pairingInvitationDeclineBtn?.addEventListener("click", () => respondToPairingInvitation(false));
  pairingInvitationDialog?.addEventListener("cancel", (event) => event.preventDefault());

  // src/components/task-list.js
  function renderTaskHistoryTable(container, tasks, idPrefix, linkNodes = false) {
    let state = container.__taskTableState;
    if (!state) {
      const table = document.createElement("table");
      table.className = "task-history-table";
      table.innerHTML = `
      <thead><tr>
        <th scope="col"><span class="sr-only">Expand</span></th>
        <th scope="col">Task</th>
        <th scope="col">Task ID</th>
        <th scope="col">Steps</th>
        <th scope="col">Status</th>
      </tr></thead>
    `;
      const rows = document.createElement("tbody");
      table.appendChild(rows);
      container.innerHTML = "";
      container.appendChild(table);
      state = {
        table,
        rows,
        expanded: /* @__PURE__ */ new Set(),
        heights: /* @__PURE__ */ new Map(),
        tasks: [],
        frame: 0,
        idPrefix,
        linkNodes
      };
      state.onScroll = () => {
        if (state.frame) return;
        const raf = globalThis.requestAnimationFrame || ((callback) => setTimeout(callback, 16));
        state.frame = raf(() => {
          state.frame = 0;
          renderTaskHistoryTable(container, state.tasks, state.idPrefix, state.linkNodes);
        });
      };
      container.addEventListener?.("scroll", state.onScroll, { passive: true });
      container.__taskTableState = state;
    }
    state.tasks = tasks;
    container.setAttribute?.("aria-busy", "false");
    state.idPrefix = idPrefix;
    state.linkNodes = linkNodes;
    const virtualized = tasks.length > 80;
    const range = virtualized ? dashboardDOM.getVirtualRange(tasks, container.scrollTop || 0, container.clientHeight || 600, {
      estimateHeight: 64,
      overscan: 8,
      heightOf: (task) => state.heights.get(task.id) || 64
    }) : { start: 0, end: tasks.length, top: 0, bottom: 0 };
    if (virtualized && !state.topSpacer) {
      const createSpacer = () => {
        const spacer = document.createElement("tbody");
        spacer.className = "task-virtual-spacer";
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 5;
        cell.setAttribute("aria-hidden", "true");
        row.appendChild(cell);
        spacer.appendChild(row);
        return { spacer, cell };
      };
      state.topSpacer = createSpacer();
      state.bottomSpacer = createSpacer();
      state.table.insertBefore(state.topSpacer.spacer, state.rows);
      state.table.appendChild(state.bottomSpacer.spacer);
    }
    if (state.topSpacer) {
      state.topSpacer.spacer.hidden = range.top === 0;
      state.topSpacer.cell.style.height = `${range.top}px`;
      state.bottomSpacer.spacer.hidden = range.bottom === 0;
      state.bottomSpacer.cell.style.height = `${range.bottom}px`;
    }
    const validStatuses = /* @__PURE__ */ new Set(["pending", "running", "synthesizing", "awaiting_approval", "completed", "failed", "cancelled"]);
    dashboardDOM.patchKeyedChildren(state.rows, tasks.slice(range.start, range.end), {
      keyOf: (task) => task.id,
      signatureOf: (task) => JSON.stringify({ task, idPrefix, linkNodes }),
      renderItem: (t, visibleIndex) => {
        const index = range.start + visibleIndex;
        const status = validStatuses.has(t.status) ? t.status : "unknown";
        const isExpanded = state.expanded.has(t.id);
        const detailsId = `${idPrefix}-details-${index}`;
        const subtasks = t.subtasks || [];
        const completedCount = subtasks.filter((subtask) => subtask.status === "completed").length;
        const subtaskHTML = subtasks.map((s) => {
          const subtaskStatus = validStatuses.has(s.status) ? s.status : "unknown";
          const subtask = s.subtask || {};
          const node = s.node_id ? linkNodes ? `<button class="activity-assignment" type="button" data-target-node="${escapeHtml(s.node_id)}">${escapeHtml(s.node_id)}</button>` : escapeHtml(s.node_id) : "Unassigned";
          const attempts = Array.isArray(s.attempts) ? s.attempts : [];
          const attemptTimestamps = attempts.length ? attempts.map((attempt, attemptIndex) => {
            const attemptNumber = Number(attempt.number) || attemptIndex + 1;
            const attemptStatus = typeof attempt.status === "string" ? attempt.status : "unknown";
            const durationMs = Number(attempt.duration_ms);
            const details = [
              `<strong>Attempt ${attemptNumber} · ${escapeHtml(attemptStatus)}</strong>`,
              attempt.id ? `<span>Attempt ID: ${escapeHtml(attempt.id)}</span>` : "",
              attempt.request_id ? `<span>Request ID: ${escapeHtml(attempt.request_id)}</span>` : "",
              attempt.node_id ? `<span>Node: ${escapeHtml(attempt.node_id)}</span>` : "",
              attempt.model ? `<span>Model: ${escapeHtml(attempt.model)}</span>` : "",
              attempt.engine ? `<span>Engine: ${escapeHtml(attempt.engine)}</span>` : "",
              attempt.failure_class ? `<span>Failure: ${escapeHtml(attempt.failure_class)}</span>` : "",
              attempt.http_status ? `<span>HTTP ${escapeHtml(attempt.http_status)}</span>` : "",
              Number.isFinite(durationMs) && durationMs >= 0 ? `<span>Duration: ${durationMs} ms</span>` : "",
              attempt.error ? `<span>Error: ${escapeHtml(attempt.error)}</span>` : ""
            ].join("");
            return `<div class="task-step-attempt">${details}${formatTimestamp("Started", attempt.started_at)}${formatTimestamp("Finished", attempt.finished_at)}</div>`;
          }).join("") : `<div class="task-step-attempt"><span>Not dispatched</span>${formatTimestamp("Task created", t.created_at)}</div>`;
          return `
        <article class="task-step">
          <div class="task-step-heading"><strong>${escapeHtml(subtask.id || "Subtask")}</strong><span class="badge ${subtaskStatus}">${escapeHtml(subtaskStatus.replaceAll("_", " "))}</span></div>
          <div class="task-step-target">${escapeHtml(subtask.role || subtask.task_type || "Task step")} · ${escapeHtml(subtask.task_type || "general")} · ${node} / ${escapeHtml(s.model || "No model")}</div>
          <div class="task-step-timestamps">${attemptTimestamps}</div>
          ${subtask.description ? `<p>${escapeHtml(subtask.description)}</p>` : ""}
          ${s.error ? `<p class="task-error-reason"><strong>Failure reason:</strong> ${escapeHtml(s.error)}</p>` : ""}
          ${s.score_factors ? `<details class="routing-score-details"><summary>Routing score ${Number(s.score || 0).toFixed(1)}</summary><div class="catalog-tags">Model match +${Number(s.score_factors.model_match || 0).toFixed(1)} · capability +${Number(s.score_factors.capability_score || 0).toFixed(1)} · quality +${Number(s.score_factors.model_quality_score || 0).toFixed(1)} · hardware +${Number(s.score_factors.hardware || 0).toFixed(1)} · GPU preference +${Number(s.score_factors.gpu_compatibility_score || 0).toFixed(1)} · load −${Number(s.score_factors.load_penalty || 0).toFixed(1)} (${s.score_factors.active_tasks || 0} active + ${s.score_factors.queued_tasks || 0} queued) · topology −${Number(s.score_factors.topology_score || 0).toFixed(1)} · GPU −${Number(s.score_factors.gpu_utilization_penalty || 0).toFixed(1)} · NPU −${Number(s.score_factors.npu_utilization_penalty || 0).toFixed(1)} · learned ${Number(s.score_factors.learned_bias || 0) >= 0 ? "+" : ""}${Number(s.score_factors.learned_bias || 0).toFixed(1)} · telemetry ${s.score_factors.telemetry_fresh ? `fresh (${s.score_factors.telemetry_age_ms || 0}ms)` : "stale/unavailable, ignored"}</div></details>` : ""}
          ${s.output ? `<div class="subtask-output-wrap"><button class="copy-output-btn" type="button" data-copy-output="${encodeURIComponent(s.output)}">Copy output</button><pre class="subtask-output">${escapeHtml(s.output)}</pre></div>` : ""}
        </article>
      `;
        }).join("");
        const item = document.createElement("tr");
        item.className = `task-history-row task-history-row-${status}`;
        item.dataset.taskId = t.id;
        item.innerHTML = `
      <td><button class="task-expand-btn" type="button" aria-expanded="${isExpanded}" aria-controls="${detailsId}" aria-label="Show details for ${escapeHtml(t.description || t.id)}"><span class="task-expand-icon" aria-hidden="true">›</span></button></td>
      <td><span class="task-history-title" title="${escapeHtml(t.description || t.id)}">${escapeHtml(t.description || t.id)}</span><span class="task-history-timestamp">${formatTimestamp("Created", t.created_at)}</span></td>
      <td><span class="task-history-id" title="${escapeHtml(t.id)}">${escapeHtml(t.id)}</span></td>
      <td>${subtasks.length ? `${completedCount} / ${subtasks.length}` : "—"}</td>
      <td><span class="badge ${status}">${escapeHtml(status.replaceAll("_", " "))}</span></td>
    `;
        const detailsRow = document.createElement("tr");
        detailsRow.id = detailsId;
        detailsRow.className = "task-details-row";
        detailsRow.dataset.taskId = t.id;
        detailsRow.hidden = !isExpanded;
        detailsRow.innerHTML = `
      <td colspan="5"><div class="task-details-content">
      <div class="task-menu-meta">
        ${formatTimestamp("Created", t.created_at)}
        ${t.updated_at ? formatTimestamp("Updated", t.updated_at) : ""}
      </div>
      <div class="task-actions">
        ${status === "running" || status === "awaiting_approval" ? `<button class="cancel-task-btn" type="button" data-task-id="${escapeHtml(t.id)}">Cancel</button>` : ""}
        ${status === "awaiting_approval" ? `<button class="approve-task-btn primary-btn" type="button" data-task-id="${escapeHtml(t.id)}">Approve and synthesize</button>` : ""}
        ${t.resumable && (status === "failed" || status === "cancelled") ? `<button class="resume-task-btn primary-btn" type="button" data-task-id="${escapeHtml(t.id)}">Resume</button>` : ""}
      </div>
      ${t.error ? `<p class="task-error-reason"><strong>Task failure reason:</strong> ${escapeHtml(t.error)}</p>` : ""}
      ${t.last_checkpoint ? `<div class="task-checkpoint">Last checkpoint: ${escapeHtml(t.last_checkpoint)}</div>` : ""}
      ${subtasks.length ? `<div class="task-subtasks">${subtaskHTML}</div>` : '<p class="task-history-empty">No subtasks recorded.</p>'}
      ${t.result ? `<section class="task-result"><h4>Final result</h4><pre class="subtask-output">${escapeHtml(t.result)}</pre></section>` : ""}
      </div></td>
    `;
        const expandButton = item.querySelector(".task-expand-btn");
        expandButton?.addEventListener("click", () => {
          detailsRow.hidden = !detailsRow.hidden;
          if (detailsRow.hidden) state.expanded.delete(t.id);
          else state.expanded.add(t.id);
          expandButton.setAttribute("aria-expanded", String(!detailsRow.hidden));
          expandButton.setAttribute("aria-label", `${detailsRow.hidden ? "Show" : "Hide"} details for ${t.description || t.id}`);
          const height = (item.getBoundingClientRect?.().height || 0) + (detailsRow.hidden ? 0 : detailsRow.getBoundingClientRect?.().height || 0);
          if (height > 0) state.heights.set(t.id, height);
        });
        detailsRow.querySelector(".resume-task-btn")?.addEventListener("click", async (event) => {
          const button = event.currentTarget;
          button.disabled = true;
          try {
            await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/resume`, { method: "POST" });
          } catch (err) {
            button.disabled = false;
            if (submitStatus) submitStatus.textContent = `Unable to resume ${t.id}: ${err.message}`;
          }
        });
        detailsRow.querySelector(".cancel-task-btn")?.addEventListener("click", async (event) => {
          const button = event.currentTarget;
          button.disabled = true;
          try {
            await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/cancel`, { method: "POST" });
          } catch (err) {
            if (submitStatus) submitStatus.textContent = `Unable to cancel ${t.id}: ${err.message}`;
          }
        });
        detailsRow.querySelector(".approve-task-btn")?.addEventListener("click", async (event) => {
          const button = event.currentTarget;
          button.disabled = true;
          try {
            await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/approve`, { method: "POST" });
          } catch (err) {
            button.disabled = false;
            if (submitStatus) submitStatus.textContent = `Unable to approve ${t.id}: ${err.message}`;
          }
        });
        detailsRow.querySelectorAll("[data-copy-output]").forEach((button) => {
          button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copyOutput), button));
        });
        return [item, detailsRow];
      }
    });
    state.rows.querySelectorAll?.(".task-history-row").forEach((row) => {
      const details = row.nextElementSibling;
      const height = (row.getBoundingClientRect?.().height || 0) + (details && !details.hidden ? details.getBoundingClientRect?.().height || 0 : 0);
      if (height > 0) state.heights.set(row.dataset.taskId, height);
    });
  }
  function renderTasks(tasks, hasMore = false, loading = false) {
    if (!tasksList) return;
    tasksList.querySelector?.(".history-load-more")?.remove();
    tasksList.setAttribute?.("aria-busy", String(loading));
    if (!tasks || tasks.length === 0) {
      tasksList.__taskTableState?.table.remove?.();
      delete tasksList.__taskTableState;
      if (loading) {
        dashboardDOM.renderSkeleton(tasksList, 3);
        if (taskHistoryCount) taskHistoryCount.textContent = dashboardViewModel.taskHistoryCountLabel(0, 0);
        return;
      }
      tasksList.innerHTML = '<p class="task-history-empty">No tasks submitted yet.</p>';
      if (taskHistoryCount) taskHistoryCount.textContent = dashboardViewModel.taskHistoryCountLabel(0, 0);
      appendTaskHistoryLoadMore(hasMore, loading);
      return;
    }
    const query = (taskHistorySearch?.value || "").trim().toLowerCase();
    const statusFilter = taskHistoryStatus?.value || "all";
    const orderedTasks = [...tasks].filter((task) => {
      const status = task.status || "unknown";
      if (statusFilter !== "all" && status !== statusFilter) return false;
      if (!query) return true;
      const searchable = [
        task.id,
        task.description,
        status,
        task.result,
        task.error,
        ...(task.subtasks || []).flatMap((item) => [
          item.subtask?.id,
          item.subtask?.description,
          item.subtask?.role,
          item.subtask?.task_type,
          item.node_id,
          item.model,
          item.output,
          item.error
        ])
      ].filter(Boolean).join(" ").toLowerCase();
      return searchable.includes(query);
    }).sort(
      (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
    if (taskHistoryCount) {
      taskHistoryCount.textContent = dashboardViewModel.taskHistoryCountLabel(orderedTasks.length, tasks.length);
    }
    if (orderedTasks.length === 0) {
      tasksList.__taskTableState?.table.remove?.();
      delete tasksList.__taskTableState;
      tasksList.innerHTML = '<p class="task-history-empty">No tasks match the selected filters.</p>';
      appendTaskHistoryLoadMore(hasMore, loading);
      return;
    }
    renderTaskHistoryTable(tasksList, orderedTasks, "task-history");
    appendTaskHistoryLoadMore(hasMore, loading);
  }
  function appendTaskHistoryLoadMore(hasMore, loading) {
    if (!hasMore || !tasksList) return;
    const button = document.createElement("button");
    button.className = "icon-button history-load-more";
    button.type = "button";
    button.textContent = loading ? "Loading…" : "Load more tasks";
    button.disabled = loading;
    button.addEventListener("click", loadMoreTaskHistory);
    tasksList.appendChild(button);
  }
  var taskHistoryFilterTimer = null;
  taskHistorySearch?.addEventListener("input", () => {
    clearTimeout(taskHistoryFilterTimer);
    taskHistoryFilterTimer = setTimeout(() => refreshTaskHistory(true), 300);
  });
  taskHistoryStatus?.addEventListener("change", () => refreshTaskHistory(true));
  async function copyText(text, button) {
    try {
      await navigator.clipboard.writeText(text);
      const original = button.textContent;
      button.textContent = "Copied";
      setTimeout(() => {
        button.textContent = original;
      }, 1200);
    } catch (err) {
      button.textContent = "Copy failed";
    }
  }
  function taskText(tasks) {
    return (tasks || []).map((t) => [
      `Task ${t.id}: ${t.description}`,
      ...(t.subtasks || []).map((s) => `${s.subtask.id} (${s.status}) on ${s.node_id}:
${s.output || s.error || ""}`)
    ].join("\n")).join("\n\n");
  }
  copyTasksBtn?.addEventListener("click", () => copyAllTaskResults());

  // src/views/cluster-setup.js
  async function checkSetupReadiness() {
    if (setupCheckBtn) {
      setupCheckBtn.disabled = true;
      setupCheckBtn.textContent = "Checking…";
    }
    setSetupStatus("Checking node engines and refreshing empty model registries…", "checking");
    setupResults?.setAttribute("aria-busy", "true");
    try {
      const allNodes = await fetchJSON("/api/nodes/all");
      const nodes = allNodes.filter((node) => node.healthy);
      if (!nodes.length) {
        setSetupStatus(
          allNodes.length ? "No cluster nodes are online. Offline nodes remain listed above." : "No nodes are registered. Start a node and pair it with this orchestrator first.",
          allNodes.length ? "attention" : "empty"
        );
        if (setupResults) {
          setupResults.innerHTML = '<p class="setup-empty">An online node is required to check engine and model readiness.</p>';
        }
        return;
      }
      const results = await Promise.all(nodes.map(async (node) => {
        try {
          const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/engines`);
          let refreshed = false;
          if (engines.length && !(node.models || []).length) {
            await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models/refresh`, { method: "POST" });
            refreshed = true;
          }
          const models = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models`);
          return { node, engines, models, refreshed, error: "" };
        } catch (err) {
          return { node, engines: [], models: [], refreshed: false, error: err.message };
        }
      }));
      let readyCount = 0;
      if (setupResults) {
        setupResults.innerHTML = results.map(({ node, engines, models, refreshed, error }) => {
          const installedCount = models.length;
          const usableCount = models.filter((model) => model.enabled).length;
          const ready = !error && engines.length > 0 && usableCount > 0;
          if (ready) readyCount += 1;
          const state = error ? `Could not inspect node: ${error}` : !engines.length ? "No inference engine detected. Configure an engine in node settings." : usableCount ? `${usableCount} usable model${usableCount === 1 ? "" : "s"}${installedCount > usableCount ? ` · ${installedCount - usableCount} disabled` : ""}${refreshed ? " (registry refreshed)" : ""}.` : installedCount ? "Models are installed, but all are disabled for scheduling." : "Engine detected, but no models are available yet.";
          const stateName = ready ? "ready" : error ? "error" : "attention";
          const badge = ready ? "Ready" : error ? "Unavailable" : "Needs setup";
          return `<article class="setup-card" data-state="${stateName}">
            <div class="setup-card-heading"><h3>${escapeHtml(node.id)}</h3><span class="setup-badge">${badge}</span></div>
            <dl class="setup-details">
              <div><dt>Engines</dt><dd>${engines.length ? engines.map((engine) => escapeHtml(engine.name)).join(", ") : "None detected"}</dd></div>
              <div><dt>Installed</dt><dd>${installedCount}</dd></div>
              <div><dt>Usable</dt><dd>${usableCount}</dd></div>
            </dl>
            ${models.length ? `<div class="catalog-tags">${models.map((model) => `${escapeHtml(model.name)} · ${escapeHtml(model.engine)} — ${model.enabled ? "usable" : "installed, disabled"}`).join("<br>")}</div>` : ""}
            <p class="setup-card-message">${escapeHtml(state)}</p>
          </article>`;
        }).join("");
      }
      setSetupStatus(
        readyCount === nodes.length ? `All ${nodes.length} node${nodes.length === 1 ? "" : "s"} are ready for inference.` : `${readyCount} of ${nodes.length} nodes are ready. Use node settings to configure engines or select models.`,
        readyCount === nodes.length ? "ready" : "attention"
      );
      await refresh();
    } catch (err) {
      setSetupStatus(`Readiness check failed: ${err.message}`, "error");
    } finally {
      if (setupCheckBtn) {
        setupCheckBtn.disabled = false;
        setupCheckBtn.textContent = "Check readiness";
      }
      setupResults?.removeAttribute("aria-busy");
    }
  }
  async function loadModelCatalog() {
    if (!catalogResults) return;
    try {
      const [modelCatalogEntriesValue] = await Promise.all([
        fetchJSON("/api/models/catalog"),
        loadCatalogNodes()
      ]);
      setModelCatalogEntries(modelCatalogEntriesValue);
      renderModelCatalog();
    } catch (err) {
      catalogStatus.textContent = `Could not load model catalog: ${err.message}`;
      catalogResults.innerHTML = "";
    }
  }
  async function loadCatalogNodes() {
    const nodes = await fetchJSON("/api/nodes/all");
    const online = nodes.filter((node) => node.healthy);
    const selected = catalogNodeSelect.value;
    catalogNodeSelect.innerHTML = online.map((node) => `<option value="${escapeHtml(node.id)}">${escapeHtml(node.id)}</option>`).join("");
    if (online.some((node) => node.id === selected)) catalogNodeSelect.value = selected;
    if (!online.length) {
      catalogEngineSelect.innerHTML = "";
      setCatalogModels([]);
      catalogStatus.textContent = "An online node is required to inspect installed models or install a catalog model.";
      catalogResults.innerHTML = '<p class="setup-empty">No online target nodes.</p>';
      return;
    }
    await loadCatalogTarget();
  }
  async function loadCatalogTarget() {
    const nodeId = catalogNodeSelect?.value;
    if (!nodeId) return;
    catalogStatus.textContent = `Loading engines and installed models for ${nodeId}…`;
    try {
      const [allEngines, models] = await Promise.all([
        fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/engines`),
        fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`)
      ]);
      setCatalogEngines(allEngines.filter((item) => item.can_pull));
      setCatalogModels(models);
      const currentEngine = catalogEngineSelect.value;
      catalogEngineSelect.innerHTML = catalogEngines.map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)} · ${escapeHtml(item.url || "configured engine")}</option>`).join("");
      if (catalogEngines.some((item) => item.name === currentEngine)) catalogEngineSelect.value = currentEngine;
      catalogStatus.textContent = catalogEngines.length ? `Target: ${nodeId} · ${catalogEngineSelect.value || "Select an engine"}. Installed models are marked usable or disabled for scheduling.` : `No configured engine on ${nodeId} supports model downloads.`;
      renderModelCatalog();
    } catch (err) {
      catalogStatus.textContent = `Could not inspect ${nodeId}: ${err.message}`;
      catalogResults.innerHTML = "";
    }
  }
  function renderModelCatalog() {
    if (!catalogResults) return;
    const engineName = catalogEngineSelect?.value || "";
    const query = (catalogSearch?.value || "").trim().toLowerCase();
    const entries = modelCatalogEntries.filter(
      (model) => model.engine === engineName && (!query || `${model.name} ${model.description} ${(model.tags || []).join(" ")}`.toLowerCase().includes(query))
    );
    if (!engineName) {
      catalogResults.innerHTML = '<p class="setup-empty">Choose an online node with a download-capable engine to browse matching models.</p>';
      return;
    }
    if (!entries.length) {
      catalogResults.innerHTML = '<p class="setup-empty">No catalog entries match this engine and search.</p>';
      return;
    }
    catalogResults.innerHTML = entries.map((model) => {
      const installed = catalogModels.find((item) => item.name === model.name && item.engine === model.engine);
      const state = installed ? installed.enabled ? "Installed · usable" : "Installed · disabled for scheduling" : "Available to download";
      const button = installed ? `<button class="secondary-button" type="button" disabled>${installed.enabled ? "Installed" : "Installed (disabled)"}</button>` : `<button class="primary-btn catalog-install" type="button" data-model="${escapeHtml(model.name)}">Install on ${escapeHtml(catalogNodeSelect.value)} · ${escapeHtml(engineName)}</button>`;
      return `<article class="setup-card catalog-card">
      <div class="setup-card-heading"><h3>${escapeHtml(model.name)}</h3><span class="setup-badge">${escapeHtml(state)}</span></div>
      <p>${escapeHtml(model.description)}</p>
      <div class="catalog-tags">Engine: ${escapeHtml(model.engine)}${model.tags?.length ? ` · ${model.tags.map(escapeHtml).join(", ")}` : ""}</div>
      ${button}
    </article>`;
    }).join("");
    catalogResults.querySelectorAll(".catalog-install").forEach((button) => {
      button.addEventListener("click", () => installCatalogModel(button.dataset.model, button));
    });
  }
  async function installCatalogModel(model, button) {
    const nodeId = catalogNodeSelect.value;
    const engineName = catalogEngineSelect.value;
    if (!nodeId || !engineName || !model) return;
    button.disabled = true;
    button.textContent = "Installing…";
    catalogStatus.textContent = `Installing ${model} with ${engineName} on ${nodeId}…`;
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/pull`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model, engine: engineName })
      });
      setCatalogModels(await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" }));
      catalogStatus.textContent = `Installed ${model} on ${nodeId} using ${engineName}.`;
      renderModelCatalog();
      await refresh();
    } catch (err) {
      catalogStatus.textContent = `Could not install ${model} on ${nodeId} using ${engineName}: ${err.message}`;
      button.disabled = false;
      button.textContent = `Retry on ${nodeId} · ${engineName}`;
    }
  }
  function setSetupStatus(message, state) {
    if (!setupStatus) return;
    setupStatus.textContent = message;
    setupStatus.dataset.state = state;
  }

  // src/components/node-card.js
  function engineStatusSummaryMarkup(engines) {
    if (!Array.isArray(engines) || !engines.length) {
      return '<span class="engine-status-empty">No engines reported</span>';
    }
    return `<div class="engine-status-summary" aria-label="Inference engine status">${engines.map((engine) => {
      const state = dashboardViewModel.engineInventoryState(engine);
      const count = Math.max(0, Number(engine.model_count) || 0);
      const countLabel = state === "error" ? `${count} known` : `${count} model${count === 1 ? "" : "s"}`;
      const title = engine.last_error ? `${engine.name}: ${engine.last_error}` : `${engine.name}: ${dashboardViewModel.engineInventoryLabel(state)}`;
      return `<span class="engine-status-chip ${state}" title="${escapeHtml(title)}">
      <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
      <span>${escapeHtml(dashboardViewModel.engineInventoryLabel(state))} · ${countLabel}</span>
    </span>`;
    }).join("")}</div>`;
  }
  function engineStatusDetailsMarkup(engines) {
    if (!Array.isArray(engines) || !engines.length) {
      return '<p class="engine-inventory-empty">No engine inventory status has been reported.</p>';
    }
    return `<section class="engine-inventory-details" aria-label="Inference engine inventory">
    <h4>Inference engine inventory</h4>
    ${engines.map((engine) => {
      const state = dashboardViewModel.engineInventoryState(engine);
      const checkedAt = engine.last_checked_at ? new Date(engine.last_checked_at) : null;
      const successfulAt = engine.last_successful_at ? new Date(engine.last_successful_at) : null;
      const checkedLabel = checkedAt && Number.isFinite(checkedAt.getTime()) ? checkedAt.toLocaleString() : "Not checked";
      const successfulLabel = successfulAt && Number.isFinite(successfulAt.getTime()) ? successfulAt.toLocaleString() : "No successful inventory check";
      const count = Math.max(0, Number(engine.model_count) || 0);
      const error = state === "error" ? `<p class="engine-inventory-error">Inventory check failed: ${escapeHtml(engine.last_error || "No error details were reported.")}</p>` : "";
      return `<article class="engine-inventory-row ${state}">
        <div class="engine-inventory-heading">
          <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
          <span class="engine-inventory-state">${escapeHtml(dashboardViewModel.engineInventoryLabel(state))}</span>
          <span>${count} known model${count === 1 ? "" : "s"}</span>
        </div>
        <small>Last checked: ${escapeHtml(checkedLabel)} · Last successful inventory: ${escapeHtml(successfulLabel)}</small>
        ${error}
      </article>`;
    }).join("")}
  </section>`;
  }
  function nodeContextDetailsMarkup(node, expanded) {
    const models = (node.models || []).map((model) => {
      const tags = Array.isArray(model.tags) && model.tags.length ? model.tags.join(", ") : "general";
      const engine = model.engine ? ` · ${escapeHtml(model.engine)}` : "";
      return `<div class="overview-node-model">
      <strong>${escapeHtml(model.name || "Unknown model")}</strong>
      <span>${escapeHtml(tags)}${engine}</span>
    </div>`;
    }).join("") || '<div class="overview-node-model"><span>No models reported</span></div>';
    const score = Number.isFinite(node.hardware?.score) ? ` · capability score ${node.hardware.score.toFixed(1)}` : "";
    return `<details class="overview-node-context"${expanded ? " open" : ""}>
    <summary>Hardware, engines &amp; models</summary>
    <div class="overview-node-context-body">
      <div>${escapeHtml(dashboardViewModel.formatNodeHardware(node))}</div>
      <div>Active tasks: ${Number(node.active_tasks) || 0}${score}</div>
      ${engineStatusSummaryMarkup(node.engines)}
      ${engineStatusDetailsMarkup(node.engines)}
      <div class="overview-node-models">${models}</div>
    </div>
  </details>`;
  }

  // src/views/shards.js
  var shardGroupList = document.getElementById("shard-group-list");
  var shardGroupStatus = document.getElementById("shard-group-status");
  var refreshShardGroupsButton = document.getElementById("refresh-shard-groups");
  var deletingShardGroups = /* @__PURE__ */ new Set();
  var shardGroupActions = /* @__PURE__ */ new Map();
  var shardMemberEditor = "";
  var shardMemberSelection = null;
  function renderShardGroups(groups = dashboardState.shardGroups) {
    if (!shardGroupList) return;
    shardGroupList.innerHTML = groups.length ? groups.map(shardGroupCard).join("") : '<p class="setup-empty">No shard groups are currently configured.</p>';
    if (shardGroupStatus) {
      shardGroupStatus.textContent = dashboardState.shardGroupsError || `${groups.length} shard ${groups.length === 1 ? "group" : "groups"}`;
    }
  }
  function shardGroupCard(group) {
    const statusClass = group.state === "ready" ? "healthy" : group.state === "failed" || group.state === "degraded" ? "unhealthy" : "unknown";
    const members = (group.members || []).map((member) => `
    <div class="pipeline-stage">
      <div class="pipeline-stage-heading">
        <strong>${escapeHtml(member.role)} · ${escapeHtml(member.node_id)}</strong>
        <span class="badge ${member.started && group.state === "ready" ? "healthy" : "unknown"}">${escapeHtml(member.engine_kind)}${member.started && group.state !== "ready" ? " · started, group not ready" : ""}</span>
      </div>
      <div class="row"><span>Engine</span><span>${escapeHtml(member.engine_id)}</span></div>
      ${Number.isInteger(member.rank) ? `<div class="row"><span>Rank</span><span>${member.rank}</span></div>` : ""}
    </div>
  `).join("");
    const rationale = group.plan?.rationale?.length ? group.plan.rationale : group.rationale || [];
    const rationaleMarkup = rationale.length ? `<ul>${rationale.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : '<p class="pipeline-empty">No placement rationale was recorded.</p>';
    const actions = group.state === "ready" || group.state === "degraded" ? ["stop", "restart"] : group.state === "stopped" || group.state === "failed" ? ["start", "restart"] : [];
    const busy = deletingShardGroups.has(group.id) || shardGroupActions.has(group.id);
    const actionMarkup = actions.map(
      (action) => `<button type="button" class="btn-secondary" data-shard-group="${escapeHtml(group.id)}" data-shard-action="${action}" ${busy ? "disabled" : ""}>${action[0].toUpperCase() + action.slice(1)}</button>`
    ).join(" ");
    return `
    <article class="card pipeline-model-card">
      <h3>${escapeHtml(group.model?.name || group.id)} <span class="badge ${statusClass}">${escapeHtml(group.state)}</span></h3>
      <div class="row"><span>Group</span><span>${escapeHtml(group.id)}</span></div>
      <div class="row"><span>Mode</span><span>${escapeHtml(group.mode)}</span></div>
      ${group.error ? `<p class="pipeline-action-message">${escapeHtml(group.error)}</p>` : ""}
      ${actionMarkup}
      ${actions.length ? `<button type="button" class="btn-secondary" data-edit-shard-members="${escapeHtml(group.id)}" ${busy ? "disabled" : ""}>Edit members</button>` : ""}
      ${shardMemberEditor === group.id ? shardMemberEditorMarkup(group, busy) : ""}
      ${shardGroupActions.has(group.id) ? `<p role="status">${escapeHtml(shardGroupActions.get(group.id))} in progress…</p>` : ""}
      ${group.state === "failed" ? `<button type="button" class="btn-secondary" data-delete-shard-group="${escapeHtml(group.id)}" ${busy ? "disabled" : ""}>${deletingShardGroups.has(group.id) ? "Deleting…" : "Delete failed group"}</button>` : ""}
      <div class="pipeline-stage-list">${members || '<p class="pipeline-empty">No members are recorded.</p>'}</div>
      <strong>Plan rationale</strong>
      ${rationaleMarkup}
    </article>
  `;
  }
  function shardMemberEditorMarkup(group, busy) {
    const head = (group.members || []).find((member) => member.role === "head")?.node_id;
    const selected = shardMemberSelection || new Set(group.pinned_nodes?.length ? group.pinned_nodes : (group.members || []).map((member) => member.node_id));
    const nodes = new Map(dashboardState.nodes.map((node) => [node.id, node]));
    for (const id of selected) if (!nodes.has(id)) nodes.set(id, { id, healthy: false });
    return `<fieldset data-shard-members-editor="${escapeHtml(group.id)}" ${busy ? "disabled" : ""}>
    <legend>Member nodes</legend>
    <p>Changing members drains inference and reloads the model. The model head must remain selected.</p>
    ${[...nodes.values()].map((node) => `<label class="shard-node-option">
      <input type="checkbox" data-shard-member="${escapeHtml(node.id)}" ${selected.has(node.id) ? "checked" : ""} ${node.id === head || !node.healthy && !selected.has(node.id) ? "disabled" : ""}>
      <span>${escapeHtml(node.id)}${node.id === head ? " (model head)" : ""}${node.healthy ? "" : " (offline)"}</span>
    </label>`).join("")}
    <button type="button" data-apply-shard-members="${escapeHtml(group.id)}">Apply and reload</button>
    <button type="button" data-cancel-shard-members="${escapeHtml(group.id)}">Cancel</button>
  </fieldset>`;
  }
  async function applyShardMembers(id) {
    const group = dashboardState.shardGroups.find((entry) => entry.id === id);
    if (!group || shardGroupActions.has(id) || deletingShardGroups.has(id)) return;
    const editor = [...shardGroupList.querySelectorAll("[data-shard-members-editor]")].find((entry) => entry.dataset.shardMembersEditor === id);
    if (!editor) return;
    const nodeIDs = [...editor.querySelectorAll("[data-shard-member]:checked")].map((input) => input.dataset.shardMember);
    if (!nodeIDs.length) {
      dashboardState.shardGroupsError = "Select at least one shard member.";
      renderShardGroups();
      return;
    }
    shardGroupActions.set(id, "Updating membership");
    dashboardState.shardGroupsError = "";
    renderShardGroups();
    let message = "";
    try {
      if (group.mode === "llamacpp-rpc") {
        for (const nodeID of nodeIDs) {
          const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeID)}/engines/managed`);
          if (!Array.isArray(engines)) throw new Error(`Invalid engine inventory from ${nodeID}`);
          if (!engines.some((engine) => engine.kind?.toLowerCase() === "ggml-rpc-server")) {
            await fetchJSON(`/api/nodes/${encodeURIComponent(nodeID)}/engines/managed/install-worker`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ source: "official" })
            });
          }
        }
      }
      const updated = await fetchJSON(`/api/shards/${encodeURIComponent(id)}/members`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_ids: nodeIDs })
      });
      if (updated?.id !== id || !updated.state) throw new Error("Invalid membership response");
      dashboardState.shardGroups = dashboardState.shardGroups.map((entry) => entry.id === id ? updated : entry);
      shardMemberEditor = "";
    } catch (error) {
      message = `Unable to change shard members: ${error.message}`;
      console.error("shard membership update failed", error);
      await refreshShardGroups();
    } finally {
      shardGroupActions.delete(id);
      if (message) dashboardState.shardGroupsError = message;
      renderShardGroups();
    }
  }
  async function deleteFailedShardGroup(id) {
    const group = dashboardState.shardGroups.find((entry) => entry.id === id);
    if (!group || group.state !== "failed" || deletingShardGroups.has(id) || shardGroupActions.has(id)) return;
    deletingShardGroups.add(id);
    dashboardState.shardGroupsError = "";
    renderShardGroups();
    try {
      await fetchJSON(`/api/shards/${encodeURIComponent(id)}`, { method: "DELETE" });
      dashboardState.shardGroups = dashboardState.shardGroups.filter((entry) => entry.id !== id);
    } catch (error) {
      dashboardState.shardGroupsError = `Unable to delete shard group ${id}: ${error.message}`;
      console.error("shard group deletion failed", error);
    } finally {
      deletingShardGroups.delete(id);
      renderShardGroups();
    }
  }
  async function runShardGroupAction(id, action) {
    const group = dashboardState.shardGroups.find((entry) => entry.id === id);
    if (!group || deletingShardGroups.has(id) || shardGroupActions.has(id)) return;
    const allowed = group.state === "ready" || group.state === "degraded" ? ["stop", "restart"] : group.state === "stopped" || group.state === "failed" ? ["start", "restart"] : [];
    if (!allowed.includes(action)) return;
    shardGroupActions.set(id, action);
    dashboardState.shardGroupsError = "";
    renderShardGroups();
    let actionError = "";
    try {
      const updated = await fetchJSON(`/api/shards/${encodeURIComponent(id)}/${action}`, { method: "POST" });
      if (!updated || updated.id !== id || !updated.state) throw new Error("response did not include the updated shard group");
      dashboardState.shardGroups = dashboardState.shardGroups.map((entry) => entry.id === id ? updated : entry);
    } catch (error) {
      actionError = `Unable to ${action} shard group ${id}: ${error.message}`;
      console.error("shard group action failed", error);
      await refreshShardGroups();
    } finally {
      if (actionError) dashboardState.shardGroupsError = actionError;
      shardGroupActions.delete(id);
      renderShardGroups();
    }
  }
  shardGroupList?.addEventListener("click", (event) => {
    const edit = event.target.closest("[data-edit-shard-members]");
    if (edit) {
      shardMemberEditor = edit.dataset.editShardMembers;
      shardMemberSelection = null;
      renderShardGroups();
      return;
    }
    if (event.target.closest("[data-cancel-shard-members]")) {
      shardMemberEditor = "";
      shardMemberSelection = null;
      renderShardGroups();
      return;
    }
    const apply = event.target.closest("[data-apply-shard-members]");
    if (apply) {
      void applyShardMembers(apply.dataset.applyShardMembers);
      return;
    }
    const action = event.target.closest("[data-shard-action]");
    if (action) {
      void runShardGroupAction(action.dataset.shardGroup, action.dataset.shardAction);
      return;
    }
    const button = event.target.closest("[data-delete-shard-group]");
    if (button) void deleteFailedShardGroup(button.dataset.deleteShardGroup);
  });
  async function refreshShardGroups() {
    if (!shardGroupList) return;
    if (shardGroupStatus) shardGroupStatus.textContent = "Refreshing shard groups…";
    try {
      const response = await fetchJSON("/api/shards");
      if (!Array.isArray(response?.groups)) throw new Error("response did not include a groups array");
      dashboardState.shardGroups = response.groups;
      dashboardState.shardGroupsError = "";
    } catch (error) {
      dashboardState.shardGroupsError = `Unable to load shard groups: ${error.message}`;
      console.error("shard group refresh failed", error);
    }
    renderShardGroups();
  }
  refreshShardGroupsButton?.addEventListener("click", () => void refreshShardGroups());
  shardGroupList?.addEventListener("change", (event) => {
    if (!event.target.matches("[data-shard-member]")) return;
    const editor = event.target.closest("[data-shard-members-editor]");
    shardMemberSelection = new Set([...editor.querySelectorAll("[data-shard-member]:checked")].map((input) => input.dataset.shardMember));
  });

  // src/views/shard-setup.js
  var shardSetupDialog = document.getElementById("shard-group-setup-dialog");
  var shardSetupNodes = document.getElementById("shard-group-setup-nodes");
  var shardSetupMode = document.getElementById("shard-group-setup-mode");
  var shardSetupSource = document.getElementById("shard-group-setup-source");
  var shardSetupModel = document.getElementById("shard-group-setup-model");
  var shardSetupHead = document.getElementById("shard-group-setup-head");
  var shardSetupContext = document.getElementById("shard-group-setup-context");
  var shardSetupStatus = document.getElementById("shard-group-setup-status");
  var shardSetupCreate = document.getElementById("shard-group-setup-create");
  var shardSetupProfiles = /* @__PURE__ */ new Map();
  var shardSetupInventory = /* @__PURE__ */ new Map();
  var shardSetupGeneration = 0;
  var shardSetupPending = false;
  var shardSetupInventoryLoading = false;
  var shardSetupHeadModels = /* @__PURE__ */ new Map();
  function openShardGroupWizard(nodeID) {
    if (!shardSetupDialog) return;
    const node = dashboardState.nodes.find((item) => item.id === nodeID);
    if (!node?.healthy) return;
    shardSetupProfiles = /* @__PURE__ */ new Map();
    shardSetupInventory = /* @__PURE__ */ new Map();
    shardSetupPending = false;
    shardSetupInventoryLoading = true;
    shardSetupMode.value = "llamacpp-rpc";
    shardSetupSource.value = "official";
    shardSetupContext.value = "4096";
    shardSetupHead.innerHTML = "";
    renderShardSetupNodes(nodeID, true);
    shardSetupDialog.showModal();
    return loadShardSetupInventory();
  }
  function renderShardSetupNodes(selectedNodeID, reset = false) {
    const selected = reset ? /* @__PURE__ */ new Set([selectedNodeID]) : new Set(
      [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")].map((input) => input.dataset.nodeId)
    );
    const nodes = dashboardState.nodes.filter((node) => node.healthy);
    shardSetupNodes.innerHTML = nodes.length ? nodes.map((node) => {
      const eligibility = shardSetupNodeEligibility(
        node.id,
        shardSetupInventory,
        shardSetupMode.value,
        shardSetupInventoryLoading
      );
      const checked = selected.has(node.id) && (eligibility.eligible || shardSetupInventoryLoading);
      const freeMemory = (node.hardware?.devices || []).filter((device) => device.kind === "gpu" || device.kind === "npu").reduce((total, device) => total + Number(device.free_memory_mb || 0), 0);
      const memory = freeMemory > 0 ? `${(freeMemory / 1024).toFixed(1)} GB free device memory` : "No measured free accelerator memory; cannot receive layers";
      return `
        <label class="shard-node-option ${eligibility.eligible ? "" : "unavailable"}">
          <input type="checkbox" data-node-id="${escapeHtml(node.id)}" ${checked ? "checked" : ""} ${eligibility.eligible ? "" : "disabled"} />
          <span><strong>${escapeHtml(node.id)}</strong><small>${escapeHtml(memory)}</small><small>${escapeHtml(eligibility.reason)}</small></span>
        </label>`;
    }).join("") : '<p class="setup-empty">No healthy nodes are available.</p>';
    shardSetupNodes.querySelectorAll("[data-node-id]").forEach((input) => {
      input.addEventListener("change", () => void loadShardSetupModels());
    });
  }
  function shardSetupNodeEligibility(nodeID, inventory, mode, inventoryLoading = false) {
    if (inventoryLoading || !inventory.has(nodeID)) {
      return { eligible: false, reason: "Checking managed engines…" };
    }
    const status = inventory.get(nodeID);
    if (status.error) {
      return { eligible: false, reason: `Managed engine check failed: ${status.error}` };
    }
    const engines = status.engines || [];
    const requiredKind = mode === "vllm-mp" ? "vllm" : "ggml-rpc-server";
    if (!engines.some((engine) => engine.kind?.toLowerCase() === requiredKind)) {
      if (mode === "llamacpp-rpc") {
        return { eligible: true, reason: "RPC worker will download and install automatically when selected" };
      }
      return { eligible: false, reason: `No configured ${requiredKind}` };
    }
    if (mode === "llamacpp-rpc" && !engines.some((engine) => engine.kind?.toLowerCase() === "llama-server")) {
      return { eligible: true, reason: "Worker ready; a selected node also needs llama-server" };
    }
    return { eligible: true, reason: "Managed worker ready" };
  }
  async function loadShardSetupInventory() {
    const generation = ++shardSetupGeneration;
    const nodes = dashboardState.nodes.filter((node) => node.healthy);
    shardSetupInventoryLoading = true;
    shardSetupInventory = /* @__PURE__ */ new Map();
    renderShardSetupNodes();
    shardSetupModel.innerHTML = '<option value="">Checking worker engines…</option>';
    shardSetupModel.disabled = true;
    shardSetupCreate.disabled = true;
    shardSetupStatus.textContent = "Checking managed engines on healthy nodes…";
    const inventory = await Promise.all(nodes.map(async (node) => {
      try {
        const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/engines/managed`);
        if (!Array.isArray(engines)) throw new Error("managed-engine response was not an array");
        return { nodeID: node.id, engines };
      } catch (error) {
        return { nodeID: node.id, error: error.message, engines: [] };
      }
    }));
    if (generation !== shardSetupGeneration) return;
    shardSetupInventory = new Map(inventory.map(({ nodeID, ...status }) => [nodeID, status]));
    shardSetupInventoryLoading = false;
    renderShardSetupNodes();
    return loadShardSetupModels();
  }
  async function loadShardSetupModels() {
    if (shardSetupPending) return;
    const generation = ++shardSetupGeneration;
    const nodeIDs = [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")].map((input) => input.dataset.nodeId);
    const mode = shardSetupMode.value;
    const installationWarnings = [];
    shardSetupProfiles = /* @__PURE__ */ new Map();
    shardSetupHeadModels = /* @__PURE__ */ new Map();
    shardSetupHead.disabled = true;
    shardSetupModel.innerHTML = '<option value="">Loading compatible models…</option>';
    shardSetupModel.disabled = true;
    shardSetupCreate.disabled = true;
    shardSetupStatus.textContent = "Reading model metadata from managed engines…";
    if (shardSetupInventoryLoading) {
      shardSetupStatus.textContent = "Checking managed engines on healthy nodes…";
      return;
    }
    if (!nodeIDs.length) {
      shardSetupModel.innerHTML = '<option value="">Select at least one node</option>';
      shardSetupStatus.textContent = mode === "vllm-mp" ? "Select nodes with configured vLLM workers. Automatic vLLM installation is not supported." : "Select workers. Missing RPC workers download and install automatically from the selected release source.";
      return;
    }
    const inventory = nodeIDs.map((nodeID) => ({
      nodeID,
      ...shardSetupInventory.get(nodeID)
    }));
    if (mode === "llamacpp-rpc") {
      shardSetupStatus.textContent = "Installing missing RPC workers and checking idle installations for updates…";
      const installations = await Promise.all(inventory.map(async (item) => {
        const worker = item.engines?.find((engine) => engine.kind?.toLowerCase() === "ggml-rpc-server");
        if (item.error || worker && worker.id !== "maistr0-auto-rpc") return null;
        try {
          const result = await fetchJSON(
            `/api/nodes/${encodeURIComponent(item.nodeID)}/engines/managed/install-worker`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ source: shardSetupSource.value })
            }
          );
          if (!Array.isArray(result?.engines)) throw new Error("worker installer did not return engine inventory");
          item.engines = result.engines;
          if (Array.isArray(result.devices)) {
            const node = dashboardState.nodes.find((node2) => node2.id === item.nodeID);
            if (node) node.hardware = { ...node.hardware, devices: result.devices };
          }
          return result.warning ? `${item.nodeID}: ${result.warning}` : null;
        } catch (error) {
          item.error = `Worker installation failed: ${error.message}`;
          return null;
        }
      }));
      if (generation !== shardSetupGeneration) return;
      for (const item of inventory) {
        if (!item.error) shardSetupInventory.set(item.nodeID, { engines: item.engines });
      }
      renderShardSetupNodes();
      installationWarnings.push(...installations.filter(Boolean));
    }
    const requiredKind = mode === "vllm-mp" ? "vllm" : "ggml-rpc-server";
    const unavailable = inventory.filter(
      ({ nodeID, engines }) => !engines?.some((engine) => engine.kind?.toLowerCase() === requiredKind)
    );
    const failedInventory = inventory.filter((item) => item.error);
    if (unavailable.length || failedInventory.length) {
      const details = [
        ...unavailable.map(({ nodeID }) => `${nodeID} has no configured ${requiredKind}`),
        ...failedInventory.map(({ nodeID, error }) => `${nodeID}: ${error}`)
      ];
      shardSetupModel.innerHTML = '<option value="">No compatible model setup</option>';
      shardSetupStatus.textContent = details.join("; ");
      return;
    }
    if (mode === "llamacpp-rpc") {
      return loadShardSetupHeadModels(inventory, generation, installationWarnings);
    }
    const profileEngines = inventory.map((item) => ({
      nodeID: item.nodeID,
      engine: item.engines.find((entry) => entry.kind?.toLowerCase() === "vllm")
    }));
    if (!profileEngines.length) {
      shardSetupModel.innerHTML = '<option value="">No model head is configured</option>';
      shardSetupStatus.textContent = "No vLLM engine is configured on every selected node.";
      return;
    }
    async function loadShardSetupHeadModels(inventory2, generation2, warnings) {
      const previous2 = shardSetupHead.value;
      shardSetupHead.innerHTML = inventory2.map(({ nodeID }) => `<option value="${escapeHtml(nodeID)}">${escapeHtml(nodeID)}</option>`).join("");
      if (inventory2.some((item) => item.nodeID === previous2)) shardSetupHead.value = previous2;
      shardSetupHead.disabled = false;
      const head = inventory2.find((item) => item.nodeID === shardSetupHead.value);
      if (!head) return;
      const candidates2 = [];
      const errors = [...warnings];
      for (const engine of head.engines.filter((entry) => entry.kind === "llama-server" && entry.id !== "maistr0-auto-head")) {
        try {
          const profile = await fetchJSON(`/api/nodes/${encodeURIComponent(head.nodeID)}/engines/managed/${encodeURIComponent(engine.id)}/profile?requested_context=4096`);
          if (!validShardModelProfile(profile)) throw new Error("incomplete managed model profile");
          candidates2.push({ key: `engine:${engine.id}`, nodeID: head.nodeID, engineID: engine.id, profile });
        } catch (error) {
          errors.push(`${head.nodeID}: ${engine.id}: ${error.message}`);
        }
      }
      const operatorHead = head.engines.some((entry) => entry.kind === "llama-server" && entry.id !== "maistr0-auto-head");
      if (!operatorHead && head.engines.some((entry) => entry.id === "maistr0-auto-rpc")) {
        try {
          const result = await fetchJSON(`/api/nodes/${encodeURIComponent(head.nodeID)}/engines/managed/head-models?requested_context=4096`);
          if (!Array.isArray(result?.models) || !Array.isArray(result?.warnings)) throw new Error("invalid local model inventory");
          errors.push(...result.warnings);
          for (const model of result.models) {
            if (validShardModelProfile(model.profile)) {
              candidates2.push({ key: `local:${model.id}`, nodeID: head.nodeID, localID: model.id, profile: model.profile });
            }
          }
        } catch (error) {
          errors.push(`${head.nodeID}: local model discovery failed: ${error.message}`);
        }
      } else if (!operatorHead && candidates2.length === 0) {
        errors.push("This node has an operator-configured RPC worker. Configure a matching managed llama-server head or choose an automatically installed worker node.");
      }
      if (generation2 !== shardSetupGeneration) return;
      shardSetupHeadModels = new Map(candidates2.map((entry) => [entry.key, entry]));
      shardSetupProfiles = new Map(candidates2.map((entry) => [entry.key, entry.profile]));
      shardSetupModel.innerHTML = candidates2.length ? candidates2.map((entry) => `<option value="${escapeHtml(entry.key)}">${escapeHtml(entry.profile.name)} · ${entry.localID ? "local GGUF" : "managed head"} · ${entry.profile.layer_count} layers · ${formatBytes(entry.profile.file_size_bytes || entry.profile.weights_size_bytes)}</option>`).join("") : '<option value="">No compatible local model found on this head</option>';
      shardSetupModel.disabled = candidates2.length === 0;
      shardSetupCreate.disabled = candidates2.length === 0;
      shardSetupStatus.textContent = [
        candidates2.length ? "Choose a model. Local Ollama/LM Studio GGUF models configure the installed llama-server automatically when creating the group." : "No readable GGUF model found. Choose another head node or install a GGUF model in Ollama/LM Studio on this node first.",
        ...errors
      ].join(" ");
    }
    const profilesByEngine = await Promise.all(profileEngines.map(async ({ nodeID, engine }) => {
      try {
        const profile = await fetchJSON(
          `/api/nodes/${encodeURIComponent(nodeID)}/engines/managed/${encodeURIComponent(engine.id)}/profile?requested_context=4096`
        );
        return { nodeID, profile };
      } catch (error) {
        return { nodeID, error: `${engine.id}: ${error.message}` };
      }
    }));
    if (generation !== shardSetupGeneration) return;
    const usable = profilesByEngine.filter((item) => item.profile && validShardModelProfile(item.profile));
    const profileErrors = profilesByEngine.filter((item) => item.error).map((item) => `${item.nodeID}: ${item.error}`);
    if (!usable.length) {
      shardSetupModel.innerHTML = '<option value="">No readable model metadata</option>';
      shardSetupStatus.textContent = profileErrors.length ? profileErrors.join("; ") : "Configured models are missing dimensions or a measurable weight size.";
      return;
    }
    const profilesByNode = new Map(usable.map((item) => [item.nodeID, item.profile]));
    const first = profilesByNode.get(nodeIDs[0]);
    const matching = first && nodeIDs.every((nodeID) => sameShardModelProfile(first, profilesByNode.get(nodeID)));
    const candidates = matching ? [first] : [];
    shardSetupProfiles = new Map(candidates.map((profile) => [profile.name, profile]));
    const previous = shardSetupModel.value;
    shardSetupModel.innerHTML = candidates.length ? candidates.map((profile) => `<option value="${escapeHtml(profile.name)}">${escapeHtml(profile.name)} · ${profile.layer_count} layers · ${formatBytes(profile.weights_size_bytes || profile.file_size_bytes)}</option>`).join("") : '<option value="">No compatible model across selected nodes</option>';
    if (candidates.some((profile) => profile.name === previous)) shardSetupModel.value = previous;
    shardSetupModel.disabled = candidates.length === 0;
    shardSetupCreate.disabled = candidates.length === 0;
    shardSetupStatus.textContent = candidates.length ? [
      `${candidates.length} model${candidates.length === 1 ? "" : "s"} have readable metadata. Placement will use current free device memory and a safety margin.`,
      ...installationWarnings
    ].join(" ") : [
      "The selected nodes must have the same configured vLLM model and matching profile metadata.",
      ...profileErrors
    ].join(" ");
  }
  function validShardModelProfile(profile) {
    return Boolean(
      profile && typeof profile.name === "string" && profile.name.trim() && Number.isInteger(profile.layer_count) && profile.layer_count > 0 && Number.isInteger(profile.hidden_size) && profile.hidden_size > 0 && Number.isInteger(profile.kv_heads) && profile.kv_heads > 0 && Number.isInteger(profile.head_dim) && profile.head_dim > 0 && (Number(profile.weights_size_bytes) > 0 || Number(profile.file_size_bytes) > 0)
    );
  }
  function sameShardModelProfile(left, right) {
    return Boolean(right && left.name.toLowerCase() === right.name.toLowerCase() && left.architecture === right.architecture && left.parameters === right.parameters && left.layer_count === right.layer_count && left.hidden_size === right.hidden_size && left.kv_heads === right.kv_heads && left.head_dim === right.head_dim && left.vocab_size === right.vocab_size && left.quantization === right.quantization && (left.weights_size_bytes || left.file_size_bytes) === (right.weights_size_bytes || right.file_size_bytes));
  }
  async function createShardGroupFromWizard(event) {
    event.preventDefault();
    if (shardSetupPending) return;
    const nodeIDs = [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")].map((input) => input.dataset.nodeId);
    const selectedProfile = shardSetupProfiles.get(shardSetupModel.value);
    const requestedContext = Number(shardSetupContext.value);
    if (!nodeIDs.length || !selectedProfile || !Number.isInteger(requestedContext) || requestedContext < 1 || requestedContext > 1 << 20) {
      shardSetupStatus.textContent = "Choose healthy nodes, a model with readable metadata, and a valid context length.";
      return;
    }
    const model = { ...selectedProfile, requested_context: requestedContext };
    const mode = shardSetupMode.value;
    const selectedHead = shardSetupHeadModels.get(shardSetupModel.value);
    shardSetupPending = true;
    shardSetupCreate.disabled = true;
    shardSetupStatus.textContent = "Planning memory-aware placement and starting managed engines…";
    try {
      let headEngineID = selectedHead?.engineID;
      if (selectedHead?.localID) {
        shardSetupStatus.textContent = `Configuring model head on ${selectedHead.nodeID}…`;
        const head = await fetchJSON(`/api/nodes/${encodeURIComponent(selectedHead.nodeID)}/engines/managed/configure-head`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ model_id: selectedHead.localID, requested_context: requestedContext })
        });
        if (!head?.id) throw new Error("model head configuration did not return an engine ID");
        headEngineID = head.id;
      }
      const group = await fetchJSON("/api/shards", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model,
          mode,
          pinned_nodes: nodeIDs,
          ...selectedHead ? { head_node_id: selectedHead.nodeID, head_engine_id: headEngineID } : {}
        })
      });
      if (!group || group.state !== "ready") throw new Error("the API did not confirm a ready shard group");
      dashboardState.shardGroups = [
        ...dashboardState.shardGroups.filter((existing) => existing.id !== group.id),
        group
      ];
      dashboardState.shardGroupsError = "";
      const allocations = (group.plan?.nodes || []).map((entry) => `${entry.node_id}: layers ${entry.start_layer}–${entry.end_layer}`).join("; ");
      shardSetupStatus.textContent = `Shard group ready: ${group.id}${allocations ? ` · ${allocations}` : ""}`;
      renderShardGroups();
      void refreshShardGroups();
    } catch (error) {
      shardSetupStatus.textContent = `Shard group creation failed: ${error.message}`;
      shardSetupCreate.disabled = false;
    } finally {
      shardSetupPending = false;
    }
  }
  shardSetupMode?.addEventListener("change", () => {
    renderShardSetupNodes();
    if (!shardSetupInventoryLoading) void loadShardSetupModels();
  });
  shardSetupSource?.addEventListener("change", () => {
    if (!shardSetupInventoryLoading) void loadShardSetupModels();
  });
  shardSetupHead?.addEventListener("change", () => void loadShardSetupModels());
  shardSetupCreate?.addEventListener("click", createShardGroupFromWizard);
  shardSetupDialog?.addEventListener("close", () => {
    shardSetupGeneration++;
    shardSetupPending = false;
  });

  // src/views/nodes.js
  function renderNodes(nodes) {
    dashboardState.nodes = nodes || [];
    if (!nodesList) return;
    if (!nodes || nodes.length === 0) {
      dashboardDOM.patchKeyedChildren(nodesList, [{ id: "__empty" }], {
        keyOf: (item) => item.id,
        signatureOf: () => "empty",
        renderItem: () => {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "No nodes registered yet.";
          return empty;
        }
      });
      return;
    }
    const ordered = sortNodesByCapability(nodes);
    const leaderID = ordered.find((n) => n.leader)?.id;
    const fastestID = ordered.reduce((best, node) => !best || node.fast_score > best.fast_score || node.fast_score === best.fast_score && node.id < best.id ? node : best, null)?.id;
    dashboardDOM.patchKeyedChildren(nodesList, ordered, {
      keyOf: (node) => node.id,
      signatureOf: (node) => JSON.stringify({
        node,
        modelPanelOpen: modelPanelOpen.has(node.id),
        modelPanel: lastFetchedModels.get(node.id)
      }),
      renderItem: (n) => {
        const card = document.createElement("div");
        card.className = "card" + (n.id === leaderID ? " leader" : "");
        const modelTags = (n.models || []).map((m) => `<button class="model-tag-btn" data-node="${escapeHtml(n.id)}" data-model="${escapeHtml(m.name)}" title="Directly test ${escapeHtml(m.name)} on ${escapeHtml(n.id)}" ${n.healthy ? "" : "disabled"}>⚡ ${escapeHtml(m.name)}</button>`).join("");
        const nodeActions = n.healthy ? `<button class="model-config-toggle" data-node="${escapeHtml(n.id)}">⚙ Configure models</button>
           <button class="node-model-refresh" data-node="${escapeHtml(n.id)}">↻ Rescan engines &amp; models</button>
           <button class="node-terminal-open" data-node="${escapeHtml(n.id)}">▸ Open node terminal</button>` : `<small class="node-offline-note">Offline · last seen ${formatRelativeTime(n.last_seen)}</small>`;
        const serviceActions = n.control_address ? `<div class="node-service-actions" aria-label="Node service controls">
            ${n.healthy ? `<button class="node-service-action" data-node="${escapeHtml(n.id)}" data-action="drain" ${n.draining ? "disabled" : ""}>Drain &amp; stop</button>
                 <button class="node-service-action" data-node="${escapeHtml(n.id)}" data-action="stop">Stop</button>
                 <button class="node-service-action" data-node="${escapeHtml(n.id)}" data-action="restart">Restart</button>` : `<button class="node-service-action" data-node="${escapeHtml(n.id)}" data-action="start">Start</button>`}
          </div>` : "";
        card.innerHTML = `
        <div class="node-card-heading"><h3>${escapeHtml(n.id)} <span class="badge ${n.healthy ? "healthy" : "unhealthy"}">${n.healthy ? "online" : "offline"}</span></h3></div>
        ${n.version_error ? `<div class="node-version-error">⚠ ${escapeHtml(n.version_error)}</div>` : ""}
        <div class="role-tags">${n.id === leaderID ? '<span class="role-tag coordinator">cluster leader · orchestrator</span>' : ""}${n.id === fastestID && n.id !== leaderID ? '<span class="role-tag worker">fastest</span>' : ""}</div>
        ${engineStatusSummaryMarkup(n.engines)}
        <div class="row"><span>Address</span><span>${escapeHtml(n.address)}</span></div>
        <div class="row"><span>OS / Arch</span><span>${escapeHtml(n.hardware.os)}/${escapeHtml(n.hardware.arch)}</span></div>
        <div class="row"><span>CPU cores</span><span>${n.hardware.cpu_cores}</span></div>
        <div class="row"><span>RAM</span><span>${(n.hardware.total_ram_mb / 1024).toFixed(1)} GB</span></div>
        ${nodeAcceleratorMarkup(n)}
        ${resourceHistoryMarkup(n)}
        ${!n.healthy ? `<div class="row"><span>Last seen</span><span>${escapeHtml(n.last_seen ? new Date(n.last_seen).toLocaleString() : "unknown")}</span></div>` : ""}
        <div class="row"><span>Active tasks</span><span>${n.active_tasks}</span></div>
        <div class="row"><span>Score</span><span>${n.hardware.score.toFixed(1)}</span></div>
        <div class="row"><span>Fast score</span><span>${(n.fast_score || 0).toFixed(1)}</span></div>
        <div class="row"><span>Default model</span><span>${escapeHtml(n.default_model || "automatic")}</span></div>
        <div>${modelTags || '<span style="color:var(--muted);font-size:0.75rem">No models installed</span>'}</div>
        ${nodeActions}
        ${n.draining ? '<small class="node-offline-note">Draining · waiting for active work</small>' : ""}
        ${serviceActions}
        <div class="model-config-panel" id="model-config-${cssId(n.id)}" style="display:${modelPanelOpen.has(n.id) ? "block" : "none"}"></div>
      `;
        card.querySelector(".node-terminal-open")?.addEventListener("click", () => openNodeTerminal(n));
        card.querySelector(".node-model-refresh")?.addEventListener("click", () => refreshNodeModels(n.id));
        card.querySelectorAll(".node-service-action").forEach((button) => {
          button.addEventListener("click", () => controlNodeService(n.id, button.dataset.action, button));
        });
        card.querySelector(".model-config-toggle")?.addEventListener("click", () => {
          if (modelPanelOpen.has(n.id)) {
            modelPanelOpen.delete(n.id);
            lastFetchedModels.delete(n.id);
          } else {
            modelPanelOpen.add(n.id);
          }
          renderNodes(nodes);
        });
        card.querySelectorAll(".model-tag-btn").forEach((btn) => {
          btn.addEventListener("click", async () => {
            const nodeId = btn.dataset.node;
            const model = btn.dataset.model;
            btn.textContent = "⏳ Testing…";
            try {
              const res = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/${encodeURIComponent(model)}/test`, { method: "POST" });
              btn.textContent = res.healthy ? `✓ ${model} (${res.duration_ms}ms)` : `✗ ${model} (err)`;
            } catch {
              btn.textContent = `✗ ${model}`;
            }
            setTimeout(() => {
              btn.textContent = `⚡ ${model}`;
            }, 3500);
          });
        });
        attachNodeContextMenu(card, n);
        if (modelPanelOpen.has(n.id)) {
          if (lastFetchedModels.has(n.id)) renderModelPanel(n.id, lastFetchedModels.get(n.id), n.engines);
          else loadModelPanel(n.id);
        }
        return card;
      }
    });
  }
  async function controlNodeService(nodeId, action, button) {
    if (action !== "start" && !window.confirm(`${action[0].toUpperCase()}${action.slice(1)} ${nodeId}?`)) return;
    const original = button.textContent;
    button.disabled = true;
    button.textContent = `${action}…`;
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/service/${action}`, { method: "POST" });
      button.textContent = action === "drain" ? "Draining…" : `${action} requested`;
      await refresh();
    } catch (err) {
      button.disabled = false;
      button.textContent = original;
      window.alert(`Unable to ${action} ${nodeId}: ${err.message}`);
    }
  }
  function renderSetupNodes(nodes) {
    if (!setupNodeList) return;
    const ordered = sortNodesByCapability(nodes || []);
    if (setupNodeSummary) {
      const online = ordered.filter((node) => node.healthy).length;
      setupNodeSummary.textContent = `${online} online · ${ordered.length - online} offline · ${ordered.length} total`;
    }
    if (!ordered.length) {
      dashboardDOM.patchKeyedChildren(setupNodeList, [{ id: "__empty" }], {
        keyOf: (item) => item.id,
        signatureOf: () => "empty",
        renderItem: () => {
          const empty = document.createElement("p");
          empty.className = "setup-empty";
          empty.textContent = "No cluster members are registered yet. Pair a node to add it to the cluster.";
          return empty;
        }
      });
      return;
    }
    dashboardDOM.patchKeyedChildren(setupNodeList, ordered, {
      keyOf: (node) => node.id,
      signatureOf: JSON.stringify,
      renderItem: (node) => {
        const online = Boolean(node.healthy);
        const card = document.createElement("article");
        card.className = `setup-node-card ${online ? "online" : "offline"}`;
        card.innerHTML = `
      <div class="setup-node-heading">
        <div><h4>${escapeHtml(node.id)}</h4><span class="panel-caption">${escapeHtml(node.address || "Address unavailable")}</span></div>
        <span class="setup-node-status ${online ? "online" : "offline"}"><span class="legend-dot ${online ? "online" : "offline"}"></span>${online ? "Online" : "Offline"}</span>
      </div>
      <div class="setup-node-details">
        <span>${(node.models || []).length} models</span>
        <span>${node.active_tasks || 0} active tasks</span>
        ${online ? "" : `<span>Last seen ${escapeHtml(node.last_seen ? formatRelativeTime(node.last_seen) : "unknown")}</span>`}
      </div>`;
        card.insertAdjacentHTML("beforeend", engineStatusSummaryMarkup(node.engines));
        attachNodeContextMenu(card, node);
        return card;
      }
    });
  }
  async function openNodeTerminal(node) {
    setTerminalNode(node);
    setTerminalDialogueId(null);
    const modal = document.getElementById("node-terminal");
    const title = document.getElementById("node-terminal-title");
    const models = document.getElementById("node-terminal-model");
    const messages = document.getElementById("node-terminal-messages");
    const status = document.getElementById("node-terminal-status");
    title.textContent = `${node.id} terminal`;
    messages.innerHTML = "";
    status.textContent = "Loading local models...";
    modal.hidden = false;
    try {
      const available = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models`);
      models.innerHTML = available.map((m) => `<option value="${escapeHtml(m.name)}">${escapeHtml(m.name)} · ${escapeHtml(m.engine)}</option>`).join("");
      status.textContent = available.length ? "Ready" : "No locally served models";
    } catch (err) {
      status.textContent = "Model discovery failed: " + err.message;
    }
    document.getElementById("node-terminal-input")?.focus();
  }
  function closeNodeTerminal() {
    document.getElementById("node-terminal").hidden = true;
    setTerminalNode(null);
    setTerminalDialogueId(null);
  }
  function appendTerminalMessage(role, content) {
    const messages = document.getElementById("node-terminal-messages");
    const item = document.createElement("div");
    item.className = `node-terminal-message ${role}`;
    item.innerHTML = `<strong>${role === "user" ? "You" : escapeHtml(terminalNode?.id || "Node")}</strong><div>${escapeHtml(content)}</div>`;
    messages.appendChild(item);
    messages.scrollTop = messages.scrollHeight;
  }
  document.getElementById("node-terminal-close")?.addEventListener("click", closeNodeTerminal);
  document.getElementById("node-terminal-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!terminalNode) return;
    const input = document.getElementById("node-terminal-input");
    const content = input.value.trim();
    const model = document.getElementById("node-terminal-model").value;
    const status = document.getElementById("node-terminal-status");
    if (!content || !model) return;
    input.value = "";
    appendTerminalMessage("user", content);
    status.textContent = "Generating...";
    setTerminalAbortController(new AbortController());
    if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = false;
    try {
      if (!terminalDialogueId) {
        const dialogue = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }), signal: terminalAbortController.signal });
        setTerminalDialogueId(dialogue.id);
      }
      const response = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/messages`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ content }), signal: terminalAbortController.signal });
      appendTerminalMessage("assistant", response.reply || response.error || "No response");
      status.textContent = `${response.model || model} · ${response.duration_ms || 0}ms`;
    } catch (err) {
      if (err.name === "AbortError") {
        appendTerminalMessage("assistant", "⚠️ Dialogue turn interrupted.");
        status.textContent = "Interrupted";
      } else {
        appendTerminalMessage("assistant", "Error: " + err.message);
        status.textContent = "Request failed";
      }
    } finally {
      setTerminalAbortController(null);
      if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = true;
    }
  });
  nodeTerminalInterruptBtn?.addEventListener("click", async () => {
    if (terminalNode && terminalDialogueId) {
      try {
        await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/interrupt`, {
          method: "POST"
        });
      } catch (err) {
        console.error("interrupt node dialogue failed", err);
      }
    }
    if (terminalAbortController) {
      terminalAbortController.abort();
    }
  });
  async function openNodeProperties(node) {
    const modal = document.getElementById("node-properties-modal");
    const form = document.getElementById("node-properties-form");
    const status = document.getElementById("node-properties-status");
    if (!modal || !form) return;
    setContextNode(node);
    status.textContent = "Loading properties...";
    modal.hidden = false;
    try {
      const [props, runtimeSettings] = await Promise.all([
        node.healthy ? fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/properties`) : {},
        fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/runtime-settings`)
      ]);
      nodeProperties.set(node.id, props);
      for (const [name, value] of Object.entries(props)) {
        const field = form.elements.namedItem(name);
        if (!field) continue;
        if (field.type === "checkbox") field.checked = Boolean(value);
        else if (name === "disabled_models") field.value = (value || []).join("\n");
        else field.value = value ?? "";
      }
      const runtimeFields = {
        runtime_role: "role",
        runtime_node_config_path: "node_config_path",
        runtime_orchestrator_config_path: "orchestrator_config_path",
        runtime_node_listen_addr: "node_listen_addr",
        runtime_orchestrator_listen_addr: "orchestrator_listen_addr",
        runtime_orchestrator_addr: "orchestrator_addr",
        runtime_tray_mode: "tray_mode",
        runtime_harness: "harness",
        runtime_memory_path: "memory_path",
        runtime_auto_open_browser: "auto_open_browser",
        runtime_discovery_enabled: "discovery_enabled",
        runtime_allow_peer_updates: "allow_peer_updates",
        runtime_no_peer_updates: "no_peer_updates"
      };
      for (const [fieldName, settingName] of Object.entries(runtimeFields)) {
        const field = form.elements.namedItem(fieldName);
        if (!field) continue;
        const value = runtimeSettings[settingName];
        if (field.type === "checkbox") field.checked = Boolean(value);
        else field.value = value ?? "";
      }
      document.getElementById("node-properties-title").textContent = `${node.id} properties`;
      document.getElementById("node-properties-subtitle").textContent = "Edit node properties and persistent startup settings. Startup changes apply after restart.";
      status.textContent = "";
      form.elements.namedItem("advertise_addr")?.focus();
    } catch (err) {
      status.textContent = "Unable to load properties: " + err.message;
    }
  }
  function closeNodeProperties() {
    const modal = document.getElementById("node-properties-modal");
    if (modal) modal.hidden = true;
  }
  async function saveNodeProperties(nodeId, form) {
    const status = document.getElementById("node-properties-status");
    const value = (name) => form.elements.namedItem(name)?.value.trim() || "";
    status.textContent = "Saving...";
    try {
      const node = dashboardState.nodes.find((item) => item.id === nodeId);
      if (node?.healthy) {
        await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/properties`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            advertise_addr: value("advertise_addr"),
            orchestrator_addr: value("orchestrator_addr"),
            default_model: value("default_model"),
            engine_name: value("engine_name"),
            engine_url: value("engine_url"),
            disabled_models: value("disabled_models").split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean),
            discovery_enabled: form.elements.namedItem("discovery_enabled").checked,
            auto_open_browser: form.elements.namedItem("auto_open_browser").checked,
            tray_mode: value("tray_mode"),
            memory_path: value("memory_path")
          })
        });
      }
      await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/runtime-settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          role: value("runtime_role"),
          node_config_path: value("runtime_node_config_path"),
          orchestrator_config_path: value("runtime_orchestrator_config_path"),
          node_listen_addr: value("runtime_node_listen_addr"),
          orchestrator_listen_addr: value("runtime_orchestrator_listen_addr"),
          orchestrator_addr: value("runtime_orchestrator_addr"),
          tray_mode: value("runtime_tray_mode"),
          harness: value("runtime_harness"),
          memory_path: value("runtime_memory_path"),
          auto_open_browser: form.elements.namedItem("runtime_auto_open_browser").checked,
          discovery_enabled: form.elements.namedItem("runtime_discovery_enabled").checked,
          allow_peer_updates: form.elements.namedItem("runtime_allow_peer_updates").checked,
          no_peer_updates: form.elements.namedItem("runtime_no_peer_updates").checked
        })
      });
      status.textContent = "Saved";
      await refresh();
      setTimeout(closeNodeProperties, 500);
    } catch (err) {
      status.textContent = "Error: " + err.message;
    }
  }
  async function refreshNodeModels(nodeId) {
    const button = [...nodesList?.querySelectorAll(".node-model-refresh") || []].find((candidate) => candidate.dataset.node === nodeId);
    if (button) button.textContent = "Rescanning...";
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" });
      lastFetchedModels.delete(nodeId);
      await refresh();
    } catch (err) {
      console.error("model refresh failed", err);
      if (button) button.textContent = "Refresh failed";
    }
  }
  refreshAllModelsBtn?.addEventListener("click", async () => {
    refreshAllModelsBtn.disabled = true;
    refreshAllModelsBtn.textContent = "Rescanning...";
    try {
      await fetchJSON("/api/nodes/models/refresh", { method: "POST" });
      await refresh();
      refreshAllModelsBtn.textContent = "Rescan complete";
    } catch (err) {
      refreshAllModelsBtn.textContent = "Refresh failed";
      console.error("all-node model refresh failed", err);
    } finally {
      setTimeout(() => {
        refreshAllModelsBtn.disabled = false;
        refreshAllModelsBtn.textContent = "↻ Rescan all nodes";
      }, 1800);
    }
  });
  function showNodeMenu(x, y) {
    const menu = document.getElementById("node-context-menu");
    if (!menu || !contextNode) return;
    const properties = menu.querySelector("[data-action=properties]");
    const pairing = menu.querySelector("[data-action=pair]");
    const nodeIsDiscovered = Boolean(contextNode.discovered);
    const joiningOrchestrator = Boolean(contextNode.joinOrchestrator);
    if (properties) {
      properties.textContent = joiningOrchestrator ? "Configure host properties" : `Configure ${contextNode.id}`;
      properties.disabled = !contextNode.healthy && !contextNode.control_address || nodeIsDiscovered || joiningOrchestrator;
    }
    if (pairing) {
      const currentOrchestrator = dashboardState.pairing.status?.orchestrator_id === contextNode.id;
      pairing.textContent = joiningOrchestrator ? dashboardState.pairing.status?.joined && currentOrchestrator ? `Re-pair with ${contextNode.id}` : `Join ${contextNode.id}'s cluster` : nodeIsDiscovered ? "Pair node to this cluster" : "Re-pair node with this cluster";
      pairing.disabled = !contextNode.healthy;
    }
    menu.querySelector("[data-action=open]").disabled = !contextNode.healthy || nodeIsDiscovered || joiningOrchestrator;
    menu.hidden = false;
    const bounds = menu.getBoundingClientRect();
    menu.style.left = `${Math.max(8, Math.min(x, window.innerWidth - bounds.width - 8))}px`;
    menu.style.top = `${Math.max(8, Math.min(y, window.innerHeight - bounds.height - 8))}px`;
  }
  function attachNodeContextMenu(element, node) {
    element.addEventListener("click", (event) => {
      if (!event.ctrlKey || !node.healthy || event.target.closest("button, input, select, textarea, a")) return;
      event.preventDefault();
      openShardGroupWizard(node.id);
    });
    element.addEventListener("contextmenu", (event) => {
      if (event.ctrlKey && node.healthy) {
        event.preventDefault();
        if (!document.getElementById("shard-group-setup-dialog")?.open) openShardGroupWizard(node.id);
        return;
      }
      event.preventDefault();
      setContextNode(node);
      showNodeMenu(event.clientX, event.clientY);
    });
  }
  function hideNodeMenu() {
    const menu = document.getElementById("node-context-menu");
    if (menu) menu.hidden = true;
  }
  document.addEventListener("click", hideNodeMenu);
  document.addEventListener("scroll", hideNodeMenu, true);
  document.getElementById("node-context-menu")?.addEventListener("click", async (event) => {
    const action = event.target.closest("[data-action]")?.dataset.action;
    if (!contextNode || !action) return;
    const node = contextNode;
    hideNodeMenu();
    if (action === "properties") {
      openNodeProperties(node);
    } else if (action === "pair") {
      openPairingDialog(node);
    } else if (action === "open") {
      window.open(node.address, "_blank", "noopener");
    } else if (action === "copy") {
      await navigator.clipboard?.writeText(node.address);
    }
  });
  document.getElementById("node-properties-close")?.addEventListener("click", closeNodeProperties);
  document.getElementById("node-properties-cancel")?.addEventListener("click", closeNodeProperties);
  document.getElementById("node-properties-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    if (contextNode) saveNodeProperties(contextNode.id, event.currentTarget);
  });
  document.getElementById("node-properties-modal")?.addEventListener("click", (event) => {
    if (event.target.id === "node-properties-modal") closeNodeProperties();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeNodeProperties();
  });
  function cssId(nodeId) {
    return nodeId.replace(/[^a-zA-Z0-9_-]/g, "_");
  }
  async function loadModelPanel(nodeId) {
    const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
    if (!panel) return;
    panel.innerHTML = '<p style="color:var(--muted)">Loading models…</p>';
    try {
      const models = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`);
      const state = new Map(models.map((m) => [m.name, m.enabled]));
      pendingModelState.set(nodeId, state);
      pendingDefaultModel.set(nodeId, (models.find((m) => m.default) || {}).name || "");
      lastFetchedModels.set(nodeId, models);
      const node = dashboardState.nodes.find((item) => item.id === nodeId);
      renderModelPanel(nodeId, models, node?.engines || []);
    } catch (err) {
      panel.innerHTML = `<p style="color:var(--bad)">Failed to load models: ${escapeHtml(err.message)}</p>`;
    }
  }
  function renderModelPanel(nodeId, models, engines = []) {
    const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
    if (!panel) return;
    const engineDetails = engineStatusDetailsMarkup(engines);
    if (!models || models.length === 0) {
      panel.innerHTML = `${engineDetails}<p style="color:var(--muted)">No models discovered on this node.</p>`;
      return;
    }
    const state = pendingModelState.get(nodeId);
    const defaultModel = pendingDefaultModel.get(nodeId) || "";
    const modelsByEngine = /* @__PURE__ */ new Map();
    for (const model of models) {
      const engineName = model.engine || "Unknown engine";
      if (!modelsByEngine.has(engineName)) modelsByEngine.set(engineName, []);
      modelsByEngine.get(engineName).push(model);
    }
    const rows = [...modelsByEngine.entries()].map(([engineName, engineModels]) => `
      <section class="model-config-engine-group">
        <h5>${escapeHtml(engineName)} · ${engineModels.length} model${engineModels.length === 1 ? "" : "s"}</h5>
        ${engineModels.map((m) => {
      const checked = state.get(m.name) ? "checked" : "";
      return `
        <label class="model-config-row">
          <input type="checkbox" data-model="${escapeHtml(m.name)}" ${checked} />
          <span class="model-config-name">${escapeHtml(m.name)}</span>
          <span class="model-config-meta">${escapeHtml((m.tags || []).join(", "))} · ${escapeHtml(m.engine || "Unknown engine")}${m.size_gb ? ` · ${Number(m.size_gb).toFixed(1)} GB` : ""}</span>
        </label>`;
    }).join("")}
      </section>
    `).join("");
    panel.innerHTML = `
    ${engineDetails}
    <h4>Installed models</h4>
    <label class="model-default-row">
      <span>Default model</span>
      <select class="model-default-select">
        <option value="">Automatic best match</option>
        ${models.map((m) => `<option value="${escapeHtml(m.name)}" ${m.name === defaultModel ? "selected" : ""} ${state.get(m.name) ? "" : "disabled"}>${escapeHtml(m.name)}</option>`).join("")}
      </select>
    </label>
    <div class="model-config-engine-groups">${rows}</div>
    <button class="model-config-save" data-node="${escapeHtml(nodeId)}">Save</button>
    <span class="model-config-status" id="model-config-status-${cssId(nodeId)}"></span>
  `;
    panel.querySelectorAll("input[type=checkbox]").forEach((cb) => {
      cb.addEventListener("change", () => {
        state.set(cb.dataset.model, cb.checked);
      });
    });
    panel.querySelector(".model-default-select").addEventListener("change", (event) => {
      pendingDefaultModel.set(nodeId, event.target.value);
    });
    panel.querySelector(".model-config-save").addEventListener("click", () => saveModelPanel(nodeId));
  }
  async function saveModelPanel(nodeId) {
    const statusEl = document.getElementById(`model-config-status-${cssId(nodeId)}`);
    const state = pendingModelState.get(nodeId);
    if (!state) return;
    const disabled = [...state.entries()].filter(([, enabled]) => !enabled).map(([name]) => name);
    const defaultModel = pendingDefaultModel.get(nodeId) || "";
    if (statusEl) statusEl.textContent = "Saving…";
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ disabled_models: disabled, default_model: defaultModel })
      });
      if (statusEl) statusEl.textContent = "Saved";
      lastFetchedModels.delete(nodeId);
      refresh();
    } catch (err) {
      if (statusEl) statusEl.textContent = "Error: " + err.message;
    }
  }

  // src/views/cluster.js
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
    const gauges = gpuGauge || npuGauge ? `<div class="node-accelerator-gauges">${gpuGauge}${npuGauge}</div>` : "";
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
    const window2 = capacity?.telemetry_freshness?.freshness_window_ms ?? 3e4;
    return state === "stale" || age < 0 || age > window2 ? "Stale resource telemetry" : "Fresh resource telemetry";
  }
  function resourceHistoryMarkup(node) {
    const hardware = node.hardware || {};
    const systemHistory = Array.isArray(node.system_history) ? node.system_history.slice(-60) : [];
    const series = [
      ...hardware.has_gpu ? [{
        key: "gpu",
        label: "GPU",
        samples: Array.isArray(node.gpu_history) ? node.gpu_history.slice(-60) : [],
        availableKey: "available",
        valueKey: "utilization_percent",
        currentAvailable: hardware.gpu_stats_available,
        currentValue: hardware.gpu_utilization_percent
      }] : [],
      ...hardware.has_npu ? [{
        key: "npu",
        label: "NPU",
        samples: Array.isArray(node.npu_history) ? node.npu_history.slice(-60) : [],
        availableKey: "available",
        valueKey: "utilization_percent",
        currentAvailable: hardware.npu_stats_available,
        currentValue: hardware.npu_utilization_percent
      }] : [],
      {
        key: "cpu",
        label: "CPU",
        samples: systemHistory,
        availableKey: "cpu_available",
        valueKey: "cpu_utilization_percent",
        currentAvailable: hardware.cpu_stats_available,
        currentValue: hardware.cpu_utilization_percent
      },
      {
        key: "ram",
        label: "RAM",
        samples: systemHistory,
        availableKey: "memory_available",
        valueKey: "memory_usage_percent",
        currentAvailable: hardware.memory_stats_available,
        currentValue: hardware.memory_usage_percent
      }
    ];
    const sampleCount = Math.max(1, ...series.map((item) => item.samples.length));
    const left = 8;
    const right = 172;
    const top = 7;
    const bottom = 39;
    const x = (index) => left + index / Math.max(sampleCount - 1, 1) * (right - left);
    const y = (value) => bottom - Math.max(0, Math.min(100, value)) / 100 * (bottom - top);
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
      return segments.map((points) => points.length === 1 ? `<circle class="resource-history-point resource-history-${item.key}" cx="${points[0].x.toFixed(1)}" cy="${points[0].y.toFixed(1)}" r="2.5"></circle>` : `<path class="resource-history-line resource-history-${item.key}" d="${points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ")}"></path>`).join("");
    }).join("");
    const legend = series.map((item) => {
      const value = item.currentValue;
      const current = item.currentAvailable && typeof value === "number" && Number.isFinite(value) ? `${Math.round(Math.max(0, Math.min(100, value)))}%` : "n/a";
      return `<span class="resource-history-item"><i class="resource-history-swatch resource-history-${item.key}"></i>${item.label} ${current}</span>`;
    }).join("");
    const labels = series.map((item) => item.label);
    const historyLabel = labels.length > 1 ? `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}` : labels[0];
    const hasSamples = series.some((item) => item.samples.some(
      (sample) => sample[item.availableKey] && typeof sample[item.valueKey] === "number" && Number.isFinite(sample[item.valueKey])
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
  var TASK_ACTIVE_STATES = /* @__PURE__ */ new Set(["pending", "running", "synthesizing", "awaiting_approval"]);
  var TASK_STATES = /* @__PURE__ */ new Set([...TASK_ACTIVE_STATES, "completed", "failed", "cancelled"]);
  var SUBTASK_STATES = /* @__PURE__ */ new Set(["pending", "running", "completed", "failed", "cancelled"]);
  function knownStatus(status, states) {
    return states.has(status) ? status : "unknown";
  }
  function nodeAssignments(tasks) {
    const assigned = /* @__PURE__ */ new Map();
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
      (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node])
    );
    const memberIDs = new Set(nodeList.map((node) => node.id));
    const discoveredNodes = (peers || []).filter((peer) => peer.role === "node" && peer.id && !memberIDs.has(peer.id)).map((peer) => ({
      id: peer.id,
      address: peer.peer_addr || peer.http_addr,
      healthy: true,
      discovered: true,
      last_seen: peer.last_seen,
      models: [],
      active_tasks: 0,
      hardware: {}
    }));
    const displayNodes = sortNodesByCapability([...nodeList, ...discoveredNodes]);
    const taskList = [...tasks || []].sort(
      (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
      const overviewTasks = [...activeTasks, ...recentTasks].sort(
        (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
        [...overviewNodes.querySelectorAll(".overview-node-context[open]")].map((details) => details.closest("[data-node-id]")?.dataset.nodeId).filter(Boolean)
      );
      const nodeItems = displayNodes.length ? displayNodes : [{ id: "__empty", empty: true }];
      dashboardDOM.patchKeyedChildren(overviewNodes, nodeItems, {
        keyOf: (item) => item.id,
        signatureOf: (item) => item.empty ? "empty" : JSON.stringify({
          node: item,
          capacity: capacityByNode.get(item.id),
          assignments: assignments.get(item.id),
          leaderID,
          expanded: expandedNodeIDs.has(item.id)
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
          const lastSeen = node.last_seen ? `${node.discovered ? "Seen" : "Last seen"} ${formatRelativeTime(node.last_seen)}` : "Last seen unavailable";
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
        }
      });
    }
    if (parts.peers !== false && clusterPeers) {
      const discovered = (peers || []).filter((peer) => peer.role && peer.role !== "node");
      clusterPeers.innerHTML = discovered.length ? `<span class="cluster-peers-label">Discovered peers</span>${discovered.map((peer) => `<span class="cluster-peer"><i class="legend-dot peer"></i><strong>${escapeHtml(peer.id || "unknown")}</strong><small>${escapeHtml(peer.role)} · ${escapeHtml(peer.http_addr || "address unavailable")}</small></span>`).join("")}` : "";
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

  // src/views/memory.js
  async function refreshMemory() {
    dashboardDOM.renderSkeleton(factsList, 2);
    dashboardDOM.renderSkeleton(experiencesList, 3);
    dashboardDOM.renderSkeleton(transcriptsList, 2);
    try {
      const query = memorySearch?.value.trim() || "";
      const experienceURL = query ? `/api/memory/experiences?q=${encodeURIComponent(query)}` : "/api/memory/experiences";
      const [insights, facts, experiences, dialogues] = await Promise.all([
        fetchJSON("/api/memory/insights"),
        fetchMemoryHistoryPage("facts", "/api/memory/facts", true),
        fetchMemoryHistoryPage("experiences", experienceURL, true),
        fetchMemoryHistoryPage("dialogues", "/api/memory/dialogues", true)
      ]);
      renderLearning(insights);
      renderFacts(facts, memoryHistoryPages.facts.hasMore, memoryHistoryPages.facts.loading);
      renderExperiences(experiences, memoryHistoryPages.experiences.hasMore, memoryHistoryPages.experiences.loading);
      renderTranscripts(dialogues, memoryHistoryPages.dialogues.hasMore, memoryHistoryPages.dialogues.loading);
    } catch (err) {
      console.error("memory refresh failed", err);
    }
  }
  async function fetchMemoryHistoryPage(name, baseURL, reset = false) {
    const state = memoryHistoryPages[name];
    if (!state) throw new Error(`unknown memory history page: ${name}`);
    if (reset) {
      state.generation++;
      state.items = [];
      state.nextCursor = "";
      state.hasMore = false;
      state.loading = false;
      state.url = baseURL;
    }
    if (state.loading || !reset && !state.hasMore) return state.items;
    const generation = state.generation;
    state.loading = true;
    const pageSize = name === "dialogues" ? 20 : name === "experiences" ? 30 : 50;
    const separator = state.url.includes("?") ? "&" : "?";
    let url = `${state.url}${separator}page_size=${pageSize}`;
    if (!reset && state.nextCursor) url += `&cursor=${encodeURIComponent(state.nextCursor)}`;
    try {
      const response = await fetchJSON(url);
      const page = Array.isArray(response) ? { items: response, next_cursor: "", has_more: false } : response;
      if (generation !== state.generation) return state.items;
      const items = Array.isArray(page?.items) ? page.items : [];
      if (reset) {
        state.items = items;
      } else {
        const itemKey = (item) => name === "facts" ? `${item.scope || "cluster"}|${item.key}` : item.id;
        const merged = new Map(state.items.map((item) => [itemKey(item), item]));
        for (const item of items) merged.set(itemKey(item), item);
        state.items = [...merged.values()];
      }
      state.nextCursor = page?.next_cursor || "";
      state.hasMore = Boolean(page?.has_more);
      return state.items;
    } finally {
      if (generation === state.generation) state.loading = false;
    }
  }
  async function loadMoreMemoryHistory(name) {
    const state = memoryHistoryPages[name];
    if (!state || state.loading || !state.hasMore || !state.nextCursor) return;
    try {
      await fetchMemoryHistoryPage(name, state.url);
    } catch (err) {
      console.error(`loading more ${name} failed`, err);
    }
    if (name === "facts") renderFacts(state.items, state.hasMore, state.loading);
    if (name === "experiences") renderExperiences(state.items, state.hasMore, state.loading);
    if (name === "dialogues") renderTranscripts(state.items, state.hasMore, state.loading);
  }
  function appendMemoryLoadMore(container, name, hasMore, loading) {
    if (!container || !hasMore) return;
    container.querySelector(".history-load-more")?.remove();
    const button = document.createElement("button");
    button.className = "icon-button history-load-more";
    button.type = "button";
    button.textContent = loading ? "Loading…" : "Load more";
    button.disabled = loading;
    button.addEventListener("click", () => loadMoreMemoryHistory(name));
    container.appendChild(button);
  }
  function renderLearning(insights) {
    if (!learningSummary) return;
    if (!insights || !insights.total_experiences) {
      learningSummary.innerHTML = '<p style="color:var(--muted)">No experience recorded yet. The cluster starts learning as soon as it runs work.</p>';
      return;
    }
    const pairings = (insights.top_pairings || []).map((p) => `<div class="row"><span>${escapeHtml(p.model)} on ${escapeHtml(p.node_id)} (${escapeHtml(p.task_type)})</span><span class="badge ${p.success_rate >= 0.7 ? "completed" : "failed"}">${Math.round(p.success_rate * 100)}% · ${p.avg_duration_ms}ms</span></div>`).join("");
    const lessons = (insights.lessons || []).map((l) => `<div class="lesson${/Avoid/i.test(l) ? " negative" : ""}">${escapeHtml(l)}</div>`).join("") || '<p style="color:var(--muted);font-size:0.82rem">Not enough evidence for firm lessons yet.</p>';
    const taskTypes = Object.entries(insights.task_types || {}).sort((a, b) => b[1] - a[1]).map(([k, v]) => `<div class="row"><span>${escapeHtml(k)}</span><span>${v}</span></div>`).join("");
    learningSummary.innerHTML = `
    <div class="card">
      <div class="memory-stat-grid">
        <div class="memory-stat"><span class="memory-stat-value">${insights.total_experiences}</span><span class="memory-stat-label">Experiences</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${Math.round((insights.success_rate || 0) * 100)}%</span><span class="memory-stat-label">Success rate</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${insights.avg_duration_ms || 0}ms</span><span class="memory-stat-label">Avg duration</span></div>
        <div class="memory-stat"><span class="memory-stat-value">${insights.tracked_pairings || 0}</span><span class="memory-stat-label">Learned pairings</span></div>
      </div>
      <h4>Lessons applied to routing</h4>
      ${lessons}
      <h4>Best node + model pairings</h4>
      ${pairings || '<p style="color:var(--muted);font-size:0.8rem">None yet.</p>'}
      <h4>Work seen by type</h4>
      ${taskTypes || '<p style="color:var(--muted);font-size:0.8rem">None yet.</p>'}
      <div class="row"><span>Database</span><span>${escapeHtml(insights.database_path || "")}</span></div>
    </div>`;
  }
  function renderFacts(facts, hasMore = false, loading = false) {
    if (!factsList) return;
    const items = facts?.length ? facts : [{ key: "__empty", scope: "__empty" }];
    factsList.querySelector(".history-load-more")?.remove();
    dashboardDOM.renderVirtualList(factsList, items, {
      keyOf: (fact) => `${fact.scope || "cluster"}|${fact.key}`,
      signatureOf: JSON.stringify,
      estimateHeight: 140,
      threshold: 30,
      renderItem: (f) => {
        if (f.scope === "__empty") {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "Nothing remembered yet.";
          return empty;
        }
        const card = document.createElement("div");
        card.className = "card";
        card.innerHTML = `
      <h3>${escapeHtml(f.key)} <span class="badge healthy">${escapeHtml(f.scope || "cluster")}</span></h3>
      <div class="row"><span>${escapeHtml(f.value)}</span></div>
      <div class="row"><span>Source</span><span>${escapeHtml(f.source || "unknown")}</span></div>
      <div class="row"><span>Recalled</span><span>${f.hits || 0}×</span></div>
      <button class="icon-button" data-forget="${escapeHtml(f.key)}" data-scope="${escapeHtml(f.scope || "cluster")}">Forget</button>
      `;
        card.querySelector("[data-forget]")?.addEventListener("click", async (e) => {
          const key = e.target.dataset.forget;
          const scope = e.target.dataset.scope;
          await fetch(`/api/memory/facts/${encodeURIComponent(key)}?scope=${encodeURIComponent(scope)}`, { method: "DELETE" });
          refreshMemory();
        });
        return card;
      }
    });
    appendMemoryLoadMore(factsList, "facts", hasMore, loading);
  }
  function renderExperiences(experiences, hasMore = false, loading = false) {
    if (!experiencesList) return;
    const items = experiences?.length ? experiences : [{ id: "__empty", empty: true }];
    experiencesList.querySelector(".history-load-more")?.remove();
    dashboardDOM.renderVirtualList(experiencesList, items, {
      keyOf: (item) => item.id || `${item.created_at || ""}|${item.description || ""}`,
      signatureOf: JSON.stringify,
      estimateHeight: 200,
      threshold: 25,
      renderItem: (e) => {
        if (e.empty) {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "No matching experience recorded.";
          return empty;
        }
        const card = document.createElement("div");
        card.className = "card";
        card.innerHTML = `
      <h3>${escapeHtml(e.kind || "task")} <span class="badge ${e.success ? "completed" : "failed"}">${e.success ? "success" : "failed"}</span></h3>
      <div class="row"><span>${escapeHtml(e.description || "")}</span></div>
      <div class="row"><span>${escapeHtml(e.node_id || "-")} / ${escapeHtml(e.model || "-")}</span><span>${e.duration_ms || 0}ms</span></div>
      <div class="row"><span>When</span><span>${new Date(e.created_at).toLocaleString()}</span></div>
      ${e.output ? `<div class="subtask-output">${escapeHtml(e.output.slice(0, 600))}</div>` : ""}
      ${e.error ? `<div class="subtask-output" style="border-color:var(--bad)">${escapeHtml(e.error)}</div>` : ""}
      `;
        return card;
      }
    });
    appendMemoryLoadMore(experiencesList, "experiences", hasMore, loading);
  }
  function renderTranscripts(dialogues, hasMore = false, loading = false) {
    if (!transcriptsList) return;
    const items = dialogues?.length ? dialogues : [{ id: "__empty", empty: true }];
    transcriptsList.querySelector(".history-load-more")?.remove();
    dashboardDOM.renderVirtualList(transcriptsList, items, {
      keyOf: (dialogue) => dialogue.id,
      signatureOf: JSON.stringify,
      estimateHeight: 240,
      threshold: 20,
      renderItem: (d) => {
        if (d.empty) {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "No stored conversations yet.";
          return empty;
        }
        const card = document.createElement("div");
        card.className = "card";
        const turns = (d.turns || []).slice(-6).map((t) => `<div class="node-msg"><strong>${escapeHtml(t.speaker)}:</strong> ${escapeHtml((t.content || "").slice(0, 400))}</div>`).join("");
        card.innerHTML = `
      <h3>${escapeHtml(d.id)} <span class="badge healthy">${(d.turns || []).length} turns</span></h3>
      <div class="row"><span>Participants</span><span>${escapeHtml((d.participants || []).join(", "))}</span></div>
      ${d.topic ? `<div class="row"><span>Topic</span><span>${escapeHtml(d.topic)}</span></div>` : ""}
      ${turns}
      `;
        return card;
      }
    });
    appendMemoryLoadMore(transcriptsList, "dialogues", hasMore, loading);
  }
  factForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const key = factKey.value.trim();
    const value = factValue.value.trim();
    if (!key || !value) return;
    try {
      await fetchJSON("/api/memory/facts", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key, value, scope: "cluster" })
      });
      factKey.value = "";
      factValue.value = "";
      refreshMemory();
    } catch (err) {
      console.error("failed to save fact", err);
    }
  });
  var memorySearchTimer = null;
  memorySearch?.addEventListener("input", () => {
    clearTimeout(memorySearchTimer);
    memorySearchTimer = setTimeout(refreshMemory, 300);
  });

  // src/views/pipelines.js
  var pipelineModelList = document.getElementById("pipeline-model-list");
  var pipelineActionStatus = document.getElementById("pipeline-action-status");
  var pipelineActions = /* @__PURE__ */ new Map();
  var pipelineActionMessage = "";
  function renderPipelineDashboard(models, pipelines) {
    if (!pipelineModelList) return;
    const focusedModel = pipelineModelList.contains(document.activeElement) ? document.activeElement?.dataset?.reshardModel : "";
    const plansByModel = new Map((pipelines || []).map((pipeline) => [pipeline.model_id, pipeline]));
    const entries = models || [];
    pipelineModelList.innerHTML = entries.length ? entries.map((model) => pipelineModelCard(model, plansByModel.get(model.model_id))).join("") : '<p class="setup-empty">No models are currently advertised by the cluster.</p>';
    if (pipelineActionStatus) pipelineActionStatus.textContent = pipelineActionMessage;
    pipelineModelList.querySelectorAll("[data-reshard-model]").forEach((button) => {
      button.addEventListener("click", () => requestPipelineReshard(button.dataset.reshardModel));
    });
    if (focusedModel) {
      [...pipelineModelList.querySelectorAll("[data-reshard-model]")].find((button) => button.dataset.reshardModel === focusedModel)?.focus();
    }
  }
  function pipelineModelCard(model, pipeline) {
    const sharded = model.kind === "sharded";
    const state = pipelineActions.get(model.model_id);
    const nodes = (model.nodes || []).map(escapeHtml).join(", ");
    const status = pipeline?.status || "not_placed";
    const plan = pipeline?.plan;
    const stages = pipeline?.stages || [];
    const stageMarkup = stages.length ? `<div class="pipeline-stage-list">${stages.map(pipelineStageMarkup).join("")}</div>` : `<p class="pipeline-empty">${sharded ? "No placement yet. Reshard to place and load stages." : "Whole-model execution."}</p>`;
    const shardedDetails = sharded ? `
      <div class="row"><span>Pipeline</span><span class="badge ${status === "ready" ? "healthy" : status === "degraded" ? "unhealthy" : "unknown"}">${escapeHtml(status.replaceAll("_", " "))}</span></div>
      ${plan ? `<div class="row"><span>Plan version</span><span>${plan.plan_version}</span></div>` : ""}
      ${stageMarkup}
      <button type="button" class="primary-btn pipeline-reshard-button" data-reshard-model="${escapeHtml(model.model_id)}" ${state?.pending ? "disabled" : ""}>${state?.pending ? "Placing / loading…" : "Reshard / Reload stages"}</button>
    ` : `<div class="row"><span>Available on</span><span>${nodes || "No node reported"}</span></div>`;
    return `
    <article class="card pipeline-model-card">
      <h3>${escapeHtml(model.model_id)} <span class="badge ${sharded ? "online" : "unknown"}">${sharded ? "Sharded" : "Whole"}</span></h3>
      ${shardedDetails}
      ${state?.message ? `<p class="pipeline-action-message">${escapeHtml(state.message)}</p>` : ""}
    </article>
  `;
  }
  function pipelineStageMarkup(stage) {
    const ready = stage.status === "ready";
    const utilization = Number.isFinite(stage.utilization_percent) ? `${stage.active}/${stage.capacity} slots · ${stage.utilization_percent.toFixed(0)}%` : `${stage.active} active · capacity unavailable`;
    const transfer = Number.isFinite(stage.activation_transfer_ms) ? `${stage.activation_transfer_ms.toFixed(1)} ms · ${formatBytes(stage.activation_bytes || 0)}` : "No activation transfer recorded";
    return `
    <div class="pipeline-stage">
      <div class="pipeline-stage-heading">
        <strong>Stage ${stage.stage + 1} · ${escapeHtml(stage.node_id)}</strong>
        <span class="badge ${ready ? "healthy" : "unhealthy"}">${escapeHtml(stage.status)}</span>
      </div>
      <div class="row"><span>Layers</span><span>${stage.start_layer}–${stage.end_layer}</span></div>
      <div class="row"><span>Engine</span><span>${escapeHtml(stage.engine)}</span></div>
      <div class="row"><span>Stage utilization</span><span>${utilization}</span></div>
      ${stage.stage > 0 ? `<div class="row"><span>Activation transfer</span><span>${transfer}</span></div>` : ""}
    </div>
  `;
  }
  async function requestPipelineReshard(modelID) {
    const current = pipelineActions.get(modelID) || {};
    if (current.pending) return;
    pipelineActions.set(modelID, { pending: true, message: "" });
    pipelineActionMessage = `Recomputing placement and loading stages for ${modelID}…`;
    renderPipelineDashboard(dashboardState.modelInventory, dashboardState.pipelinePlans);
    try {
      const result = await fetchJSON(`/api/pipelines/${encodeURIComponent(modelID)}/reshard`, { method: "POST" });
      const planVersion = result?.plan?.plan_version;
      const message = planVersion ? `Placement ${planVersion} for ${modelID} is loaded.` : `Placement for ${modelID} is loaded.`;
      pipelineActions.set(modelID, { pending: false, message });
      pipelineActionMessage = message;
    } catch (error) {
      const message = `Reshard failed for ${modelID}: ${error.message}`;
      pipelineActions.set(modelID, { pending: false, message });
      pipelineActionMessage = message;
    }
    renderPipelineDashboard(dashboardState.modelInventory, dashboardState.pipelinePlans);
  }

  // src/views/routing.js
  function renderRouting(nodes, tasks) {
    if (!routingList) return;
    if (!nodes || nodes.length === 0) {
      dashboardDOM.patchKeyedChildren(routingList, [{ id: "__empty" }], {
        keyOf: (item) => item.id,
        signatureOf: () => "empty",
        renderItem: () => {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "No nodes registered yet.";
          return empty;
        }
      });
      return;
    }
    const byNode = new Map(nodes.map((n) => [n.id, {
      node: n,
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
      unknown: 0,
      recent: []
    }]));
    const sortedTasks = [...tasks || []].sort(
      (a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
    for (const t of sortedTasks) {
      for (const s of t.subtasks || []) {
        const bucket = byNode.get(s.node_id);
        if (!bucket) continue;
        const status = knownStatus(s.status, SUBTASK_STATES);
        bucket[status]++;
        if (bucket.recent.length < 5) {
          bucket.recent.push({ taskId: t.id, subtaskId: s.subtask?.id || "subtask", model: s.model, status, taskType: s.subtask?.task_type || "work" });
        }
      }
    }
    const workload = [...byNode.values()].sort((a, b) => {
      if ((b.node.active_tasks || 0) !== (a.node.active_tasks || 0)) return (b.node.active_tasks || 0) - (a.node.active_tasks || 0);
      return b.running + b.pending - (a.running + a.pending);
    });
    dashboardDOM.patchKeyedChildren(routingList, workload, {
      keyOf: (item) => item.node.id,
      signatureOf: JSON.stringify,
      renderItem: ({ node, pending, running, completed, failed, cancelled, unknown, recent }) => {
        const card = document.createElement("div");
        card.className = "card";
        const total = pending + running + completed + failed + cancelled + unknown;
        const pct = (n) => total === 0 ? 0 : Math.round(n / total * 100);
        const recentHTML = recent.length ? recent.map(
          (r) => `<div class="row"><span>${r.taskId}/${r.subtaskId} (${r.taskType}) &rarr; ${escapeHtml(r.model)}</span><span class="badge ${r.status}">${r.status}</span></div>`
        ).join("") : '<p style="color:var(--muted);font-size:0.8rem">No jobs routed yet.</p>';
        card.innerHTML = `
      <h3>${escapeHtml(node.id)} <span class="badge ${node.healthy ? "healthy" : "unhealthy"}">${node.active_tasks} active</span></h3>
      <div class="workload-bar" role="img" aria-label="${pending} pending, ${running} running, ${completed} completed, ${failed} failed, ${cancelled} cancelled">
        <div class="workload-seg workload-pending" style="width:${pct(pending)}%"></div>
        <div class="workload-seg workload-running" style="width:${pct(running)}%"></div>
        <div class="workload-seg workload-completed" style="width:${pct(completed)}%"></div>
        <div class="workload-seg workload-failed" style="width:${pct(failed)}%"></div>
        <div class="workload-seg workload-cancelled" style="width:${pct(cancelled)}%"></div>
      </div>
      <div class="row"><span>Pending</span><span>${pending}</span></div>
      <div class="row"><span>Running</span><span>${running}</span></div>
      <div class="row"><span>Completed</span><span>${completed}</span></div>
      <div class="row"><span>Failed</span><span>${failed}</span></div>
      <div class="row"><span>Cancelled${unknown ? ` / unknown` : ""}</span><span>${cancelled}${unknown ? ` / ${unknown}` : ""}</span></div>
      <h4>Recent jobs</h4>
      ${recentHTML}
    `;
        return card;
      }
    });
  }
  function visibleNetworkPeers(peers, nodes, pairingStatus2) {
    const visible = new Map((peers || []).map((peer) => [peer.id, peer]));
    for (const node of nodes || []) {
      if (!node.healthy || visible.has(node.id)) continue;
      visible.set(node.id, {
        id: node.id,
        role: "node",
        peer_addr: node.address,
        last_seen: node.last_seen,
        orchestrator_id: pairingStatus2?.orchestrator_id
      });
    }
    if (pairingStatus2?.role === "node" && pairingStatus2.node_id) {
      visible.set(pairingStatus2.node_id, {
        ...visible.get(pairingStatus2.node_id),
        id: pairingStatus2.node_id,
        role: "node",
        peer_addr: pairingStatus2.node_addr,
        orchestrator_id: pairingStatus2.joined ? pairingStatus2.orchestrator_id : "",
        local: true
      });
    }
    return [...visible.values()].sort((a, b) => (a.id || "").localeCompare(b.id || ""));
  }
  function renderDiscovery(peers, authentication = "unavailable") {
    const securityStatus = document.getElementById("discovery-security-status");
    if (securityStatus) {
      if (authentication === "hmac-sha256") {
        securityStatus.textContent = "LAN discovery authentication: HMAC-SHA256 enabled on this instance.";
      } else if (authentication === "unauthenticated") {
        securityStatus.textContent = "LAN discovery authentication: Unauthenticated (default). Configure the same discovery_hmac_secret on every cluster member to authenticate announcements.";
      } else {
        securityStatus.textContent = "LAN discovery authentication status is unavailable.";
      }
    }
    if (!discoveryList) return;
    const ordered = visibleNetworkPeers(peers, dashboardState.nodes, dashboardState.pairing.status);
    if (ordered.length === 0) {
      dashboardDOM.patchKeyedChildren(discoveryList, [{ id: "__empty" }], {
        keyOf: (item) => item.id,
        signatureOf: () => "empty",
        renderItem: () => {
          const empty = document.createElement("p");
          empty.className = "task-history-empty";
          empty.textContent = "No visible nodes found on the local network yet.";
          return empty;
        }
      });
      return;
    }
    dashboardDOM.patchKeyedChildren(discoveryList, ordered, {
      keyOf: (peer) => peer.id,
      signatureOf: (peer) => JSON.stringify({
        peer,
        pairing: dashboardState.pairing.status,
        member: dashboardState.nodes.find((node) => node.id === peer.id)
      }),
      renderItem: (p) => {
        const card = document.createElement("div");
        card.className = "card";
        const associated = p.role === "node" ? p.orchestrator_id || "Not announced" : p.role === "orchestrator" ? dashboardState.pairing.status?.role === "node" ? p.id === dashboardState.pairing.status.orchestrator_id ? "current orchestrator" : "available to join" : "self" : "Not elected";
        const member = p.role === "node" ? dashboardState.nodes.find((node) => node.id === p.id) : null;
        const localMember = p.id === dashboardState.pairing.status?.node_id && dashboardState.pairing.status?.joined;
        const joinOrchestrator = p.role === "orchestrator" && dashboardState.pairing.status?.role === "node" && !p.local;
        const orchestratorTarget = joinOrchestrator ? {
          id: p.id,
          role: p.role,
          address: p.peer_addr || "",
          peer_addr: p.peer_addr,
          healthy: true,
          discovered: true,
          joinOrchestrator: true
        } : null;
        const alreadyJoined = dashboardState.pairing.status?.joined && dashboardState.pairing.status.orchestrator_id === p.id;
        const joinLabel = alreadyJoined ? `Re-pair with ${p.id}` : dashboardState.pairing.status?.joined ? `Switch to ${p.id}'s cluster` : `Join ${p.id}'s cluster`;
        card.innerHTML = `
      <h3>${escapeHtml(p.id)} <span class="badge healthy">${escapeHtml(p.role)}</span></h3>
      <div class="row"><span>Address</span><span>${escapeHtml(p.peer_addr || p.http_addr || "Address unavailable")}</span></div>
      ${p.role === "node" ? `<div class="row"><span>Status</span><span>${member || localMember ? "Visible · cluster member" : "Visible · not a member of this cluster"}${p.local ? " · local node" : ""}</span></div>` : ""}
      <div class="row"><span>Associated orchestrator</span><span>${escapeHtml(associated)}</span></div>
      <div class="row"><span>Last seen</span><span>${p.local ? "This device" : p.last_seen ? new Date(p.last_seen).toLocaleTimeString() : "Online in cluster"}</span></div>
      ${orchestratorTarget ? `<button type="button" class="discovery-join-orchestrator" data-action="join-orchestrator" ${p.peer_addr ? "" : "disabled"}>${escapeHtml(joinLabel)}</button>` : ""}
    `;
        if (p.role === "node") {
          attachNodeContextMenu(card, member || {
            id: p.id,
            address: p.peer_addr || p.http_addr,
            healthy: true,
            discovered: !p.local,
            last_seen: p.last_seen,
            models: [],
            active_tasks: 0,
            hardware: {}
          });
        } else if (orchestratorTarget) {
          card.querySelector("[data-action=join-orchestrator]")?.addEventListener("click", () => {
            openPairingDialog(orchestratorTarget);
          });
          attachNodeContextMenu(card, orchestratorTarget);
        }
        return card;
      }
    });
  }

  // src/app.js
  function updateConnectionStatus() {
    if (!connectionStatus) return;
    const state = dashboardState.connection;
    const lastSync = state.lastSyncAt ? new Date(state.lastSyncAt).toLocaleTimeString() : "not yet";
    connectionStatus.dataset.connected = String(state.connected);
    connectionStatus.textContent = `${state.connected ? "Connected" : "Reconnecting"} · Last sync ${lastSync}`;
  }
  var loadedDashboardPanels = /* @__PURE__ */ new Set(["agent-tab"]);
  function resetDashboardSignatures() {
    dashboardState.renderSignatures = {};
  }
  function markDashboardSynced() {
    dashboardState.connection.lastSyncAt = Date.now();
    updateConnectionStatus();
  }
  async function refresh() {
    if (!document.getElementById("cluster-tab")?.classList.contains("active")) return;
    try {
      const [nodes, peers, build] = await Promise.all([
        fetchJSON("/api/nodes/all"),
        fetchJSON("/api/discovery"),
        fetchJSON("/api/version")
      ]);
      applySnapshot({ nodes, tasks: dashboardState.tasks || [], discovery: peers, build_version: build.version });
      markDashboardSynced();
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
      let discoveryAuthentication = "unavailable";
      try {
        const status = await fetchJSON("/api/security/status");
        if (["unauthenticated", "hmac-sha256"].includes(status.discovery_authentication)) {
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
        discovery_authentication: discoveryAuthentication
      });
    } catch (err) {
      console.error("local network refresh failed", err);
    }
  }
  setInterval(() => {
    if (loadedDashboardPanels.has("discovery-panel") && document.getElementById("cluster-tab")?.classList.contains("active") && document.getElementById("discovery-panel")?.classList.contains("active")) {
      void refreshDiscovery();
    }
  }, 5e3);
  function applySnapshot(snap) {
    const signatures = dashboardState.renderSignatures || (dashboardState.renderSignatures = {});
    const taskSnapshot = dashboardSnapshot.mergeTaskSnapshot(
      dashboardState.tasks,
      snap.tasks,
      snap.complete_tasks,
      dashboardState.fullTaskHistoryLoaded
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
      (dashboardState.capacity?.nodes || []).map((node) => [node.node_id, node])
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
    const peerSignature = JSON.stringify({ peers: peerState, discoveryAuthentication: dashboardState.discoveryAuthentication, nodes: nodeState.map((node) => ({
      id: node.id,
      address: node.address,
      healthy: node.healthy,
      last_seen: node.last_seen
    })), pairing: dashboardState.pairing.status });
    const discoveredNodeState = (snap.discovery || []).filter((peer) => peer.role === "node").map((peer) => ({ id: peer.id, address: peer.peer_addr || peer.http_addr, last_seen: peer.last_seen }));
    const overviewNodeSignature = JSON.stringify({ members: overviewNodeState, discovered: discoveredNodeState });
    let overviewChanged = false;
    const overviewParts = {
      activity: activitySignature !== signatures.activity,
      nodes: overviewNodeSignature !== signatures.overviewNodes,
      peers: peerSignature !== signatures.overviewPeers
    };
    if (loadedDashboardPanels.has("cluster-overview-panel") && (overviewParts.activity || overviewParts.nodes || overviewParts.peers)) {
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
        id: task.id,
        updated_at: task.updated_at,
        subtasks: task.subtasks
      }))
    });
    if (loadedDashboardPanels.has("routing-panel") && routingSignature !== signatures.routing) {
      renderRouting(dashboardState.nodes, dashboardState.tasks);
      signatures.routing = routingSignature;
    }
    const pipelineSignature = JSON.stringify({
      models: dashboardState.modelInventory,
      plans: dashboardState.pipelinePlans
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
  function activateDashboardTab(tabID) {
    if (tabID === "memory-tab") {
      loadedDashboardPanels.add(tabID);
      void refreshMemory();
      return;
    }
    if (tabID !== "cluster-tab") return;
    const selected = Array.from(clusterWorkspaceTabs).find((tab) => tab.getAttribute("aria-selected") === "true");
    activateWorkspacePanel(selected?.getAttribute("aria-controls") || "cluster-overview-panel");
    void refresh();
  }
  function activateWorkspacePanel(panelID) {
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
      build_version: dashboardState.buildVersion
    });
    if (panelID === "setup-readiness-panel") void checkSetupReadiness();
    if (panelID === "setup-catalog-panel") void loadModelCatalog();
    if (panelID === "tasks-panel") void refreshTaskHistory(true);
    if (panelID === "discovery-panel") void refreshDiscovery();
    if (panelID === "routing-panel") void refreshShardGroups();
  }
  function refreshOverviewTimes() {
    overviewNodes?.querySelectorAll("[data-last-seen]").forEach((label) => {
      label.textContent = `Last seen ${formatRelativeTime(label.dataset.lastSeen)}`;
    });
  }
  setInterval(refreshOverviewTimes, 15e3);
  function connectEvents() {
    dashboardSnapshot.connectDashboardEvents((snapshot) => {
      markDashboardSynced();
      applySnapshot(snapshot);
    }, {
      onConnectionChange: (connected) => {
        dashboardState.connection.connected = connected;
        updateConnectionStatus();
      }
    });
  }
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
      if (document.getElementById("routing-panel")?.classList.contains("active")) {
        void refreshShardGroups();
      }
    }
  }, 15e3);
  connectEvents();

  // src/shared/navigation.js
  function wirePrimaryTabs(tabs, panels) {
    const tabList = tabs[0]?.closest('[role="tablist"]');
    if (tabList) tabList.setAttribute("role", "tablist");
    const selectTab = (tab) => {
      tabs.forEach((item) => {
        const active = item === tab;
        item.classList.toggle("active", active);
        item.setAttribute("role", "tab");
        item.setAttribute("aria-controls", item.dataset.tab);
        item.setAttribute("aria-selected", String(active));
        item.tabIndex = active ? 0 : -1;
      });
      panels.forEach((panel) => {
        const active = panel.id === tab.dataset.tab;
        panel.classList.toggle("active", active);
        panel.setAttribute("role", "tabpanel");
        panel.setAttribute("aria-labelledby", tab.id);
        panel.hidden = !active;
      });
      activateDashboardTab(tab.dataset.tab);
    };
    tabs.forEach((tab, index) => {
      const target = document.getElementById(tab.dataset.tab);
      if (!tab.id) tab.id = `dashboard-${tab.dataset.tab}-tab`;
      if (target) {
        target.setAttribute("role", "tabpanel");
        target.setAttribute("aria-labelledby", tab.id);
        target.hidden = !tab.classList.contains("active");
      }
      tab.tabIndex = tab.classList.contains("active") ? 0 : -1;
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
  wirePrimaryTabs(navTabs, tabContents);
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
      activateWorkspacePanel(targetID);
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
  setupCheckBtn?.addEventListener("click", checkSetupReadiness);
  catalogNodeSelect?.addEventListener("change", loadCatalogTarget);
  catalogEngineSelect?.addEventListener("change", renderModelCatalog);
  catalogSearch?.addEventListener("input", renderModelCatalog);

  // src/views/view-state.js
  var VIEW_STATE_KEY = "maistr0.viewState";
  var DIRECT_CHAT_KEY = "maistr0.directChat";
  var MAX_SAVED_DIRECT_MESSAGES = 100;
  var SUB_TAB_SELECTOR = ".cluster-workspace-tab, .setup-tab, .overview-tab";
  var refreshViewBtn = document.getElementById("refresh-view-btn");
  function readSessionJSON(key) {
    try {
      return JSON.parse(globalThis.sessionStorage?.getItem(key) || "null");
    } catch {
      return null;
    }
  }
  function writeSessionJSON(key, value) {
    try {
      globalThis.sessionStorage?.setItem(key, JSON.stringify(value));
    } catch {
    }
  }
  function captureViewState() {
    return {
      primary: document.querySelector(".nav-tab.active")?.id || "",
      agent: document.querySelector(".agent-tab.active")?.dataset.agent || "direct",
      subTabs: [...document.querySelectorAll(SUB_TAB_SELECTOR)].filter((tab) => tab.id && tab.getAttribute("aria-selected") === "true").map((tab) => tab.id)
    };
  }
  function savedDirectMessages(value) {
    if (!Array.isArray(value)) return [];
    return value.filter((message) => message && ["user", "assistant"].includes(message.role) && typeof message.content === "string").map((message) => ({ role: message.role, content: message.content })).slice(-MAX_SAVED_DIRECT_MESSAGES);
  }
  function saveViewState() {
    writeSessionJSON(VIEW_STATE_KEY, captureViewState());
    writeSessionJSON(DIRECT_CHAT_KEY, savedDirectMessages(directMessages));
  }
  function selectSavedTab(id) {
    const tab = id ? document.getElementById(id) : null;
    if (tab?.getAttribute("role") === "tab" && tab.getAttribute("aria-selected") !== "true") tab.click();
  }
  function restoreViewState() {
    const messages = savedDirectMessages(readSessionJSON(DIRECT_CHAT_KEY));
    if (messages.length) {
      setDirectMessages(messages);
      if (activeChatMode === "direct") renderDirectMessages();
    }
    const saved = readSessionJSON(VIEW_STATE_KEY);
    if (!saved || typeof saved !== "object") return;
    for (const id of Array.isArray(saved.subTabs) ? saved.subTabs : []) selectSavedTab(id);
    selectSavedTab(saved.primary);
    if (saved.agent && saved.agent !== "direct") {
      document.querySelector(`.agent-tab[data-agent="${CSS.escape(saved.agent)}"]`)?.click();
    }
  }
  function activeSubPanel(selector) {
    return document.querySelector(`${selector}[aria-selected="true"]`)?.getAttribute("aria-controls") || "";
  }
  async function refreshActiveView() {
    resetDashboardSignatures();
    const primary = document.querySelector(".nav-tab.active")?.dataset.tab;
    if (primary === "memory-tab") return refreshMemory();
    if (primary === "agent-tab") {
      if (activeChatMode === "direct") {
        renderDirectMessages();
        return;
      }
      if (activeSessionId) await selectSession(activeSessionId);
      return;
    }
    if (primary !== "cluster-tab") return;
    const work = [refresh()];
    const panel = activeSubPanel(".cluster-workspace-tab");
    if (panel === "tasks-panel") work.push(refreshTaskHistory(true));
    if (panel === "discovery-panel") work.push(refreshDiscovery());
    if (panel === "routing-panel") work.push(refreshShardGroups());
    if (panel === "setup-panel") {
      const setupPanel = activeSubPanel(".setup-tab");
      if (setupPanel === "pairing-panel") work.push(loadPairingStatus());
      if (setupPanel === "setup-readiness-panel") work.push(checkSetupReadiness());
      if (setupPanel === "setup-catalog-panel") work.push(loadModelCatalog());
    }
    await Promise.allSettled(work);
  }
  refreshViewBtn?.addEventListener("click", async () => {
    if (refreshViewBtn.disabled) return;
    refreshViewBtn.disabled = true;
    refreshViewBtn.textContent = "↻ Refreshing…";
    try {
      await refreshActiveView();
      markDashboardSynced();
    } catch (err) {
      console.error("refreshing the active view failed", err);
    } finally {
      refreshViewBtn.disabled = false;
      refreshViewBtn.textContent = "↻ Refresh view";
    }
  });
  window.addEventListener("pagehide", saveViewState);
  restoreViewState();
})();

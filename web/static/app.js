// DOM Elements
const nodesList = document.getElementById("nodes-list");
const routingList = document.getElementById("routing-list");
const discoveryList = document.getElementById("discovery-list");
const tasksList = document.getElementById("tasks-list");
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

// Header stats & navigation
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
const agentTabs = document.querySelectorAll(".agent-tab");
const agentModelSelect = document.getElementById("agent-model-select");
const agentStepsSelect = document.getElementById("agent-steps-select");
const agentSettingsTitle = document.getElementById("agent-settings-title");
const agentToolsList = document.getElementById("agent-tools-list");
const directChatHelp = document.getElementById("direct-chat-help");
const clearDirectChatBtn = document.getElementById("clear-direct-chat-btn");
const directClusterContext = document.getElementById("direct-cluster-context");
const directClusterStatus = document.getElementById("direct-cluster-status");
const directClusterNodesEl = document.getElementById("direct-cluster-nodes");
const directClusterRefreshBtn = document.getElementById("direct-cluster-refresh");
const dispatchDirectTaskBtn = document.getElementById("dispatch-direct-task-btn");
const interruptMsgBtn = document.getElementById("interrupt-msg-btn");
const nodeTerminalInterruptBtn = document.getElementById("node-terminal-interrupt");
const chatMessagesEl = document.getElementById("chat-messages");
const chatInput = document.getElementById("chat-input");
const sendMsgBtn = document.getElementById("send-msg-btn");
const chatStatus = document.getElementById("chat-status");
const promptChips = document.querySelectorAll(".prompt-chip");

// State
let activeSessionId = null;
const activeSessionIds = new Map();
let currentSessions = [];
let isSending = false;
let activeAbortController = null;
let terminalAbortController = null;
let streamingAssistantEl = null;
let currentAgentHarness = "pi";
let activeChatMode = "pi";
let directMessages = [];
const modelPanelOpen = new Set();
const pendingModelState = new Map();
const pendingDefaultModel = new Map();
const lastFetchedModels = new Map();
let contextNode = null;
let currentNodes = [];
const nodeProperties = new Map();
let terminalNode = null;
let terminalDialogueId = null;
let pairingTargetNode = null;
let currentPairingStatus = null;
let pairingSetupFocused = false;
let pairingPromptShown = false;

// Tab Switching
navTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    navTabs.forEach((t) => t.classList.remove("active"));
    tabContents.forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    const target = document.getElementById(tab.dataset.tab);
    if (target) target.classList.add("active");
    if (tab.dataset.tab === "memory-tab") refreshMemory();
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

setupCheckBtn?.addEventListener("click", checkSetupReadiness);
catalogNodeSelect?.addEventListener("change", loadCatalogTarget);
catalogEngineSelect?.addEventListener("change", renderModelCatalog);
catalogSearch?.addEventListener("input", renderModelCatalog);

function openPairingDialog(node = null) {
  if (!pairingConnectDialog || pairingConnectDialog.open) return;
  if (currentPairingStatus?.role === "node" && node?.id === currentPairingStatus.node_id) {
    node = null;
  }
  pairingTargetNode = node;
  if (pairingStatus) pairingStatus.textContent = "";
  const title = document.getElementById("pairing-title-node");
  const description = document.getElementById("pairing-dialog-description");
  if (node) {
    const invitation = Boolean(node.discovered);
    if (title) title.textContent = invitation
      ? `Pair ${node.id} with this cluster`
      : `Re-pair ${node.id} with this cluster`;
    if (description) description.textContent = invitation
      ? `Send a pairing invitation to ${node.address}. The node must confirm locally before it joins.`
      : "Enter this cluster's HTTPS address and an active one-use PIN. The selected node must be online and already trusted by this cluster.";
    if (pairingAddressField) pairingAddressField.hidden = invitation;
    if (pairingAddress) pairingAddress.required = !invitation;
    if (pairingAddress) pairingAddress.value = currentPairingStatus?.address || currentPairingStatus?.orchestrator_addr || "";
    if (pairingCode) pairingCode.value = currentPairingStatus?.role === "orchestrator" ? currentPairingStatus.pin || "" : "";
    if (pairingConnectButton) pairingConnectButton.textContent = invitation ? "Send invitation" : "Re-pair node";
  } else {
    if (pairingAddressField) pairingAddressField.hidden = false;
    if (pairingAddress) pairingAddress.required = true;
    const joiningNode = currentPairingStatus?.role === "node";
    if (title) title.textContent = joiningNode
      ? currentPairingStatus.joined ? "Re-pair node" : "Join cluster"
      : "Connect to orchestrator";
    if (description) description.textContent = joiningNode
      ? "Enter the orchestrator HTTPS address and one-use PIN shown on its Setup page."
      : "Enter the HTTPS address and one-use PIN displayed on the orchestrator dashboard.";
    if (joiningNode) {
      pairingAddress.value = currentPairingStatus.orchestrator_addr || pairingAddress.value;
      pairingCode.value = "";
    }
    if (pairingConnectButton) pairingConnectButton.textContent = joiningNode
      ? currentPairingStatus.joined ? "Re-pair node" : "Join cluster"
      : "Connect node";
  }
  pairingConnectDialog.showModal();
  pairingAddress?.focus();
}

pairingPinRegenerateBtn?.addEventListener("click", async () => {
  pairingPinRegenerateBtn.disabled = true;
  pairingPinRegenerateBtn.textContent = "Generating PIN…";
  if (pairingState) pairingState.textContent = "Generating a new one-use PIN…";
  try {
    await fetchJSON("/api/pairing/pin", { method: "POST" });
    await loadPairingStatus();
    if (pairingState) pairingState.textContent = "New PIN generated. Enter it only on the node you intend to pair.";
  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not generate a new PIN: ${err.message}`;
  } finally {
    pairingPinRegenerateBtn.disabled = false;
    pairingPinRegenerateBtn.textContent = "Generate / replace pairing PIN";
  }
});

async function loadPairingStatus() {
  try {
    const status = await fetchJSON("/api/pairing/status");
    if (status.dashboard_url) {
      window.location.replace(status.dashboard_url);
      return;
    }
    currentPairingStatus = status;
    if (status.role === "orchestrator") {
      if (pairingPinView) pairingPinView.hidden = false;
      if (pairingNodeConnect) pairingNodeConnect.hidden = true;
      if (pairingInstructions) pairingInstructions.textContent = "Add a node to this cluster by generating a one-use PIN, then entering it on that node.";
      const hasPIN = Boolean(status.pin);
      if (pairingPinRegenerateBtn) {
        pairingPinRegenerateBtn.textContent = hasPIN ? "Generate / replace pairing PIN" : "Generate pairing PIN";
      }
      if (pairingAddressDisplay) pairingAddressDisplay.textContent = status.address || "HTTPS address unavailable";
      if (pairingPin) pairingPin.textContent = status.pin || "Expired";
      if (pairingPinExpiry) {
        pairingPinExpiry.textContent = status.expires_at
          ? `Expires ${new Date(status.expires_at).toLocaleTimeString()}. The PIN can be used once.`
          : "No active PIN. Generate a new PIN to pair another node.";
      }
      if (pairingState) pairingState.textContent = hasPIN
        ? "Share the PIN and HTTPS address only with the node you intend to add."
        : "No active PIN. Generate one to add or re-pair a node.";
      return;
    }
    if (status.role === "node") {
      if (pairingPinView) pairingPinView.hidden = true;
      if (pairingNodeConnect) pairingNodeConnect.hidden = false;
      if (pairingConnectOpenBtn) pairingConnectOpenBtn.textContent = status.joined
        ? "Re-pair / change orchestrator"
        : "Enter address and pairing PIN";
      if (pairingInstructions) pairingInstructions.textContent = "Join this node to an orchestrator with the one-use PIN shown on its Setup page. You can also re-pair this node to another cluster here.";
      if (status.orchestrator_addr && pairingAddress && !pairingAddress.value) {
        pairingAddress.value = status.orchestrator_addr;
      }
      if (pairingState) {
        pairingState.textContent = status.joined
          ? `Connected to ${status.orchestrator_addr || "the orchestrator"}.`
          : status.trusted
            ? "Pairing succeeded. Waiting for the node to register with the orchestrator."
            : "Not paired. Enter the orchestrator HTTPS address and its one-use PIN.";
      }
      if (!status.joined && !pairingSetupFocused) {
        pairingSetupFocused = true;
        document.querySelector('.nav-tab[data-tab="cluster-tab"]')?.click();
        document.querySelector('[data-window="setup-panel"]')?.click();
        document.getElementById("setup-pairing-tab")?.click();
      }
      const invitationPending = await checkPairingInvitation();
      if (!invitationPending && !status.joined && !pairingPromptShown) {
        pairingPromptShown = true;
        openPairingDialog();
      } else if (status.joined) {
        pairingPromptShown = false;
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
        pairingInvitationDetails.textContent = `${invitation.orchestrator_id} at ${invitation.address} wants to add this node to its cluster. The invitation expires ${new Date(invitation.expires_at).toLocaleTimeString()}.`;
      }
      if (pairingInvitationDialog && !pairingInvitationDialog.open) pairingInvitationDialog.showModal();
      return true;
    }

    async function respondToPairingInvitation(accept) {
      if (!pairingInvitationAcceptBtn || !pairingInvitationDeclineBtn) return;
      pairingInvitationAcceptBtn.disabled = true;
      pairingInvitationDeclineBtn.disabled = true;
      if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept ? "Connecting to the cluster securely…" : "Declining invitation…";
      try {
        const result = await fetchJSON(`/api/pairing/invitation/${accept ? "accept" : "reject"}`, { method: "POST" });
        pairingPromptShown = true;
        pairingInvitationDialog?.close();
        if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept
          ? `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}.`
          : "Invitation declined.";
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

  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not load pairing status: ${err.message}`;
    if (pairingNodeConnect) pairingNodeConnect.hidden = false;
    if (pairingConnectOpenBtn) pairingConnectOpenBtn.hidden = false;
  }
}

async function activateAgentTab(harness) {
  if (isSending) return;
  const previousMode = activeChatMode;
  activeChatMode = harness;
  if (newSessionBtn) newSessionBtn.disabled = true;
  if (agentHarnessSelect) agentHarnessSelect.disabled = true;
  agentTabs.forEach((tab) => {
    const active = tab.dataset.agent === harness;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
    tab.disabled = true;
  });
  setAgentConsoleMode(harness);
  try {
    if (currentAgentHarness !== harness) {
      const backend = await fetchJSON("/api/agent/backend", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ harness }),
      });
      applyAgentBackend(backend);
      await loadAgentTools();
    }
    const sessions = sessionsForHarness(harness);
    const rememberedID = activeSessionIds.get(harness);
    const sessionID = sessions.some((session) => session.id === rememberedID)
      ? rememberedID
      : sessions[0]?.id;
    if (sessionID) await selectSession(sessionID);
    else await createNewSession();
  } catch (err) {
    console.error("failed to switch agent tab", err);
    if (agentHarnessStatus) agentHarnessStatus.textContent = `Could not switch agent: ${err.message}`;
    activeChatMode = previousMode;
    setAgentConsoleMode(previousMode);
    agentTabs.forEach((tab) => {
      const active = tab.dataset.agent === previousMode;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", String(active));
    });
  } finally {
    agentTabs.forEach((tab) => { tab.disabled = false; });
    if (newSessionBtn) newSessionBtn.disabled = false;
    if (agentHarnessSelect) agentHarnessSelect.disabled = false;
    renderSessionList();
  }
}

function setAgentConsoleMode(mode) {
  const direct = mode === "direct";
  const sessionSection = sessionListEl?.closest(".sidebar-section");
  const toolsSection = agentToolsList?.closest(".sidebar-section");
  const harnessLabel = agentHarnessSelect?.closest("label");
  const modelLabel = agentModelSelect?.closest("label");
  const stepsLabel = agentStepsSelect?.closest("label");
  const modelLabelText = modelLabel?.querySelector("span");
  const quickPrompts = document.querySelector(".quick-prompts");

  if (sessionSection) sessionSection.hidden = direct;
  if (toolsSection) toolsSection.hidden = direct;
  if (directClusterContext) directClusterContext.hidden = !direct;
  if (harnessLabel) harnessLabel.hidden = direct;
  if (stepsLabel) stepsLabel.hidden = direct;
  if (directChatHelp) directChatHelp.hidden = !direct;
  if (quickPrompts) quickPrompts.hidden = direct;
  if (clearDirectChatBtn) clearDirectChatBtn.hidden = !direct;
  if (dispatchDirectTaskBtn) dispatchDirectTaskBtn.hidden = !direct;
  if (sendMsgBtn) sendMsgBtn.textContent = direct ? "Ask Orchestrator" : "Send";
  if (agentSettingsTitle) {
    agentSettingsTitle.textContent = direct ? "Direct Chat Settings" : `${mode === "pi" ? "Pi" : "DeepSeek"} Agent Settings`;
  }
  if (modelLabelText) {
    modelLabelText.textContent = direct ? "Cluster Model" : "Coordinator Model";
    if (agentModelSelect?.options.length) {
      agentModelSelect.options[0].textContent = direct
        ? "Auto (Orchestrator scheduler)"
        : "Auto (Fastest / Cluster Leader)";
    }
  }
  if (chatInput) {
    chatInput.placeholder = direct
      ? "Ask about the cluster, or enter work to dispatch as a task..."
      : "Message the orchestrator... (Enter to send, Shift+Enter for new line)";
  }
  if (direct) {
    renderDirectMessages();
    void refreshDirectClusterContext();
  }
}

function activateDirectChat() {
  if (isSending) return;
  activeChatMode = "direct";
  agentTabs.forEach((tab) => {
    const active = tab.dataset.agent === "direct";
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  setAgentConsoleMode("direct");
  chatInput?.focus();
}

agentTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    if (tab.dataset.agent === "direct") activateDirectChat();
    else activateAgentTab(tab.dataset.agent);
  });

  directClusterRefreshBtn?.addEventListener("click", () => {
    void refreshDirectClusterContext();
  });
});

pairingConnectForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!pairingConnectButton || !pairingAddress || !pairingCode) return;
  pairingConnectButton.disabled = true;
  pairingStatus.textContent = "Connecting securely…";
  try {
    const node = pairingTargetNode;
    const invitation = Boolean(node?.discovered);
    const endpoint = invitation
      ? `/api/discovery/${encodeURIComponent(node.id)}/pairing/invite`
      : node
        ? `/api/nodes/${encodeURIComponent(node.id)}/pairing/connect`
        : "/api/pairing/connect";
    const result = await fetchJSON(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(invitation
        ? { pin: pairingCode.value.trim() }
        : { address: pairingAddress.value.trim(), pin: pairingCode.value.trim() }),
    });
    pairingCode.value = "";
    pairingStatus.textContent = invitation
      ? `Invitation sent to ${node.id}. Confirm it on the node to complete pairing.`
      : node
        ? `${node.id} paired with this cluster.`
      : `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}. Connecting to the cluster…`;
    if (invitation && pairingState) {
      pairingState.textContent = `Invitation sent to ${node.id}; waiting for confirmation on that node.`;
    }
    pairingTargetNode = null;
    pairingConnectDialog?.close();
    await loadPairingStatus();
    await refresh();
  } catch (err) {
    pairingStatus.textContent = `Pairing failed: ${err.message}`;
  } finally {
    pairingConnectButton.disabled = false;
  }
});

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
        allNodes.length
          ? "No cluster nodes are online. Offline nodes remain listed above."
          : "No nodes are registered. Start a node and pair it with this orchestrator first.",
        allNodes.length ? "attention" : "empty",
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
        const state = error
            ? `Could not inspect node: ${error}`
          : !engines.length
            ? "No inference engine detected. Configure an engine in node settings."
            : usableCount
              ? `${usableCount} usable model${usableCount === 1 ? "" : "s"}${installedCount > usableCount ? ` · ${installedCount - usableCount} disabled` : ""}${refreshed ? " (registry refreshed)" : ""}.`
              : installedCount
                ? "Models are installed, but all are disabled for scheduling."
                : "Engine detected, but no models are available yet.";
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
        readyCount === nodes.length
          ? `All ${nodes.length} node${nodes.length === 1 ? "" : "s"} are ready for inference.`
          : `${readyCount} of ${nodes.length} nodes are ready. Use node settings to configure engines or select models.`,
        readyCount === nodes.length ? "ready" : "attention",
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
    [modelCatalogEntries] = await Promise.all([
      fetchJSON("/api/models/catalog"),
      loadCatalogNodes(),
    ]);
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
  catalogNodeSelect.innerHTML = online
    .map((node) => `<option value="${escapeHtml(node.id)}">${escapeHtml(node.id)}</option>`)
    .join("");
  if (online.some((node) => node.id === selected)) catalogNodeSelect.value = selected;
  if (!online.length) {
    catalogEngineSelect.innerHTML = "";
    catalogModels = [];
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
      fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`),
    ]);
    catalogEngines = allEngines.filter((item) => item.can_pull);
    catalogModels = models;
    const currentEngine = catalogEngineSelect.value;
    catalogEngineSelect.innerHTML = catalogEngines
      .map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)} · ${escapeHtml(item.url || "configured engine")}</option>`)
      .join("");
    if (catalogEngines.some((item) => item.name === currentEngine)) catalogEngineSelect.value = currentEngine;
    catalogStatus.textContent = catalogEngines.length
      ? `Target: ${nodeId} · ${catalogEngineSelect.value || "Select an engine"}. Installed models are marked usable or disabled for scheduling.`
      : `No configured engine on ${nodeId} supports model downloads.`;
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
  const entries = modelCatalogEntries.filter((model) =>
    model.engine === engineName &&
    (!query || `${model.name} ${model.description} ${(model.tags || []).join(" ")}`.toLowerCase().includes(query))
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
    const state = installed ? (installed.enabled ? "Installed · usable" : "Installed · disabled for scheduling") : "Available to download";
    const button = installed
      ? `<button class="secondary-button" type="button" disabled>${installed.enabled ? "Installed" : "Installed (disabled)"}</button>`
      : `<button class="primary-btn catalog-install" type="button" data-model="${escapeHtml(model.name)}">Install on ${escapeHtml(catalogNodeSelect.value)} · ${escapeHtml(engineName)}</button>`;
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
      body: JSON.stringify({ model, engine: engineName }),
    });
    catalogModels = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" });
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

async function fetchJSON(url, opts) {
  const resp = await fetch(url, opts);
  if (!resp.ok) {
    throw new Error(await resp.text());
  }
  if (resp.status === 204) return null;
  return resp.json();
}

async function refreshPersistenceStatus() {
  if (!persistenceWarning) return;
  try {
    const status = await fetchJSON("/api/status");
    const persistence = status.persistence;
    persistenceWarning.hidden = persistence?.available !== false;
    persistenceWarning.textContent = persistence?.warning || "Persistent memory is unavailable.";
  } catch (error) {
    console.error("Could not load persistence status:", error);
  }
}

function updateClusterTitle(count) {
  const title = document.querySelector("header h1");
  if (title) title.textContent = `mAIstr0 · ${count} ${count === 1 ? "Node" : "Nodes"}`;
}

// --- Interactive Orchestrator Functions ---

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
  currentAgentHarness = backend.name;
  const mode = activeChatMode === "direct" ? "direct" : backend.name;
  activeChatMode = mode;
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

async function loadClusterModels() {
  try {
    const models = await fetchJSON("/api/agent/models");
    if (!agentModelSelect) return;
    const currentVal = agentModelSelect.value;
    agentModelSelect.innerHTML = '<option value="">Auto (Fastest / Cluster Leader)</option>';
    const seen = new Set();
    for (const m of models) {
      if (seen.has(m.name)) continue;
      seen.add(m.name);
      const opt = document.createElement("option");
      opt.value = m.name;
      opt.textContent = `${m.name} (${(m.tags || []).join(", ")}) · on ${m.node_id}`;
      if (m.name === currentVal) opt.selected = true;
      agentModelSelect.appendChild(opt);
    }
  } catch (err) {
    console.error("failed to load cluster models", err);
  }
}

async function loadSessions() {
  try {
    currentSessions = await fetchJSON("/api/agent/sessions");
    renderSessionList();
    const sessions = sessionsForHarness(currentAgentHarness);
    const preferredID = activeSessionIds.get(currentAgentHarness);
    const sessionID = sessions.some((session) => session.id === preferredID)
      ? preferredID
      : sessions[0]?.id;
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
  return currentSessions.filter((session) => (session.harness || "pi") === harness);
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
  activeSessionId = id;
  const sessionInfo = currentSessions.find((session) => session.id === id);
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
        title: title || `Session ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`,
        coordinator_model: agentModelSelect?.value || "",
        max_steps: parseInt(agentStepsSelect?.value || "8", 10),
      }),
    });
    currentSessions.unshift(session);
    activeSessionIds.set(currentAgentHarness, session.id);
    await selectSession(session.id);
  } catch (err) {
    console.error("failed to create session", err);
  }
}

async function deleteSession(id) {
  try {
    await fetch(`/api/agent/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
    currentSessions = currentSessions.filter((s) => s.id !== id);
    if (activeSessionId === id) {
      const sessions = sessionsForHarness(currentAgentHarness);
      activeSessionId = sessions[0]?.id || null;
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
  chatMessagesEl.innerHTML = "";
  if (!session || !session.messages || session.messages.length === 0) {
    chatMessagesEl.innerHTML = `
      <div class="welcome-card">
        <h2>Welcome to the mAIstr0 Orchestrator</h2>
        <p>The orchestrator plans and reasons visibly, then spreads LLM workloads across worker nodes while preserving every node response.</p>
        <ul>
          <li><strong>Dynamic Workload Balancing:</strong> LLM queries and subtasks are routed to the least-loaded nodes.</li>
          <li><strong>Parallel Multi-Node Dispatch:</strong> Subtasks can be executed simultaneously across distinct cluster nodes.</li>
          <li><strong>Interactive ReAct Loop:</strong> Real-time streaming of thoughts, tool calls, and node execution metrics.</li>
        </ul>
      </div>
    `;
    return;
  }

  for (const msg of session.messages) {
    appendMessageToChat(msg);
  }
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}

function populateDirectModelOptions(nodes) {
  if (!agentModelSelect) return;
  const selectedModel = agentModelSelect.value;
  const models = new Map();
  for (const node of nodes.filter((entry) => entry.healthy)) {
    for (const model of node.models || []) {
      const key = model.name.toLowerCase();
      const current = models.get(key) || { name: model.name, engine: model.engine, tags: new Set(), nodes: new Set() };
      for (const tag of model.tags || []) current.tags.add(tag);
      current.nodes.add(node.id);
      models.set(key, current);
    }
  }

  agentModelSelect.replaceChildren(new Option("Auto (Orchestrator scheduler)", ""));
  for (const model of [...models.values()].sort((a, b) => a.name.localeCompare(b.name))) {
    const option = new Option(
      `${model.name} · ${[...model.tags].join(", ") || "general"} · ${[...model.nodes].join(", ")}`,
      model.name,
    );
    agentModelSelect.add(option);
  }
  if (models.has(selectedModel.toLowerCase())) agentModelSelect.value = selectedModel;
}

function formatDirectNodeHardware(node) {
  const hardware = node.hardware || {};
  const parts = [];
  if (hardware.os || hardware.arch) parts.push([hardware.os, hardware.arch].filter(Boolean).join("/"));
  if (hardware.cpu_cores) parts.push(`${hardware.cpu_cores} CPU cores`);
  if (hardware.total_ram_mb) parts.push(`${(hardware.total_ram_mb / 1024).toFixed(1)} GB RAM`);
  if (hardware.has_gpu) {
    parts.push(`${hardware.gpu_vendor || "GPU"}${hardware.gpu_stats_available ? ` · ${hardware.gpu_utilization_percent}% busy` : ""}`);
  }
  if (hardware.has_npu) {
    parts.push(`${hardware.npu_vendor || "NPU"}${hardware.npu_stats_available ? ` · ${hardware.npu_utilization_percent}% busy` : ""}`);
  }
  return parts.length ? parts.join(" · ") : "Hardware details unavailable";
}

function renderDirectClusterContext(nodes) {
  if (!directClusterNodesEl || !directClusterStatus) return;
  const healthyCount = nodes.filter((node) => node.healthy).length;
  const availableModels = new Set(
    nodes.filter((node) => node.healthy).flatMap((node) => (node.models || []).map((model) => model.name)),
  );
  directClusterStatus.textContent = `${nodes.length} nodes · ${healthyCount} online · ${availableModels.size} available models. Model tags indicate task capabilities; dispatch uses the native capability-aware scheduler.`;
  if (!nodes.length) {
    directClusterNodesEl.innerHTML = '<p class="direct-cluster-empty">No nodes are registered with this orchestrator.</p>';
    return;
  }
  directClusterNodesEl.innerHTML = nodes.map((node) => {
    const models = (node.models || []).map((model) => `
      <div class="direct-model-row">
        <strong>${escapeHtml(model.name)}</strong>
        <span>${escapeHtml((model.tags || []).join(", ") || "general")}${model.engine ? ` · ${escapeHtml(model.engine)}` : ""}</span>
      </div>
    `).join("") || '<div class="direct-model-row"><span>No models reported</span></div>';
    const score = Number.isFinite(node.hardware?.score) ? ` · capability score ${node.hardware.score.toFixed(1)}` : "";
    return `
      <details class="direct-node-card" ${node.healthy ? "open" : ""}>
        <summary>
          <span class="direct-node-name">${escapeHtml(node.id)}</span>
          <span class="badge ${node.healthy ? "healthy" : "offline"}">${node.healthy ? "Online" : "Offline"}</span>
        </summary>
        <div class="direct-node-details">
          <div>${escapeHtml(formatDirectNodeHardware(node))}</div>
          <div>Active tasks: ${Number(node.active_tasks) || 0}${score}</div>
          <div class="direct-model-list">${models}</div>
        </div>
      </details>
    `;
  }).join("");
}

async function refreshDirectClusterContext() {
  if (!directClusterStatus) return null;
  if (directClusterRefreshBtn) directClusterRefreshBtn.disabled = true;
  directClusterStatus.textContent = "Refreshing live nodes, models, and capabilities…";
  try {
    const nodes = await fetchJSON("/api/nodes/all");
    if (!Array.isArray(nodes)) throw new Error("orchestrator returned an invalid node inventory");
    populateDirectModelOptions(nodes);
    renderDirectClusterContext(nodes);
    return nodes;
  } catch (err) {
    directClusterStatus.textContent = `Cluster inventory unavailable: ${err.message}`;
    console.error("could not refresh direct chat cluster context", err);
    return null;
  } finally {
    if (directClusterRefreshBtn) directClusterRefreshBtn.disabled = false;
  }
}

function directClusterSystemMessage(nodes) {
  const inventory = nodes.map((node) => ({
    id: node.id,
    status: node.healthy ? "online" : "offline",
    active_tasks: node.active_tasks || 0,
    hardware: node.hardware || {},
    models: (node.models || []).map((model) => ({
      name: model.name,
      engine: model.engine,
      task_capability_tags: model.tags || [],
      size_gb: model.size_gb || 0,
    })),
  }));
  return {
    role: "system",
    content: `You are the conversational interface to the mAIstr0 orchestrator. Answer questions about the cluster from this live inventory and distinguish unknown details from facts. Model capability tags are task categories. The native task scheduler chooses healthy node/model pairs using task type compatibility, hardware capability, accelerator utilization, current load, and learned dispatch history. The user can explicitly dispatch work with the Dispatch Task action; do not claim a task was run unless that action was used. For task execution, the native planner and scheduler—not this chat response—create and route subtasks.\nLive cluster inventory: ${JSON.stringify(inventory)}`,
  };
}

function renderDirectMessages() {
  if (!chatMessagesEl) return;
  chatMessagesEl.innerHTML = "";
  if (!directMessages.length) {
    chatMessagesEl.innerHTML = `
      <div class="welcome-card">
        <h2>Direct Orchestrator Chat</h2>
        <p>Ask about the live cluster or use Dispatch Task to have the native planner route work to nodes with suitable models and capabilities. No agent or harness is involved.</p>
      </div>
    `;
    return;
  }
  for (const message of directMessages) {
    appendMessageToChat({ ...message, coordinator: "Direct Orchestrator" });
  }
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
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
      // Multi-round collaboration flow
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
      // Single turn node converse
      bodyHTML = `
        <div class="dialogue-single-turn">
          <div class="node-msg"><strong>Node LLM (Turn ${tr.output.turn || 1}):</strong>\n${escapeHtml(tr.output.node_reply)}</div>
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
    chatMessagesEl.appendChild(card);
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

  chatMessagesEl.appendChild(msgDiv);
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}

async function sendDirectMessage(text) {
  if (!text || isSending) return;
  isSending = true;
  streamingAssistantEl = null;
  activeAbortController = new AbortController();
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (interruptMsgBtn) interruptMsgBtn.hidden = false;
  if (dispatchDirectTaskBtn) dispatchDirectTaskBtn.disabled = true;
  if (clearDirectChatBtn) clearDirectChatBtn.disabled = true;
  agentTabs.forEach((tab) => { tab.disabled = true; });
  if (chatStatus) chatStatus.textContent = "Sending directly to the orchestrator...";

  directMessages.push({ role: "user", content: text });
  appendMessageToChat({ role: "user", content: text });
  chatInput.value = "";
  let assistantContent = "";

  try {
    const nodes = await refreshDirectClusterContext();
    if (!nodes) throw new Error("Could not load the current cluster inventory.");
    if (!nodes.some((node) => node.healthy)) {
      throw new Error("No healthy cluster nodes are available to answer this message.");
    }
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/x-ndjson" },
      body: JSON.stringify({
        model: agentModelSelect?.value || "",
        messages: [directClusterSystemMessage(nodes), ...directMessages],
        stream: true,
      }),
      signal: activeAbortController.signal,
    });
    if (!response.ok) {
      let message = await response.text();
      try {
        message = JSON.parse(message).error || message;
      } catch {
        // Keep the plain-text server error.
      }
      throw new Error(message.trim() || response.statusText);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    const consumeLine = (line) => {
      if (!line.trim()) return;
      const event = JSON.parse(line);
      if (event.error) throw new Error(event.error);
      const delta = event.message?.content || "";
      if (!delta) return;
      assistantContent += delta;
      if (!streamingAssistantEl) {
        streamingAssistantEl = document.createElement("div");
        streamingAssistantEl.className = "chat-msg assistant";
        const meta = document.createElement("div");
        meta.className = "msg-meta";
        meta.textContent = `🤖 Direct Orchestrator${agentModelSelect?.value ? ` · ${agentModelSelect.value}` : ""}`;
        const bubble = document.createElement("div");
        bubble.className = "msg-bubble";
        streamingAssistantEl.append(meta, bubble);
        chatMessagesEl.appendChild(streamingAssistantEl);
      }
      const bubble = streamingAssistantEl.querySelector(".msg-bubble");
      if (bubble) bubble.textContent += delta;
      chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
    };

    while (true) {
      const { done, value } = await reader.read();
      buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) consumeLine(line);
      if (done) break;
    }
    if (buffer.trim()) consumeLine(buffer);
    if (!assistantContent) throw new Error("The orchestrator returned an empty response.");
    directMessages.push({ role: "assistant", content: assistantContent });
    if (chatStatus) chatStatus.textContent = "";
  } catch (err) {
    if (assistantContent) directMessages.push({ role: "assistant", content: assistantContent });
    if (err.name === "AbortError") {
      if (chatStatus) chatStatus.textContent = "Interrupted.";
    } else {
      if (chatStatus) chatStatus.textContent = `Direct chat failed: ${err.message}`;
      appendMessageToChat({
        role: "assistant",
        coordinator: "Direct Orchestrator",
        content: `⚠️ Direct chat failed: ${err.message}`,
      });
    }
  } finally {
    isSending = false;
    activeAbortController = null;
    streamingAssistantEl = null;
    if (interruptMsgBtn) interruptMsgBtn.hidden = true;
    if (sendMsgBtn) sendMsgBtn.disabled = false;
    if (dispatchDirectTaskBtn) dispatchDirectTaskBtn.disabled = false;
    if (clearDirectChatBtn) clearDirectChatBtn.disabled = false;
    agentTabs.forEach((tab) => { tab.disabled = false; });
  }
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
    "Live progress and results are available in the Tasks workspace.",
  ].join("\n");
}

async function dispatchDirectTask(description) {
  if (!description || isSending) return;
  isSending = true;
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (dispatchDirectTaskBtn) dispatchDirectTaskBtn.disabled = true;
  if (clearDirectChatBtn) clearDirectChatBtn.disabled = true;
  agentTabs.forEach((tab) => { tab.disabled = true; });
  if (chatStatus) chatStatus.textContent = "Planning against live capabilities and routing to suitable nodes…";

  directMessages.push({ role: "user", content: description });
  appendMessageToChat({ role: "user", content: description });
  chatInput.value = "";
  try {
    const nodes = await refreshDirectClusterContext();
    if (!nodes) throw new Error("Could not load the current cluster inventory.");
    if (!nodes.some((node) => node.healthy)) {
      throw new Error("No healthy cluster nodes are available to plan or run this task.");
    }
    const task = await fetchJSON("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ description, plan_mode: "auto" }),
    });
    const summary = formatDirectTaskDispatch(task);
    directMessages.push({ role: "assistant", content: summary });
    appendMessageToChat({ role: "assistant", coordinator: "Direct Orchestrator", content: summary });
    if (chatStatus) chatStatus.textContent = "";
    void refreshDirectClusterContext();
    void refresh();
  } catch (err) {
    const message = `Task dispatch failed: ${err.message}`;
    directMessages.push({ role: "assistant", content: message });
    appendMessageToChat({ role: "assistant", coordinator: "Direct Orchestrator", content: message });
    if (chatStatus) chatStatus.textContent = message;
  } finally {
    isSending = false;
    if (sendMsgBtn) sendMsgBtn.disabled = false;
    if (dispatchDirectTaskBtn) dispatchDirectTaskBtn.disabled = false;
    if (clearDirectChatBtn) clearDirectChatBtn.disabled = false;
    agentTabs.forEach((tab) => { tab.disabled = false; });
  }
}

async function sendInteractiveMessage(text) {
  if (activeChatMode === "direct") return sendDirectMessage(text);
  if (!text || !activeSessionId || isSending) return;
  isSending = true;
  streamingAssistantEl = null;
  activeAbortController = new AbortController();
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (interruptMsgBtn) interruptMsgBtn.hidden = false;
  if (newSessionBtn) newSessionBtn.disabled = true;
  if (agentHarnessSelect) agentHarnessSelect.disabled = true;
  agentTabs.forEach((tab) => { tab.disabled = true; });
  if (chatStatus) chatStatus.textContent = "⚡ Reasoning & dispatching across cluster...";

  // Optimistically append user message
  appendMessageToChat({ role: "user", content: text });
  chatInput.value = "";

  try {
    const url = `/api/agent/sessions/${encodeURIComponent(activeSessionId)}/messages?stream=true`;
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "text/event-stream" },
      body: JSON.stringify({ content: text }),
      signal: activeAbortController.signal,
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
        // ignore
      }
    } else {
      if (chatStatus) chatStatus.textContent = "Error: " + err.message;
      appendMessageToChat({
        role: "assistant",
        content: "⚠️ Error during execution: " + err.message,
      });
    }
  } finally {
    isSending = false;
    activeAbortController = null;
    if (interruptMsgBtn) interruptMsgBtn.hidden = true;
    if (sendMsgBtn) sendMsgBtn.disabled = false;
    if (newSessionBtn) newSessionBtn.disabled = false;
    if (agentHarnessSelect) agentHarnessSelect.disabled = false;
    agentTabs.forEach((tab) => { tab.disabled = false; });
  }
}

function handleStreamEvent(event) {
  if (event.type === "step_started") {
    if (chatStatus) chatStatus.textContent = `⚡ Step ${event.step}: Reasoning on coordinator LLM...`;
  } else if (event.type === "thought" && event.content) {
    const thoughtBox = document.createElement("div");
    thoughtBox.className = "thought-box";
    thoughtBox.innerHTML = `💭 <strong>Step ${event.step || 1}:</strong> ${escapeHtml(event.content)}`;
    chatMessagesEl.appendChild(thoughtBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "node_output" && event.output) {
    const outputBox = document.createElement("div");
    outputBox.className = "node-output-box";
    outputBox.innerHTML = `<strong>Orchestrator coordinator output · ${escapeHtml(event.node_id || "node")} · ${escapeHtml(event.model || "")}</strong><pre>${escapeHtml(event.output)}</pre>`;
    chatMessagesEl.appendChild(outputBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "tool_call" && event.tool_call) {
    if (chatStatus) chatStatus.textContent = `⚡ Spreading work: Calling tool ${event.tool_call.name}...`;
  } else if (event.type === "tool_response" && event.tool_resp) {
    appendMessageToChat({ role: "tool", tool_response: event.tool_resp });
  } else if (event.type === "final_message" && event.message) {
    if (streamingAssistantEl) {
      streamingAssistantEl.remove();
      streamingAssistantEl = null;
    }
    appendMessageToChat(event.message);
  } else if (event.type === "text_delta" && event.content) {
    if (!streamingAssistantEl) {
      streamingAssistantEl = document.createElement("div");
      streamingAssistantEl.className = "chat-msg assistant";
      const meta = document.createElement("div");
      meta.className = "msg-meta";
      meta.textContent = "🤖 Pi";
      const bubble = document.createElement("div");
      bubble.className = "msg-bubble";
      streamingAssistantEl.append(meta, bubble);
      chatMessagesEl.appendChild(streamingAssistantEl);
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

// Event Listeners for Chat
interruptMsgBtn?.addEventListener("click", async () => {
  if (!isSending) return;
  if (chatStatus) chatStatus.textContent = "Interrupting...";
  if (activeChatMode !== "direct" && activeSessionId) {
    try {
      await fetchJSON(`/api/agent/sessions/${encodeURIComponent(activeSessionId)}/interrupt`, {
        method: "POST",
      });
    } catch (err) {
      console.error("interrupt agent session failed", err);
    }
  }
  if (activeAbortController) {
    activeAbortController.abort();
  }
});

// Event Listeners for Chat
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
  directMessages = [];
  if (chatStatus) chatStatus.textContent = "";
  renderDirectMessages();
});

dispatchDirectTaskBtn?.addEventListener("click", () => {
  void dispatchDirectTask(chatInput.value.trim());
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

// --- Persistent memory & learning ---

async function refreshMemory() {
  try {
    const [insights, facts, experiences, dialogues] = await Promise.all([
      fetchJSON("/api/memory/insights"),
      fetchJSON("/api/memory/facts"),
      memorySearch && memorySearch.value.trim()
        ? fetchJSON(`/api/memory/experiences?q=${encodeURIComponent(memorySearch.value.trim())}&limit=30`)
        : fetchJSON("/api/memory/experiences?limit=30"),
      fetchJSON("/api/memory/dialogues?limit=20"),
    ]);
    renderLearning(insights);
    renderFacts(facts);
    renderExperiences(experiences);
    renderTranscripts(dialogues);
  } catch (err) {
    console.error("memory refresh failed", err);
  }
}

function renderLearning(insights) {
  if (!learningSummary) return;
  if (!insights || !insights.total_experiences) {
    learningSummary.innerHTML = '<p style="color:var(--muted)">No experience recorded yet. The cluster starts learning as soon as it runs work.</p>';
    return;
  }

  const pairings = (insights.top_pairings || [])
    .map((p) => `<div class="row"><span>${escapeHtml(p.model)} on ${escapeHtml(p.node_id)} (${escapeHtml(p.task_type)})</span><span class="badge ${p.success_rate >= 0.7 ? "completed" : "failed"}">${Math.round(p.success_rate * 100)}% · ${p.avg_duration_ms}ms</span></div>`)
    .join("");

  const lessons = (insights.lessons || [])
    .map((l) => `<div class="lesson${/Avoid/i.test(l) ? " negative" : ""}">${escapeHtml(l)}</div>`)
    .join("") || '<p style="color:var(--muted);font-size:0.82rem">Not enough evidence for firm lessons yet.</p>';

  const taskTypes = Object.entries(insights.task_types || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `<div class="row"><span>${escapeHtml(k)}</span><span>${v}</span></div>`)
    .join("");

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

function renderFacts(facts) {
  if (!factsList) return;
  factsList.innerHTML = "";
  if (!facts || facts.length === 0) {
    factsList.innerHTML = '<p style="color:var(--muted)">Nothing remembered yet.</p>';
    return;
  }
  for (const f of facts) {
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
    factsList.appendChild(card);
  }
}

function renderExperiences(experiences) {
  if (!experiencesList) return;
  experiencesList.innerHTML = "";
  if (!experiences || experiences.length === 0) {
    experiencesList.innerHTML = '<p style="color:var(--muted)">No matching experience recorded.</p>';
    return;
  }
  for (const e of experiences) {
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
    experiencesList.appendChild(card);
  }
}

function renderTranscripts(dialogues) {
  if (!transcriptsList) return;
  transcriptsList.innerHTML = "";
  if (!dialogues || dialogues.length === 0) {
    transcriptsList.innerHTML = '<p style="color:var(--muted)">No stored conversations yet.</p>';
    return;
  }
  for (const d of dialogues) {
    const card = document.createElement("div");
    card.className = "card";
    const turns = (d.turns || []).slice(-6)
      .map((t) => `<div class="node-msg"><strong>${escapeHtml(t.speaker)}:</strong> ${escapeHtml((t.content || "").slice(0, 400))}</div>`)
      .join("");
    card.innerHTML = `
      <h3>${escapeHtml(d.id)} <span class="badge healthy">${(d.turns || []).length} turns</span></h3>
      <div class="row"><span>Participants</span><span>${escapeHtml((d.participants || []).join(", "))}</span></div>
      ${d.topic ? `<div class="row"><span>Topic</span><span>${escapeHtml(d.topic)}</span></div>` : ""}
      ${turns}
    `;
    transcriptsList.appendChild(card);
  }
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
      body: JSON.stringify({ key, value, scope: "cluster" }),
    });
    factKey.value = "";
    factValue.value = "";
    refreshMemory();
  } catch (err) {
    console.error("failed to save fact", err);
  }
});

let memorySearchTimer = null;
memorySearch?.addEventListener("input", () => {
  clearTimeout(memorySearchTimer);
  memorySearchTimer = setTimeout(refreshMemory, 300);
});

// --- Cluster Overview & Routing ---

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
  const utilization = Number(hardware.gpu_utilization_percent);
  if (!hardware.gpu_stats_available || !Number.isFinite(utilization)) {
    if (!hardware.has_gpu) return `<span class="gpu-gauge-unavailable" role="img" aria-label="No GPU detected">CPU node</span>`;
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
  const utilization = Number(hardware.npu_utilization_percent);
  if (!hardware.npu_stats_available || !Number.isFinite(utilization)) {
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

function acceleratorHistoryMarkup(node, type) {
  const label = type.toUpperCase();
  const history = (node[`${type}_history`] || []).slice(-60);
  const points = history
    .map((sample, index) => ({
      index,
      value: Number(sample.utilization_percent),
      available: sample.available,
    }))
    .filter((sample) => sample.available && Number.isFinite(sample.value));
  if (!points.length) {
    const message = node.hardware?.[`has_${type}`] ? `${label} telemetry unavailable` : `No ${label} detected`;
    return `<div class="accelerator-history-empty">${message}</div>`;
  }

  const width = 180;
  const height = 48;
  const left = 10;
  const right = 176;
  const top = 5;
  const bottom = 39;
  const denominator = Math.max(history.length - 1, 1);
  const x = (index) => left + (index / denominator) * (right - left);
  const y = (value) => bottom - (Math.max(0, Math.min(100, value)) / 100) * (bottom - top);
  const segments = [];
  let segment = [];
  history.forEach((sample, index) => {
    const value = Number(sample.utilization_percent);
    if (!sample.available || !Number.isFinite(value)) {
      if (segment.length) segments.push(segment);
      segment = [];
      return;
    }
    segment.push({ x: x(index), y: y(value) });
  });
  if (segment.length) segments.push(segment);
  const paths = segments
    .filter((items) => items.length > 1)
    .map((items) => `<path class="accelerator-history-line" d="${items.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ")}"></path>`)
    .join("");
  const isolatedPoints = segments
    .filter((items) => items.length === 1)
    .map(([point]) => `<circle class="accelerator-history-point" cx="${point.x}" cy="${point.y}" r="2.5"></circle>`)
    .join("");
  const latest = points[points.length - 1];
  const lastPoint = { x: x(latest.index), y: y(latest.value) };
  const latestPoint = segments.some((items) => items.length === 1 && items[0].x === lastPoint.x)
    ? ""
    : `<circle class="accelerator-history-point" cx="${lastPoint.x}" cy="${lastPoint.y}" r="2.5"></circle>`;
  return `
    <div class="accelerator-history">
      <div class="accelerator-history-heading"><span>${label} utilization</span><strong>${Math.round(latest.value)}%</strong></div>
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${label} utilization history for ${escapeHtml(node.id)}">
        <path class="accelerator-history-grid" d="M${left},${top}H${right} M${left},${(top + bottom) / 2}H${right} M${left},${bottom}H${right}"></path>
        ${paths}
        ${isolatedPoints}
        ${latestPoint}
      </svg>
      <small>Recent samples · up to 5-minute window</small>
    </div>`;
}

function gpuHistoryMarkup(node) {
  return acceleratorHistoryMarkup(node, "gpu");
}

function npuHistoryMarkup(node) {
  return acceleratorHistoryMarkup(node, "npu");
}

function systemHistoryMarkup(node) {
  const history = (node.system_history || []).slice(-60);
  if (!history.length) {
    return '<div class="system-history-empty">CPU and memory history is warming up.</div>';
  }
  const line = (availableKey, valueKey, className) => {
    const points = history.flatMap((sample, index) => {
      const value = Number(sample[valueKey]);
      return sample[availableKey] && Number.isFinite(value)
        ? [{ x: 8 + index * 164 / Math.max(history.length - 1, 1), y: 39 - Math.max(0, Math.min(100, value)) * 0.32 }]
        : [];
    });
    if (points.length < 2) return "";
    return `<path class="${className}" d="${points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ")}"></path>`;
  };
  const cpuValue = node.hardware?.cpu_stats_available ? `${node.hardware.cpu_utilization_percent}%` : "n/a";
  const memoryValue = node.hardware?.memory_stats_available ? `${node.hardware.memory_usage_percent}%` : "n/a";
  return `<div class="system-history">
    <div class="gpu-history-heading"><span>CPU ${cpuValue} · RAM ${memoryValue}</span><span>Recent</span></div>
    <svg viewBox="0 0 180 48" role="img" aria-label="CPU and memory utilization history for ${escapeHtml(node.id)}">
      <path class="gpu-history-grid" d="M8,7H172 M8,23H172 M8,39H172"></path>
      ${line("cpu_available", "cpu_utilization_percent", "system-history-cpu")}
      ${line("memory_available", "memory_usage_percent", "system-history-memory")}
    </svg>
    <small>CPU · RAM · up to 5-minute window</small>
  </div>`;
}

const TASK_ACTIVE_STATES = new Set(["pending", "running", "synthesizing", "awaiting_approval"]);
const TASK_STATES = new Set([...TASK_ACTIVE_STATES, "completed", "failed", "cancelled"]);
const SUBTASK_STATES = new Set(["pending", "running", "completed", "failed", "cancelled"]);

function knownStatus(status, states) {
  return states.has(status) ? status : "unknown";
}

function formatRelativeTime(value) {
  if (!value) return "time unavailable";
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "time unavailable";
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(timestamp).toLocaleDateString();
}

function nodeAssignments(tasks) {
  const assigned = new Map();
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
  const memberIDs = new Set(nodeList.map((node) => node.id));
  const discoveredNodes = (peers || [])
    .filter((peer) => peer.role === "node" && peer.id && !memberIDs.has(peer.id))
    .map((peer) => ({
      id: peer.id,
      address: peer.peer_addr || peer.http_addr,
      healthy: true,
      discovered: true,
      last_seen: peer.last_seen,
      models: [],
      active_tasks: 0,
      hardware: {},
    }));
  const displayNodes = sortNodesByCapability([...nodeList, ...discoveredNodes]);
  const taskList = [...(tasks || [])].sort((a, b) =>
    new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
    const renderTask = (task) => {
      const taskStatus = knownStatus(task.status, TASK_STATES);
      const subtasks = (task.subtasks || []).map((subtask) => {
        const status = knownStatus(subtask.status, SUBTASK_STATES);
        const target = subtask.node_id
          ? `<button class="activity-assignment" type="button" data-target-node="${escapeHtml(subtask.node_id)}">${escapeHtml(subtask.node_id)}</button>`
          : '<span class="activity-unassigned">unassigned</span>';
        return `<div class="activity-subtask"><span>${escapeHtml(subtask.subtask?.id || "subtask")} · ${escapeHtml(subtask.subtask?.task_type || "work")}</span><span class="badge ${status}">${status}</span><span class="activity-target">→ ${target}</span></div>`;
      }).join("");
      return `<article class="activity-card ${TASK_ACTIVE_STATES.has(task.status) ? "active" : "history"}" data-task-id="${escapeHtml(task.id)}">
        <div class="activity-card-heading"><strong>${escapeHtml(task.id)}</strong><span class="badge ${taskStatus}">${taskStatus}</span></div>
        <p>${escapeHtml(task.description || "No task description")}</p>
        <small>${TASK_ACTIVE_STATES.has(task.status) ? "Updated" : "Finished"} ${formatRelativeTime(task.updated_at || task.created_at)}</small>
        <div class="activity-subtasks">${subtasks || '<span class="activity-unassigned">No assigned subtasks</span>'}</div>
      </article>`;
    };
    overviewActivity.innerHTML = activeTasks.length || recentTasks.length
      ? `${activeTasks.length ? `<h4 class="activity-group-heading">Active · ${activeTasks.length}</h4>${activeTasks.map(renderTask).join("")}` : ""}
         ${recentTasks.length ? `<h4 class="activity-group-heading">Recent</h4>${recentTasks.map(renderTask).join("")}` : ""}`
      : '<p class="overview-empty">No task activity yet.</p>';
  }

  if (parts.nodes !== false && overviewNodes) {
    const leaderID = nodeList.find((node) => node.leader)?.id;
    overviewNodes.innerHTML = displayNodes.length
      ? displayNodes.map((node) => {
          const nodeTasks = assignments.get(node.id) || [];
          const state = node.discovered ? "discovered" : node.healthy ? "online" : "offline";
          const status = node.discovered ? "available · not paired" : state;
          const lastSeen = node.last_seen
            ? `${node.discovered ? "Seen" : "Last seen"} ${formatRelativeTime(node.last_seen)}`
            : "Last seen unavailable";
          return `<article class="overview-node ${state}" data-node-id="${escapeHtml(node.id)}">
            <div class="overview-node-heading"><span class="legend-dot ${node.discovered ? "peer" : state}"></span><strong>${escapeHtml(node.id)}</strong><span class="badge ${state}">${status}</span></div>
            <div class="overview-node-address">${escapeHtml(node.address || "Address unavailable")}</div>
            <div class="overview-node-meta">${node.discovered ? "Discovered on the local network" : `${node.id === leaderID ? "Cluster leader · " : ""}${node.active_tasks || 0} active tasks · ${(node.models || []).length} models`}</div>
            <div class="overview-node-metrics">${gpuGaugeMarkup(node, 36)}${npuGaugeMarkup(node, 36)}${node.hardware?.cpu_stats_available ? `<span>CPU ${node.hardware.cpu_utilization_percent}%</span>` : ""}${node.hardware?.memory_stats_available ? `<span>RAM ${node.hardware.memory_usage_percent}%</span>` : ""}</div>
            <small data-last-seen="${escapeHtml(node.last_seen || "")}">${lastSeen}</small>
            ${nodeTasks.length ? `<div class="overview-node-assignments">${nodeTasks.map((item) => `<span class="assignment-chip ${item.status}">${escapeHtml(item.taskID)} · ${item.status}</span>`).join("")}</div>` : ""}
          </article>`;
        }).join("")
      : '<p class="overview-empty">No known cluster members.</p>';
    const nodesByID = new Map(displayNodes.map((node) => [node.id, node]));
    overviewNodes.querySelectorAll("[data-node-id]").forEach((card) => {
      const node = nodesByID.get(card.dataset.nodeId);
      if (node) attachNodeContextMenu(card, node);
    });
  }

  if (parts.peers !== false && clusterPeers) {
    const discovered = (peers || []).filter((peer) => peer.role && peer.role !== "node");
    clusterPeers.innerHTML = discovered.length
      ? `<span class="cluster-peers-label">Discovered peers</span>${discovered.map((peer) => `<span class="cluster-peer"><i class="legend-dot peer"></i><strong>${escapeHtml(peer.id || "unknown")}</strong><small>${escapeHtml(peer.role)} · ${escapeHtml(peer.http_addr || "address unavailable")}</small></span>`).join("")}`
      : "";
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

function renderNodes(nodes) {
  currentNodes = nodes || [];
  if (!nodesList) return;
  nodesList.innerHTML = "";
  if (!nodes || nodes.length === 0) {
    nodesList.innerHTML = '<p style="color:var(--muted)">No nodes registered yet.</p>';
    return;
  }

  const ordered = sortNodesByCapability(nodes);
  const leaderID = ordered.find((n) => n.leader)?.id;
  const fastestID = ordered.reduce((best, node) => !best || node.fast_score > best.fast_score || (node.fast_score === best.fast_score && node.id < best.id) ? node : best, null)?.id;

  for (const n of ordered) {
    const card = document.createElement("div");
    card.className = "card" + (n.id === leaderID ? " leader" : "");
    const gpuGauge = gpuGaugeMarkup(n, 46);
    const npuGauge = npuGaugeMarkup(n, 46);
    const modelTags = (n.models || [])
      .map((m) => `<button class="model-tag-btn" data-node="${escapeHtml(n.id)}" data-model="${escapeHtml(m.name)}" title="Directly test ${escapeHtml(m.name)} on ${escapeHtml(n.id)}" ${n.healthy ? "" : "disabled"}>⚡ ${escapeHtml(m.name)}</button>`)
      .join("");
    const nodeActions = n.healthy
      ? `<button class="model-config-toggle" data-node="${escapeHtml(n.id)}">⚙ Configure models</button>
         <button class="node-model-refresh" data-node="${escapeHtml(n.id)}">↻ Refresh local registry</button>
         <button class="node-terminal-open" data-node="${escapeHtml(n.id)}">▸ Open node terminal</button>`
      : `<small class="node-offline-note">Offline · last seen ${formatRelativeTime(n.last_seen)}</small>`;
    card.innerHTML = `
      <div class="node-card-heading"><h3>${escapeHtml(n.id)} <span class="badge ${n.healthy ? "healthy" : "unhealthy"}">${n.healthy ? "online" : "offline"}</span></h3><div class="node-accelerator-gauges">${gpuGauge}${npuGauge}</div></div>
      ${n.version_error ? `<div class="node-version-error">⚠ ${escapeHtml(n.version_error)}</div>` : ""}
      <div class="role-tags">${n.id === leaderID ? '<span class="role-tag coordinator">cluster leader · orchestrator</span>' : ''}${n.id === fastestID && n.id !== leaderID ? '<span class="role-tag worker">fastest</span>' : ''}</div>
      <div class="row"><span>Address</span><span>${escapeHtml(n.address)}</span></div>
      <div class="row"><span>OS / Arch</span><span>${escapeHtml(n.hardware.os)}/${escapeHtml(n.hardware.arch)}</span></div>
      <div class="row"><span>CPU cores</span><span>${n.hardware.cpu_cores}</span></div>
      <div class="row"><span>RAM</span><span>${(n.hardware.total_ram_mb / 1024).toFixed(1)} GB</span></div>
      <div class="row"><span>GPU</span><span>${n.hardware.has_gpu ? escapeHtml(n.hardware.gpu_vendor || "enabled") : "none"}</span></div>
      <div class="row"><span>NPU</span><span>${n.hardware.has_npu ? escapeHtml(n.hardware.npu_vendor || "enabled") : "none"}</span></div>
      ${gpuHistoryMarkup(n)}
      ${npuHistoryMarkup(n)}
      ${systemHistoryMarkup(n)}
      ${!n.healthy ? `<div class="row"><span>Last seen</span><span>${escapeHtml(n.last_seen ? new Date(n.last_seen).toLocaleString() : "unknown")}</span></div>` : ""}
      <div class="row"><span>Active tasks</span><span>${n.active_tasks}</span></div>
      <div class="row"><span>Score</span><span>${n.hardware.score.toFixed(1)}</span></div>
      <div class="row"><span>Fast score</span><span>${(n.fast_score || 0).toFixed(1)}</span></div>
      <div class="row"><span>Default model</span><span>${escapeHtml(n.default_model || "automatic")}</span></div>
      <div>${modelTags || '<span style="color:var(--muted);font-size:0.75rem">No models installed</span>'}</div>
      ${nodeActions}
      <div class="model-config-panel" id="model-config-${cssId(n.id)}" style="display:${modelPanelOpen.has(n.id) ? "block" : "none"}"></div>
    `;
    nodesList.appendChild(card);
    card.querySelector(".node-terminal-open")?.addEventListener("click", () => openNodeTerminal(n));
    card.querySelector(".node-model-refresh")?.addEventListener("click", () => refreshNodeModels(n.id));
    attachNodeContextMenu(card, n);

    // Wire model direct test buttons
    card.querySelectorAll(".model-tag-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const nodeId = btn.dataset.node;
        const model = btn.dataset.model;
        btn.textContent = "⏳ Testing…";
        try {
          const res = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/${encodeURIComponent(model)}/test`, { method: "POST" });
          if (res.healthy) {
            btn.textContent = `✓ ${model} (${res.duration_ms}ms)`;
          } else {
            btn.textContent = `✗ ${model} (err)`;
          }
        } catch (e) {
          btn.textContent = `✗ ${model}`;
        }
        setTimeout(() => { btn.textContent = `⚡ ${model}`; }, 3500);
      });
    });
    if (modelPanelOpen.has(n.id)) {
      if (lastFetchedModels.has(n.id)) {
        renderModelPanel(n.id, lastFetchedModels.get(n.id));
      } else {
        loadModelPanel(n.id);
      }
    }
  }

  nodesList.querySelectorAll(".model-config-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nodeId = btn.dataset.node;
      if (modelPanelOpen.has(nodeId)) {
        modelPanelOpen.delete(nodeId);
        lastFetchedModels.delete(nodeId);
      } else {
        modelPanelOpen.add(nodeId);
      }
      renderNodes(nodes);
    });
  });
}

function renderSetupNodes(nodes) {
  if (!setupNodeList) return;
  const ordered = sortNodesByCapability(nodes || []);
  if (setupNodeSummary) {
    const online = ordered.filter((node) => node.healthy).length;
    setupNodeSummary.textContent = `${online} online · ${ordered.length - online} offline · ${ordered.length} total`;
  }
  if (!ordered.length) {
    setupNodeList.innerHTML = '<p class="setup-empty">No cluster members are registered yet. Pair a node to add it to the cluster.</p>';
    return;
  }
  setupNodeList.innerHTML = "";
  for (const node of ordered) {
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
    attachNodeContextMenu(card, node);
    setupNodeList.appendChild(card);
  }
}

async function openNodeTerminal(node) {
  terminalNode = node;
  terminalDialogueId = null;
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
  terminalNode = null;
  terminalDialogueId = null;
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
  terminalAbortController = new AbortController();
  if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = false;
  try {
    if (!terminalDialogueId) {
      const dialogue = await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model }), signal: terminalAbortController.signal });
      terminalDialogueId = dialogue.id;
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
    terminalAbortController = null;
    if (nodeTerminalInterruptBtn) nodeTerminalInterruptBtn.hidden = true;
  }
});

nodeTerminalInterruptBtn?.addEventListener("click", async () => {
  if (terminalNode && terminalDialogueId) {
    try {
      await fetchJSON(`/api/nodes/${encodeURIComponent(terminalNode.id)}/dialogues/${encodeURIComponent(terminalDialogueId)}/interrupt`, {
        method: "POST",
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
  contextNode = node;
  status.textContent = "Loading properties...";
  modal.hidden = false;
  try {
    const props = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/properties`);
    nodeProperties.set(node.id, props);
    for (const [name, value] of Object.entries(props)) {
      const field = form.elements.namedItem(name);
      if (!field) continue;
      if (field.type === "checkbox") field.checked = Boolean(value);
      else if (name === "disabled_models") field.value = (value || []).join("\n");
      else field.value = value ?? "";
    }
    document.getElementById("node-properties-title").textContent = `${node.id} properties`;
    document.getElementById("node-properties-subtitle").textContent = "Edit runtime settings. Identity and listen address apply after restart.";
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
        memory_path: value("memory_path"),
      }),
    });
    status.textContent = "Saved";
    await refresh();
    setTimeout(closeNodeProperties, 500);
  } catch (err) { status.textContent = "Error: " + err.message; }
}

async function refreshNodeModels(nodeId) {
  const button = [...(nodesList?.querySelectorAll(".node-model-refresh") || [])]
    .find((candidate) => candidate.dataset.node === nodeId);
  if (button) button.textContent = "Refreshing...";
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
  refreshAllModelsBtn.textContent = "Refreshing...";
  try {
    await fetchJSON("/api/nodes/models/refresh", { method: "POST" });
    await refresh();
    refreshAllModelsBtn.textContent = "Registries refreshed";
  } catch (err) {
    refreshAllModelsBtn.textContent = "Refresh failed";
    console.error("all-node model refresh failed", err);
  } finally {
    setTimeout(() => {
      refreshAllModelsBtn.disabled = false;
      refreshAllModelsBtn.textContent = "↻ Refresh all model registries";
    }, 1800);
  }
});

function showNodeMenu(x, y) {
  const menu = document.getElementById("node-context-menu");
  if (!menu || !contextNode) return;
  const properties = menu.querySelector("[data-action=properties]");
  const pairing = menu.querySelector("[data-action=pair]");
  const nodeIsDiscovered = Boolean(contextNode.discovered);
  if (properties) {
    properties.textContent = `Configure ${contextNode.id}`;
    properties.disabled = !contextNode.healthy || nodeIsDiscovered;
  }
  if (pairing) {
    pairing.textContent = nodeIsDiscovered ? "Pair node to this cluster" : "Re-pair node with this cluster";
    pairing.disabled = !contextNode.healthy;
  }
  menu.querySelector("[data-action=open]").disabled = !contextNode.healthy || nodeIsDiscovered;
  menu.hidden = false;
  const bounds = menu.getBoundingClientRect();
  menu.style.left = `${Math.max(8, Math.min(x, window.innerWidth - bounds.width - 8))}px`;
  menu.style.top = `${Math.max(8, Math.min(y, window.innerHeight - bounds.height - 8))}px`;
}

function attachNodeContextMenu(element, node) {
  element.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    contextNode = node;
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
    renderModelPanel(nodeId, models);
  } catch (err) {
    panel.innerHTML = `<p style="color:var(--bad)">Failed to load models: ${escapeHtml(err.message)}</p>`;
  }
}

function renderModelPanel(nodeId, models) {
  const panel = document.getElementById(`model-config-${cssId(nodeId)}`);
  if (!panel) return;
  if (!models || models.length === 0) {
    panel.innerHTML = '<p style="color:var(--muted)">No models discovered on this node.</p>';
    return;
  }
  const state = pendingModelState.get(nodeId);
  const defaultModel = pendingDefaultModel.get(nodeId) || "";
  const rows = models
    .map((m) => {
      const checked = state.get(m.name) ? "checked" : "";
      return `
        <label class="model-config-row">
          <input type="checkbox" data-model="${escapeHtml(m.name)}" ${checked} />
          <span class="model-config-name">${escapeHtml(m.name)}</span>
          <span class="model-config-meta">${(m.tags || []).join(", ")} · ${m.engine}${m.size_gb ? ` · ${m.size_gb.toFixed(1)} GB` : ""}</span>
        </label>`;
    })
    .join("");
  panel.innerHTML = `
    <label class="model-default-row">
      <span>Default model</span>
      <select class="model-default-select">
        <option value="">Automatic best match</option>
        ${models.map((m) => `<option value="${escapeHtml(m.name)}" ${m.name === defaultModel ? "selected" : ""} ${state.get(m.name) ? "" : "disabled"}>${escapeHtml(m.name)}</option>`).join("")}
      </select>
    </label>
    ${rows}
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
      body: JSON.stringify({ disabled_models: disabled, default_model: defaultModel }),
    });
    if (statusEl) statusEl.textContent = "Saved";
    lastFetchedModels.delete(nodeId);
    refresh();
  } catch (err) {
    if (statusEl) statusEl.textContent = "Error: " + err.message;
  }
}

function renderRouting(nodes, tasks) {
  if (!routingList) return;
  routingList.innerHTML = "";
  if (!nodes || nodes.length === 0) {
    routingList.innerHTML = '<p style="color:var(--muted)">No nodes registered yet.</p>';
    return;
  }

  const byNode = new Map(nodes.map((n) => [n.id, {
    node: n, pending: 0, running: 0, completed: 0, failed: 0, cancelled: 0, unknown: 0, recent: [],
  }]));
  const sortedTasks = [...(tasks || [])].sort((a, b) =>
    new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
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
    return (b.running + b.pending) - (a.running + a.pending);
  });
  for (const { node, pending, running, completed, failed, cancelled, unknown, recent } of workload) {
    const card = document.createElement("div");
    card.className = "card";
    const total = pending + running + completed + failed + cancelled + unknown;
    const pct = (n) => (total === 0 ? 0 : Math.round((n / total) * 100));
    const recentHTML = recent.length
      ? recent
          .map(
            (r) => `<div class="row"><span>${r.taskId}/${r.subtaskId} (${r.taskType}) &rarr; ${escapeHtml(r.model)}</span><span class="badge ${r.status}">${r.status}</span></div>`
          )
          .join("")
      : '<p style="color:var(--muted);font-size:0.8rem">No jobs routed yet.</p>';
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
    routingList.appendChild(card);
  }
}

function visibleNetworkPeers(peers, nodes, pairingStatus) {
  const visible = new Map((peers || []).map((peer) => [peer.id, peer]));
  for (const node of nodes || []) {
    if (!node.healthy || visible.has(node.id)) continue;
    visible.set(node.id, {
      id: node.id, role: "node", peer_addr: node.address, last_seen: node.last_seen,
      orchestrator_id: pairingStatus?.orchestrator_id,
    });
  }
  if (pairingStatus?.role === "node" && pairingStatus.node_id) {
    visible.set(pairingStatus.node_id, {
      ...visible.get(pairingStatus.node_id),
      id: pairingStatus.node_id, role: "node", peer_addr: pairingStatus.node_addr,
      orchestrator_id: pairingStatus.joined ? pairingStatus.orchestrator_id : "",
      local: true,
    });
  }
  return [...visible.values()].sort((a, b) => (a.id || "").localeCompare(b.id || ""));
}

function renderDiscovery(peers) {
  if (!discoveryList) return;
  discoveryList.innerHTML = "";
  const ordered = visibleNetworkPeers(peers, currentNodes, currentPairingStatus);
  if (ordered.length === 0) {
    discoveryList.innerHTML = '<p style="color:var(--muted)">No visible nodes found on the local network yet.</p>';
    return;
  }

  for (const p of ordered) {
    const card = document.createElement("div");
    card.className = "card";
    const associated = p.role === "node" ? (p.orchestrator_id || "Not announced") : p.role === "orchestrator" ? "self" : "Not elected";
    const member = p.role === "node" ? currentNodes.find((node) => node.id === p.id) : null;
    const localMember = p.id === currentPairingStatus?.node_id && currentPairingStatus?.joined;
    card.innerHTML = `
      <h3>${escapeHtml(p.id)} <span class="badge healthy">${escapeHtml(p.role)}</span></h3>
      <div class="row"><span>Address</span><span>${escapeHtml(p.peer_addr || p.http_addr || "Address unavailable")}</span></div>
      ${p.role === "node" ? `<div class="row"><span>Status</span><span>${member || localMember ? "Visible · cluster member" : "Visible · not a member of this cluster"}${p.local ? " · local node" : ""}</span></div>` : ""}
      <div class="row"><span>Associated orchestrator</span><span>${escapeHtml(associated)}</span></div>
      <div class="row"><span>Last seen</span><span>${p.local ? "This device" : p.last_seen ? new Date(p.last_seen).toLocaleTimeString() : "Online in cluster"}</span></div>
    `;
    discoveryList.appendChild(card);
    if (p.role === "node") {
      attachNodeContextMenu(card, member || {
        id: p.id,
        address: p.peer_addr || p.http_addr,
        healthy: true,
        discovered: !p.local,
        last_seen: p.last_seen,
        models: [],
        active_tasks: 0,
        hardware: {},
      });
    }
  }
}

function renderTasks(tasks) {
  if (!tasksList) return;
  tasksList.innerHTML = "";
  if (!tasks || tasks.length === 0) {
    tasksList.innerHTML = '<p style="color:var(--muted)">No tasks submitted yet.</p>';
    return;
  }
  const orderedTasks = [...tasks].sort((a, b) =>
    new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
  );
  for (const t of orderedTasks) {
    const card = document.createElement("div");
    card.className = "card";
    const subtaskHTML = (t.subtasks || [])
      .map(
        (s) => `
        <div class="row"><span>${escapeHtml(s.subtask.id)} · ${escapeHtml(s.subtask.role || s.subtask.task_type)} (${escapeHtml(s.subtask.task_type)}) &rarr; ${escapeHtml(s.node_id)} / ${escapeHtml(s.model)}</span>
        <span class="badge ${s.status}">${s.status}</span></div>
        ${s.score_factors ? `<details class="routing-score-details"><summary>Routing score ${Number(s.score || 0).toFixed(1)}</summary><div class="catalog-tags">Model match +${Number(s.score_factors.model_match || 0).toFixed(1)} · hardware +${Number(s.score_factors.hardware || 0).toFixed(1)} · load −${Number(s.score_factors.load_penalty || 0).toFixed(1)} (${s.score_factors.active_tasks || 0} active + ${s.score_factors.queued_tasks || 0} queued) · GPU −${Number(s.score_factors.gpu_utilization_penalty || 0).toFixed(1)} · NPU −${Number(s.score_factors.npu_utilization_penalty || 0).toFixed(1)} · learned ${Number(s.score_factors.learned_bias || 0) >= 0 ? "+" : ""}${Number(s.score_factors.learned_bias || 0).toFixed(1)} · telemetry ${s.score_factors.telemetry_fresh ? `fresh (${s.score_factors.telemetry_age_ms || 0}ms)` : "stale/unavailable, ignored"}</div></details>` : ""}
        ${s.output ? `<div class="subtask-output-wrap"><button class="copy-output-btn" type="button" data-copy-output="${encodeURIComponent(s.output)}">Copy</button><div class="subtask-output">${escapeHtml(s.output)}</div></div>` : ""}
        ${s.error ? `<div class="subtask-output" style="border-color:var(--bad)">${escapeHtml(s.error)}</div>` : ""}
      `
      )
      .join("");
    card.innerHTML = `
      <h3>${escapeHtml(t.id)} <span class="badge ${t.status}">${escapeHtml(t.status)}</span>${t.status === "running" || t.status === "awaiting_approval" ? ` <button class="cancel-task-btn" type="button" data-task-id="${escapeHtml(t.id)}">Cancel</button>` : ""}${t.status === "awaiting_approval" ? ` <button class="approve-task-btn primary-btn" type="button" data-task-id="${escapeHtml(t.id)}">Approve and synthesize</button>` : ""}</h3>
      <div class="row"><span colspan="2">${escapeHtml(t.description)}</span></div>
      ${t.error ? `<div class="subtask-output" style="border-color:var(--bad)">${escapeHtml(t.error)}</div>` : ""}
      ${subtaskHTML}
      ${t.result ? `<div class="subtask-output-wrap"><strong>Task result</strong><div class="subtask-output">${escapeHtml(t.result)}</div></div>` : ""}
    `;
    tasksList.appendChild(card);
    card.querySelector(".cancel-task-btn")?.addEventListener("click", async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      try {
        await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/cancel`, { method: "POST" });
      } catch (err) {
        if (submitStatus) submitStatus.textContent = `Unable to cancel ${t.id}: ${err.message}`;
      }
    });
    card.querySelector(".approve-task-btn")?.addEventListener("click", async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      try {
        await fetchJSON(`/api/tasks/${encodeURIComponent(t.id)}/approve`, { method: "POST" });
      } catch (err) {
        button.disabled = false;
        if (submitStatus) submitStatus.textContent = `Unable to approve ${t.id}: ${err.message}`;
      }
    });
    card.querySelectorAll("[data-copy-output]").forEach((button) => {
      button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copyOutput), button));
    });
  }
}

async function copyText(text, button) {
  try {
    await navigator.clipboard.writeText(text);
    const original = button.textContent;
    button.textContent = "Copied";
    setTimeout(() => { button.textContent = original; }, 1200);
  } catch (err) {
    button.textContent = "Copy failed";
  }
}

function taskText(tasks) {
  return (tasks || []).map((t) => [
    `Task ${t.id}: ${t.description}`,
    ...(t.subtasks || []).map((s) => `${s.subtask.id} (${s.status}) on ${s.node_id}:\n${s.output || s.error || ""}`),
  ].join("\n")).join("\n\n");
}

copyTasksBtn?.addEventListener("click", () => copyText(taskText(window.latestTasks || []), copyTasksBtn));

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

async function refresh() {
  try {
    const [nodes, tasks, peers, build] = await Promise.all([
      fetchJSON("/api/nodes/all"),
      fetchJSON("/api/tasks"),
      fetchJSON("/api/discovery"),
      fetchJSON("/api/version"),
    ]);
    applySnapshot({ nodes, tasks, discovery: peers, build_version: build.version, complete_tasks: true });
  } catch (err) {
    console.error(err);
  }
}

async function refreshDiscovery() {
  try {
    const discovery = await fetchJSON("/api/discovery");
    applySnapshot({
      nodes: window.latestNodes || [],
      tasks: window.latestTasks || [],
      discovery,
    });
  } catch (err) {
    console.error("local network refresh failed", err);
  }
}

setInterval(refreshDiscovery, 5000);

function applySnapshot(snap) {
  if (snap.complete_tasks) {
    window.latestTasks = snap.tasks || [];
    window.fullTaskHistoryLoaded = true;
  } else if (window.fullTaskHistoryLoaded) {
    const merged = new Map((window.latestTasks || []).map((task) => [task.id, task]));
    for (const task of snap.tasks || []) merged.set(task.id, task);
    window.latestTasks = [...merged.values()].sort((a, b) =>
      new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
    );
  } else {
    window.latestTasks = snap.tasks || [];
  }
  window.latestNodes = snap.nodes || [];
  const nodeCount = snap.member_count ?? window.latestNodes.filter((node) => node.healthy).length;
  updateClusterTitle(nodeCount);

  // Update header stats pills
  if (statNodes) statNodes.textContent = `Nodes: ${nodeCount}`;
  let modelCount = 0;
  let activeLoad = 0;
  for (const n of window.latestNodes) {
    if (!n.healthy) continue;
    modelCount += (n.models || []).length;
    activeLoad += (n.active_tasks || 0);
  }
  if (statModels) statModels.textContent = `Models: ${modelCount}`;
  if (statLoad) statLoad.textContent = `Active Load: ${activeLoad}`;
  if (statMemory && snap.memory) {
    statMemory.textContent = `Memory: ${snap.memory.total_experiences || 0} · ${Math.round((snap.memory.success_rate || 0) * 100)}%`;
  }
  if (statVersion && snap.build_version) statVersion.textContent = snap.build_version;

  const nodeState = window.latestNodes.map((node) => ({
    id: node.id, version: node.version, address: node.address, hardware: node.hardware,
    models: node.models, default_model: node.default_model, fast_score: node.fast_score,
    active_tasks: node.active_tasks, healthy: node.healthy, leader: node.leader,
    version_error: node.version_error, last_seen: node.healthy ? undefined : node.last_seen,
    gpu_sample: node.gpu_history?.at(-1)?.timestamp,
    npu_sample: node.npu_history?.at(-1)?.timestamp,
    system_sample: node.system_history?.at(-1)?.timestamp,
  }));
  const nodeSignature = JSON.stringify(nodeState);
  const overviewNodeState = window.latestNodes.map((node) => ({
    id: node.id,
    address: node.address,
    healthy: node.healthy,
    leader: node.leader,
    active_tasks: node.active_tasks,
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
  }));
  const taskState = window.latestTasks.map((task) => ({
    id: task.id, description: task.description, status: task.status, updated_at: task.updated_at,
    subtasks: (task.subtasks || []).map((item) => ({
      id: item.subtask?.id, type: item.subtask?.task_type, status: item.status, node_id: item.node_id,
      model: item.model,
    })),
  }));
  const taskSignature = JSON.stringify(window.latestTasks.map((task) => [task.id, task.updated_at]));
  const activitySignature = JSON.stringify(taskState);
  const peerState = (snap.discovery || []).map((peer) => ({
    id: peer.id, role: peer.role, address: peer.http_addr, peer_addr: peer.peer_addr, last_seen: peer.last_seen,
    orchestrator_id: peer.orchestrator_id,
  }));
  const peerSignature = JSON.stringify({ peers: peerState, nodes: nodeState.map((node) => ({
    id: node.id, address: node.address, healthy: node.healthy, last_seen: node.last_seen,
  })), pairing: currentPairingStatus });
  const discoveredNodeState = (snap.discovery || [])
    .filter((peer) => peer.role === "node")
    .map((peer) => ({ id: peer.id, address: peer.peer_addr || peer.http_addr, last_seen: peer.last_seen }));
  const overviewNodeSignature = JSON.stringify({ members: overviewNodeState, discovered: discoveredNodeState });
  let overviewChanged = false;
  const overviewParts = {
    activity: activitySignature !== window.lastActivitySignature,
    nodes: overviewNodeSignature !== window.lastOverviewNodeSignature,
    peers: peerSignature !== window.lastOverviewPeerSignature,
  };
  if (overviewParts.activity || overviewParts.nodes || overviewParts.peers) {
    renderOverview(window.latestNodes, window.latestTasks, snap.discovery || [], overviewParts);
    window.lastActivitySignature = activitySignature;
    window.lastOverviewNodeSignature = overviewNodeSignature;
    window.lastOverviewPeerSignature = peerSignature;
    overviewChanged = true;
  }
  if (nodeSignature !== window.lastNodeSignature) {
    renderNodes(window.latestNodes);
    renderSetupNodes(window.latestNodes);
    window.lastNodeSignature = nodeSignature;
  }
  const routingSignature = JSON.stringify({
    nodes: nodeState.map((node) => ({ id: node.id, healthy: node.healthy, active_tasks: node.active_tasks })),
    tasks: taskState.map((task) => ({
      id: task.id, updated_at: task.updated_at,
      subtasks: task.subtasks,
    })),
  });
  if (routingSignature !== window.lastRoutingSignature) {
    renderRouting(window.latestNodes, window.latestTasks);
    window.lastRoutingSignature = routingSignature;
  }
  if (peerSignature !== window.lastPeerSignature) {
    renderDiscovery(snap.discovery || []);
    window.lastPeerSignature = peerSignature;
  }
  if (taskSignature !== window.lastTaskSignature) {
    renderTasks(window.latestTasks);
    window.lastTaskSignature = taskSignature;
  }
  if (overviewChanged) refreshOverviewTimes();
}

function refreshOverviewTimes() {
  overviewNodes?.querySelectorAll("[data-last-seen]").forEach((label) => {
    label.textContent = `Last seen ${formatRelativeTime(label.dataset.lastSeen)}`;
  });
}

setInterval(refreshOverviewTimes, 15000);

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
        require_approval: taskRequireApproval?.checked || false,
      }),
    });
    submitStatus.textContent = `Submitted as ${task.id}`;
    taskInput.value = "";
    refresh();
  } catch (err) {
    submitStatus.textContent = "Error: " + err.message;
  }
});

// Initialization
loadAgentTools();
loadAgentBackend().then(loadSessions);
loadPairingStatus();
refreshPersistenceStatus();
setInterval(loadPairingStatus, 5000);
setInterval(refreshPersistenceStatus, 30000);
setInterval(() => {
  if (activeChatMode === "direct") void refreshDirectClusterContext();
}, 15000);
loadClusterModels();
refresh();
checkSetupReadiness();
loadModelCatalog();
connectEvents();

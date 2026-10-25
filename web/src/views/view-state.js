const VIEW_STATE_KEY = "maistr0.viewState";
const DIRECT_CHAT_KEY = "maistr0.directChat";
const MAX_SAVED_DIRECT_MESSAGES = 100;
const SUB_TAB_SELECTOR = ".cluster-workspace-tab, .setup-tab, .overview-tab";
const refreshViewBtn = document.getElementById("refresh-view-btn");

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
    // Storage can be unavailable or full; reloading then falls back to defaults.
  }
}

function captureViewState() {
  return {
    primary: document.querySelector(".nav-tab.active")?.id || "",
    agent: document.querySelector(".agent-tab.active")?.dataset.agent || "direct",
    subTabs: [...document.querySelectorAll(SUB_TAB_SELECTOR)]
      .filter((tab) => tab.id && tab.getAttribute("aria-selected") === "true")
      .map((tab) => tab.id),
  };
}

function savedDirectMessages(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((message) => message && ["user", "assistant"].includes(message.role) &&
      typeof message.content === "string")
    .map((message) => ({ role: message.role, content: message.content }))
    .slice(-MAX_SAVED_DIRECT_MESSAGES);
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
    directMessages = messages;
    if (activeChatMode === "direct") renderDirectMessages();
  }
  const saved = readSessionJSON(VIEW_STATE_KEY);
  if (!saved || typeof saved !== "object") return;
  // Sub-tabs first so activating the primary tab loads the restored panel.
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
  for (const key of Object.keys(window)) {
    if (/^last\w*Signature$/.test(key)) window[key] = undefined;
  }
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

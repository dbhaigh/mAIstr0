import { activeChatMode, agentTabs, persistenceWarning } from "./state.js";
import { activateDirectChat } from "../views/orchestrator-controls.js";
import { loadAgentBackend, loadAgentTools, loadSessions } from "../views/orchestrator-sessions.js";

export async function fetchJSON(url, opts) {
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

export async function refreshPersistenceStatus() {
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

export function updateClusterTitle(count) {
  const title = document.querySelector("header h1");
  if (title) title.textContent = `mAIstr0 · ${count} ${count === 1 ? "Node" : "Nodes"}`;
}

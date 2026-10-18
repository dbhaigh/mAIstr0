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
    void refresh();
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
    const endpoint = invitation
      ? `/api/discovery/${encodeURIComponent(node.id)}/pairing/invite`
      : joiningOrchestrator
        ? "/api/pairing/connect"
      : node
        ? `/api/nodes/${encodeURIComponent(node.id)}/pairing/connect`
        : "/api/pairing/connect";
    const result = await fetchJSON(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(invitation
        ? { pin: pairingCode.value.trim() }
        : {
            address: pairingAddress.value.trim(),
            pin: pairingCode.value.trim(),
            ...(joiningOrchestrator ? { orchestrator_id: node.id } : {}),
          }),
    });
    pairingCode.value = "";
    pairingStatus.textContent = invitation
      ? `Invitation sent to ${node.id}. Confirm it on the node to complete pairing.`
      : joiningOrchestrator
        ? `Connected to ${result.orchestrator_id}. Waiting for cluster registration…`
      : node
        ? `${node.id} paired with this cluster.`
      : `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}. Connecting to the cluster…`;
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

function openPairingDialog(node = null) {
  if (!pairingConnectDialog || pairingConnectDialog.open) return;
  if (dashboardState.pairing.status?.role === "node" && node?.id === dashboardState.pairing.status.node_id) {
    node = null;
  }
  dashboardState.pairing.targetNode = node;
  if (pairingStatus) pairingStatus.textContent = "";
  const title = document.getElementById("pairing-title-node");
  const description = document.getElementById("pairing-dialog-description");
  if (node) {
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
    if (description) description.textContent = joiningNode ? "Enter the orchestrator HTTPS address. Nodes on the same subnet connect automatically; otherwise enter the one-use PIN shown on its Setup page." : "Enter the HTTPS address and one-use PIN displayed on the orchestrator dashboard.";
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
    dashboardState.pairing.status = status;
    if (status.role === "orchestrator") {
      if (pairingPinView) pairingPinView.hidden = false;
      if (pairingNodeConnect) pairingNodeConnect.hidden = true;
      if (pairingInstructions) pairingInstructions.textContent = "Nodes on the same subnet join this cluster automatically. Nodes on different subnets can join using a one-use PIN generated here.";
      const hasPIN = Boolean(status.pin);
      if (pairingPinRegenerateBtn) {
        pairingPinRegenerateBtn.textContent = hasPIN ? "Generate / replace pairing PIN" : "Generate pairing PIN";
      }
      if (pairingAddressDisplay) pairingAddressDisplay.textContent = status.address || "HTTPS address unavailable";
      if (pairingPin) pairingPin.textContent = status.pin || "Expired";
      if (pairingPinExpiry) {
        pairingPinExpiry.textContent = status.expires_at ? `Expires ${new Date(status.expires_at).toLocaleTimeString()}. The PIN can be used once.` : "No active PIN. Generate a new PIN to pair another node.";
      }
      if (pairingState) pairingState.textContent = hasPIN ? "Share the PIN and HTTPS address only with the node you intend to add." : "No active PIN. Generate one to add or re-pair a node.";
      return;
    }
    if (status.role === "node") {
      if (pairingPinView) pairingPinView.hidden = true;
      if (pairingNodeConnect) pairingNodeConnect.hidden = false;
      if (pairingConnectOpenBtn) pairingConnectOpenBtn.textContent = status.joined ? "Re-pair / change orchestrator" : "Enter address and pairing PIN";
      if (pairingInstructions) pairingInstructions.textContent = "Nodes on the same subnet join an orchestrator automatically. You can also connect to an orchestrator using the one-use PIN shown on its Setup page.";
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
      if (status.auto_join_available && dashboardState.pairing.dialogAutoOpened) {
        pairingConnectDialog?.close();
        dashboardState.pairing.dialogAutoOpened = false;
        dashboardState.pairing.promptShown = false;
      }
      if (!invitationPending && !status.joined && !status.auto_join_available && !dashboardState.pairing.promptShown) {
        dashboardState.pairing.promptShown = true;
        dashboardState.pairing.dialogAutoOpened = true;
        openPairingDialog();
      } else if (status.joined) {
        dashboardState.pairing.promptShown = false;
        dashboardState.pairing.dialogAutoOpened = false;
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
        body: JSON.stringify({ harness })
      });
      applyAgentBackend(backend);
      await loadAgentTools();
    }
    const sessions = sessionsForHarness(harness);
    const rememberedID = activeSessionIds.get(harness);
    const sessionID = sessions.some((session) => session.id === rememberedID) ? rememberedID : sessions[0]?.id;
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
    const invitation = Boolean(node?.discovered);
    const endpoint = invitation ? `/api/discovery/${encodeURIComponent(node.id)}/pairing/invite` : node ? `/api/nodes/${encodeURIComponent(node.id)}/pairing/connect` : "/api/pairing/connect";
    const result = await fetchJSON(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(invitation ? { pin: pairingCode.value.trim() } : { address: pairingAddress.value.trim(), pin: pairingCode.value.trim() })
    });
    pairingCode.value = "";
    pairingStatus.textContent = invitation ? `Invitation sent to ${node.id}. Confirm it on the node to complete pairing.` : node ? `${node.id} paired with this cluster.` : `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}. Connecting to the cluster…`;
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
function updateClusterTitle(count) {
  const title = document.querySelector("header h1");
  if (title) title.textContent = `mAIstr0 · ${count} ${count === 1 ? "Node" : "Nodes"}`;
}
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
  agentSupportsCoordinatorModel = Boolean(backend.supports_coordinator_model);
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
  activeSessionId = id;
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
          <h2>What would you like to orchestrate?</h2>
          <p>Ask a question or give a task to your local cluster.</p>
        </div>
      `;
    return;
  }
  for (const msg of session.messages) {
    appendMessageToChat(msg);
  }
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
}
function renderDirectMessages() {
  if (!chatMessagesEl) return;
  chatMessagesEl.innerHTML = "";
  if (!directMessages.length) {
    chatMessagesEl.innerHTML = `
      <div class="welcome-card">
        <h2>The mAIstr0 awaits your command</h2>
      </div>
    `;
    return;
  }
  for (const message of directMessages) {
    appendMessageToChat({ ...message, coordinator: "mAIstr0" });
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
  isSending = true;
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (clearDirectChatBtn) clearDirectChatBtn.disabled = true;
  agentTabs.forEach((tab) => {
    tab.disabled = true;
  });
  if (chatStatus) chatStatus.textContent = "Planning against live capabilities and routing to suitable nodes…";
  directMessages.push({ role: "user", content: description });
  appendMessageToChat({ role: "user", content: description });
  chatInput.value = "";
  try {
    const nodes = await fetchJSON("/api/nodes/all");
    if (!Array.isArray(nodes)) throw new Error("Could not load the current cluster inventory.");
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
    const message = `Task dispatch failed: ${err.message}`;
    directMessages.push({ role: "assistant", content: message });
    appendMessageToChat({ role: "assistant", coordinator: "mAIstr0", content: message });
    if (chatStatus) chatStatus.textContent = message;
  } finally {
    isSending = false;
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
  isSending = true;
  streamingAssistantEl = null;
  activeAbortController = new AbortController();
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
    isSending = false;
    activeAbortController = null;
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
      const bubble2 = document.createElement("div");
      bubble2.className = "msg-bubble";
      streamingAssistantEl.append(meta, bubble2);
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
  directMessages = [];
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

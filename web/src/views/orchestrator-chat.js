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
    dashboardDOM.appendVirtualListItem(chatMessagesEl, card);
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

  dashboardDOM.appendVirtualListItem(chatMessagesEl, msgDiv);
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
    "Live progress and results are available in the Tasks workspace.",
  ].join("\n");
}

async function dispatchDirectTask(description) {
  if (!description || isSending) return;
  isSending = true;
  if (sendMsgBtn) sendMsgBtn.disabled = true;
  if (clearDirectChatBtn) clearDirectChatBtn.disabled = true;
  agentTabs.forEach((tab) => { tab.disabled = true; });
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
      body: JSON.stringify({ description, plan_mode: "auto" }),
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
    agentTabs.forEach((tab) => { tab.disabled = false; });
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
    dashboardDOM.appendVirtualListItem(chatMessagesEl, thoughtBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "node_output" && event.output) {
    const outputBox = document.createElement("div");
    outputBox.className = "node-output-box";
    outputBox.innerHTML = `<strong>Orchestrator coordinator output · ${escapeHtml(event.node_id || "node")} · ${escapeHtml(event.model || "")}</strong><pre>${escapeHtml(event.output)}</pre>`;
    dashboardDOM.appendVirtualListItem(chatMessagesEl, outputBox);
    chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
  } else if (event.type === "tool_call" && event.tool_call) {
    if (chatStatus) chatStatus.textContent = `⚡ Spreading work: Calling tool ${event.tool_call.name}...`;
  } else if (event.type === "tool_response" && event.tool_resp) {
    appendMessageToChat({ role: "tool", tool_response: event.tool_resp });
  } else if (event.type === "final_message" && event.message) {
    if (streamingAssistantEl) {
      dashboardDOM.removeVirtualListItem(chatMessagesEl, streamingAssistantEl);
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
      dashboardDOM.appendVirtualListItem(chatMessagesEl, streamingAssistantEl);
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

promptChips.forEach((chip) => {
  chip.addEventListener("click", () => {
    const p = chip.dataset.prompt;
    if (p) {
      if (chatInput) chatInput.value = p;
      sendInteractiveMessage(p);
    }
  });
});

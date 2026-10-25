const shardSetupDialog = document.getElementById("shard-group-setup-dialog");
const shardSetupNodes = document.getElementById("shard-group-setup-nodes");
const shardSetupMode = document.getElementById("shard-group-setup-mode");
const shardSetupSource = document.getElementById("shard-group-setup-source");
const shardSetupModel = document.getElementById("shard-group-setup-model");
const shardSetupHead = document.getElementById("shard-group-setup-head");
const shardSetupContext = document.getElementById("shard-group-setup-context");
const shardSetupStatus = document.getElementById("shard-group-setup-status");
const shardSetupCreate = document.getElementById("shard-group-setup-create");
let shardSetupProfiles = new Map();
let shardSetupInventory = new Map();
let shardSetupGeneration = 0;
let shardSetupPending = false;
let shardSetupInventoryLoading = false;
let shardSetupHeadModels = new Map();

function openShardGroupWizard(nodeID) {
  if (!shardSetupDialog) return;
  const node = dashboardState.nodes.find((item) => item.id === nodeID);
  if (!node?.healthy) return;
  shardSetupProfiles = new Map();
  shardSetupInventory = new Map();
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
  const selected = reset ? new Set([selectedNodeID]) : new Set(
    [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")]
      .map((input) => input.dataset.nodeId),
  );
  const nodes = dashboardState.nodes.filter((node) => node.healthy);
  shardSetupNodes.innerHTML = nodes.length
    ? nodes.map((node) => {
      const eligibility = shardSetupNodeEligibility(
        node.id,
        shardSetupInventory,
        shardSetupMode.value,
        shardSetupInventoryLoading,
      );
      const checked = selected.has(node.id) && (eligibility.eligible || shardSetupInventoryLoading);
      const freeMemory = (node.hardware?.devices || []).filter((device) => device.kind === "gpu" || device.kind === "npu")
        .reduce((total, device) => total + Number(device.free_memory_mb || 0), 0);
      const memory = freeMemory > 0 ? `${(freeMemory / 1024).toFixed(1)} GB free device memory` : "No measured free accelerator memory; cannot receive layers";
      return `
        <label class="shard-node-option ${eligibility.eligible ? "" : "unavailable"}">
          <input type="checkbox" data-node-id="${escapeHtml(node.id)}" ${checked ? "checked" : ""} ${eligibility.eligible ? "" : "disabled"} />
          <span><strong>${escapeHtml(node.id)}</strong><small>${escapeHtml(memory)}</small><small>${escapeHtml(eligibility.reason)}</small></span>
        </label>`;
    }).join("")
    : '<p class="setup-empty">No healthy nodes are available.</p>';
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
  if (mode === "llamacpp-rpc" &&
      !engines.some((engine) => engine.kind?.toLowerCase() === "llama-server")) {
    return { eligible: true, reason: "Worker ready; a selected node also needs llama-server" };
  }
  return { eligible: true, reason: "Managed worker ready" };
}

async function loadShardSetupInventory() {
  const generation = ++shardSetupGeneration;
  const nodes = dashboardState.nodes.filter((node) => node.healthy);
  shardSetupInventoryLoading = true;
  shardSetupInventory = new Map();
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
  const nodeIDs = [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")]
    .map((input) => input.dataset.nodeId);
  const mode = shardSetupMode.value;
  const installationWarnings = [];
  shardSetupProfiles = new Map();
  shardSetupHeadModels = new Map();
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
    shardSetupStatus.textContent = mode === "vllm-mp"
      ? "Select nodes with configured vLLM workers. Automatic vLLM installation is not supported."
      : "Select workers. Missing RPC workers download and install automatically from the selected release source.";
    return;
  }

  const inventory = nodeIDs.map((nodeID) => ({
    nodeID,
    ...shardSetupInventory.get(nodeID),
  }));

  if (mode === "llamacpp-rpc") {
    shardSetupStatus.textContent = "Installing missing RPC workers and checking idle installations for updates…";
    const installations = await Promise.all(inventory.map(async (item) => {
      const worker = item.engines?.find((engine) => engine.kind?.toLowerCase() === "ggml-rpc-server");
      if (item.error || (worker && worker.id !== "maistr0-auto-rpc")) return null;
      try {
        const result = await fetchJSON(
          `/api/nodes/${encodeURIComponent(item.nodeID)}/engines/managed/install-worker`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ source: shardSetupSource.value }),
          },
        );
        if (!Array.isArray(result?.engines)) throw new Error("worker installer did not return engine inventory");
        item.engines = result.engines;
        if (Array.isArray(result.devices)) {
          const node = dashboardState.nodes.find((node) => node.id === item.nodeID);
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
  const unavailable = inventory.filter(({ nodeID, engines }) =>
    !engines?.some((engine) => engine.kind?.toLowerCase() === requiredKind),
  );
  const failedInventory = inventory.filter((item) => item.error);
  if (unavailable.length || failedInventory.length) {
    const details = [
      ...unavailable.map(({ nodeID }) => `${nodeID} has no configured ${requiredKind}`),
      ...failedInventory.map(({ nodeID, error }) => `${nodeID}: ${error}`),
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
    engine: item.engines.find((entry) => entry.kind?.toLowerCase() === "vllm"),
  }));
  if (!profileEngines.length) {
    shardSetupModel.innerHTML = '<option value="">No model head is configured</option>';
    shardSetupStatus.textContent = "No vLLM engine is configured on every selected node.";
    return;
  }

  async function loadShardSetupHeadModels(inventory, generation, warnings) {
    const previous = shardSetupHead.value;
    shardSetupHead.innerHTML = inventory.map(({ nodeID }) =>
      `<option value="${escapeHtml(nodeID)}">${escapeHtml(nodeID)}</option>`).join("");
    if (inventory.some((item) => item.nodeID === previous)) shardSetupHead.value = previous;
    shardSetupHead.disabled = false;
    const head = inventory.find((item) => item.nodeID === shardSetupHead.value);
    if (!head) return;
    const candidates = [];
    const errors = [...warnings];
    for (const engine of head.engines.filter((entry) =>
      entry.kind === "llama-server" && entry.id !== "maistr0-auto-head")) {
      try {
        const profile = await fetchJSON(`/api/nodes/${encodeURIComponent(head.nodeID)}/engines/managed/${encodeURIComponent(engine.id)}/profile?requested_context=4096`);
        if (!validShardModelProfile(profile)) throw new Error("incomplete managed model profile");
        candidates.push({ key: `engine:${engine.id}`, nodeID: head.nodeID, engineID: engine.id, profile });
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
            candidates.push({ key: `local:${model.id}`, nodeID: head.nodeID, localID: model.id, profile: model.profile });
          }
        }
      } catch (error) {
        errors.push(`${head.nodeID}: local model discovery failed: ${error.message}`);
      }
    } else if (!operatorHead && candidates.length === 0) {
      errors.push("This node has an operator-configured RPC worker. Configure a matching managed llama-server head or choose an automatically installed worker node.");
    }
    if (generation !== shardSetupGeneration) return;
    shardSetupHeadModels = new Map(candidates.map((entry) => [entry.key, entry]));
    shardSetupProfiles = new Map(candidates.map((entry) => [entry.key, entry.profile]));
    shardSetupModel.innerHTML = candidates.length
      ? candidates.map((entry) => `<option value="${escapeHtml(entry.key)}">${escapeHtml(entry.profile.name)} · ${entry.localID ? "local GGUF" : "managed head"} · ${entry.profile.layer_count} layers · ${formatBytes(entry.profile.file_size_bytes || entry.profile.weights_size_bytes)}</option>`).join("")
      : '<option value="">No compatible local model found on this head</option>';
    shardSetupModel.disabled = candidates.length === 0;
    shardSetupCreate.disabled = candidates.length === 0;
    shardSetupStatus.textContent = [
      candidates.length
        ? "Choose a model. Local Ollama/LM Studio GGUF models configure the installed llama-server automatically when creating the group."
        : "No readable GGUF model found. Choose another head node or install a GGUF model in Ollama/LM Studio on this node first.",
      ...errors,
    ].join(" ");
  }

  const profilesByEngine = await Promise.all(profileEngines.map(async ({ nodeID, engine }) => {
    try {
      const profile = await fetchJSON(
        `/api/nodes/${encodeURIComponent(nodeID)}/engines/managed/${encodeURIComponent(engine.id)}/profile?requested_context=4096`,
      );
      return { nodeID, profile };
    } catch (error) {
      return { nodeID, error: `${engine.id}: ${error.message}` };
    }
  }));
  if (generation !== shardSetupGeneration) return;

  const usable = profilesByEngine.filter((item) => item.profile && validShardModelProfile(item.profile));
  const profileErrors = profilesByEngine
    .filter((item) => item.error)
    .map((item) => `${item.nodeID}: ${item.error}`);
  if (!usable.length) {
    shardSetupModel.innerHTML = '<option value="">No readable model metadata</option>';
    shardSetupStatus.textContent = profileErrors.length
      ? profileErrors.join("; ")
      : "Configured models are missing dimensions or a measurable weight size.";
    return;
  }

  const profilesByNode = new Map(usable.map((item) => [item.nodeID, item.profile]));
  const first = profilesByNode.get(nodeIDs[0]);
  const matching = first && nodeIDs.every((nodeID) => sameShardModelProfile(first, profilesByNode.get(nodeID)));
  const candidates = matching ? [first] : [];
  shardSetupProfiles = new Map(candidates.map((profile) => [profile.name, profile]));
  const previous = shardSetupModel.value;
  shardSetupModel.innerHTML = candidates.length
    ? candidates.map((profile) => `<option value="${escapeHtml(profile.name)}">${escapeHtml(profile.name)} · ${profile.layer_count} layers · ${formatBytes(profile.weights_size_bytes || profile.file_size_bytes)}</option>`).join("")
    : '<option value="">No compatible model across selected nodes</option>';
  if (candidates.some((profile) => profile.name === previous)) shardSetupModel.value = previous;
  shardSetupModel.disabled = candidates.length === 0;
  shardSetupCreate.disabled = candidates.length === 0;
  shardSetupStatus.textContent = candidates.length
    ? [
      `${candidates.length} model${candidates.length === 1 ? "" : "s"} have readable metadata. Placement will use current free device memory and a safety margin.`,
      ...installationWarnings,
    ].join(" ")
    : [
      "The selected nodes must have the same configured vLLM model and matching profile metadata.",
      ...profileErrors,
    ].join(" ");
}

function validShardModelProfile(profile) {
  return Boolean(
    profile && typeof profile.name === "string" && profile.name.trim()
    && Number.isInteger(profile.layer_count) && profile.layer_count > 0
    && Number.isInteger(profile.hidden_size) && profile.hidden_size > 0
    && Number.isInteger(profile.kv_heads) && profile.kv_heads > 0
    && Number.isInteger(profile.head_dim) && profile.head_dim > 0
    && (Number(profile.weights_size_bytes) > 0 || Number(profile.file_size_bytes) > 0),
  );
}

function sameShardModelProfile(left, right) {
  return Boolean(right && left.name.toLowerCase() === right.name.toLowerCase()
    && left.architecture === right.architecture
    && left.parameters === right.parameters
    && left.layer_count === right.layer_count
    && left.hidden_size === right.hidden_size
    && left.kv_heads === right.kv_heads
    && left.head_dim === right.head_dim
    && left.vocab_size === right.vocab_size
    && left.quantization === right.quantization
    && (left.weights_size_bytes || left.file_size_bytes) === (right.weights_size_bytes || right.file_size_bytes));
}

async function createShardGroupFromWizard(event) {
  event.preventDefault();
  if (shardSetupPending) return;
  const nodeIDs = [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")]
    .map((input) => input.dataset.nodeId);
  const selectedProfile = shardSetupProfiles.get(shardSetupModel.value);
  const requestedContext = Number(shardSetupContext.value);
  if (!nodeIDs.length || !selectedProfile || !Number.isInteger(requestedContext)
      || requestedContext < 1 || requestedContext > 1 << 20) {
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
        body: JSON.stringify({ model_id: selectedHead.localID, requested_context: requestedContext }),
      });
      if (!head?.id) throw new Error("model head configuration did not return an engine ID");
      headEngineID = head.id;
    }
    const group = await fetchJSON("/api/shards", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model, mode, pinned_nodes: nodeIDs,
        ...(selectedHead ? { head_node_id: selectedHead.nodeID, head_engine_id: headEngineID } : {}),
      }),
    });
    if (!group || group.state !== "ready") throw new Error("the API did not confirm a ready shard group");
    dashboardState.shardGroups = [
      ...dashboardState.shardGroups.filter((existing) => existing.id !== group.id),
      group,
    ];
    dashboardState.shardGroupsError = "";
    const allocations = (group.plan?.nodes || [])
      .map((entry) => `${entry.node_id}: layers ${entry.start_layer}–${entry.end_layer}`)
      .join("; ");
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

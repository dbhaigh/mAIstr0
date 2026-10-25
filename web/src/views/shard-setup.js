const shardSetupDialog = document.getElementById("shard-group-setup-dialog");
const shardSetupNodes = document.getElementById("shard-group-setup-nodes");
const shardSetupMode = document.getElementById("shard-group-setup-mode");
const shardSetupModel = document.getElementById("shard-group-setup-model");
const shardSetupContext = document.getElementById("shard-group-setup-context");
const shardSetupStatus = document.getElementById("shard-group-setup-status");
const shardSetupCreate = document.getElementById("shard-group-setup-create");
let shardSetupProfiles = new Map();
let shardSetupGeneration = 0;
let shardSetupPending = false;

function openShardGroupWizard(nodeID) {
  if (!shardSetupDialog) return;
  const node = dashboardState.nodes.find((item) => item.id === nodeID);
  if (!node?.healthy) return;
  shardSetupProfiles = new Map();
  shardSetupPending = false;
  shardSetupMode.value = "llamacpp-rpc";
  shardSetupContext.value = "4096";
  renderShardSetupNodes(nodeID, true);
  shardSetupDialog.showModal();
  void loadShardSetupModels();
}

function renderShardSetupNodes(selectedNodeID, reset = false) {
  const selected = reset ? new Set([selectedNodeID]) : new Set(
    [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")]
      .map((input) => input.dataset.nodeId),
  );
  const nodes = dashboardState.nodes.filter((node) => node.healthy);
  shardSetupNodes.innerHTML = nodes.length
    ? nodes.map((node) => {
      const freeMemory = (node.hardware?.devices || [])
        .reduce((total, device) => total + Number(device.free_memory_mb || 0), 0);
      const memory = freeMemory > 0 ? `${(freeMemory / 1024).toFixed(1)} GB free device memory` : "Free device memory unavailable";
      return `
        <label class="shard-node-option">
          <input type="checkbox" data-node-id="${escapeHtml(node.id)}" ${selected.has(node.id) ? "checked" : ""} />
          <span><strong>${escapeHtml(node.id)}</strong><small>${escapeHtml(memory)}</small></span>
        </label>`;
    }).join("")
    : '<p class="setup-empty">No healthy nodes are available.</p>';
  shardSetupNodes.querySelectorAll("[data-node-id]").forEach((input) => {
    input.addEventListener("change", () => void loadShardSetupModels());
  });
}

async function loadShardSetupModels() {
  const generation = ++shardSetupGeneration;
  const nodeIDs = [...shardSetupDialog.querySelectorAll("[data-node-id]:checked")]
    .map((input) => input.dataset.nodeId);
  const mode = shardSetupMode.value;
  shardSetupProfiles = new Map();
  shardSetupModel.innerHTML = '<option value="">Loading compatible models…</option>';
  shardSetupModel.disabled = true;
  shardSetupCreate.disabled = true;
  shardSetupStatus.textContent = "Checking managed engines and reading model metadata…";
  if (!nodeIDs.length) {
    shardSetupModel.innerHTML = '<option value="">Select at least one node</option>';
    shardSetupStatus.textContent = "Select healthy nodes to continue.";
    return;
  }

  const inventory = await Promise.all(nodeIDs.map(async (nodeID) => {
    try {
      const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(nodeID)}/engines/managed`);
      if (!Array.isArray(engines)) throw new Error("managed-engine response was not an array");
      return { nodeID, engines };
    } catch (error) {
      return { nodeID, error: error.message, engines: [] };
    }
  }));
  if (generation !== shardSetupGeneration) return;

  const requiredKind = mode === "vllm-mp" ? "vllm" : "ggml-rpc-server";
  const unavailable = inventory.filter(({ nodeID, engines }) =>
    !engines.some((engine) => engine.kind?.toLowerCase() === requiredKind),
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

  const profileEngines = mode === "vllm-mp"
    ? inventory.map((item) => ({
      nodeID: item.nodeID,
      engine: item.engines.find((entry) => entry.kind?.toLowerCase() === "vllm"),
    }))
    : inventory.flatMap((item) => {
      const hasWorker = item.engines.some((entry) => entry.kind?.toLowerCase() === "ggml-rpc-server");
      const head = item.engines.find((entry) => entry.kind?.toLowerCase() === "llama-server");
      return hasWorker && head ? [{ nodeID: item.nodeID, engine: head }] : [];
    });
  if (!profileEngines.length) {
    shardSetupModel.innerHTML = '<option value="">No model head is configured</option>';
    shardSetupStatus.textContent = mode === "vllm-mp"
      ? "No vLLM engine is configured on every selected node."
      : "At least one selected worker must also have a managed llama-server for the model head.";
    return;
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

  let candidates;
  if (mode === "vllm-mp") {
    const profilesByNode = new Map(usable.map((item) => [item.nodeID, item.profile]));
    const first = profilesByNode.get(nodeIDs[0]);
    const matching = first && nodeIDs.every((nodeID) => sameShardModelProfile(first, profilesByNode.get(nodeID)));
    candidates = matching ? [first] : [];
  } else {
    const expectedHeadCount = inventory.filter((item) =>
      item.engines.some((entry) => entry.kind?.toLowerCase() === "llama-server"),
    ).length;
    const byName = new Map();
    for (const { profile } of usable) {
      const key = profile.name.toLowerCase();
      const entry = byName.get(key) || { profile, count: 0, consistent: true };
      entry.consistent = entry.consistent && sameShardModelProfile(entry.profile, profile);
      entry.count++;
      byName.set(key, entry);
    }
    candidates = [...byName.values()]
      .filter((entry) => entry.consistent && entry.count === expectedHeadCount)
      .map((entry) => entry.profile);
  }
  shardSetupProfiles = new Map(candidates.map((profile) => [profile.name, profile]));
  const previous = shardSetupModel.value;
  shardSetupModel.innerHTML = candidates.length
    ? candidates.map((profile) => `<option value="${escapeHtml(profile.name)}">${escapeHtml(profile.name)} · ${profile.layer_count} layers · ${formatBytes(profile.weights_size_bytes || profile.file_size_bytes)}</option>`).join("")
    : '<option value="">No compatible model across selected nodes</option>';
  if (candidates.some((profile) => profile.name === previous)) shardSetupModel.value = previous;
  shardSetupModel.disabled = candidates.length === 0;
  shardSetupCreate.disabled = candidates.length === 0;
  shardSetupStatus.textContent = candidates.length
    ? `${candidates.length} model${candidates.length === 1 ? "" : "s"} have readable metadata. Placement will use current free device memory and a safety margin.`
    : [
      mode === "vllm-mp"
        ? "The selected nodes must have the same configured vLLM model and matching profile metadata."
        : "The selected workers need compatible model-head engines with readable metadata.",
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
  shardSetupPending = true;
  shardSetupCreate.disabled = true;
  shardSetupStatus.textContent = "Planning memory-aware placement and starting managed engines…";
  try {
    const group = await fetchJSON("/api/shards", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, mode: shardSetupMode.value, pinned_nodes: nodeIDs }),
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

shardSetupMode?.addEventListener("change", () => void loadShardSetupModels());
shardSetupCreate?.addEventListener("click", createShardGroupFromWizard);
shardSetupDialog?.addEventListener("close", () => {
  shardSetupGeneration++;
  shardSetupPending = false;
});

import { fetchJSON } from "../shared/api.js";
import { escapeHtml } from "../shared/format.js";
import { dashboardState } from "../shared/state.js";

const shardGroupList = document.getElementById("shard-group-list");
const shardGroupStatus = document.getElementById("shard-group-status");
const refreshShardGroupsButton = document.getElementById("refresh-shard-groups");
const deletingShardGroups = new Set();
const shardGroupActions = new Map();
let shardMemberEditor = "";
let shardMemberSelection = null;

export function renderShardGroups(groups = dashboardState.shardGroups) {
  if (!shardGroupList) return;
  shardGroupList.innerHTML = groups.length
    ? groups.map(shardGroupCard).join("")
    : '<p class="setup-empty">No shard groups are currently configured.</p>';
  if (shardGroupStatus) {
    shardGroupStatus.textContent = dashboardState.shardGroupsError
      || `${groups.length} shard ${groups.length === 1 ? "group" : "groups"}`;
  }
}

function shardGroupCard(group) {
  const statusClass = group.state === "ready"
    ? "healthy"
    : group.state === "failed" || group.state === "degraded"
      ? "unhealthy"
      : "unknown";
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
  const rationaleMarkup = rationale.length
    ? `<ul>${rationale.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
    : '<p class="pipeline-empty">No placement rationale was recorded.</p>';
  const actions = group.state === "ready" || group.state === "degraded"
    ? ["stop", "restart"]
    : group.state === "stopped" || group.state === "failed"
      ? ["start", "restart"]
      : [];
  const busy = deletingShardGroups.has(group.id) || shardGroupActions.has(group.id);
  const actionMarkup = actions.map((action) =>
    `<button type="button" class="btn-secondary" data-shard-group="${escapeHtml(group.id)}" data-shard-action="${action}" ${busy ? "disabled" : ""}>${action[0].toUpperCase() + action.slice(1)}</button>`
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
      <input type="checkbox" data-shard-member="${escapeHtml(node.id)}" ${selected.has(node.id) ? "checked" : ""} ${node.id === head || (!node.healthy && !selected.has(node.id)) ? "disabled" : ""}>
      <span>${escapeHtml(node.id)}${node.id === head ? " (model head)" : ""}${node.healthy ? "" : " (offline)"}</span>
    </label>`).join("")}
    <button type="button" data-apply-shard-members="${escapeHtml(group.id)}">Apply and reload</button>
    <button type="button" data-cancel-shard-members="${escapeHtml(group.id)}">Cancel</button>
  </fieldset>`;
}

async function applyShardMembers(id) {
  const group = dashboardState.shardGroups.find((entry) => entry.id === id);
  if (!group || shardGroupActions.has(id) || deletingShardGroups.has(id)) return;
  const editor = [...shardGroupList.querySelectorAll("[data-shard-members-editor]")]
    .find((entry) => entry.dataset.shardMembersEditor === id);
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
            method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ source: "official" }),
          });
        }
      }
    }
    const updated = await fetchJSON(`/api/shards/${encodeURIComponent(id)}/members`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ node_ids: nodeIDs }),
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
  const allowed = group.state === "ready" || group.state === "degraded"
    ? ["stop", "restart"]
    : group.state === "stopped" || group.state === "failed"
      ? ["start", "restart"] : [];
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
    // A failed operation can still change persisted state or stop members.
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
  if (apply) { void applyShardMembers(apply.dataset.applyShardMembers); return; }
  const action = event.target.closest("[data-shard-action]");
  if (action) {
    void runShardGroupAction(action.dataset.shardGroup, action.dataset.shardAction);
    return;
  }
  const button = event.target.closest("[data-delete-shard-group]");
  if (button) void deleteFailedShardGroup(button.dataset.deleteShardGroup);
});

export async function refreshShardGroups() {
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

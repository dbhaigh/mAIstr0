const shardGroupList = document.getElementById("shard-group-list");
const shardGroupStatus = document.getElementById("shard-group-status");
const refreshShardGroupsButton = document.getElementById("refresh-shard-groups");

function renderShardGroups(groups = dashboardState.shardGroups) {
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
        <span class="badge ${member.started ? "healthy" : "unknown"}">${escapeHtml(member.engine_kind)}</span>
      </div>
      <div class="row"><span>Engine</span><span>${escapeHtml(member.engine_id)}</span></div>
      ${Number.isInteger(member.rank) ? `<div class="row"><span>Rank</span><span>${member.rank}</span></div>` : ""}
    </div>
  `).join("");
  const rationale = group.plan?.rationale?.length ? group.plan.rationale : group.rationale || [];
  const rationaleMarkup = rationale.length
    ? `<ul>${rationale.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
    : '<p class="pipeline-empty">No placement rationale was recorded.</p>';

  return `
    <article class="card pipeline-model-card">
      <h3>${escapeHtml(group.model?.name || group.id)} <span class="badge ${statusClass}">${escapeHtml(group.state)}</span></h3>
      <div class="row"><span>Group</span><span>${escapeHtml(group.id)}</span></div>
      <div class="row"><span>Mode</span><span>${escapeHtml(group.mode)}</span></div>
      ${group.error ? `<p class="pipeline-action-message">${escapeHtml(group.error)}</p>` : ""}
      <div class="pipeline-stage-list">${members || '<p class="pipeline-empty">No members are recorded.</p>'}</div>
      <strong>Plan rationale</strong>
      ${rationaleMarkup}
    </article>
  `;
}

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

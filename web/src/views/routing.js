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
  const ordered = visibleNetworkPeers(peers, dashboardState.nodes, dashboardState.pairing.status);
  if (ordered.length === 0) {
    discoveryList.innerHTML = '<p style="color:var(--muted)">No visible nodes found on the local network yet.</p>';
    return;
  }

  for (const p of ordered) {
    const card = document.createElement("div");
    card.className = "card";
    const associated = p.role === "node"
      ? (p.orchestrator_id || "Not announced")
      : p.role === "orchestrator"
        ? dashboardState.pairing.status?.role === "node"
          ? p.id === dashboardState.pairing.status.orchestrator_id ? "current orchestrator" : "available to join"
          : "self"
        : "Not elected";
    const member = p.role === "node" ? dashboardState.nodes.find((node) => node.id === p.id) : null;
    const localMember = p.id === dashboardState.pairing.status?.node_id && dashboardState.pairing.status?.joined;
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
    } else if (p.role === "orchestrator" && dashboardState.pairing.status?.role === "node" && !p.local) {
      attachNodeContextMenu(card, {
        id: p.id,
        role: p.role,
        address: p.peer_addr || p.http_addr,
        peer_addr: p.peer_addr,
        healthy: true,
        discovered: true,
        joinOrchestrator: true,
      });
    }
  }
}

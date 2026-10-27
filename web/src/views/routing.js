import { openPairingDialog } from "../components/pairing-dialog.js";
import { dashboardDOM } from "../shared/dom.js";
import { escapeHtml } from "../shared/format.js";
import { dashboardState, discoveryList, pairingStatus, routingList } from "../shared/state.js";
import { SUBTASK_STATES, knownStatus } from "./cluster.js";
import { attachNodeContextMenu } from "./nodes.js";

export function renderRouting(nodes, tasks) {
  if (!routingList) return;
  if (!nodes || nodes.length === 0) {
    dashboardDOM.patchKeyedChildren(routingList, [{ id: "__empty" }], {
      keyOf: (item) => item.id,
      signatureOf: () => "empty",
      renderItem: () => {
        const empty = document.createElement("p");
        empty.className = "task-history-empty";
        empty.textContent = "No nodes registered yet.";
        return empty;
      },
    });
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
  dashboardDOM.patchKeyedChildren(routingList, workload, {
    keyOf: (item) => item.node.id,
    signatureOf: JSON.stringify,
    renderItem: ({ node, pending, running, completed, failed, cancelled, unknown, recent }) => {
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
    return card;
    },
  });
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

export function renderDiscovery(peers, authentication = "unavailable") {
  const securityStatus = document.getElementById("discovery-security-status");
  if (securityStatus) {
    if (authentication === "hmac-sha256") {
      securityStatus.textContent = "LAN discovery authentication: HMAC-SHA256 enabled on this instance.";
    } else if (authentication === "unauthenticated") {
      securityStatus.textContent = "LAN discovery authentication: Unauthenticated (default). Configure the same discovery_hmac_secret on every cluster member to authenticate announcements.";
    } else {
      securityStatus.textContent = "LAN discovery authentication status is unavailable.";
    }
  }
  if (!discoveryList) return;
  const ordered = visibleNetworkPeers(peers, dashboardState.nodes, dashboardState.pairing.status);
  if (ordered.length === 0) {
    dashboardDOM.patchKeyedChildren(discoveryList, [{ id: "__empty" }], {
      keyOf: (item) => item.id,
      signatureOf: () => "empty",
      renderItem: () => {
        const empty = document.createElement("p");
        empty.className = "task-history-empty";
        empty.textContent = "No visible nodes found on the local network yet.";
        return empty;
      },
    });
    return;
  }

  dashboardDOM.patchKeyedChildren(discoveryList, ordered, {
    keyOf: (peer) => peer.id,
    signatureOf: (peer) => JSON.stringify({
      peer,
      pairing: dashboardState.pairing.status,
      member: dashboardState.nodes.find((node) => node.id === peer.id),
    }),
    renderItem: (p) => {
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
    const joinOrchestrator = p.role === "orchestrator" &&
      dashboardState.pairing.status?.role === "node" && !p.local;
    const orchestratorTarget = joinOrchestrator ? {
      id: p.id,
      role: p.role,
      address: p.peer_addr || "",
      peer_addr: p.peer_addr,
      healthy: true,
      discovered: true,
      joinOrchestrator: true,
    } : null;
    const alreadyJoined = dashboardState.pairing.status?.joined &&
      dashboardState.pairing.status.orchestrator_id === p.id;
    const joinLabel = alreadyJoined
      ? `Re-pair with ${p.id}`
      : dashboardState.pairing.status?.joined
        ? `Switch to ${p.id}'s cluster`
        : `Join ${p.id}'s cluster`;
    card.innerHTML = `
      <h3>${escapeHtml(p.id)} <span class="badge healthy">${escapeHtml(p.role)}</span></h3>
      <div class="row"><span>Address</span><span>${escapeHtml(p.peer_addr || p.http_addr || "Address unavailable")}</span></div>
      ${p.role === "node" ? `<div class="row"><span>Status</span><span>${member || localMember ? "Visible · cluster member" : "Visible · not a member of this cluster"}${p.local ? " · local node" : ""}</span></div>` : ""}
      <div class="row"><span>Associated orchestrator</span><span>${escapeHtml(associated)}</span></div>
      <div class="row"><span>Last seen</span><span>${p.local ? "This device" : p.last_seen ? new Date(p.last_seen).toLocaleTimeString() : "Online in cluster"}</span></div>
      ${orchestratorTarget ? `<button type="button" class="discovery-join-orchestrator" data-action="join-orchestrator" ${p.peer_addr ? "" : "disabled"}>${escapeHtml(joinLabel)}</button>` : ""}
    `;
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
    } else if (orchestratorTarget) {
      card.querySelector("[data-action=join-orchestrator]")?.addEventListener("click", () => {
        openPairingDialog(orchestratorTarget);
      });
      attachNodeContextMenu(card, orchestratorTarget);
    }
    return card;
    },
  });
}

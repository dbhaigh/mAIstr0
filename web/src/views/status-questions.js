function isClusterStatusQuestion(text) {
  const query = text.toLowerCase();
  const subject = /\b(cluster|nodes?|workers?|orchestrators?|peers?|engines?|models?|llms?|gpus?|npus?|cpus?|ram|vram|memory|hardware|capacity|telemetry|shards?|pipelines?|tasks?|queues?|versions?|leader|leadership|maistr0|network)\b/.test(query);
  const read = /^(?:(?:hi|hello|please)[,.! ]+)*(?:what|which|who|where|when|why|how|is|are|do|does|can|could|would|has|have|show|list|tell|check|inspect|describe|compare|report|status|cluster)\b/.test(query.trim()) ||
    /\b(status|health|inventory|available|online|offline|running|common|shared|same)\b/.test(query);
  const capability = /^(?:what|which|can|could|does|do|is|are)\b/.test(query.trim()) &&
    (/\b(enough|support|capable|able)\b/.test(query) || /\b(?:can|could)\s+(?:(?:the|this|my|our)\s+)?(?:cluster|nodes?|workers?|hardware|gpus?|npus?|cpus?|i)\b/.test(query));
  const mutation = /\b(install|uninstall|download|pull|delete|remove|restart|reboot|stop|start|kill|deploy|configure|change|update|upgrade|enable|disable|create|build|write|implement|schedule|dispatch|benchmark|test|fix|repair|optimize|optimise)\b/.test(query) ||
    (/\brun\b/.test(query) && !capability);
  return subject && read && !mutation;
}

async function answerClusterStatusQuestion(nodes) {
  const sources = [
    ["/api/cluster/health", "Capacity and telemetry"],
    ["/api/status", "Orchestrator status"],
    ["/api/shards", "Shard groups"],
    ["/api/tasks?page_size=50", "Recent tasks"],
    ["/api/version", "Build version"],
  ];
  const results = await Promise.allSettled(sources.map(([url]) => fetchJSON(url)));
  const data = {};
  const errors = [];
  results.forEach((result, index) => {
    const [url, label] = sources[index];
    if (result.status === "fulfilled") {
      if (url.startsWith("/api/tasks") && Array.isArray(result.value)) {
        // Unpaged (legacy) task lists are plain arrays.
        data[url] = { items: result.value.slice(0, 50), has_more: result.value.length > 50 };
      } else if (!result.value || typeof result.value !== "object" || Array.isArray(result.value)) {
        errors.push(`${label} unavailable: invalid API response`);
      } else {
        data[url] = result.value;
      }
    } else {
      errors.push(`${label} unavailable: ${result.reason.message}`);
    }
  });
  return formatClusterStatusAnswer(nodes, data, errors);
}

function formatClusterStatusAnswer(nodes, data, errors = []) {
  const health = data["/api/cluster/health"];
  const status = data["/api/status"];
  const shardList = data["/api/shards"];
  const taskPage = data["/api/tasks?page_size=50"];
  const online = nodes.filter((node) => node.healthy);
  const offline = nodes.filter((node) => !node.healthy);
  const sorted = [...nodes].sort((a, b) => a.id.localeCompare(b.id));
  const number = (value, suffix = "") => typeof value === "number" && Number.isFinite(value) ? `${value}${suffix}` : "unknown";
  // Available CPU/RAM counters omit zero values on the wire.
  const utilization = (available, value) => available ? number(value ?? 0, "%") : "unknown";
  const lines = [
    "Live cluster status (read-only; no LLM planner or task dispatch).",
    "These are reported snapshots, not fresh engine probes. Model names identify advertised models, not verified identical weights or revisions.",
    `Nodes: ${online.length} online, ${offline.length} offline, ${nodes.length} registered.`,
  ];
  const modelNames = (node) => new Set((node.models || []).map((model) => model.name).filter(Boolean));
  const inventoryUnknown = (node) => !node.healthy || (node.engines || []).some((engine) =>
    !engine.ready || !["available", "empty"].includes(dashboardViewModel.engineInventoryState(engine)));
  const intersection = (members) => {
    if (!members.length) return [];
    return [...modelNames(members[0])].filter((name) => members.every((node) => modelNames(node).has(name))).sort();
  };
  const common = intersection(online);
  if (!nodes.length) {
    lines.push("No registered nodes; common models cannot be determined.");
  } else if (nodes.some(inventoryUnknown)) {
    lines.push("Models available on ALL registered nodes: unknown; at least one node is offline or has an unavailable engine/inventory.");
  } else {
    lines.push(`Models reported on ALL ${nodes.length} registered nodes: ${common.length ? common.join(", ") : "none"}.`);
  }
  if (online.length) {
    lines.push(`Common advertised model names across ${online.length} online node(s): ${common.length ? common.join(", ") : "none"}.`);
    if (online.some(inventoryUnknown)) lines.push("Online model inventory is incomplete; common names above are not a readiness guarantee.");
    if (online.length === 1) lines.push("Only one node is online; this does not establish multi-node model availability.");
  }
  const capacity = new Map((health?.nodes || []).map((node) => [node.node_id, node]));
  for (const node of sorted) {
    const hardware = node.hardware || {};
    const load = capacity.get(node.id);
    lines.push("", `${node.id}: ${node.healthy ? "online" : "OFFLINE"}${node.leader ? ", leader" : ""}; version ${node.version || "unknown"}.`);
    lines.push(`  Address: ${node.address || "unknown"}; OS: ${hardware.os || "unknown"}/${hardware.arch || "unknown"}.`);
    lines.push(`  Last seen: ${node.last_seen || "unknown"}; telemetry: ${load?.telemetry_state || "unknown"}; sampled at ${node.telemetry_at || "unknown"}.`);
    lines.push(`  CPU: ${number(hardware.cpu_cores)} cores; utilization: ${utilization(hardware.cpu_stats_available, hardware.cpu_utilization_percent)}.`);
    lines.push(`  RAM: ${number(hardware.total_ram_mb, " MiB")}; utilization: ${utilization(hardware.memory_stats_available, hardware.memory_usage_percent)}.`);
    lines.push(`  GPU: ${hardware.has_gpu ? hardware.gpu_vendor || "detected" : "not reported"}; utilization: ${utilization(hardware.gpu_stats_available, hardware.gpu_utilization_percent)}; free memory: ${number(hardware.gpu_memory_free_mb, " MiB")}.`);
    lines.push(`  NPU: ${hardware.has_npu ? hardware.npu_vendor || "detected" : "not reported"}; utilization: ${utilization(hardware.npu_stats_available, hardware.npu_utilization_percent)}.`);
    for (const device of hardware.devices || []) {
      lines.push(`  Device ${device.kind}:${device.index} ${device.name || device.vendor || ""}: memory ${number(device.memory_mb, " MiB")}, free ${number(device.free_memory_mb, " MiB")} (omitted memory is unknown).`);
    }
    for (const link of hardware.network_links || []) {
      lines.push(`  Network ${link.name}: ${number(link.speed_mbps, " Mbps")}; trusted ${link.trusted === true ? "yes" : "not reported"}; RDMA ${link.rdma === true ? "yes" : "not reported"}.`);
    }
    lines.push(`  Tasks: ${number(node.active_tasks)} active; capacity: ${number(load?.remaining)} remaining / ${number(load?.max)} slots.`);
    lines.push(`  Default model: ${node.default_model || "not set"}.`);
    const models = [...modelNames(node)].sort();
    lines.push(`  ${inventoryUnknown(node) ? "Last reported models (availability unconfirmed)" : "Reported models"}: ${models.length ? models.join(", ") : "none reported"}.`);
    const engines = node.engines || [];
    lines.push(`  Engines: ${engines.length ? engines.map((engine) => `${engine.name}: ${engine.ready ? "ready" : "not ready"}, inventory ${dashboardViewModel.engineInventoryState(engine)}${engine.last_error ? `, error ${engine.last_error}` : ""}`).join("; ") : node.version ? "none configured or detected" : "readiness not reported (legacy node)"}.`);
    if (node.version_error) lines.push(`  Version warning: ${node.version_error}`);
  }
  if (status) {
    lines.push("", `Orchestrator: ${status.role || "unknown role"}; mode ${status.mode || "full"}; version ${data["/api/version"]?.version || "unknown"}.`);
    lines.push(`Persistence: ${status.persistence?.available === true ? "available" : status.persistence?.available === false ? "unavailable" : "not reported"}${status.persistence?.warning ? `; ${status.persistence.warning}` : ""}.`);
  }
  if (health) {
    const dispatch = health.global_dispatch || {};
    lines.push("", `Dispatch: ${number(dispatch.active_slots)} active slots / ${number(dispatch.global_limit)} limit; ${number(dispatch.queue_depth)} queued; ${number(dispatch.active_workers)} active workers.`);
    const engines = health.engine_readiness || {};
    lines.push(`Engine readiness: ${number(engines.ready_engines)} / ${number(engines.total_engines)} ready.`);
    const telemetry = health.telemetry_freshness || {};
    lines.push(`Telemetry: ${number(telemetry.fresh_nodes)} fresh, ${number(telemetry.stale_nodes)} stale, ${number(telemetry.unavailable_nodes)} unavailable.`);
    for (const warning of health.warnings || []) lines.push(`Warning: ${warning}`);
  }
  if (Array.isArray(shardList?.groups)) {
    lines.push("", `Shard groups: ${shardList.groups.length}.`);
    for (const group of shardList.groups) {
      lines.push(`  ${group.id}: ${group.model?.name || "unknown model"}, ${group.mode}, ${group.state}${group.error ? `; ${group.error}` : ""}.`);
      lines.push(`  Members: ${(group.members || []).map((member) => `${member.node_id}/${member.engine_id} (${member.role})`).join(", ") || "none"}.`);
    }
  } else if (shardList) {
    errors = [...errors, "Shard groups unavailable: invalid API response"];
  }
  if (Array.isArray(taskPage?.items)) {
    lines.push("", `Recent tasks: ${taskPage.items.length}${taskPage.has_more ? " (more history available in Tasks)" : ""}.`);
    for (const task of taskPage.items) lines.push(`  ${task.id}: ${task.status}; updated ${task.updated_at || "unknown"}.`);
  } else if (taskPage) {
    errors = [...errors, "Recent tasks unavailable: invalid API response"];
  }
  if (errors.length) lines.push("", "Incomplete status:", ...errors);
  return lines.join("\n");
}

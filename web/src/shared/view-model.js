(function (root, factory) {
  const viewModel = factory();
  if (typeof module === "object" && module.exports) module.exports = viewModel;
  root.dashboardViewModel = viewModel;
})(globalThis, () => {
  function engineInventoryState(engine) {
    const knownStates = ["unknown", "available", "empty", "error"];
    if (knownStates.includes(engine?.inventory_state)) return engine.inventory_state;
    if (!engine?.last_checked_at) return "unknown";
    if (!engine.ready) return "error";
    return Number(engine.model_count) > 0 ? "available" : "empty";
  }

  function engineInventoryLabel(state) {
    switch (state) {
      case "available": return "Inventory available";
      case "empty": return "Reachable · no models";
      case "error": return "Check failed";
      default: return "Not checked";
    }
  }

  function formatNodeHardware(node) {
    const hardware = node.hardware || {};
    const parts = [];
    if (hardware.os || hardware.arch) parts.push([hardware.os, hardware.arch].filter(Boolean).join("/"));
    if (hardware.cpu_cores) parts.push(`${hardware.cpu_cores} CPU cores`);
    if (hardware.total_ram_mb) parts.push(`${(hardware.total_ram_mb / 1024).toFixed(1)} GB RAM`);
    if (hardware.has_gpu) {
      parts.push(`${hardware.gpu_vendor || "GPU"}${hardware.gpu_stats_available ? ` · ${hardware.gpu_utilization_percent}% busy` : ""}`);
    }
    if (hardware.has_npu) {
      parts.push(`${hardware.npu_vendor || "NPU"}${hardware.npu_stats_available ? ` · ${hardware.npu_utilization_percent}% busy` : ""}`);
    }
    return parts.length ? parts.join(" · ") : "Hardware details unavailable";
  }

  function dashboardStatusViewModel(snapshot, nodes, capacity) {
    const nodeCount = snapshot.member_count ?? nodes.filter((node) => node.healthy).length;
    let modelCount = 0;
    let activeLoad = 0;
    if (Array.isArray(capacity?.nodes)) {
      for (const node of capacity.nodes) {
        if (!node.healthy) continue;
        activeLoad += Number(node.active) || 0;
      }
    } else {
      for (const node of nodes) {
        if (!node.healthy) continue;
        activeLoad += (node.active_tasks || 0);
      }
    }
    for (const node of nodes) {
      if (!node.healthy) continue;
      modelCount += (node.models || []).length;
    }
    const dispatch = capacity?.global_dispatch;
    return {
      nodeCount,
      modelCount,
      activeLoad,
      loadLabel: dispatch
        ? `Active Load: ${activeLoad} · Dispatch ${dispatch.active_slots}/${dispatch.global_limit} · Queue ${dispatch.queue_depth}`
        : `Active Load: ${activeLoad}`,
      memoryLabel: snapshot.memory
        ? `Memory: ${snapshot.memory.total_experiences || 0} · ${Math.round((snapshot.memory.success_rate || 0) * 100)}%`
        : null,
      buildVersion: snapshot.build_version || null,
    };
  }

  function taskHistoryCountLabel(visibleCount, totalCount) {
    if (totalCount === 0) return "0 tasks";
    return `Showing ${visibleCount} of ${totalCount} ${totalCount === 1 ? "task" : "tasks"}`;
  }

  function pairingPINViewModel(status) {
    const hasPIN = Boolean(status?.pin);
    return {
      visible: status?.role === "orchestrator",
      hasPIN,
      pinText: status?.pin || "Expired",
      regenerateText: hasPIN ? "Generate / replace pairing PIN" : "Generate pairing PIN",
      expiryText: status?.expires_at
        ? `Expires ${new Date(status.expires_at).toLocaleTimeString()}. The PIN can be used once.`
        : "No active PIN. Generate a new PIN to pair another node.",
      stateText: hasPIN
        ? "Share the PIN and HTTPS address only with the node you intend to add."
        : "No active PIN. Generate one to add or re-pair a node.",
    };
  }

  return Object.freeze({
    dashboardStatusViewModel,
    engineInventoryLabel,
    engineInventoryState,
    formatNodeHardware,
    pairingPINViewModel,
    taskHistoryCountLabel,
  });
});

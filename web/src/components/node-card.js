function engineStatusSummaryMarkup(engines) {
  if (!Array.isArray(engines) || !engines.length) {
    return '<span class="engine-status-empty">No engines reported</span>';
  }
  return `<div class="engine-status-summary" aria-label="Inference engine status">${engines.map((engine) => {
    const state = dashboardViewModel.engineInventoryState(engine);
    const count = Math.max(0, Number(engine.model_count) || 0);
    const countLabel = state === "error" ? `${count} known` : `${count} model${count === 1 ? "" : "s"}`;
    const title = engine.last_error
      ? `${engine.name}: ${engine.last_error}`
      : `${engine.name}: ${dashboardViewModel.engineInventoryLabel(state)}`;
    return `<span class="engine-status-chip ${state}" title="${escapeHtml(title)}">
      <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
      <span>${escapeHtml(dashboardViewModel.engineInventoryLabel(state))} · ${countLabel}</span>
    </span>`;
  }).join("")}</div>`;
}

function engineStatusDetailsMarkup(engines) {
  if (!Array.isArray(engines) || !engines.length) {
    return '<p class="engine-inventory-empty">No engine inventory status has been reported.</p>';
  }
  return `<section class="engine-inventory-details" aria-label="Inference engine inventory">
    <h4>Inference engine inventory</h4>
    ${engines.map((engine) => {
      const state = dashboardViewModel.engineInventoryState(engine);
      const checkedAt = engine.last_checked_at ? new Date(engine.last_checked_at) : null;
      const successfulAt = engine.last_successful_at ? new Date(engine.last_successful_at) : null;
      const checkedLabel = checkedAt && Number.isFinite(checkedAt.getTime()) ? checkedAt.toLocaleString() : "Not checked";
      const successfulLabel = successfulAt && Number.isFinite(successfulAt.getTime()) ? successfulAt.toLocaleString() : "No successful inventory check";
      const count = Math.max(0, Number(engine.model_count) || 0);
      const error = state === "error"
        ? `<p class="engine-inventory-error">Inventory check failed: ${escapeHtml(engine.last_error || "No error details were reported.")}</p>`
        : "";
      return `<article class="engine-inventory-row ${state}">
        <div class="engine-inventory-heading">
          <strong>${escapeHtml(engine.name || "Unknown engine")}</strong>
          <span class="engine-inventory-state">${escapeHtml(dashboardViewModel.engineInventoryLabel(state))}</span>
          <span>${count} known model${count === 1 ? "" : "s"}</span>
        </div>
        <small>Last checked: ${escapeHtml(checkedLabel)} · Last successful inventory: ${escapeHtml(successfulLabel)}</small>
        ${error}
      </article>`;
    }).join("")}
  </section>`;
}

function nodeContextDetailsMarkup(node, expanded) {
  const models = (node.models || []).map((model) => {
    const tags = Array.isArray(model.tags) && model.tags.length ? model.tags.join(", ") : "general";
    const engine = model.engine ? ` · ${escapeHtml(model.engine)}` : "";
    return `<div class="overview-node-model">
      <strong>${escapeHtml(model.name || "Unknown model")}</strong>
      <span>${escapeHtml(tags)}${engine}</span>
    </div>`;
  }).join("") || '<div class="overview-node-model"><span>No models reported</span></div>';
  const score = Number.isFinite(node.hardware?.score) ? ` · capability score ${node.hardware.score.toFixed(1)}` : "";

  return `<details class="overview-node-context"${expanded ? " open" : ""}>
    <summary>Hardware, engines &amp; models</summary>
    <div class="overview-node-context-body">
      <div>${escapeHtml(dashboardViewModel.formatNodeHardware(node))}</div>
      <div>Active tasks: ${Number(node.active_tasks) || 0}${score}</div>
      ${engineStatusSummaryMarkup(node.engines)}
      ${engineStatusDetailsMarkup(node.engines)}
      <div class="overview-node-models">${models}</div>
    </div>
  </details>`;
}

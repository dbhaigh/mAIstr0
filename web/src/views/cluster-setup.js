import { refresh } from "../app.js";
import { fetchJSON } from "../shared/api.js";
import { escapeHtml } from "../shared/format.js";
import {
  catalogEngineSelect,
  catalogEngines,
  catalogModels,
  catalogNodeSelect,
  catalogResults,
  catalogSearch,
  catalogStatus,
  modelCatalogEntries,
  setCatalogEngines,
  setCatalogModels,
  setModelCatalogEntries,
  setupCheckBtn,
  setupResults,
  setupStatus,
} from "../shared/state.js";

export async function checkSetupReadiness() {
  if (setupCheckBtn) {
    setupCheckBtn.disabled = true;
    setupCheckBtn.textContent = "Checking…";
  }
  setSetupStatus("Checking node engines and refreshing empty model registries…", "checking");
  setupResults?.setAttribute("aria-busy", "true");
  try {
    const allNodes = await fetchJSON("/api/nodes/all");
    const nodes = allNodes.filter((node) => node.healthy);
    if (!nodes.length) {
      setSetupStatus(
        allNodes.length
          ? "No cluster nodes are online. Offline nodes remain listed above."
          : "No nodes are registered. Start a node and pair it with this orchestrator first.",
        allNodes.length ? "attention" : "empty",
      );
      if (setupResults) {
        setupResults.innerHTML = '<p class="setup-empty">An online node is required to check engine and model readiness.</p>';
      }
      return;
    }
    const results = await Promise.all(nodes.map(async (node) => {
      try {
        const engines = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/engines`);
        let refreshed = false;
        if (engines.length && !(node.models || []).length) {
          await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models/refresh`, { method: "POST" });
          refreshed = true;
        }
        const models = await fetchJSON(`/api/nodes/${encodeURIComponent(node.id)}/models`);
        return { node, engines, models, refreshed, error: "" };
      } catch (err) {
        return { node, engines: [], models: [], refreshed: false, error: err.message };
      }
    }));
    let readyCount = 0;
    if (setupResults) {
      setupResults.innerHTML = results.map(({ node, engines, models, refreshed, error }) => {
        const installedCount = models.length;
        const usableCount = models.filter((model) => model.enabled).length;
        const ready = !error && engines.length > 0 && usableCount > 0;
        if (ready) readyCount += 1;
        const state = error
            ? `Could not inspect node: ${error}`
          : !engines.length
            ? "No inference engine detected. Configure an engine in node settings."
            : usableCount
              ? `${usableCount} usable model${usableCount === 1 ? "" : "s"}${installedCount > usableCount ? ` · ${installedCount - usableCount} disabled` : ""}${refreshed ? " (registry refreshed)" : ""}.`
              : installedCount
                ? "Models are installed, but all are disabled for scheduling."
                : "Engine detected, but no models are available yet.";
          const stateName = ready ? "ready" : error ? "error" : "attention";
          const badge = ready ? "Ready" : error ? "Unavailable" : "Needs setup";
          return `<article class="setup-card" data-state="${stateName}">
            <div class="setup-card-heading"><h3>${escapeHtml(node.id)}</h3><span class="setup-badge">${badge}</span></div>
            <dl class="setup-details">
              <div><dt>Engines</dt><dd>${engines.length ? engines.map((engine) => escapeHtml(engine.name)).join(", ") : "None detected"}</dd></div>
              <div><dt>Installed</dt><dd>${installedCount}</dd></div>
              <div><dt>Usable</dt><dd>${usableCount}</dd></div>
            </dl>
            ${models.length ? `<div class="catalog-tags">${models.map((model) => `${escapeHtml(model.name)} · ${escapeHtml(model.engine)} — ${model.enabled ? "usable" : "installed, disabled"}`).join("<br>")}</div>` : ""}
            <p class="setup-card-message">${escapeHtml(state)}</p>
          </article>`;
        }).join("");
    }
    setSetupStatus(
        readyCount === nodes.length
          ? `All ${nodes.length} node${nodes.length === 1 ? "" : "s"} are ready for inference.`
          : `${readyCount} of ${nodes.length} nodes are ready. Use node settings to configure engines or select models.`,
        readyCount === nodes.length ? "ready" : "attention",
    );
    await refresh();
  } catch (err) {
    setSetupStatus(`Readiness check failed: ${err.message}`, "error");
  } finally {
    if (setupCheckBtn) {
        setupCheckBtn.disabled = false;
        setupCheckBtn.textContent = "Check readiness";
    }
    setupResults?.removeAttribute("aria-busy");
  }
}

export async function loadModelCatalog() {
  if (!catalogResults) return;
  try {
    const [modelCatalogEntriesValue] = await Promise.all([
      fetchJSON("/api/models/catalog"),
      loadCatalogNodes(),
    ]);
    setModelCatalogEntries(modelCatalogEntriesValue);
    renderModelCatalog();
  } catch (err) {
    catalogStatus.textContent = `Could not load model catalog: ${err.message}`;
    catalogResults.innerHTML = "";
  }
}

async function loadCatalogNodes() {
  const nodes = await fetchJSON("/api/nodes/all");
  const online = nodes.filter((node) => node.healthy);
  const selected = catalogNodeSelect.value;
  catalogNodeSelect.innerHTML = online
    .map((node) => `<option value="${escapeHtml(node.id)}">${escapeHtml(node.id)}</option>`)
    .join("");
  if (online.some((node) => node.id === selected)) catalogNodeSelect.value = selected;
  if (!online.length) {
    catalogEngineSelect.innerHTML = "";
    setCatalogModels([]);
    catalogStatus.textContent = "An online node is required to inspect installed models or install a catalog model.";
    catalogResults.innerHTML = '<p class="setup-empty">No online target nodes.</p>';
    return;
  }
  await loadCatalogTarget();
}

export async function loadCatalogTarget() {
  const nodeId = catalogNodeSelect?.value;
  if (!nodeId) return;
  catalogStatus.textContent = `Loading engines and installed models for ${nodeId}…`;
  try {
    const [allEngines, models] = await Promise.all([
      fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/engines`),
      fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models`),
    ]);
    setCatalogEngines(allEngines.filter((item) => item.can_pull));
    setCatalogModels(models);
    const currentEngine = catalogEngineSelect.value;
    catalogEngineSelect.innerHTML = catalogEngines
      .map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)} · ${escapeHtml(item.url || "configured engine")}</option>`)
      .join("");
    if (catalogEngines.some((item) => item.name === currentEngine)) catalogEngineSelect.value = currentEngine;
    catalogStatus.textContent = catalogEngines.length
      ? `Target: ${nodeId} · ${catalogEngineSelect.value || "Select an engine"}. Installed models are marked usable or disabled for scheduling.`
      : `No configured engine on ${nodeId} supports model downloads.`;
    renderModelCatalog();
  } catch (err) {
    catalogStatus.textContent = `Could not inspect ${nodeId}: ${err.message}`;
    catalogResults.innerHTML = "";
  }
}

export function renderModelCatalog() {
  if (!catalogResults) return;
  const engineName = catalogEngineSelect?.value || "";
  const query = (catalogSearch?.value || "").trim().toLowerCase();
  const entries = modelCatalogEntries.filter((model) =>
    model.engine === engineName &&
    (!query || `${model.name} ${model.description} ${(model.tags || []).join(" ")}`.toLowerCase().includes(query))
  );
  if (!engineName) {
    catalogResults.innerHTML = '<p class="setup-empty">Choose an online node with a download-capable engine to browse matching models.</p>';
    return;
  }
  if (!entries.length) {
    catalogResults.innerHTML = '<p class="setup-empty">No catalog entries match this engine and search.</p>';
    return;
  }
  catalogResults.innerHTML = entries.map((model) => {
    const installed = catalogModels.find((item) => item.name === model.name && item.engine === model.engine);
    const state = installed ? (installed.enabled ? "Installed · usable" : "Installed · disabled for scheduling") : "Available to download";
    const button = installed
      ? `<button class="secondary-button" type="button" disabled>${installed.enabled ? "Installed" : "Installed (disabled)"}</button>`
      : `<button class="primary-btn catalog-install" type="button" data-model="${escapeHtml(model.name)}">Install on ${escapeHtml(catalogNodeSelect.value)} · ${escapeHtml(engineName)}</button>`;
    return `<article class="setup-card catalog-card">
      <div class="setup-card-heading"><h3>${escapeHtml(model.name)}</h3><span class="setup-badge">${escapeHtml(state)}</span></div>
      <p>${escapeHtml(model.description)}</p>
      <div class="catalog-tags">Engine: ${escapeHtml(model.engine)}${model.tags?.length ? ` · ${model.tags.map(escapeHtml).join(", ")}` : ""}</div>
      ${button}
    </article>`;
  }).join("");
  catalogResults.querySelectorAll(".catalog-install").forEach((button) => {
    button.addEventListener("click", () => installCatalogModel(button.dataset.model, button));
  });
}

async function installCatalogModel(model, button) {
  const nodeId = catalogNodeSelect.value;
  const engineName = catalogEngineSelect.value;
  if (!nodeId || !engineName || !model) return;
  button.disabled = true;
  button.textContent = "Installing…";
  catalogStatus.textContent = `Installing ${model} with ${engineName} on ${nodeId}…`;
  try {
    await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/pull`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, engine: engineName }),
    });
    setCatalogModels(await fetchJSON(`/api/nodes/${encodeURIComponent(nodeId)}/models/refresh`, { method: "POST" }));
    catalogStatus.textContent = `Installed ${model} on ${nodeId} using ${engineName}.`;
    renderModelCatalog();
    await refresh();
  } catch (err) {
    catalogStatus.textContent = `Could not install ${model} on ${nodeId} using ${engineName}: ${err.message}`;
    button.disabled = false;
    button.textContent = `Retry on ${nodeId} · ${engineName}`;
  }
}

function setSetupStatus(message, state) {
  if (!setupStatus) return;
  setupStatus.textContent = message;
  setupStatus.dataset.state = state;
}

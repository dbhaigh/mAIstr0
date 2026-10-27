import { fetchJSON } from "../shared/api.js";
import { escapeHtml, formatBytes } from "../shared/format.js";
import { dashboardState } from "../shared/state.js";

const pipelineModelList = document.getElementById("pipeline-model-list");
const pipelineActionStatus = document.getElementById("pipeline-action-status");
const pipelineActions = new Map();
let pipelineActionMessage = "";

export function renderPipelineDashboard(models, pipelines) {
  if (!pipelineModelList) return;
  const focusedModel = pipelineModelList.contains(document.activeElement)
    ? document.activeElement?.dataset?.reshardModel
    : "";
  const plansByModel = new Map((pipelines || []).map((pipeline) => [pipeline.model_id, pipeline]));
  const entries = models || [];

  pipelineModelList.innerHTML = entries.length
    ? entries.map((model) => pipelineModelCard(model, plansByModel.get(model.model_id))).join("")
    : '<p class="setup-empty">No models are currently advertised by the cluster.</p>';
  if (pipelineActionStatus) pipelineActionStatus.textContent = pipelineActionMessage;

  pipelineModelList.querySelectorAll("[data-reshard-model]").forEach((button) => {
    button.addEventListener("click", () => requestPipelineReshard(button.dataset.reshardModel));
  });
  if (focusedModel) {
    [...pipelineModelList.querySelectorAll("[data-reshard-model]")]
      .find((button) => button.dataset.reshardModel === focusedModel)
      ?.focus();
  }
}

function pipelineModelCard(model, pipeline) {
  const sharded = model.kind === "sharded";
  const state = pipelineActions.get(model.model_id);
  const nodes = (model.nodes || []).map(escapeHtml).join(", ");
  const status = pipeline?.status || "not_placed";
  const plan = pipeline?.plan;
  const stages = pipeline?.stages || [];
  const stageMarkup = stages.length
    ? `<div class="pipeline-stage-list">${stages.map(pipelineStageMarkup).join("")}</div>`
    : `<p class="pipeline-empty">${sharded ? "No placement yet. Reshard to place and load stages." : "Whole-model execution."}</p>`;
  const shardedDetails = sharded
    ? `
      <div class="row"><span>Pipeline</span><span class="badge ${status === "ready" ? "healthy" : status === "degraded" ? "unhealthy" : "unknown"}">${escapeHtml(status.replaceAll("_", " "))}</span></div>
      ${plan ? `<div class="row"><span>Plan version</span><span>${plan.plan_version}</span></div>` : ""}
      ${stageMarkup}
      <button type="button" class="primary-btn pipeline-reshard-button" data-reshard-model="${escapeHtml(model.model_id)}" ${state?.pending ? "disabled" : ""}>${state?.pending ? "Placing / loading…" : "Reshard / Reload stages"}</button>
    `
    : `<div class="row"><span>Available on</span><span>${nodes || "No node reported"}</span></div>`;

  return `
    <article class="card pipeline-model-card">
      <h3>${escapeHtml(model.model_id)} <span class="badge ${sharded ? "online" : "unknown"}">${sharded ? "Sharded" : "Whole"}</span></h3>
      ${shardedDetails}
      ${state?.message ? `<p class="pipeline-action-message">${escapeHtml(state.message)}</p>` : ""}
    </article>
  `;
}

function pipelineStageMarkup(stage) {
  const ready = stage.status === "ready";
  const utilization = Number.isFinite(stage.utilization_percent)
    ? `${stage.active}/${stage.capacity} slots · ${stage.utilization_percent.toFixed(0)}%`
    : `${stage.active} active · capacity unavailable`;
  const transfer = Number.isFinite(stage.activation_transfer_ms)
    ? `${stage.activation_transfer_ms.toFixed(1)} ms · ${formatBytes(stage.activation_bytes || 0)}`
    : "No activation transfer recorded";
  return `
    <div class="pipeline-stage">
      <div class="pipeline-stage-heading">
        <strong>Stage ${stage.stage + 1} · ${escapeHtml(stage.node_id)}</strong>
        <span class="badge ${ready ? "healthy" : "unhealthy"}">${escapeHtml(stage.status)}</span>
      </div>
      <div class="row"><span>Layers</span><span>${stage.start_layer}–${stage.end_layer}</span></div>
      <div class="row"><span>Engine</span><span>${escapeHtml(stage.engine)}</span></div>
      <div class="row"><span>Stage utilization</span><span>${utilization}</span></div>
      ${stage.stage > 0 ? `<div class="row"><span>Activation transfer</span><span>${transfer}</span></div>` : ""}
    </div>
  `;
}

async function requestPipelineReshard(modelID) {
  const current = pipelineActions.get(modelID) || {};
  if (current.pending) return;
  pipelineActions.set(modelID, { pending: true, message: "" });
  pipelineActionMessage = `Recomputing placement and loading stages for ${modelID}…`;
  renderPipelineDashboard(dashboardState.modelInventory, dashboardState.pipelinePlans);
  try {
    const result = await fetchJSON(`/api/pipelines/${encodeURIComponent(modelID)}/reshard`, { method: "POST" });
    const planVersion = result?.plan?.plan_version;
    const message = planVersion
      ? `Placement ${planVersion} for ${modelID} is loaded.`
      : `Placement for ${modelID} is loaded.`;
    pipelineActions.set(modelID, { pending: false, message });
    pipelineActionMessage = message;
  } catch (error) {
    const message = `Reshard failed for ${modelID}: ${error.message}`;
    pipelineActions.set(modelID, { pending: false, message });
    pipelineActionMessage = message;
  }
  renderPipelineDashboard(dashboardState.modelInventory, dashboardState.pipelinePlans);
}

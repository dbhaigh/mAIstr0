let activeNodeID = "";
let submitUpgradeRequest = null;
let activeStatus = null;

const dialog = document.getElementById("node-upgrade-dialog");
const form = document.getElementById("node-upgrade-form");
const title = document.getElementById("node-upgrade-title");
const source = document.getElementById("node-upgrade-source");
const pathField = document.getElementById("node-upgrade-path-field");
const pathInput = document.getElementById("node-upgrade-path");
const peerField = document.getElementById("node-upgrade-peer-field");
const peerInput = document.getElementById("node-upgrade-peer");
const status = document.getElementById("node-upgrade-status");
const submitButton = document.getElementById("node-upgrade-submit");

function isUpgradeActive(value) {
  return ["queued", "starting", "running", "restarting", "rejoining"].includes(value?.state);
}

function updateSourceFields() {
  const peerSource = source?.value === "peer";
  if (pathField) pathField.hidden = peerSource;
  if (peerField) peerField.hidden = !peerSource;
  if (pathInput) pathInput.required = !peerSource;
  if (peerInput) peerInput.required = peerSource;
}

function statusText(value) {
  if (!value) return "Choose a local executable path or a trusted peer source.";
  const state = String(value.state || "unknown").replaceAll("_", " ");
  const phase = String(value.phase || "").replaceAll("_", " ");
  const progress = Math.max(0, Math.min(100, Number(value.progress) || 0));
  const ticket = String(value.ticket_status || "not requested").replaceAll("_", " ");
  const reason = value.failure_reason ? ` Failure: ${value.failure_reason}` : "";
  return `Upgrade ${state}${phase ? ` · ${phase}` : ""} · ${progress}% · Rejoin ticket: ${ticket}.${reason}`;
}

export function openUpgradeDialog(node, submit) {
  if (!dialog || !form || !source || !pathInput || !peerInput) {
    throw new Error("Upgrade dialog is unavailable.");
  }
  activeNodeID = node.id;
  submitUpgradeRequest = submit;
  title.textContent = `Upgrade ${node.id}`;
  source.value = "path";
  pathInput.value = "";
  peerInput.value = "";
  updateSourceFields();
  updateUpgradeDialogStatus(node.id, node.upgrade_status);
  dialog.showModal();
  pathInput.focus();
}

export function updateUpgradeDialogStatus(nodeID, value) {
  if (nodeID !== activeNodeID || !status) return;
  activeStatus = value || null;
  status.textContent = statusText(value);
  if (submitButton) submitButton.disabled = isUpgradeActive(value);
}

source?.addEventListener("change", updateSourceFields);
document.getElementById("node-upgrade-cancel")?.addEventListener("click", () => dialog?.close());
form?.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!activeNodeID || !submitUpgradeRequest) return;
  const value = source?.value === "peer" ? peerInput?.value.trim() : pathInput?.value.trim();
  if (!value) {
    if (status) status.textContent = source?.value === "peer"
      ? "Enter the ID of a trusted peer."
      : "Enter an absolute path on the target node.";
    return;
  }
  const request = source?.value === "peer" ? { peer_id: value } : { path: value };
  if (submitButton) submitButton.disabled = true;
  if (status) status.textContent = "Submitting upgrade request…";
  try {
    await submitUpgradeRequest(activeNodeID, request);
    if (status?.textContent === "Submitting upgrade request…") {
      status.textContent = "Upgrade request accepted; waiting for node status.";
    }
  } catch (error) {
    if (status) status.textContent = `Unable to start upgrade: ${error?.message || error}`;
  } finally {
    if (submitButton) submitButton.disabled = isUpgradeActive(activeStatus);
  }
});

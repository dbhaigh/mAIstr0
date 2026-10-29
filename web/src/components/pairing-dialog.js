import { refresh } from "../app.js";
import { fetchJSON } from "../shared/api.js";
import { dashboardState, pairingAddress, pairingAddressDisplay, pairingAddressField, pairingCode, pairingConnectButton, pairingConnectDialog, pairingConnectOpenBtn, pairingFingerprint, pairingFingerprintDisplay, pairingInstructions, pairingInvitationAcceptBtn, pairingInvitationDeclineBtn, pairingInvitationDetails, pairingInvitationDialog, pairingInvitationFingerprint, pairingInvitationStatus, pairingNodeConnect, pairingPin, pairingPinExpiry, pairingPinRegenerateBtn, pairingPinView, pairingPolicyState, pairingRequireVerification, pairingState, pairingStatus } from "../shared/state.js";
import { dashboardViewModel } from "../shared/view-model.js";

let generatedPairingPIN = null;
let pairingPolicyPending = false;

function pairingStatusWithGeneratedPIN(status) {
  if (status.pin) {
    generatedPairingPIN = { pin: status.pin, expires_at: status.expires_at };
    return status;
  }
  if (!generatedPairingPIN) return status;
  if (!status.expires_at || status.expires_at !== generatedPairingPIN.expires_at ||
      Date.parse(status.expires_at) <= Date.now()) {
    generatedPairingPIN = null;
    return status;
  }
  return { ...status, pin: generatedPairingPIN.pin };
}

export function openPairingDialog(node = null) {
  if (!pairingConnectDialog || pairingConnectDialog.open) return;
  if (dashboardState.pairing.status?.role === "node" && node?.id === dashboardState.pairing.status.node_id) {
    node = null;
  }
  dashboardState.pairing.targetNode = node;
  if (pairingFingerprint) pairingFingerprint.value = node && !node.joinOrchestrator && !node.discovered
    ? dashboardState.pairing.status?.fingerprint || "" : "";
  if (pairingStatus) pairingStatus.textContent = "";
  const title = document.getElementById("pairing-title-node");
  const description = document.getElementById("pairing-dialog-description");
  if (node?.joinOrchestrator) {
    const pairingStatus = dashboardState.pairing.status;
    const alreadyPairedWithTarget = pairingStatus?.joined && pairingStatus.orchestrator_id === node.id;
    if (title) title.textContent = alreadyPairedWithTarget
      ? `Re-pair with ${node.id}`
      : pairingStatus?.joined
        ? `Switch to ${node.id}'s cluster`
        : `Join ${node.id}'s cluster`;
    if (description) description.textContent = `Connect securely to ${node.id} at ${node.peer_addr || node.address}. Discovery does not authenticate its certificate. With strict verification off, same-subnet pairing skips the fingerprint and PIN; otherwise verify the fingerprint out of band and enter the PIN.`;
    if (pairingAddressField) pairingAddressField.hidden = false;
    if (pairingAddress) {
      pairingAddress.required = true;
      pairingAddress.value = node.peer_addr || node.address || "";
    }
    if (pairingCode) pairingCode.value = "";
    if (pairingConnectButton) pairingConnectButton.textContent = alreadyPairedWithTarget
      ? `Re-pair with ${node.id}`
      : pairingStatus?.joined
        ? `Switch to ${node.id}`
        : "Join cluster";
  } else if (node) {
    const invitation = Boolean(node.discovered);
    if (title) title.textContent = invitation
      ? `Pair ${node.id} with this cluster`
      : `Re-pair ${node.id} with this cluster`;
    if (description) description.textContent = invitation
      ? `Send a pairing invitation to ${node.address}. The node must confirm locally before it joins.`
      : "Enter this cluster's HTTPS address and an active one-use PIN. The selected node must be online and already trusted by this cluster.";
    if (pairingAddressField) pairingAddressField.hidden = invitation;
    if (pairingAddress) pairingAddress.required = !invitation;
    if (pairingAddress) pairingAddress.value = dashboardState.pairing.status?.address || dashboardState.pairing.status?.orchestrator_addr || "";
    if (pairingCode) pairingCode.value = dashboardState.pairing.status?.role === "orchestrator" ? dashboardState.pairing.status.pin || "" : "";
    if (pairingConnectButton) pairingConnectButton.textContent = invitation ? "Send invitation" : "Re-pair node";
  } else {
    if (pairingAddressField) pairingAddressField.hidden = false;
    if (pairingAddress) pairingAddress.required = true;
    const joiningNode = dashboardState.pairing.status?.role === "node";
    if (title) title.textContent = joiningNode
      ? dashboardState.pairing.status.joined ? "Re-pair node" : "Join cluster"
      : "Connect to orchestrator";
    if (description) description.textContent = joiningNode
      ? "Enter the orchestrator HTTPS address, its out-of-band verified certificate fingerprint, and the one-use PIN shown on its local Setup page."
      : "Enter the HTTPS address and one-use PIN displayed on the orchestrator dashboard.";
    if (joiningNode) {
      pairingAddress.value = dashboardState.pairing.status.orchestrator_addr || pairingAddress.value;
      pairingCode.value = "";
    }
    if (pairingConnectButton) pairingConnectButton.textContent = joiningNode
      ? dashboardState.pairing.status.joined ? "Re-pair node" : "Join cluster"
      : "Connect node";
  }
  pairingConnectDialog.showModal();
  pairingAddress?.focus();
}

pairingPinRegenerateBtn?.addEventListener("click", async () => {
  pairingPinRegenerateBtn.disabled = true;
  pairingPinRegenerateBtn.textContent = "Generating PIN…";
  if (pairingState) pairingState.textContent = "Generating a new one-use PIN…";
  try {
    const generated = await fetchJSON("/api/pairing/pin", { method: "POST" });
    if (!generated.pin || !generated.expires_at) throw new Error("The server did not return an active PIN.");
    generatedPairingPIN = { pin: generated.pin, expires_at: generated.expires_at };
    if (pairingPin) pairingPin.textContent = generated.pin;
    if (pairingPinExpiry) {
      pairingPinExpiry.textContent = `Expires ${new Date(generated.expires_at).toLocaleTimeString()}. The PIN can be used once.`;
    }
    await loadPairingStatus();
    if (pairingState) pairingState.textContent = "New PIN generated. Enter it only on the node you intend to pair.";
  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not generate a new PIN: ${err.message}`;
  } finally {
    pairingPinRegenerateBtn.disabled = false;
    pairingPinRegenerateBtn.textContent = "Generate / replace pairing PIN";
  }
});

pairingRequireVerification?.addEventListener("change", async () => {
  const required = pairingRequireVerification.checked;
  pairingPolicyPending = true;
  pairingRequireVerification.disabled = true;
  if (pairingPolicyState) pairingPolicyState.textContent = "Saving cluster-wide pairing policy…";
  try {
    const saved = await fetchJSON("/api/pairing/policy", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ require_pairing_verification: required }),
    });
    if (typeof saved.require_pairing_verification !== "boolean") {
      throw new Error("The server did not confirm the pairing policy.");
    }
    dashboardState.pairing.status = {
      ...dashboardState.pairing.status,
      require_pairing_verification: saved.require_pairing_verification,
    };
    pairingRequireVerification.checked = saved.require_pairing_verification;
    if (pairingPolicyState) {
      pairingPolicyState.textContent = saved.require_pairing_verification
        ? "Strict mode is on. New joins and re-pairs require both checks; existing trust is unchanged."
        : "Strict mode is off. Nodes on a connected local subnet may auto-join; existing trust is unchanged.";
    }
  } catch (err) {
    pairingRequireVerification.checked = !required;
    if (pairingPolicyState) pairingPolicyState.textContent = `Could not save pairing policy: ${err.message}`;
  } finally {
    pairingPolicyPending = false;
    pairingRequireVerification.disabled = false;
  }
});

export async function loadPairingStatus() {
  try {
    const status = pairingStatusWithGeneratedPIN(await fetchJSON("/api/pairing/status"));
    if (status.dashboard_url) {
      window.location.replace(status.dashboard_url);
      return;
    }
    dashboardState.pairing.status = status;
    if (pairingFingerprintDisplay) pairingFingerprintDisplay.textContent = status.fingerprint || "Unavailable";
    if (status.role === "orchestrator") {
      const pinViewModel = dashboardViewModel.pairingPINViewModel(status);
      if (pairingPinView) pairingPinView.hidden = !pinViewModel.visible;
      if (pairingNodeConnect) pairingNodeConnect.hidden = true;
      if (pairingInstructions) pairingInstructions.textContent = "Strict mode requires an out-of-band verified fingerprint and one-use PIN. With strict mode off, nodes on a connected local subnet can auto-join.";
      if (pairingRequireVerification && !pairingPolicyPending) {
        pairingRequireVerification.checked = status.require_pairing_verification === true;
      }
      if (pairingPolicyState && !pairingPolicyPending) {
        pairingPolicyState.textContent = status.require_pairing_verification
          ? "Strict mode is on. New joins and re-pairs require both checks; existing trust is unchanged."
          : "Strict mode is off. Nodes on a connected local subnet may auto-join; existing trust is unchanged.";
      }
      if (pairingPinRegenerateBtn) {
        pairingPinRegenerateBtn.textContent = pinViewModel.regenerateText;
      }
      if (pairingAddressDisplay) pairingAddressDisplay.textContent = status.address || "HTTPS address unavailable";
      if (pairingPin) pairingPin.textContent = pinViewModel.pinText;
      if (pairingPinExpiry) pairingPinExpiry.textContent = pinViewModel.expiryText;
      if (pairingState) pairingState.textContent = pinViewModel.stateText;
      return;
    }
    if (status.role === "node") {
      if (pairingPinView) pairingPinView.hidden = true;
      if (pairingNodeConnect) pairingNodeConnect.hidden = false;
      if (pairingConnectOpenBtn) pairingConnectOpenBtn.textContent = status.joined
        ? "Re-pair / change orchestrator"
        : "Enter address and pairing PIN";
      if (pairingInstructions) pairingInstructions.textContent = status.auto_join_available
        ? "This orchestrator is on a connected local subnet. If cluster strict mode is off, the node joins automatically without an out-of-band fingerprint or PIN."
        : status.require_pairing_verification
          ? "Cluster strict mode is on. New joins and re-pairs require the orchestrator's verified certificate fingerprint and one-use PIN."
          : "For non-local connections, verify the orchestrator's certificate fingerprint out of band and enter its one-use PIN. Discovery alone does not establish trust.";
      if (status.orchestrator_addr && pairingAddress && !pairingAddress.value) {
        pairingAddress.value = status.orchestrator_addr;
      }
      if (pairingState) {
        pairingState.textContent = status.joined
          ? `Connected to ${status.orchestrator_addr || "the orchestrator"}.`
          : status.auto_join_available
            ? `Connecting automatically to ${status.orchestrator_addr || "a discovered cluster"} on this subnet.`
          : status.trusted
            ? "Pairing succeeded. Waiting for the node to register with the orchestrator."
            : "Not paired. Enter the orchestrator HTTPS address and its one-use PIN.";
      }
      if (!status.joined && !dashboardState.pairing.setupFocused) {
        dashboardState.pairing.setupFocused = true;
        document.querySelector('.nav-tab[data-tab="cluster-tab"]')?.click();
        document.querySelector('[data-window="setup-panel"]')?.click();
        document.getElementById("setup-pairing-tab")?.click();
      }
      const invitationPending = await checkPairingInvitation();
      if ((status.auto_join_available || status.joined || status.trusted) && dashboardState.pairing.dialogAutoOpened) {
        pairingConnectDialog?.close();
        dashboardState.pairing.dialogAutoOpened = false;
        dashboardState.pairing.promptShown = false;
      }
      if (!invitationPending && !status.joined && !status.trusted && status.pairing_required !== false &&
          !status.auto_join_available && !dashboardState.pairing.promptShown) {
        dashboardState.pairing.promptShown = true;
        dashboardState.pairing.dialogAutoOpened = true;
        openPairingDialog();
      } else if (status.joined) {
        dashboardState.pairing.promptShown = false;
        dashboardState.pairing.dialogAutoOpened = false;
      }
    }

  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not load pairing status: ${err.message}`;
    if (pairingNodeConnect) pairingNodeConnect.hidden = false;
    if (pairingConnectOpenBtn) pairingConnectOpenBtn.hidden = false;
  }
}

    async function checkPairingInvitation() {
      const invitation = await fetchJSON("/api/pairing/invitation");
      if (!invitation.pending) {
        if (pairingInvitationDialog?.open) pairingInvitationDialog.close();
        return false;
      }
      pairingConnectDialog?.close();
      if (pairingInvitationDetails) {
        pairingInvitationDetails.textContent = `${invitation.orchestrator_id} at ${invitation.address} wants to add this node to its cluster. Verify SHA-256 ${invitation.fingerprint} out of band before accepting. The invitation expires ${new Date(invitation.expires_at).toLocaleTimeString()}.`;
      }
      if (pairingInvitationDialog && !pairingInvitationDialog.open) {
        if (pairingInvitationFingerprint) pairingInvitationFingerprint.value = "";
        pairingInvitationDialog.showModal();
      }
      return true;
    }

    async function respondToPairingInvitation(accept) {
      if (!pairingInvitationAcceptBtn || !pairingInvitationDeclineBtn) return;
      pairingInvitationAcceptBtn.disabled = true;
      pairingInvitationDeclineBtn.disabled = true;
      if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept ? "Connecting to the cluster securely…" : "Declining invitation…";
      try {
        const fingerprint = pairingInvitationFingerprint?.value.trim() || "";
        if (accept && !/^[a-fA-F0-9]{64}$/.test(fingerprint)) throw new Error("Enter the orchestrator fingerprint verified out of band.");
        const result = await fetchJSON(`/api/pairing/invitation/${accept ? "accept" : "reject"}`, {
          method: "POST",
          ...(accept ? { headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fingerprint }) } : {}),
        });
        dashboardState.pairing.promptShown = true;
        pairingInvitationDialog?.close();
        if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept
          ? `Paired with ${result.orchestrator_id} at ${result.orchestrator_addr}.`
          : "Invitation declined.";
        await loadPairingStatus();
        await refresh();
      } catch (err) {
        if (pairingInvitationStatus) pairingInvitationStatus.textContent = `${accept ? "Could not join cluster" : "Could not decline invitation"}: ${err.message}`;
      } finally {
        pairingInvitationAcceptBtn.disabled = false;
        pairingInvitationDeclineBtn.disabled = false;
      }
    }

    pairingInvitationAcceptBtn?.addEventListener("click", () => respondToPairingInvitation(true));
    pairingInvitationDeclineBtn?.addEventListener("click", () => respondToPairingInvitation(false));
    pairingInvitationDialog?.addEventListener("cancel", (event) => event.preventDefault());

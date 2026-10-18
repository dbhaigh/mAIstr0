function openPairingDialog(node = null) {
  if (!pairingConnectDialog || pairingConnectDialog.open) return;
  if (dashboardState.pairing.status?.role === "node" && node?.id === dashboardState.pairing.status.node_id) {
    node = null;
  }
  dashboardState.pairing.targetNode = node;
  if (pairingStatus) pairingStatus.textContent = "";
  const title = document.getElementById("pairing-title-node");
  const description = document.getElementById("pairing-dialog-description");
  if (node) {
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
      ? "Enter the orchestrator HTTPS address. Nodes on the same subnet connect automatically; otherwise enter the one-use PIN shown on its Setup page."
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
    await fetchJSON("/api/pairing/pin", { method: "POST" });
    await loadPairingStatus();
    if (pairingState) pairingState.textContent = "New PIN generated. Enter it only on the node you intend to pair.";
  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not generate a new PIN: ${err.message}`;
  } finally {
    pairingPinRegenerateBtn.disabled = false;
    pairingPinRegenerateBtn.textContent = "Generate / replace pairing PIN";
  }
});

async function loadPairingStatus() {
  try {
    const status = await fetchJSON("/api/pairing/status");
    if (status.dashboard_url) {
      window.location.replace(status.dashboard_url);
      return;
    }
    dashboardState.pairing.status = status;
    if (status.role === "orchestrator") {
      const pinViewModel = dashboardViewModel.pairingPINViewModel(status);
      if (pairingPinView) pairingPinView.hidden = !pinViewModel.visible;
      if (pairingNodeConnect) pairingNodeConnect.hidden = true;
      if (pairingInstructions) pairingInstructions.textContent = "Nodes on the same subnet join this cluster automatically. Nodes on different subnets can join using a one-use PIN generated here.";
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
      if (pairingInstructions) pairingInstructions.textContent = "Nodes on the same subnet join an orchestrator automatically. You can also connect to an orchestrator using the one-use PIN shown on its Setup page.";
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
      if (status.auto_join_available && dashboardState.pairing.dialogAutoOpened) {
        pairingConnectDialog?.close();
        dashboardState.pairing.dialogAutoOpened = false;
        dashboardState.pairing.promptShown = false;
      }
      if (!invitationPending && !status.joined && !status.auto_join_available && !dashboardState.pairing.promptShown) {
        dashboardState.pairing.promptShown = true;
        dashboardState.pairing.dialogAutoOpened = true;
        openPairingDialog();
      } else if (status.joined) {
        dashboardState.pairing.promptShown = false;
        dashboardState.pairing.dialogAutoOpened = false;
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
        pairingInvitationDetails.textContent = `${invitation.orchestrator_id} at ${invitation.address} wants to add this node to its cluster. The invitation expires ${new Date(invitation.expires_at).toLocaleTimeString()}.`;
      }
      if (pairingInvitationDialog && !pairingInvitationDialog.open) pairingInvitationDialog.showModal();
      return true;
    }

    async function respondToPairingInvitation(accept) {
      if (!pairingInvitationAcceptBtn || !pairingInvitationDeclineBtn) return;
      pairingInvitationAcceptBtn.disabled = true;
      pairingInvitationDeclineBtn.disabled = true;
      if (pairingInvitationStatus) pairingInvitationStatus.textContent = accept ? "Connecting to the cluster securely…" : "Declining invitation…";
      try {
        const result = await fetchJSON(`/api/pairing/invitation/${accept ? "accept" : "reject"}`, { method: "POST" });
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

  } catch (err) {
    if (pairingState) pairingState.textContent = `Could not load pairing status: ${err.message}`;
    if (pairingNodeConnect) pairingNodeConnect.hidden = false;
    if (pairingConnectOpenBtn) pairingConnectOpenBtn.hidden = false;
  }
}

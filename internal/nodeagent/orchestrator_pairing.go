package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"net/http"
	"strings"
	"time"
)

func (c *orchestratorLink) pairLocal(orchestratorID, certificatePEM, address string) error {
	if !c.trust.Initialized() {
		return errors.New("node identity is not initialized")
	}
	if err := c.trust.TrustPeerDetails(orchestratorID, certificatePEM, "orchestrator", address); err != nil {
		return err
	}
	c.setLocalOrchestratorID(orchestratorID)
	c.setOrchestratorAddr(address)
	c.setOrchestratorID(orchestratorID)
	return nil
}

func (c *orchestratorLink) pairOrchestrator(candidate orchestratorCandidate) error {
	return c.pairOrchestratorContext(context.Background(), candidate)
}

func (c *orchestratorLink) pairOrchestratorContext(ctx context.Context, candidate orchestratorCandidate) error {
	return c.pairWithAddressContext(ctx, candidate.address, candidate.id, c.configState.configSnapshot().PairingPIN)
}

func (c *orchestratorLink) pairWithOrchestrator(address, pin string) error {
	address, err := normalizeOrchestratorAddress(address)
	if err != nil {
		return err
	}
	return c.pairWithAddress(address, "", strings.TrimSpace(pin))
}

func (c *orchestratorLink) pairWithDiscoveredOrchestrator(id, address, pin string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("selected orchestrator identity is required")
	}
	address, err := normalizeOrchestratorAddress(address)
	if err != nil {
		return err
	}
	for _, candidate := range c.orchestratorCandidates() {
		if candidate.id != id {
			continue
		}
		candidateAddress, err := normalizeOrchestratorAddress(candidate.address)
		if err == nil && candidateAddress == address {
			return c.pairWithAddress(address, id, strings.TrimSpace(pin))
		}
	}
	return errSelectedOrchestratorNotDiscovered
}

func (c *orchestratorLink) pairWithAddress(address, expectedID, pin string) error {
	return c.pairWithAddressContext(context.Background(), address, expectedID, pin)
}

func (c *orchestratorLink) pairWithAddressContext(ctx context.Context, address, expectedID, pin string) (resultErr error) {
	defer func() {
		if resultErr != nil {
			logging.Errorf("nodeagent", "nodeagent: pairing with orchestrator at %s failed: %v", address, resultErr)
		}
	}()
	if !c.trust.Initialized() {
		return errors.New("node identity is not initialized")
	}
	if pin == "" {
		logging.Infof("nodeagent", "nodeagent: attempting auto-join with orchestrator at %s", address)
	} else {
		logging.Infof("nodeagent", "nodeagent: starting pairing with orchestrator at %s", address)
	}
	cfg := c.configState.configSnapshot()
	body, err := json.Marshal(nodePairingRequest{
		ID: cfg.NodeID, Role: "node", PeerAddr: cfg.AdvertiseAddr, PIN: pin,
		CertificatePEM: c.trust.CertificatePEM(),
	})
	if err != nil {
		return fmt.Errorf("encode pairing request: %w", err)
	}
	transport := c.trust.BootstrapTransport()
	if expectedID != "" {
		transport = c.trust.BootstrapTransportForID(expectedID)
	}
	defer transport.CloseIdleConnections()
	client := httpx.NewClient(httpx.ClientOptions{
		Timeout:   httpx.ProbeTimeout,
		Transport: transport,
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/api/pairing/join", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("prepare pairing request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("pair with orchestrator at %s: %w", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("pairing rejected by orchestrator at %s with HTTP status %d", address, resp.StatusCode)
	}
	var result nodePairingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode pairing response: %w", err)
	}
	if result.Role != "orchestrator" || result.ID == "" || (expectedID != "" && result.ID != expectedID) {
		return fmt.Errorf("pairing server identity %q does not match discovered orchestrator %q", result.ID, expectedID)
	}
	if err := c.trust.TrustPeerDetails(result.ID, result.CertificatePEM, "orchestrator", address); err != nil {
		return err
	}
	for id, certificate := range result.Peers {
		if id == cfg.NodeID {
			continue
		}
		if err := c.trust.TrustPeer(id, certificate); err != nil {
			return fmt.Errorf("trust paired peer %q: %w", id, err)
		}
	}
	c.configState.clearPairingPIN()
	c.setOrchestratorAddr(address)
	c.setOrchestratorID(result.ID)
	logging.Infof("nodeagent", "nodeagent: successfully paired with orchestrator %s at %s", result.ID, address)
	return nil
}

func (c *orchestratorLink) storePairingInvitation(invitation *pairingInvitation, now time.Time) (*pairingInvitation, error) {
	c.pairingInviteMu.Lock()
	defer c.pairingInviteMu.Unlock()
	if c.pairingAccepting {
		return nil, errPairingInvitationAccepting
	}
	if current := c.pairingInvite; current != nil && current.ExpiresAt.After(now) {
		if current.OrchestratorID != invitation.OrchestratorID || current.Address != invitation.Address || current.PIN != invitation.PIN {
			return nil, errPairingInvitationWaiting
		}
		return current, nil
	}
	c.pairingInvite = invitation
	return invitation, nil
}

func (c *orchestratorLink) pairingInvitationStatus(now time.Time) *pairingInvitation {
	c.pairingInviteMu.Lock()
	defer c.pairingInviteMu.Unlock()
	if c.pairingInvite == nil || !c.pairingInvite.ExpiresAt.After(now) {
		c.pairingInvite = nil
		return nil
	}
	invitation := *c.pairingInvite
	return &invitation
}

func (c *orchestratorLink) beginPairingInvitationAcceptance(now time.Time) (*pairingInvitation, error) {
	c.pairingInviteMu.Lock()
	defer c.pairingInviteMu.Unlock()
	if c.pairingAccepting {
		return nil, errPairingInvitationAlreadyAccepting
	}
	if c.pairingInvite == nil || !c.pairingInvite.ExpiresAt.After(now) {
		c.pairingInvite = nil
		return nil, errPairingInvitationExpired
	}
	c.pairingAccepting = true
	return c.pairingInvite, nil
}

func (c *orchestratorLink) finishPairingInvitationAcceptance(invitation *pairingInvitation, err error) {
	c.pairingInviteMu.Lock()
	c.pairingAccepting = false
	if err == nil && c.pairingInvite == invitation {
		c.pairingInvite = nil
	}
	c.pairingInviteMu.Unlock()
}

func (c *orchestratorLink) rejectPairingInvitation() error {
	c.pairingInviteMu.Lock()
	defer c.pairingInviteMu.Unlock()
	if c.pairingAccepting {
		return errPairingInvitationRejecting
	}
	c.pairingInvite = nil
	return nil
}

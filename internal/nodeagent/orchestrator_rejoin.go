package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

const maxAcceptedRejoinTicketTTL = 10 * time.Minute

type rejoinTicketResponse struct {
	NodeID         string    `json:"node_id"`
	OrchestratorID string    `json:"orchestrator_id"`
	Ticket         string    `json:"ticket"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type nodeRejoinRequest struct {
	ID             string `json:"id"`
	PeerAddr       string `json:"peer_addr,omitempty"`
	CertificatePEM string `json:"certificate_pem"`
	Ticket         string `json:"ticket"`
}

func (c *orchestratorLink) prepareRejoinTicket(ctx context.Context) error {
	orchestratorID, address := c.getOrchestratorID(), c.getOrchestratorAddr()
	if orchestratorID == "" || address == "" || !c.trust.IsTrustedOrchestratorID(orchestratorID) {
		return errors.New("current paired orchestrator is unavailable for upgrade rejoin")
	}
	if !strings.HasPrefix(strings.ToLower(address), "https://") {
		return errors.New("upgrade rejoin requires the current orchestrator HTTPS address")
	}
	transport := c.trust.BootstrapTransportForID(orchestratorID)
	defer transport.CloseIdleConnections()
	client := httpx.NewClient(httpx.ClientOptions{
		Timeout:   httpx.ProbeTimeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("rejoin ticket redirects are not permitted")
		},
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/api/pairing/rejoin-ticket", nil)
	if err != nil {
		return fmt.Errorf("prepare rejoin ticket request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request upgrade rejoin ticket: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("rejoin ticket request was rejected with HTTP status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, httpx.ControlJSONBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read rejoin ticket response: %w", err)
	}
	if int64(len(data)) > httpx.ControlJSONBodyBytes {
		return errors.New("rejoin ticket response exceeds the control-body limit")
	}
	var result rejoinTicketResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("decode rejoin ticket response: %w", err)
	}
	now := time.Now()
	if result.NodeID != c.configState.configSnapshot().NodeID ||
		result.OrchestratorID != orchestratorID ||
		!result.ExpiresAt.After(now) ||
		result.ExpiresAt.After(now.Add(maxAcceptedRejoinTicketTTL)) {
		return errors.New("orchestrator returned a rejoin ticket with invalid identity or expiry")
	}
	ticket := peeridentity.PendingRejoinTicket{
		NodeID: result.NodeID, OrchestratorID: result.OrchestratorID,
		Token: result.Ticket, ExpiresAt: result.ExpiresAt,
	}
	previousDeferred := c.rejoinTicketDeferred.Swap(true)
	if err := c.trust.StorePendingRejoinTicket(ticket); err != nil {
		c.rejoinTicketDeferred.Store(previousDeferred)
		return fmt.Errorf("cache upgrade rejoin ticket: %w", err)
	}
	logging.Infof("nodeagent", "nodeagent: cached a short-lived upgrade rejoin ticket for orchestrator %s", orchestratorID)
	return nil
}

func (c *orchestratorLink) tryPendingRejoin(ctx context.Context, candidate orchestratorCandidate) bool {
	if c.rejoinTicketDeferred.Load() {
		return false
	}
	ticket, exists, err := c.trust.PendingRejoinTicket(candidate.id, time.Now())
	if c.rejoinTicketDeferred.Load() {
		return false
	}
	if err != nil {
		if errors.Is(err, peeridentity.ErrPendingRejoinTicketExpired) {
			c.reportUpgrade(cluster.UpgradeStatus{
				State: "failed", Phase: "rejoin_ticket_expired", Progress: 95,
				TicketStatus: "expired", FailureReason: "the cached rejoin ticket expired before it could be used",
			})
			return false
		}
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "reading_rejoin_ticket", Progress: 90,
			TicketStatus: "failed", FailureReason: "could not read the cached rejoin ticket",
		})
		logging.Errorf("nodeagent", "nodeagent: read pending rejoin ticket: %v", err)
		return false
	}
	if !exists {
		return false
	}
	c.reportUpgrade(cluster.UpgradeStatus{
		State: "rejoining", Phase: "rejoining_orchestrator", Progress: 95,
		TicketStatus: "ready",
	})
	body, err := json.Marshal(nodeRejoinRequest{
		ID: ticket.NodeID, PeerAddr: c.configState.configSnapshot().AdvertiseAddr,
		CertificatePEM: c.trust.CertificatePEM(), Ticket: ticket.Token,
	})
	if err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "preparing_rejoin", Progress: 95,
			TicketStatus: "ready", FailureReason: "could not prepare the rejoin request",
		})
		logging.Errorf("nodeagent", "nodeagent: encode rejoin request: %v", err)
		return false
	}
	transport := c.trust.BootstrapTransportForID(candidate.id)
	defer transport.CloseIdleConnections()
	client := httpx.NewClient(httpx.ClientOptions{
		Timeout:   httpx.ProbeTimeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("rejoin redirects are not permitted")
		},
	})
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, candidate.address+"/api/pairing/rejoin", bytes.NewReader(body),
	)
	if err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "rejoining", Phase: "waiting_for_orchestrator", Progress: 95,
			TicketStatus: "ready",
		})
		logging.Errorf("nodeagent", "nodeagent: prepare rejoin request: %v", err)
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "rejoining", Phase: "waiting_for_orchestrator", Progress: 95,
			TicketStatus: "ready",
		})
		logging.Errorf("nodeagent", "nodeagent: rejoin with orchestrator %s failed: %v", candidate.id, err)
		return false
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnauthorized ||
			response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusNotFound {
			if clearErr := c.trust.ClearPendingRejoinTicket(candidate.id, ticket.Token); clearErr != nil {
				logging.Errorf("nodeagent", "nodeagent: clear rejected rejoin ticket: %v", clearErr)
			}
			c.reportUpgrade(cluster.UpgradeStatus{
				State: "failed", Phase: "rejoin_rejected", Progress: 95,
				TicketStatus:  "rejected",
				FailureReason: fmt.Sprintf("the orchestrator rejected the rejoin ticket (HTTP %d)", response.StatusCode),
			})
		} else {
			c.reportUpgrade(cluster.UpgradeStatus{
				State: "rejoining", Phase: "waiting_for_orchestrator", Progress: 95,
				TicketStatus: "ready",
			})
		}
		logging.Warnf("nodeagent", "nodeagent: rejoin with orchestrator %s was rejected with HTTP status %d", candidate.id, response.StatusCode)
		return false
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, httpx.ControlJSONBodyBytes+1))
	if err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "reading_rejoin_response", Progress: 95,
			TicketStatus: "failed", FailureReason: "could not read the orchestrator rejoin response",
		})
		logging.Errorf("nodeagent", "nodeagent: read rejoin response: %v", err)
		return false
	}
	if int64(len(data)) > httpx.ControlJSONBodyBytes {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "reading_rejoin_response", Progress: 95,
			TicketStatus: "failed", FailureReason: "the orchestrator rejoin response exceeded the size limit",
		})
		logging.Errorf("nodeagent", "nodeagent: rejoin response exceeds the control-body limit")
		return false
	}
	var result nodePairingResponse
	if err := json.Unmarshal(data, &result); err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "validating_rejoin_response", Progress: 95,
			TicketStatus: "failed", FailureReason: "the orchestrator rejoin response was invalid",
		})
		logging.Errorf("nodeagent", "nodeagent: decode rejoin response: %v", err)
		return false
	}
	if err := c.applyOrchestratorPairingResponse(result, response.TLS, candidate.id, candidate.address); err != nil {
		c.reportUpgrade(cluster.UpgradeStatus{
			State: "failed", Phase: "validating_rejoin_response", Progress: 95,
			TicketStatus: "failed", FailureReason: "the orchestrator rejoin response could not be validated",
		})
		logging.Errorf("nodeagent", "nodeagent: validate rejoin response from %s: %v", candidate.id, err)
		return false
	}
	if err := c.trust.ClearPendingRejoinTicket(candidate.id, ticket.Token); err != nil {
		logging.Errorf("nodeagent", "nodeagent: clear consumed rejoin ticket: %v", err)
	}
	c.reportUpgrade(cluster.UpgradeStatus{
		State: "completed", Phase: "rejoined", Progress: 100,
		TicketStatus: "consumed",
	})
	logging.Infof("nodeagent", "nodeagent: automatically rejoined previously paired orchestrator %s after upgrade", candidate.id)
	return true
}

func (c *orchestratorLink) reportUpgrade(status cluster.UpgradeStatus) {
	if c.reportUpgradeStatus == nil {
		return
	}
	status.UpdatedAt = time.Now().UTC()
	c.reportUpgradeStatus(status)
}

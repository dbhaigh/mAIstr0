package peeridentity

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/fileguard"
)

var (
	ErrRejoinTicketInvalid        = errors.New("peeridentity: rejoin ticket is invalid")
	ErrRejoinTicketExpired        = errors.New("peeridentity: rejoin ticket is expired")
	ErrRejoinTicketWrongNode      = errors.New("peeridentity: rejoin ticket belongs to another node")
	ErrRejoinNodeNotTrusted       = errors.New("peeridentity: rejoin ticket requires a previously trusted node")
	ErrRejoinPeerAddress          = errors.New("peeridentity: rejoin peer address is invalid")
	ErrPendingRejoinTicketExpired = errors.New("peeridentity: pending rejoin ticket expired")
)

type rejoinTicketEntry struct {
	NodeID    string    `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// PendingRejoinTicket is the short-lived client-side credential used only to
// restore trust after an explicitly requested local upgrade.
type PendingRejoinTicket struct {
	NodeID         string    `json:"node_id"`
	OrchestratorID string    `json:"orchestrator_id"`
	Token          string    `json:"ticket"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type pendingRejoinFile struct {
	Ticket PendingRejoinTicket `json:"ticket"`
}

const rejoinTicketSize = 32

// IssueRejoinTicket creates a node-bound bearer ticket and persists only its
// SHA-256 digest alongside the trusted roster.
func (i *Identity) IssueRejoinTicket(nodeID string, now time.Time, ttl time.Duration) (string, time.Time, error) {
	if strings.TrimSpace(nodeID) == "" || ttl <= 0 {
		return "", time.Time{}, errors.New("peeridentity: node ID and positive rejoin ticket TTL are required")
	}
	var tokenBytes [rejoinTicketSize]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return "", time.Time{}, fmt.Errorf("peeridentity: generate rejoin ticket: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes[:])
	digest := rejoinTicketDigest(token)
	expiresAt := now.Add(ttl)

	i.mu.Lock()
	peer, trusted := i.trusted[nodeID]
	if !trusted || peer.Role != "node" {
		i.mu.Unlock()
		return "", time.Time{}, ErrRejoinNodeNotTrusted
	}
	previous := i.issuedRejoinTickets
	next := make(map[string]rejoinTicketEntry, len(previous)+1)
	for hash, entry := range previous {
		if entry.ExpiresAt.After(now) && entry.NodeID != nodeID {
			next[hash] = entry
		}
	}
	next[digest] = rejoinTicketEntry{NodeID: nodeID, ExpiresAt: expiresAt}
	i.issuedRejoinTickets = next
	if err := i.persistLocked(); err != nil {
		i.issuedRejoinTickets = previous
		i.mu.Unlock()
		return "", time.Time{}, fmt.Errorf("peeridentity: persist rejoin ticket: %w", err)
	}
	i.mu.Unlock()
	auditTrustBoundary(0, "rejoin.ticket.issued",
		"identity_id", i.id, "node_id", nodeID, "expires_at", expiresAt, "outcome", "success")
	return token, expiresAt, nil
}

// ConsumeRejoinTicketAndTrust atomically consumes a valid ticket and replaces
// the previously trusted node certificate in the identity file.
func (i *Identity) ConsumeRejoinTicketAndTrust(nodeID, token, certificatePEM, address string, now time.Time) error {
	digest, err := rejoinTicketDigestChecked(token)
	if err != nil {
		return err
	}
	certificate, err := validatePeerCertificate(nodeID, certificatePEM)
	if err != nil {
		return err
	}
	address, err = validateRejoinPeerAddress(address)
	if err != nil {
		return err
	}

	i.mu.Lock()
	entry, exists := i.issuedRejoinTickets[digest]
	if !exists || !i.isTrustedNodeLocked(nodeID) {
		i.mu.Unlock()
		return ErrRejoinTicketInvalid
	}
	if entry.NodeID != nodeID {
		i.mu.Unlock()
		return ErrRejoinTicketWrongNode
	}
	if !entry.ExpiresAt.After(now) {
		previous := i.issuedRejoinTickets
		next := cloneRejoinTickets(previous)
		delete(next, digest)
		i.issuedRejoinTickets = next
		if err := i.persistLocked(); err != nil {
			i.issuedRejoinTickets = previous
			i.mu.Unlock()
			return fmt.Errorf("peeridentity: persist expired rejoin ticket removal: %w", err)
		}
		i.mu.Unlock()
		return ErrRejoinTicketExpired
	}

	previousPeer := i.trusted[nodeID]
	previousTickets := i.issuedRejoinTickets
	nextTickets := cloneRejoinTickets(previousTickets)
	delete(nextTickets, digest)
	fingerprint := certificateFingerprint(certificate)
	nextPeer := previousPeer
	nextPeer.Fingerprint = fingerprint
	nextPeer.Certificate = certificatePEM
	nextPeer.Role = "node"
	if address != "" {
		nextPeer.Address = address
	}
	i.trusted[nodeID] = nextPeer
	i.issuedRejoinTickets = nextTickets
	if err := i.persistLocked(); err != nil {
		i.trusted[nodeID] = previousPeer
		i.issuedRejoinTickets = previousTickets
		i.mu.Unlock()
		return fmt.Errorf("peeridentity: persist rejoined node trust: %w", err)
	}
	changed := !strings.EqualFold(previousPeer.Fingerprint, fingerprint)
	i.mu.Unlock()
	if changed {
		i.invalidateConnections()
	}
	auditTrustBoundary(0, "rejoin.ticket.consumed",
		"identity_id", i.id, "node_id", nodeID, "certificate_fingerprint", fingerprint, "outcome", "success")
	return nil
}

func (i *Identity) isTrustedNodeLocked(id string) bool {
	peer, ok := i.trusted[id]
	return ok && peer.Role == "node"
}

func cloneRejoinTickets(source map[string]rejoinTicketEntry) map[string]rejoinTicketEntry {
	clone := make(map[string]rejoinTicketEntry, len(source))
	for hash, entry := range source {
		clone[hash] = entry
	}
	return clone
}

func rejoinTicketDigest(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func rejoinTicketDigestChecked(token string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != rejoinTicketSize ||
		subtle.ConstantTimeCompare([]byte(base64.RawURLEncoding.EncodeToString(decoded)), []byte(token)) != 1 {
		return "", ErrRejoinTicketInvalid
	}
	return rejoinTicketDigest(token), nil
}

func validateRejoinPeerAddress(address string) (string, error) {
	if address == "" {
		return "", nil
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", ErrRejoinPeerAddress
	}
	return "https://" + parsed.Host, nil
}

// StorePendingRejoinTicket writes the raw one-use token to a protected
// sidecar, separate from the shared identity file used by the host controller.
func (i *Identity) StorePendingRejoinTicket(ticket PendingRejoinTicket) error {
	if ticket.NodeID != i.id || strings.TrimSpace(ticket.OrchestratorID) == "" ||
		!ticket.ExpiresAt.After(time.Now()) {
		return errors.New("peeridentity: pending rejoin ticket identity or expiry is invalid")
	}
	if _, err := rejoinTicketDigestChecked(ticket.Token); err != nil {
		return err
	}
	i.rejoinMu.Lock()
	defer i.rejoinMu.Unlock()
	path, err := i.pendingRejoinPath()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(pendingRejoinFile{Ticket: ticket})
	if err != nil {
		return fmt.Errorf("peeridentity: encode pending rejoin ticket: %w", err)
	}
	if err := writePrivateFile(path, raw); err != nil {
		return fmt.Errorf("peeridentity: persist pending rejoin ticket: %w", err)
	}
	auditTrustBoundary(0, "rejoin.ticket.cached",
		"identity_id", i.id, "orchestrator_id", ticket.OrchestratorID, "expires_at", ticket.ExpiresAt, "outcome", "success")
	return nil
}

// PendingRejoinTicket reads the pending ticket only for its intended
// orchestrator. Expired tickets are deleted before they can be presented.
func (i *Identity) PendingRejoinTicket(orchestratorID string, now time.Time) (PendingRejoinTicket, bool, error) {
	i.rejoinMu.Lock()
	defer i.rejoinMu.Unlock()
	path, err := i.pendingRejoinPath()
	if err != nil {
		return PendingRejoinTicket{}, false, err
	}
	raw, err := readPrivateFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return PendingRejoinTicket{}, false, nil
	}
	if err != nil {
		return PendingRejoinTicket{}, false, fmt.Errorf("peeridentity: read pending rejoin ticket: %w", err)
	}
	var saved pendingRejoinFile
	if err := json.Unmarshal(raw, &saved); err != nil {
		return PendingRejoinTicket{}, false, fmt.Errorf("peeridentity: decode pending rejoin ticket: %w", err)
	}
	ticket := saved.Ticket
	if ticket.NodeID != i.id || ticket.OrchestratorID == "" {
		return PendingRejoinTicket{}, false, errors.New("peeridentity: pending rejoin ticket identity does not match")
	}
	if !ticket.ExpiresAt.After(now) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return PendingRejoinTicket{}, false, fmt.Errorf("peeridentity: remove expired rejoin ticket: %w", err)
		}
		auditTrustBoundary(0, "rejoin.ticket.cache_expired",
			"identity_id", i.id, "orchestrator_id", ticket.OrchestratorID, "outcome", "expired")
		return PendingRejoinTicket{}, false, ErrPendingRejoinTicketExpired
	}
	if _, err := rejoinTicketDigestChecked(ticket.Token); err != nil {
		return PendingRejoinTicket{}, false, err
	}
	if orchestratorID != "" && ticket.OrchestratorID != orchestratorID {
		return PendingRejoinTicket{}, false, nil
	}
	return ticket, true, nil
}

// ClearPendingRejoinTicket removes a matching cached ticket after successful
// rejoin. A mismatched token or orchestrator cannot clear another ticket.
func (i *Identity) ClearPendingRejoinTicket(orchestratorID, token string) error {
	i.rejoinMu.Lock()
	defer i.rejoinMu.Unlock()
	path, err := i.pendingRejoinPath()
	if err != nil {
		return err
	}
	raw, err := readPrivateFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("peeridentity: read pending rejoin ticket for removal: %w", err)
	}
	var saved pendingRejoinFile
	if err := json.Unmarshal(raw, &saved); err != nil {
		return fmt.Errorf("peeridentity: decode pending rejoin ticket for removal: %w", err)
	}
	if saved.Ticket.OrchestratorID != orchestratorID ||
		subtle.ConstantTimeCompare([]byte(saved.Ticket.Token), []byte(token)) != 1 {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("peeridentity: remove pending rejoin ticket: %w", err)
	}
	return nil
}

func (i *Identity) pendingRejoinPath() (string, error) {
	directory := filepath.Join(filepath.Dir(i.path), "rejoin-"+safeRejoinPath(i.id))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("peeridentity: create rejoin cache directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", fmt.Errorf("peeridentity: restrict rejoin cache directory permissions: %w", err)
	}
	if err := fileguard.RestrictToCurrentUser(directory); err != nil {
		return "", fmt.Errorf("peeridentity: restrict rejoin cache directory ACL: %w", err)
	}
	return filepath.Join(directory, "pending.json"), nil
}

func safeRejoinPath(value string) string {
	var safe strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			safe.WriteRune(r)
		default:
			safe.WriteByte('-')
		}
	}
	return safe.String()
}

func writePrivateFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".pending-*.tmp")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(fmt.Errorf("restrict pending file mode: %w", err), file.Close())
	}
	if err := fileguard.RestrictToCurrentUser(tempPath); err != nil {
		return errors.Join(fmt.Errorf("restrict pending file ACL: %w", err), file.Close())
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace pending file: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("restrict pending file mode: %w", err)
	}
	if err := fileguard.RestrictToCurrentUser(path); err != nil {
		return fmt.Errorf("restrict pending file ACL: %w", err)
	}
	return nil
}

func readPrivateFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(errors.New("peeridentity: pending rejoin cache is not a regular file"), file.Close())
	}
	if err := file.Chmod(0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("peeridentity: restrict pending cache mode: %w", err), file.Close())
	}
	if err := fileguard.RestrictToCurrentUser(path); err != nil {
		return nil, errors.Join(fmt.Errorf("peeridentity: restrict pending cache ACL: %w", err), file.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, 16<<10))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	return raw, nil
}

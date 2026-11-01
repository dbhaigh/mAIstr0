package orchestrator

import (
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

// SetAdvertiseAddress configures the public HTTPS address advertised to peers.
//
// Parameters: address (string) must be an https URL with a host and without
// user information, path, query, or fragment.
// Returns: an error when the address is not a valid advertised HTTPS URL.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) SetAdvertiseAddress(address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("orchestrator advertised address must be an https URL without user information")
	}
	s.advertiseAddr = "https://" + parsed.Host
	return nil
}

// SetLocalNodeDashboard configures the local pairing UI used after yielding.
//
// Parameters: address (string) must be an HTTP URL whose host is localhost or
// a loopback IP, with no path, query, or fragment.
// Returns: an error when address is not a valid loopback HTTP URL.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) SetLocalNodeDashboard(address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("local node dashboard must be a loopback HTTP URL")
	}
	ip := net.ParseIP(parsed.Hostname())
	if parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("local node dashboard must be a loopback HTTP URL")
	}
	s.localNodeDashboard = "http://" + parsed.Host
	return nil
}

// InitializeIdentity loads the persistent TLS identity and creates a
// temporary pairing PIN for first-time node enrollment.
//
// Returns: an error if identity loading, PIN creation, or trust-state cleanup
// fails.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) InitializeIdentity() error {
	if s.identity != nil {
		return nil
	}
	identity, err := peeridentity.Open(s.identityPath, s.SelfID())
	if err != nil {
		return err
	}
	if _, err := identity.NewPairingPIN(); err != nil {
		return err
	}
	if err := identity.RemovePairedOrchestrators(); err != nil {
		return fmt.Errorf("remove unsupported orchestrator pairing: %w", err)
	}
	s.identity = identity
	s.electionPort().setIdentity(identity)
	s.nodeClientPort().setTransport(identity.ClientTransport)
	logging.Infof("orchestrator", "secure pairing PIN generated (valid for 30 minutes)")
	return nil
}

// UseIdentity attaches an existing cluster identity to a promoted orchestrator.
// Call it before Run; it preserves the identity's paired-member roster.
//
// Parameters: identity (*peeridentity.Identity) must be nonnil and have a
// nonempty ID, and must not already be initialized on this Server.
// Returns: an error when the identity is invalid, already configured, or
// cannot create a pairing PIN.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) UseIdentity(identity *peeridentity.Identity) error {
	if identity == nil || identity.ID() == "" {
		return errors.New("orchestrator identity is required")
	}
	if s.identity != nil {
		return errors.New("orchestrator identity is already initialized")
	}
	if _, err := identity.NewPairingPIN(); err != nil {
		return fmt.Errorf("create promoted orchestrator pairing PIN: %w", err)
	}
	s.identity = identity
	s.electionPort().setSelfID(identity.ID())
	s.electionPort().setIdentity(identity)
	s.nodeClientPort().setTransport(identity.ClientTransport)
	logging.Infof("orchestrator", "secure pairing PIN generated (valid for 30 minutes)")
	return nil
}

// IdentityCertificate returns the local TLS certificate in PEM form.
//
// Returns: the certificate PEM, or an error when identity is not initialized.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) IdentityCertificate() (string, error) {
	if s.identity == nil {
		return "", errors.New("orchestrator identity is not initialized")
	}
	return s.identity.CertificatePEM(), nil
}

// IdentityPath returns the configured persistent identity file path.
//
// Returns: the path used to load or persist the peer identity.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) IdentityPath() string {
	return s.identityPath
}

// TrustPeer adds a peer certificate to the local identity's trusted roster.
//
// Parameters: id (string) is the peer node ID; certificatePEM (string) is its
// certificate in PEM form.
// Returns: an error when identity is uninitialized or the peer certificate
// cannot be trusted.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_identity_test.go](server_identity_test.go).
func (s *Server) TrustPeer(id, certificatePEM string) error {
	if s.identity == nil {
		return errors.New("orchestrator identity is not initialized")
	}
	return s.identity.TrustPeer(id, certificatePEM)
}

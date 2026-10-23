package orchestrator

import (
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

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
// It must be called before Run and preserves the node's paired-member roster.
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

func (s *Server) IdentityCertificate() (string, error) {
	if s.identity == nil {
		return "", errors.New("orchestrator identity is not initialized")
	}
	return s.identity.CertificatePEM(), nil
}

func (s *Server) TrustPeer(id, certificatePEM string) error {
	if s.identity == nil {
		return errors.New("orchestrator identity is not initialized")
	}
	return s.identity.TrustPeer(id, certificatePEM)
}

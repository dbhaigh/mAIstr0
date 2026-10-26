package nodeagent

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/peeridentity"
)

func (c *identityComponent) initialize(nodeID, memoryPath, advertiseAddr string) error {
	if c.identity != nil {
		return nil
	}
	path := peeridentity.DefaultPath(nodeID)
	if memoryPath != "" {
		path = memoryPath + ".identity"
	}
	identity, err := peeridentity.Open(path, nodeID)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.ToLower(advertiseAddr), "https://") {
		return fmt.Errorf("node advertised address must use https for paired cluster traffic")
	}
	c.identity = identity
	return nil
}

func (c *identityComponent) certificate() (string, error) {
	if c.identity == nil {
		return "", errors.New("node identity is not initialized")
	}
	return c.identity.CertificatePEM(), nil
}

func (c *identityComponent) Initialized() bool {
	return c.identity != nil
}

func (c *identityComponent) clientTransport() (http.RoundTripper, error) {
	if c.identity == nil {
		return nil, errors.New("node identity is not initialized")
	}
	return c.identity.ClientTransport(), nil
}

func (c *identityComponent) CertificatePEM() string {
	if c.identity == nil {
		return ""
	}
	return c.identity.CertificatePEM()
}

func (c *identityComponent) BootstrapTransport() *http.Transport {
	if c.identity == nil {
		return nil
	}
	return c.identity.BootstrapTransport()
}

func (c *identityComponent) BootstrapTransportForID(expectedID string) *http.Transport {
	if c.identity == nil {
		return nil
	}
	return c.identity.BootstrapTransportForID(expectedID)
}

func (c *identityComponent) BootstrapTransportForFingerprint(expectedID, fingerprint string) *http.Transport {
	if c.identity == nil {
		return nil
	}
	return c.identity.BootstrapTransportForFingerprint(expectedID, fingerprint)
}

func (c *identityComponent) TrustPeer(id, certificate string) error {
	if c.identity == nil {
		return errors.New("node identity is not initialized")
	}
	return c.identity.TrustPeer(id, certificate)
}

func (c *identityComponent) ValidatePeer(id, certificate string) error {
	if c.identity == nil {
		return errors.New("node identity is not initialized")
	}
	return c.identity.ValidatePeer(id, certificate)
}

func (c *identityComponent) TrustPeerDetails(id, certificate, role, address string) error {
	if c.identity == nil {
		return errors.New("node identity is not initialized")
	}
	return c.identity.TrustPeerDetails(id, certificate, role, address)
}

func (c *identityComponent) TrustedIDs() []string {
	if c.identity == nil {
		return nil
	}
	return c.identity.TrustedIDs()
}

func (c *identityComponent) TrustedPeers() map[string]string {
	if c.identity == nil {
		return nil
	}
	return c.identity.TrustedPeers()
}

func (c *identityComponent) RemovePeer(id string) error {
	if c.identity == nil {
		return errors.New("node identity is not initialized")
	}
	return c.identity.RemovePeer(id)
}

func (c *identityComponent) IsTrustedID(id string) bool {
	return c.identity != nil && c.identity.IsTrustedID(id)
}

func (c *identityComponent) IsTrustedOrchestratorID(id string) bool {
	return c.identity != nil && c.identity.IsTrustedOrchestratorID(id)
}

func (c *identityComponent) requireTrustedPeer(handler http.Handler) http.Handler {
	return c.identity.RequireTrustedPeer(handler)
}

func (c *identityComponent) serverTLSConfig() *tls.Config {
	return c.identity.ServerTLSConfig()
}

// ClusterIdentity returns the initialized identity shared by the node agent
// and any orchestrator promoted on this process.
func (a *Agent) ClusterIdentity() *peeridentity.Identity {
	return a.identityComponent.identity
}

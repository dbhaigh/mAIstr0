package peeridentity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type trustedPeer struct {
	Fingerprint string `json:"fingerprint"`
	Certificate string `json:"certificate"`
	Role        string `json:"role,omitempty"`
	Address     string `json:"address,omitempty"`
}

type identityFile struct {
	ID          string                 `json:"id"`
	Certificate string                 `json:"certificate"`
	PrivateKey  string                 `json:"private_key"`
	Trusted     map[string]trustedPeer `json:"trusted"`
}

// Identity owns a node's self-signed TLS identity and the pins for cluster
// members explicitly admitted through pairing.
type Identity struct {
	// mu protects identity, pairing, and trust state and serializes persistence
	// of those updates. No other application lock is nested beneath it.
	mu              sync.RWMutex
	id              string
	path            string
	certificate     tls.Certificate
	certificatePEM  string
	privateKeyPEM   string
	trusted         map[string]trustedPeer
	pairingPIN      string
	pairingExpires  time.Time
	pairingAttempts int
	clientTransport *http.Transport
	activeConns     map[*identityConn]struct{}
}

type identityConn struct {
	net.Conn
	identity *Identity
	once     sync.Once
}

func (c *identityConn) Close() error {
	var err error
	c.once.Do(func() {
		c.identity.mu.Lock()
		delete(c.identity.activeConns, c)
		c.identity.mu.Unlock()
		err = c.Conn.Close()
	})
	return err
}

// Open loads or creates a persistent certificate for id.
func Open(path, id string) (*Identity, error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(id) == "" {
		return nil, errors.New("peeridentity: identity path and id are required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("peeridentity: create identity directory: %w", err)
	}
	identity := &Identity{id: id, path: path, trusted: make(map[string]trustedPeer)}
	raw, err := os.ReadFile(path)
	if err == nil {
		var saved identityFile
		if err := json.Unmarshal(raw, &saved); err != nil {
			return nil, fmt.Errorf("peeridentity: decode identity file: %w", err)
		}
		if saved.ID != id {
			return nil, fmt.Errorf("peeridentity: identity file belongs to %q, not %q", saved.ID, id)
		}
		identity.certificatePEM = saved.Certificate
		identity.privateKeyPEM = saved.PrivateKey
		identity.trusted = saved.Trusted
		if identity.trusted == nil {
			identity.trusted = make(map[string]trustedPeer)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("peeridentity: read identity file: %w", err)
	} else {
		if err := identity.createCertificate(); err != nil {
			return nil, err
		}
	}
	certificate, err := tls.X509KeyPair([]byte(identity.certificatePEM), []byte(identity.privateKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("peeridentity: load TLS identity: %w", err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("peeridentity: parse TLS certificate: %w", err)
	}
	if leaf.Subject.CommonName != id {
		return nil, fmt.Errorf("peeridentity: certificate belongs to %q, not %q", leaf.Subject.CommonName, id)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return nil, errors.New("peeridentity: identity certificate is not valid yet")
	}
	if !now.Before(leaf.NotAfter) {
		if err := identity.createCertificate(); err != nil {
			return nil, fmt.Errorf("peeridentity: rotate expired identity certificate: %w", err)
		}
		certificate = identity.certificate
		leaf, err = x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("peeridentity: parse rotated TLS certificate: %w", err)
		}
	}
	certificate.Leaf = leaf
	identity.certificate = certificate
	if err := identity.persistLocked(); err != nil {
		return nil, err
	}
	return identity, nil
}

// DefaultPath returns the per-user location for an owner's private identity.
func DefaultPath(owner string) string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	var safe strings.Builder
	for _, r := range owner {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			safe.WriteRune(r)
		default:
			safe.WriteByte('-')
		}
	}
	return filepath.Join(base, "maistr0", "identity-"+safe.String()+".json")
}

func (i *Identity) createCertificate() error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("peeridentity: generate private key: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          new(big.Int).SetInt64(now.UnixNano()),
		Subject:               pkix.Name{CommonName: i.id},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("peeridentity: create self-signed certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("peeridentity: encode private key: %w", err)
	}
	i.certificatePEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	i.privateKeyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	certificate, err := tls.X509KeyPair([]byte(i.certificatePEM), []byte(i.privateKeyPEM))
	if err != nil {
		return fmt.Errorf("peeridentity: load generated TLS identity: %w", err)
	}
	i.certificate = certificate
	return nil
}

// ID returns the stable identity represented by this certificate.
func (i *Identity) ID() string { return i.id }

// CertificatePEM returns the public certificate for PIN-authenticated
// bootstrap pairing.
func (i *Identity) CertificatePEM() string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.certificatePEM
}

// ServerTLSConfig requests a client certificate; authorization is performed
// by the trusted-peer middleware after the TLS handshake.
func (i *Identity) ServerTLSConfig() *tls.Config {
	i.mu.RLock()
	config := &tls.Config{
		Certificates: []tls.Certificate{i.certificate},
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	i.mu.RUnlock()
	config.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		certificate, err := i.currentIdentityCertificate()
		if err != nil {
			return nil, err
		}
		current := config.Clone()
		current.GetConfigForClient = nil
		current.Certificates = []tls.Certificate{*certificate}
		return current, nil
	}
	return config
}

// PinnedPeerServerTLSConfig requires a client certificate whose current
// fingerprint is pinned by this identity.
func (i *Identity) PinnedPeerServerTLSConfig() *tls.Config {
	i.mu.RLock()
	config := &tls.Config{
		Certificates: []tls.Certificate{i.certificate},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	i.mu.RUnlock()
	config.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		certificate, err := i.currentIdentityCertificate()
		if err != nil {
			return nil, err
		}
		current := config.Clone()
		current.GetConfigForClient = nil
		current.Certificates = []tls.Certificate{*certificate}
		return current, nil
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || !i.IsTrustedCertificate(state.PeerCertificates[0]) {
			return errors.New("peeridentity: client certificate is not pinned to a paired cluster member")
		}
		return nil
	}
	return config
}

// PinnedClientTLSConfig authenticates this identity and accepts only a
// currently pinned server certificate with the requested stable peer ID.
func (i *Identity) PinnedClientTLSConfig(expectedID string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // The self-signed certificate is verified by its pin below.
		MinVersion:         tls.VersionTLS13,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return i.currentIdentityCertificate()
		},
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("peeridentity: relay server did not present a certificate")
			}
			certificate := state.PeerCertificates[0]
			if certificate.Subject.CommonName != expectedID {
				return fmt.Errorf("peeridentity: relay server identity does not match expected peer %q", expectedID)
			}
			if !i.IsTrustedCertificate(certificate) {
				return errors.New("peeridentity: relay server certificate is not pinned to a paired cluster member")
			}
			return nil
		},
	}
}

// DialPinned establishes an mTLS connection to a currently paired peer.
func (i *Identity) DialPinned(ctx context.Context, network, address, expectedID string) (net.Conn, error) {
	if strings.TrimSpace(expectedID) == "" {
		return nil, errors.New("peeridentity: expected peer ID is required")
	}
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second},
		Config:    i.PinnedClientTLSConfig(expectedID),
	}
	connection, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tlsConnection, ok := connection.(*tls.Conn)
	if !ok {
		_ = connection.Close()
		return nil, errors.New("peeridentity: pinned dial did not return a TLS connection")
	}
	state := tlsConnection.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = connection.Close()
		return nil, errors.New("peeridentity: relay server did not present a certificate")
	}
	peerCertificate := state.PeerCertificates[0]
	i.mu.Lock()
	peer, exists := i.trusted[expectedID]
	if !exists || peer.Fingerprint != certificateFingerprint(peerCertificate) ||
		!certificateCurrentlyValid(peerCertificate, time.Now()) {
		i.mu.Unlock()
		_ = connection.Close()
		return nil, errors.New("peeridentity: relay server certificate is no longer pinned")
	}
	if i.activeConns == nil {
		i.activeConns = make(map[*identityConn]struct{})
	}
	tracked := &identityConn{Conn: connection, identity: i}
	i.activeConns[tracked] = struct{}{}
	i.mu.Unlock()
	return tracked, nil
}

// TrackPinnedPeerConnection registers an authenticated inbound or outbound
// connection so pin changes can terminate traffic using a stale identity.
func (i *Identity) TrackPinnedPeerConnection(certificate *x509.Certificate, connection net.Conn) (net.Conn, error) {
	if certificate == nil || connection == nil {
		return nil, errors.New("peeridentity: a pinned certificate and connection are required")
	}
	if !certificateCurrentlyValid(certificate, time.Now()) {
		return nil, errors.New("peeridentity: peer certificate is outside its validity period")
	}
	id := certificate.Subject.CommonName
	if id == "" {
		return nil, errors.New("peeridentity: peer certificate has no identity")
	}
	i.mu.Lock()
	peer, exists := i.trusted[id]
	if !exists || peer.Fingerprint != certificateFingerprint(certificate) {
		i.mu.Unlock()
		return nil, errors.New("peeridentity: peer connection certificate is not currently pinned")
	}
	if i.activeConns == nil {
		i.activeConns = make(map[*identityConn]struct{})
	}
	tracked := &identityConn{Conn: connection, identity: i}
	i.activeConns[tracked] = struct{}{}
	i.mu.Unlock()
	return tracked, nil
}

// ClientTransport returns a transport that presents this identity and accepts
// only server certificates pinned by a completed pairing.
func (i *Identity) ClientTransport() *http.Transport {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.clientTransport != nil {
		return i.clientTransport
	}
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			tracked := &identityConn{Conn: conn, identity: i}
			i.mu.Lock()
			if i.activeConns == nil {
				i.activeConns = make(map[*identityConn]struct{})
			}
			i.activeConns[tracked] = struct{}{}
			i.mu.Unlock()
			return tracked, nil
		},
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{i.certificate},
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return i.currentIdentityCertificate()
			},
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return errors.New("peeridentity: server certificate is not pinned to a paired cluster member")
				}
				if !certificateCurrentlyValid(state.PeerCertificates[0], time.Now()) {
					return errors.New("peeridentity: server certificate is outside its validity period")
				}
				if !i.IsTrustedCertificate(state.PeerCertificates[0]) {
					return errors.New("peeridentity: server certificate is not pinned to a paired cluster member")
				}
				return nil
			},
		},
	}
	i.clientTransport = transport
	return transport
}

func (i *Identity) currentIdentityCertificate() (*tls.Certificate, error) {
	i.mu.Lock()
	leaf := i.certificate.Leaf
	if leaf == nil {
		if len(i.certificate.Certificate) == 0 {
			i.mu.Unlock()
			return nil, errors.New("peeridentity: identity certificate is unavailable")
		}
		var err error
		leaf, err = x509.ParseCertificate(i.certificate.Certificate[0])
		if err != nil {
			i.mu.Unlock()
			return nil, fmt.Errorf("peeridentity: parse identity certificate: %w", err)
		}
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) {
		i.mu.Unlock()
		return nil, errors.New("peeridentity: identity certificate is not valid yet")
	}
	rotated := false
	if !now.Before(leaf.NotAfter) {
		previousCertificate := i.certificate
		previousPEM := i.certificatePEM
		previousKey := i.privateKeyPEM
		if err := i.createCertificate(); err != nil {
			i.mu.Unlock()
			return nil, fmt.Errorf("peeridentity: rotate expired identity certificate: %w", err)
		}
		leaf, err := x509.ParseCertificate(i.certificate.Certificate[0])
		if err != nil {
			i.certificate = previousCertificate
			i.certificatePEM = previousPEM
			i.privateKeyPEM = previousKey
			i.mu.Unlock()
			return nil, fmt.Errorf("peeridentity: parse rotated identity certificate: %w", err)
		}
		i.certificate.Leaf = leaf
		if err := i.persistLocked(); err != nil {
			i.certificate = previousCertificate
			i.certificatePEM = previousPEM
			i.privateKeyPEM = previousKey
			i.mu.Unlock()
			return nil, fmt.Errorf("peeridentity: persist rotated identity certificate: %w", err)
		}
		rotated = true
	}
	certificate := i.certificate
	transport := i.clientTransport
	i.mu.Unlock()
	if rotated && transport != nil {
		transport.CloseIdleConnections()
	}
	return &certificate, nil
}

func (i *Identity) invalidateConnections() {
	i.mu.Lock()
	transport := i.clientTransport
	connections := make([]*identityConn, 0, len(i.activeConns))
	for connection := range i.activeConns {
		connections = append(connections, connection)
	}
	i.mu.Unlock()

	if transport != nil {
		transport.CloseIdleConnections()
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
}

// BootstrapTransport is used only for PIN-authenticated first contact. It
// must not be used for ordinary cluster requests.
func (i *Identity) BootstrapTransport() *http.Transport {
	return i.bootstrapTransport("", "")
}

// BootstrapTransportForID verifies a previously pinned identity. A discovered
// name alone does not authenticate a first-contact certificate.
func (i *Identity) BootstrapTransportForID(expectedID string) *http.Transport {
	return i.bootstrapTransport(expectedID, "")
}

// BootstrapTransportForFingerprint uses an operator-verified out-of-band pin
// for enrollment or certificate rotation. It does not trust discovery metadata.
func (i *Identity) BootstrapTransportForFingerprint(expectedID, fingerprint string) *http.Transport {
	return i.bootstrapTransport(expectedID, fingerprint)
}

func (i *Identity) bootstrapTransport(expectedID, fingerprint string) *http.Transport {
	i.mu.RLock()
	defer i.mu.RUnlock()
	config := &tls.Config{
		Certificates:       []tls.Certificate{i.certificate},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("peeridentity: bootstrap server did not present a certificate")
		}
		certificate := state.PeerCertificates[0]
		if !certificateCurrentlyValid(certificate, time.Now()) {
			return errors.New("peeridentity: bootstrap server certificate is outside its validity period")
		}
		if expectedID != "" && certificate.Subject.CommonName != expectedID {
			return fmt.Errorf("peeridentity: bootstrap server identity does not match expected peer %q", expectedID)
		}
		if fingerprint != "" {
			if err := ValidateFingerprint(fingerprint); err != nil {
				return err
			}
			if !strings.EqualFold(certificateFingerprint(certificate), fingerprint) {
				return errors.New("peeridentity: bootstrap server certificate fingerprint does not match the verified fingerprint")
			}
		} else if !i.IsTrustedCertificate(certificate) {
			return errors.New("peeridentity: first-contact pairing requires an out-of-band verified certificate fingerprint")
		}
		return nil
	}
	return &http.Transport{TLSClientConfig: config}
}

// NewPairingPIN creates a short-lived code accepted during first-contact
// pairing. The code is not persisted and must be communicated out of band.
func (i *Identity) NewPairingPIN() (string, error) {
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("peeridentity: generate pairing PIN: %w", err)
	}
	value := uint32(random[0])<<24 | uint32(random[1])<<16 | uint32(random[2])<<8 | uint32(random[3])
	pin := fmt.Sprintf("%06d", value%900000+100000)
	i.mu.Lock()
	i.pairingPIN = pin
	i.pairingExpires = time.Now().Add(30 * time.Minute)
	i.pairingAttempts = 0
	i.mu.Unlock()
	return pin, nil
}

// VerifyPairingPIN checks the active short-lived pairing code.
func (i *Identity) VerifyPairingPIN(pin string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.pairingPINActiveLocked(time.Now()) &&
		len(pin) == len(i.pairingPIN) &&
		subtle.ConstantTimeCompare([]byte(pin), []byte(i.pairingPIN)) == 1
}

// ActivePairingPIN returns the current pairing code only while it can still
// be consumed.
func (i *Identity) ActivePairingPIN() (string, time.Time, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if !i.pairingPINActiveLocked(time.Now()) {
		return "", time.Time{}, false
	}
	return i.pairingPIN, i.pairingExpires, true
}

func (i *Identity) pairingPINActiveLocked(now time.Time) bool {
	return i.pairingPIN != "" && i.pairingAttempts < 20 && now.Before(i.pairingExpires)
}

// ConsumePairingPIN atomically accepts a valid code once.
func (i *Identity) ConsumePairingPIN(pin string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.pairingPINActiveLocked(time.Now()) {
		return false
	}
	if len(pin) != len(i.pairingPIN) || subtle.ConstantTimeCompare([]byte(pin), []byte(i.pairingPIN)) != 1 {
		i.pairingAttempts++
		if i.pairingAttempts >= 20 {
			i.pairingPIN = ""
		}
		return false
	}
	i.pairingPIN = ""
	i.pairingExpires = time.Time{}
	return true
}

// TrustPeer adds or refreshes a peer certificate pin after pairing or a
// trusted roster update.
func (i *Identity) TrustPeer(id, certificatePEM string) error {
	return i.TrustPeerDetails(id, certificatePEM, "", "")
}

// TrustPeerDetails pins a peer certificate and stores its role and optional
// HTTPS address for reconnecting to paired orchestrators after restart.
func (i *Identity) TrustPeerDetails(id, certificatePEM, role, address string) error {
	if role != "" && role != "node" && role != "orchestrator" {
		return fmt.Errorf("peeridentity: invalid peer role %q", role)
	}
	if address != "" {
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("peeridentity: peer address must be an https URL without user information")
		}
		address = "https://" + parsed.Host
	}
	cert, err := validatePeerCertificate(id, certificatePEM)
	if err != nil {
		return err
	}
	i.mu.Lock()
	previous, existed := i.trusted[id]
	peer := previous
	fingerprint := certificateFingerprint(cert)
	changed := peer.Fingerprint != fingerprint
	peer.Fingerprint = fingerprint
	peer.Certificate = certificatePEM
	if role != "" {
		peer.Role = role
	}
	if address != "" {
		peer.Address = address
	}
	i.trusted[id] = peer
	err = i.persistLocked()
	if err != nil {
		i.restoreTrustLocked(id, previous, existed)
		changed = false
	}
	i.mu.Unlock()
	if changed {
		i.invalidateConnections()
	}
	return err
}

// restoreTrustLocked undoes an unpersisted pin change so a peer is never
// trusted in memory without also being trusted after a restart. Callers hold mu.
func (i *Identity) restoreTrustLocked(id string, previous trustedPeer, existed bool) {
	if existed {
		i.trusted[id] = previous
		return
	}
	delete(i.trusted, id)
}

// RemovePairedOrchestrators removes federation-era orchestrator pins so
// upgraded instances only retain ordinary node identities in their roster.
func (i *Identity) RemovePairedOrchestrators() error {
	i.mu.Lock()
	changed := false
	for id, peer := range i.trusted {
		if peer.Role == "orchestrator" {
			delete(i.trusted, id)
			changed = true
		}
	}
	var err error
	if changed {
		err = i.persistLocked()
	}
	i.mu.Unlock()
	if changed {
		i.invalidateConnections()
	}
	return err
}

func (i *Identity) ValidatePeer(id, certificatePEM string) error {
	_, err := validatePeerCertificate(id, certificatePEM)
	return err
}

func validatePeerCertificate(id, certificatePEM string) (*x509.Certificate, error) {
	cert, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, err
	}
	if cert.Subject.CommonName != id {
		return nil, fmt.Errorf("peeridentity: certificate identity %q does not match %q", cert.Subject.CommonName, id)
	}
	if !certificateCurrentlyValid(cert, time.Now()) {
		return nil, errors.New("peeridentity: peer certificate is outside its validity period")
	}
	return cert, nil
}

func certificateCurrentlyValid(cert *x509.Certificate, now time.Time) bool {
	return cert != nil && !now.Before(cert.NotBefore) && now.Before(cert.NotAfter)
}

// IsTrustedID reports whether the identity was explicitly paired.
func (i *Identity) IsTrustedID(id string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	_, ok := i.trusted[id]
	return ok
}

// IsTrustedOrchestratorID reports whether id is pinned with the orchestrator role.
func (i *Identity) IsTrustedOrchestratorID(id string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	peer, ok := i.trusted[id]
	return ok && peer.Role == "orchestrator"
}

// TrustedIDs returns the IDs of all paired cluster members.
func (i *Identity) TrustedIDs() []string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ids := make([]string, 0, len(i.trusted))
	for id := range i.trusted {
		ids = append(ids, id)
	}
	return ids
}

// RemovePeer revokes a paired member's certificate pin.
func (i *Identity) RemovePeer(id string) error {
	i.mu.Lock()
	_, exists := i.trusted[id]
	delete(i.trusted, id)
	err := i.persistLocked()
	i.mu.Unlock()
	if exists {
		i.invalidateConnections()
	}
	return err
}

// IsTrustedCertificate validates a peer against the persisted certificate pin.
func (i *Identity) IsTrustedCertificate(cert *x509.Certificate) bool {
	if !certificateCurrentlyValid(cert, time.Now()) {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	peer, ok := i.trusted[cert.Subject.CommonName]
	return ok && peer.Fingerprint == certificateFingerprint(cert)
}

// RequireTrustedPeer rejects requests that did not present a currently pinned
// client certificate.
func (i *Identity) RequireTrustedPeer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 ||
			!i.IsTrustedCertificate(r.TLS.PeerCertificates[0]) {
			http.Error(w, "request requires a paired cluster identity", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// LoopbackOnly rejects plaintext HTTP requests from non-loopback clients.
func LoopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			http.Error(w, "plaintext HTTP is available only on loopback; pair the node and use mutual TLS", http.StatusForbidden)
			return
		}
		if !isLoopbackHost(r.Host) {
			http.Error(w, "loopback dashboard requires a localhost or loopback Host", http.StatusForbidden)
			return
		}
		if !sameLocalOrigin(r) {
			http.Error(w, "cross-origin dashboard requests are not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackHost(hostHeader string) bool {
	if hostHeader == "" || strings.HasSuffix(hostHeader, ":") {
		return false
	}
	parsed, err := url.Parse("//" + hostHeader)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	hostname := parsed.Hostname()
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	ip := net.ParseIP(hostname)
	return ip != nil && ip.IsLoopback()
}

func sameLocalOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" && !sameOriginHost(origin, r.Host, true) {
		return false
	}
	fetchSite := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")))
	if fetchSite != "" && fetchSite != "same-origin" {
		if fetchSite != "none" || (r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions) {
			return false
		}
	}
	if origin == "" && fetchSite == "" {
		referer := r.Header.Get("Referer")
		if referer != "" && !sameOriginHost(referer, r.Host, false) {
			return false
		}
	}
	return true
}

func sameOriginHost(value, requestHost string, originOnly bool) bool {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil ||
		parsed.Scheme != "http" {
		return false
	}
	if originOnly && (parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "") {
		return false
	}
	requestURL, err := url.Parse("//" + requestHost)
	if err != nil || requestURL.Hostname() == "" || requestURL.User != nil {
		return false
	}
	originIP, requestIP := net.ParseIP(parsed.Hostname()), net.ParseIP(requestURL.Hostname())
	hostMatches := strings.EqualFold(parsed.Hostname(), requestURL.Hostname())
	if originIP != nil || requestIP != nil {
		hostMatches = originIP != nil && requestIP != nil && originIP.Equal(requestIP)
	}
	if !hostMatches {
		return false
	}
	originPort, requestPort := parsed.Port(), requestURL.Port()
	if originPort == "" {
		originPort = "80"
	}
	if requestPort == "" {
		requestPort = "80"
	}
	return originPort == requestPort
}

// TrustedPeers returns a copy of the public paired-member certificate roster.
func (i *Identity) TrustedPeers() map[string]string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make(map[string]string, len(i.trusted))
	for id, peer := range i.trusted {
		out[id] = peer.Certificate
	}
	return out
}

func (i *Identity) persistLocked() error {
	saved := identityFile{
		ID: i.id, Certificate: i.certificatePEM, PrivateKey: i.privateKeyPEM, Trusted: i.trusted,
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		return fmt.Errorf("peeridentity: encode identity file: %w", err)
	}
	file, err := os.OpenFile(i.path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("peeridentity: persist identity file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("peeridentity: restrict identity permissions: %w", errors.Join(err, file.Close()))
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("peeridentity: truncate identity file: %w", errors.Join(err, file.Close()))
	}
	_, writeErr := file.Write(raw)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("peeridentity: persist identity file: %w", err)
	}
	return nil
}

func parseCertificate(certificatePEM string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certificatePEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("peeridentity: invalid PEM certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("peeridentity: parse certificate: %w", err)
	}
	return cert, nil
}

func certificateFingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

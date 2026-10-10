package peeridentity

import (
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
	defer i.mu.RUnlock()
	return &tls.Config{
		Certificates: []tls.Certificate{i.certificate},
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

// ClientTransport returns a transport that presents this identity and accepts
// only server certificates pinned by a completed pairing.
func (i *Identity) ClientTransport() *http.Transport {
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{i.certificate},
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 || !i.IsTrustedCertificate(state.PeerCertificates[0]) {
					return errors.New("peeridentity: server certificate is not pinned to a paired cluster member")
				}
				return nil
			},
		},
	}
}

// BootstrapTransport is used only for PIN-authenticated first contact. It
// must not be used for ordinary cluster requests.
func (i *Identity) BootstrapTransport() *http.Transport {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return &http.Transport{TLSClientConfig: &tls.Config{
		Certificates:       []tls.Certificate{i.certificate},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	}}
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
	return i.pairingPIN != "" && i.pairingAttempts < 20 && time.Now().Before(i.pairingExpires) &&
		len(pin) == len(i.pairingPIN) &&
		subtle.ConstantTimeCompare([]byte(pin), []byte(i.pairingPIN)) == 1
}

// ActivePairingPIN returns the current pairing code only while it can still
// be consumed.
func (i *Identity) ActivePairingPIN() (string, time.Time, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.pairingPIN == "" || i.pairingAttempts >= 20 || !time.Now().Before(i.pairingExpires) {
		return "", time.Time{}, false
	}
	return i.pairingPIN, i.pairingExpires, true
}

// ConsumePairingPIN atomically accepts a valid code once.
func (i *Identity) ConsumePairingPIN(pin string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.pairingPIN == "" || time.Now().After(i.pairingExpires) || i.pairingAttempts >= 20 {
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
	cert, err := validatePeerCertificate(id, certificatePEM)
	if err != nil {
		return err
	}
	fingerprint := certificateFingerprint(cert)
	i.mu.Lock()
	defer i.mu.Unlock()
	peer := i.trusted[id]
	peer.Fingerprint = fingerprint
	peer.Certificate = certificatePEM
	i.trusted[id] = peer
	return i.persistLocked()
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
	defer i.mu.Unlock()
	peer := i.trusted[id]
	peer.Fingerprint = certificateFingerprint(cert)
	peer.Certificate = certificatePEM
	if role != "" {
		peer.Role = role
	}
	if address != "" {
		peer.Address = address
	}
	i.trusted[id] = peer
	return i.persistLocked()
}

// PairedOrchestratorAddresses returns persisted HTTPS addresses for paired
// orchestrators.
func (i *Identity) PairedOrchestratorAddresses() map[string]string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	addresses := make(map[string]string)
	for id, peer := range i.trusted {
		if peer.Role == "orchestrator" && peer.Address != "" {
			addresses[id] = peer.Address
		}
	}
	return addresses
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
	return cert, nil
}

// IsTrustedID reports whether the identity was explicitly paired.
func (i *Identity) IsTrustedID(id string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	_, ok := i.trusted[id]
	return ok
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
	defer i.mu.Unlock()
	delete(i.trusted, id)
	return i.persistLocked()
}

// IsTrustedCertificate validates a peer against the persisted certificate pin.
func (i *Identity) IsTrustedCertificate(cert *x509.Certificate) bool {
	if cert == nil {
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
		next.ServeHTTP(w, r)
	})
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
	if err := os.WriteFile(i.path, raw, 0o600); err != nil {
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

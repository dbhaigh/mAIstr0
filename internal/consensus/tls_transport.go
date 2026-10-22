package consensus

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

const (
	handshakeVersion  = 1
	handshakeAccepted = 1
	handshakeRejected = 2
	maxHandshakeID    = 256
)

var handshakeMagic = [4]byte{'M', 'S', 'T', 'R'}

type PeerIDResolver func(raft.ServerAddress) (string, bool)
type PeerCertificateValidator func(id string, certificate *x509.Certificate) error

type TLSStreamConfig struct {
	Listener         net.Listener
	ClusterID        string
	LocalID          string
	ServerTLS        *tls.Config
	ClientTLS        func(raft.ServerAddress) (*tls.Config, error)
	ResolvePeerID    PeerIDResolver
	ValidatePeer     PeerCertificateValidator
	HandshakeTimeout time.Duration
}

// TLSStreamLayer provides Raft's stream transport with mutual TLS, pinned
// peer-identity callbacks, and a cluster-ID handshake before Raft RPC bytes.
type TLSStreamLayer struct {
	listener         net.Listener
	clusterID        string
	localID          string
	serverTLS        *tls.Config
	clientTLS        func(raft.ServerAddress) (*tls.Config, error)
	resolvePeerID    PeerIDResolver
	validatePeer     PeerCertificateValidator
	handshakeTimeout time.Duration
}

func NewTLSStreamLayer(cfg TLSStreamConfig) (*TLSStreamLayer, error) {
	if cfg.Listener == nil || cfg.ServerTLS == nil || cfg.ClientTLS == nil ||
		cfg.ResolvePeerID == nil || cfg.ValidatePeer == nil {
		return nil, errors.New("consensus: TLS stream requires listener, TLS configs, peer resolver, and certificate validator")
	}
	if err := validateIdentifier("cluster ID", cfg.ClusterID); err != nil {
		return nil, err
	}
	if err := validateIdentifier("node ID", cfg.LocalID); err != nil {
		return nil, err
	}
	serverTLS := cfg.ServerTLS.Clone()
	if len(serverTLS.Certificates) == 0 {
		return nil, errors.New("consensus: TLS stream server certificate is required")
	}
	serverID, err := certificateID(serverTLS.Certificates[0])
	if err != nil {
		return nil, err
	}
	if serverID != cfg.LocalID {
		return nil, fmt.Errorf("consensus: TLS server certificate belongs to %q, not %q", serverID, cfg.LocalID)
	}
	if serverTLS.MinVersion == 0 || serverTLS.MinVersion < tls.VersionTLS13 {
		serverTLS.MinVersion = tls.VersionTLS13
	}
	serverTLS.ClientAuth = tls.RequireAnyClientCert
	return &TLSStreamLayer{
		listener:         cfg.Listener,
		clusterID:        cfg.ClusterID,
		localID:          cfg.LocalID,
		serverTLS:        serverTLS,
		clientTLS:        cfg.ClientTLS,
		resolvePeerID:    cfg.ResolvePeerID,
		validatePeer:     cfg.ValidatePeer,
		handshakeTimeout: durationOr(cfg.HandshakeTimeout, 3*time.Second),
	}, nil
}

func (l *TLSStreamLayer) Accept() (net.Conn, error) {
	for {
		raw, err := l.listener.Accept()
		if err != nil {
			return nil, err
		}
		conn := tls.Server(raw, l.serverTLS)
		_ = conn.SetDeadline(time.Now().Add(l.handshakeTimeout))
		if err := conn.Handshake(); err != nil {
			_ = conn.Close()
			continue
		}
		peerID, clusterID, err := readHandshake(conn)
		if err != nil {
			_ = conn.Close()
			continue
		}
		if subtle.ConstantTimeCompare([]byte(clusterID), []byte(l.clusterID)) != 1 {
			_ = writeHandshakeStatus(conn, handshakeRejected)
			_ = conn.Close()
			continue
		}
		state := conn.ConnectionState()
		if len(state.PeerCertificates) == 0 ||
			state.PeerCertificates[0].Subject.CommonName != peerID ||
			l.validatePeer(peerID, state.PeerCertificates[0]) != nil {
			_ = writeHandshakeStatus(conn, handshakeRejected)
			_ = conn.Close()
			continue
		}
		if err := writeHandshakeStatus(conn, handshakeAccepted); err != nil {
			_ = conn.Close()
			continue
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			_ = conn.Close()
			continue
		}
		return conn, nil
	}
}

func (l *TLSStreamLayer) Close() error   { return l.listener.Close() }
func (l *TLSStreamLayer) Addr() net.Addr { return l.listener.Addr() }

func (l *TLSStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if timeout <= 0 {
		timeout = l.handshakeTimeout
	}
	peerID, ok := l.resolvePeerID(address)
	if !ok {
		return nil, fmt.Errorf("consensus: no identity is known for Raft address %q", address)
	}
	if err := validateIdentifier("peer ID", peerID); err != nil {
		return nil, err
	}
	clientTLS, err := l.clientTLS(address)
	if err != nil {
		return nil, fmt.Errorf("consensus: configure TLS for peer %q: %w", peerID, err)
	}
	if clientTLS == nil || len(clientTLS.Certificates) == 0 {
		return nil, errors.New("consensus: TLS stream client certificate is required")
	}
	clientTLS = clientTLS.Clone()
	if clientTLS.MinVersion == 0 || clientTLS.MinVersion < tls.VersionTLS13 {
		clientTLS.MinVersion = tls.VersionTLS13
	}
	if clientTLS.InsecureSkipVerify && clientTLS.VerifyConnection == nil && clientTLS.VerifyPeerCertificate == nil {
		return nil, errors.New("consensus: insecure TLS client must verify the pinned peer certificate")
	}
	previousVerify := clientTLS.VerifyConnection
	clientTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || state.PeerCertificates[0].Subject.CommonName != peerID {
			return fmt.Errorf("consensus: TLS peer certificate does not identify %q", peerID)
		}
		if previousVerify != nil {
			return previousVerify(state)
		}
		return nil
	}
	localID, err := certificateID(clientTLS.Certificates[0])
	if err != nil {
		return nil, err
	}
	if localID != l.localID {
		return nil, fmt.Errorf("consensus: TLS client certificate belongs to %q, not %q", localID, l.localID)
	}
	raw, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, fmt.Errorf("consensus: dial Raft peer %q: %w", peerID, err)
	}
	conn := tls.Client(raw, clientTLS)
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.Handshake(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("consensus: TLS handshake with %q: %w", peerID, err)
	}
	if err := writeHandshake(conn, l.clusterID, l.localID); err != nil {
		_ = conn.Close()
		return nil, err
	}
	status, err := readHandshakeStatus(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if status != handshakeAccepted {
		_ = conn.Close()
		return nil, fmt.Errorf("consensus: peer %q rejected the cluster or identity handshake", peerID)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func NewNetworkTransport(layer *TLSStreamLayer, maxPool int, timeout time.Duration) *raft.NetworkTransport {
	return raft.NewNetworkTransport(layer, maxPool, timeout, io.Discard)
}

func writeHandshake(writer io.Writer, clusterID, peerID string) error {
	if len(clusterID) > maxHandshakeID || len(peerID) > maxHandshakeID {
		return errors.New("consensus: cluster or node ID exceeds transport handshake limit")
	}
	header := make([]byte, 9)
	copy(header[:4], handshakeMagic[:])
	header[4] = handshakeVersion
	binary.BigEndian.PutUint16(header[5:7], uint16(len(clusterID)))
	binary.BigEndian.PutUint16(header[7:9], uint16(len(peerID)))
	if err := writeFull(writer, header); err != nil {
		return fmt.Errorf("consensus: write transport handshake: %w", err)
	}
	if err := writeFull(writer, []byte(clusterID)); err != nil {
		return fmt.Errorf("consensus: write cluster ID: %w", err)
	}
	if err := writeFull(writer, []byte(peerID)); err != nil {
		return fmt.Errorf("consensus: write node ID: %w", err)
	}
	return nil
}

func readHandshake(reader io.Reader) (peerID, clusterID string, err error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		return "", "", fmt.Errorf("consensus: read transport handshake: %w", err)
	}
	if string(header[:4]) != string(handshakeMagic[:]) || header[4] != handshakeVersion {
		return "", "", errors.New("consensus: invalid transport handshake")
	}
	clusterLength := int(binary.BigEndian.Uint16(header[5:7]))
	peerLength := int(binary.BigEndian.Uint16(header[7:9]))
	if clusterLength == 0 || peerLength == 0 || clusterLength > maxHandshakeID || peerLength > maxHandshakeID {
		return "", "", errors.New("consensus: invalid transport identity length")
	}
	identities := make([]byte, clusterLength+peerLength)
	if _, err := io.ReadFull(reader, identities); err != nil {
		return "", "", fmt.Errorf("consensus: read transport identities: %w", err)
	}
	return string(identities[clusterLength:]), string(identities[:clusterLength]), nil
}

func writeHandshakeStatus(writer io.Writer, status byte) error {
	return writeFull(writer, []byte{status})
}

func readHandshakeStatus(reader io.Reader) (byte, error) {
	var status [1]byte
	if _, err := io.ReadFull(reader, status[:]); err != nil {
		return 0, fmt.Errorf("consensus: read transport handshake response: %w", err)
	}
	if status[0] != handshakeAccepted && status[0] != handshakeRejected {
		return 0, errors.New("consensus: invalid transport handshake response")
	}
	return status[0], nil
}

func writeFull(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}

func certificateID(certificate tls.Certificate) (string, error) {
	if certificate.Leaf != nil {
		return certificate.Leaf.Subject.CommonName, nil
	}
	if len(certificate.Certificate) == 0 {
		return "", errors.New("consensus: TLS certificate is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return "", fmt.Errorf("consensus: parse TLS certificate: %w", err)
	}
	return leaf.Subject.CommonName, nil
}

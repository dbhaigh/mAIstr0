package consensus

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestTLSStreamLayerAuthenticatesClusterAndPeer(t *testing.T) {
	serverIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "server.json"), "raft-server")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "client.json"), "raft-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := serverIdentity.TrustPeer(clientIdentity.ID(), clientIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}

	serverListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverLayer, err := newIdentityTLSLayer(t, serverListener, "cluster-a", serverIdentity, clientIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverLayer.Close() })

	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clientLayer, err := newIdentityTLSLayer(t, clientListener, "cluster-a", clientIdentity, serverIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientLayer.Close() })

	accepted := make(chan net.Conn, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		conn, err := serverLayer.Accept()
		if err != nil {
			acceptErrors <- err
			return
		}
		accepted <- conn
	}()
	clientConn, err := clientLayer.Dial(raft.ServerAddress(serverLayer.Addr().String()), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()
	select {
	case serverConn := <-accepted:
		defer serverConn.Close()
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept the authenticated stream")
	}
}

func TestTLSStreamLayerRejectsDifferentCluster(t *testing.T) {
	serverIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "server.json"), "raft-server")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "client.json"), "raft-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := serverIdentity.TrustPeer(clientIdentity.ID(), clientIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	serverListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverLayer, err := newIdentityTLSLayer(t, serverListener, "cluster-a", serverIdentity, clientIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer serverLayer.Close()
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clientLayer, err := newIdentityTLSLayer(t, clientListener, "cluster-b", clientIdentity, serverIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientLayer.Close()

	accepted := make(chan error, 1)
	go func() {
		_, err := serverLayer.Accept()
		accepted <- err
	}()
	conn, err := clientLayer.Dial(raft.ServerAddress(serverLayer.Addr().String()), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("stream from a different cluster was accepted")
	}
	_ = serverLayer.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("server accept did not stop after listener close")
	}
}

func newIdentityTLSLayer(
	t *testing.T,
	listener net.Listener,
	clusterID string,
	identity *peeridentity.Identity,
	remoteID string,
) (*TLSStreamLayer, error) {
	t.Helper()
	serverTLS := identity.ServerTLSConfig()
	serverTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || !identity.IsTrustedCertificate(state.PeerCertificates[0]) {
			return errors.New("peer certificate is not pinned")
		}
		return nil
	}
	clientTransport := identity.ClientTransport()
	clientTLS := clientTransport.TLSClientConfig.Clone()
	clientTransport.CloseIdleConnections()
	return NewTLSStreamLayer(TLSStreamConfig{
		Listener:  listener,
		ClusterID: clusterID,
		LocalID:   identity.ID(),
		ServerTLS: serverTLS,
		ClientTLS: func(raft.ServerAddress) (*tls.Config, error) {
			return clientTLS.Clone(), nil
		},
		ResolvePeerID: func(raft.ServerAddress) (string, bool) {
			return remoteID, true
		},
		ValidatePeer: func(id string, certificate *x509.Certificate) error {
			if id != remoteID || !identity.IsTrustedCertificate(certificate) {
				return errors.New("peer identity is not pinned")
			}
			return nil
		},
		HandshakeTimeout: time.Second,
	})
}

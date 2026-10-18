package consensus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrClusterIdentityRequired = errors.New("consensus: a cluster identity is required to join")
	ErrClusterIdentityMismatch = errors.New("consensus: persisted cluster identity does not match")
)

type ClusterIdentity struct {
	ClusterID string `json:"cluster_id"`
	NodeID    string `json:"node_id"`
}

// LoadOrCreateIdentity loads this node's persistent consensus identity. A new
// cluster identity is generated only when bootstrap is explicitly requested.
func LoadOrCreateIdentity(path, nodeID, clusterID string, bootstrap bool) (ClusterIdentity, error) {
	if strings.TrimSpace(path) == "" {
		return ClusterIdentity{}, errors.New("consensus: identity path is required")
	}
	if err := validateIdentifier("node ID", nodeID); err != nil {
		return ClusterIdentity{}, err
	}
	if clusterID != "" {
		if err := validateIdentifier("cluster ID", clusterID); err != nil {
			return ClusterIdentity{}, err
		}
	}

	identity, err := readClusterIdentity(path)
	if err == nil {
		if identity.NodeID != nodeID {
			return ClusterIdentity{}, fmt.Errorf("consensus: identity file belongs to node %q, not %q", identity.NodeID, nodeID)
		}
		if clusterID != "" && identity.ClusterID != clusterID {
			return ClusterIdentity{}, fmt.Errorf("%w: persisted %q, requested %q", ErrClusterIdentityMismatch, identity.ClusterID, clusterID)
		}
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return ClusterIdentity{}, err
	}
	if clusterID == "" && !bootstrap {
		return ClusterIdentity{}, ErrClusterIdentityRequired
	}
	if clusterID == "" {
		clusterID, err = newClusterID()
		if err != nil {
			return ClusterIdentity{}, err
		}
	}

	identity = ClusterIdentity{ClusterID: clusterID, NodeID: nodeID}
	raw, err := json.Marshal(identity)
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("consensus: encode cluster identity: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return ClusterIdentity{}, fmt.Errorf("consensus: create identity directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return LoadOrCreateIdentity(path, nodeID, clusterID, false)
	}
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("consensus: create identity file: %w", err)
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return ClusterIdentity{}, fmt.Errorf("consensus: write identity file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return ClusterIdentity{}, fmt.Errorf("consensus: sync identity file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return ClusterIdentity{}, fmt.Errorf("consensus: close identity file: %w", err)
	}
	return identity, nil
}

func readClusterIdentity(path string) (ClusterIdentity, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("consensus: read identity file: %w", err)
	}
	var identity ClusterIdentity
	if err := json.Unmarshal(raw, &identity); err != nil {
		return ClusterIdentity{}, fmt.Errorf("consensus: decode identity file: %w", err)
	}
	if err := validateIdentifier("cluster ID", identity.ClusterID); err != nil {
		return ClusterIdentity{}, err
	}
	if err := validateIdentifier("node ID", identity.NodeID); err != nil {
		return ClusterIdentity{}, err
	}
	return identity, nil
}

func validateIdentifier(label, value string) error {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
		return fmt.Errorf("consensus: %s must be non-empty, trimmed, and at most 256 bytes", label)
	}
	return nil
}

func newClusterID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("consensus: generate cluster ID: %w", err)
	}
	return "mstr-" + hex.EncodeToString(random[:]), nil
}

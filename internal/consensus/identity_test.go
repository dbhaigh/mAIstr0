package consensus

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateIdentityPersistsClusterAndNodeIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster-identity.json")
	first, err := LoadOrCreateIdentity(path, "node-a", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.ClusterID == "" || first.NodeID != "node-a" {
		t.Fatalf("created identity = %+v", first)
	}

	loaded, err := LoadOrCreateIdentity(path, "node-a", first.ClusterID, false)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != first {
		t.Fatalf("reloaded identity = %+v, want %+v", loaded, first)
	}
	if _, err := LoadOrCreateIdentity(path, "node-b", first.ClusterID, false); err == nil {
		t.Fatal("identity opened with a different node ID")
	}
	if _, err := LoadOrCreateIdentity(path, "node-a", "different-cluster", false); !errors.Is(err, ErrClusterIdentityMismatch) {
		t.Fatalf("cluster mismatch error = %v, want %v", err, ErrClusterIdentityMismatch)
	}
}

func TestLoadOrCreateIdentityRequiresBootstrapOrClusterID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster-identity.json")
	if _, err := LoadOrCreateIdentity(path, "node-a", "", false); !errors.Is(err, ErrClusterIdentityRequired) {
		t.Fatalf("uninitialized join error = %v, want %v", err, ErrClusterIdentityRequired)
	}
	joined, err := LoadOrCreateIdentity(path, "node-a", "existing-cluster", false)
	if err != nil {
		t.Fatal(err)
	}
	if joined.ClusterID != "existing-cluster" {
		t.Fatalf("joined cluster ID = %q", joined.ClusterID)
	}
}

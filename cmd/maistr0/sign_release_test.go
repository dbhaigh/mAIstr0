package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/selfupdate"
)

func TestSignReleaseCommandSignsManifestAndPrintsOnlyPublicKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	keyPath := filepath.Join(root, "publisher.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "maistr0.peer.json")
	manifest := selfupdate.Manifest{
		Version: "1.2.3", OS: "windows", Arch: "amd64",
		SHA256: strings.Repeat("a", 64), Size: 1024,
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var output, errorOutput bytes.Buffer
	if err := runSignRelease([]string{"--manifest", manifestPath, "--key", keyPath}, &output, &errorOutput); err != nil {
		t.Fatalf("runSignRelease: %v; stderr: %s", err, errorOutput.String())
	}
	if !strings.Contains(output.String(), base64.StdEncoding.EncodeToString(publicKey)) {
		t.Fatalf("output omitted trusted publisher key: %q", output.String())
	}
	if bytes.Contains(output.Bytes(), privateKey) {
		t.Fatal("command output included private key bytes")
	}
	signedData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var signed selfupdate.Manifest
	if err := json.Unmarshal(signedData, &signed); err != nil {
		t.Fatal(err)
	}
	if err := signed.VerifySignature(publicKey); err != nil {
		t.Fatalf("written signature is invalid: %v", err)
	}
}

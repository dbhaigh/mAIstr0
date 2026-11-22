package selfupdate

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

func (m Manifest) signingBytes() ([]byte, error) {
	m.Signature = ""
	return json.Marshal(m)
}

func (m *Manifest) Sign(privateKey ed25519.PrivateKey) error {
	if len(privateKey) != ed25519.PrivateKeySize {
		return errors.New("invalid Ed25519 peer update private key")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	payload, err := m.signingBytes()
	if err != nil {
		return fmt.Errorf("encode peer update manifest for signing: %w", err)
	}
	m.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func (m Manifest) VerifySignature(publicKey ed25519.PublicKey) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("invalid trusted Ed25519 publisher key")
	}
	signature, err := base64.StdEncoding.DecodeString(m.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("peer update manifest signature is missing or malformed")
	}
	payload, err := m.signingBytes()
	if err != nil {
		return fmt.Errorf("encode peer update manifest for verification: %w", err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("peer update manifest signature verification failed")
	}
	return nil
}

func (catalog ReleaseCatalog) signingBytes() ([]byte, error) {
	catalog.Signature = ""
	return json.Marshal(catalog)
}

func (catalog *ReleaseCatalog) Sign(privateKey ed25519.PrivateKey) error {
	if len(privateKey) != ed25519.PrivateKeySize {
		return errors.New("invalid Ed25519 central update signing key")
	}
	if err := catalog.Validate(); err != nil {
		return err
	}
	payload, err := catalog.signingBytes()
	if err != nil {
		return fmt.Errorf("encode central update catalog for signing: %w", err)
	}
	catalog.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func (catalog ReleaseCatalog) VerifySignature(publicKey ed25519.PublicKey) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("invalid trusted Ed25519 central update publisher key")
	}
	if err := catalog.Validate(); err != nil {
		return err
	}
	signature, err := decodeSignature(catalog.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("central update catalog signature is missing or malformed")
	}
	payload, err := catalog.signingBytes()
	if err != nil {
		return fmt.Errorf("encode central update catalog for verification: %w", err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("central update catalog signature verification failed")
	}
	return nil
}

func decodeSignature(value string) ([]byte, error) {
	signature, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("central update signature is malformed")
	}
	return signature, nil
}

func sameArtifact(left, right Manifest) bool {
	return left.Version == right.Version &&
		left.OS == right.OS &&
		left.Arch == right.Arch &&
		left.SHA256 == right.SHA256 &&
		left.Size == right.Size
}

func SignManifestFile(manifestPath, privateKeyPath string) (ed25519.PublicKey, error) {
	privateKey, err := loadSigningKey(privateKeyPath, "peer update")
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read peer update manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode peer update manifest: %w", err)
	}
	if err := manifest.Sign(privateKey); err != nil {
		return nil, fmt.Errorf("sign peer update manifest: %w", err)
	}
	signed, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode signed peer update manifest: %w", err)
	}
	if err := os.WriteFile(manifestPath, append(signed, '\n'), 0o600); err != nil {
		return nil, fmt.Errorf("write signed peer update manifest: %w", err)
	}
	return privateKey.Public().(ed25519.PublicKey), nil
}

func SignReleaseCatalogFile(catalogPath, privateKeyPath string) (ed25519.PublicKey, error) {
	privateKey, err := loadSigningKey(privateKeyPath, "central update")
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("read central update catalog: %w", err)
	}
	var catalog ReleaseCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("decode central update catalog: %w", err)
	}
	if err := catalog.Sign(privateKey); err != nil {
		return nil, fmt.Errorf("sign central update catalog: %w", err)
	}
	signed, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode signed central update catalog: %w", err)
	}
	if err := os.WriteFile(catalogPath, append(signed, '\n'), 0o600); err != nil {
		return nil, fmt.Errorf("write signed central update catalog: %w", err)
	}
	return privateKey.Public().(ed25519.PublicKey), nil
}

func loadSigningKey(privateKeyPath, purpose string) (ed25519.PrivateKey, error) {
	keyData, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read %s signing key: %w", purpose, err)
	}
	block, _ := pem.Decode(keyData)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s signing key must be an unencrypted PKCS#8 PEM Ed25519 key", purpose)
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s signing key as PKCS#8 Ed25519", purpose)
	}
	privateKey, ok := parsedKey.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s signing key is not Ed25519", purpose)
	}
	return privateKey, nil
}

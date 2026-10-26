package peeridentity

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"errors"
)

// ValidateFingerprint accepts a SHA-256 certificate fingerprint in hex form.
func ValidateFingerprint(value string) error {
	if len(value) != 64 {
		return errors.New("certificate fingerprint must contain exactly 64 hexadecimal characters")
	}
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != 32 {
		return errors.New("certificate fingerprint must contain exactly 64 hexadecimal characters")
	}
	return nil
}

// CertificateFingerprint identifies the complete DER certificate, not its name.
func CertificateFingerprint(certificatePEM string) (string, error) {
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return "", err
	}
	return certificateFingerprint(certificate), nil
}

func ConnectionFingerprint(state *tls.ConnectionState) (string, error) {
	if state == nil || len(state.PeerCertificates) == 0 {
		return "", errors.New("TLS peer certificate is required")
	}
	return certificateFingerprint(state.PeerCertificates[0]), nil
}

// ValidatePeerConnection binds an enrollment record to the certificate whose
// private key was proven by the TLS handshake.
func ValidatePeerConnection(id, certificatePEM string, state *tls.ConnectionState) error {
	certificate, err := validatePeerCertificate(id, certificatePEM)
	if err != nil {
		return err
	}
	if state == nil || len(state.PeerCertificates) == 0 ||
		!bytes.Equal(certificate.Raw, state.PeerCertificates[0].Raw) {
		return errors.New("pairing certificate does not match the TLS peer")
	}
	return nil
}

package orchestrator

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInvitationFingerprintSecurityContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "contracts", "fixtures", "security", "invitation-requires-fingerprint.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Request struct {
			Method string          `json:"method"`
			Path   string          `json:"path"`
			Body   json.RawMessage `json:"body"`
		} `json:"request"`
		Response struct {
			Status      int    `json:"status"`
			ContentType string `json:"content_type"`
			Body        string `json:"body"`
		} `json:"expected_response"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	fixture := newTrustFixture(t)
	request := httptest.NewRequest(contract.Request.Method, contract.Request.Path, bytes.NewReader(contract.Request.Body))
	request.SetPathValue("id", "unpaired-node")
	response := httptest.NewRecorder()
	fixture.srv.handlePairingInvite(response, request)
	if response.Code != contract.Response.Status || response.Header().Get("Content-Type") != contract.Response.ContentType || response.Body.String() != contract.Response.Body {
		t.Fatalf("invitation security contract drift: %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	if !fixture.identity.VerifyPairingPIN(fixture.pin) || len(fixture.identity.TrustedIDs()) != 0 {
		t.Fatal("rejected invitation changed the PIN or trust")
	}
}

func TestEnrollmentRejectsCertificateSubstitutionWithoutConsumingPIN(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{true: "no client certificate", false: "different key"}[missing], func(t *testing.T) {
			fixture := newTrustFixture(t)
			claimed, presenter := trustNode(t, "claim"), trustNode(t, "presenter")
			body, err := json.Marshal(pairingJoinRequest{ID: claimed.ID(), Role: "node", PIN: fixture.pin, CertificatePEM: claimed.CertificatePEM()})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
			request.TLS = trustTLS(t, presenter)
			if missing {
				request.TLS = &tls.ConnectionState{}
			}
			rec := httptest.NewRecorder()
			fixture.srv.Mux().ServeHTTP(rec, request)
			if rec.Code != http.StatusForbidden || fixture.identity.IsTrustedID(claimed.ID()) || !fixture.identity.VerifyPairingPIN(fixture.pin) {
				t.Fatalf("invalid certificate enrollment changed trust/PIN: %d %s", rec.Code, rec.Body.String())
			}
			valid := trustJoin(t, fixture.srv.Mux(), pairingJoinRequest{ID: claimed.ID(), Role: "node", PIN: fixture.pin, CertificatePEM: claimed.CertificatePEM()})
			if valid.Code != http.StatusOK {
				t.Fatalf("authorized enrollment failed after rejection: %d %s", valid.Code, valid.Body.String())
			}
		})
	}
}

func TestRemotePairingStatusNeverExposesPIN(t *testing.T) {
	fixture := newTrustFixture(t)
	for _, remote := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil)
		if remote {
			request.TLS = trustTLS(t, trustNode(t, "reader"))
		}
		rec := httptest.NewRecorder()
		fixture.srv.Mux().ServeHTTP(rec, request)
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), fixture.pin) == remote {
			t.Fatalf("PIN visibility mismatch for remote=%v: %d %s", remote, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"fingerprint"`) {
			t.Fatal("operator cannot obtain the certificate fingerprint")
		}
	}
}

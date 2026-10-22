package memory

import (
	"bytes"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestSyncProtocolNegotiatesCompatibilityFeatures(t *testing.T) {
	for _, test := range []struct {
		name        string
		peerVersion int
		wantVersion int
		pages       bool
		compression bool
	}{
		{name: "legacy", peerVersion: 0, wantVersion: FirstSyncProtocolVersion},
		{name: "version 2", peerVersion: 2, wantVersion: 2, pages: true},
		{name: "current", peerVersion: SyncProtocolVersion, wantVersion: SyncProtocolVersion, pages: true, compression: true},
		{name: "future", peerVersion: SyncProtocolVersion + 1, wantVersion: SyncProtocolVersion, pages: true, compression: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := newHeaderResponseWriter()
			if err := SetSyncResponseHeadersForPeer(response, map[string]uint64{"node": 4}, test.peerVersion); err != nil {
				t.Fatal(err)
			}
			version, watermarks, err := ReadSyncHeaders(response.Header())
			if err != nil {
				t.Fatal(err)
			}
			if version != test.wantVersion || !reflect.DeepEqual(watermarks, map[string]uint64{"node": 4}) {
				t.Fatalf("negotiated headers = version %d, watermarks %v", version, watermarks)
			}
			if got := SupportsSyncPages(version); got != test.pages {
				t.Fatalf("SupportsSyncPages(%d) = %t, want %t", version, got, test.pages)
			}
			if got := SupportsSyncCompression(version); got != test.compression {
				t.Fatalf("SupportsSyncCompression(%d) = %t, want %t", version, got, test.compression)
			}
		})
	}
}

func TestSyncBodyCompressionIsNegotiatedAndBounded(t *testing.T) {
	payload := []byte(strings.Repeat("sync-record-", SyncCompressionThreshold))
	legacy, compressed, err := EncodeSyncBody(payload, SyncPageProtocolVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	if compressed || !bytes.Equal(legacy, payload) {
		t.Fatal("version 2 peer must receive an uncompressed payload")
	}
	encoded, compressed, err := EncodeSyncBody(payload, SyncCompressionProtocolVersion, true)
	if err != nil {
		t.Fatal(err)
	}
	if !compressed || len(encoded) >= len(payload) {
		t.Fatalf("version 3 payload was not usefully compressed: compressed=%t, %d -> %d bytes", compressed, len(payload), len(encoded))
	}
	decoded, err := DecodeSyncBody(bytes.NewReader(encoded), "gzip", SyncCompressionProtocolVersion, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("gzip round trip changed sync payload")
	}
	if _, err := DecodeSyncBody(bytes.NewReader(encoded), "gzip", SyncPageProtocolVersion, int64(len(payload))); err == nil {
		t.Fatal("version 2 peer accepted a compressed payload")
	}
	if _, err := DecodeSyncBody(bytes.NewReader(encoded), "gzip", SyncCompressionProtocolVersion, int64(len(payload)-1)); !errors.Is(err, ErrSyncBodyTooLarge) {
		t.Fatalf("oversized decompressed payload error = %v", err)
	}
	unnegotiated, compressed, err := EncodeSyncBody(payload, SyncCompressionProtocolVersion, false)
	if err != nil || compressed || !bytes.Equal(unnegotiated, payload) {
		t.Fatalf("unnegotiated compression = %t, err %v", compressed, err)
	}
	if _, err := DecodeSyncBody(bytes.NewReader(payload), "br", SyncCompressionProtocolVersion, int64(len(payload))); err == nil {
		t.Fatal("unsupported content encoding was accepted")
	}
}

func TestAcceptsSyncCompressionHonorsQuality(t *testing.T) {
	for _, test := range []struct {
		header string
		want   bool
	}{
		{header: "gzip", want: true},
		{header: "br, GZIP", want: true},
		{header: "gzip;q=0.5", want: true},
		{header: "gzip;q=0", want: false},
		{header: "br", want: false},
	} {
		request, err := http.NewRequest(http.MethodGet, "https://example.test/sync", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Accept-Encoding", test.header)
		if got := AcceptsSyncCompression(request); got != test.want {
			t.Errorf("AcceptsSyncCompression(%q) = %t, want %t", test.header, got, test.want)
		}
	}
}

type headerResponseWriter struct {
	header http.Header
}

func newHeaderResponseWriter() *headerResponseWriter {
	return &headerResponseWriter{header: make(http.Header)}
}

func (w *headerResponseWriter) Header() http.Header {
	return w.header
}

func (*headerResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (*headerResponseWriter) WriteHeader(int) {}

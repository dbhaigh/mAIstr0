package supervisor

import (
	"net/http"
	"testing"
	"time"
)

func TestWorkerHTTPClientPreservesLegacyDownloadPolicy(t *testing.T) {
	client := workerHTTPClient()
	if client.Timeout != 10*time.Minute {
		t.Fatalf("worker download timeout = %s, want legacy 10m", client.Timeout)
	}
	if client.Transport != http.DefaultTransport {
		t.Fatal("worker client does not share the default transport")
	}
	for _, tt := range []struct {
		name    string
		url     string
		hops    int
		wantErr bool
	}{
		{"approved release", "https://release-assets.githubusercontent.com/file", 1, false},
		{"too many redirects", "https://github.com/file", 5, true},
		{"unapproved host", "https://example.com/file", 1, true},
		{"HTTP downgrade", "http://github.com/file", 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = client.CheckRedirect(request, make([]*http.Request, tt.hops))
			if (err != nil) != tt.wantErr {
				t.Fatalf("redirect policy error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

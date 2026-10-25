package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShardFixturesCoverRegisteredOperations(t *testing.T) {
	data, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	manifest, operations := parseOpenAPI(t, string(data))
	files, err := filepath.Glob(filepath.Join("fixtures", "shards", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	covered := make(map[string]bool)
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var fixture struct {
			Request struct {
				Method string `json:"method"`
				Path   string `json:"path"`
			} `json:"request"`
			Response struct {
				Status int `json:"status"`
			} `json:"expected_response"`
		}
		if err := json.Unmarshal(data, &fixture); err != nil {
			t.Fatal(err)
		}
		route := fixture.Request.Method + " " + fixture.Request.Path
		if _, ok := manifest["orchestrator|"+route]; !ok ||
			!operations[fixture.Request.Path][strings.ToLower(fixture.Request.Method)] {
			t.Errorf("%s refers to an undocumented or unregistered operation %q", path, route)
		}
		if fixture.Response.Status == 0 {
			t.Errorf("%s omits HTTP response status", path)
		}
		covered[route] = true
	}
	for route := range manifest {
		server, pattern, _ := strings.Cut(route, "|")
		if server == "orchestrator" && strings.Contains(pattern, " /api/shards") && !covered[pattern] {
			t.Errorf("sharding operation %q has no contract fixture", pattern)
		}
	}
}

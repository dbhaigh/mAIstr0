package cluster

import (
	"testing"

	"github.com/maistr0/maistr0/internal/engine"
)

func TestModelListingsPageFiltersAndContinuesByStablePosition(t *testing.T) {
	registry := NewRegistry()
	registry.Upsert(NodeStatus{
		ID: "node-b",
		Models: []engine.Model{
			{Name: "alpha", Engine: "ollama"},
			{Name: "delta", Engine: "ollama"},
		},
	})
	registry.Upsert(NodeStatus{
		ID: "node-a",
		Models: []engine.Model{
			{Name: "gamma", Engine: "ollama"},
			{Name: "other", Engine: "vllm"},
			{Name: "beta", Engine: "ollama"},
			{Name: "alpha", Engine: "ollama"},
		},
	})

	options := ModelListingPageOptions{Source: "NODE-A", Engine: "OLLAMA", Limit: 2}
	first, err := registry.ListModelListingsPage(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || !first.HasMore {
		t.Fatalf("first page = %+v, want two items and more", first)
	}
	if first.Items[0].Name != "alpha" || first.Items[1].Name != "beta" {
		t.Fatalf("first page names = %q, %q; want alpha, beta", first.Items[0].Name, first.Items[1].Name)
	}

	options.HasPosition = true
	options.Position = first.Next
	second, err := registry.ListModelListingsPage(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.HasMore || second.Items[0].Name != "gamma" {
		t.Fatalf("second page = %+v, want only gamma", second)
	}
}

func TestUniqueModelListingsDeduplicateAcrossPages(t *testing.T) {
	registry := NewRegistry()
	registry.Upsert(NodeStatus{
		ID: "node-b",
		Models: []engine.Model{
			{Name: "shared", Engine: "ollama"},
			{Name: "zeta", Engine: "ollama"},
		},
	})
	registry.Upsert(NodeStatus{
		ID: "node-a",
		Models: []engine.Model{
			{Name: "shared", Engine: "vllm"},
			{Name: "alpha", Engine: "ollama"},
			{Name: "shared", Engine: "ollama"},
		},
	})

	options := ModelListingPageOptions{Limit: 1, UniqueByName: true}
	first, err := registry.ListModelListingsPage(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || first.Items[0].Name != "alpha" || !first.HasMore {
		t.Fatalf("first unique page = %+v", first)
	}
	options.HasPosition = true
	options.Position = first.Next
	second, err := registry.ListModelListingsPage(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].Name != "shared" || !second.HasMore {
		t.Fatalf("second unique page = %+v", second)
	}
	if second.Items[0].NodeID != "node-a" || second.Items[0].Engine != "vllm" {
		t.Fatalf("duplicate winner = %+v, want node-a/vllm", second.Items[0])
	}
	options.Position = second.Next
	third, err := registry.ListModelListingsPage(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Items) != 1 || third.Items[0].Name != "zeta" || third.HasMore {
		t.Fatalf("third unique page = %+v", third)
	}
}

func TestModelListingsPageCopiesSelectedModelMetadata(t *testing.T) {
	registry := NewRegistry()
	registry.Upsert(NodeStatus{
		ID: "node-a",
		Models: []engine.Model{{
			Name: "alpha", Engine: "ollama", Tags: []string{"text"},
		}},
	})

	page, err := registry.ListModelListingsPage(ModelListingPageOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	registry.Upsert(NodeStatus{
		ID: "node-a",
		Models: []engine.Model{{
			Name: "alpha", Engine: "ollama", Tags: []string{"changed"},
		}},
	})
	if len(page.Items) != 1 || page.Items[0].Tags[0] != "text" {
		t.Fatalf("page metadata changed after registry update: %+v", page)
	}
}

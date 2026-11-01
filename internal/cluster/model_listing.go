package cluster

import (
	"fmt"
	"sort"
	"strings"
)

// Listing kinds keep node-backed models before synthetic shard models.
const (
	ModelListingNode = iota
	ModelListingShard
	ModelListingLogical
)

// ModelListing is one model exposed by a cluster model-list endpoint.
type ModelListing struct {
	NodeID        string
	NodeAddress   string
	Name          string
	Engine        string
	Tags          []string
	SizeGB        float64
	ContextLength int
	Members       []string
	Default       bool
	Healthy       bool
	Kind          int
	Index         int
}

// ModelListingPosition is the keyset position used to continue a model page.
type ModelListingPosition struct {
	Unique bool   `json:"unique,omitempty"`
	Kind   int    `json:"kind,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	Name   string `json:"name,omitempty"`
	Engine string `json:"engine,omitempty"`
	Index  int    `json:"index,omitempty"`
}

// ModelListingPageOptions bounds and filters one model-listing query.
type ModelListingPageOptions struct {
	Source       string
	Engine       string
	Limit        int
	UniqueByName bool
	HasPosition  bool
	Position     ModelListingPosition
	// Excluded applies to node-backed models; ExcludedAll also applies to additions.
	Excluded    []string
	ExcludedAll []string
	Additional  []ModelListing
}

// ModelListingPage is one bounded set of model-listing results.
type ModelListingPage struct {
	Items   []ModelListing
	Next    ModelListingPosition
	HasMore bool
}

// ListModelListingsPage scans the current active inventory but retains only
// the requested page, avoiding a full copy of every node's model list.
func (r *Registry) ListModelListingsPage(options ModelListingPageOptions) (ModelListingPage, error) {
	if options.Limit < 1 {
		return ModelListingPage{}, fmt.Errorf("model listing page limit must be positive")
	}

	excluded := make(map[string]struct{}, len(options.Excluded))
	for _, name := range options.Excluded {
		excluded[strings.ToLower(name)] = struct{}{}
	}
	excludedAll := make(map[string]struct{}, len(options.ExcludedAll))
	for _, name := range options.ExcludedAll {
		excludedAll[strings.ToLower(name)] = struct{}{}
	}
	selected := make([]ModelListing, 0, options.Limit+1)
	consider := func(candidate ModelListing) {
		if options.Source != "" && !strings.EqualFold(candidate.NodeID, options.Source) {
			return
		}
		if options.Engine != "" && !strings.EqualFold(candidate.Engine, options.Engine) {
			return
		}
		if _, skip := excludedAll[strings.ToLower(candidate.Name)]; skip {
			return
		}
		if candidate.Kind == ModelListingNode {
			if _, skip := excluded[strings.ToLower(candidate.Name)]; skip {
				return
			}
		}
		if options.HasPosition {
			if options.UniqueByName {
				if candidate.Name <= options.Position.Name {
					return
				}
			} else if compareModelListingPosition(candidate, options.Position) <= 0 {
				return
			}
		}

		if options.UniqueByName {
			for index := range selected {
				if selected[index].Name != candidate.Name {
					continue
				}
				if compareModelListings(candidate, selected[index], true) < 0 {
					selected[index] = candidate
				}
				return
			}
		}

		if len(selected) < options.Limit+1 {
			selected = append(selected, candidate)
			return
		}
		worst := 0
		for index := 1; index < len(selected); index++ {
			if compareModelListings(selected[index], selected[worst], options.UniqueByName) > 0 {
				worst = index
			}
		}
		if compareModelListings(candidate, selected[worst], options.UniqueByName) < 0 {
			selected[worst] = candidate
		}
	}

	r.mu.RLock()
	for _, node := range r.nodes {
		if !node.Healthy {
			continue
		}
		for index, model := range node.Models {
			consider(ModelListing{
				NodeID: node.ID, NodeAddress: node.Address,
				Name: model.Name, Engine: model.Engine, Tags: model.Tags,
				SizeGB: model.SizeGB, ContextLength: model.ContextLength,
				Default: model.Name == node.DefaultModel, Healthy: node.Healthy,
				Index: index,
			})
		}
	}
	for _, additional := range options.Additional {
		consider(additional)
	}
	r.mu.RUnlock()

	sort.Slice(selected, func(i, j int) bool {
		return compareModelListings(selected[i], selected[j], options.UniqueByName) < 0
	})
	hasMore := len(selected) > options.Limit
	if hasMore {
		selected = selected[:options.Limit]
	}
	for index := range selected {
		selected[index].Tags = cloneSlice(selected[index].Tags)
		selected[index].Members = cloneSlice(selected[index].Members)
	}

	page := ModelListingPage{Items: selected, HasMore: hasMore}
	if hasMore {
		page.Next = modelListingPosition(selected[len(selected)-1], options.UniqueByName)
	}
	return page, nil
}

func modelListingPosition(model ModelListing, unique bool) ModelListingPosition {
	return ModelListingPosition{
		Unique: unique, Kind: model.Kind, NodeID: model.NodeID,
		Name: model.Name, Engine: model.Engine, Index: model.Index,
	}
}

func compareModelListingPosition(model ModelListing, position ModelListingPosition) int {
	if model.Kind != position.Kind {
		return compareInts(model.Kind, position.Kind)
	}
	if result := strings.Compare(model.NodeID, position.NodeID); result != 0 {
		return result
	}
	if result := strings.Compare(model.Name, position.Name); result != 0 {
		return result
	}
	if result := strings.Compare(model.Engine, position.Engine); result != 0 {
		return result
	}
	return compareInts(model.Index, position.Index)
}

func compareModelListings(left, right ModelListing, unique bool) int {
	if unique {
		if result := strings.Compare(left.Name, right.Name); result != 0 {
			return result
		}
		if left.Kind != right.Kind {
			return compareInts(left.Kind, right.Kind)
		}
		if result := strings.Compare(left.NodeID, right.NodeID); result != 0 {
			return result
		}
		if left.Index != right.Index {
			return compareInts(left.Index, right.Index)
		}
		return strings.Compare(left.Engine, right.Engine)
	} else if left.Kind != right.Kind {
		return compareInts(left.Kind, right.Kind)
	}
	if result := strings.Compare(left.NodeID, right.NodeID); result != 0 {
		return result
	}
	if result := strings.Compare(left.Name, right.Name); result != 0 {
		return result
	}
	if result := strings.Compare(left.Engine, right.Engine); result != 0 {
		return result
	}
	return compareInts(left.Index, right.Index)
}

func compareInts(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

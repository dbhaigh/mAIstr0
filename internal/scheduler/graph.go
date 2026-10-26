package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Edge represents a directed dependency where From must complete before To can run.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ParallelGroup represents a group of subtasks intended to execute concurrently.
type ParallelGroup struct {
	ID       string    `json:"id,omitempty"`
	Subtasks []Subtask `json:"subtasks"`
}

// Chain represents a sequence of subtasks that execute sequentially.
type Chain struct {
	ID       string    `json:"id,omitempty"`
	Subtasks []Subtask `json:"subtasks"`
}

// TaskGraph is a directed acyclic graph (DAG) representing subtasks (nodes) and dependencies (edges).
type TaskGraph struct {
	Nodes          map[string]Subtask `json:"nodes"`
	Edges          []Edge             `json:"edges"`
	ParallelGroups []ParallelGroup    `json:"parallel_groups,omitempty"`
	Chains         []Chain            `json:"chains,omitempty"`
}

// NewTaskGraph creates an empty TaskGraph.
func NewTaskGraph() *TaskGraph {
	return &TaskGraph{
		Nodes:          make(map[string]Subtask),
		Edges:          make([]Edge, 0),
		ParallelGroups: make([]ParallelGroup, 0),
		Chains:         make([]Chain, 0),
	}
}

// AddNode adds a subtask node to the graph.
func (g *TaskGraph) AddNode(subtask Subtask) error {
	if g.Nodes == nil {
		g.Nodes = make(map[string]Subtask)
	}
	id := strings.TrimSpace(subtask.ID)
	if id == "" {
		return errors.New("scheduler: node id cannot be empty")
	}
	subtask.ID = id
	if _, exists := g.Nodes[id]; exists {
		return fmt.Errorf("scheduler: duplicate node id %s", id)
	}
	g.Nodes[id] = subtask
	return nil
}

// AddEdge adds a directed dependency edge from prerequisite 'from' to dependent 'to'.
func (g *TaskGraph) AddEdge(from, to string) error {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" || to == "" {
		return errors.New("scheduler: edge endpoints cannot be empty")
	}
	if from == to {
		return fmt.Errorf("scheduler: self-dependency not allowed on node %s", from)
	}
	// Avoid duplicate edges
	for _, e := range g.Edges {
		if e.From == from && e.To == to {
			return nil
		}
	}
	g.Edges = append(g.Edges, Edge{From: from, To: to})
	// Sync with DependsOn in the target node if present
	if target, ok := g.Nodes[to]; ok {
		if !containsString(target.DependsOn, from) {
			target.DependsOn = append(target.DependsOn, from)
			g.Nodes[to] = target
		}
	}
	return nil
}

// AddChain adds a sequential chain of subtasks: subtasks[0] -> subtasks[1] -> ... -> subtasks[n-1].
func (g *TaskGraph) AddChain(subtasks ...Subtask) ([]Subtask, error) {
	if len(subtasks) == 0 {
		return nil, nil
	}
	added := make([]Subtask, 0, len(subtasks))
	for i := range subtasks {
		st := subtasks[i]
		if st.ID == "" {
			st.ID = idFor(len(g.Nodes))
		}
		if i > 0 {
			prev := subtasks[i-1].ID
			if !containsString(st.DependsOn, prev) {
				st.DependsOn = append(st.DependsOn, prev)
			}
		}
		if err := g.AddNode(st); err != nil {
			return nil, err
		}
		if i > 0 {
			if err := g.AddEdge(subtasks[i-1].ID, st.ID); err != nil {
				return nil, err
			}
		}
		added = append(added, st)
	}
	g.Chains = append(g.Chains, Chain{Subtasks: added})
	return added, nil
}

// AddParallelGroup adds an explicit group of subtasks with no dependencies between each other.
func (g *TaskGraph) AddParallelGroup(subtasks ...Subtask) ([]Subtask, error) {
	if len(subtasks) == 0 {
		return nil, nil
	}
	added := make([]Subtask, 0, len(subtasks))
	for i := range subtasks {
		st := subtasks[i]
		if st.ID == "" {
			st.ID = idFor(len(g.Nodes))
		}
		if err := g.AddNode(st); err != nil {
			return nil, err
		}
		added = append(added, st)
	}
	g.ParallelGroups = append(g.ParallelGroups, ParallelGroup{Subtasks: added})
	return added, nil
}

// Dependencies returns all direct prerequisites for the given node.
func (g *TaskGraph) Dependencies(nodeID string) []string {
	deps := make([]string, 0)
	for _, e := range g.Edges {
		if e.To == nodeID {
			deps = append(deps, e.From)
		}
	}
	return deps
}

// InDegree returns the number of prerequisites that must complete before this node can run.
func (g *TaskGraph) InDegree(nodeID string) int {
	count := 0
	for _, e := range g.Edges {
		if e.To == nodeID {
			count++
		}
	}
	return count
}

// OutDegree returns the number of dependent nodes waiting on this node.
func (g *TaskGraph) OutDegree(nodeID string) int {
	count := 0
	for _, e := range g.Edges {
		if e.From == nodeID {
			count++
		}
	}
	return count
}

// ReadyNodes returns all subtasks that are ready to run next (prerequisites are satisfied and not yet completed).
func (g *TaskGraph) ReadyNodes(completed map[string]bool) []Subtask {
	var ready []Subtask
	for id, node := range g.Nodes {
		if completed[id] {
			continue
		}
		allPrereqsDone := true
		for _, dep := range g.Dependencies(id) {
			if !completed[dep] {
				allPrereqsDone = false
				break
			}
		}
		if allPrereqsDone {
			ready = append(ready, node)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].ID < ready[j].ID
	})
	return ready
}

// TaskGraphFromSubtasks converts a flat slice of Subtasks (using DependsOn) into a validated TaskGraph.
func TaskGraphFromSubtasks(subtasks []Subtask) (*TaskGraph, error) {
	normalized, err := ValidatePlan(subtasks)
	if err != nil {
		return nil, err
	}
	g := NewTaskGraph()
	for _, st := range normalized {
		if err := g.AddNode(st); err != nil {
			return nil, err
		}
	}
	for _, st := range normalized {
		for _, dep := range st.DependsOn {
			if err := g.AddEdge(dep, st.ID); err != nil {
				return nil, err
			}
		}
	}
	return g, nil
}

// ToSubtasks returns a topologically sorted slice of Subtasks with DependsOn correctly set.
func (g *TaskGraph) ToSubtasks() ([]Subtask, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g.TopologicalSort()
}

// Validate checks for node limits, missing references, self-dependencies, and cycles.
func (g *TaskGraph) Validate() error {
	if len(g.Nodes) == 0 {
		return errors.New("scheduler: plan must contain at least one subtask")
	}
	if len(g.Nodes) > 64 {
		return errors.New("scheduler: plan cannot contain more than 64 subtasks")
	}

	// Verify all edges connect existing nodes
	for _, e := range g.Edges {
		if e.From == e.To {
			return fmt.Errorf("scheduler: subtask %s cannot depend on itself", e.From)
		}
		if _, ok := g.Nodes[e.From]; !ok {
			return fmt.Errorf("scheduler: dependency edge refers to unknown prerequisite node %s", e.From)
		}
		if _, ok := g.Nodes[e.To]; !ok {
			return fmt.Errorf("scheduler: dependency edge refers to unknown dependent node %s", e.To)
		}
	}

	// Verify all DependsOn arrays connect existing nodes
	for id, node := range g.Nodes {
		for _, dep := range node.DependsOn {
			if dep == id {
				return fmt.Errorf("scheduler: subtask %s cannot depend on itself", id)
			}
			if _, ok := g.Nodes[dep]; !ok {
				return fmt.Errorf("scheduler: subtask %s depends on unknown subtask %s", id, dep)
			}
		}
	}

	// Cycle detection using 3-color DFS
	// 0: unvisited, 1: visiting, 2: visited
	color := make(map[string]int, len(g.Nodes))
	var visit func(id string) error
	visit = func(id string) error {
		switch color[id] {
		case 1:
			return fmt.Errorf("scheduler: plan contains a dependency cycle at %s", id)
		case 2:
			return nil
		}
		color[id] = 1
		// Check both Edges and DependsOn
		for _, dep := range g.Dependencies(id) {
			if err := visit(dep); err != nil {
				return err
			}
		}
		if node, ok := g.Nodes[id]; ok {
			for _, dep := range node.DependsOn {
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[id] = 2
		return nil
	}

	for id := range g.Nodes {
		if color[id] == 0 {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}

// TopologicalSort returns the subtasks sorted in dependency order.
func (g *TaskGraph) TopologicalSort() ([]Subtask, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int, len(g.Nodes))
	dependents := make(map[string][]string, len(g.Nodes))

	// Ensure all DependsOn are synchronized into edges
	for id, node := range g.Nodes {
		for _, dep := range node.DependsOn {
			_ = g.AddEdge(dep, id)
		}
	}

	for id := range g.Nodes {
		inDegree[id] = 0
	}
	for _, e := range g.Edges {
		inDegree[e.To]++
		dependents[e.From] = append(dependents[e.From], e.To)
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	result := make([]Subtask, 0, len(g.Nodes))
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		node := g.Nodes[curr]
		// Synchronize DependsOn
		node.DependsOn = uniqueStrings(g.Dependencies(curr))
		sort.Strings(node.DependsOn)
		result = append(result, node)

		for _, dep := range dependents[curr] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
				sort.Strings(queue)
			}
		}
	}

	if len(result) != len(g.Nodes) {
		return nil, errors.New("scheduler: plan contains a dependency cycle")
	}
	return result, nil
}

// Clone returns a deep copy of the TaskGraph.
func (g *TaskGraph) Clone() *TaskGraph {
	if g == nil {
		return nil
	}
	cloned := &TaskGraph{
		Nodes:          make(map[string]Subtask, len(g.Nodes)),
		Edges:          make([]Edge, len(g.Edges)),
		ParallelGroups: make([]ParallelGroup, len(g.ParallelGroups)),
		Chains:         make([]Chain, len(g.Chains)),
	}
	copy(cloned.Edges, g.Edges)
	for k, v := range g.Nodes {
		cloned.Nodes[k] = v.Clone()
	}
	for i, pg := range g.ParallelGroups {
		clonedPG := ParallelGroup{ID: pg.ID, Subtasks: make([]Subtask, len(pg.Subtasks))}
		for j, st := range pg.Subtasks {
			clonedPG.Subtasks[j] = st.Clone()
		}
		cloned.ParallelGroups[i] = clonedPG
	}
	for i, ch := range g.Chains {
		clonedCh := Chain{ID: ch.ID, Subtasks: make([]Subtask, len(ch.Subtasks))}
		for j, st := range ch.Subtasks {
			clonedCh.Subtasks[j] = st.Clone()
		}
		cloned.Chains[i] = clonedCh
	}
	return cloned
}

func containsString(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// UnmarshalJSON supports unmarshaling from both standard TaskGraph JSON and flat {"subtasks": [...]} format.
func (g *TaskGraph) UnmarshalJSON(data []byte) error {
	type rawGraph struct {
		Nodes          any             `json:"nodes"`
		Edges          []Edge          `json:"edges"`
		Subtasks       []Subtask       `json:"subtasks"`
		ParallelGroups []ParallelGroup `json:"parallel_groups"`
		Chains         []Chain         `json:"chains"`
	}

	var raw rawGraph
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*g = *NewTaskGraph()

	// If nodes is a map or slice
	if raw.Nodes != nil {
		switch nodesVal := raw.Nodes.(type) {
		case map[string]any:
			nodeBytes, err := json.Marshal(nodesVal)
			if err == nil {
				var nodeMap map[string]Subtask
				if err := json.Unmarshal(nodeBytes, &nodeMap); err == nil {
					for k, v := range nodeMap {
						if v.ID == "" {
							v.ID = k
						}
						_ = g.AddNode(v)
					}
				}
			}
		case []any:
			nodeBytes, err := json.Marshal(nodesVal)
			if err == nil {
				var nodeSlice []Subtask
				if err := json.Unmarshal(nodeBytes, &nodeSlice); err == nil {
					for _, v := range nodeSlice {
						_ = g.AddNode(v)
					}
				}
			}
		}
	}

	// If subtasks was supplied instead of nodes
	if len(raw.Subtasks) > 0 && len(g.Nodes) == 0 {
		for _, st := range raw.Subtasks {
			_ = g.AddNode(st)
			for _, dep := range st.DependsOn {
				_ = g.AddEdge(dep, st.ID)
			}
		}
	}

	for _, e := range raw.Edges {
		_ = g.AddEdge(e.From, e.To)
	}

	for _, chain := range raw.Chains {
		_, _ = g.AddChain(chain.Subtasks...)
	}

	for _, group := range raw.ParallelGroups {
		_, _ = g.AddParallelGroup(group.Subtasks...)
	}

	return nil
}

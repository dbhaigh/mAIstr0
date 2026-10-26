package evaluation

import (
	"container/heap"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

type Distribution struct {
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
}

type Metrics struct {
	Requests          int          `json:"requests"`
	Succeeded         int          `json:"succeeded"`
	Failed            int          `json:"failed"`
	Attempts          int          `json:"attempts"`
	Decisions         int          `json:"decisions"`
	Examined          int          `json:"candidates_examined"`
	Eligible          int          `json:"eligible_candidates"`
	FailureRate       float64      `json:"failure_rate"`
	Throughput        float64      `json:"successful_requests_per_second"`
	HorizonUS         int64        `json:"horizon_us"`
	Queue             Distribution `json:"queue_wait_all_requests"`
	FirstToken        Distribution `json:"simulated_ttft_successes"`
	Latency           Distribution `json:"simulated_e2e_successes"`
	Utilization       []float64    `json:"node_utilization"`
	UtilizationStddev float64      `json:"utilization_stddev"`
	MaxActive         []int        `json:"max_active_per_node"`
	Started           []int        `json:"started_per_node"`
	InvalidStarts     int          `json:"invalid_starts"`
}

const (
	eventFailure = iota
	eventRecovery
	eventDeadline
	eventComplete
	eventArrival
	eventRetry
)

type event struct {
	at         int64
	kind       int
	id         int
	generation int
	order      int
}

type events []event

func (e events) Len() int { return len(e) }
func (e events) Less(i, j int) bool {
	if e[i].at != e[j].at {
		return e[i].at < e[j].at
	}
	if e[i].kind != e[j].kind {
		return e[i].kind < e[j].kind
	}
	return e[i].order < e[j].order
}
func (e events) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e *events) Push(v any)   { *e = append(*e, v.(event)) }
func (e *events) Pop() any {
	old := *e
	v := old[len(old)-1]
	*e = old[:len(old)-1]
	return v
}

const (
	pending = iota
	queued
	running
	done
)

type requestState struct {
	state      int
	node       int
	attempts   int
	start      int64
	queuedAt   int64
	queueUS    int64
	generation int
	excluded   []bool
}

type simulation struct {
	scenario Scenario
	selector *selector
	nodes    []cluster.NodeStatus
	requests []requestState
	queues   [][]int
	busyUS   []int64
	events   events
	next     int
	now      int64
	left     int
	metrics  Metrics
	queue    []int64
	ttft     []int64
	latency  []int64
}

func (s Scenario) validate() error {
	if len(s.Nodes) == 0 || len(s.Requests) == 0 || s.TimeoutUS <= 0 ||
		len(s.TokensPerSecond) != len(s.Nodes) || len(s.PrefillUS) != len(s.Nodes) {
		return fmt.Errorf("scenario requires nodes, requests, positive timeout and per-node service parameters")
	}
	seen := make(map[string]bool, len(s.Nodes))
	for i, node := range s.Nodes {
		tps := s.TokensPerSecond[i]
		if node.MaxConcurrentTasks <= 0 || node.ActiveTasks != 0 || node.QueuedTasks != 0 ||
			tps <= 0 || math.IsNaN(tps) || math.IsInf(tps, 0) || s.PrefillUS[i] < 0 {
			return fmt.Errorf("invalid service parameters or nonempty initial load for node %d", i)
		}
		if node.ID == "" || seen[node.ID] {
			return fmt.Errorf("node IDs must be nonempty and unique")
		}
		seen[node.ID] = true
	}
	for i, r := range s.Requests {
		if r.ArrivalUS < 0 || r.OutputTokens <= 0 || (i > 0 && r.ArrivalUS < s.Requests[i-1].ArrivalUS) {
			return fmt.Errorf("invalid or unordered request %d", i)
		}
	}
	for i, outage := range s.Outages {
		if outage.Node < 0 || outage.Node >= len(s.Nodes) || outage.AtUS < 0 || outage.EndUS <= outage.AtUS {
			return fmt.Errorf("invalid outage %d", i)
		}
		for _, prior := range s.Outages[:i] {
			if prior.Node == outage.Node && prior.AtUS < outage.EndUS && outage.AtUS < prior.EndUS {
				return fmt.Errorf("overlapping outages for node %d", outage.Node)
			}
		}
	}
	return nil
}

// Simulate is a deterministic discrete-event model, not an inference or
// production workpool benchmark. Arrivals at the same instant are ordered by
// trace index. Reservations are atomic in this single-threaded virtual clock.
func Simulate(scenario Scenario, policy Policy) (Metrics, error) {
	if err := scenario.validate(); err != nil {
		return Metrics{}, err
	}
	selector, err := newSelector(policy, scenario.Nodes)
	if err != nil {
		return Metrics{}, err
	}
	s := &simulation{
		scenario: scenario, selector: selector, nodes: slices.Clone(scenario.Nodes),
		requests: make([]requestState, len(scenario.Requests)), queues: make([][]int, len(scenario.Nodes)),
		busyUS: make([]int64, len(scenario.Nodes)), left: len(scenario.Requests),
		metrics: Metrics{Requests: len(scenario.Requests), MaxActive: make([]int, len(scenario.Nodes)), Started: make([]int, len(scenario.Nodes))},
	}
	for i, r := range scenario.Requests {
		s.requests[i].node = -1
		s.requests[i].excluded = make([]bool, len(s.nodes))
		s.push(event{at: r.ArrivalUS, kind: eventArrival, id: i})
		s.push(event{at: r.ArrivalUS + scenario.TimeoutUS, kind: eventDeadline, id: i})
	}
	for i, outage := range scenario.Outages {
		s.push(event{at: outage.AtUS, kind: eventFailure, id: i})
		s.push(event{at: outage.EndUS, kind: eventRecovery, id: i})
	}
	for s.left > 0 && len(s.events) > 0 {
		e := heap.Pop(&s.events).(event)
		s.now = e.at
		switch e.kind {
		case eventFailure:
			node := scenario.Outages[e.id].Node
			s.nodes[node].Healthy = false
			for i := range s.requests {
				r := &s.requests[i]
				if r.node == node && (r.state == queued || r.state == running) {
					s.finishAttempt(i, false, true)
				}
			}
		case eventRecovery:
			s.nodes[scenario.Outages[e.id].Node].Healthy = true
		case eventDeadline:
			if s.requests[e.id].state != done {
				s.finishAttempt(e.id, false, false)
			}
		case eventComplete:
			r := &s.requests[e.id]
			if r.state == running && r.generation == e.generation {
				s.finishAttempt(e.id, true, false)
			}
		case eventArrival, eventRetry:
			if s.requests[e.id].state == pending {
				s.assign(e.id)
			}
		}
	}
	if s.left != 0 {
		return Metrics{}, fmt.Errorf("simulation ended with %d unfinished requests", s.left)
	}
	if s.metrics.InvalidStarts != 0 {
		return Metrics{}, fmt.Errorf("simulation violated %d start invariants", s.metrics.InvalidStarts)
	}
	s.metrics.HorizonUS = max(1, s.now-scenario.Requests[0].ArrivalUS)
	s.metrics.FailureRate = float64(s.metrics.Failed) / float64(s.metrics.Requests)
	s.metrics.Throughput = float64(s.metrics.Succeeded) * 1e6 / float64(s.metrics.HorizonUS)
	s.metrics.Queue = distribution(s.queue)
	s.metrics.FirstToken = distribution(s.ttft)
	s.metrics.Latency = distribution(s.latency)
	mean := 0.0
	for i, busy := range s.busyUS {
		// Denominator includes downtime and model-ineligible nodes, making
		// utilization comparable across policies on the same cluster.
		u := float64(busy) / (float64(s.metrics.HorizonUS) * float64(s.nodes[i].MaxConcurrentTasks))
		s.metrics.Utilization = append(s.metrics.Utilization, u)
		mean += u / float64(len(s.nodes))
	}
	for _, u := range s.metrics.Utilization {
		s.metrics.UtilizationStddev += (u - mean) * (u - mean) / float64(len(s.nodes))
	}
	s.metrics.UtilizationStddev = math.Sqrt(s.metrics.UtilizationStddev)
	return s.metrics, nil
}

func (s *simulation) push(e event) {
	e.order, s.next = s.next, s.next+1
	heap.Push(&s.events, e)
}

func (s *simulation) assign(id int) {
	r := &s.requests[id]
	now := Epoch().Add(time.Duration(s.now) * time.Microsecond)
	for i := range s.nodes {
		// Preserve each scenario's missing/stale/future offset while modelling
		// regular heartbeats. Health transitions come only from outage events.
		if !s.scenario.Nodes[i].TelemetryAt.IsZero() {
			s.nodes[i].TelemetryAt = now.Add(s.scenario.Nodes[i].TelemetryAt.Sub(Epoch()))
		}
	}
	choice := s.selector.choose(s.nodes, s.scenario.Requests[id].Task, r.excluded, now)
	s.metrics.Decisions++
	s.metrics.Examined += choice.Examined
	s.metrics.Eligible += choice.Eligible
	if choice.Node < 0 {
		s.finishAttempt(id, false, false)
		return
	}
	r.node, r.state, r.queuedAt = choice.Node, queued, s.now
	r.attempts++
	r.excluded[r.node] = true
	s.metrics.Attempts++
	s.nodes[r.node].QueuedTasks++
	s.queues[r.node] = append(s.queues[r.node], id)
	s.startQueued(r.node)
}

func (s *simulation) startQueued(node int) {
	n := &s.nodes[node]
	for n.Healthy && n.ActiveTasks < n.MaxConcurrentTasks && len(s.queues[node]) > 0 {
		id := s.queues[node][0]
		s.queues[node] = s.queues[node][1:]
		r := &s.requests[id]
		if r.state != queued || r.node != node {
			continue
		}
		n.QueuedTasks--
		n.ActiveTasks++
		n.AvailableCapacity = n.MaxConcurrentTasks - n.ActiveTasks
		s.metrics.MaxActive[node] = max(s.metrics.MaxActive[node], n.ActiveTasks)
		s.metrics.Started[node]++
		if _, eligible := score.Prepare(*n, s.scenario.Requests[id].Task); !eligible ||
			!n.Healthy || n.ActiveTasks > n.MaxConcurrentTasks {
			s.metrics.InvalidStarts++
		}
		r.state, r.start = running, s.now
		r.queueUS += s.now - r.queuedAt
		r.generation++
		service := s.scenario.PrefillUS[node] +
			int64(math.Ceil(float64(s.scenario.Requests[id].OutputTokens)*1e6/s.scenario.TokensPerSecond[node]))
		s.push(event{at: s.now + service, kind: eventComplete, id: id, generation: r.generation})
	}
}

func (s *simulation) finishAttempt(id int, success, retry bool) {
	r := &s.requests[id]
	node := r.node
	switch r.state {
	case running:
		s.nodes[node].ActiveTasks--
		s.nodes[node].AvailableCapacity = s.nodes[node].MaxConcurrentTasks - s.nodes[node].ActiveTasks
		s.busyUS[node] += s.now - r.start
	case queued:
		s.nodes[node].QueuedTasks--
		r.queueUS += s.now - r.queuedAt
	}
	r.state = pending
	if retry && r.attempts < 5 && s.now+100_000 < s.scenario.Requests[id].ArrivalUS+s.scenario.TimeoutUS {
		s.push(event{at: s.now + 100_000, kind: eventRetry, id: id})
	} else {
		r.state = done
		s.left--
		s.queue = append(s.queue, r.queueUS)
		if success {
			s.metrics.Succeeded++
			s.latency = append(s.latency, s.now-s.scenario.Requests[id].ArrivalUS)
			s.ttft = append(s.ttft, r.start+s.scenario.PrefillUS[node]-s.scenario.Requests[id].ArrivalUS)
		} else {
			s.metrics.Failed++
		}
	}
	if node >= 0 {
		s.startQueued(node)
	}
}

func distribution(values []int64) Distribution {
	if len(values) == 0 {
		return Distribution{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	d := Distribution{}
	for _, value := range sorted {
		d.MeanMS += float64(value) / 1000 / float64(len(sorted))
	}
	percentile := func(p float64) float64 {
		return float64(sorted[int(math.Ceil(p*float64(len(sorted))))-1]) / 1000
	}
	d.P50MS, d.P95MS, d.P99MS = percentile(.5), percentile(.95), percentile(.99)
	return d
}

package evaluation

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler/score"
)

type DecisionSample struct {
	Iterations   int     `json:"iterations"`
	WallNS       float64 `json:"wall_ns_per_decision"`
	ProcessCPUNS float64 `json:"process_cpu_ns_per_decision"`
	Allocs       float64 `json:"allocations_per_decision"`
	Bytes        float64 `json:"bytes_per_decision"`
}

type Result struct {
	Policy  Policy           `json:"policy"`
	Samples []DecisionSample `json:"decision_samples"`
	Metrics Metrics          `json:"simulation"`
}

type ScenarioResult struct {
	Name            string    `json:"name"`
	Seed            int64     `json:"seed"`
	Requests        int       `json:"requests"`
	Nodes           int       `json:"nodes"`
	TimeoutUS       int64     `json:"timeout_us"`
	TokensPerSecond []float64 `json:"synthetic_tokens_per_second"`
	PrefillUS       []int64   `json:"synthetic_prefill_us"`
	Outages         []Outage  `json:"outages,omitempty"`
	TraceSHA256     string    `json:"scenario_sha256"`
	Results         []Result  `json:"results"`
}

type Report struct {
	SchemaVersion     int                 `json:"schema_version"`
	GeneratorVersion  int                 `json:"generator_version"`
	GoVersion         string              `json:"go_version"`
	OS                string              `json:"os"`
	Arch              string              `json:"arch"`
	LogicalCPUs       int                 `json:"logical_cpus"`
	GOMAXPROCS        int                 `json:"gomaxprocs"`
	Iterations        int                 `json:"minimum_decision_iterations_per_sample"`
	MinimumSampleMS   int                 `json:"minimum_sample_ms"`
	Repetitions       int                 `json:"repetitions"`
	InferenceMeasured bool                `json:"real_inference_measured"`
	ScoringConfig     score.ScoringConfig `json:"scoring_config"`
	Scenarios         []ScenarioResult    `json:"scenarios"`
}

// Measure runs isolated ranking batches separately from the virtual-clock
// simulation. CPU means process user+kernel CPU, not inferred wall time.
// Allocation deltas include runtime activity; repeat samples expose noise.
func Measure(scenarios []Scenario, iterations, repetitions int) (Report, error) {
	if iterations < 1 || repetitions < 1 {
		return Report{}, fmt.Errorf("iterations and repetitions must be positive")
	}
	report := Report{
		SchemaVersion: 1, GeneratorVersion: 1, GoVersion: runtime.Version(),
		OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(),
		GOMAXPROCS: runtime.GOMAXPROCS(0), Iterations: iterations, Repetitions: repetitions,
		MinimumSampleMS: 100, ScoringConfig: score.DefaultScoringConfig(),
	}
	for _, scenario := range scenarios {
		if err := scenario.validate(); err != nil {
			return Report{}, err
		}
		data, err := json.Marshal(scenario)
		if err != nil {
			return Report{}, fmt.Errorf("encode scenario fingerprint: %w", err)
		}
		scenarioResult := ScenarioResult{
			Name: scenario.Name, Seed: scenario.Seed, Requests: len(scenario.Requests),
			Nodes: len(scenario.Nodes), TimeoutUS: scenario.TimeoutUS,
			TokensPerSecond: scenario.TokensPerSecond, PrefillUS: scenario.PrefillUS, Outages: scenario.Outages,
			TraceSHA256: fmt.Sprintf("%x", sha256.Sum256(data)),
		}
		for _, policy := range Policies() {
			metrics, err := Simulate(scenario, policy)
			if err != nil {
				return Report{}, fmt.Errorf("%s/%s: %w", scenario.Name, policy, err)
			}
			result := Result{Policy: policy, Metrics: metrics}
			for range repetitions {
				sample, err := measureDecision(scenario, policy, iterations)
				if err != nil {
					return Report{}, err
				}
				result.Samples = append(result.Samples, sample)
			}
			scenarioResult.Results = append(scenarioResult.Results, result)
		}
		report.Scenarios = append(report.Scenarios, scenarioResult)
	}
	return report, nil
}

func measureDecision(scenario Scenario, policy Policy, iterations int) (DecisionSample, error) {
	selector, err := newSelector(policy, scenario.Nodes)
	if err != nil {
		return DecisionSample{}, err
	}
	for range 100 {
		selector.choose(scenario.Nodes, scenario.Requests[0].Task, nil, Epoch())
	}
	if _, err := processCPUTime(); err != nil {
		return DecisionSample{}, fmt.Errorf("initialize process CPU measurement: %w", err)
	}
	for {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		cpuBefore, err := processCPUTime()
		if err != nil {
			return DecisionSample{}, fmt.Errorf("measure process CPU: %w", err)
		}
		start := time.Now()
		for i := range iterations {
			selector.choose(scenario.Nodes, scenario.Requests[i%len(scenario.Requests)].Task, nil, Epoch())
		}
		elapsed := time.Since(start)
		cpuAfter, err := processCPUTime()
		if err != nil {
			return DecisionSample{}, fmt.Errorf("measure process CPU: %w", err)
		}
		runtime.ReadMemStats(&after)
		// Short Windows process-CPU intervals can quantize to zero. Increase
		// real work rather than substituting wall time or reporting zero cost.
		if elapsed < 100*time.Millisecond || cpuAfter == cpuBefore {
			if iterations > int(^uint(0)>>1)/2 {
				return DecisionSample{}, fmt.Errorf("CPU sample iteration count overflow")
			}
			iterations *= 2
			continue
		}
		return DecisionSample{
			Iterations:   iterations,
			WallNS:       float64(elapsed.Nanoseconds()) / float64(iterations),
			ProcessCPUNS: float64((cpuAfter - cpuBefore).Nanoseconds()) / float64(iterations),
			Allocs:       float64(after.Mallocs-before.Mallocs) / float64(iterations),
			Bytes:        float64(after.TotalAlloc-before.TotalAlloc) / float64(iterations),
		}, nil
	}
}

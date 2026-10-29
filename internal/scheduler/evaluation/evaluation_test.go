package evaluation

import (
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

func TestGeneratedTracesAreDeterministicAndPoliciesRespectInvariants(t *testing.T) {
	for _, seed := range []int64{17, 29} {
		scenarios, err := Scenarios(seed, 128)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Scenarios(seed, 128)
		if err != nil || !reflect.DeepEqual(scenarios, again) {
			t.Fatalf("trace generation is not reproducible: %v", err)
		}
		for _, scenario := range scenarios {
			for _, policy := range Policies() {
				t.Run(strconv.FormatInt(seed, 10)+"/"+scenario.Name+"/"+string(policy), func(t *testing.T) {
					before, err := json.Marshal(scenario)
					if err != nil {
						t.Fatal(err)
					}
					got, err := Simulate(scenario, policy)
					if err != nil {
						t.Fatal(err)
					}
					repeated, err := Simulate(scenario, policy)
					if err != nil || !reflect.DeepEqual(got, repeated) {
						t.Fatalf("simulation is not reproducible: %v", err)
					}
					after, err := json.Marshal(scenario)
					if err != nil || string(before) != string(after) {
						t.Fatalf("simulation mutated input: %v", err)
					}
					if got.Succeeded+got.Failed != got.Requests || got.InvalidStarts != 0 {
						t.Fatalf("invalid terminal counts: %+v", got)
					}
					for i, active := range got.MaxActive {
						if active > scenario.Nodes[i].MaxConcurrentTasks || got.Utilization[i] < 0 || got.Utilization[i] > 1 {
							t.Fatalf("node %d exceeded capacity: %+v", i, got)
						}
						if scenario.Name == "resource_limits" && i%2 == 0 && got.Started[i] != 0 {
							t.Fatalf("insufficient NPU memory selected on node %d", i)
						}
					}
				})
			}
		}
	}
}

func tinyScenario() Scenario {
	return Scenario{
		Name: "hand_calculated", TimeoutUS: 5_000_000,
		Nodes: []cluster.NodeStatus{{
			ID: "synthetic", Healthy: true, MaxConcurrentTasks: 1, AvailableCapacity: 1,
			Models: []engine.Model{{Name: "synthetic", Tags: []string{"general"}, SizeGB: 1}},
		}},
		TokensPerSecond: []float64{10}, PrefillUS: []int64{50_000},
		Requests: []Request{
			{OutputTokens: 10, Task: score.Subtask{TaskType: "general"}},
			{OutputTokens: 10, Task: score.Subtask{TaskType: "general"}},
		},
	}
}

func TestSimulationMatchesHandCalculatedQueueAndLatency(t *testing.T) {
	for _, policy := range Policies() {
		t.Run(string(policy), func(t *testing.T) {
			got, err := Simulate(tinyScenario(), policy)
			if err != nil {
				t.Fatal(err)
			}
			if got.Succeeded != 2 || got.Failed != 0 || got.HorizonUS != 2_100_000 ||
				got.Queue.MeanMS != 525 || got.Latency.P50MS != 1050 || got.Latency.P99MS != 2100 ||
				got.FirstToken.P50MS != 50 || got.FirstToken.P99MS != 1100 ||
				got.MaxActive[0] != 1 || got.Utilization[0] != 1 {
				t.Fatalf("wrong hand-calculated measurements: %+v", got)
			}
		})
	}
}

func TestSimulationDeadlineAndFailureCleanup(t *testing.T) {
	tests := []struct {
		name            string
		edit            func(*Scenario)
		success, failed int
	}{
		{"queued deadline", func(s *Scenario) {
			s.TimeoutUS = 1_500_000
			s.Requests = append(s.Requests, s.Requests[0])
		}, 1, 2},
		{"failure and recovery", func(s *Scenario) {
			s.Outages = []Outage{{Node: 0, AtUS: 500_000, EndUS: 2_000_000}}
			s.Requests[1].ArrivalUS = 3_000_000
		}, 1, 1},
		{"complete at deadline is timeout", func(s *Scenario) {
			s.TimeoutUS = 1_050_000
		}, 0, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := tinyScenario()
			test.edit(&s)
			got, err := Simulate(s, Existing)
			if err != nil {
				t.Fatal(err)
			}
			if got.Succeeded != test.success || got.Failed != test.failed || got.MaxActive[0] > 1 {
				t.Fatalf("failure cleanup measurements: %+v", got)
			}
		})
	}
}

func TestEveryPolicyUsesProductionHardEligibilityAndStableTies(t *testing.T) {
	for _, policy := range Policies() {
		for _, test := range []struct {
			name string
			edit func(*cluster.NodeStatus)
			task score.Subtask
		}{
			{"unhealthy", func(n *cluster.NodeStatus) { n.Healthy = false }, score.Subtask{TaskType: "general"}},
			{"engine", func(n *cluster.NodeStatus) {
				n.Models[0].Engine = "unready"
				n.Engines = []engine.Status{{Name: "unready", Ready: false}}
			}, score.Subtask{TaskType: "general"}},
			{"capability", func(n *cluster.NodeStatus) { n.Capabilities = []string{"cpu"} }, score.Subtask{RequiredCapabilities: []string{"gpu"}}},
		} {
			t.Run(string(policy)+"/"+test.name, func(t *testing.T) {
				base := tinyScenario().Nodes[0]
				bad := base
				bad.Models = slices.Clone(base.Models)
				test.edit(&bad)
				if test.name == "capability" {
					base.Capabilities = []string{"gpu"}
				}
				nodes := []cluster.NodeStatus{bad, base}
				s, err := newSelector(policy, nodes)
				if err != nil {
					t.Fatal(err)
				}
				got := s.choose(nodes, test.task, nil, Epoch())
				if got.Node != 1 || got.Eligible != 1 || got.Examined != 2 {
					t.Fatalf("ineligible candidate selected: %+v", got)
				}
			})
		}
		t.Run(string(policy)+"/tie", func(t *testing.T) {
			node := tinyScenario().Nodes[0]
			nodes := []cluster.NodeStatus{node, node}
			s, err := newSelector(policy, nodes)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.choose(nodes, score.Subtask{TaskType: "general"}, nil, Epoch()); got.Node != 0 {
				t.Fatalf("tie must preserve input order: %+v", got)
			}
		})
	}
}

func TestEvaluationRejectsInvalidInputs(t *testing.T) {
	for _, count := range []int{0, -1, 100001} {
		if _, err := Scenarios(17, count); err == nil {
			t.Fatalf("accepted request count %d", count)
		}
	}
	for _, edit := range []func(*Scenario){
		func(s *Scenario) { s.TokensPerSecond[0] = math.NaN() },
		func(s *Scenario) { s.TimeoutUS = 0 },
		func(s *Scenario) { s.Requests[1].ArrivalUS = -1 },
		func(s *Scenario) { s.Outages = []Outage{{Node: 1, AtUS: 0, EndUS: 1}} },
		func(s *Scenario) { s.Outages = []Outage{{Node: 0, AtUS: 0, EndUS: 3}, {Node: 0, AtUS: 1, EndUS: 2}} },
	} {
		s := tinyScenario()
		edit(&s)
		if _, err := Simulate(s, Existing); err == nil {
			t.Fatal("accepted invalid scenario")
		}
	}
	if _, err := Simulate(tinyScenario(), Policy("invalid")); err == nil {
		t.Fatal("accepted invalid policy")
	}
}

func TestMeasurementOutputShape(t *testing.T) {
	report, err := Measure([]Scenario{tinyScenario()}, 100000, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 1 || report.InferenceMeasured || len(report.Scenarios) != 1 ||
		len(report.Scenarios[0].TraceSHA256) != 64 || len(report.Scenarios[0].Results) != 4 {
		t.Fatalf("invalid report: %+v", report)
	}
	for _, result := range report.Scenarios[0].Results {
		if len(result.Samples) != 2 || result.Samples[0].WallNS <= 0 ||
			result.Samples[0].ProcessCPUNS <= 0 || result.Metrics.Requests != 2 {
			t.Fatalf("invalid result: %+v", result)
		}
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(report, decoded) {
		t.Fatalf("report JSON round-trip failed: %v", err)
	}
}

func BenchmarkPolicies(b *testing.B) {
	scenarios, err := Scenarios(17, 32)
	if err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{8, 80, 800} {
		nodes := make([]cluster.NodeStatus, count)
		for i := range nodes {
			nodes[i] = scenarios[0].Nodes[i%8]
			nodes[i].ID = "synthetic-" + strconv.Itoa(i)
		}

		for _, policy := range Policies() {
			b.Run(strconv.Itoa(count)+"/"+string(policy), func(b *testing.B) {
				s, err := newSelector(policy, nodes)
				if err != nil {
					b.Fatal(err)
				}
				task := scenarios[0].Requests[0].Task
				b.ReportAllocs()
				examined, eligible := 0, 0
				b.ResetTimer()
				for range b.N {
					got := s.choose(nodes, task, nil, Epoch())
					if got.Node < 0 {
						b.Fatal("no eligible node")
					}
					examined += got.Examined
					eligible += got.Eligible
				}
				b.ReportMetric(float64(examined)/float64(b.N), "examined/op")
				b.ReportMetric(float64(eligible)/float64(b.N), "eligible/op")
			})
		}
	}
}

func TestExistingPolicyMatchesProductionPlanningWithQueuedAndFullNodes(t *testing.T) {
	scenarios, err := Scenarios(17, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			nodes := slices.Clone(scenario.Nodes)
			now := time.Now()
			for i := range nodes {
				nodes[i].ActiveTasks, nodes[i].QueuedTasks = i%3, (i+1)%4
				nodes[i].AvailableCapacity = max(0, nodes[i].MaxConcurrentTasks-nodes[i].ActiveTasks)
				if !nodes[i].TelemetryAt.IsZero() {
					nodes[i].TelemetryAt = now.Add(nodes[i].TelemetryAt.Sub(Epoch()))
				}
			}
			task := scenario.Requests[0].Task
			policy, err := newSelector(Existing, nodes)
			if err != nil {
				t.Fatal(err)
			}
			got := policy.choose(nodes, task, nil, now)
			want, err := scheduler.Assign([]scheduler.Subtask{{
				ID: "synthetic", TaskType: task.TaskType, RequiredCapabilities: task.RequiredCapabilities,
			}}, nodes)
			if err != nil || len(want) != 1 || got.Node < 0 || nodes[got.Node].ID != want[0].NodeID {
				t.Fatalf("benchmark scorer diverges from production: %+v, %+v, %v", got, want, err)
			}
		})
	}
}

func BenchmarkPoliciesParallel(b *testing.B) {
	scenarios, err := Scenarios(17, 32)
	if err != nil {
		b.Fatal(err)
	}
	nodes := scenarios[0].Nodes
	task := scenarios[0].Requests[0].Task
	for _, policy := range Policies() {
		b.Run(string(policy), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				s, err := newSelector(policy, nodes)
				if err != nil {
					b.Error(err)
					return
				}
				for pb.Next() {
					if s.choose(nodes, task, nil, Epoch()).Node < 0 {
						b.Error("no eligible node")
						return
					}
				}
			})
		})
	}
}

package scheduler

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

func setDecisionTestLogger(t testing.TB, logger *slog.Logger) {
	t.Helper()
	old, output, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(logger)
	t.Cleanup(func() {
		slog.SetDefault(old)
		log.SetOutput(output)
		log.SetFlags(flags)
	})
}

func TestDecisionDiagnosticsExplainWithoutPrivateFields(t *testing.T) {
	var output bytes.Buffer
	setDecisionTestLogger(t, slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	node := cluster.NodeStatus{
		ID: "private-node-name", Address: "https://private-user:private-password@private-host",
		Healthy: true, MaxConcurrentTasks: 1, AvailableCapacity: 0, ActiveTasks: 1,
		Capabilities: []string{"private-capability"}, TelemetryAt: time.Now().Add(-time.Minute),
		Models: []engine.Model{{Name: "private-model-name", SizeGB: 2, Tags: []string{"general"}}},
	}
	unready := node
	unready.Engines = []engine.Status{{Name: "", Ready: false}}
	unhealthy := node
	unhealthy.Healthy = false
	task := Subtask{
		ID: "private-task-id", Role: "private-role",
		Description: "private-prompt-content", ExpectedOutput: "private-response-content",
		TaskType: "general", PreferredModel: "private-model-name",
		PreferredCapabilities: []string{"private-capability"},
	}
	assignments, err := Assign([]Subtask{task}, []cluster.NodeStatus{unhealthy, unready, node})
	if err != nil || len(assignments) != 1 {
		t.Fatalf("assignment: %v", err)
	}
	for _, secret := range []string{
		node.ID, node.Address, task.ID, task.Role, task.Description, task.ExpectedOutput,
		task.PreferredModel, "private-capability", "private-user", "private-password",
	} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("diagnostic leaked private field %q", secret)
		}
	}
	reasons := make(map[string]bool)
	var decisionID float64
	scanner := bufio.NewScanner(&output)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		reason, ok := record["reason"].(string)
		if !ok {
			t.Fatalf("missing fixed reason: %v", record)
		}
		reasons[reason] = true
		if reason == "selected" || reason == "eligible" {
			id, ok := record["decision_seq"].(float64)
			if !ok || (decisionID != 0 && id != decisionID) {
				t.Fatalf("candidate/winner correlation lost: %v", record)
			}
			decisionID = id
			total := 0.0
			for _, key := range []string{"model_match", "hardware", "capability", "quality", "gpu_compatibility", "topology", "latency", "learned_bias"} {
				total += record[key].(float64)
			}
			for _, key := range []string{"load_penalty", "capacity_penalty", "gpu_penalty", "npu_penalty", "cpu_penalty", "memory_penalty"} {
				total -= record[key].(float64)
			}
			if math.Abs(total-record["score"].(float64)) > 1e-9 || record["telemetry_fresh"] != false {
				t.Fatalf("score/freshness explanation differs from decision: %v", record)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"unhealthy", "engine_unready", "eligible", "selected"} {
		if !reasons[reason] {
			t.Fatalf("missing reason %q: %v", reason, reasons)
		}
	}
}

func TestDebugDiagnosticsDoNotChangeAssignment(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{ID: "first", Healthy: true, Models: []engine.Model{{Name: "synthetic"}}},
		{ID: "second", Healthy: true, Models: []engine.Model{{Name: "synthetic"}}},
	}
	for _, level := range []slog.Level{slog.LevelWarn, slog.LevelDebug} {
		setDecisionTestLogger(t, slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level})))
		got, err := Assign([]Subtask{{ID: "synthetic", TaskType: "general"}}, nodes)
		if err != nil || len(got) != 1 || got[0].NodeID != "first" {
			t.Fatalf("level %v changed tie-breaking: %v, %v", level, got, err)
		}
	}
}

func BenchmarkDecisionDiagnostics(b *testing.B) {
	node := cluster.NodeStatus{ID: "synthetic", Healthy: true}
	for _, test := range []struct {
		name  string
		level slog.Level
	}{{"disabled", slog.LevelInfo}, {"enabled", slog.LevelDebug}} {
		b.Run(test.name, func(b *testing.B) {
			setDecisionTestLogger(b, slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: test.level})))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				LogDecision("benchmark", "eligible", node, "synthetic", 0, score.ScoreFactors{})
			}
		})
	}
}

package scheduler

import (
	"io"
	"log"
	"strconv"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

func BenchmarkAssignWithConfigNodes80(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	nodes := make([]cluster.NodeStatus, 80)
	for i := range nodes {
		engineName := "vllm"
		tags := []string{"general"}
		size := 4.0
		if i%4 == 0 {
			tags = []string{"code"}
			size = 8.0
		} else if i%4 == 1 {
			engineName = "unavailable"
		}
		nodes[i] = cluster.NodeStatus{
			ID:      "node-" + strconv.Itoa(i),
			Address: "http://node",
			Healthy: true,
			Engines: []engine.Status{{Name: "vllm", Ready: true}},
			Models: []engine.Model{{
				Name: "model", Engine: engineName, Tags: tags, SizeGB: size,
			}},
		}
	}
	subtasks := []Subtask{
		{ID: "code-1", TaskType: "code", RequiredCapabilities: []string{"coder"}, MinModelSizeGB: 7},
		{ID: "code-2", TaskType: "code", RequiredCapabilities: []string{"coder"}, MinModelSizeGB: 7},
		{ID: "code-3", TaskType: "code", RequiredCapabilities: []string{"coder"}, MinModelSizeGB: 7},
	}
	cfg := DefaultScoringConfig()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := AssignWithConfig(subtasks, nodes, &cfg, nil); err != nil {
			b.Fatal(err)
		}
	}
}

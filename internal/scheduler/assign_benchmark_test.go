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

func BenchmarkAssignWithExperienceNodes32ManyModels(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	nodes := make([]cluster.NodeStatus, 32)
	for i := range nodes {
		models := make([]engine.Model, 8)
		for j := range models {
			tags := []string{"general"}
			if j%2 == 0 {
				tags = []string{"code", "general"}
			}
			models[j] = engine.Model{
				Name:   "model-" + strconv.Itoa(i) + "-" + strconv.Itoa(j),
				Engine: "vllm",
				Tags:   tags,
				SizeGB: 4 + float64(j),
			}
		}
		nodes[i] = cluster.NodeStatus{
			ID:           "node-" + strconv.Itoa(i),
			Address:      "http://node",
			Healthy:      true,
			Capabilities: []string{"coder"},
			Engines:      []engine.Status{{Name: "vllm", Ready: true}},
			Models:       models,
		}
	}
	subtasks := []Subtask{
		{ID: "code-1", TaskType: "code", RequiredCapabilities: []string{"coder"}, PreferredCapabilities: []string{"code"}, MinModelSizeGB: 7},
		{ID: "code-2", TaskType: "code", RequiredCapabilities: []string{"coder"}, PreferredCapabilities: []string{"code"}, MinModelSizeGB: 7},
		{ID: "code-3", TaskType: "code", RequiredCapabilities: []string{"coder"}, PreferredCapabilities: []string{"code"}, MinModelSizeGB: 7},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := AssignWithExperience(subtasks, nodes, nil); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkExperience struct{}

func (benchmarkExperience) LearnedBias(string, string, string) float64 { return 0.1 }

func BenchmarkAssignWithExperienceNodeModels(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(logOutput)

	for _, count := range []int{10, 100, 1_000} {
		b.Run(strconv.Itoa(count)+"-nodes-models", func(b *testing.B) {
			nodes := make([]cluster.NodeStatus, count)
			for i := range nodes {
				nodes[i] = cluster.NodeStatus{
					ID: "node-" + strconv.Itoa(i), Address: "http://node", Healthy: true,
					Engines: []engine.Status{{Name: "simulated", Ready: true}},
					Models: []engine.Model{{
						Name: "simulated-general", Engine: "simulated", Tags: []string{"general"},
					}},
				}
			}
			subtasks := []Subtask{{ID: "benchmark", Description: "baseline assignment", TaskType: "general"}}
			experience := benchmarkExperience{}

			b.ReportAllocs()
			b.ReportMetric(float64(count), "nodes/op")
			b.ReportMetric(float64(count), "models/op")
			b.ResetTimer()
			for range b.N {
				if _, err := AssignWithExperience(subtasks, nodes, experience); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

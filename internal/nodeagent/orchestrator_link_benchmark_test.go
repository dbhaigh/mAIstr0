package nodeagent

import (
	"context"
	"strconv"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func BenchmarkBuildSnapshot(b *testing.B) {
	const nodeCount = 100
	const modelsPerNode = 4
	const taskCount = 50

	peers := make([]cluster.NodeStatus, nodeCount)
	for nodeIndex := range peers {
		nodeID := "node-" + strconv.Itoa(nodeIndex)
		models := make([]engine.Model, modelsPerNode)
		for modelIndex := range models {
			models[modelIndex] = engine.Model{
				Name:      "benchmark-model-" + strconv.Itoa(modelIndex),
				Engine:    "simulated",
				Tags:      []string{"general"},
				Protocols: []string{"openai"},
			}
		}
		peers[nodeIndex] = cluster.NodeStatus{
			ID: nodeID, Address: "http://simulated-node",
			MaxConcurrentTasks: 8, AvailableCapacity: 8,
			Engines: []engine.Status{{Name: "simulated", Ready: true}},
			Models:  models,
		}
	}

	state := &agentState{}
	state.replacePeers(peers)
	tasks := taskmgr.NewManager()
	model := engine.Model{
		Name: "benchmark-model", Engine: "simulated",
		Tags: []string{"general"}, Protocols: []string{"openai"},
	}
	for taskIndex := range taskCount {
		assignment := scheduler.Assignment{
			Subtask: scheduler.Subtask{
				ID:          "subtask-" + strconv.Itoa(taskIndex),
				Description: "snapshot fixture",
				TaskType:    "general",
			},
			NodeID:  "node-" + strconv.Itoa(taskIndex),
			Address: "http://simulated-node",
			Model:   model.Name,
			Engine:  "simulated",
		}
		if _, err := tasks.CreateChecked("snapshot fixture task", []scheduler.Assignment{assignment}); err != nil {
			b.Fatal(err)
		}
	}

	link := &orchestratorLink{
		state: state,
		currentTasks: func(context.Context) ([]*taskmgr.Task, error) {
			return tasks.All(), nil
		},
	}
	ctx := context.Background()
	frame, err := link.buildSnapshot(ctx)
	if err != nil {
		b.Fatal(err)
	}
	frameBytes := float64(len(frame))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := link.buildSnapshot(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(nodeCount, "nodes/op")
	b.ReportMetric(taskCount, "tasks/op")
	b.ReportMetric(frameBytes, "frame-B/op")
}

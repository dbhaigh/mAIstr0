package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/workpool"
)

type cancelDecisionHandler struct {
	slog.Handler
	cancel context.CancelFunc
}

func BenchmarkCapacityReservationDiagnostics(b *testing.B) {
	for _, test := range []struct {
		name  string
		level slog.Level
	}{{"disabled", slog.LevelInfo}, {"enabled", slog.LevelDebug}} {
		b.Run(test.name, func(b *testing.B) {
			old, output, flags := slog.Default(), log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: test.level})))
			b.Cleanup(func() {
				slog.SetDefault(old)
				log.SetOutput(output)
				log.SetFlags(flags)
			})
			registry := cluster.NewRegistry()
			registry.Upsert(cluster.NodeStatus{
				ID: "synthetic", Address: "http://synthetic.invalid", MaxConcurrentTasks: 1, AvailableCapacity: 1,
			})
			gate := newDispatchCapacityGate(registry, nil, 1)
			targets := []workpool.CapacityTarget{{NodeID: "synthetic", Address: "http://synthetic.invalid"}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				reservation, err := gate.Reserve(context.Background(), targets)
				if err != nil {
					b.Fatal(err)
				}
				reservation.Release()
			}
		})
	}
}

func (h cancelDecisionHandler) Handle(ctx context.Context, record slog.Record) error {
	err := h.Handler.Handle(ctx, record)
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "reason" && attr.Value.String() == "node_capacity_wait" {
			h.cancel()
		}
		return true
	})
	return err
}

func TestFullNodeDecisionExplainsWaitWithoutGrantingReservation(t *testing.T) {
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "private-node", Address: "http://private-address", MaxConcurrentTasks: 2,
		AvailableCapacity: 0, ActiveTasks: 2,
	})
	gate := newDispatchCapacityGate(registry, nil, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	old, oldOutput, oldFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(cancelDecisionHandler{
		Handler: slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}),
		cancel:  cancel,
	}))
	t.Cleanup(func() {
		slog.SetDefault(old)
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})
	reservation, err := gate.Reserve(ctx, []workpool.CapacityTarget{{NodeID: "private-node", Address: "http://private-address"}})
	if reservation != nil || !errors.Is(err, context.Canceled) || gate.Active() != 0 {
		t.Fatalf("full node granted capacity or leaked: %v, %v, active=%d", reservation, err, gate.Active())
	}
	if !strings.Contains(output.String(), `"reason":"node_capacity_wait"`) ||
		strings.Contains(output.String(), "private-node") || strings.Contains(output.String(), "private-address") {
		t.Fatalf("missing/private wait explanation: %s", output.String())
	}
	active, limit := gate.nodeDispatchGate("private-node", 2).occupancy()
	if active != 2 || limit != 2 {
		t.Fatalf("reported external occupancy changed: %d/%d", active, limit)
	}
}

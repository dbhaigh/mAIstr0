package hub

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/testutil"
)

func BenchmarkProfilePublication(b *testing.B) {
	for _, clients := range []int{0, 1, 64} {
		b.Run(strconv.Itoa(clients), func(b *testing.B) {
			h := New()
			channels := make([]chan []byte, clients)
			for i := range channels {
				channels[i] = h.subscribe()
			}
			var builds atomic.Uint64
			payload := strings.Repeat("synthetic state ", 4096)
			p := NewPublisher(h, func(context.Context) ([]byte, error) {
				return json.Marshal(struct {
					Revision uint64 `json:"revision"`
					State    string `json:"state"`
				}{builds.Add(1), payload})
			})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); p.run(ctx, time.Hour, 0) }()
			b.Cleanup(func() {
				cancel()
				<-done
				for _, ch := range channels {
					h.unsubscribe(ch)
				}
			})
			// Drain the initial subscription build before timing changed frames.
			for _, ch := range channels {
				select {
				case <-ch:
				case <-time.After(time.Second):
					b.Fatal("initial publication did not complete")
				}
			}
			latencies := &testutil.Latencies{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				p.Request()
				for _, ch := range channels {
					select {
					case frame := <-ch:
						if len(frame) == 0 {
							b.Fatal("empty publication")
						}
					case <-ctx.Done():
						b.Fatal(ctx.Err())
					}
				}
				latencies.Record(time.Since(start))
			}
			latencies.Report(b, 1)
			if clients == 0 && builds.Load() != 0 {
				b.Fatal("publisher built a snapshot without clients")
			}
		})
	}
}

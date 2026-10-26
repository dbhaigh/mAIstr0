package testutil

import (
	"math"
	"slices"
	"testing"
	"time"
)

// Latencies keeps a bounded rolling window. Record is called by the benchmark
// coordinator, not concurrent workers, so timing does not add a shared lock.
type Latencies struct {
	samples [8192]time.Duration
	count   int
}

func (l *Latencies) Record(elapsed time.Duration) {
	l.samples[l.count%len(l.samples)] = elapsed
	l.count++
}

func (l *Latencies) Report(b *testing.B, requestsPerOp int) {
	b.Helper()
	b.StopTimer()
	values := slices.Clone(l.samples[:min(l.count, len(l.samples))])
	if len(values) == 0 {
		b.Fatal("benchmark recorded no latency samples")
	}
	slices.Sort(values)
	for _, p := range []struct {
		name  string
		value float64
	}{{"p50-us", .50}, {"p95-us", .95}, {"p99-us", .99}} {
		value := values[int(math.Ceil(float64(len(values))*p.value))-1]
		b.ReportMetric(float64(value)/float64(time.Microsecond), p.name)
	}
	b.ReportMetric(float64(b.N*requestsPerOp)/b.Elapsed().Seconds(), "requests/s")
	b.ReportMetric(float64(len(values)), "latency-samples")
}

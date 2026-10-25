package workpool

import (
	"context"
	"testing"
)

func BenchmarkPoolSubmitAndDrain(b *testing.B) {
	pool := New[int](4, 128, func(context.Context, int) {})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.Submit(context.Background(), i); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	pool.Close()
}

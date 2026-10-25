package orchestrator

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestStreamRouterResponseConcurrent(t *testing.T) {
	const streams = 100
	var wait sync.WaitGroup
	errors := make(chan error, streams)
	wait.Add(streams)
	for i := range streams {
		go func(i int) {
			defer wait.Done()
			want := strings.Repeat(fmt.Sprintf("stream-%03d-", i), 512)
			got := httptest.NewRecorder()
			if err := streamRouterResponse(got, strings.NewReader(want)); err != nil {
				errors <- err
				return
			}
			if got.Body.String() != want {
				errors <- fmt.Errorf("stream %d output mismatch", i)
			}
		}(i)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

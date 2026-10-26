package closeutil

import (
	"errors"
	"testing"
)

type recordingCloser struct {
	calls int
	err   error
}

func (c *recordingCloser) Close() error {
	c.calls++
	return c.err
}

func TestDiscardClosesOnceAndIgnoresError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "error", err: errors.New("close failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			closer := &recordingCloser{err: test.err}
			Discard(closer)
			if closer.calls != 1 {
				t.Fatalf("Close calls = %d, want 1", closer.calls)
			}
		})
	}
}

func TestDiscardEvaluatesCloserAtDefer(t *testing.T) {
	first, second := &recordingCloser{}, &recordingCloser{}
	func() {
		current := first
		defer Discard(current)
		current = second
		_ = current
	}()
	if first.calls != 1 || second.calls != 0 {
		t.Fatalf("calls = first %d second %d, want 1 and 0", first.calls, second.calls)
	}
}

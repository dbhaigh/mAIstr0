package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestDispatchFailureAddsPhaseAndPreservesCause(t *testing.T) {
	cause := errors.New("worker response was unavailable")
	failure := newDispatchFailure(dispatchPhaseExecution, cause)

	var got *dispatchFailure
	if !errors.As(failure, &got) || got.phase != dispatchPhaseExecution {
		t.Fatalf("dispatch failure = %#v, want execution phase", failure)
	}
	if !errors.Is(failure, cause) {
		t.Fatalf("dispatch failure %v does not wrap its cause", failure)
	}
	if !strings.Contains(failure.Error(), "execution phase") ||
		!strings.Contains(failure.Error(), cause.Error()) {
		t.Fatalf("dispatch failure message = %q", failure.Error())
	}
}

func TestRecordDispatchFailureEmitsStructuredTraceEvent(t *testing.T) {
	var output bytes.Buffer
	previous, previousOutput, previousFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
	})

	dispatcher := &dispatcherComponent{}
	dispatcher.recordDispatchFailure(
		context.Background(),
		"task-1",
		"subtask-1",
		&dispatchFailure{
			phase:      dispatchPhaseCapacityReservation,
			cause:      errors.New("sensitive worker response"),
			relatedErr: errors.New("sensitive scheduler response"),
			assignment: scheduler.Assignment{
				Subtask: scheduler.Subtask{ID: "subtask-1"},
				NodeID:  "node-7",
				Model:   "model-a",
				Engine:  "ollama",
			},
			retryable:     true,
			failureClass:  "capacity_reservation",
			policyAttempt: 2,
			httpStatus:    503,
		},
	)

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode structured dispatch event: %v", err)
	}
	if event["msg"] != "dispatch phase failed" ||
		event["event_type"] != "dispatch_error" ||
		event["request_id"] != "task-1:subtask-1" ||
		event["task_id"] != "task-1" ||
		event["subtask_id"] != "subtask-1" ||
		event["phase"] != string(dispatchPhaseCapacityReservation) ||
		event["node_id"] != "node-7" ||
		event["model"] != "model-a" ||
		event["failure_class"] != "capacity_reservation" ||
		event["retryable"] != true ||
		event["policy_attempt"] != float64(2) ||
		event["http_status"] != float64(503) {
		t.Fatalf("structured dispatch event = %+v", event)
	}
	errorType, hasErrorType := event["error_type"].(string)
	relatedErrorType, hasRelatedErrorType := event["related_error_type"].(string)
	if !hasErrorType || errorType == "" || !hasRelatedErrorType || relatedErrorType == "" {
		t.Fatalf("structured dispatch event is missing error type attributes: %+v", event)
	}
	if strings.Contains(output.String(), "sensitive worker response") ||
		strings.Contains(output.String(), "sensitive scheduler response") {
		t.Fatalf("structured dispatch event contains raw error text: %s", output.String())
	}
}

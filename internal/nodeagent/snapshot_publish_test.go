package nodeagent

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestPublishSnapshotSkipsTaskFetchWithoutSubscribers(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int32
	events := hub.New()
	link := &orchestratorLink{
		state:  &agentState{},
		events: events,
		currentTasks: func(context.Context) ([]*taskmgr.Task, error) {
			fetches.Add(1)
			return []*taskmgr.Task{{ID: "task-1"}}, nil
		},
	}
	link.background.Go(link.broadcastLoop)
	defer link.background.Stop()

	link.publishSnapshot()
	if got := fetches.Load(); got != 0 {
		t.Fatalf("currentTasks called %d times without subscribers", got)
	}

	server := httptest.NewServer(events)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !events.HasSubscribers() {
		select {
		case <-deadline.C:
			t.Fatal("dashboard subscriber was not registered")
		case <-poll.C:
		}
	}

	link.publishSnapshot()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "data: ") || !strings.Contains(line, `"task-1"`) {
		t.Fatalf("snapshot event = %q", line)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("currentTasks called %d times with a subscriber, want 1", got)
	}
}

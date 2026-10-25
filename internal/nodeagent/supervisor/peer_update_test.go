package supervisor

import "testing"

func TestPeerUpdateRequiresIdleManagedProcesses(t *testing.T) {
	for _, tc := range []struct {
		state string
		busy  bool
	}{
		{stateStopped, false}, {stateFailed, true}, {stateStarting, true}, {stateReady, true}, {stateStopping, true}, {stateRestarting, true},
	} {
		t.Run(tc.state, func(t *testing.T) {
			manager := &Manager{engines: map[string]*managedEngine{"worker": {status: Status{State: tc.state}}}}
			if idle := manager.UpdateIdle(); idle == tc.busy {
				t.Fatalf("state %s idle=%v", tc.state, idle)
			}
			manager.installingWorker = true
			if manager.UpdateIdle() {
				t.Fatal("installation did not block update")
			}
			manager.installingWorker = false
			manager.workerControls = 1
			if manager.UpdateIdle() {
				t.Fatal("pending engine control did not block update")
			}
		})
	}
}

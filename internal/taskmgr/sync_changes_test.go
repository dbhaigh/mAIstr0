package taskmgr

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/syncinterval"
)

func TestChangeSignalWakesAfterLocalTaskWrite(t *testing.T) {
	manager := NewManager()
	changes := manager.Changes()
	if _, err := manager.CreateChecked("local task", nil); err != nil {
		t.Fatal(err)
	}
	if !syncinterval.Wait(time.Second, changes) {
		t.Fatal("local task write did not wake the sync wait")
	}
}

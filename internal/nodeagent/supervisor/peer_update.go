package supervisor

func (m *Manager) UpdateIdle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.installingWorker || m.workerControls != 0 || m.closed {
		return false
	}
	for _, engine := range m.engines {
		engine.mu.Lock()
		busy := engine.command != nil || engine.status.State != stateStopped
		engine.mu.Unlock()
		if busy {
			return false
		}
	}
	return true
}

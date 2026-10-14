package nodeagent

import "context"

func (c *executorComponent) beginActiveTask() func() {
	c.state.mu.Lock()
	c.state.activeTasks++
	c.state.mu.Unlock()
	return func() {
		c.state.mu.Lock()
		c.state.activeTasks--
		c.state.mu.Unlock()
	}
}

func (c *executorComponent) beginExecution(ctx context.Context) (func(), error) {
	select {
	case c.executionSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	releaseActive := c.beginActiveTask()
	return func() {
		<-c.executionSlots
		releaseActive()
	}, nil
}

func (c *executorComponent) activeTaskCount() int {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return c.state.activeTasks
}

func (c *executorComponent) availableCapacity(maxConcurrent int) int {
	active := c.activeTaskCount()
	if active >= maxConcurrent {
		return 0
	}
	return maxConcurrent - active
}

func (a *Agent) activeTaskCount() int {
	return a.executorComponent.activeTaskCount()
}

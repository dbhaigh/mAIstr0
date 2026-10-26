package nodeagent

import "context"

func (c *executorComponent) beginActiveTask() func() {
	c.activeMu.Lock()
	c.activeTasks++
	c.activeMu.Unlock()
	return func() {
		c.activeMu.Lock()
		c.activeTasks--
		c.activeMu.Unlock()
	}
}

func (c *executorComponent) beginExecution(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case c.executionSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-c.executionSlots
		return nil, err
	}
	releaseActive := c.beginActiveTask()
	return func() {
		<-c.executionSlots
		releaseActive()
	}, nil
}

func (c *executorComponent) activeTaskCount() int {
	c.activeMu.Lock()
	defer c.activeMu.Unlock()
	return c.activeTasks
}

func (c *executorComponent) availableCapacity(maxConcurrent int) int {
	active := c.activeTaskCount()
	if active >= maxConcurrent {
		return 0
	}
	return maxConcurrent - active
}

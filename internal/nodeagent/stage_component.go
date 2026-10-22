package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/maistr0/maistr0/internal/engine"
)

var (
	errStageAssignmentConflict = errors.New("a different stage assignment is already loaded for this model range")
	errStageNotLoaded          = errors.New("stage assignment is not loaded")
	errStageUnavailable        = errors.New("stage assignment is unavailable")
	errStageEngineUnsupported  = errors.New("selected engine does not support pipeline stages")
	errStageModelUnavailable   = errors.New("stage model is not available on this node")
)

type stageEngineResolver func(engine.StageAssignment) (engine.PipelineStageEngine, error)

type stageComponent struct {
	mu       sync.Mutex
	stages   map[string]*loadedStage
	requests map[string]*activePipelineRequest
}

type loadedStage struct {
	assignment engine.StageAssignment
	runtime    engine.PipelineStageEngine
	ready      chan struct{}
	idle       chan struct{}
	loadErr    error
	loaded     bool
	unloading  bool
	inFlight   int
}

type stageReadinessSnapshot struct {
	SupportedModels    []string
	MaxLayersItCanHost int
	Assignments        int
}

type stageLease struct {
	component *stageComponent
	entry     *loadedStage
	once      sync.Once
}

type activePipelineRequest struct {
	mu              sync.Mutex
	assignment      engine.StageAssignment
	requestID       string
	runtime         engine.PipelineStageEngineV2
	lease           *stageLease
	releaseCapacity func()
}

func newStageComponent() *stageComponent {
	return &stageComponent{
		stages: make(map[string]*loadedStage), requests: make(map[string]*activePipelineRequest),
	}
}

func (c *stageComponent) load(
	ctx context.Context,
	assignment engine.StageAssignment,
	resolve stageEngineResolver,
) error {
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return err
	}
	key := nodeStageKey(assignment)
	c.mu.Lock()
	if current, exists := c.stages[key]; exists {
		if current.assignment != assignment {
			c.mu.Unlock()
			return errStageAssignmentConflict
		}
		c.mu.Unlock()
		if err := waitStageReady(ctx, current); err != nil {
			return err
		}
		return current.loadErr
	}
	entry := &loadedStage{
		assignment: assignment,
		ready:      make(chan struct{}),
		idle:       closedStageSignal(),
	}
	c.stages[key] = entry
	c.mu.Unlock()

	runtime, err := resolve(assignment)
	if err == nil && runtime == nil {
		err = errStageEngineUnsupported
	}
	if err == nil {
		entry.runtime = runtime
		err = runtime.LoadStage(ctx, assignment)
	}

	c.mu.Lock()
	entry.loadErr = err
	entry.loaded = err == nil
	if err != nil {
		delete(c.stages, key)
	}
	close(entry.ready)
	c.mu.Unlock()
	return err
}

func (c *stageComponent) unload(ctx context.Context, assignment engine.StageAssignment) error {
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return err
	}
	key := nodeStageKey(assignment)
	c.mu.Lock()
	entry, exists := c.stages[key]
	c.mu.Unlock()
	if !exists {
		return errStageNotLoaded
	}
	if err := waitStageReady(ctx, entry); err != nil {
		return err
	}

	c.mu.Lock()
	if c.stages[key] != entry || !entry.loaded {
		c.mu.Unlock()
		return errStageNotLoaded
	}
	if !stageAssignmentMatches(entry.assignment, assignment) {
		c.mu.Unlock()
		return errStageAssignmentConflict
	}
	if entry.unloading {
		c.mu.Unlock()
		return errStageUnavailable
	}
	entry.unloading = true
	idle := entry.idle
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.finishUnload(key, entry, false)
		return ctx.Err()
	case <-idle:
	}
	if err := entry.runtime.UnloadStage(ctx, entry.assignment); err != nil {
		c.finishUnload(key, entry, false)
		return err
	}
	c.finishUnload(key, entry, true)
	return nil
}

func (c *stageComponent) acquire(ctx context.Context, assignment engine.StageAssignment) (*stageLease, error) {
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return nil, err
	}
	key := nodeStageKey(assignment)
	c.mu.Lock()
	entry, exists := c.stages[key]
	c.mu.Unlock()
	if !exists {
		return nil, errStageNotLoaded
	}
	if err := waitStageReady(ctx, entry); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stages[key] != entry || !entry.loaded || entry.unloading {
		return nil, errStageUnavailable
	}
	if !stageAssignmentMatches(entry.assignment, assignment) {
		return nil, errStageAssignmentConflict
	}
	if entry.inFlight == 0 {
		entry.idle = make(chan struct{})
	}
	entry.inFlight++
	return &stageLease{component: c, entry: entry}, nil
}

func (c *stageComponent) openPipelineRequest(
	ctx context.Context,
	assignment engine.StageAssignment,
	requestID string,
	releaseCapacity func(),
) error {
	lease, err := c.acquire(ctx, assignment)
	if err != nil {
		return err
	}
	runtime, ok := lease.entry.runtime.(engine.PipelineStageEngineV2)
	if !ok {
		lease.release()
		return errStageEngineUnsupported
	}
	if err := runtime.OpenPipelineRequest(ctx, lease.entry.assignment, requestID); err != nil {
		lease.release()
		return err
	}
	request := &activePipelineRequest{
		assignment: lease.entry.assignment, requestID: requestID,
		runtime: runtime, lease: lease, releaseCapacity: releaseCapacity,
	}
	key := pipelineRequestKey(assignment, requestID)
	c.mu.Lock()
	if _, exists := c.requests[key]; exists {
		c.mu.Unlock()
		closeErr := runtime.ClosePipelineRequest(context.Background(), lease.entry.assignment, requestID)
		lease.release()
		return errors.Join(errors.New("pipeline request is already open"), closeErr)
	}
	c.requests[key] = request
	c.mu.Unlock()
	return nil
}

func (c *stageComponent) pipelineRequest(
	assignment engine.StageAssignment,
	requestID string,
) (*activePipelineRequest, error) {
	assignment.Normalize()
	key := pipelineRequestKey(assignment, requestID)
	c.mu.Lock()
	request, exists := c.requests[key]
	c.mu.Unlock()
	if !exists {
		return nil, errors.New("pipeline request is not open")
	}
	if !stageAssignmentMatches(request.assignment, assignment) {
		return nil, errStageAssignmentConflict
	}
	return request, nil
}

func (c *stageComponent) prefillPipelineStage(
	ctx context.Context,
	request engine.PipelineStageRequest,
) (engine.PipelineStageOutput, error) {
	active, err := c.pipelineRequest(request.Assignment, request.RequestID)
	if err != nil {
		return engine.PipelineStageOutput{}, err
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	return active.runtime.PrefillPipelineStage(ctx, active.assignment, request)
}

func (c *stageComponent) decodePipelineStage(
	ctx context.Context,
	request engine.PipelineStageRequest,
) (engine.PipelineStageOutput, error) {
	active, err := c.pipelineRequest(request.Assignment, request.RequestID)
	if err != nil {
		return engine.PipelineStageOutput{}, err
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	return active.runtime.DecodePipelineStage(ctx, active.assignment, request)
}

func (c *stageComponent) closePipelineRequest(
	ctx context.Context,
	assignment engine.StageAssignment,
	requestID string,
) error {
	active, err := c.pipelineRequest(assignment, requestID)
	if err != nil {
		return err
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	closeErr := active.runtime.ClosePipelineRequest(ctx, active.assignment, active.requestID)
	key := pipelineRequestKey(active.assignment, active.requestID)
	c.mu.Lock()
	if c.requests[key] == active {
		delete(c.requests, key)
	}
	c.mu.Unlock()
	active.lease.release()
	if active.releaseCapacity != nil {
		active.releaseCapacity()
	}
	return closeErr
}

func (lease *stageLease) release() {
	lease.once.Do(func() {
		component := lease.component
		component.mu.Lock()
		lease.entry.inFlight--
		if lease.entry.inFlight == 0 {
			close(lease.entry.idle)
		}
		component.mu.Unlock()
	})
}

func (c *stageComponent) finishUnload(key string, entry *loadedStage, remove bool) {
	c.mu.Lock()
	if c.stages[key] == entry {
		if remove {
			delete(c.stages, key)
		} else {
			entry.unloading = false
		}
	}
	c.mu.Unlock()
}

func (c *stageComponent) readinessSnapshot() stageReadinessSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	models := make(map[string]struct{})
	snapshot := stageReadinessSnapshot{}
	for _, stage := range c.stages {
		if !stage.loaded || stage.unloading {
			continue
		}
		models[stage.assignment.ModelID] = struct{}{}
		layerCount := stage.assignment.EndLayer - stage.assignment.StartLayer
		if layerCount > snapshot.MaxLayersItCanHost {
			snapshot.MaxLayersItCanHost = layerCount
		}
		snapshot.Assignments++
	}
	snapshot.SupportedModels = make([]string, 0, len(models))
	for model := range models {
		snapshot.SupportedModels = append(snapshot.SupportedModels, model)
	}
	sort.Strings(snapshot.SupportedModels)
	return snapshot
}

func waitStageReady(ctx context.Context, entry *loadedStage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-entry.ready:
		return entry.loadErr
	}
}

func closedStageSignal() chan struct{} {
	signal := make(chan struct{})
	close(signal)
	return signal
}

func stageAssignmentMatches(loaded, requested engine.StageAssignment) bool {
	return loaded.ModelID == requested.ModelID &&
		loaded.StartLayer == requested.StartLayer &&
		loaded.EndLayer == requested.EndLayer &&
		(requested.Engine == "" || loaded.Engine == requested.Engine) &&
		(requested.NextStageAddr == "" || loaded.NextStageAddr == requested.NextStageAddr) &&
		loaded.IsFinal == requested.IsFinal
}

func nodeStageKey(assignment engine.StageAssignment) string {
	return fmt.Sprintf("%s\x00%d\x00%d", assignment.ModelID, assignment.StartLayer, assignment.EndLayer)
}

func pipelineRequestKey(assignment engine.StageAssignment, requestID string) string {
	return nodeStageKey(assignment) + "\x00" + requestID
}

func (a *Agent) resolveStageEngine(assignment engine.StageAssignment) (engine.PipelineStageEngine, error) {
	local := a.engineForModel(assignment.ModelID, assignment.Engine)
	if local == nil {
		return nil, fmt.Errorf("%w: %q", errStageModelUnavailable, assignment.ModelID)
	}
	if runtime, ok := local.(engine.PipelineStageEngine); ok {
		return runtime, nil
	}
	endpoint, ok := local.(interface{ URL() string })
	if !ok {
		return nil, fmt.Errorf("%w: %s", errStageEngineUnsupported, local.Name())
	}
	return engine.NewHTTPPipelineStageEngine(local.Name(), endpoint.URL(), nil)
}

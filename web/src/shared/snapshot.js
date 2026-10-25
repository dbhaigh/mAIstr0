(function (root, factory) {
  const snapshot = factory(root);
  if (typeof module === "object" && module.exports) module.exports = snapshot;
  root.dashboardSnapshot = snapshot;
})(globalThis, (root) => {
  function hashSnapshot(serialized) {
    let hash = 2166136261;
    for (let i = 0; i < serialized.length; i++) {
      hash ^= serialized.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }

  function createSnapshotScheduler(onSnapshot, options = {}) {
    const requestFrame = options.requestAnimationFrameImpl ||
      root.requestAnimationFrame?.bind(root) ||
      ((callback) => root.setTimeout(callback, 0));
    const cancelFrame = options.cancelAnimationFrameImpl ||
      root.cancelAnimationFrame?.bind(root) ||
      root.clearTimeout?.bind(root);
    let pending = null;
    let lastHash = null;
    let lastSerialized = null;
    let scheduled = false;
    let frameID = null;

    const flush = () => {
      scheduled = false;
      frameID = null;
      const update = pending;
      pending = null;
      if (!update || (update.hash === lastHash && update.serialized === lastSerialized)) return;
      lastHash = update.hash;
      lastSerialized = update.serialized;
      onSnapshot(update.snapshot);
    };

    return Object.freeze({
      enqueue(snapshot) {
        const serialized = JSON.stringify(snapshot);
        const hash = hashSnapshot(serialized);
        if ((hash === lastHash && serialized === lastSerialized) ||
            (pending && hash === pending.hash && serialized === pending.serialized)) return false;
        pending = { snapshot, serialized, hash };
        if (!scheduled) {
          scheduled = true;
          frameID = requestFrame(flush);
        }
        return true;
      },
      cancel() {
        if (scheduled && cancelFrame && frameID !== null) cancelFrame(frameID);
        pending = null;
        scheduled = false;
        frameID = null;
      },
      flush,
    });
  }

  function mergeTaskSnapshot(currentTasks, incomingTasks, complete, fullTaskHistoryLoaded) {
    const tasks = incomingTasks || [];
    if (complete) return { tasks, fullTaskHistoryLoaded: true };
    if (!fullTaskHistoryLoaded) return { tasks, fullTaskHistoryLoaded: false };

    const merged = new Map((currentTasks || []).map((task) => [task.id, task]));
    for (const task of tasks) merged.set(task.id, task);
    return {
      tasks: [...merged.values()].sort((a, b) =>
        new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0)
      ),
      fullTaskHistoryLoaded: true,
    };
  }

  function connectDashboardEvents(onSnapshot, options = {}) {
    const EventSourceImpl = options.EventSourceImpl || root.EventSource;
    const onError = options.onError || ((error) => root.console.error(error));
    const onConnectionChange = options.onConnectionChange || (() => {});
    const scheduler = createSnapshotScheduler((snapshot) => {
      try {
        onSnapshot(snapshot);
      } catch (error) {
        onError(error);
      }
    }, options);
    const eventSource = new EventSourceImpl("/api/events");
    eventSource.onopen = () => onConnectionChange(true);
    eventSource.onerror = () => onConnectionChange(false);
    eventSource.onmessage = (event) => {
      try {
        scheduler.enqueue(JSON.parse(event.data));
      } catch (error) {
        onError(error);
      }
    };
    return eventSource;
  }

  return Object.freeze({ connectDashboardEvents, createSnapshotScheduler, hashSnapshot, mergeTaskSnapshot });
});

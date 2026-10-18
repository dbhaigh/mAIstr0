(function (root, factory) {
  const snapshot = factory(root);
  if (typeof module === "object" && module.exports) module.exports = snapshot;
  root.dashboardSnapshot = snapshot;
})(globalThis, (root) => {
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
    const eventSource = new EventSourceImpl("/api/events");
    eventSource.onmessage = (event) => {
      try {
        onSnapshot(JSON.parse(event.data));
      } catch (error) {
        onError(error);
      }
    };
    return eventSource;
  }

  return Object.freeze({ connectDashboardEvents, mergeTaskSnapshot });
});

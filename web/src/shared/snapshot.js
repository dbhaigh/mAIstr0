export const dashboardSnapshot = ((root) => {
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
    const reconnectBaseDelayMs = Math.max(0, options.reconnectBaseDelayMs ?? 1000);
    const reconnectMaxDelayMs = Math.max(reconnectBaseDelayMs, options.reconnectMaxDelayMs ?? 30000);
    const stableConnectionMs = Math.max(0, options.stableConnectionMs ?? 30000);
    const heartbeatTimeoutMs = Math.max(1, options.heartbeatTimeoutMs ?? 45000);
    const heartbeatCheckIntervalMs = Math.max(1, options.heartbeatCheckIntervalMs ?? 5000);
    const setTimeoutImpl = options.setTimeoutImpl || root.setTimeout.bind(root);
    const clearTimeoutImpl = options.clearTimeoutImpl || root.clearTimeout.bind(root);
    const setIntervalImpl = options.setIntervalImpl || root.setInterval.bind(root);
    const clearIntervalImpl = options.clearIntervalImpl || root.clearInterval.bind(root);
    const nowImpl = options.nowImpl || Date.now;
    const randomImpl = options.randomImpl || Math.random;
    const scheduler = createSnapshotScheduler((snapshot) => {
      try {
        onSnapshot(snapshot);
      } catch (error) {
        onError(error);
      }
    }, options);

    let eventSource = null;
    let reconnectTimer = null;
    let stableTimer = null;
    let healthTimer = null;
    let retryAttempt = 0;
    let lastHeartbeatAt = 0;
    let heartbeatSeen = false;
    let connected = false;
    let closed = false;

    function setConnected(next) {
      if (connected === next) return;
      connected = next;
      onConnectionChange(next);
    }

    function clearStableTimer() {
      if (stableTimer === null) return;
      clearTimeoutImpl(stableTimer);
      stableTimer = null;
    }

    function stopHealthMonitor() {
      if (healthTimer === null) return;
      clearIntervalImpl(healthTimer);
      healthTimer = null;
    }

    function scheduleReconnect() {
      if (closed || reconnectTimer !== null) return;
      const exponentialDelay = Math.min(
        reconnectMaxDelayMs,
        reconnectBaseDelayMs * (2 ** Math.min(retryAttempt, 30)),
      );
      const jitter = 0.8 + Math.max(0, Math.min(1, randomImpl())) * 0.4;
      const delay = Math.min(reconnectMaxDelayMs, Math.round(exponentialDelay * jitter));
      retryAttempt = Math.min(retryAttempt + 1, 30);
      reconnectTimer = setTimeoutImpl(() => {
        reconnectTimer = null;
        open();
      }, delay);
    }

    function disconnect(source) {
      if (closed || source !== eventSource) return;
      eventSource = null;
      source.close?.();
      clearStableTimer();
      stopHealthMonitor();
      heartbeatSeen = false;
      lastHeartbeatAt = 0;
      setConnected(false);
      scheduleReconnect();
    }

    function checkHealth(source) {
      if (closed || source !== eventSource) return;
      if (source.readyState === 2 || (connected && source.readyState === 0)) {
        disconnect(source);
        return;
      }
      if (heartbeatSeen && nowImpl() - lastHeartbeatAt >= heartbeatTimeoutMs) {
        onError(new Error("dashboard event stream heartbeat timed out"));
        disconnect(source);
      }
    }

    function open() {
      if (closed) return;
      let source;
      try {
        source = new EventSourceImpl("/api/events");
      } catch (error) {
        onError(error);
        setConnected(false);
        scheduleReconnect();
        return;
      }
      eventSource = source;
      source.onopen = () => {
        if (closed || source !== eventSource) return;
        setConnected(true);
        clearStableTimer();
        stableTimer = setTimeoutImpl(() => {
          stableTimer = null;
          if (source === eventSource && connected) retryAttempt = 0;
        }, stableConnectionMs);
      };
      source.onerror = () => disconnect(source);
      source.onmessage = (event) => {
        if (closed || source !== eventSource) return;
        try {
          scheduler.enqueue(JSON.parse(event.data));
        } catch (error) {
          onError(error);
        }
      };
      source.addEventListener?.("heartbeat", () => {
        if (closed || source !== eventSource) return;
        heartbeatSeen = true;
        lastHeartbeatAt = nowImpl();
      });
      healthTimer = setIntervalImpl(
        () => checkHealth(source),
        heartbeatCheckIntervalMs,
      );
    }

    function close() {
      if (closed) return;
      closed = true;
      scheduler.cancel();
      if (reconnectTimer !== null) {
        clearTimeoutImpl(reconnectTimer);
        reconnectTimer = null;
      }
      clearStableTimer();
      stopHealthMonitor();
      if (eventSource) {
        const source = eventSource;
        eventSource = null;
        source.close?.();
      }
      setConnected(false);
    }

    open();
    return Object.freeze({
      close,
      get currentSource() {
        return eventSource;
      },
    });
  }

  return Object.freeze({ connectDashboardEvents, createSnapshotScheduler, hashSnapshot, mergeTaskSnapshot });
})(globalThis);

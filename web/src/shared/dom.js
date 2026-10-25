(function (root, factory) {
  const dom = factory(root);
  if (typeof module === "object" && module.exports) module.exports = dom;
  root.dashboardDOM = dom;
})(globalThis, (root) => {
  const keyedState = new WeakMap();
  const virtualState = new WeakMap();

  function patchKeyedChildren(container, items, options) {
    if (!container || !options || typeof options.renderItem !== "function") return;
    const keyOf = options.keyOf || ((item) => item.id);
    const signatureOf = options.signatureOf || JSON.stringify;
    const previous = keyedState.get(container) || new Map();
    const desired = new Set(items.map((item, index) => String(keyOf(item, index))));

    for (const [key, entry] of previous) {
      if (!desired.has(key)) {
        entry.nodes.forEach((node) => node.remove());
        previous.delete(key);
      }
    }

    let cursor = container.firstElementChild;
    const next = new Map();
    for (const [index, item] of items.entries()) {
      const key = String(keyOf(item, index));
      const signature = signatureOf(item, index);
      let entry = previous.get(key);
      if (entry && entry.signature !== signature) {
        // Never anchor an insertion on a node that is about to be detached.
        while (cursor && entry.nodes.includes(cursor)) cursor = cursor.nextElementSibling;
        entry.nodes.forEach((node) => node.remove());
        entry = null;
      }
      if (!entry) {
        const rendered = options.renderItem(item, index);
        const nodes = Array.isArray(rendered) ? rendered : [rendered];
        for (const node of nodes) {
          node.dataset.renderKey = key;
          container.insertBefore(node, cursor);
        }
        entry = { signature, nodes };
      } else {
        for (const node of entry.nodes) {
          if (node !== cursor) container.insertBefore(node, cursor);
          cursor = node.nextElementSibling;
        }
      }
      if (!entry.nodes.length) continue;
      if (entry.nodes[0].parentNode === container) {
        cursor = entry.nodes[entry.nodes.length - 1].nextElementSibling;
      }
      next.set(key, entry);
    }
    keyedState.set(container, next);
  }

  function getVirtualRange(items, scrollTop, viewportHeight, options = {}) {
    const estimate = Math.max(1, options.estimateHeight || 96);
    const overscan = Math.max(0, options.overscan ?? 6);
    const heightOf = options.heightOf || (() => estimate);
    if (!items.length) return { start: 0, end: 0, top: 0, bottom: 0 };

    const offsets = new Array(items.length + 1);
    offsets[0] = 0;
    for (let i = 0; i < items.length; i++) {
      offsets[i + 1] = offsets[i] + Math.max(1, heightOf(items[i], i) || estimate);
    }

    const findIndex = (offset) => {
      let low = 0;
      let high = items.length;
      while (low < high) {
        const middle = (low + high) >>> 1;
        if (offsets[middle + 1] <= offset) low = middle + 1;
        else high = middle;
      }
      return low;
    };
    const first = findIndex(Math.max(0, scrollTop));
    const last = findIndex(Math.max(0, scrollTop) + Math.max(1, viewportHeight));
    const start = Math.max(0, first - overscan);
    const end = Math.min(items.length, last + overscan + 1);
    return {
      start,
      end,
      top: offsets[start],
      bottom: offsets[items.length] - offsets[end],
    };
  }

  function renderVirtualList(container, items, options) {
    if (!container || !options || typeof options.renderItem !== "function") return;
    let state = virtualState.get(container);
    if (!state) {
      const top = document.createElement("div");
      const content = document.createElement("div");
      const bottom = document.createElement("div");
      top.className = "virtual-list-spacer virtual-list-spacer-top";
      bottom.className = "virtual-list-spacer virtual-list-spacer-bottom";
      content.className = "virtual-list-items";
      top.setAttribute("aria-hidden", "true");
      bottom.setAttribute("aria-hidden", "true");
      container.replaceChildren(top, content, bottom);
      container.classList.add("virtual-list-container");
      state = { top, content, bottom, items: [], options, frame: 0, heights: new Map(), observer: null };
      state.onScroll = () => {
        if (state.frame) return;
        const raf = root.requestAnimationFrame || ((callback) => root.setTimeout(callback, 16));
        state.frame = raf(() => {
          state.frame = 0;
          renderVirtualList(container, state.items, state.options);
        });
      };
      container.addEventListener("scroll", state.onScroll, { passive: true });
      if (typeof root.ResizeObserver === "function") {
        state.observer = new root.ResizeObserver((entries) => {
          let changed = false;
          for (const entry of entries) {
            const key = entry.target.dataset.renderKey;
            const height = entry.contentRect.height;
            if (key && Number.isFinite(height) && height > 0 &&
                Math.abs((state.heights.get(key) || 0) - height) > 1) {
              state.heights.set(key, height);
              changed = true;
            }
          }
          if (changed) state.onScroll();
        });
      }
      virtualState.set(container, state);
    }

    container.setAttribute("aria-busy", "false");
    state.items = items;
    state.options = options;
    const keyOf = options.keyOf || ((item, index) => item.id ?? index);
    const heightOf = (item, index) => state.heights.get(String(keyOf(item, index))) || options.estimateHeight || 96;
    const threshold = options.threshold || 40;
    const range = items.length > threshold
      ? getVirtualRange(items, container.scrollTop, container.clientHeight || 600, {
        estimateHeight: options.estimateHeight,
        overscan: options.overscan,
        heightOf,
      })
      : { start: 0, end: items.length, top: 0, bottom: 0 };
    const visible = items.slice(range.start, range.end);
    const signatureOf = options.signatureOf || JSON.stringify;
    state.top.style.height = `${range.top}px`;
    state.bottom.style.height = `${range.bottom}px`;
    state.top.hidden = range.top === 0;
    state.bottom.hidden = range.bottom === 0;
    patchKeyedChildren(state.content, visible, {
      keyOf: (item, index) => keyOf(item, range.start + index),
      signatureOf,
      renderItem: (item, index) => options.renderItem(item, range.start + index),
    });
    state.observer?.disconnect();
    state.content.querySelectorAll(":scope > [data-render-key]").forEach((node) => state.observer?.observe(node));
  }

  function resetVirtualList(container, options = {}) {
    if (!container) return;
    const state = virtualState.get(container);
    if (state) {
      state.items = [];
      state.options = { ...options, keyOf: (node) => node.dataset.virtualKey, renderItem: (node) => node };
      state.heights.clear();
      container.scrollTop = 0;
      renderVirtualList(container, [], state.options);
      return;
    }
    container.scrollTop = 0;
    renderVirtualList(container, [], {
      ...options,
      keyOf: (node) => node.dataset.virtualKey,
      signatureOf: (node) => node.dataset.virtualSignature || "",
      renderItem: (node) => node,
    });
  }

  function appendVirtualListItem(container, node) {
    let state = virtualState.get(container);
    if (!state) {
      resetVirtualList(container);
      state = virtualState.get(container);
    }
    if (!node.dataset.virtualKey) node.dataset.virtualKey = `virtual-${state.items.length}`;
    state.items = [...state.items, node];
    const wasAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 48;
    renderVirtualList(container, state.items, state.options);
    if (wasAtBottom) container.scrollTop = container.scrollHeight;
  }

  function removeVirtualListItem(container, node) {
    const state = virtualState.get(container);
    if (!state) {
      node.remove();
      return;
    }
    state.items = state.items.filter((item) => item !== node);
    state.heights.delete(node.dataset.virtualKey);
    renderVirtualList(container, state.items, state.options);
  }

  function renderSkeleton(container, count = 3) {
    if (!container) return;
    const state = virtualState.get(container);
    if (state) {
      state.observer?.disconnect();
      if (state.frame) {
        const cancel = root.cancelAnimationFrame || root.clearTimeout;
        cancel?.call(root, state.frame);
      }
      container.removeEventListener("scroll", state.onScroll);
      virtualState.delete(container);
      keyedState.delete(state.content);
    }
    container.setAttribute("aria-busy", "true");
    const placeholders = Array.from({ length: count }, (_, index) => {
      const card = document.createElement("div");
      card.className = "skeleton-card";
      card.setAttribute("aria-hidden", "true");
      card.innerHTML = `<span class="skeleton skeleton-heading"></span><span class="skeleton"></span><span class="skeleton skeleton-short"></span>`;
      card.dataset.skeletonIndex = String(index);
      return card;
    });
    container.replaceChildren(...placeholders);
  }

  return Object.freeze({
    appendVirtualListItem,
    getVirtualRange,
    patchKeyedChildren,
    renderSkeleton,
    removeVirtualListItem,
    renderVirtualList,
    resetVirtualList,
  });
});

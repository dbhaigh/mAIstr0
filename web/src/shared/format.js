const dashboardFormat = (() => {
  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (character) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[character]);
  }

  function formatBytes(bytes) {
    const value = Number(bytes);
    if (!Number.isFinite(value) || value < 0) return "0 B";
    if (value < 1024) return `${value} B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
    return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
  }

  function formatRelativeTime(value) {
    if (!value) return "time unavailable";
    const timestamp = new Date(value).getTime();
    if (!Number.isFinite(timestamp)) return "time unavailable";
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return new Date(timestamp).toLocaleDateString();
  }

  function formatTimestamp(label, value) {
    if (!value) return `<span class="task-time-unavailable">${escapeHtml(label)} unavailable</span>`;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return `<span class="task-time-unavailable">${escapeHtml(label)} unavailable</span>`;
    }
    return `<time datetime="${escapeHtml(value)}">${escapeHtml(label)} ${escapeHtml(date.toLocaleString())}</time>`;
  }

  return Object.freeze({ escapeHtml, formatBytes, formatRelativeTime, formatTimestamp });
})();

const { escapeHtml, formatBytes, formatRelativeTime, formatTimestamp } = dashboardFormat;

export { dashboardFormat, escapeHtml, formatBytes, formatRelativeTime, formatTimestamp };

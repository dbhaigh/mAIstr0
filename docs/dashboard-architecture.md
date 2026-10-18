# Dashboard architecture

The dashboard is plain JavaScript, HTML, and CSS. Its source of truth is under
`web/src/`, organized into shared state/navigation/API helpers, reusable
components, and view files:

- Interactive Orchestrator: `views/orchestrator-chat.js`,
  `views/orchestrator-controls.js`, and `views/orchestrator-sessions.js`.
- Cluster & Workload: `views/cluster.js`, `views/cluster-setup.js`,
  `views/nodes.js`, `views/routing.js`, and the node/task components.
- Memory & Learning: `views/memory.js`.
- Pairing flows: `components/pairing-dialog.js`, with PIN display decisions
  projected by the pure shared view-model helpers.

`shared/view-model.js` contains DOM-independent status, task-count, engine
inventory, hardware-formatting, and pairing-PIN projections. `shared/snapshot.js`
owns incremental task-history merging and parsing/dispatching SSE snapshots.
Both expose CommonJS exports for direct Node tests and browser globals for the
static dashboard; neither requires a framework or runtime package.

From `web/`, run `npm run build` to concatenate the ordered source modules into
`web/static/app.js`. `static/index.html` loads that single generated script,
avoiding duplicate top-level declarations and stale parallel bundles.
`web/embed.go` embeds the complete `static/` directory in the Go binary, so the
dashboard remains available without external asset files. Run the focused UI
suite with `node --test web/app_test.js`; the pure shared modules are tested
directly, and integration-oriented rendering checks use the generated bundle.

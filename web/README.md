# Dashboard frontend

The dashboard source is organized by shared concerns, views, and reusable
components:

- `src/shared/state.js` owns the node, task, session, pairing, and capacity
  state shared across views.
- `src/shared/` contains navigation and API/status helpers.
- `src/views/orchestrator-*.js`, `src/views/cluster.js`, `src/views/nodes.js`,
  `src/views/routing.js`, and `src/views/memory.js` hold the view-specific
  behavior.
- `src/components/` contains pairing, node-card, and task-list behavior.
- `src/app.js` coordinates snapshots, SSE updates, and startup.

Task and memory history use opt-in cursor pages behind explicit “Load more”
controls. Existing full-list API responses remain available when paging
parameters are omitted, and the dashboard keeps live task updates merged into
the currently loaded task pages.

These source modules are assembled in a stable order and bundled as one
classic script. This keeps the existing browser loading and `web.StaticFiles`
embed contract unchanged; the browser does not load modules or require Node.
`static/app.js` is generated output.

From the repository root, install the build dependency and regenerate the
embedded asset with:

```sh
npm --prefix web ci
npm --prefix web run build
node --test web/app_test.js
go test ./web/...
```

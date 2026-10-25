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

The ES-module entry point bundles the source as a classic script so the
existing browser loading and `web.StaticFiles` embed contract stays unchanged;
the browser does not load modules or require Node. The build emits stable
`static/app.js` and `static/style.css` paths for compatibility, plus
content-hashed JS/CSS assets referenced by `static/index.html`. It also writes
precompressed gzip and Brotli sidecars. The embedded handler negotiates those
encodings, uses ETags, and applies immutable caching to hashed assets.
The stable aliases remain supported compatibility URLs; dashboard tests execute
the hashed script actually referenced by the page and verify that the JavaScript
alias and its compressed sidecars match it.

SSE snapshots are coalesced into animation frames and identical snapshots are
skipped. Keyed list updates preserve stable DOM nodes; long task, memory, and
chat histories are windowed, and inactive dashboard panels defer their
initial loading until activated.

From the repository root, install the build dependency and regenerate the
embedded asset with:

```sh
npm --prefix web ci
npm --prefix web run build
node --test web/app_test.js
go test ./web/...
```

Commit regenerated assets whenever frontend source or styles change. Linux CI
rebuilds with the locked dependency and rejects modified or untracked assets in
`static/`, then both CI platforms run the dashboard tests.
The build normalizes text assets to LF before hashing and compression so Windows
checkout line endings do not change the embedded asset names or sidecars.

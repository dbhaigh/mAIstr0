# Dashboard architecture

The dashboard is plain JavaScript, HTML, and CSS. Its source of truth is under
`web/src/`, organized into shared helpers, reusable components, and views:

- Interactive Orchestrator: `views/orchestrator-chat.js`,
  `views/orchestrator-controls.js`, and `views/orchestrator-sessions.js`.
- Cluster & Workload: `views/cluster.js`, `views/cluster-setup.js`,
  `views/nodes.js`, `views/routing.js`, and the node/task components.
- Memory & Learning: `views/memory.js`.
- Pairing flows: `components/pairing-dialog.js`; shared view-model functions
  project PIN display state without changing the pairing API.

All source modules use explicit ES-module imports and exports. The
DOM-independent `shared/view-model.js`, `shared/snapshot.js`, `shared/dom.js`,
and `shared/format.js` helpers are imported by both the app and Node tests.
`entry.js` lists the app modules; `scripts/build.mjs` uses esbuild with
`bundle: true` and `format: "iife"` to produce one classic browser script. The
browser does not fetch modules, and application functions are not attached to
`window`.

The Go binary embeds `web/static/` through `web/embed.go`. That directory
contains the page, hashed JS/CSS, and Brotli representations; it does not
duplicate stable aliases or gzip files. The handler resolves `/app.js` and
`/style.css` to the hashes referenced by the page, and generates/caches gzip
representations at runtime. Generated files remain committed because ordinary
Go builds need them; CI rebuilds and checks for drift.

The SSE endpoint retains full snapshots and its existing data-only framing.
The hub coalesces build requests and suppresses byte-identical frames. The
client tracks section signatures to avoid redundant DOM updates. Current
measurements and a future, versioned delta-event proposal are in
[dashboard-events.md](./dashboard-events.md).

Run the frontend checks from the repository root with `npm --prefix web run
build`, `node --test web/app_test.js`, and `go test ./web/...`.

# Dashboard frontend

The source is organized into shared state/navigation/API helpers, view logic,
and reusable components under `src/`. `src/entry.js` imports the modules, and
`src/style.css` is the stylesheet source. Task and memory history use cursor
pages behind “Load more” controls; existing unpaged API responses remain
available.

Source files use native ES-module imports and exports. The esbuild script
bundles the entry point as a classic browser script, preserving the existing
page loading and `go:embed` contract without exposing application functions as
browser globals. Node tests import pure helper modules directly and use a
module-syntax adapter for focused VM tests.

The committed `static/` directory contains `index.html`, content-hashed JS/CSS,
and Brotli sidecars. The Go handler preserves `/app.js` and `/style.css` as
compatibility URLs by resolving them to the assets referenced by `index.html`.
It creates and caches gzip representations on first request; Brotli remains
precompressed because the standard library has no Brotli encoder. ETags,
encoding negotiation, and immutable caching for hashed URLs remain enabled.
`go:embed static` requires generated files in the tree, so keep them committed
and retain CI's rebuild/drift check.

SSE still uses full JSON snapshots with the existing `data: <json>\n\n` framing.
The publisher coalesces requests and suppresses byte-identical complete
snapshots; gzip is negotiated for the stream and flushed per event. The client
skips DOM updates for sections whose signatures are unchanged. See
[dashboard event measurements and the proposed delta protocol](../docs/dashboard-events.md).

From the repository root, build and validate the dashboard with:

```sh
npm --prefix web ci
npm --prefix web run build
node --test web/app_test.js
go test ./web/...
```

Commit regenerated assets whenever frontend source or styles change. The build
normalizes text assets to LF before hashing and compression so Windows checkout
line endings do not change asset names or sidecars.

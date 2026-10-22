import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { transform } from "esbuild";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sourceFiles = [
  "src/shared/state.js",
  "src/shared/view-model.js",
  "src/shared/snapshot.js",
  "src/shared/navigation.js",
  "src/components/pairing-dialog.js",
  "src/views/orchestrator-controls.js",
  "src/views/cluster-setup.js",
  "src/shared/api.js",
  "src/views/orchestrator-sessions.js",
  "src/components/node-card.js",
  "src/views/orchestrator-chat.js",
  "src/views/memory.js",
  "src/views/cluster.js",
  "src/views/nodes.js",
  "src/views/pipelines.js",
  "src/views/routing.js",
  "src/components/task-list.js",
  "src/app.js",
];
const input = (await Promise.all(
  sourceFiles.map((file) => readFile(path.join(webRoot, file), "utf8")),
)).join("\n");
const { code, warnings } = await transform(input, {
  charset: "utf8",
  loader: "js",
  sourcefile: "dashboard.js",
  target: "es2022",
});
if (warnings.length > 0) {
  console.warn(warnings.map((warning) => warning.text).join("\n"));
}

const output = path.join(webRoot, "static", "app.js");
await mkdir(path.dirname(output), { recursive: true });
await writeFile(output, code);
console.log(`Built ${path.relative(webRoot, output)} from ${sourceFiles.length} source modules.`);

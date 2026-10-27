import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants as zlibConstants } from "node:zlib";
import { build } from "esbuild";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const staticDir = path.join(webRoot, "static");
await mkdir(staticDir, { recursive: true });
const sourceRoot = path.join(webRoot, "src");
const { outputFiles, warnings } = await build({
  absWorkingDir: webRoot,
  bundle: true,
  charset: "utf8",
  entryPoints: ["src/entry.js"],
  format: "iife",
  outfile: "dashboard.js",
  platform: "browser",
  target: "es2022",
  write: false,
});
if (warnings.length > 0) {
  console.warn(warnings.map((warning) => warning.text).join("\n"));
}
if (outputFiles.length !== 1) throw new Error(`expected one dashboard bundle, got ${outputFiles.length}`);
const bundle = outputFiles[0].text;
const bundleHash = createHash("sha256").update(bundle).digest("hex").slice(0, 12);
const css = Buffer.from((await readFile(path.join(sourceRoot, "style.css"), "utf8")).replace(/\r\n/g, "\n"));
const cssHash = createHash("sha256").update(css).digest("hex").slice(0, 12);
const bundleName = `app.${bundleHash}.js`;
const cssName = `style.${cssHash}.css`;

for (const name of await readdir(staticDir)) {
  if (/^(?:app(?:\.[a-f0-9]{12})?\.js|style(?:\.[a-f0-9]{12})?\.css)(?:\.(?:br|gz))?$/.test(name) ||
      /^index\.html\.(?:br|gz)$/.test(name)) {
    await rm(path.join(staticDir, name));
  }
}

await writeFile(path.join(staticDir, bundleName), bundle);
await writeFile(path.join(staticDir, cssName), css);
const indexPath = path.join(staticDir, "index.html");
const index = (await readFile(indexPath, "utf8"))
  .replace(/href="\/style(?:\.[a-f0-9]{12})?\.css"/, `href="/${cssName}"`)
  .replace(/src="\/app(?:\.[a-f0-9]{12})?\.js"/, `src="/${bundleName}"`);
await writeFile(indexPath, index);

for (const name of await readdir(staticDir)) {
  if (/\.(?:br|gz)$/.test(name)) continue;
  let asset = await readFile(path.join(staticDir, name));
  if (/\.(?:html|js|css)$/.test(name)) {
    asset = Buffer.from(asset.toString("utf8").replace(/\r\n/g, "\n"));
    await writeFile(path.join(staticDir, name), asset);
  }
  await writeFile(
    path.join(staticDir, `${name}.br`),
    brotliCompressSync(asset, {
      params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 11 },
    }),
  );
}

console.log(`Built ${bundleName} and ${cssName} with Brotli sidecars and on-demand gzip.`);

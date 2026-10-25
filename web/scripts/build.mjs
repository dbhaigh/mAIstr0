import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants as zlibConstants, gzipSync } from "node:zlib";
import { transform } from "esbuild";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const staticDir = path.join(webRoot, "static");
await mkdir(staticDir, { recursive: true });
const sourceRoot = path.join(webRoot, "src");
const entry = await readFile(path.join(sourceRoot, "entry.js"), "utf8");
const sourceFiles = [...entry.matchAll(/^import\s+"([^"]+)";$/gm)].map((match) => {
  const sourcePath = path.resolve(sourceRoot, match[1]);
  if (!sourcePath.startsWith(`${sourceRoot}${path.sep}`)) {
    throw new Error(`dashboard entry import escapes source directory: ${match[1]}`);
  }
  return sourcePath;
});
if (sourceFiles.length === 0) throw new Error("dashboard entry has no source imports");
const input = (await Promise.all(sourceFiles.map((file) => readFile(file, "utf8")))).join("\n");
const { code, warnings } = await transform(input, {
  charset: "utf8",
  loader: "js",
  sourcefile: "dashboard.js",
  target: "es2022",
});
if (warnings.length > 0) {
  console.warn(warnings.map((warning) => warning.text).join("\n"));
}
const bundle = code;
const bundleHash = createHash("sha256").update(bundle).digest("hex").slice(0, 12);
const css = Buffer.from((await readFile(path.join(staticDir, "style.css"), "utf8")).replace(/\r\n/g, "\n"));
const cssHash = createHash("sha256").update(css).digest("hex").slice(0, 12);
const bundleName = `app.${bundleHash}.js`;
const cssName = `style.${cssHash}.css`;

for (const name of await readdir(staticDir)) {
  if (/^(?:app|style)\.[a-f0-9]{12}\.(?:js|css)(?:\.(?:br|gz))?$/.test(name)) {
    await rm(path.join(staticDir, name));
  }
}

await writeFile(path.join(staticDir, "app.js"), bundle);
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
  await writeFile(path.join(staticDir, `${name}.gz`), gzipSync(asset, { level: 9 }));
  await writeFile(
    path.join(staticDir, `${name}.br`),
    brotliCompressSync(asset, {
      params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 11 },
    }),
  );
}

console.log(`Built ${bundleName} and ${cssName} with gzip and Brotli sidecars.`);

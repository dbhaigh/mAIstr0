import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

export function validatePolicy(policy) {
  if (policy.schema_version !== 1 || !/^[a-f0-9]{40}$/.test(policy.baseline_commit) ||
      !Number.isInteger(policy.rounds) || policy.rounds < 6 || policy.rounds > 20 ||
      !Number.isInteger(policy.gomaxprocs) || policy.gomaxprocs < 1 || policy.gomaxprocs > 8 ||
      !/^[1-9]\d*(ms|s)$/.test(policy.benchtime) ||
      !Array.isArray(policy.benchmarks) || policy.benchmarks.length === 0) {
    throw new Error("Invalid benchmark policy or insufficient repetitions");
  }
  for (const name of ["timing_ratio", "allocation_ratio"]) {
    if (!Number.isFinite(policy[name]) || policy[name] <= 1) throw new Error(`Invalid ${name}`);
  }
  for (const name of ["timing_absolute_ns", "maximum_relative_range", "allocation_absolute_bytes", "allocation_absolute_count"]) {
    if (!Number.isFinite(policy[name]) || policy[name] <= 0) throw new Error(`Invalid ${name}`);
  }
  const packages = new Set();
  for (const group of policy.benchmarks) {
    if (!/^\.\/internal\/[a-z]+$/.test(group.package) || packages.has(group.package) ||
        !Array.isArray(group.names) || group.names.length === 0 ||
        group.names.some((name) => !/^Benchmark[A-Za-z0-9_]+$/.test(name)) ||
        new Set(group.names).size !== group.names.length) {
      throw new Error("Invalid or duplicate benchmark group");
    }
    packages.add(group.package);
  }
  return policy;
}

export function parseEvents(text, groups) {
  const outputs = new Map();
  for (const line of text.split(/\r?\n/).filter(Boolean)) {
    const event = JSON.parse(line);
    if (event.Action === "fail") throw new Error("Go benchmark reported failure");
    if (event.Action === "output" && typeof event.Output === "string" && typeof event.Package === "string") {
      outputs.set(event.Package, (outputs.get(event.Package) || "") + event.Output);
    }
  }
  const samples = {};
  for (const [pkg, output] of outputs) {
    for (const line of output.split(/\r?\n/)) {
      const match = /^(Benchmark\S+)\s+(\d+)\s+(.*\bns\/op.*)$/.exec(line);
      if (!match) continue;
      const name = match[1].replace(/-\d+$/, "");
      const key = `${pkg}:${name}`;
      if (samples[key]) throw new Error(`Duplicate sample: ${key}`);
      const metrics = {};
      for (const metric of match[3].matchAll(/([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s+(\S+)/g)) {
        if (Object.hasOwn(metrics, metric[2])) throw new Error(`Duplicate metric: ${key}`);
        metrics[metric[2]] = Number(metric[1]);
      }
      if (Number(match[2]) <= 0 || !Object.values(metrics).every((value) => Number.isFinite(value) && value >= 0) ||
          !["ns/op", "B/op", "allocs/op"].every((unit) =>
        Number.isFinite(metrics[unit]) && metrics[unit] >= (unit === "ns/op" ? Number.MIN_VALUE : 0))) {
        throw new Error(`Invalid or missing benchmark metrics: ${key}`);
      }
      samples[key] = metrics;
    }
  }
  for (const group of groups) {
    for (const name of group.names) {
      const packages = [...outputs.keys()].filter((pkg) => pkg.endsWith(group.package.slice(1)));
      if (!packages.some((pkg) => Object.keys(samples).some((key) =>
        key === `${pkg}:${name}` || key.startsWith(`${pkg}:${name}/`)))) {
        throw new Error(`Missing benchmark: ${group.package}/${name}`);
      }
    }
  }
  if (Object.keys(samples).length === 0) throw new Error("No benchmark samples");
  return samples;
}

function summarize(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return {
    min: sorted[0],
    median: sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2,
    max: sorted.at(-1),
  };
}

export function compare(policy, before, after) {
  validatePolicy(policy);
  if (before.length !== policy.rounds || after.length !== policy.rounds) throw new Error("Missing benchmark rounds");
  const keys = Object.keys(before[0]).sort();
  if (keys.length === 0) throw new Error("No baseline samples");
  for (const round of [...before, ...after]) {
    if (JSON.stringify(Object.keys(round).sort()) !== JSON.stringify(keys)) throw new Error("Benchmark case sets differ");
  }
  let regression = false;
  let inconclusive = false;
  const results = keys.map((key) => {
    const metrics = {};
    for (const [unit, ratio, absolute] of [
      ["ns/op", policy.timing_ratio, policy.timing_absolute_ns],
      ["B/op", policy.allocation_ratio, policy.allocation_absolute_bytes],
      ["allocs/op", policy.allocation_ratio, policy.allocation_absolute_count],
    ]) {
      const values = (rounds) => rounds.map((round) => {
        const value = round[key][unit];
        if (!Number.isFinite(value) || value < 0 || (unit === "ns/op" && value === 0)) {
          throw new Error(`Invalid ${unit}: ${key}`);
        }
        return value;
      });
      const baseline = summarize(values(before)), candidate = summarize(values(after));
      const delta = candidate.median - baseline.median;
      const slower = candidate.median > baseline.median * ratio && delta > absolute;
      const separated = candidate.min > baseline.max;
      let status = "passed";
      if (unit === "ns/op") {
        const material = Math.max(baseline.median, candidate.median) >= absolute;
        const noisy = [baseline, candidate].some((s) => (s.max - s.min) / s.median > policy.maximum_relative_range);
        if (!material) {
          status = "below-timing-floor";
        } else if (noisy || (slower && !separated)) {
          status = "inconclusive";
          inconclusive = true;
        } else if (slower) {
          status = "regression";
          regression = true;
        }
      } else if (slower) {
        if (separated) {
          status = "regression";
          regression = true;
        } else {
          status = "inconclusive";
          inconclusive = true;
        }
      }
      metrics[unit] = { baseline, candidate, delta, ratio: baseline.median ? candidate.median / baseline.median : null, status };
    }
    // These are fixture input sizes, not performance results.
    for (const unit of ["nodes/op", "tasks/op"]) {
      const expected = before[0][key][unit];
      if ([...before, ...after].some((round) => round[key][unit] !== expected)) {
        throw new Error(`Workload size changed (${unit}): ${key}`);
      }
    }
    return { benchmark: key, metrics };
  });
  if (!results.some((result) => result.metrics["ns/op"].status !== "below-timing-floor")) {
    inconclusive = true;
  }
  return { status: regression ? "regression" : inconclusive ? "inconclusive" : "passed", results };
}

function command(executable, args, cwd, env) {
  const result = spawnSync(executable, args, { cwd, env, encoding: "utf8", maxBuffer: 64 << 20, timeout: 600000 });
  if (result.error || result.status !== 0) {
    throw new Error(`${executable} ${args.join(" ")} failed: ${result.error?.message || result.stderr.slice(-8000) || result.status}`);
  }
  return result.stdout;
}

export function run(policy, baseline, candidate, output) {
  validatePolicy(policy);
  fs.mkdirSync(output);
  const report = {
    schema_version: 1, status: "inconclusive", policy,
    environment: { at: new Date().toISOString(), os: process.platform, arch: process.arch,
      cpu: os.cpus()[0]?.model, logical_cpus: os.cpus().length, node: process.version,
      gomaxprocs: policy.gomaxprocs, hardware_inference: false },
    before: [], after: [],
  };
  try {
    if (process.platform !== "linux" || process.arch !== "x64") throw new Error("Timing qualification requires Linux amd64");
    const env = { ...process.env, GOMAXPROCS: String(policy.gomaxprocs), GOFLAGS: "", CGO_ENABLED: "0", GOOS: "linux", GOARCH: "amd64" };
    report.environment.go = command("go", ["version"], candidate, env).trim();
    report.baseline_commit = command("git", ["rev-parse", "HEAD"], baseline, env).trim();
    report.candidate_commit = command("git", ["rev-parse", "HEAD"], candidate, env).trim();
    report.candidate_dirty = command("git", ["status", "--porcelain"], candidate, env).trim() !== "";
    if (report.baseline_commit !== policy.baseline_commit) throw new Error("Baseline checkout does not match approved commit");
    const requirements = [baseline, candidate].map((dir) => /^go (\S+)$/m.exec(fs.readFileSync(path.join(dir, "go.mod"), "utf8"))?.[1]);
    if (!requirements[0] || requirements[0] !== requirements[1] || !report.environment.go.includes(`go${requirements[1]} `)) {
      throw new Error("Toolchain changed; reviewed baseline requalification is required");
    }
    const packages = policy.benchmarks.map((group) => group.package);
    const pattern = `^(${policy.benchmarks.flatMap((group) => group.names).join("|")})$`;
    for (const dir of [baseline, candidate]) {
      command("go", ["test", "-run", "^$", ...packages], dir, env);
    }
    for (let round = 0; round < policy.rounds; round++) {
      // Alternating AB/BA mitigates warm-cache and host-drift order effects.
      for (const side of round % 2 ? ["after", "before"] : ["before", "after"]) {
        const text = command("go", ["test", "-json", "-run", "^$", "-bench", pattern, "-benchmem",
          `-benchtime=${policy.benchtime}`, "-count=1", "-timeout=5m", ...packages],
        side === "before" ? baseline : candidate, env);
        fs.writeFileSync(path.join(output, `${side}-${round + 1}.jsonl`), text, { flag: "wx", mode: 0o600 });
        report[side].push(parseEvents(text, policy.benchmarks));
      }
    }
    Object.assign(report, compare(policy, report.before, report.after));
  } catch (error) {
    report.error = error.message;
  }
  fs.writeFileSync(path.join(output, "comparison.json"), JSON.stringify(report, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  return report;
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try {
    const [config, baseline, candidate, output, ...extra] = process.argv.slice(2);
    if (!output || extra.length) throw new Error("Usage: node build/benchmark-gate.mjs CONFIG BASELINE CANDIDATE NEW_OUTPUT_DIRECTORY");
    const report = run(JSON.parse(fs.readFileSync(config, "utf8")), baseline, candidate, output);
    console.log(`Benchmark qualification: ${report.status}${report.error ? `: ${report.error}` : ""}`);
    process.exitCode = report.status === "passed" ? 0 : report.status === "regression" ? 1 : 2;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 2;
  }
}

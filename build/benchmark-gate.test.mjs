import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import { compare, parseEvents, validatePolicy } from "./benchmark-gate.mjs";

const policy = JSON.parse(fs.readFileSync(new URL("./benchmark-gate.json", import.meta.url), "utf8"));
const key = "example:BenchmarkFixture";
function rounds(ns, bytes = 1000, allocs = 10) {
  return Array.from({ length: policy.rounds }, () => ({ [key]: { "ns/op": ns, "B/op": bytes, "allocs/op": allocs } }));
}

test("approved policy is valid and cannot silently drop repetitions or thresholds", () => {
  validatePolicy(policy);
  assert.throws(() => validatePolicy({ ...policy, rounds: 1 }), /repetitions/);
  assert.throws(() => validatePolicy({ ...policy, timing_ratio: 0 }), /timing_ratio/);
});

test("sustained material timing and allocation regressions fail", () => {
  assert.equal(compare(policy, rounds(1000000), rounds(1800000)).status, "regression");
  assert.equal(compare(policy, rounds(1000000), rounds(1000000, 1500)).status, "regression");
  assert.equal(compare(policy, rounds(1000000), rounds(1000000, 1000, 15)).status, "regression");
});

test("normal variance, improvements and microsecond fluctuations do not fail", () => {
  assert.equal(compare(policy, rounds(1000000), rounds(1100000, 1100, 11)).status, "passed");
  assert.equal(compare(policy, rounds(1000000), rounds(700000, 700, 7)).status, "passed");
  const before = rounds(1000000), after = rounds(1000000);
  before.forEach((round) => { round["example:BenchmarkSmall"] = { "ns/op": 1000, "B/op": 0, "allocs/op": 0 }; });
  after.forEach((round) => { round["example:BenchmarkSmall"] = { "ns/op": 5000, "B/op": 0, "allocs/op": 0 }; });
  assert.equal(compare(policy, before, after).status, "passed");
});

test("noisy, overlapping or entirely below-floor timing is inconclusive, not a pass", () => {
  const noisy = rounds(1000000);
  noisy[0][key]["ns/op"] = 2000000;
  assert.equal(compare(policy, noisy, rounds(1800000)).status, "inconclusive");
  const overlap = rounds(1800000);
  overlap[0][key]["ns/op"] = 900000;
  assert.equal(compare(policy, rounds(1000000), overlap).status, "inconclusive");
  assert.equal(compare(policy, rounds(1000), rounds(1000)).status, "inconclusive");
  const allocationOverlap = rounds(1000000, 1600);
  allocationOverlap[0][key]["B/op"] = 900;
  assert.equal(compare(policy, rounds(1000000), allocationOverlap).status, "inconclusive");
});

test("missing cases, rounds, metrics or changed fixture sizes cannot pass", () => {
  assert.throws(() => compare(policy, rounds(1000000).slice(1), rounds(1000000)), /rounds/);
  assert.throws(() => compare(policy, rounds(1000000), rounds(1000000).map(() => ({}))), /case sets/);
  const invalid = rounds(1000000);
  invalid[0][key]["B/op"] = NaN;
  assert.throws(() => compare(policy, invalid, rounds(1000000)), /Invalid B/);
  const changed = rounds(1000000);
  changed[0][key]["nodes/op"] = 100;
  assert.throws(() => compare(policy, rounds(1000000), changed), /Workload size/);
});

test("Go JSON output fragments are reassembled and failures/missing metrics rejected", () => {
  const event = (Output) => JSON.stringify({ Action: "output", Package: "example/internal/example", Output });
  const groups = [{ package: "./internal/example", names: ["BenchmarkFixture"] }];
  const text = `${event("BenchmarkFixture-2  ")}\n${event("100  1000000 ns/op  1000 B/op  10 allocs/op\n")}\n`;
  assert.equal(parseEvents(text, groups)["example/internal/example:BenchmarkFixture"]["ns/op"], 1000000);
  assert.throws(() => parseEvents(text, [{ package: "./internal/missing", names: ["BenchmarkFixture"] }]), /Missing benchmark/);
  assert.throws(() => parseEvents("", groups), /Missing benchmark/);
  assert.throws(() => parseEvents(text + JSON.stringify({ Action: "fail" }), groups), /failure/);
  assert.throws(() => parseEvents(text.replace("10 allocs/op", ""), groups), /missing benchmark metrics/);
  assert.throws(() => parseEvents(text.replace("1000 B/op", "1e999 B/op"), groups), /Invalid/);
  assert.throws(() => parseEvents(text + text, groups), /Duplicate sample/);
  assert.throws(() => parseEvents("{truncated", groups), SyntaxError);
});

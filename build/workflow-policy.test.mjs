import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const directory = new URL("../.github/workflows/", import.meta.url);
const workflows = fs.readdirSync(directory).filter((name) => /\.ya?ml$/.test(name))
  .map((name) => ({ name, text: fs.readFileSync(new URL(name, directory), "utf8") }));

test("all external workflow actions are pinned to immutable commits", () => {
  assert.ok(workflows.length > 0);
  for (const { name, text } of workflows) {
    for (const match of text.matchAll(/^\s*(?:-\s*)?uses:\s+(\S+)/gm)) {
      if (match[1].startsWith("./")) continue;
      assert.match(match[1], /^[\w.-]+\/[\w./-]+@[a-f0-9]{40}$/, `${name}: mutable action ${match[1]}`);
    }
  }
});

test("workflow defaults are read-only and release credentials are not inherited", () => {
  for (const { name, text } of workflows) {
    assert.match(text, /^permissions:\r?\n  contents: read\r?$/m, `${name}: missing read-only default`);
    assert.doesNotMatch(text, /^\s*secrets:\s*inherit/m, `${name}: inherited credentials`);
    if (name !== "release.yml") {
      assert.doesNotMatch(text, /^\s+contents:\s*write/m, `${name}: unexpected write permission`);
    }
  }
});

test("workflow policy rejects error suppression and privileged PR execution", () => {
  for (const { name, text } of workflows) {
    assert.doesNotMatch(text, /^\s*pull_request_target:/m, `${name}: privileged PR trigger`);
    for (const line of text.split(/\r?\n/)) {
      if (/^\s*continue-on-error:/.test(line)) {
        assert.match(line, /^\s*continue-on-error:\s*false\s*(?:#.*)?$/, `${name}: error suppression`);
      }
      assert.doesNotMatch(line, /\|\|\s*(?:true|:)\s*(?:#.*)?$/, `${name}: ignored shell failure`);
    }
  }
});

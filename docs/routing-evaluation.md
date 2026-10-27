# Task routing and decomposition evaluation

## Scope and method

The offline fixture harness is
[TestRoutingQualityFixtures](../internal/scheduler/routing_quality_test.go).
It contains 40 synthetic task descriptions: 10 each for sequential cues, list
items, action conjunctions, and role inference. Every fixture specifies the
expected task count and order, task types, roles, dependency shape, synthesis
marker, and node/model assignment.

The test calls the production `scheduler.Decompose` and `scheduler.AssignGraph`
against a fixed seven-node cluster. Its model inventory uses
[`engine.NewSimulated`](../internal/engine/simulated.go) for the general and
code models and synthetic tagged models for other task types. No network or
inference is involved in this offline measurement. Assignment latency is
measured over 100 `AssignGraph` calls per fixture with scheduler info logging
disabled.

- **Exact decomposition** counts fixtures where task count/order, task type,
  role, index-based dependencies, and synthesis markers all match.
- **Exact assignment** counts expected subtasks assigned to their expected
  node *and* model.
- Component counts are out of 105 expected subtasks; exact decomposition and
  count matches are out of 40 fixtures.
- Assignment latency is the mean, p50, and p95 across the 40 fixture-level
  average call times. It excludes task generation and HTTP dispatch.
- Failures are classified as decomposition count/type/role/dependency/
  synthesis mismatches or assignment error/missing/node/model mismatches.

Reproduce the current result with:

```powershell
go test ./internal/scheduler -run TestRoutingQualityFixtures -count=1 -v
```

The test prints a machine-readable `routing_quality` JSON line and per-category
match counts. Its 40 prompts are hand-authored acceptance fixtures, not a
statistical estimate of real-world planning quality.

## Baseline and rule-by-rule results

The baseline row was collected before changing the default heuristic. Each
following row is cumulative; only the named rule changed between measurements.

| Stage | Exact plans | Exact assignments | Count | Task type | Role | Dependencies | Synthesis |
|---|---:|---:|---:|---:|---:|---:|---:|
| Baseline | 16/40 (40.0%) | 74/105 (70.5%) | 28/40 | 75/105 | 65/105 | 73/105 | 81/105 |
| Sequential cues | 19/40 (47.5%) | 74/105 (70.5%) | 28/40 | 75/105 | 65/105 | 81/105 | 81/105 |
| Semicolon list items | 20/40 (50.0%) | 76/105 (72.4%) | 29/40 | 77/105 | 67/105 | 82/105 | 82/105 |
| Action conjunctions | 29/40 (72.5%) | 101/105 (96.2%) | 39/40 | 102/105 | 91/105 | 102/105 | 102/105 |
| Topic list introductions | 30/40 (75.0%) | 103/105 (98.1%) | 40/40 | 105/105 | 94/105 | 105/105 | 105/105 |
| Role inference | 40/40 (100%) | 103/105 (98.1%) | 40/40 | 105/105 | 105/105 | 105/105 | 105/105 |

### Findings by rule

1. **Sequential cues**: the default heuristic now links work beginning with
   cues such as “next,” “subsequently,” “finally,” and “lastly” to the previous
   step. Exact plan matches rose by 3/40 and dependency-component matches by
   8/105. Assignment accuracy did not move. The explicitly selected
   `heuristic_v1` path retains its legacy behavior.
2. **Semicolon list items**: semicolons now delimit list values when parsing an
   enumeration. This fixed numbered, semicolon-delimited steps: exact plans
   rose by 1/40 and assignments by 2/105.
3. **Action conjunctions**: a supported imperative followed by `and` and a
   second supported imperative becomes parallel work plus synthesis. Ordinary
   phrases such as “summarize the memo and its appendix” remain one task. Exact
   plans rose by 9/40 and assignments by 25/105; conjunction fixtures improved
   from 0/10 to 9/10 at this stage.
4. **Topic list introductions**: `topics:` is now recognized as an enumeration
   heading. Exact plans rose by 1/40 and assignments by 2/105; list fixtures
   reached 10/10.
5. **Role inference**: leading research actions infer `researcher`; targeted
   review actions infer `reviewer`; other roles continue to follow task type.
   Exact plans rose from 30/40 to 40/40, entirely through role matches. The
   role matcher is intentionally narrow so existing planner goldens and the
   legacy `heuristic_v1` behavior remain unchanged.

The current category results are 10/10 exact plans for each of the four
fixture groups. There are two remaining assignment mismatches: both are
`summarize` work items in a repeated-list fixture where accumulated simulated
load causes the scorer to choose `sim-general` / `simulated-general` instead
of the expected `sim-summarize` / `simulated-summarize`. Scheduler scoring was
not changed; this result remains visible as two `assignment.node` and two
`assignment.model` failures.

## Assignment timing

The original baseline run measured a 6.86 µs mean, 5.29 µs p50, and 15.70 µs
p95 per fixture-level `AssignGraph` call. Ten post-change test runs had a median
run mean of 6.31 µs, a median p50 of 5.20 µs, and a median p95 of 10.54 µs.
The baseline timing is a single process run and the plans now contain more
work; this is diagnostic timing, not a controlled performance comparison or a
claim of an assignment-speed improvement.

## HTTP smoke test

[TestTaskHTTPAPIReturnsOrderedSimulatedSynthesis](../internal/orchestrator/routing_smoke_test.go)
submits a two-item task through `POST /api/tasks`, dispatches through a
simulated-engine worker, and checks that the final output is non-empty and
contains the two deterministic subtask results in dependency order.

## Limitations

The fixtures are synthetic and authored alongside the rules; perfect fixture
match is not evidence that arbitrary descriptions decompose correctly.
Conjunction splitting is deliberately limited to recognized imperative
openers. Assignment expectations model a small, fixed cluster and expose a
load-related fallback, but do not estimate behavior across real node
inventories, model quality, or concurrent reservations.

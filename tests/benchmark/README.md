# Synthetic graph pipeline benchmarks

These manual tests measure candidate selection and executable-job hydration over
a generated artifact pipeline. The fixture uses a `benchmark` schema and
`pipeline_graph`, with named JSON fields, generated keys, versioned artifacts,
jobs, input edges, shared group configuration, and optional flags. No external
dataset or application service is required.

## Scanner workloads

| Scanner | Work exercised |
|---|---|
| `evaluation` | Discover items with no current evaluation job; join shared group configuration. |
| `validation` | Traverse evaluation lineage and collect configuration, rules, and prior events. |
| `dispatch` | Select one rule binding, resolve historical outputs, and suppress current pending jobs. |
| `aggregate` | Traverse validation history and build group membership; excluded members still contribute to the revision sum. |

Each candidate is checked for exact artifact IDs, versions, types, flags, input
roles, strong-input counts, and revision sums. Historical versions and pending
jobs exercise selection and suppression, not just row counts.

```sh
bazel test -c opt //tests/benchmark:graph_scanner_benchmark \
  --test_output=all \
  --test_env=GRAPH_BENCH_ITEMS=2000 \
  --test_env=GRAPH_BENCH_ITERATIONS=1 \
  --test_env=GRAPH_BENCH_DROP_DATABASE=1 \
  --test_env=GRAPH_BENCH_EXIT_NORMALLY=1
```

## Producer workloads

The producer benchmark grows one database through three states:

| Stage | Existing data | Pending work |
|---|---|---|
| `a` | Items and shared group configuration | Evaluation jobs |
| `b` | Stage A plus evaluation artifacts | Validation jobs |
| `c` | Stage B plus validation artifacts | Aggregate jobs |

All three query variants must return the same exact job/input pairs, versions,
strong flags, and artifact types. Any invalid input excludes its entire job.

| Variant | Query shape |
|---|---|
| `current` | Graph traversal with inline payload selection |
| `ids_then_hydrate` | Graph IDs and versions followed by relational payload joins |
| `no_graph` | Equivalent joins on the base tables |

```sh
bazel test -c opt //tests/benchmark:graph_producer_benchmark \
  --test_output=all \
  --test_env=GRAPH_BENCH_ITEMS=2000 \
  --test_env=GRAPH_BENCH_ITERATIONS=1
```

## Controls and measurements

| Variable | Default | Purpose |
|---|---|---|
| `GRAPH_BENCH_ITEMS` | Scanner: 310; producer: 5000 | Number of generated items |
| `GRAPH_BENCH_ITERATIONS` | Scanner: 3; producer: 1 | Timed executions per selected workload |
| `GRAPH_BENCH_PAYLOAD_BYTES` | 1200 | Deterministic filler bytes per artifact; jobs and flags use smaller fractions |
| `GRAPH_BENCH_ABORT_PROBABILITY` | Emulator default (20) | Percent chance of trying to abort a lock holder; use 3 for the low-abort check |
| `GRAPH_BENCH_VERSIONS` | 2 | Scanner: historical versions on every third item |
| `GRAPH_BENCH_SCANNERS` | All four | Comma-separated scanner names |
| `GRAPH_BENCH_STAGES` | `a,b,c` | Producer stages to time; prerequisite data is still seeded |
| `GRAPH_BENCH_VARIANTS` | All three | Comma-separated producer variants |
| `GRAPH_BENCH_DROP_DATABASE` | 0 | Scanner: set to 1 to exercise database deletion |
| `GRAPH_BENCH_EXIT_NORMALLY` | Unset | Scanner: set to 1 to exercise orderly shutdown |

The tests log `GRAPH_BENCH` or `GRAPH_PRODUCER` lines with elapsed time, returned
rows, and peak RSS. RSS is the process high-water mark and includes the embedded
emulator, client result buffers, fixture, and verification data. It is not a
per-query allocation measurement. Keep item counts, payload sizes, versions,
selected workloads, and runner resources fixed when comparing implementations.

The producer retains its explicit process-exit teardown behavior; the scanner
can exercise orderly teardown using the flags above. Both targets are tagged
`manual` and must be selected explicitly.

The producer target also runs a separate contended-write test: four writers
increment one counter 50 times each using the client's transaction retries.
It verifies that retries occurred and that all 200 increments were committed
exactly once. `GRAPH_RETRIES` reports callback attempts and committed writes.
This tests retry correctness; it does not measure the full Oak workflow.

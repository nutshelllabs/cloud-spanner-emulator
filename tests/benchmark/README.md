# Synthetic graph pipeline benchmarks

This manual test measures candidate selection over
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

## Controls and measurements

| Variable | Default | Purpose |
|---|---|---|
| `GRAPH_BENCH_ITEMS` | 310 | Number of generated items |
| `GRAPH_BENCH_ITERATIONS` | 3 | Timed executions per selected workload |
| `GRAPH_BENCH_PAYLOAD_BYTES` | 1200 | Deterministic filler bytes per artifact; jobs and flags use smaller fractions |
| `GRAPH_BENCH_VERSIONS` | 2 | historical versions on every third item |
| `GRAPH_BENCH_SCANNERS` | All four | Comma-separated scanner names |
| `GRAPH_BENCH_DROP_DATABASE` | 0 | set to 1 to exercise database deletion |
| `GRAPH_BENCH_EXIT_NORMALLY` | Unset | set to 1 to exercise orderly shutdown |

The test logs `GRAPH_BENCH` lines with elapsed time, returned
rows, and peak RSS. RSS is the process high-water mark and includes the embedded
emulator, client result buffers, fixture, and verification data. It is not a
per-query allocation measurement. Keep item counts, payload sizes, versions,
selected workloads, and runner resources fixed when comparing implementations.

The scanner can exercise orderly teardown using the flags above. The target
is tagged `manual` and must be selected explicitly.

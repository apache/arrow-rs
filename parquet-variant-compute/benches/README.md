<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# List-index validity benchmarks

This is the focused benchmark follow-up to #11052, related to #11101.
It does not cover the full Variant benchmark epic.

```sh
cargo bench -p parquet-variant-compute --bench variant_get_list_validity
cargo run --release -p parquet-variant-compute --example variant_get_allocations > allocations.csv
```

The registered Criterion target uses the default allocator without tracking.
The separate example uses a thread-local wrapper around `System`; never use its
elapsed time as a runtime measurement. Both executables share fixture generation
and preflight validation in `variant_get/mod.rs`.

## Workloads

There are 104 cases: 64/8192 rows, List/ListView, unsliced/offset-three slices,
and Variant/Int64 output. The struct control is included only once per layout
pair. Patterns repeat from row zero; sliced cases build three extra rows and
start at pattern position three. List children are shredded as Int64 (nested
lists use the same layout at both levels). Object field `x` uses the indicated
child schema. All fields are nullable.

| Case | Repeating input pattern | Path |
| --- | --- | --- |
| `struct_control` | `{"x":1}` | `x` |
| `list_inbounds` | `[1,2]` | `[0]` |
| `list_mixed` | Arrow null, `[]`, `[null]`, `[1]` | `[0]` |
| `list_oob` | `[1]` | `[9]` |
| `nested_list` | Arrow null, `[]`, `[[]]`, `[[null]]`, `[[1]]` | `[0][0]` |
| `object_list` | Arrow null, `{"x":[]}`, `{"x":[null]}`, `{"x":[1]}` | `x[0]` |
| `list_fallback` | `[1]`, `["two"]`, `[null]` | `[0]` |

`list_fallback` mixes typed integers and binary string fallback. Int64 output
uses the default safe-cast behavior: the string and explicit Variant null become
Arrow nulls. Variant output preserves the string and explicit null as valid
values. The all-OOB fixture retains populated physical children, ensuring it
exercises list indexing rather than an absent shredded schema.

Fixture construction, JSON parsing, shredding, path/schema preparation, and
validation are outside measurement. Each measured call includes the consumed
`GetOptions` clone. Criterion includes output destruction; the allocation probe
snapshots live bytes while holding the output, then checks they return to zero
after dropping it. Criterion reports time and rows/second (elements/second).

## Baseline correctness

Both unshredded reference and shredded output are checked against explicit
expected values and Arrow validity before measurement. The sole tolerated
baseline discrepancy is #11050: a missing final list index appears as a valid
Variant null. Only the exact affected pattern positions may differ, and all
such positions must agree about whether the fix is present. Any other value or
validity discrepancy fails preflight. Each case logs its affected row count;
allocation CSV includes `known_missing_as_null`.

Run the fixed implementation with strict validation:

```sh
VARIANT_GET_REQUIRE_FIXED=1 cargo bench -p parquet-variant-compute --bench variant_get_list_validity
VARIANT_GET_REQUIRE_FIXED=1 cargo run --release -p parquet-variant-compute --example variant_get_allocations
```

Cases with nonzero baseline counts compare **different output semantics**.
Keep these separate from equivalent-output comparisons when reporting costs.
A benchmark-only checkout of main can run before the correctness fix merges.

## Allocation boundaries

The CSV has five single-call samples per case. The probe checks a known
allocation/reallocation/deallocation sequence before measurements, requires
repeatable per-call statistics, and checks that dropping the result releases
all newly retained bytes.

- `allocations`: successful allocation or reallocation calls.
- `allocated_bytes`: requested sizes, including the entire new realloc size.
- `peak_additional_live`: maximum net live requested bytes during the call;
  realloc changes live bytes by the size difference. Allocator-internal
  transient copies, rounding, metadata, and RSS are not measured.
- `new_live_retained`: net new live requested bytes while the result is held.
- `new_live_after_drop`: must be zero.

Input/options owners remain alive throughout. Their shared backing buffers are
excluded from **new** live memory; these columns are not total result-retained
memory and do not sum Arrow buffer capacities (which can double-count shared
allocations). Unique input-buffer retention and private inline type sizes are
outside this focused suite. Tracking is scoped to the calling thread and is
suitable for this synchronous kernel, not kernels which move work across threads.

## Identical base/head comparison

Use isolated worktrees at the exact implementation base and head. For #11052
these were `f9e02ba76ad11e1380b559735ac912c602b604fb` and
`d881317dc67e292bf0f305f2fb21b59b6a7f1458`. Copy the same benchmark/example files
and bench registration into both worktrees; use the same Cargo.lock, compiler,
features, profile, allocator, and machine. Use **separate target directories**
so a reused binary cannot invalidate the comparison. Do not apply the production
patch to the benchmark PR.

For each revision (set `VARIANT_GET_REQUIRE_FIXED=1` only for the fixed head):

```sh
CARGO_TARGET_DIR=target-comparison cargo bench --locked -p parquet-variant-compute \
  --bench variant_get_list_validity -- --save-baseline review
CARGO_TARGET_DIR=target-comparison cargo run --locked --release \
  -p parquet-variant-compute --example variant_get_allocations > allocations.csv
```

`target-comparison` is relative to each separate worktree. Preserve the raw
Criterion `sample.json` and `estimates.json`, allocation CSV, preflight logs,
commit IDs, source/lockfile hashes, toolchain/OS/architecture, and commands.
Compare matching case IDs and report confidence intervals alongside timings;
short local runs are exploratory, not evidence that small changes are noise-free.
Use a positional Criterion filter such as `list_mixed/list/slice0/variant/8192`
to repeat an individual case with longer warm-up/measurement durations. The
existing empty-path and unshredded object-path controls remain in `variant_kernels`.

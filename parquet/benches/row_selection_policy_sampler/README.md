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

# Row-selection policy sampler

This bench-only binary collects paired `Mask` and `Selectors` observations for
one projected column at a time. It does not change the production
`AutoPerColumn` planner.

Fixtures and Parquet metadata are built before measurement. Each observation
times reader construction through complete batch consumption against preloaded
bytes; fixture generation, correctness checks, checksums, and storage I/O are
outside the timer. The two forced policies are run serially in randomized A/B
order, and their decoded `RecordBatch` values must match before a point is
sampled.

Run a short smoke pass:

```shell
cargo bench -p parquet --features arrow,async \
  --bench arrow_reader_row_selection_policy_sampler -- \
  --stage smoke --output /tmp/row-selection-smoke.jsonl
```

Run a time-bounded Pilot and resume it later:

```shell
cargo bench -p parquet --features arrow,async \
  --bench arrow_reader_row_selection_policy_sampler -- \
  --stage pilot --budget-seconds 600 \
  --output /tmp/row-selection-pilot.jsonl

cargo bench -p parquet --features arrow,async \
  --bench arrow_reader_row_selection_policy_sampler -- \
  --stage pilot --budget-seconds 600 \
  --output /tmp/row-selection-pilot.jsonl --resume
```

After the Pilot identifies a coarse boundary, scan intermediate run lengths
and selectivities before fitting a model:

```shell
cargo bench -p parquet --features arrow,async \
  --bench arrow_reader_row_selection_policy_sampler -- \
  --stage refinement --budget-seconds 600 \
  --output /tmp/row-selection-refinement.jsonl
```

The refinement defaults use a 3% practical decision band. This prevents tiny,
unstable differences near the boundary from being treated as decisive wins.

Validate sparse page loading through the public push decoder:

```shell
cargo bench -p parquet --features arrow,async \
  --bench arrow_reader_row_selection_policy_sampler -- \
  --stage page-validation \
  --output /tmp/row-selection-pages.jsonl
```

This stage asks the empty-buffer decoder which ranges it needs under both
forced policies and fails unless each requests a strict subset of the encoded
column. Timed observations still use preloaded buffers, so they measure decode
latency rather than filesystem latency.

## Output and restart rules

The versioned JSONL stream contains a manifest, start/end control points, raw
paired observations, summaries, and a final run record. Each observation also
carries the sampling settings used for it, so a resumed run remains
self-describing if its time budget or sampling granularity is adjusted. A Pilot
always runs its small mandatory type coverage before the wall-clock budget can
stop the randomized expansion set.

With `--resume`, complete and unsupported experiment IDs are skipped while
incomplete timeout records are retried. Resume rejects a different schema,
manifest, stage, seed, or machine signature. A mandatory fixture error aborts
the run; an optional fixture error is recorded as `unsupported` so the rest of
the manifest can continue. The sampler never silently substitutes one policy
for the other.

## Promoting measurements into the planner

Treat effects inside the refinement stage's 3% decision band as ties. Before a
threshold changes production planning, repeat the same manifest on a warm
machine, require control drift below 10%, and check that the practical winner
is stable. Rules apply only to sampled Arrow/encoding families; unmodeled,
nested, or ambiguous metadata must retain the compatibility fallback.

Use `--help` for sampling, filtering, confidence, and checkpoint options. Raw
JSONL files are machine-specific experiment artifacts and are not intended to
be committed to the repository.

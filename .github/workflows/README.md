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

# Continuous integration and the merge queue

`ci.yml` calls the test workflows (`arrow.yml`, `parquet.yml`, and so on) and
publishes the single required status check, **Required Checks**. This follows
the [DataFusion Comet CI structure](https://github.com/apache/datafusion-comet/pull/5842).
The test workflows use `workflow_call`; event routing and cancellation belong
to `ci.yml` so that called workflows cannot cancel one another.
`parquet.yml` covers `parquet`, the three `parquet-variant*` crates,
`parquet_derive`, `parquet_derive_test`, and `parquet-geospatial`.

| Event                       | Suites                                                                 |
| --------------------------- | ---------------------------------------------------------------------- |
| Pull request                | Dev, Rust and rustdoc checks, plus suites selected by changed paths    |
| Merge queue (`merge_group`) | All suites, including all 12 Miri partitions                           |
| Push to `main`              | Build/publish rustdocs and populate caches; no test suites            |

Miri runs only in the merge queue, after approval and before merging. It does
not run on PR updates or again after the merge. Queue builds test the proposed
merge result against `main` and any earlier entries in the queue.

## Path filtering and required checks

`.github/ci/paths.yaml` keeps the per-suite PR filters. Shared build inputs
such as the workspace manifests, toolchain and CI configuration select every
PR suite. `.github/ci/scripts/compute-changes.py` compares the PR head with its
merge base, including both paths of a renamed file. Queue builds run every
suite regardless of changed paths.

`compute-changes.py` owns suite selection for all three events: it applies path
filters on PRs, selects all suites for queue builds, and selects docs, integration
and Parquet on pushes to `main`. Every suite call in `ci.yml` uses the same `contains(...)` condition
to check the selected list. The Miri workflow does not repeat the event check.

Like [Comet's cache refresh mode](https://github.com/apache/datafusion-comet/pull/5930),
the selector also sets `cache-refresh-only` for pushes. `ci.yml` passes it to
integration and Parquet, which reuse their existing setup and cache steps:

- Docs builds and publishes rustdocs, populating the shared Cargo cache.
- Integration installs dependencies and builds the Python extension with Maturin
  using one PyArrow version, populating its Cargo and compilation caches.
- Parquet installs Python dependencies, populating its pip cache.

Archery, Rust/Python tests, Clippy, Black, and Parquet binary builds are skipped
in this mode. PR and queue runs retain their full selected workflows, including
all three PyArrow versions. Cache keys and paths are shared between modes;
Maturin cache keys include the manifests and toolchain file hash so dependency
changes can create new entries, with older entries available as fallbacks.
Pushes also run suite selection/validation and report the aggregate status.

The required workflow has no path filter: otherwise a docs-only PR could wait
forever for a check that never starts. **Required Checks** runs even after a
dependency fails. Intentionally skipped suites are allowed; a failed or
cancelled suite, or unsuccessful suite selection, blocks merging.

When adding a suite, register it in `ci.yml`, the aggregator's `needs`, and the
path filters (or the always-run set in `compute-changes.py`).
`check-ci-config.py` validates routing policy and checks that every reusable
workflow is covered and that `.asf.yaml` names the actual aggregator job. It also
checks that source changes to packages named literally with `cargo -p` or
`--package` in workflow steps select that workflow. This is not a shell parser:
indirect inputs, scripts and commands using `cd` need explicit routing tests.
The validator also checks that cache mode reaches the reusable workflows and
that only their cache setup steps run in that mode. Update `CACHE_REFRESH_STEPS`
when adding a setup step that must run on push.
To check changes locally:

```sh
python3 -m pip install PyYAML==6.0.3
python3 .github/ci/scripts/check-ci-config.py
```

## Queue configuration

The `Merge Queue` ruleset in `.asf.yaml` enables the queue for the default
branch using the raw Rulesets API format, as in
[DataFusion Comet](https://github.com/apache/datafusion-comet/pull/5843).
ASF applies this configuration after it lands on `main`; pushing a feature
branch does not activate the queue.

One approving review is required. Auto-merge lets a maintainer arrange for a
PR to enter the queue when its prerequisites pass. The queue runs up to two
builds concurrently and requires each entry's checks to pass (`ALLGREEN`). Up
to five already-green entries can merge together, with a separate squash commit
for each PR. This limit does not batch CI builds. The check timeout is five hours,
including runner scheduling time. ASF Infra's `apache/root` team can bypass the
queue to recover a blocked repository.

If an entry fails, inspect its **Required Checks** dependencies, fix the
failure, and enqueue it again. Keep the required check name and configuration
validator in sync when editing CI: a required name that never reports can
block all merges, including the fix itself.

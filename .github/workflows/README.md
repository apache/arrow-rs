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
`parquet.yml` covers both `parquet` and the three `parquet-variant*` crates.

| Event                       | Suites                                                                 |
| --------------------------- | ---------------------------------------------------------------------- |
| Pull request                | Dev, Rust and rustdoc checks, plus suites selected by changed paths    |
| Merge queue (`merge_group`) | All suites, including all 12 Miri partitions                           |
| Push to `main`              | All suites except Miri; refreshes shared caches and publishes rustdocs |

Miri runs only in the merge queue, after approval and before merging. It does
not run on PR updates or again after the merge. Queue builds test the proposed
merge result against `main` and any earlier entries in the queue.

## Path filtering and required checks

`.github/ci/paths.yaml` keeps the per-suite PR filters. Shared build inputs
such as the workspace manifests, toolchain and CI configuration select every
PR suite. `.github/ci/scripts/compute-changes.py` compares the PR head with its
merge base, including both paths of a renamed file. Queue builds run every
suite regardless of changed paths.

The required workflow has no path filter: otherwise a docs-only PR could wait
forever for a check that never starts. **Required Checks** runs even after a
dependency fails. Intentionally skipped suites are allowed; a failed or
cancelled suite, or unsuccessful suite selection, blocks merging.

When adding a suite, register it in `ci.yml`, the aggregator's `needs`, and the
path filters (or the always-run set in `compute-changes.py`).
`check-ci-config.py` validates routing policy and checks that every reusable
workflow is covered and that `.asf.yaml` names the actual aggregator job.
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

<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

CI selects checks for pull requests according to their source and changed files.
See the [contributor guide](../../CONTRIBUTING.md#pull-requests-from-forks) for fork CI setup.
Fork pushes and manual runs execute the regular suites except macOS tests, Windows tests, and Miri.
macOS tests run only upstream to avoid frequent timeouts on fork runners, and Windows tests
to reduce compute in forks. Both remain enabled for same-repository and release-branch PRs,
upstream manual runs, and the merge queue.
The merge queue runs the full test suite, except Miri, against the proposed merge result.
Pushes to upstream `main` populate caches and publish documentation without repeating tests.
A single required status check combines the results and blocks merging on failure.

Miri runs separately every day at 03:17 UTC and on demand through the `miri`
workflow's **Run workflow** button or
`gh workflow run miri.yaml --repo apache/arrow-rs --ref <branch-or-tag>`.
Failed scheduled runs in `apache/arrow-rs` open a `Daily Miri checks failed` issue
unless one is already open. Close the issue after Miri passes again to allow new alerts.

New jobs that are small and short should use `runs-on: ubuntu-slim`, as ASF Infra recommends.
These runners have 1 vCPU, 5 GB of memory, no Docker engine and a 15-minute hard limit, but
they usually start much sooner than `ubuntu-latest` when the shared ASF runner pool is busy.
They come without a Rust toolchain, so jobs that need `cargo` can use
`.github/actions/setup-rustup`.

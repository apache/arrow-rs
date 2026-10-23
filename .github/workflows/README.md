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

CI selects checks for pull requests according to their source and changed files.
See the [contributor guide](../../CONTRIBUTING.md#pull-requests-from-forks) for fork CI setup.
The merge queue runs the full test suite, except Miri, against the proposed merge result.
Pushes to upstream `main` populate caches and publish documentation without repeating tests.
A single required status check combines the results and blocks merging on failure.

Miri runs separately every day at 03:17 UTC and on demand through the `miri`
workflow's **Run workflow** button or
`gh workflow run miri.yaml --repo apache/arrow-rs --ref <branch-or-tag>`.

New jobs that are small and short should use `runs-on: ubuntu-slim`, as ASF Infra recommends.
These runners have 1 vCPU, 5 GB of memory, no Docker engine and a 15-minute hard limit, but
they usually start much sooner than `ubuntu-latest` when the shared ASF runner pool is busy.
They come without a Rust toolchain, so jobs that need `cargo` can use
`.github/actions/setup-rustup`.

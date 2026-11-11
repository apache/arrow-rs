<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Agent Guidelines for Apache Arrow-rs

This file is an entry point. Follow the detailed guidance in
[CONTRIBUTING.md](CONTRIBUTING.md) and the documentation for the crates you change.

## Developer Documentation

- [Repository overview and crate list](README.md#repository-structure)
- Crate READMEs: [Arrow](arrow/README.md), [Parquet](parquet/README.md),
  [Arrow Flight](arrow-flight/README.md), and the README for each affected crate
- [Contributor guide](CONTRIBUTING.md)
- [Pull request template](.github/pull_request_template.md)

## Setup and Validation

Initialize test data before running tests or examples:

```bash
git submodule update --init
```

Run the checks relevant to your change from the repository root:

```bash
# Check Rust formatting and lints for code changes
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Check spelling, including documentation changes
# If typos is not installed, run: cargo install typos-cli
typos --config typos.toml

# Run the full workspace test suite for code changes
cargo test --workspace
```

See the [testing](CONTRIBUTING.md#running-the-tests), [formatting](CONTRIBUTING.md#code-formatting), [Clippy](CONTRIBUTING.md#clippy-lints), and [spelling](CONTRIBUTING.md#spell-checking) guidance for details.

## Additional Checks

For performance work, see [benchmarks](CONTRIBUTING.md#performance-improvements). For memory-safety checks, see [Miri](CONTRIBUTING.md#miri).
For public API changes, see [breaking changes](CONTRIBUTING.md#breaking-changes).

## Issue Ownership

The following agent-specific rules are stricter than the general contribution
guidance in CONTRIBUTING.md:

- Before starting work on an issue, check its current GitHub assignees.
  Do not work on or submit a pull request for an issue assigned to someone
  other than the user you are assisting. Report the assignment to the user
  and suggest another issue.
- If the issue's "Willingness to contribute" answer is "I plan to contribute
  ... independently", treat the reporter as assigned, even when the issue
  has no assignees.
- Also check recent comments and linked pull requests for work already in
  progress. If another contributor is working on an unassigned issue, ask
  the user to coordinate with them before proceeding.
- Recheck the issue's assignees immediately before submitting a pull request.
  If another contributor has been assigned, stop and inform the user.
  If you cannot verify the current assignment, stop and ask the user.
- Do not assume an old assignment or lack of recent activity permits taking
  over an issue. Do not remove or change assignments to bypass these rules.
- If ownership cannot be verified, stop before starting work, not just before
  submitting a PR.
- Check open PRs and comments indicating ongoing work by other contributors,
  rather than treating every linked PR as a blocker.
- If the requesting user's GitHub identity is unclear, ask; if there are
  multiple assignees, any assignee other than that user triggers the
  restriction.

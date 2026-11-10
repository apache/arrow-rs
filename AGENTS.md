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

## Code Guidelines

- A PR merges as one squashed commit titled with the PR title, so it should do only what that title says. Put unrelated renames, reformatting, refactors, and drive-by fixes in a separate PR.
- Default to no comment. Comment only what the code cannot say: invariants, non-obvious constraints, and why a choice that looks wrong is right. Do not restate the code or describe how it changed. Public items get a one-line doc summary of what they do for the caller.
- Return an error for invalid input instead of panicking, as described in [Guidelines for `panic` vs `Result`](README.md#guidelines-for-panic-vs-result).

### Public API

Released API is permanent. Breaking changes land only in major releases, and deprecated items stay for at least two major releases. Read [breaking changes](CONTRIBUTING.md#breaking-changes) and the [deprecation guidelines](README.md#deprecation-guidelines) before adding or changing a `pub` item. Items shared between workspace crates are public API too, because arrow-rs does not hide them. Add an item only when the change needs it, prefer private or `pub(crate)`, and give it a defined result for every input.

`cargo-semver-checks` reports breakage only, so it does not flag additions. In a review, name every new or changed public item.

### Unsafe Code

Prefer safe code. Use `unsafe` only when a benchmark shows a gain over the safe version on the path downstream callers use. A safe function must be sound for every input its signature accepts: check the input, return an error or panic, or make it an `unsafe fn` with a `# Safety` section. A `debug_assert!` does not make code sound, because release builds skip it. Add one for each `SAFETY` precondition, and run [Miri](CONTRIBUTING.md#miri) on `unsafe` changes.

### Performance

A performance PR needs benchmarks that show why the change is faster. Name the benchmark(s) the change targets. Say which part of the change produces the gain, and cover both well-predicted input (constant lengths, all valid) and poorly predicted input (random). Name the inputs that get slower. Prefer the smallest change that keeps the gain, and do not add code tuned to the benchmark's data. See [benchmarks](CONTRIBUTING.md#performance-improvements).

## Issue Ownership

The following agent-specific rules are stricter than the general contribution
guidance in CONTRIBUTING.md:

- Before starting work on an issue, check its current GitHub assignees.
  Do not work on or submit a pull request for an issue assigned to someone
  other than the user you are assisting. Report the assignment to the user
  and suggest another issue.
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

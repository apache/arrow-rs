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

## Introduction

We welcome and encourage contributions of all kinds, such as:

1. Tickets with issue reports of feature requests
2. Documentation improvements
3. Code (PR or PR Review)

In addition to submitting new PRs, we have a healthy tradition of community
members helping review each other's PRs. Doing so is a great way to help the
community as well as get more familiar with Rust and the relevant codebases.

## Finding and Creating Issues to Work On

You can find a curated [good-first-issue] list to help you get started.

Arrow-rs is an open contribution project, and thus there is no particular
project imposed deadline for completing any issue or any restriction on who can
work on an issue, nor how many people can work on an issue at the same time.

Contributors drive the project forward based on their own priorities and
interests and thus you are free to work on any issue that interests you.

If someone is already working on an issue that you want or need but hasn't
been able to finish it yet, you should feel free to work on it as well. In
general it is both polite and will help avoid unnecessary duplication of work if
you leave a note on an issue when you start working on it.

If you want to work on an issue which is not already assigned to someone else
and there are no comment indicating that someone is already working on that
issue then you can assign the issue to yourself by submitting a single word
comment `take`. This will assign the issue to yourself. However, if you are
unable to make progress you should unassign the issue by using the `unassign me`
link at the top of the issue page (and ask for help if are stuck) so that
someone else can get involved in the work.

If you plan to work on a new feature that doesn't have an existing ticket, it is
a good idea to open a ticket to discuss the feature. Advanced discussion often
helps avoid wasted effort by determining early if the feature is a good fit for
Arrow-rs before too much time is invested. It also often helps to discuss your
ideas with the community to get feedback on implementation.

[good-first-issue]: https://github.com/apache/arrow-rs/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22

## Developer's guide to Arrow Rust

### Setting Up Your Build Environment

Install the Rust tool chain:

https://www.rust-lang.org/tools/install

Also, make sure your Rust tool chain is up-to-date, because we always use the latest stable version of Rust to test this project.

```bash
rustup update stable
```

### How to compile

This is a standard cargo project with workspaces. To build it, you need to have `rust` and `cargo`:

```bash
cargo build
```

You can also use rust's official docker image:

```bash
docker run --rm -v $(pwd):/arrow-rs -it rust /bin/bash -c "cd /arrow-rs && rustup component add rustfmt && cargo build"
```

The command above assumes that are in the root directory of the project, not in the same
directory as this README.md.

You can also compile specific workspaces:

```bash
cd arrow && cargo build
```

### Git Submodules

Before running tests and examples, it is necessary to set up the local development environment.

The tests rely on test data that is contained in git submodules.

To pull down this data run the following:

```bash
git submodule update --init
```

This populates data in two git submodules:

- `../parquet-testing/data` (sourced from https://github.com/apache/parquet-testing.git)
- `../testing` (sourced from https://github.com/apache/arrow-testing)

By default, `cargo test` will look for these directories at their
standard location. The following environment variables can be used to override the location:

```bash
# Optionally specify a different location for test data
export PARQUET_TEST_DATA=$(cd ../parquet-testing/data; pwd)
export ARROW_TEST_DATA=$(cd ../testing/data; pwd)
```

From here on, this is a pure Rust project and `cargo` can be used to run tests, benchmarks, docs and examples as usual.

## Running the tests

Run tests using the Rust standard `cargo test` command:

```bash
# run all unit and integration tests
cargo test

# run tests for the arrow crate
cargo test -p arrow
```

For some changes, you may want to run additional tests. You can find up-to-date information on the current CI tests in [.github/workflows](https://github.com/apache/arrow-rs/tree/master/.github/workflows). Here are some examples of additional tests you may want to run:

```bash
# run tests for the parquet crate
cargo test -p parquet

# run arrow tests with all features enabled
cargo test -p arrow --all-features

# run the doc tests
cargo test --doc
```

## Code Formatting

Our CI uses `rustfmt` to check code formatting. Before submitting a
PR be sure to run the following and check for lint issues:

```bash
cargo +stable fmt --all -- --check
```

Note that currently the above will not check all source files in the parquet crate. To check all
parquet files run the following from the top-level `arrow-rs` directory:

```bash
cargo fmt -p parquet -- --check --config skip_children=true `find ./parquet -name "*.rs" \! -name format.rs`
```

## Breaking Changes

Our [release schedule] allows breaking API changes only in major releases.
This means that if your PR has a breaking API change, it should be marked as
`api-change` and it will not be merged until development opens for the next
major release. See [this ticket] for details.

[release schedule]: README.md#release-versioning-and-schedule
[this ticket]: https://github.com/apache/arrow-rs/issues/5907

## Clippy Lints

We use `clippy` for checking lints during development, and CI runs `clippy` checks.

Run the following to check for `clippy` lints:

```bash
# run clippy with default settings
cargo clippy --workspace --all-targets --all-features -- -D warnings

```

If you use Visual Studio Code with the `rust-analyzer` plugin, you can enable `clippy` to run each time you save a file. See https://users.rust-lang.org/t/how-to-use-clippy-in-vs-code-with-rust-analyzer/41881.

One of the concerns with `clippy` is that it often produces a lot of false positives, or that some recommendations may hurt readability. We do not have a policy of which lints are ignored, but if you disagree with a `clippy` lint, you may disable the lint and briefly justify it.

Search for `allow(clippy::` in the codebase to identify lints that are ignored/allowed. We currently prefer ignoring lints on the lowest unit possible.

- If you are introducing a line that returns a lint warning or error, you may disable the lint on that line.
- If you have several lints on a function or module, you may disable the lint on the function or module.
- If a lint is pervasive across multiple modules, you may disable it at the crate level.

## Running Benchmarks

Running benchmarks are a good way to test the performance of a change. As benchmarks usually take a long time to run, we recommend running targeted tests instead of the full suite.

```bash
# run all benchmarks
cargo bench

# run arrow benchmarks
cargo bench -p arrow

# run benchmark for the parse_time function within the arrow-cast crate
cargo bench -p arrow-cast --bench parse_time
```

To set the baseline for your benchmarks, use the --save-baseline flag:

```bash
git checkout master

cargo bench --bench parse_time -- --save-baseline master

git checkout feature

cargo bench --bench parse_time -- --baseline master
```

## Git Pre-Commit Hook

We can use [git pre-commit hook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) to automate various kinds of git pre-commit checking/formatting.

Suppose you are in the root directory of the project.

First check if the file already exists:

```bash
ls -l .git/hooks/pre-commit
```

If the file already exists, to avoid mistakenly **overriding**, you MAY have to check
the link source or file content. Else if not exist, let's safely soft link [pre-commit.sh](pre-commit.sh) as file `.git/hooks/pre-commit`:

```bash
ln -s  ../../pre-commit.sh .git/hooks/pre-commit
```

If sometimes you want to commit without checking, just run `git commit` with `--no-verify`:

```bash
git commit --no-verify -m "... commit message ..."
```

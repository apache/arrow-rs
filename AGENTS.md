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
cargo +stable fmt --all -- --check
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


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

# Native Rust implementation of Apache Arrow and Apache Parquet

Welcome to the [Rust][rust] implementation of [Apache Arrow], the popular in-memory columnar format.

This repository contains the following crates:

| Crate              | Description                                                                  | Latest API Docs                                  | README                            |
| ------------------ | ---------------------------------------------------------------------------- | ------------------------------------------------ | --------------------------------- |
| [`arrow`]          | Core functionality (memory layout, arrays, low level computations)           | [docs.rs](https://docs.rs/arrow/latest)          | [(README)][arrow-readme]          |
| [`arrow-flight`]   | Support for Arrow-Flight IPC protocol                                        | [docs.rs](https://docs.rs/arrow-flight/latest)   | [(README)][flight-readme]         |
| [`parquet`]        | Support for Parquet columnar file format                                     | [docs.rs](https://docs.rs/parquet/latest)        | [(README)][parquet-readme]        |
| [`parquet_derive`] | A crate for deriving RecordWriter/RecordReader for arbitrary, simple structs | [docs.rs](https://docs.rs/parquet-derive/latest) | [(README)][parquet-derive-readme] |

The current development version the API documentation in this repo can be found [here](https://arrow.apache.org/rust).

Note: previously the [`object_store`] crate was also part of this repository,
but it has been moved to the [arrow-rs-object-store repository]

[apache arrow]: https://arrow.apache.org/
[`arrow`]: https://crates.io/crates/arrow
[`parquet`]: https://crates.io/crates/parquet
[`parquet_derive`]: https://crates.io/crates/parquet-derive
[`arrow-flight`]: https://crates.io/crates/arrow-flight
[arrow-rs-object-store repository]: https://github.com/apache/arrow-rs-object-store

## Release Versioning and Schedule

The Arrow Rust project releases approximately monthly and follows [Semantic
Versioning].

Due to available maintainer and testing bandwidth, [`arrow`] crates ([`arrow`],
[`arrow-flight`], etc.) are released on the same schedule with the same versions
as the [`parquet`] and [`parquet-derive`] crates.

This crate releases every month. We release new major versions (with potentially
breaking API changes) at most once a quarter, and release incremental minor
versions in the intervening months. See [ticket #5368] for more details.

To keep our maintenance burden down, we do regularly scheduled releases (major
and minor) from the `main` branch. How we handle PRs with breaking API changes
is described in the [contributing] guide.

[contributing]: CONTRIBUTING.md#breaking-changes

Planned Release Schedule

| Approximate Date | Version    | Notes                                   |
| ---------------- | ---------- | --------------------------------------- |
| October 2025     | [`57.0.0`] | Major, potentially breaking API changes |
| November 2025    | [`57.1.0`] | Minor, NO breaking API changes          |
| December 2025    | [`57.2.0`] | Minor, NO breaking API changes          |
| January 2026     | [`58.0.0`] | Major, potentially breaking API changes |

[`57.0.0`]: https://github.com/apache/arrow-rs/issues/7835
[`57.1.0`]: https://github.com/apache/arrow-rs/milestone/3
[`57.2.0`]: https://github.com/apache/arrow-rs/milestone/5
[`58.0.0`]: https://github.com/apache/arrow-rs/milestone/6
[ticket #5368]: https://github.com/apache/arrow-rs/issues/5368
[semantic versioning]: https://semver.org/

### Rust Version Compatibility Policy

arrow-rs and parquet are built and tested with stable Rust, and will keep a rolling MSRV (minimum supported Rust version) that can only be updated in major releases on a need by basis (e.g. project dependencies bump their MSRV or a particular Rust feature is useful for us etc.). The new MSRV if selected will be at least 6 months old. The minor releases are guaranteed to have the same MSRV.

Note: If a Rust hotfix is released for the current MSRV, the MSRV will be updated to the specific minor version that includes all applicable hotfixes preceding other policies.

### Guidelines for `panic` vs `Result`

In general, use panics for bad states that are unreachable, unrecoverable or harmful.
For those caused by invalid user input, however, we prefer to report that invalidity
gracefully as an error result instead of panicking. In general, invalid input should result
in an `Error` as soon as possible. It _is_ ok for code paths after validation to assume
validation has already occurred and panic if not. See [ticket #6737] for more nuances.

[ticket #6737]: https://github.com/apache/arrow-rs/issues/6737

### Deprecation Guidelines

Minor releases may deprecate, but not remove APIs. Deprecating APIs allows
downstream Rust programs to still compile, but generate compiler warnings. This
gives downstream crates time to migrate prior to API removal.

To deprecate an API:

- Mark the API as deprecated using `#[deprecated]` and specify the exact arrow-rs version in which it was deprecated
- Concisely describe the preferred API to help the user transition

The deprecated version is the next version which will be released (please
consult the list above). To mark the API as deprecated, use the
`#[deprecated(since = "...", note = "...")]` attribute.

For example

```rust
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
```

In general, deprecated APIs will remain in the codebase for at least two major releases after
they were deprecated (typically between 6 - 9 months later). For example, an API
deprecated in `51.3.0` can be removed in `54.0.0` (or later). Deprecated APIs
may be removed earlier or later than these guidelines at the discretion of the
maintainers.

## Related Projects

There are several related crates in different repositories

| Crate               | Description                                                  | Documentation                      |
| ------------------- | ------------------------------------------------------------ | ---------------------------------- |
| [`object_store`]    | Object Storage (aws, azure, gcp, local, in-memory) interface | [(README)](object_store-readme)    |
| [`datafusion`]      | In-memory query engine with SQL support                      | [(README)][datafusion-readme]      |
| [`ballista`]        | Distributed query execution                                  | [(README)][ballista-readme]        |
| [`parquet_opendal`] | Use [`opendal`] for [`parquet`] Arrow IO                     | [(README)][parquet_opendal-readme] |

[`datafusion`]: https://crates.io/crates/datafusion
[`ballista`]: https://crates.io/crates/ballista
[`parquet_opendal`]: https://crates.io/crates/parquet_opendal
[parquet_opendal-readme]: https://github.com/apache/opendal/blob/main/integrations/parquet/README.md
[object_store-readme]: https://github.com/apache/arrow-rs-object-store/blob/main/README.md

Collectively, these crates support a wider array of functionality for analytic computations in Rust.

For example, you can write SQL queries or a `DataFrame` (using the
[`datafusion`] crate) to read a parquet file (using the [`parquet`] crate),
evaluate it in-memory using Arrow's columnar format (using the [`arrow`] crate),
and send to another process (using the [`arrow-flight`] crate).

Generally speaking, the [`arrow`] crate offers functionality for using Arrow
arrays, and [`datafusion`] offers most operations typically found in SQL,
including `join`s and window functions.

You can find more details about each crate in their respective READMEs.

## Arrow Rust Community

The `dev@arrow.apache.org` mailing list serves as the core communication channel for the Arrow community. Instructions for signing up and links to the archives can be found on the [Arrow Community](https://arrow.apache.org/community/) page. All major announcements and communications happen there.

The Rust Arrow community also uses the official [ASF Slack](https://s.apache.org/slack-invite) for informal discussions and coordination. This is
a great place to meet other contributors and get guidance on where to contribute. Join us in the `#arrow-rust` channel and feel free to ask for an invite via:

1. the `dev@arrow.apache.org` mailing list
2. the [GitHub Discussions][discussions]
3. the [Discord channel](https://discord.gg/YAb2TdazKQ)

The Rust implementation uses [GitHub issues][issues] as the system of record for new features and bug fixes and
this plays a critical role in the release process.

For design discussions we generally use GitHub issues.

There is more information in the [contributing] guide.

[rust]: https://www.rust-lang.org/
[`object_store`]: https://crates.io/crates/object-store
[arrow-readme]: arrow/README.md
[contributing]: CONTRIBUTING.md
[parquet-readme]: parquet/README.md
[flight-readme]: arrow-flight/README.md
[datafusion-readme]: https://github.com/apache/datafusion/blob/main/README.md
[ballista-readme]: https://github.com/apache/datafusion-ballista/blob/main/README.md
[parquet-derive-readme]: parquet_derive/README.md
[issues]: https://github.com/apache/arrow-rs/issues
[discussions]: https://github.com/apache/arrow-rs/discussions

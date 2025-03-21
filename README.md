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

This repo contains the following main components:

| Crate              | Description                                                                  | Latest API Docs                                  | README                            |
| ------------------ | ---------------------------------------------------------------------------- | ------------------------------------------------ | --------------------------------- |
| [`arrow`]          | Core functionality (memory layout, arrays, low level computations)           | [docs.rs](https://docs.rs/arrow/latest)          | [(README)][arrow-readme]          |
| [`arrow-flight`]   | Support for Arrow-Flight IPC protocol                                        | [docs.rs](https://docs.rs/arrow-flight/latest)   | [(README)][flight-readme]         |
| [`object-store`]   | Support for object store interactions (aws, azure, gcp, local, in-memory)    | [docs.rs](https://docs.rs/object_store/latest)   | [(README)][objectstore-readme]    |
| [`parquet`]        | Support for Parquet columnar file format                                     | [docs.rs](https://docs.rs/parquet/latest)        | [(README)][parquet-readme]        |
| [`parquet_derive`] | A crate for deriving RecordWriter/RecordReader for arbitrary, simple structs | [docs.rs](https://docs.rs/parquet-derive/latest) | [(README)][parquet-derive-readme] |

The current development version the API documentation in this repo can be found [here](https://arrow.apache.org/rust).

[apache arrow]: https://arrow.apache.org/
[`arrow`]: https://crates.io/crates/arrow
[`parquet`]: https://crates.io/crates/parquet
[`parquet_derive`]: https://crates.io/crates/parquet-derive
[`arrow-flight`]: https://crates.io/crates/arrow-flight
[`object-store`]: https://crates.io/crates/object-store

## Release Versioning and Schedule

### `arrow` and `parquet` crates

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

| Approximate Date | Version  | Notes                                   |
| ---------------- | -------- | --------------------------------------- |
| Mar 2025         | `54.2.0` | Minor, NO breaking API changes          |
| Apr 2025         | `55.0.0` | Major, potentially breaking API changes |
| May 2025         | `55.1.0` | Minor, NO breaking API changes          |

[ticket #5368]: https://github.com/apache/arrow-rs/issues/5368
[semantic versioning]: https://semver.org/

### `object_store` crate

The [`object_store`] crate is released independently of the `arrow` and
`parquet` crates and follows [Semantic Versioning]. We aim to release new
versions approximately every 2 months.

[`object_store`]: https://crates.io/crates/object_store

Planned Release Schedule

| Approximate Date | Version  | Notes                                   |
| ---------------- | -------- | --------------------------------------- |
| Feb 2025         | `0.12.0` | Major, potentially breaking API changes |
| Apr 2025         | `0.12.1` | Minor, NO breaking API changes          |

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

Foe example

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

| Crate                    | Description                                 | Documentation                           |
| ------------------------ | ------------------------------------------- | --------------------------------------- |
| [`datafusion`]           | In-memory query engine with SQL support     | [(README)][datafusion-readme]           |
| [`ballista`]             | Distributed query execution                 | [(README)][ballista-readme]             |
| [`object_store_opendal`] | Use [`opendal`] as [`object_store`] backend | [(README)][object_store_opendal-readme] |
| [`parquet_opendal`]      | Use [`opendal`] for [`parquet`] Arrow IO    | [(README)][parquet_opendal-readme]      |

[`datafusion`]: https://crates.io/crates/datafusion
[`ballista`]: https://crates.io/crates/ballista
[`object_store_opendal`]: https://crates.io/crates/object_store_opendal
[`opendal`]: https://crates.io/crates/opendal
[object_store_opendal-readme]: https://github.com/apache/opendal/blob/main/integrations/object_store/README.md
[`parquet_opendal`]: https://crates.io/crates/parquet_opendal
[parquet_opendal-readme]: https://github.com/apache/opendal/blob/main/integrations/parquet/README.md

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

For design discussions we generally collaborate on Google documents and file a GitHub issue linking to the document.

There is more information in the [contributing] guide.

[rust]: https://www.rust-lang.org/
[arrow-readme]: arrow/README.md
[contributing]: CONTRIBUTING.md
[parquet-readme]: parquet/README.md
[flight-readme]: arrow-flight/README.md
[datafusion-readme]: https://github.com/apache/datafusion/blob/main/README.md
[ballista-readme]: https://github.com/apache/datafusion-ballista/blob/main/README.md
[objectstore-readme]: object_store/README.md
[parquet-derive-readme]: parquet_derive/README.md
[issues]: https://github.com/apache/arrow-rs/issues
[discussions]: https://github.com/apache/arrow-rs/discussions

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

[![Coverage Status](https://codecov.io/gh/apache/arrow-rs/rust/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/arrow-rs?branch=master)

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

Starting June 2024, we plan to release new major versions with potentially
breaking API changes at most once a quarter, and release incremental minor versions in
the intervening months. See [this ticket] for more details.

To keep our maintenance burden down, we do regularly scheduled releases (major
and minor) from the `master` branch. How we handle PRs with breaking API changes
is described in the [contributing] guide.

[contributing]: CONTRIBUTING.md#breaking-changes

For example:

| Approximate Date | Version  | Notes                                   |
| ---------------- | -------- | --------------------------------------- |
| Jun 2024         | `52.0.0` | Major, potentially breaking API changes |
| Jul 2024         | `52.1.0` | Minor, NO breaking API changes          |
| Aug 2024         | `52.2.0` | Minor, NO breaking API changes          |
| Sep 2024         | `53.0.0` | Major, potentially breaking API changes |

[this ticket]: https://github.com/apache/arrow-rs/issues/5368
[semantic versioning]: https://semver.org/

### `object_store` crate

The [`object_store`] crate is released independently of the `arrow` and
`parquet` crates and follows [Semantic Versioning]. We aim to release new
versions approximately every 2 months.

[`object_store`]: https://crates.io/crates/object_store

## Related Projects

There are several related crates in different repositories

| Crate                    | Description                                 | Documentation                           |
| ------------------------ | ------------------------------------------- | --------------------------------------- |
| [`datafusion`]           | In-memory query engine with SQL support     | [(README)][datafusion-readme]           |
| [`ballista`]             | Distributed query execution                 | [(README)][ballista-readme]             |
| [`object_store_opendal`] | Use [`opendal`] as [`object_store`] backend | [(README)][object_store_opendal-readme] |

[`datafusion`]: https://crates.io/crates/datafusion
[`ballista`]: https://crates.io/crates/ballista
[`object_store_opendal`]: https://crates.io/crates/object_store_opendal
[`opendal`]: https://crates.io/crates/opendal
[object_store_opendal-readme]: https://github.com/apache/opendal/blob/main/integrations/object_store/README.md

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

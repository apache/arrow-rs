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

# Apache Parquet Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/parquet.svg)](https://crates.io/crates/parquet)
[![docs.rs](https://img.shields.io/docsrs/parquet.svg)](https://docs.rs/parquet/latest/parquet/)

This crate contains the official Native Rust implementation of [Apache Parquet](https://parquet.apache.org/), which is part of the [Apache Arrow](https://arrow.apache.org/) project.

See the [API documentation](https://docs.rs/parquet/latest) for examples and the full API.

The API documentation for most recent, unreleased code is available [here](https://arrow.apache.org/rust/parquet/index.html).

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

## Versioning / Releases

The `parquet` crate follows the [SemVer standard] defined by Cargo and works well
within the Rust crate ecosystem. See the [repository README] for more details on
the release schedule, version and deprecation policy.

[semver standard]: https://doc.rust-lang.org/cargo/reference/semver.html
[repository readme]: https://github.com/apache/arrow-rs

Note that for historical reasons, this crate uses versions with major numbers
greater than `0.x` (e.g. `19.0.0`), unlike many other crates in the Rust
ecosystem which spend extended time releasing versions `0.x` to signal planned
ongoing API changes. Minor arrow releases contain only compatible changes, while
major releases may contain breaking API changes.

## Feature Flags

The `parquet` crate provides the following features which may be enabled in your `Cargo.toml`:

- `arrow` (default) - support for reading / writing [`arrow`] arrays to / from Parquet
- `async` - support `async` APIs for reading Parquet
- `json` - support for reading / writing `json` data to / from Parquet
- `brotli` (default) - support for Parquet using `brotli` compression
- `flate2` (default) - support for Parquet using `gzip` compression
- `lz4` (default) - support for Parquet using `lz4` compression
- `zstd` (default) - support for Parquet using `zstd` compression
- `snap` (default) - support for Parquet using `snappy` compression
- `cli` - parquet [CLI tools](https://github.com/apache/arrow-rs/tree/main/parquet/src/bin)
- `crc` - enables functionality to automatically verify checksums of each page (if present) when decoding
- `experimental` - Experimental APIs which may change, even between minor releases
- `simdutf8` (default) - Use the [`simdutf8`] crate for SIMD-accelerated UTF-8 validation

[`arrow`]: https://crates.io/crates/arrow
[`simdutf8`]: https://crates.io/crates/simdutf8

## Parquet Feature Status

- [x] All encodings supported
- [x] All compression codecs supported
- [x] Read support
  - [x] Primitive column value readers
  - [x] Row record reader
  - [x] Arrow record reader
  - [x] Async support (to Arrow)
- [x] Statistics support
- [x] Write support
  - [x] Primitive column value writers
  - [ ] Row record writer
  - [x] Arrow record writer
  - [x] Async support
- [x] Predicate pushdown
- [x] Parquet format 4.0.0 support

## License

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>.

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

# Apache Arrow Official Native Rust Implementation

[![Crates.io](https://img.shields.io/crates/v/arrow.svg)](https://crates.io/crates/arrow)

This crate contains the official Native Rust implementation of [Apache Arrow][arrow] in memory format, governed by the Apache Software Foundation.

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

## Versioning / Releases

The arrow crate follows the [SemVer standard](https://doc.rust-lang.org/cargo/reference/semver.html) defined by Cargo and works well within the Rust crate ecosystem.

However, for historical reasons, this crate uses versions with major numbers greater than `0.x` (e.g. `8.0.0`), unlike many other crates in the Rust ecosystem which spend extended time releasing versions `0.x` to signal planned ongoing API changes. Minor arrow releases contain only compatible changes, while major releases may contain breaking API changes.

## Features

The arrow crate provides the following features which may be enabled:

- `csv` (default) - support for reading and writing Arrow arrays to/from csv files
- `ipc` (default) - support for the [arrow-flight](https://crates.io/crates/arrow-flight) IPC and wire format
- `prettyprint` - support for formatting record batches as textual columns
- `js` - support for building arrow for WebAssembly / JavaScript
- `simd` - (_Requires Nightly Rust_) alternate optimized
  implementations of some [compute](https://github.com/apache/arrow-rs/tree/master/arrow/src/compute/kernels)
  kernels using explicit SIMD instructions available through [packed_simd_2](https://docs.rs/packed_simd_2/latest/packed_simd_2/).
- `chrono-tz` - support of parsing timezone using [chrono-tz](https://docs.rs/chrono-tz/0.6.0/chrono_tz/)

## Safety

TLDR: You should avoid using the `alloc` and `buffer` and `bitmap` modules if at all possible. These modules contain `unsafe` code, are easy to misuse, and are not needed for most users.

As with all open source code, you should carefully evaluate the suitability of `arrow` for your project, taking into consideration your needs and risk tolerance prior to doing so.

_Background_: There are various parts of the `arrow` crate which use `unsafe` and `transmute` code internally. We are actively working as a community to minimize undefined behavior and remove `unsafe` usage to align more with Rust's core principles of safety.

As `arrow` exists today, it is fairly easy to misuse the code in modules named above, leading to undefined behavior.

## Building for WASM

Arrow can compile to WebAssembly using the `wasm32-unknown-unknown` and `wasm32-wasi` targets.

In order to compile Arrow for `wasm32-unknown-unknown` you will need to disable default features, then include the desired features, but exclude test dependencies (the `test_utils` feature). For example, use this snippet in your `Cargo.toml`:

```toml
[dependencies]
arrow = { version = "5.0", default-features = false, features = ["csv", "ipc", "simd"] }
```

## Examples

The examples folder shows how to construct some different types of Arrow
arrays, including dynamic arrays:

Examples can be run using the `cargo run --example` command. For example:

```bash
cargo run --example builders
cargo run --example dynamic_types
cargo run --example read_csv
```

[arrow]: https://arrow.apache.org/

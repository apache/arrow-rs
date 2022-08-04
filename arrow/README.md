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

[![crates.io](https://img.shields.io/crates/v/arrow.svg)](https://crates.io/crates/arrow)
[![docs.rs](https://img.shields.io/docsrs/arrow.svg)](https://docs.rs/arrow/latest/arrow/)

This crate contains the official Native Rust implementation of [Apache Arrow][arrow] in memory format, governed by the Apache Software Foundation. Additional details can be found on [crates.io](https://crates.io/crates/arrow), [docs.rs](https://docs.rs/arrow/latest/arrow/) and [examples](https://github.com/apache/arrow-rs/tree/master/arrow/examples).

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions.

## Versioning / Releases

The arrow crate follows the [SemVer standard](https://doc.rust-lang.org/cargo/reference/semver.html) defined by Cargo and works well within the Rust crate ecosystem.

However, for historical reasons, this crate uses versions with major numbers greater than `0.x` (e.g. `20.0.0`), unlike many other crates in the Rust ecosystem which spend extended time releasing versions `0.x` to signal planned ongoing API changes. Minor arrow releases contain only compatible changes, while major releases may contain breaking API changes.

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

Arrow seeks to uphold the Rust Soundness Pledge as articulated eloquently [here](https://raphlinus.github.io/rust/2020/01/18/soundness-pledge.html). Specifically:

> The intent of this crate is to be free of soundness bugs. The developers will do their best to avoid them, and welcome help in analyzing and fixing them

Where soundness in turn is defined as:

> Code is unable to trigger undefined behaviour using safe APIs

One way to ensure this would be to not use `unsafe`, however, as described in the opening chapter of the [Rustonomicon](https://doc.rust-lang.org/nomicon/meet-safe-and-unsafe.html) this is not a requirement, and flexibility in this regard is actually one of Rust's great strengths.

In particular there are a number of scenarios where `unsafe` is largely unavoidable:

* Invariants that cannot be statically verified by the compiler and unlock non-trivial performance wins, e.g. values in a StringArray are UTF-8, [TrustedLen](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html) iterators, etc...
* FFI 
* SIMD

Additionally, this crate exposes a number of `unsafe` APIs, allowing downstream crates to explicitly opt-out of potentially expensive invariant checking where appropriate. 

We have a number of strategies to help reduce this risk:

* Provide strongly-typed `Array` and `ArrayBuilder` APIs to safely and efficiently interact with arrays
* Extensive validation logic to safely construct `ArrayData` from untrusted sources
* All commits are verified using [MIRI](https://github.com/rust-lang/miri) to detect undefined behaviour
* We provide a `force_validate` feature that enables additional validation checks for use in test/debug builds
* There is ongoing work to reduce and better document the use of unsafe, and we welcome contributions in this space

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


## Performance

Most of the compute kernels benefit a lot from being optimized for a specific CPU target.
This is especially so on x86-64 since without specifying a target the compiler can only assume support for SSE2 vector instructions.
One of the following values as `-Ctarget-cpu=value` in `RUSTFLAGS` can therefore improve performance significantly:

 - `native`: Target the exact features of the cpu that the build is running on.
   This should give the best performance when building and running locally, but should be used carefully for example when building in a CI pipeline or when shipping pre-compiled software. 
 - `x86-64-v3`: Includes AVX2 support and is close to the intel `haswell` architecture released in 2013 and should be supported by any recent Intel or Amd cpu.
 - `x86-64-v4`: Includes AVX512 support available on intel `skylake` server and `icelake`/`tigerlake`/`rocketlake` laptop and desktop processors.

These flags should be used in addition to the `simd` feature, since they will also affect the code generated by the simd library. 
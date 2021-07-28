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

This crate contains the official Native Rust implementation of [Apache Arrow](https://arrow.apache.org/) in memory format. Please see the API documents for additional details.

## Versioning / Releases

Unlike many other crates in the Rust ecosystem which spend extended time in "pre 1.0.0" state, releasing versions 0.x, the arrow-rs crate follows the versioning scheme of the overall [Apache Arrow](https://arrow.apache.org/) project in an effort to signal which language implementations have been integration tested with each other.

## Features

The arrow crate provides the following features which may be enabled:

- `csv` (default) - support for reading and writing Arrow arrays to/from csv files
- `ipc` (default) - support for the [arrow-flight]((https://crates.io/crates/arrow-flight) IPC and wire format
- `prettyprint` - support for formatting record batches as textual columns
- `js` - support for building arrow for WebAssembly / JavaScript
- `simd` - (_Requires Nightly Rust_) alternate optimized
  implementations of some [compute](https://github.com/apache/arrow/tree/master/rust/arrow/src/compute)
  kernels using explicit SIMD processor intrinsics.

## Safety

TLDR: You should avoid using the `alloc` and `buffer` and `bitmap` modules if at all possible. These modules contain `unsafe` code and are easy to misuse.

As with all open source code, you should carefully evaluate the suitability of `arrow` for your project, taking into consideration your needs and risk tolerance prior to use.

*Background*: There are various parts of the `arrow` crate which use `unsafe` and `transmute` code internally. We are actively working as a community to minimize undefined behavior and remove `unsafe` usage to align more with Rust's core principles of safety (e.g. the arrow2 project).

As `arrow` exists today, it is fairly easy to misuse the APIs, leading to undefined behavior, and it is especially easy to misuse code in modules named above. For an example, as described in [the arrow2 crate](https://github.com/jorgecarleitao/arrow2#why), the following code compiles, does not panic, but results in undefined behavior:

```rust
let buffer = Buffer::from_slic_ref(&[0i32, 2i32])
let data = ArrayData::new(DataType::Int64, 10, 0, None, 0, vec![buffer], vec![]);
let array = Float64Array::from(Arc::new(data));

println!("{:?}", array.value(1));
```

NOTE: We plan to deprecate and make these modules private as part of a follow on release, as part of our journey of redesigning this crate.

## Building for WASM

In order to compile Arrow for Web Assembly (the `wasm32-unknown-unknown` WASM target), you will likely need to turn off this crate's default features and use the `js` feature.

```toml
[dependencies]
arrow = { version = "5.0", default-features = false, features = ["js"] }
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

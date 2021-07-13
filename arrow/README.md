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

## Features

The arrow crate provides the following optional features:

- `csv` (default) - support for reading and writing Arrow arrays to/from csv files
- `ipc` (default) - support for the [arrow-flight]((https://crates.io/crates/arrow-flight) IPC and wire format
- `prettyprint` - support for formatting record batches as textual columns
- `js` - support for building arrow for WebAssembly / JavaScript
- `simd` - (_Requires Nightly Rust_) alternate optimized
  implementations of some [compute](https://github.com/apache/arrow/tree/master/rust/arrow/src/compute)
  kernels using explicit SIMD processor intrinsics.

## Building for WASM

In order to compile Arrow for Web Assembly (the `wasm32-unknown-unknown` WASM target), you will likely need to turn off this crate's default features.

```toml
[dependencies]
arrow = {version = "5.0" default-features = false }
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

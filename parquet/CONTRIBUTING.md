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

## Build

Run `cargo build` or `cargo build --release` to build in release mode.
Some features take advantage of SSE4.2 instructions, which can be
enabled by adding `RUSTFLAGS="-C target-feature=+sse4.2"` before the
`cargo build` command.

## Test

Run `cargo test` for unit tests. To also run tests related to the binaries, use `cargo test --features cli`.

## Binaries

The following binaries are provided (use `cargo install --features cli` to install them):

- **parquet-schema** for printing Parquet file schema and metadata.
  `Usage: parquet-schema <file-path>`, where `file-path` is the path to a Parquet file. Use `-v/--verbose` flag
  to print full metadata or schema only (when not specified only schema will be printed).

- **parquet-read** for reading records from a Parquet file.
  `Usage: parquet-read <file-path> [num-records]`, where `file-path` is the path to a Parquet file,
  and `num-records` is the number of records to read from a file (when not specified all records will
  be printed). Use `-j/--json` to print records in JSON lines format.

- **parquet-rowcount** for reporting the number of records in one or more Parquet files.
  `Usage: parquet-rowcount <file-paths>...`, where `<file-paths>...` is a space separated list of one or more
  files to read.

If you see `Library not loaded` error, please make sure `LD_LIBRARY_PATH` is set properly:

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(rustc --print sysroot)/lib
```

## Benchmarks

Run `cargo bench` for benchmarks.

## Docs

To build documentation, run `cargo doc --no-deps --all-features`.
To compile and view in the browser, run `cargo doc --no-deps --all-features --open`.

Before submitting a pull request, run `cargo fmt --all` to format the change.

## Update Parquet Format

To generate the parquet format (thrift definitions) code run [`./regen.sh`](./regen.sh).

You may need to manually patch up doc comments that contain unescaped `[]`

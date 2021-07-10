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

[![Crates.io](https://img.shields.io/crates/v/parquet.svg)](https://crates.io/crates/parquet)

This crate contains the official Native Rust implementation of [Apache Parquet](https://parquet.apache.org/), which is part of the [Apache Arrow](https://arrow.apache.org/) project.

## Example

Example usage of reading data:

```rust
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};

let file = File::open(&Path::new("/path/to/file")).unwrap();
let reader = SerializedFileReader::new(file).unwrap();
let mut iter = reader.get_row_iter(None).unwrap();
while let Some(record) = iter.next() {
    println!("{}", record);
}
```

See [crate documentation](https://docs.rs/crate/parquet) on available API.

## Upgrading from versions prior to 4.0

If you are upgrading from version 3.0 or previous of this crate, you
likely need to change your code to use [`ConvertedType`] rather than
[`LogicalType`] to preserve existing behaviour in your code.

Version 2.4.0 of the Parquet format introduced a `LogicalType` to replace the existing `ConvertedType`.
This crate used `parquet::basic::LogicalType` to map to the `ConvertedType`, but this has been renamed to `parquet::basic::ConvertedType` from version 4.0 of this crate.

The `ConvertedType` is deprecated in the format, but is still written
to preserve backward compatibility.
It is preferred that `LogicalType` is used, as it supports nanosecond
precision timestamps without using the deprecated `Int96` Parquet type.

## Supported Parquet Version

- Parquet-format 2.6.0

To update Parquet format to a newer version, check if [parquet-format](https://github.com/sunchao/parquet-format-rs)
version is available. Then simply update version of `parquet-format` crate in Cargo.toml.

## Features

- [x] All encodings supported
- [x] All compression codecs supported
- [x] Read support
  - [x] Primitive column value readers
  - [x] Row record reader
  - [x] Arrow record reader
- [x] Statistics support
- [x] Write support
  - [x] Primitive column value writers
  - [ ] Row record writer
  - [x] Arrow record writer
- [ ] Predicate pushdown
- [x] Parquet format 2.6.0 support

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.

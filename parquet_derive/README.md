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

# Parquet Derive

A crate for deriving `RecordWriter` and `RecordReader` for arbitrary, _simple_ structs. This does not
generate readers or writers for arbitrarily nested structures. It only works for primitives and a few
generic structures and various levels of reference. Please see features checklist for what is currently
supported.

Derive also has some support for the chrono time library. You must must enable the `chrono` feature to get this support.

## Usage

See example in [ParquetRecordWriter](<https://docs.rs/parquet_derive/latest/parquet_derive/derive.ParquetRecordWriter.html>) for reading/writing to a parquet file.

Add this to your Cargo.toml:

```toml
[dependencies]
parquet = "39.0.0"
parquet_derive = "39.0.0"
```

and this to your crate root:

```rust
extern crate parquet;
#[macro_use] extern crate parquet_derive;
```

Example usage of deriving a `RecordWriter` for your struct:

```rust
use parquet;
use parquet::record::RecordWriter;

#[derive(ParquetRecordWriter)]
struct ACompleteRecord<'a> {
    pub a_bool: bool,
    pub a_str: &'a str,
    pub a_string: String,
    pub a_borrowed_string: &'a String,
    pub maybe_a_str: Option<&'a str>,
    pub magic_number: i32,
    pub low_quality_pi: f32,
    pub high_quality_pi: f64,
    pub maybe_pi: Option<f32>,
    pub maybe_best_pi: Option<f64>,
}

// Initialize your parquet file
let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
let mut row_group = writer.next_row_group().unwrap();

// Build up your records
let chunks = vec![ACompleteRecord{...}];

// The derived `RecordWriter` takes over here
(&chunks[..]).write_to_row_group(&mut row_group);

writer.close_row_group(row_group).unwrap();
writer.close().unwrap();
```

Example usage of deriving a `RecordReader` for your struct:

```rust
use parquet::file::{serialized_reader::SerializedFileReader, reader::FileReader};
use parquet_derive::ParquetRecordReader;

#[derive(ParquetRecordReader)]
struct ACompleteRecord {
    pub a_bool: bool,
    pub a_string: String,
    pub i16: i16,
    pub i32: i32,
    pub u64: u64,
    pub isize: isize,
    pub float: f32,
    pub double: f64,
    pub now: chrono::NaiveDateTime,
    pub byte_vec: Vec<u8>,
}

// Initialize your parquet file
let reader = SerializedFileReader::new(file).unwrap();
let mut row_group = reader.get_row_group(0).unwrap();

// create your records vector to read into
let mut chunks: Vec<ACompleteRecord> = Vec::new();

// The derived `RecordReader` takes over here
chunks.read_from_row_group(&mut *row_group, 1).unwrap();
```

## Features

- [x] Support writing `String`, `&str`, `bool`, `i32`, `f32`, `f64`, `Vec<u8>`
- [ ] Support writing dictionaries
- [x] Support writing logical types like timestamp
- [x] Derive definition_levels for `Option` for writing
- [ ] Derive definition levels for nested structures for writing
- [ ] Derive writing tuple struct
- [ ] Derive writing `tuple` container types

- [x] Support reading `String`, `&str`, `bool`, `i32`, `f32`, `f64`, `Vec<u8>`
- [ ] Support reading/writing dictionaries
- [x] Support reading/writing logical types like timestamp
- [ ] Handle definition_levels for `Option` for reading
- [ ] Handle definition levels for nested structures for reading
- [ ] Derive reading/writing tuple struct
- [ ] Derive reading/writing `tuple` container types

## Requirements

- Same as `parquet-rs`

## Test

Testing a `*_derive` crate requires an intermediate crate. Go to `parquet_derive_test` and run `cargo test` for
unit tests.

To compile and test doctests, run `cargo test --doc -- --show-output`

## Docs

To build documentation, run `cargo doc --no-deps`.
To compile and view in the browser, run `cargo doc --no-deps --open`.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.
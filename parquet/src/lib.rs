// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*!
This crate contains the official Native Rust implementation of
[Apache Parquet](https://parquet.apache.org/), part of
the [Apache Arrow](https://arrow.apache.org/) project.
The crate provides a number of APIs to read and write Parquet files,
covering a range of use cases.

Please see the [parquet crates.io](https://crates.io/crates/parquet)
page for feature flags and tips to improve performance.

# Getting Started

## Format Overview

Parquet is a columnar format, which means that unlike row formats like
the [CSV format](https://en.wikipedia.org/wiki/Comma-separated_values) for instance,
values are iterated along columns instead of rows. Parquet is similar in spirit to
[Arrow](https://arrow.apache.org/), with Parquet focusing on storage
efficiency while Arrow prioritizes compute efficiency.

Parquet files are partitioned for scalability. Each file contains metadata,
along with zero or more "row groups", each row group containing one or
more columns. The APIs in this crate reflect this structure.

Parquet distinguishes between "logical" and "physical" data types.
For instance, strings (logical type) are stored as byte arrays (physical type).
Likewise, temporal types like dates, times, timestamps, etc. (logical type)
are stored as integers (physical type). This crate exposes both kinds of types.

For more details about the Parquet format, see the
[Parquet spec](https://github.com/apache/parquet-format/blob/master/README.md#file-format).

## APIs

This crate exposes both low-level and high-level APIs, organized as follows:

1. The [`arrow`] module reads and writes Parquet data to/from Arrow
`RecordBatch`es. This is the recommended high-level API. It allows leveraging
the wide range of data transforms provided by the
[arrow](https://docs.rs/arrow/latest/arrow/index.html) crate and by the ecosystem
of libraries and services using Arrow as a interop format.

2. The [`mod@file`] module allows reading and writing Parquet files without taking a
dependency on Arrow. This is the recommended low-level API. Parquet files are
read and written one row group at a time by calling respectively
[`SerializedFileReader::get_row_group`](file::serialized_reader::SerializedFileReader)
 and [`SerializedFileWriter::next_row_group`](file::writer::SerializedFileWriter).
Within each row group, columns are read and written one at a time using
respectively [`ColumnReader`](column::reader::ColumnReader) and
[`ColumnWriter`](column::writer::ColumnWriter). The [`mod@file`] module also allows
reading files in a row-wise manner via
[`SerializedFileReader::get_row_iter`](file::serialized_reader::SerializedFileReader).
This is a convenience API which favors simplicity over performance and completeness.
It is not recommended for production use.

3. Within the [`arrow`] module, async reading and writing is provided by
[`arrow::async_reader`] and [`arrow::async_writer`]. These APIs are more advanced and
require the `async` feature. Within this module,
[`ParquetObjectReader`](arrow::async_reader::ParquetObjectReader)
enables connecting directly to the Cloud Provider storage services of AWS, Azure, GCP
and the likes via the [object_store](https://docs.rs/object_store/latest/object_store/)
crate, enabling network-bandwidth optimizations via predicate and projection push-downs.

*/

/// Defines a an item with an experimental public API
///
/// The module will not be documented, and will only be public if the
/// experimental feature flag is enabled
///
/// Experimental components have no stability guarantees
#[cfg(feature = "experimental")]
macro_rules! experimental {
    ($(#[$meta:meta])* $vis:vis mod $module:ident) => {
        #[doc(hidden)]
        $(#[$meta])*
        pub mod $module;
    }
}

#[cfg(not(feature = "experimental"))]
macro_rules! experimental {
    ($(#[$meta:meta])* $vis:vis mod $module:ident) => {
        $(#[$meta])*
        $vis mod $module;
    }
}

#[macro_use]
pub mod errors;
pub mod basic;

/// Automatically generated code for reading parquet thrift definition.
// see parquet/CONTRIBUTING.md for instructions on regenerating
#[allow(clippy::derivable_impls, clippy::match_single_binding)]
pub mod format;

#[macro_use]
pub mod data_type;

// Exported for external use, such as benchmarks
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use self::encodings::{decoding, encoding};

experimental!(#[macro_use] mod util);
#[cfg(feature = "arrow")]
pub mod arrow;
pub mod column;
experimental!(mod compression);
experimental!(mod encodings);
pub mod bloom_filter;
pub mod file;
pub mod record;
pub mod schema;

pub mod thrift;

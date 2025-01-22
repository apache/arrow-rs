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

//!
//! This crate contains the official Native Rust implementation of
//! [Apache Parquet](https://parquet.apache.org/), part of
//! the [Apache Arrow](https://arrow.apache.org/) project.
//! The crate provides a number of APIs to read and write Parquet files,
//! covering a range of use cases.
//!
//! Please see the [parquet crates.io](https://crates.io/crates/parquet)
//! page for feature flags and tips to improve performance.
//!
//! # Format Overview
//!
//! Parquet is a columnar format, which means that unlike row formats like [CSV], values are
//! iterated along columns instead of rows. Parquet is similar in spirit to [Arrow], but
//! focuses on storage efficiency whereas Arrow prioritizes compute efficiency.
//!
//! Parquet files are partitioned for scalability. Each file contains metadata,
//! along with zero or more "row groups", each row group containing one or
//! more columns. The APIs in this crate reflect this structure.
//!
//! Data in Parquet files is strongly typed and differentiates between logical
//! and physical types (see [`schema`]). In addition, Parquet files may contain
//! other metadata, such as statistics, which can be used to optimize reading
//! (see [`file::metadata`]).
//! For more details about the Parquet format itself, see the [Parquet spec]
//!
//! [Parquet spec]: https://github.com/apache/parquet-format/blob/master/README.md#file-format
//!
//! # APIs
//!
//! This crate exposes a number of APIs for different use-cases.
//!
//! ## Metadata and Schema
//!
//! The [`schema`] module provides APIs to work with Parquet schemas. The
//! [`file::metadata`] module provides APIs to work with Parquet metadata.
//!
//! ## Read/Write Arrow
//!
//! The [`arrow`] module allows reading and writing Parquet data to/from Arrow `RecordBatch`.
//! This makes for a simple and performant interface to parquet data, whilst allowing workloads
//! to leverage the wide range of data transforms provided by the [arrow] crate, and by the
//! ecosystem of libraries and services using [Arrow] as an interop format.
//!
//! ## Read/Write Arrow Async
//!
//! When the `async` feature is enabled, [`arrow::async_reader`] and [`arrow::async_writer`]
//! provide the ability to read and write [`arrow`] data asynchronously. Additionally, with the
//! `object_store` feature is enabled, [`ParquetObjectReader`](arrow::async_reader::ParquetObjectReader)
//! provides efficient integration with object storage services such as S3 via the [object_store]
//! crate, automatically optimizing IO based on any predicates or projections provided.
//!
//! ## Read/Write Parquet
//!
//! Workloads needing finer-grained control, or avoid a dependence on arrow,
//! can use the lower-level APIs in [`mod@file`]. These APIs expose the underlying parquet
//! data model, and therefore require knowledge of the underlying parquet format,
//! including the details of [Dremel] record shredding and [Logical Types]. Most workloads
//! should prefer the arrow interfaces.
//!
//! [arrow]: https://docs.rs/arrow/latest/arrow/index.html
//! [Arrow]: https://arrow.apache.org/
//! [CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
//! [Dremel]: https://research.google/pubs/pub36632/
//! [Logical Types]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
//! [object_store]: https://docs.rs/object_store/latest/object_store/

#![warn(missing_docs)]
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

/// Automatically generated code from the Parquet thrift definition.
///
/// This module code generated from [parquet.thrift]. See [crate::file] for
/// more information on reading Parquet encoded data.
///
/// [parquet.thrift]: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
// see parquet/CONTRIBUTING.md for instructions on regenerating
// Don't try clippy and format auto generated code
#[allow(clippy::all, missing_docs)]
#[rustfmt::skip]
pub mod format;

#[macro_use]
pub mod data_type;

// Exported for external use, such as benchmarks
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use self::encodings::{decoding, encoding};

experimental!(#[macro_use] mod util);

pub use util::utf8;

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

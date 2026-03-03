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
//! ## Reading and Writing Arrow (`arrow` feature)
//!
//! The [`arrow`] module supports reading and writing Parquet data to/from
//! Arrow [`RecordBatch`]es. Using Arrow is simple and performant, and allows workloads
//! to leverage the wide range of data transforms provided by the [arrow] crate, and by the
//! ecosystem of [Arrow] compatible systems.
//!
//! Most users will use [`ArrowWriter`] for writing and [`ParquetRecordBatchReaderBuilder`] for
//! reading from synchronous IO sources such as files or in-memory buffers.
//!
//! Lower level APIs include
//! * [`ParquetPushDecoder`] for file grained control over interleaving of IO and CPU.
//! * [`ArrowColumnWriter`] for writing using multiple threads,
//! * [`RowFilter`] to apply filters during decode
//!
//! [`ArrowWriter`]: arrow::arrow_writer::ArrowWriter
//! [`ParquetRecordBatchReaderBuilder`]: arrow::arrow_reader::ParquetRecordBatchReaderBuilder
//! [`ParquetPushDecoder`]: arrow::push_decoder::ParquetPushDecoder
//! [`ArrowColumnWriter`]: arrow::arrow_writer::ArrowColumnWriter
//! [`RowFilter`]: arrow::arrow_reader::RowFilter
//!
//! ## `async` Reading and Writing Arrow (`arrow` feature + `async` feature)
//!
//! The [`async_reader`] and [`async_writer`] modules provide async APIs to
//! read and write [`RecordBatch`]es  asynchronously.
//!
//! Most users will use [`AsyncArrowWriter`] for writing and [`ParquetRecordBatchStreamBuilder`]
//! for reading. When the `object_store` feature is enabled, [`ParquetObjectReader`]
//! provides efficient integration with object storage services such as S3 via the [object_store]
//! crate, automatically optimizing IO based on any predicates or projections provided.
//!
//! [`async_reader`]: arrow::async_reader
//! [`async_writer`]: arrow::async_writer
//! [`AsyncArrowWriter`]: arrow::async_writer::AsyncArrowWriter
//! [`ParquetRecordBatchStreamBuilder`]: arrow::async_reader::ParquetRecordBatchStreamBuilder
//! [`ParquetObjectReader`]: arrow::async_reader::ParquetObjectReader
//!
//! ## Variant Logical Type (`variant_experimental` feature)
//!
//! The [`variant`] module supports reading and writing Parquet files
//! with the [Variant Binary Encoding] logical type, which can represent
//! semi-structured data such as JSON efficiently.
//!
//! [Variant Binary Encoding]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//!
//! ## Read/Write Parquet Directly
//!
//! Workloads needing finer-grained control, or to avoid a dependence on arrow,
//! can use the APIs in [`mod@file`] directly. These APIs  are harder to use
//! as they directly use the underlying Parquet data model, and require knowledge
//! of the Parquet format, including the details of [Dremel] record shredding
//! and [Logical Types].
//!
//! [arrow]: https://docs.rs/arrow/latest/arrow/index.html
//! [Arrow]: https://arrow.apache.org/
//! [`RecordBatch`]: https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html
//! [CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
//! [Dremel]: https://research.google/pubs/pub36632/
//! [Logical Types]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
//! [object_store]: https://docs.rs/object_store/latest/object_store/

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/parquet-format/25f05e73d8cd7f5c83532ce51cb4f4de8ba5f2a2/logo/parquet-logos_1.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/parquet-format/25f05e73d8cd7f5c83532ce51cb4f4de8ba5f2a2/logo/parquet-logos_1.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
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

#[cfg(all(
    feature = "flate2",
    not(any(feature = "flate2-zlib-rs", feature = "flate2-rust_backened"))
))]
compile_error!(
    "When enabling `flate2` you must enable one of the features: `flate2-zlib-rs` or `flate2-rust_backened`."
);

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
#[deprecated(
    since = "57.0.0",
    note = "The `format` module is no longer maintained, and will be removed in `59.0.0`"
)]
pub mod format;

#[macro_use]
pub mod data_type;

use std::fmt::Debug;
use std::ops::Range;
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

#[cfg(feature = "encryption")]
experimental!(pub mod encryption);

pub mod file;
pub mod record;
pub mod schema;

mod parquet_macros;
mod parquet_thrift;
pub mod thrift;
/// What data is needed to read the next item from a decoder.
///
/// This is used to communicate between the decoder and the caller
/// to indicate what data is needed next, or what the result of decoding is.
#[derive(Debug)]
pub enum DecodeResult<T: Debug> {
    /// The ranges of data necessary to proceed
    // TODO: distinguish between minimim needed to make progress and what could be used?
    NeedsData(Vec<Range<u64>>),
    /// The decoder produced an output item
    Data(T),
    /// The decoder finished processing
    Finished,
}

#[cfg(feature = "variant_experimental")]
pub mod variant;
experimental!(pub mod geospatial);

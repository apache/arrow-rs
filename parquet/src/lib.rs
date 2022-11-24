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

//! This crate contains the official Native Rust implementation of
//! [Apache Parquet](https://parquet.apache.org/), part of
//! the [Apache Arrow](https://arrow.apache.org/) project.
//!
//! Please see the [parquet crates.io](https://crates.io/crates/parquet)
//! page for feature flags and tips to improve performance.
//!
//! # Getting Started
//! Start with some examples:
//!
//! 1. [mod@file] for reading and writing parquet files using the
//! [ColumnReader](column::reader::ColumnReader) API.
//!
//! 2. [arrow] for reading and writing parquet files to Arrow
//! `RecordBatch`es
//!
//! 3. [arrow::async_reader] for `async` reading and writing parquet
//! files to Arrow `RecordBatch`es (requires the `async` feature).

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

#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use self::util::memory;

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

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
#![allow(incomplete_features)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(
    clippy::approx_constant,
    clippy::cast_ptr_alignment,
    clippy::float_cmp,
    clippy::float_equality_without_abs,
    clippy::from_over_into,
    clippy::many_single_char_names,
    clippy::needless_range_loop,
    clippy::new_without_default,
    clippy::or_fun_call,
    clippy::same_item_push,
    clippy::too_many_arguments,
    clippy::transmute_ptr_to_ptr,
    clippy::upper_case_acronyms
)]

/// Defines a module with an experimental public API
///
/// The module will not be documented, and will only be public if the
/// experimental feature flag is enabled
///
/// Experimental modules have no stability guarantees
macro_rules! experimental_mod {
    ($module:ident $(, #[$meta:meta])*) => {
        #[cfg(feature = "experimental")]
        #[doc(hidden)]
        $(#[$meta])*
        pub mod $module;
        #[cfg(not(feature = "experimental"))]
        $(#[$meta])*
        mod $module;
    };
}

#[macro_use]
pub mod errors;
pub mod basic;

#[macro_use]
pub mod data_type;

// Exported for external use, such as benchmarks
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use self::encodings::{decoding, encoding};

#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use self::util::memory;

experimental_mod!(util, #[macro_use]);
#[cfg(any(feature = "arrow", test))]
pub mod arrow;
pub mod column;
experimental_mod!(compression);
mod encodings;
pub mod file;
pub mod record;
pub mod schema;

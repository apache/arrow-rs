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

//! [`VariantArray`] and compute kernels for the [Variant Binary Encoding] from [Apache Parquet].
//!
//! ## Main APIs
//! - [`VariantArray`] : Represents an array of `Variant` values.
//! - [`VariantArrayBuilder`]: For building [`VariantArray`]
//! - [`json_to_variant`]: Function to convert a batch of JSON strings to a `VariantArray`.
//! - [`variant_to_json`]: Function to convert a `VariantArray` to a batch of JSON strings.
//! - [`mod@cast_to_variant`]: Module to cast other Arrow arrays to `VariantArray`.
//! - [`variant_get`]: Module to get values from a `VariantArray` using a specified [`VariantPath`]
//!
//! ## 🚧 Work In Progress
//!
//! This crate is under active development and is not yet ready for production use.
//! If you are interested in helping, you can find more information on the GitHub [Variant issue]
//!
//! [Variant Binary Encoding]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//! [Apache Parquet]: https://parquet.apache.org/
//! [`VariantPath`]: parquet_variant::VariantPath
//! [Variant issue]: https://github.com/apache/arrow-rs/issues/6736

mod arrow_to_variant;
pub mod cast_to_variant;
mod from_json;
mod shred_variant;
mod to_json;
mod type_conversion;
mod unshred_variant;
mod variant_array;
mod variant_array_builder;
pub mod variant_get;
mod variant_to_arrow;

pub use variant_array::{BorrowedShreddingState, ShreddingState, VariantArray, VariantType};
pub use variant_array_builder::{VariantArrayBuilder, VariantValueArrayBuilder};

pub use cast_to_variant::{cast_to_variant, cast_to_variant_with_options};
pub use from_json::json_to_variant;
pub use shred_variant::shred_variant;
pub use to_json::variant_to_json;
pub use type_conversion::CastOptions;
pub use unshred_variant::unshred_variant;

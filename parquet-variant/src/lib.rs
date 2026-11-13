// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Implementation of [Variant Binary Encoding] from [Apache Parquet].
//!
//! [Variant Binary Encoding]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//! [Apache Parquet]: https://parquet.apache.org/
//!
//! ## Main APIs
//! - [`Variant`]: Represents a variant value, which can be an object, list, or primitive.
//! - [`VariantBuilder`]: For building `Variant` values.
//!
//! ## 🚧 Work In Progress
//!
//! This crate is under active development and is not yet ready for production use.
//! If you are interested in helping, you can find more information on the GitHub [Variant issue]
//!
//! [Variant issue]: https://github.com/apache/arrow-rs/issues/6736
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

mod builder;
mod decoder;
mod path;
mod utils;
mod variant;

pub use arrow_schema::extension::VariantType;
pub use builder::*;
pub use path::{QuotedKeyEscapes, VariantPath, VariantPathElement};
pub use variant::*;

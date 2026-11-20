// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Conversion between [JSON] and the [Variant Binary Encoding] from [Apache Parquet].
//!
//! [JSON]: https://www.json.org/json-en.html
//! [Variant Binary Encoding]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//! [Apache Parquet]: https://parquet.apache.org/
//!
//! * See [`JsonToVariant`] trait for converting a JSON string to a Variant.
//! * See [`VariantToJson`] trait for converting a Variant to a JSON string.
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

mod from_json;
mod to_json;

pub use from_json::{JsonToVariant, append_json};
pub use to_json::VariantToJson;

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

mod from_json;
mod to_json;

pub use from_json::JsonToVariant;
pub use to_json::VariantToJson;

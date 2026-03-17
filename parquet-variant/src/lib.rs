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

mod builder;
mod decoder;
mod path;
mod utils;
mod variant;

pub use builder::*;
pub use path::{VariantPath, VariantPathElement};
pub use variant::*;

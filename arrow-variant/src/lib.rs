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

//! [`arrow-variant`] contains utilities for working with the [Arrow Variant][format] binary format.
//!
//! The Arrow Variant binary format is a serialization of a JSON-like value into a binary format
//! optimized for columnar storage and processing in Apache Arrow. It supports storing primitive
//! values, objects, and arrays with support for complex nested structures.
//!
//! # Creating Variant Values
//!
//! ```
//! # use std::io::Cursor;
//! # use arrow_variant::builder::VariantBuilder;
//! # use arrow_schema::ArrowError;
//! # fn main() -> Result<(), ArrowError> {
//! // Create a builder for variant values
//! let mut metadata_buffer = vec![];
//! let mut builder = VariantBuilder::new(&mut metadata_buffer);
//!
//! // Create an object
//! let mut value_buffer = vec![];
//! let mut object_builder = builder.new_object(&mut value_buffer);
//! object_builder.append_value("foo", 1);
//! object_builder.append_value("bar", 100);
//! object_builder.finish();
//!
//! // value_buffer now contains a valid variant value
//! // builder contains metadata with fields "foo" and "bar"
//!
//! // Create another object reusing the same metadata
//! let mut value_buffer2 = vec![];
//! let mut object_builder2 = builder.new_object(&mut value_buffer2);
//! object_builder2.append_value("foo", 2);
//! object_builder2.append_value("bar", 200);
//! object_builder2.finish();
//!
//! // Create a nested object: the equivalent of {"foo": {"bar": 100}}
//! let mut value_buffer3 = vec![];
//! let mut object_builder3 = builder.new_object(&mut value_buffer3);
//!
//! // Create a nested object under the "foo" field
//! let mut foo_builder = object_builder3.append_object("foo");
//! foo_builder.append_value("bar", 100);
//! foo_builder.finish();
//!
//! // Finish the root object builder
//! object_builder3.finish();
//!
//! // Finalize the metadata
//! builder.finish();
//! # Ok(())
//! # }
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

/// Builder API for creating variant values
pub mod builder;
/// Encoder module for converting values to Variant binary format
pub mod encoder;

// Re-export primary types
pub use builder::{PrimitiveValue, VariantBuilder};
pub use encoder::{VariantBasicType, VariantPrimitiveType};

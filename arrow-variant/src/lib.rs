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

//! Apache Arrow Variant utilities
//!
//! This crate contains utilities for working with the Arrow Variant binary format.
//!
//! # Creating variant values
//!
//! Use the [`VariantBuilder`] to create variant values:
//!
//! ```
//! # use arrow_variant::builder::{VariantBuilder, PrimitiveValue};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut metadata_buffer = vec![];
//! let mut value_buffer = vec![];
//!
//! // Create a builder
//! let mut builder = VariantBuilder::new(&mut metadata_buffer);
//!
//! // For an object
//! {
//!     let mut object = builder.new_object(&mut value_buffer);
//!     object.append_value("name", "Alice");
//!     object.append_value("age", 30);
//!     object.append_value("active", true);
//!     object.append_value("height", 5.8);
//!     object.finish();
//! }
//!
//! // OR for an array
//! /*
//! {
//!     let mut array = builder.new_array(&mut value_buffer);
//!     array.append_value(1);
//!     array.append_value("two");
//!     array.append_value(3.0);
//!     array.finish();
//! }
//! */
//!
//! // Finish the builder
//! builder.finish();
//! # Ok(())
//! # }
//! ```
//!
//! # Reading variant values
//!
//! Use the [`Variant`] type to read variant values:
//!
//! ```
//! # use arrow_variant::builder::VariantBuilder;
//! # use arrow_variant::Variant;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let mut metadata_buffer = vec![];
//! # let mut value_buffer = vec![];
//! # {
//! #     let mut builder = VariantBuilder::new(&mut metadata_buffer);
//! #     let mut object = builder.new_object(&mut value_buffer);
//! #     object.append_value("name", "Alice");
//! #     object.append_value("age", 30);
//! #     object.finish();
//! #     builder.finish();
//! # }
//! // Parse the variant
//! let variant = Variant::new(&metadata_buffer, &value_buffer);
//!
//! // Access object fields
//! if let Some(name) = variant.get("name")? {
//!    assert_eq!(name.as_string()?, "Alice");
//! }
//!
//! if let Some(age) = variant.get("age")? {
//!    assert_eq!(age.as_i32()?, 30);
//! }
//! # Ok(())
//! # }
//! ```

/// The `builder` module provides tools for creating variant values.
pub mod builder;

/// The `decoder` module provides tools for parsing the variant binary format.
pub mod decoder;

/// The `encoder` module provides tools for converting values to Variant binary format.
pub mod encoder;

/// The `variant` module provides the core `Variant` data type.
pub mod variant;

// Re-export primary types
pub use crate::builder::{PrimitiveValue, VariantBuilder};
pub use crate::encoder::{VariantBasicType, VariantPrimitiveType};
pub use crate::variant::Variant;

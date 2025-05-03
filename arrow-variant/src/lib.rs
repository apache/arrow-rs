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

//! Transfer data between the Arrow Variant binary format and JSON.
//!
//! The Arrow Variant extension type stores data as two binary values:
//! metadata and value. This crate provides utilities to convert between
//! JSON and the Variant binary format.
//!
//! # Example
//!
//! ```rust
//! use arrow_variant::{from_json, to_json};
//! use arrow_schema::extension::Variant;
//!
//! // Convert JSON to Variant
//! let json_str = r#"{"key":"value"}"#;
//! let variant = from_json(json_str).unwrap();
//!
//! // Convert Variant back to JSON
//! let json = to_json(&variant).unwrap();
//! assert_eq!(json_str, json);
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub mod error;
pub mod reader;
pub mod writer;
/// Encoder module for converting JSON to Variant binary format
pub mod encoder;
/// Decoder module for converting Variant binary format to JSON
pub mod decoder;
/// Utilities for working with variant as struct arrays
pub mod variant_utils;

pub use error::Error;
pub use reader::{from_json, from_json_array, from_json_value, from_json_value_array};
pub use writer::{to_json, to_json_array, to_json_value, to_json_value_array};
pub use encoder::{encode_value, encode_json, VariantBasicType, VariantPrimitiveType};
pub use decoder::{decode_value, decode_json};
pub use variant_utils::{create_variant_array, get_variant, validate_struct_array, create_empty_variant_array};

/// Utility functions for working with Variant metadata
pub mod metadata;
pub use metadata::{create_metadata, parse_metadata};

/// Integration utilities and tests
pub mod integration;
pub use integration::{create_test_variant, create_test_variant_array, validate_variant_roundtrip};

/// Converts a JSON string to a Variant and back
/// 
/// # Examples
/// 
/// ```
/// use arrow_variant::{from_json, to_json};
/// 
/// let json_str = r#"{"key":"value"}"#;
/// let variant = from_json(json_str).unwrap();
/// let result = to_json(&variant).unwrap();
/// assert_eq!(json_str, result);
/// ```
pub fn validate_json_roundtrip(json_str: &str) -> Result<(), Error> {
    let variant = from_json(json_str)?;
    let result = to_json(&variant)?;
    assert_eq!(json_str, result);
    Ok(())
} 
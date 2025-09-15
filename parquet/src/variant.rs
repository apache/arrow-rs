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

//! ⚠️ Experimental Support for reading and writing [`Variant`]s to / from Parquet files ⚠️
//!
//! This is a 🚧 Work In Progress
//!
//! Note: Requires the `variant_experimental` feature of the `parquet` crate to be enabled.
//!
//! # Features
//! * [`Variant`] represents variant value, which can be an object, list, or primitive.
//! * [`VariantBuilder`] for building `Variant` values.
//! * [`VariantArray`] for representing a column of Variant values.
//! * [`compute`] module with functions for manipulating Variants, such as
//!   [`variant_get`] to extracting a value by path and functions to convert
//!   between `Variant` and JSON.
//!
//! [Variant Logical Type]: Variant
//! [`VariantArray`]: compute::VariantArray
//! [`variant_get`]: compute::variant_get
//!
//! # Example: Writing a Parquet file with Variant column
//! ```rust
//! # use parquet::variant::compute::{VariantArray, VariantArrayBuilder};
//! # use parquet::variant::VariantBuilderExt;
//! # use std::sync::Arc;
//! # use arrow_array::{ArrayRef, RecordBatch};
//! # use parquet::arrow::ArrowWriter;
//! # fn main() -> Result<(), parquet::errors::ParquetError> {
//!  // Use the VariantArrayBuilder to build a VariantArray
//!  let mut builder = VariantArrayBuilder::new(3);
//!  // row 1: {"name": "Alice"}
//!  builder.new_object().with_field("name", "Alice").finish();
//!  let array = builder.build();
//!
//! // TODO support writing VariantArray directly
//! // at the moment it panics when trying to downcast to a struct array
//! // https://github.com/apache/arrow-rs/issues/8296
//! //  let array: ArrayRef = Arc::new(array);
//! let array: ArrayRef = Arc::new(array.into_inner());
//!
//!  // create a RecordBatch with the VariantArray
//!  let batch = RecordBatch::try_from_iter(vec![("data", array)])?;
//!
//!  // write the RecordBatch to a Parquet file
//!  let file = std::fs::File::create("variant.parquet")?;
//!  let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
//!  writer.write(&batch)?;
//!  writer.close()?;
//!
//! # std::fs::remove_file("variant.parquet")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Writing JSON with a Parquet file with Variant column
//! ```rust
//! # use std::sync::Arc;
//! # use arrow_array::{ArrayRef, RecordBatch, StringArray};
//! # use parquet::variant::compute::json_to_variant;
//! # use parquet::variant::compute::VariantArray;
//! # use parquet::arrow::ArrowWriter;
//! # fn main() -> Result<(), parquet::errors::ParquetError> {
//! // Create an array of JSON strings, simulating a column of JSON data
//! // TODO use StringViewArray when available
//! let input_array = StringArray::from(vec![
//!   Some(r#"{"name": "Alice", "age": 30}"#),
//!   Some(r#"{"name": "Bob", "age": 25, "address": {"city": "New York"}}"#),
//!   None,
//!   Some("{}"),
//! ]);
//! let input_array: ArrayRef = Arc::new(input_array);
//!
//! // Convert the JSON strings to a VariantArray
//! let array: VariantArray = json_to_variant(&input_array)?;
//!
//! // TODO support writing VariantArray directly
//! // at the moment it panics when trying to downcast to a struct array
//! // https://github.com/apache/arrow-rs/issues/8296
//! //  let array: ArrayRef = Arc::new(array);
//! let array: ArrayRef = Arc::new(array.into_inner());
//!
//!  // create a RecordBatch with the VariantArray
//!  let batch = RecordBatch::try_from_iter(vec![("data", array)])?;
//!
//!  // write the RecordBatch to a Parquet file
//!  let file = std::fs::File::create("variant-json.parquet")?;
//!  let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
//!  writer.write(&batch)?;
//!  writer.close()?;
//! # std::fs::remove_file("variant-json.parquet")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Reading a Parquet file with Variant column
//! (TODO: add example)
pub use parquet_variant::*;
pub use parquet_variant_compute as compute;

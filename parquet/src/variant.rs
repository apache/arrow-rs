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
//! * Representation of [`Variant`], and [`VariantArray`] for working with
//!   Variant values (see [`parquet_variant`] for more details)
//! * Kernels for working with arrays of Variant values
//!   such as conversion between `Variant` and JSON, and shredding/unshredding
//!   (see [`parquet_variant_compute`] for more details)
//!
//! # Example: Writing a Parquet file with Variant column
//! ```rust
//! # use parquet::variant::{VariantArray, VariantType, VariantArrayBuilder, VariantBuilderExt};
//! # use std::sync::Arc;
//! # use arrow_array::{Array, ArrayRef, RecordBatch};
//! # use arrow_schema::{DataType, Field, Schema};
//! # use parquet::arrow::ArrowWriter;
//! # fn main() -> Result<(), parquet::errors::ParquetError> {
//!  // Use the VariantArrayBuilder to build a VariantArray
//!  let mut builder = VariantArrayBuilder::new(3);
//!  builder.new_object().with_field("name", "Alice").finish(); // row 1: {"name": "Alice"}
//!  builder.append_value("such wow"); // row 2: "such wow" (a string)
//!  let array = builder.build();
//!
//!  // Since VariantArray is an ExtensionType, it needs to be converted
//!  // to an ArrayRef and Field with the appropriate metadata
//!  // before it can be written to a Parquet file
//!  let field = array.field("data");
//!  let array = ArrayRef::from(array);
//!  // create a RecordBatch with the VariantArray
//!  let schema = Schema::new(vec![field]);
//!  let batch = RecordBatch::try_new(Arc::new(schema), vec![array])?;
//!
//!  // Now you can write the RecordBatch to the Parquet file, as normal
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
//! # Example: Writing JSON into a Parquet file with Variant column
//! ```rust
//! # use std::sync::Arc;
//! # use arrow_array::{ArrayRef, RecordBatch, StringArray};
//! # use arrow_schema::Schema;
//! # use parquet::variant::{json_to_variant, VariantArray};
//! # use parquet::arrow::ArrowWriter;
//! # fn main() -> Result<(), parquet::errors::ParquetError> {
//!  // Create an array of JSON strings, simulating a column of JSON data
//!  let input_array: ArrayRef = Arc::new(StringArray::from(vec![
//!   Some(r#"{"name": "Alice", "age": 30}"#),
//!   Some(r#"{"name": "Bob", "age": 25, "address": {"city": "New York"}}"#),
//!   None,
//!   Some("{}"),
//!  ]));
//!
//!  // Convert the JSON strings to a VariantArray
//!  let array: VariantArray = json_to_variant(&input_array)?;
//!  // create a RecordBatch with the VariantArray
//!  let schema = Schema::new(vec![array.field("data")]);
//!  let batch = RecordBatch::try_new(Arc::new(schema), vec![ArrayRef::from(array)])?;
//!
//!  // write the RecordBatch to a Parquet file as normal
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
//!
//! Use the [`VariantType`] extension type to find the Variant column:
//!
//! ```
//! # use std::sync::Arc;
//! # use std::path::PathBuf;
//! # use arrow_array::{ArrayRef, RecordBatch, RecordBatchReader};
//! # use parquet::variant::{Variant, VariantArray, VariantType};
//! # use parquet::arrow::arrow_reader::ArrowReaderBuilder;
//! # fn main() -> Result<(), parquet::errors::ParquetError> {
//! # use arrow_array::StructArray;
//! # fn file_path() -> PathBuf { // return a testing file path
//! #    PathBuf::from(arrow::util::test_util::parquet_test_data())
//! #   .join("..")
//! #   .join("shredded_variant")
//! #   .join("case-075.parquet")
//! # }
//! // Read the Parquet file using standard Arrow Parquet reader.
//! // Note this file has 2 columns: "id", "var", and the "var" column
//  // contains a variant that looks like this:
//  // "Variant(metadata=VariantMetadata(dict={}), value=Variant(type=STRING, value=iceberg))"
//! let file = std::fs::File::open(file_path())?;
//! let mut reader = ArrowReaderBuilder::try_new(file)?.build()?;
//!
//! // You can check if a column contains a Variant using
//! // the VariantType extension type
//! let schema = reader.schema();
//! let field = schema.field_with_name("var")?;
//! assert!(field.try_extension_type::<VariantType>().is_ok());
//!
//! // The reader will yield RecordBatches with a StructArray
//! // to convert them to VariantArray, use VariantArray::try_new
//! let batch = reader.next().unwrap().unwrap();
//!
//! let col = batch.column_by_name("var").unwrap();
//! let var_array = VariantArray::try_new(col)?;
//! assert_eq!(var_array.len(), 1);
//! let var_value: Variant = var_array.value(0);
//! assert_eq!(var_value, Variant::from("iceberg")); // the value in case-075.parquet
//! # Ok(())
//! # }
//! ```
pub use parquet_variant::*;
pub use parquet_variant_compute::*;

#[cfg(test)]
mod tests {
    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::ArrowReaderBuilder;
    use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use crate::file::reader::ChunkReader;
    use arrow::util::test_util::parquet_test_data;
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::Schema;
    use bytes::Bytes;
    use parquet_variant::{Variant, VariantBuilderExt};
    use parquet_variant_compute::{VariantArray, VariantArrayBuilder, VariantType};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn roundtrip_basic() {
        roundtrip(variant_array());
    }

    /// Ensure a file with Variant LogicalType, written by another writer in
    /// parquet-testing, can be read as a VariantArray
    #[test]
    fn read_logical_type() {
        // Note: case-075 2 columns ("id", "var")
        // The variant looks like this:
        // "Variant(metadata=VariantMetadata(dict={}), value=Variant(type=STRING, value=iceberg))"
        let batch = read_shredded_variant_test_case("case-075.parquet");

        assert_variant_metadata(&batch, "var");
        let var_column = batch.column_by_name("var").expect("expected var column");
        let var_array =
            VariantArray::try_new(&var_column).expect("expected var column to be a VariantArray");

        // verify the value
        assert_eq!(var_array.len(), 1);
        assert!(var_array.is_valid(0));
        let var_value = var_array.value(0);
        assert_eq!(var_value, Variant::from("iceberg"));
    }

    /// Writes a variant to a parquet file and ensures the parquet logical type
    /// annotation is correct
    #[test]
    fn write_logical_type() {
        let array = variant_array();
        let batch = variant_array_to_batch(array);
        let buffer = write_to_buffer(&batch);

        // read the parquet file's metadata and verify the logical type
        let metadata = read_metadata(&Bytes::from(buffer));
        let schema = metadata.file_metadata().schema_descr();
        let fields = schema.root_schema().get_fields();
        assert_eq!(fields.len(), 1);
        let field = &fields[0];
        assert_eq!(field.name(), "data");
        // data should have been written with the Variant logical type
        assert_eq!(
            field.get_basic_info().logical_type_ref(),
            Some(&crate::basic::LogicalType::Variant {
                specification_version: None
            })
        );
    }

    /// Return a VariantArray with 3 rows:
    ///
    /// 1. `{"name": "Alice"}`
    /// 2. `"such wow"` (a string)
    /// 3. `null`
    fn variant_array() -> VariantArray {
        let mut builder = VariantArrayBuilder::new(3);
        // row 1: {"name": "Alice"}
        builder.new_object().with_field("name", "Alice").finish();
        // row 2: "such wow" (a string)
        builder.append_value("such wow");
        // row 3: null
        builder.append_null();
        builder.build()
    }

    /// Writes a VariantArray to a parquet file and reads it back, verifying that
    /// the data is the same
    fn roundtrip(array: VariantArray) {
        let source_batch = variant_array_to_batch(array);
        assert_variant_metadata(&source_batch, "data");

        let buffer = write_to_buffer(&source_batch);
        let result_batch = read_to_batch(Bytes::from(buffer));
        assert_variant_metadata(&result_batch, "data");
        assert_eq!(result_batch, source_batch); // NB this also checks the schemas
    }

    /// creates a RecordBatch with a single column "data" from a VariantArray,
    fn variant_array_to_batch(array: VariantArray) -> RecordBatch {
        let field = array.field("data");
        let schema = Schema::new(vec![field]);
        RecordBatch::try_new(Arc::new(schema), vec![ArrayRef::from(array)]).unwrap()
    }

    /// writes a RecordBatch to memory buffer and returns the buffer
    fn write_to_buffer(batch: &RecordBatch) -> Vec<u8> {
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buffer
    }

    /// Reads the Parquet metadata
    fn read_metadata<T: ChunkReader + 'static>(input: &T) -> ParquetMetaData {
        let mut reader = ParquetMetaDataReader::new();
        reader.try_parse(input).unwrap();
        reader.finish().unwrap()
    }

    /// Reads a RecordBatch from a reader (e.g. Vec or File)
    fn read_to_batch<T: ChunkReader + 'static>(reader: T) -> RecordBatch {
        let reader = ArrowReaderBuilder::try_new(reader)
            .unwrap()
            .build()
            .unwrap();
        let mut batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        batches.swap_remove(0)
    }

    /// Verifies the variant metadata is present in the schema for the specified
    /// field name.
    fn assert_variant_metadata(batch: &RecordBatch, field_name: &str) {
        let schema = batch.schema();
        let field = schema
            .field_with_name(field_name)
            .expect("could not find expected field");

        // explicitly check the metadata so it is clear in the tests what the
        // names are
        let metadata_value = field
            .metadata()
            .get("ARROW:extension:name")
            .expect("metadata does not exist");

        assert_eq!(metadata_value, "arrow.parquet.variant");

        // verify that `VariantType` also correctly finds the metadata
        field
            .try_extension_type::<VariantType>()
            .expect("VariantExtensionType should be readable");
    }

    /// Read the specified test case filename from parquet-testing
    /// See parquet-testing/shredded_variant/cases.json for more details
    fn read_shredded_variant_test_case(name: &str) -> RecordBatch {
        let case_file = PathBuf::from(parquet_test_data())
            .join("..") // go up from data/ to parquet-testing/
            .join("shredded_variant")
            .join(name);
        let case_file = std::fs::File::open(case_file).unwrap();
        read_to_batch(case_file)
    }
}

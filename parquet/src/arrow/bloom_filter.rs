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

//! Arrow-aware bloom filter
//!
//! This module provides [`ArrowSbbf`], a wrapper around [`Sbbf`] that handles
//! Arrow-to-Parquet type coercion when checking bloom filters.
//!
//! # Overview
//!
//! Arrow supports types like `Int8` and `Int16` that have no corresponding Parquet type.
//! Data of these types must be coerced to the appropriate Parquet type when writing.
//!
//! For example, Arrow small integer types are coerced writing to Parquet:
//! - `Int8` → `INT32` (i8 → i32)
//! - `Int16` → `INT32` (i16 → i32)
//! - `UInt8` → `INT32` (u8 → u32 → i32)
//! - `UInt16` → `INT32` (u16 → u32 → i32)
//!
//! Decimal types with small precision are also coerced:
//! - `Decimal32/64/128/256(precision 1-9)` → `INT32` (truncated via `as i32`)
//! - `Decimal64/128/256(precision 10-18)` → `INT64` (truncated via `as i64`)
//!
//! In these situations, bloom filters store hashes of the *physical* Parquet values, but Arrow users
//! will often need to check using the *logical* Arrow values.
//!
//! [`ArrowSbbf`] wraps an [`Sbbf`] and an Arrow [`DataType`], automatically coercing
//! values to their Parquet representation before checking the bloom filter.
//!
//! # Example
//!
//! ```ignore
//! use arrow_schema::DataType;
//! use parquet::arrow::bloom_filter::ArrowSbbf;
//!
//! // Get bloom filter from row group reader
//! let sbbf = row_group_reader.get_column_bloom_filter(0)?;
//! let arrow_sbbf = ArrowSbbf::new(&sbbf, &DataType::Int8);
//!
//! // Check with i8 value - automatically coerced to i32
//! let value: i8 = 42;
//! if arrow_sbbf.check(&value) {
//!     println!("Value might be present");
//! }
//! ```
//!
//! # Known Limitations
//!
//! **Date64 with coerce_types=true**: When [`WriterProperties::set_coerce_types`] is enabled,
//! `Date64` values (i64 milliseconds) are coerced to `Date32` (i32 days). Currently,
//! [`ArrowSbbf`] cannot detect this case and will produce false negatives when checking
//! `Date64` values against bloom filters created with `coerce_types=true`.
//!
//! Workaround: Manually convert milliseconds to days before checking:
//! ```ignore
//! let ms = 864_000_000_i64; // 10 days in milliseconds
//! let days = (ms / 86_400_000) as i32;
//! arrow_sbbf.check(&days); // Check with i32 days instead
//! ```
//!
//! [`WriterProperties::set_coerce_types`]: crate::file::properties::WriterPropertiesBuilder::set_coerce_types

use crate::bloom_filter::Sbbf;
use crate::data_type::AsBytes;
use arrow_schema::DataType;

/// Wraps an [`Sbbf`] and provides automatic type coercion based on Arrow schema.
/// Ensures that checking bloom filters works correctly for Arrow types that require
/// coercion to Parquet physical types (e.g., Int8 → INT32).
#[derive(Debug, Clone)]
pub struct ArrowSbbf<'a> {
    sbbf: &'a Sbbf,
    arrow_type: &'a DataType,
}

impl<'a> ArrowSbbf<'a> {
    /// Create a new Arrow-aware bloom filter wrapper
    /// * `sbbf` - Parquet bloom filter for the column
    /// * `arrow_type` - Arrow data type for the column
    pub fn new(sbbf: &'a Sbbf, arrow_type: &'a DataType) -> Self {
        Self { sbbf, arrow_type }
    }

    /// Check if a value might be present in the bloom filter
    /// Automatically handles type coercion based on the Arrow data type.
    ///
    /// Returns `true` if the value might be present (may have false positives),
    /// or `false` if the value is definitely not present.
    pub fn check<T: AsBytes + ?Sized>(&self, value: &T) -> bool {
        match self.arrow_type {
            DataType::Int8 => {
                // Arrow Int8 -> Parquet INT32
                let bytes = value.as_bytes();
                if bytes.len() == 1 {
                    let i8_val = i8::from_le_bytes([bytes[0]]);
                    let i32_val = i8_val as i32;
                    self.sbbf.check(&i32_val)
                } else {
                    // Unexpected size, fall back to direct check
                    self.sbbf.check(value)
                }
            }
            DataType::Int16 => {
                // Arrow Int16 -> Parquet INT32
                let bytes = value.as_bytes();
                if bytes.len() == 2 {
                    let i16_val = i16::from_le_bytes([bytes[0], bytes[1]]);
                    let i32_val = i16_val as i32;
                    self.sbbf.check(&i32_val)
                } else {
                    // Unexpected size, fall back to direct check
                    self.sbbf.check(value)
                }
            }
            DataType::UInt8 => {
                // Arrow UInt8 -> Parquet INT32
                let bytes = value.as_bytes();
                if bytes.len() == 1 {
                    let u8_val = bytes[0];
                    let u32_val = u8_val as u32;
                    let i32_val = u32_val as i32;
                    self.sbbf.check(&i32_val)
                } else {
                    // Unexpected size, fall back to direct check
                    self.sbbf.check(value)
                }
            }
            DataType::UInt16 => {
                // Arrow UInt16 -> Parquet INT32
                let bytes = value.as_bytes();
                if bytes.len() == 2 {
                    let u16_val = u16::from_le_bytes([bytes[0], bytes[1]]);
                    let u32_val = u16_val as u32;
                    let i32_val = u32_val as i32;
                    self.sbbf.check(&i32_val)
                } else {
                    // Unexpected size, fall back to direct check
                    self.sbbf.check(value)
                }
            }
            DataType::Decimal32(precision, _)
            | DataType::Decimal64(precision, _)
            | DataType::Decimal128(precision, _)
            | DataType::Decimal256(precision, _)
                if *precision >= 1 && *precision <= 9 =>
            {
                // Decimal with precision 1-9 -> Parquet INT32
                // Writer truncates via `as i32`
                let bytes = value.as_bytes();
                match bytes.len() {
                    4 => {
                        // Decimal32: i32 value, directly reinterpret
                        let i32_val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        self.sbbf.check(&i32_val)
                    }
                    8 => {
                        // Decimal64: i64 value, truncate to i32
                        let i64_val = i64::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ]);
                        let i32_val = i64_val as i32;
                        self.sbbf.check(&i32_val)
                    }
                    16 => {
                        // Decimal128: i128 value, truncate to i32
                        let i128_val = i128::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
                            bytes[13], bytes[14], bytes[15],
                        ]);
                        let i32_val = i128_val as i32;
                        self.sbbf.check(&i32_val)
                    }
                    32 => {
                        // Decimal256: i256 stored as 32 bytes, truncate to i32
                        // Read first 16 bytes as i128, then truncate
                        let i128_val = i128::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
                            bytes[13], bytes[14], bytes[15],
                        ]);
                        let i32_val = i128_val as i32;
                        self.sbbf.check(&i32_val)
                    }
                    _ => self.sbbf.check(value),
                }
            }
            DataType::Decimal64(precision, _)
            | DataType::Decimal128(precision, _)
            | DataType::Decimal256(precision, _)
                if *precision >= 10 && *precision <= 18 =>
            {
                // Decimal with precision 10-18 -> Parquet INT64
                // Writer truncates via `as i64`
                let bytes = value.as_bytes();
                match bytes.len() {
                    8 => {
                        // Decimal64: i64 value, directly use
                        let i64_val = i64::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ]);
                        self.sbbf.check(&i64_val)
                    }
                    16 => {
                        // Decimal128: i128 value, truncate to i64
                        let i128_val = i128::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
                            bytes[13], bytes[14], bytes[15],
                        ]);
                        let i64_val = i128_val as i64;
                        self.sbbf.check(&i64_val)
                    }
                    32 => {
                        // Decimal256: i256 stored as 32 bytes, truncate to i64
                        // Read first 16 bytes as i128, then truncate
                        let i128_val = i128::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
                            bytes[13], bytes[14], bytes[15],
                        ]);
                        let i64_val = i128_val as i64;
                        self.sbbf.check(&i64_val)
                    }
                    _ => self.sbbf.check(value),
                }
            }
            // No coercion needed
            _ => self.sbbf.check(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::file::properties::{ReaderProperties, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use arrow_array::{
        ArrayRef, Date64Array, Decimal128Array, Decimal32Array, Float16Array, Int16Array,
        Int8Array, RecordBatch, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempfile;

    /// Helper function to build a bloom filter for testing
    ///
    /// Writes the given array to a Parquet file with bloom filters enabled,
    /// then reads it back and returns the bloom filter for the first column.
    fn build_sbbf(array: ArrayRef, field: Field) -> Sbbf {
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write with bloom filter enabled
        let mut file = tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();

        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back with bloom filters enabled
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();

        // Get and return the bloom filter
        let row_group_reader = reader.get_row_group(0).unwrap();
        row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist")
            .clone()
    }

    #[test]
    fn test_int8_coercion() {
        // Int8 -> Parquet INT32 (sign extension changes bytes)
        let test_value = 3_i8;
        let array = Arc::new(Int8Array::from(vec![1_i8, 2, test_value, 4, 5]));
        let field = Field::new("col", DataType::Int8, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should fail (bloom filter has i32)
        assert!(!sbbf.check(&test_value), "Direct check should fail");

        // ArrowSbbf check should succeed (coerces i8 to i32)
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_int16_coercion() {
        // Int16 -> Parquet INT32 (sign extension changes bytes)
        let test_value = 300_i16;
        let array = Arc::new(Int16Array::from(vec![100_i16, 200, test_value, 400, 500]));
        let field = Field::new("col", DataType::Int16, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should fail (bloom filter has i32)
        assert!(!sbbf.check(&test_value), "Direct check should fail");

        // ArrowSbbf check should succeed (coerces i16 to i32)
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_uint8_coercion() {
        // UInt8 -> Parquet INT32 (zero extension changes bytes)
        let test_value = 30_u8;
        let array = Arc::new(UInt8Array::from(vec![10_u8, 20, test_value, 40, 50]));
        let field = Field::new("col", DataType::UInt8, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should fail (bloom filter has i32)
        assert!(!sbbf.check(&test_value), "Direct check should fail");

        // ArrowSbbf check should succeed (coerces u8 to i32)
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_uint16_coercion() {
        // UInt16 -> Parquet INT32 (zero extension changes bytes)
        let test_value = 3000_u16;
        let array = Arc::new(UInt16Array::from(vec![
            1000_u16, 2000, test_value, 4000, 5000,
        ]));
        let field = Field::new("col", DataType::UInt16, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should fail (bloom filter has i32)
        assert!(!sbbf.check(&test_value), "Direct check should fail");

        // ArrowSbbf check should succeed (coerces u16 to i32)
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_uint32_no_coercion() {
        // UInt32 -> Parquet INT32 (reinterpret cast preserves bit pattern)
        let test_value = 3_000_000_000_u32; // > i32::MAX
        let array = Arc::new(UInt32Array::from(vec![
            100_u32,
            test_value,
            4_000_000_000_u32,
        ]));
        let field = Field::new("col", DataType::UInt32, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should succeed (bit pattern preserved)
        assert!(sbbf.check(&test_value), "Direct check should succeed");

        // ArrowSbbf check should also succeed
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_uint64_no_coercion() {
        // UInt64 -> Parquet INT64 (reinterpret cast preserves bit pattern)
        let test_value = 10_000_000_000_000_000_000_u64; // > i64::MAX
        let array = Arc::new(UInt64Array::from(vec![
            100_u64,
            test_value,
            15_000_000_000_000_000_000_u64,
        ]));
        let field = Field::new("col", DataType::UInt64, false);
        let sbbf = build_sbbf(array, field.clone());

        // Direct check should succeed (bit pattern preserved)
        assert!(sbbf.check(&test_value), "Direct check should succeed");

        // ArrowSbbf check should also succeed
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_decimal128_small_coercion() {
        // Decimal128(5, 2) -> Parquet INT32 (precision 1-9, truncation changes bytes)
        let test_value = 20075_i128;
        let array = Decimal128Array::from(vec![10050_i128, test_value, 30099_i128])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let field = Field::new("col", DataType::Decimal128(5, 2), false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Write with bloom filter
        let mut file = tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();
        let bloom_filter = row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist");

        // Test 1: Direct check with i128 bytes should fail (bloom filter has i32)
        let test_bytes = test_value.to_le_bytes();
        let direct_result = bloom_filter.check(&test_bytes[..]);
        println!(
            "Decimal128(5,2): Direct Sbbf check with i128 bytes = {}",
            direct_result
        );
        assert!(
            !direct_result,
            "Direct check with i128 bytes should fail (bloom filter has i32)"
        );

        // Test 2: ArrowSbbf check should succeed (coerces i128 to i32)
        let arrow_sbbf = ArrowSbbf::new(&bloom_filter, field.data_type());
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        println!(
            "Decimal128(5,2): ArrowSbbf check with i128 bytes = {}",
            arrow_result
        );
        assert!(
            arrow_result,
            "ArrowSbbf check should succeed (coerces to i32)"
        );
    }

    #[test]
    fn test_decimal128_medium_coercion() {
        // Decimal128(15, 2) -> Parquet INT64 (precision 10-18, truncation changes bytes)
        // Direct Sbbf check: false (bug - checking i128 but filter has i64)
        // ArrowSbbf check: true (fix - coerces i128 to i64)
        let test_value = 9876543210987_i128;
        let array = Decimal128Array::from(vec![1234567890123_i128, test_value, 5555555555555_i128])
            .with_precision_and_scale(15, 2)
            .unwrap();
        let field = Field::new("col", DataType::Decimal128(15, 2), false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Write with bloom filter
        let mut file = tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();
        let bloom_filter = row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist");

        // Test 1: Direct check with i128 bytes should fail (bloom filter has i64)
        let test_bytes = test_value.to_le_bytes();
        let direct_result = bloom_filter.check(&test_bytes[..]);
        println!(
            "Decimal128(15,2): Direct Sbbf check with i128 bytes = {}",
            direct_result
        );
        assert!(
            !direct_result,
            "Direct check with i128 bytes should fail (bloom filter has i64)"
        );

        // Test 2: ArrowSbbf check should succeed (coerces i128 to i64)
        let arrow_sbbf = ArrowSbbf::new(&bloom_filter, field.data_type());
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        println!(
            "Decimal128(15,2): ArrowSbbf check with i128 bytes = {}",
            arrow_result
        );
        assert!(
            arrow_result,
            "ArrowSbbf check should succeed (coerces to i64)"
        );
    }

    #[test]
    fn test_decimal32_no_coercion() {
        // Decimal32(5, 2) -> Parquet INT32 (reinterpret cast preserves bit pattern for small precision)
        // Direct Sbbf check: true (works without ArrowSbbf for Decimal32)
        // ArrowSbbf check: true (wrapper doesn't hurt)
        // Note: Decimal32 stores i32 natively, so no truncation occurs
        let test_value = 20075_i32;
        let array = Decimal32Array::from(vec![10050_i32, test_value, 30099_i32])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let field = Field::new("col", DataType::Decimal32(5, 2), false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // Direct check should succeed (bit pattern preserved)
        assert!(sbbf.check(&test_value), "Direct check should succeed");

        // ArrowSbbf check should also succeed
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_float16_no_coercion() {
        // Float16 -> Parquet FIXED_LEN_BYTE_ARRAY(2) (stored as-is)
        // Direct Sbbf check: true (bit pattern preserved)
        // ArrowSbbf check: true (wrapper doesn't hurt)
        use half::f16;
        let test_value = f16::from_f32(2.5);
        let array = Float16Array::from(vec![f16::from_f32(1.5), test_value, f16::from_f32(3.5)]);
        let field = Field::new("col", DataType::Float16, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Write with bloom filter
        let mut file = tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();
        let bloom_filter = row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist");

        // Test 1: Direct check with f16 bytes should work (bit pattern preserved)
        let test_bytes = test_value.to_le_bytes();
        let direct_result = bloom_filter.check(&test_bytes[..]);
        println!("Float16: Direct Sbbf check with bytes = {}", direct_result);
        assert!(direct_result, "Direct check with bytes should succeed");

        // Test 2: ArrowSbbf check should also work
        let arrow_sbbf = ArrowSbbf::new(&bloom_filter, field.data_type());
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        println!("Float16: ArrowSbbf check with bytes = {}", arrow_result);
        assert!(arrow_result, "ArrowSbbf check should succeed");
    }

    #[test]
    fn test_date64_no_coercion_by_default() {
        // Date64 -> Parquet INT64 (stored as-is when coerce_types=false, which is default)
        // Direct Sbbf check: true (no coercion by default)
        // ArrowSbbf check: true (wrapper doesn't hurt)
        // Note: Date64 CAN be coerced to Date32/INT32 with coerce_types=true,
        // but that's not the default behavior, so bloom filters work correctly by default
        let test_value = 2_000_000_000_i64;
        let array = Date64Array::from(vec![1_000_000_000_i64, test_value, 3_000_000_000_i64]);
        let field = Field::new("col", DataType::Date64, false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // Direct check should succeed (no coercion by default)
        assert!(sbbf.check(&test_value), "Direct check should succeed");

        // ArrowSbbf check should also succeed
        let arrow_sbbf = ArrowSbbf::new(&sbbf, field.data_type());
        assert!(
            arrow_sbbf.check(&test_value),
            "ArrowSbbf check should succeed"
        );
    }

    #[test]
    fn test_date64_with_coerce_types() {
        // This test documents a KNOWN LIMITATION of ArrowSbbf.
        //
        // When WriterProperties::set_coerce_types(true) is used:
        // - Date64 (i64 milliseconds) -> Date32 (i32 days) -> Parquet INT32
        // - Conversion: milliseconds / 86_400_000 = days
        //
        // ArrowSbbf cannot currently detect this case because it only has access to
        // the Arrow DataType, not the Parquet schema or coerce_types setting.
        //
        // Potential solutions (not implemented):
        // 1. Double-check both i64 and i32 representations (increases false positive rate ~2x)
        // 2. Change ArrowSbbf API to accept Parquet column descriptor (breaking change)
        // 3. Require users to manually convert ms -> days when using coerce_types
        //
        // We chose option 3 (documented limitation) because:
        // - coerce_types=true is not the default
        // - Date64 + bloom filters + coerce_types is a rare combination
        // - Can wait for user feedback before adding complexity

        const MS_PER_DAY: i64 = 86_400_000;
        let test_value_ms = 10 * MS_PER_DAY; // 10 days in milliseconds
        let array = Date64Array::from(vec![5 * MS_PER_DAY, test_value_ms, 15 * MS_PER_DAY]);
        let field = Field::new("col", DataType::Date64, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Write with bloom filter AND coerce_types enabled
        let mut file = tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_coerce_types(true) // This causes Date64 -> Date32 (INT32)
            .build();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();
        let bloom_filter = row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist");

        // Test 1: Direct check with i64 milliseconds fails (bloom filter has i32 days)
        let direct_result = bloom_filter.check(&test_value_ms);
        println!(
            "Date64 (coerce_types=true): Direct Sbbf check with i64 ms = {}",
            direct_result
        );
        assert!(
            !direct_result,
            "Direct check with i64 ms should fail (bloom filter has i32 days)"
        );

        // Test 2: ArrowSbbf also fails (KNOWN LIMITATION - not yet implemented)
        let arrow_sbbf = ArrowSbbf::new(&bloom_filter, field.data_type());
        let arrow_result = arrow_sbbf.check(&test_value_ms);
        println!(
            "Date64 (coerce_types=true): ArrowSbbf check with i64 ms = {}",
            arrow_result
        );
        assert!(
            !arrow_result,
            "ArrowSbbf cannot handle Date64 coercion (documented limitation)"
        );

        // Workaround: Manually convert to days before checking
        let days = (test_value_ms / MS_PER_DAY) as i32;
        let workaround_result = bloom_filter.check(&days);
        println!(
            "Date64 (coerce_types=true): Manual conversion to i32 days = {}",
            workaround_result
        );
        assert!(
            workaround_result,
            "Workaround: checking with i32 days should succeed"
        );
    }
}

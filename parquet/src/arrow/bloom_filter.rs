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
//! For example, Arrow small integer types are coerced when writing to Parquet:
//! - `Int8` → `INT32` (i8 → i32)
//! - `Int16` → `INT32` (i16 → i32)
//! - `UInt8` → `INT32` (u8 → u32 → i32)
//! - `UInt16` → `INT32` (u16 → u32 → i32)
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
//!
//! // Create ArrowSbbf with the logical Arrow type
//! let arrow_sbbf = ArrowSbbf::new(&sbbf, &DataType::Int8);
//!
//! // Check with i8 value - automatically coerced to i32
//! let value: i8 = 42;
//! if arrow_sbbf.check(&value) {
//!     println!("Value might be present");
//! }
//! ```
//!
//! # Date64 with Type Coercion
//!
//! `Date64` requires special handling because it can be stored as either INT32 or INT64
//! depending on [`WriterProperties::set_coerce_types`].
//!
//! When `coerce_types=true`, Date64 is stored as Date32 (INT32, days since epoch).
//! Users must convert their Date64 values before checking:
//!
//! ```ignore
//! use arrow_array::temporal_conversions::MILLISECONDS_IN_DAY;
//! use arrow_array::Date64Array;
//! use arrow_cast::cast;
//! use arrow_schema::DataType;
//!
//! let date64_value = 864_000_000_i64; // milliseconds
//!
//! // Check the column's physical type to determine conversion
//! let column_chunk = row_group_reader.metadata().column(0);
//! match column_chunk.column_type() {
//!     ParquetType::INT32 => {
//!         // Date64 was coerced to Date32 - convert milliseconds to days
//!         let date32_value = (date64_value / MILLISECONDS_IN_DAY) as i32;
//!         let arrow_sbbf = ArrowSbbf::new(&sbbf, &DataType::Date32);
//!         arrow_sbbf.check(&date32_value)
//!     }
//!     ParquetType::INT64 => {
//!         // Date64 stored as-is - check directly
//!         let arrow_sbbf = ArrowSbbf::new(&sbbf, &DataType::Date64);
//!         arrow_sbbf.check(&date64_value)
//!     }
//!     _ => unreachable!()
//! }
//! ```
//!
//! Alternatively, use [`arrow_cast::cast`] for the conversion:
//! ```ignore
//! let date64_array = Date64Array::from(vec![date64_value]);
//! let date32_array = cast(&date64_array, &DataType::Date32)?;
//! let date32_value = date32_array.as_primitive::<Date32Type>().value(0);
//! ```
//!
//! [`WriterProperties::set_coerce_types`]: crate::file::properties::WriterPropertiesBuilder::set_coerce_types
//! [`arrow_cast::cast`]: https://docs.rs/arrow-cast/latest/arrow_cast/cast/fn.cast.html

use crate::bloom_filter::Sbbf;
use crate::data_type::AsBytes;
use arrow_schema::DataType as ArrowType;

/// Wraps an [`Sbbf`] and provides automatic type coercion based on Arrow schema.
#[derive(Debug, Clone)]
pub struct ArrowSbbf<'a> {
    sbbf: &'a Sbbf,
    arrow_type: &'a ArrowType,
}

impl<'a> ArrowSbbf<'a> {
    /// Create a new Arrow-aware bloom filter wrapper
    ///
    /// # Arguments
    /// * `sbbf` - Parquet bloom filter for the column
    /// * `arrow_type` - Arrow data type for the column
    pub fn new(sbbf: &'a Sbbf, arrow_type: &'a ArrowType) -> Self {
        Self { sbbf, arrow_type }
    }

    /// Check if a value might be present in the bloom filter
    /// Automatically handles type coercion based on the Arrow data type.
    ///
    /// Returns `true` if the value might be present (may have false positives),
    /// or `false` if the value is definitely not present.
    pub fn check<T: AsBytes + ?Sized>(&self, value: &T) -> bool {
        match self.arrow_type {
            ArrowType::Int8 => {
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
            ArrowType::Int16 => {
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
            ArrowType::UInt8 => {
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
            ArrowType::UInt16 => {
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
            ArrowType::Decimal32(precision, _)
            | ArrowType::Decimal64(precision, _)
            | ArrowType::Decimal128(precision, _)
            | ArrowType::Decimal256(precision, _)
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
            ArrowType::Decimal64(precision, _)
            | ArrowType::Decimal128(precision, _)
            | ArrowType::Decimal256(precision, _)
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
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;
    use tempfile::tempfile;

    /// Helper function to build a bloom filter for testing
    ///
    /// Writes the given array to a Parquet file with bloom filters enabled,
    /// then reads it back and returns the bloom filter for the first column.
    fn build_sbbf(array: ArrayRef, field: Field) -> Sbbf {
        build_sbbf_with_props(
            array,
            field,
            WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .build(),
        )
    }

    /// Helper function to build a bloom filter with custom writer properties
    fn build_sbbf_with_props(array: ArrayRef, field: Field, props: WriterProperties) -> Sbbf {
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write with custom properties
        let mut file = tempfile().unwrap();
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

        // Get bloom filter
        let row_group_reader = reader.get_row_group(0).unwrap();
        row_group_reader
            .get_column_bloom_filter(0)
            .expect("Bloom filter should exist")
            .clone()
    }

    #[test]
    fn test_check_int8() {
        let test_value = 3_i8;
        let array = Arc::new(Int8Array::from(vec![1_i8, 2, test_value, 4, 5]));
        let field = Field::new("col", ArrowType::Int8, false);
        let sbbf = build_sbbf(array, field.clone());

        // Check without coercion fails
        assert!(!sbbf.check(&test_value));

        // Arrow Int8 -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Int8);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_int16() {
        let test_value = 300_i16;
        let array = Arc::new(Int16Array::from(vec![100_i16, 200, test_value, 400, 500]));
        let field = Field::new("col", ArrowType::Int16, false);
        let sbbf = build_sbbf(array, field.clone());

        // Check without coercion fails
        assert!(!sbbf.check(&test_value));

        // Arrow Int16 -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Int16);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_uint8() {
        let test_value = 30_u8;
        let array = Arc::new(UInt8Array::from(vec![10_u8, 20, test_value, 40, 50]));
        let field = Field::new("col", ArrowType::UInt8, false);
        let sbbf = build_sbbf(array, field.clone());

        // Check without coercion fails
        assert!(!sbbf.check(&test_value));

        // Arrow UInt8 -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::UInt8);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_uint16() {
        let test_value = 3000_u16;
        let array = Arc::new(UInt16Array::from(vec![
            1000_u16, 2000, test_value, 4000, 5000,
        ]));
        let field = Field::new("col", ArrowType::UInt16, false);
        let sbbf = build_sbbf(array, field.clone());

        // Check without coercion fails
        assert!(!sbbf.check(&test_value));

        // Arrow UInt16 -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::UInt16);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_uint32() {
        let test_value = 3_000_000_000_u32; // > i32::MAX
        let array = Arc::new(UInt32Array::from(vec![
            100_u32,
            test_value,
            4_000_000_000_u32,
        ]));
        let field = Field::new("col", ArrowType::UInt32, false);
        let sbbf = build_sbbf(array, field.clone());

        // No coercion necessary
        assert!(sbbf.check(&test_value));

        // Arrow UInt32 -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::UInt32);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_uint64() {
        let test_value = 10_000_000_000_000_000_000_u64; // > i64::MAX
        let array = Arc::new(UInt64Array::from(vec![
            100_u64,
            test_value,
            15_000_000_000_000_000_000_u64,
        ]));
        let field = Field::new("col", ArrowType::UInt64, false);
        let sbbf = build_sbbf(array, field.clone());

        // No coercion necessary
        assert!(sbbf.check(&test_value));

        // Arrow UInt64 -> Parquet INT64
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::UInt64);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_decimal128_small() {
        let test_value = 20075_i128;
        let array = Decimal128Array::from(vec![10050_i128, test_value, 30099_i128])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let field = Field::new("col", ArrowType::Decimal128(5, 2), false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // Check without coercion fails
        let test_bytes = test_value.to_le_bytes();
        let direct_result = sbbf.check(&test_bytes[..]);
        assert!(!direct_result);

        // Arrow Decimal128(5, 2) -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Decimal128(5, 2));
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        assert!(arrow_result);
    }

    #[test]
    fn test_check_decimal128_medium() {
        let test_value = 9876543210987_i128;
        let array = Decimal128Array::from(vec![1234567890123_i128, test_value, 5555555555555_i128])
            .with_precision_and_scale(15, 2)
            .unwrap();
        let field = Field::new("col", ArrowType::Decimal128(15, 2), false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // Check without coercion fails
        let test_bytes = test_value.to_le_bytes();
        let direct_result = sbbf.check(&test_bytes[..]);
        assert!(!direct_result);

        // Arrow Decimal128(15, 2) -> Parquet INT64
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Decimal128(15, 2));
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        assert!(arrow_result);
    }

    #[test]
    fn test_check_decimal32() {
        let test_value = 20075_i32;
        let array = Decimal32Array::from(vec![10050_i32, test_value, 30099_i32])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let field = Field::new("col", ArrowType::Decimal32(5, 2), false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // No coercion necessary
        assert!(sbbf.check(&test_value));

        // Arrow Decimal32(5, 2) -> Parquet INT32
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Decimal32(5, 2));
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_float16() {
        use half::f16;
        let test_value = f16::from_f32(2.5);
        let array = Float16Array::from(vec![f16::from_f32(1.5), test_value, f16::from_f32(3.5)]);
        let field = Field::new("col", ArrowType::Float16, false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // No coercion necessary
        let test_bytes = test_value.to_le_bytes();
        let direct_result = sbbf.check(&test_bytes[..]);
        assert!(direct_result);

        // Arrow Float16 -> Parquet FIXED_LEN_BYTE_ARRAY
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Float16);
        let arrow_result = arrow_sbbf.check(&test_bytes[..]);
        assert!(arrow_result);
    }

    #[test]
    fn test_check_date64_default() {
        let test_value = 2_000_000_000_i64;
        let array = Date64Array::from(vec![1_000_000_000_i64, test_value, 3_000_000_000_i64]);
        let field = Field::new("col", ArrowType::Date64, false);
        let sbbf = build_sbbf(Arc::new(array), field.clone());

        // No coercion necessary
        assert!(sbbf.check(&test_value));

        // Arrow Date64 -> Parquet INT64
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Date64);
        assert!(arrow_sbbf.check(&test_value));
    }

    #[test]
    fn test_check_date64_with_coerce_types() {
        use arrow_array::temporal_conversions::MILLISECONDS_IN_DAY;

        let test_value_ms = 10 * MILLISECONDS_IN_DAY; // 10 days in milliseconds
        let array = Date64Array::from(vec![
            5 * MILLISECONDS_IN_DAY,
            test_value_ms,
            15 * MILLISECONDS_IN_DAY,
        ]);
        let field = Field::new("col", ArrowType::Date64, false);

        // Write with coerce_types enabled
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_coerce_types(true)
            .build();
        let sbbf = build_sbbf_with_props(Arc::new(array), field.clone(), props);

        // Check without coercion fails
        assert!(!sbbf.check(&test_value_ms));

        // Arrow Date64 -> Arrow Date32 -> Parquet INT32
        let date32_value = (test_value_ms / MILLISECONDS_IN_DAY) as i32;
        let arrow_sbbf = ArrowSbbf::new(&sbbf, &ArrowType::Date32); // Note: Date32, not Date64
        assert!(arrow_sbbf.check(&date32_value));
    }
}

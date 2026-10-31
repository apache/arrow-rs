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

//! Check Arrow arrays against Parquet bloom filters

use arrow_array::builder::BooleanBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, RunEndIndexType};
use arrow_array::{Array, BooleanArray, RunArray, downcast_run_array};
use arrow_buffer::{ArrowNativeType, BooleanBuffer};
use arrow_schema::DataType;

use crate::arrow::arrow_writer::{
    FixedLenByteArrayVisitor, PrimitiveVisitor, visit_fixed_len_byte_array, visit_i32, visit_i64,
};
use crate::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use crate::bloom_filter::Sbbf;
use crate::data_type::AsBytes;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescriptor;

/// A Parquet bloom filter ([`Sbbf`]) that can be checked with Arrow arrays
///
/// [`ArrowWriter`] converts some Arrow types to a different Parquet physical
/// type before writing them, for example `Int8` and `UInt16` are stored as
/// `INT32`, and the bloom filter is built from the converted values. Checking
/// such a filter with [`Sbbf::check`] and the original Arrow value hashes
/// different bytes, and so reports values that are present as absent.
///
/// [`ArrowSbbf::check`] applies the same conversion as [`ArrowWriter`]
/// before checking the filter.
///
/// These are the Arrow types whose values [`ArrowWriter`] stores differently:
///
/// | Arrow type | Parquet physical type | Stored value |
/// |---|---|---|
/// | `Int8`, `Int16` | `INT32` | Sign-extended |
/// | `UInt8`, `UInt16` | `INT32` | Zero-extended |
/// | `UInt32` | `INT32` | Same bits, so values above `i32::MAX` are negative |
/// | `UInt64` | `INT64` | Same bits, so values above `i64::MAX` are negative |
/// | `Date64` | `INT32` with [`coerce_types`], otherwise `INT64` | With [`coerce_types`], days rounded towards zero; otherwise milliseconds |
/// | `Decimal32` to `Decimal256` of precision 1 to 9 | `INT32` | Unscaled value, with wider types truncated to 32 bits |
/// | `Decimal64` to `Decimal256` of precision 10 to 18 | `INT64` | Unscaled value, with wider types truncated to 64 bits |
/// | `Decimal128`, `Decimal256` of precision 19 or more | `FIXED_LEN_BYTE_ARRAY` | Unscaled value as big-endian two's complement, truncated to the length for the precision |
/// | `Float16` | `FIXED_LEN_BYTE_ARRAY` of length 2 | Little-endian bytes |
/// | `Interval(YearMonth)`, `Interval(DayTime)` | `FIXED_LEN_BYTE_ARRAY` of length 12 | Months, days and milliseconds as little-endian 32-bit integers; the fields the Arrow type does not have are zero |
///
/// Other types are stored as they are. A dictionary or run-end encoded array is
/// stored as its values.
///
/// ```
/// # use std::sync::Arc;
/// # use bytes::Bytes;
/// # use arrow_array::{ArrayRef, Int8Array, RecordBatch};
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::arrow::bloom_filter::ArrowSbbf;
/// # use parquet::file::properties::{ReaderProperties, WriterProperties};
/// # use parquet::file::reader::{FileReader, SerializedFileReader};
/// # use parquet::file::serialized_reader::ReadOptionsBuilder;
/// let array: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
/// let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
///
/// let props = WriterProperties::builder()
///     .set_bloom_filter_enabled(true)
///     .build();
/// let mut buffer = Vec::new();
/// let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
/// writer.write(&batch).unwrap();
/// writer.close().unwrap();
///
/// let options = ReadOptionsBuilder::new()
///     .with_reader_properties(
///         ReaderProperties::builder()
///             .set_read_bloom_filter(true)
///             .build(),
///     )
///     .build();
/// let reader = SerializedFileReader::new_with_options(Bytes::from(buffer), options).unwrap();
/// let row_group = reader.get_row_group(0).unwrap();
/// let sbbf = row_group.get_column_bloom_filter(0).unwrap();
/// let column = reader.metadata().row_group(0).column(0).column_descr();
///
/// // Checking the Arrow value directly hashes a single byte, not the INT32 that was written
/// assert!(!sbbf.check(&1_i8));
///
/// let found = ArrowSbbf::new(sbbf, column)
///     .check(&Int8Array::from(vec![1, 3, 4]))
///     .unwrap();
/// assert!(found.value(0));
/// assert!(found.value(1));
/// assert!(!found.value(2));
/// ```
///
/// [`ArrowWriter`]: crate::arrow::ArrowWriter
/// [`coerce_types`]: crate::file::properties::WriterPropertiesBuilder::set_coerce_types
#[derive(Debug, Clone, Copy)]
pub struct ArrowSbbf<'a> {
    sbbf: &'a Sbbf,
    physical_type: PhysicalType,
    /// The length of the column's values if it is a `FIXED_LEN_BYTE_ARRAY` column,
    /// otherwise 0
    type_length: usize,
    /// The scale of the column's values if it is a decimal column
    decimal_scale: Option<i32>,
}

impl<'a> ArrowSbbf<'a> {
    /// Create a new [`ArrowSbbf`] for the bloom filter of the column described by `column`
    pub fn new(sbbf: &'a Sbbf, column: &ColumnDescriptor) -> Self {
        let decimal_scale = match column.logical_type_ref() {
            Some(LogicalType::Decimal(decimal)) => Some(decimal.scale),
            _ if column.converted_type() == ConvertedType::DECIMAL => Some(column.type_scale()),
            _ => None,
        };
        Self {
            sbbf,
            physical_type: column.physical_type(),
            // The schema checks that the length of a FIXED_LEN_BYTE_ARRAY column
            // is positive, and other columns have no length
            type_length: column.type_length().try_into().unwrap_or(0),
            decimal_scale,
        }
    }

    /// Check each value of `array` against the bloom filter
    ///
    /// Returns `true` if the value may be present, and `false` if it is
    /// definitely absent. Nulls in `array` are null in the result.
    ///
    /// Decimal values of any precision can be checked against a decimal column
    /// of the same scale. They are converted to the column's physical type and,
    /// for a `FIXED_LEN_BYTE_ARRAY` column, to the column's length, truncating
    /// a value that is too large in the same way as [`ArrowWriter`].
    ///
    /// Values of time types (timestamps, times, durations and `Date64`) are
    /// checked in the unit of `array`'s data type, which must be the unit the
    /// column was written in, as they are not converted between units. For
    /// example, a `Timestamp(Millisecond, _)` value of `1_000` is not found by
    /// checking the same instant as a `Timestamp(Microsecond, _)` value of
    /// `1_000_000`.
    ///
    /// Returns an error if [`ArrowWriter`] cannot write `array`'s data type to a
    /// column of this physical type, if only one of `array` and the column is
    /// a decimal or their scales differ, or if `array` is a `FixedSizeBinary`
    /// array whose values have a different length than the column's.
    ///
    /// [`ArrowWriter`]: crate::arrow::ArrowWriter
    pub fn check(&self, array: &dyn Array) -> Result<BooleanArray> {
        if let Some(dictionary) = array.as_any_dictionary_opt() {
            let values = self.check(dictionary.values().as_ref())?;
            let found = arrow_select::take::take(&values, dictionary.keys(), None)?;
            return Ok(found.as_boolean().clone());
        }
        match array.data_type() {
            DataType::Null => return Ok(BooleanArray::new_null(array.len())),
            DataType::RunEndEncoded(_, _) => {
                return downcast_run_array!(
                    array => self.check_run_array(array),
                    _ => unreachable!("RunEndEncoded array is not a RunArray"),
                );
            }
            _ => {}
        }
        self.validate_decimal(array.data_type())?;

        let sbbf = self.sbbf;
        let primitive = CheckPrimitive {
            sbbf,
            physical_type: self.physical_type,
        };
        let found = match (self.physical_type, array.data_type()) {
            (PhysicalType::INT32, _) => visit_i32(array, primitive)?,
            (PhysicalType::INT64, _) => visit_i64(array, primitive)?,
            (PhysicalType::FLOAT, DataType::Float32) => {
                primitive.slice(array.as_primitive::<Float32Type>().values())?
            }
            (PhysicalType::DOUBLE, DataType::Float64) => {
                primitive.slice(array.as_primitive::<Float64Type>().values())?
            }
            (PhysicalType::BOOLEAN, DataType::Boolean) => array
                .as_boolean()
                .values()
                .iter()
                .map(|v| sbbf.check(&v))
                .collect(),
            (PhysicalType::BYTE_ARRAY, DataType::Utf8) => {
                check_bytes(sbbf, array.as_string::<i32>())
            }
            (PhysicalType::BYTE_ARRAY, DataType::LargeUtf8) => {
                check_bytes(sbbf, array.as_string::<i64>())
            }
            (PhysicalType::BYTE_ARRAY, DataType::Utf8View) => {
                check_bytes(sbbf, array.as_string_view())
            }
            (PhysicalType::BYTE_ARRAY, DataType::Binary) => {
                check_bytes(sbbf, array.as_binary::<i32>())
            }
            (PhysicalType::BYTE_ARRAY, DataType::LargeBinary) => {
                check_bytes(sbbf, array.as_binary::<i64>())
            }
            (PhysicalType::BYTE_ARRAY, DataType::BinaryView) => {
                check_bytes(sbbf, array.as_binary_view())
            }
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, _) => {
                let visitor = CheckFixedLenByteArray {
                    sbbf,
                    array,
                    type_length: self.type_length,
                };
                visit_fixed_len_byte_array(array, visitor)?
            }
            (physical_type, data_type) => return Err(unsupported(physical_type, data_type)),
        };
        Ok(BooleanArray::new(found, array.logical_nulls()))
    }

    /// Checks the values of a run-end encoded array, and repeats each result for its run
    fn check_run_array<R: RunEndIndexType>(&self, array: &RunArray<R>) -> Result<BooleanArray> {
        let found = self.check(array.values_slice().as_ref())?;
        let mut builder = BooleanBuilder::with_capacity(array.len());
        let mut run_start = 0;
        for (i, run_end) in array.run_ends().sliced_values().enumerate() {
            let run_end = run_end.as_usize();
            match found.is_valid(i) {
                true => builder.append_n(run_end - run_start, found.value(i)),
                false => builder.append_nulls(run_end - run_start),
            }
            run_start = run_end;
        }
        Ok(builder.finish())
    }

    /// Returns an error unless both or neither of the column and `data_type` are
    /// decimals, and decimals have the same scale
    ///
    /// Decimal values are stored as unscaled integers, so a value of another
    /// scale is a different integer, and so is the integer value of an array
    /// that is not a decimal. Converting it to the column's scale could lose
    /// digits, so it is not supported.
    fn validate_decimal(&self, data_type: &DataType) -> Result<()> {
        let scale = match data_type {
            DataType::Decimal32(_, scale)
            | DataType::Decimal64(_, scale)
            | DataType::Decimal128(_, scale)
            | DataType::Decimal256(_, scale) => Some(i32::from(*scale)),
            _ => None,
        };
        match (self.decimal_scale, scale) {
            (None, None) => Ok(()),
            (Some(column_scale), Some(scale)) if column_scale == scale => Ok(()),
            (Some(column_scale), Some(scale)) => Err(ParquetError::General(format!(
                "Cannot check a bloom filter of a decimal column with scale {column_scale} with decimal values with scale {scale}"
            ))),
            (Some(column_scale), None) => Err(ParquetError::General(format!(
                "Cannot check a bloom filter of a decimal column with scale {column_scale} with {data_type} values"
            ))),
            (None, Some(_)) => Err(ParquetError::General(
                "Cannot check a bloom filter of a column that is not a decimal with decimal values"
                    .to_string(),
            )),
        }
    }
}

/// Returns the error for checking a bloom filter of `physical_type` with values
/// of `data_type`, which [`ArrowWriter`](crate::arrow::ArrowWriter) cannot write
/// to such a column
fn unsupported(physical_type: PhysicalType, data_type: &DataType) -> ParquetError {
    ParquetError::General(format!(
        "Cannot check a bloom filter of physical type {physical_type} with {data_type} values"
    ))
}

/// Checks the `INT32`, `INT64`, `FLOAT` or `DOUBLE` values passed to it against a bloom filter
struct CheckPrimitive<'a> {
    sbbf: &'a Sbbf,
    physical_type: PhysicalType,
}

impl<T: AsBytes> PrimitiveVisitor<T> for CheckPrimitive<'_> {
    type Output = BooleanBuffer;

    fn slice(self, values: &[T]) -> Result<BooleanBuffer> {
        Ok(values.iter().map(|v| self.sbbf.check(v)).collect())
    }

    fn map<S: Copy>(self, values: &[S], convert: impl Fn(S) -> T) -> Result<BooleanBuffer> {
        Ok(values
            .iter()
            .map(|v| self.sbbf.check(&convert(*v)))
            .collect())
    }

    fn unsupported(&self, data_type: &DataType, _error: ParquetError) -> ParquetError {
        unsupported(self.physical_type, data_type)
    }
}

/// Checks the `FIXED_LEN_BYTE_ARRAY` values of `array` passed to it against a bloom filter
struct CheckFixedLenByteArray<'a> {
    sbbf: &'a Sbbf,
    array: &'a dyn Array,
    type_length: usize,
}

impl FixedLenByteArrayVisitor for CheckFixedLenByteArray<'_> {
    type Output = BooleanBuffer;

    fn decimal_length(&self, _precision: u8, _native_length: usize) -> usize {
        // Decimals of any precision are converted to the column's length
        self.type_length
    }

    fn values(self, length: usize, fill: impl Fn(usize, &mut [u8])) -> Result<BooleanBuffer> {
        if length != self.type_length {
            return Err(ParquetError::General(format!(
                "Cannot check a bloom filter of a FIXED_LEN_BYTE_ARRAY column of length {} with {} values, which have length {length}",
                self.type_length,
                self.array.data_type()
            )));
        }
        let mut value = vec![0; length];
        Ok((0..self.array.len())
            .map(|i| {
                value.fill(0);
                fill(i, &mut value);
                self.sbbf.check(value.as_slice())
            })
            .collect())
    }

    fn unsupported(&self, data_type: &DataType, _error: ParquetError) -> ParquetError {
        unsupported(PhysicalType::FIXED_LEN_BYTE_ARRAY, data_type)
    }
}

fn check_bytes<'a, T: AsRef<[u8]> + ?Sized + 'a>(
    sbbf: &Sbbf,
    values: impl arrow_array::ArrayAccessor<Item = &'a T>,
) -> BooleanBuffer {
    (0..values.len())
        .map(|i| sbbf.check(values.value(i).as_ref()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::file::properties::{ReaderProperties, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use crate::schema::types::{ColumnDescPtr, ColumnPath, Type};
    use arrow_array::types::{Int16Type, Int32Type};
    use arrow_array::*;
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, i256};
    use bytes::Bytes;
    use std::sync::Arc;

    /// Writes `batch` with bloom filters, and returns the filter and descriptor of each column
    fn write_and_read_bloom_filters(
        batch: &RecordBatch,
        coerce_types: bool,
    ) -> Vec<(Sbbf, ColumnDescPtr)> {
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_coerce_types(coerce_types)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buffer), options).unwrap();
        let row_group = reader.get_row_group(0).unwrap();
        (0..batch.num_columns())
            .map(|i| {
                let sbbf = row_group.get_column_bloom_filter(i).unwrap().clone();
                let column = reader.metadata().row_group(0).column(i).column_descr_ptr();
                (sbbf, column)
            })
            .collect()
    }

    /// Writes `array` with a bloom filter and returns the filter and column descriptor
    fn write_and_read_bloom_filter(array: ArrayRef, coerce_types: bool) -> (Sbbf, ColumnDescPtr) {
        let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
        write_and_read_bloom_filters(&batch, coerce_types)
            .pop()
            .unwrap()
    }

    /// Writes `written` and checks that every value is found and no value of `absent` is
    fn assert_round_trip(written: ArrayRef, absent: ArrayRef, coerce_types: bool) {
        let (sbbf, column) = write_and_read_bloom_filter(written.clone(), coerce_types);
        let sbbf = ArrowSbbf::new(&sbbf, &column);

        let found = sbbf.check(written.as_ref()).unwrap();
        assert!(
            found.iter().all(|v| v == Some(true)),
            "{written:?}: {found:?}"
        );

        let found = sbbf.check(absent.as_ref()).unwrap();
        assert!(
            found.iter().all(|v| v == Some(false)),
            "{absent:?}: {found:?}"
        );
    }

    #[test]
    fn time_units_are_not_converted() {
        // The same instant in a different unit hashes a different value
        assert_round_trip(
            Arc::new(TimestampMillisecondArray::from(vec![1_000, 2_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000])),
            false,
        );
    }

    #[test]
    fn narrow_integers() {
        assert_round_trip(
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int8Array::from(vec![-2, 2, 100])),
            false,
        );
        assert_round_trip(
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int16Array::from(vec![-2, 2, 1000])),
            false,
        );
        assert_round_trip(
            Arc::new(UInt8Array::from(vec![0, 1, 128, u8::MAX])),
            Arc::new(UInt8Array::from(vec![2, 127, 200])),
            false,
        );
        assert_round_trip(
            Arc::new(UInt16Array::from(vec![0, 1, 32768, u16::MAX])),
            Arc::new(UInt16Array::from(vec![2, 32767, 60000])),
            false,
        );
    }

    #[test]
    fn unsigned_integers_above_signed_max() {
        assert_round_trip(
            Arc::new(UInt32Array::from(vec![0, i32::MAX as u32 + 1, u32::MAX])),
            Arc::new(UInt32Array::from(vec![1, u32::MAX - 1])),
            false,
        );
        assert_round_trip(
            Arc::new(UInt64Array::from(vec![0, i64::MAX as u64 + 1, u64::MAX])),
            Arc::new(UInt64Array::from(vec![1, u64::MAX - 1])),
            false,
        );
    }

    #[test]
    fn date64() {
        let day = 86_400_000;
        let written: ArrayRef = Arc::new(Date64Array::from(vec![0, day, 365 * day]));
        let absent: ArrayRef = Arc::new(Date64Array::from(vec![2 * day, 366 * day]));
        // Stored as INT64 milliseconds
        assert_round_trip(written.clone(), absent.clone(), false);
        // Stored as INT32 days
        assert_round_trip(written, absent, true);
    }

    #[test]
    fn date64_stored_as_days() {
        // Milliseconds are divided by a day and rounded towards zero
        let written: ArrayRef = Arc::new(Date64Array::from(vec![5 * 86_400_000, -1]));
        let (sbbf, column) = write_and_read_bloom_filter(written.clone(), true);
        assert!(sbbf.check(&5_i32));
        assert!(sbbf.check(&0_i32));

        let found = ArrowSbbf::new(&sbbf, &column)
            .check(written.as_ref())
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true]));
    }

    #[test]
    fn decimals() {
        // Stored as INT32, INT64 and FIXED_LEN_BYTE_ARRAY depending on precision
        for precision in [5, 15, 30] {
            let decimal = |values: Vec<i128>| -> ArrayRef {
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, 2)
                        .unwrap(),
                )
            };
            assert_round_trip(
                decimal(vec![-12345, 0, 12345]),
                decimal(vec![-1, 1, 54321]),
                false,
            );
        }
    }

    fn decimal128(values: Vec<i128>, precision: u8, scale: i8) -> Decimal128Array {
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap()
    }

    #[test]
    fn decimal_precision_differs_from_column() {
        // Precision 30 is stored as a 13 byte FIXED_LEN_BYTE_ARRAY
        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(decimal128(vec![12345, -12345], 30, 2)), false);
        assert_eq!(column.type_length(), 13);
        let sbbf = ArrowSbbf::new(&sbbf, &column);

        // The values are converted to the column's width, whatever the array's precision
        for precision in [20, 30, 38] {
            let found = sbbf
                .check(&decimal128(vec![12345, -12345, 54321], precision, 2))
                .unwrap();
            assert_eq!(
                found,
                BooleanArray::from(vec![true, true, false]),
                "precision {precision}"
            );
        }
        let found = sbbf
            .check(
                &Decimal256Array::from(vec![i256::from(12345), i256::from(-12345)])
                    .with_precision_and_scale(76, 2)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true]));

        // Narrower decimal types are sign-extended to 13 bytes
        let found = sbbf
            .check(
                &Decimal32Array::from(vec![12345, -12345, 54321])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true, false]));
        let found = sbbf
            .check(
                &Decimal64Array::from(vec![12345, -12345, 54321])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true, false]));

        // Values that need more than 13 bytes are truncated to their low 13
        // bytes, like ArrowWriter does. These are the bytes of 12345 and -12345
        let wide = 1_i128 << 104;
        let found = sbbf
            .check(&decimal128(vec![12345 + wide, -12345 - wide], 38, 2))
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true]));
    }

    #[test]
    fn decimal_column_longer_than_i256() {
        // ArrowWriter stores decimals in at most 32 bytes, but another writer can
        // use a longer FIXED_LEN_BYTE_ARRAY, to which values are sign-extended
        let column = Type::primitive_type_builder("c", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(33)
            .with_logical_type(Some(LogicalType::decimal(2, 10)))
            .with_precision(10)
            .with_scale(2)
            .build()
            .unwrap();
        let column = ColumnDescriptor::new(Arc::new(column), 1, 0, ColumnPath::from("c"));
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        for (sign, value) in [(0, 12345), (0xFF, -12345)] {
            let mut bytes = vec![sign];
            bytes.extend_from_slice(&i256::from(value).to_be_bytes());
            sbbf.insert(bytes.as_slice());
        }

        let found = ArrowSbbf::new(&sbbf, &column)
            .check(&decimal128(vec![12345, -12345, 54321], 10, 2))
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true, false]));
    }

    #[test]
    fn narrower_decimal_type_than_column() {
        // Precision 12 is stored as INT64
        let written = Decimal64Array::from(vec![12345, -12345])
            .with_precision_and_scale(12, 2)
            .unwrap();
        let (sbbf, column) = write_and_read_bloom_filter(Arc::new(written), false);
        assert_eq!(column.physical_type(), PhysicalType::INT64);

        let found = ArrowSbbf::new(&sbbf, &column)
            .check(
                &Decimal32Array::from(vec![12345, -12345, 54321])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true, false]));
    }

    #[test]
    fn decimal_too_large_for_precision() {
        // Arrow does not validate precision, so these values need more bytes than
        // the column has. They are truncated in the same way as ArrowWriter
        // truncates them, for INT32, INT64 and FIXED_LEN_BYTE_ARRAY columns.
        for precision in [5, 15, 20] {
            let written: ArrayRef = Arc::new(decimal128(
                vec![i128::MAX / 3, -(i128::MAX / 3)],
                precision,
                2,
            ));
            let (sbbf, column) = write_and_read_bloom_filter(written.clone(), false);
            let found = ArrowSbbf::new(&sbbf, &column)
                .check(written.as_ref())
                .unwrap();
            assert_eq!(
                found,
                BooleanArray::from(vec![true, true]),
                "precision {precision}"
            );
        }
    }

    #[test]
    fn decimal_scale_differs_from_column() {
        // Stored as INT32, INT64 and FIXED_LEN_BYTE_ARRAY depending on precision
        for precision in [5, 15, 30] {
            let (sbbf, column) =
                write_and_read_bloom_filter(Arc::new(decimal128(vec![12345], precision, 2)), false);
            let sbbf = ArrowSbbf::new(&sbbf, &column);
            let found = sbbf.check(&decimal128(vec![12345], precision, 2)).unwrap();
            assert_eq!(found, BooleanArray::from(vec![true]));

            // 123.45 at scale 3 is 123450, so checking 12345 would check 12.345
            let err = sbbf
                .check(&decimal128(vec![123450], precision, 3))
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "Parquet error: Cannot check a bloom filter of a decimal column with scale 2 with decimal values with scale 3",
                "precision {precision}"
            );
        }

        // Nor can decimal values be checked against a column that is not a decimal
        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(Int32Array::from(vec![12345])), false);
        let err = ArrowSbbf::new(&sbbf, &column)
            .check(&decimal128(vec![12345], 5, 0))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of a column that is not a decimal with decimal values"
        );
    }

    #[test]
    fn decimal_column_requires_decimal_values() {
        // Stored as INT32, INT64 and FIXED_LEN_BYTE_ARRAY depending on precision
        for precision in [5, 15, 30] {
            let (sbbf, column) =
                write_and_read_bloom_filter(Arc::new(decimal128(vec![500], precision, 2)), false);
            let sbbf = ArrowSbbf::new(&sbbf, &column);

            // 500 at scale 2 is 5.00, so checking the integer 500 or 5 would be ambiguous
            let err = sbbf.check(&Int32Array::from(vec![500])).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Parquet error: Cannot check a bloom filter of a decimal column with scale 2 with Int32 values",
                "precision {precision}"
            );
        }

        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(decimal128(vec![500], 30, 2)), false);
        let err = ArrowSbbf::new(&sbbf, &column)
            .check(&FixedSizeBinaryArray::try_from_iter([[0; 13]].into_iter()).unwrap())
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of a decimal column with scale 2 with FixedSizeBinary(13) values"
        );
    }

    #[test]
    fn fixed_size_binary_length_differs_from_column() {
        let written = FixedSizeBinaryArray::try_from_iter([b"abcd", b"efgh"].into_iter()).unwrap();
        let (sbbf, column) = write_and_read_bloom_filter(Arc::new(written.clone()), false);
        let sbbf = ArrowSbbf::new(&sbbf, &column);

        let found = sbbf.check(&written).unwrap();
        assert_eq!(found, BooleanArray::from(vec![true, true]));

        let err = sbbf
            .check(&FixedSizeBinaryArray::try_from_iter([b"abc"].into_iter()).unwrap())
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of a FIXED_LEN_BYTE_ARRAY column of length 4 with FixedSizeBinary(3) values, which have length 3"
        );
    }

    #[test]
    fn float16() {
        assert_round_trip(
            Arc::new(Float16Array::from(vec![
                half::f16::from_f32(-1.5),
                half::f16::from_f32(0.0),
                half::f16::from_f32(2.25),
            ])),
            Arc::new(Float16Array::from(vec![
                half::f16::from_f32(1.5),
                half::f16::from_f32(3.0),
            ])),
            false,
        );
    }

    #[test]
    fn intervals() {
        assert_round_trip(
            Arc::new(IntervalYearMonthArray::from(vec![-1, 0, 13])),
            Arc::new(IntervalYearMonthArray::from(vec![1, 12])),
            false,
        );
        assert_round_trip(
            Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTime::new(1, 2),
                IntervalDayTime::new(-3, 4),
            ])),
            Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTime::new(2, 1),
                IntervalDayTime::new(0, 0),
            ])),
            false,
        );
    }

    #[test]
    fn strings() {
        assert_round_trip(
            Arc::new(StringArray::from(vec!["a", "bc", ""])),
            Arc::new(StringArray::from(vec!["b", "abc"])),
            false,
        );
    }

    #[test]
    fn dictionary_and_nulls() {
        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(Int8Array::from(vec![1, 2, 3])), false);
        let sbbf = ArrowSbbf::new(&sbbf, &column);

        let found = sbbf
            .check(&Int8Array::from(vec![Some(1), None, Some(4)]))
            .unwrap();
        assert_eq!(
            found,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );

        let dictionary = DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(1), None, Some(0), Some(1)]),
            Arc::new(Int8Array::from(vec![4, 3])),
        );
        let found = sbbf.check(&dictionary).unwrap();
        assert_eq!(
            found,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)])
        );

        let found = sbbf.check(&NullArray::new(2)).unwrap();
        assert_eq!(found, BooleanArray::new_null(2));
    }

    #[test]
    fn null_array() {
        let columns: [ArrayRef; 2] = [
            Arc::new(Float64Array::from(vec![1.5])),
            Arc::new(StringArray::from(vec!["a"])),
        ];
        for written in columns {
            let (sbbf, column) = write_and_read_bloom_filter(written, false);
            let found = ArrowSbbf::new(&sbbf, &column)
                .check(&NullArray::new(2))
                .unwrap();
            assert_eq!(found, BooleanArray::from(vec![None, None]));
        }
    }

    #[test]
    fn run_end_encoded() {
        let run_ends = Int32Array::from(vec![2, 3, 5, 6]);
        let values = Int16Array::from(vec![1, -2, 7, 3]);
        let written = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let (sbbf, column) = write_and_read_bloom_filter(Arc::new(written), false);
        let sbbf = ArrowSbbf::new(&sbbf, &column);

        let values = Int16Array::from(vec![Some(-2), Some(4), None, Some(1)]);
        let array =
            RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 3, 4, 6]), &values).unwrap();
        let found = sbbf.check(&array).unwrap();
        assert_eq!(
            found,
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(true),
                Some(true)
            ])
        );

        let found = sbbf.check(&array.slice(2, 3)).unwrap();
        assert_eq!(
            found,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn unsupported_type() {
        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(Int8Array::from(vec![1, 2, 3])), false);
        let err = ArrowSbbf::new(&sbbf, &column)
            .check(&StringArray::from(vec!["1"]))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of physical type INT32 with Utf8 values"
        );

        let written = FixedSizeBinaryArray::try_from_iter([b"abcd"].into_iter()).unwrap();
        let (sbbf, column) = write_and_read_bloom_filter(Arc::new(written), false);
        let sbbf = ArrowSbbf::new(&sbbf, &column);
        let err = sbbf.check(&Int32Array::from(vec![1])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of physical type FIXED_LEN_BYTE_ARRAY with Int32 values"
        );
        let err = sbbf
            .check(&IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(1, 2, 3),
            ]))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Cannot check a bloom filter of physical type FIXED_LEN_BYTE_ARRAY with Interval(MonthDayNano) values"
        );
    }

    /// Returns `values` with a null inserted at index 2
    fn with_null<T>(values: [T; 4]) -> Vec<Option<T>> {
        let mut values: Vec<_> = values.into_iter().map(Some).collect();
        values.insert(2, None);
        values
    }

    /// Returns a column of each Arrow type that [`ArrowWriter`] can write to a
    /// primitive column, with edge values and a null
    fn writer_type_columns() -> Vec<(&'static str, ArrayRef)> {
        let day = 86_400_000;
        let f16 = half::f16::from_f32;
        let fsb = |values: Vec<Option<&[u8; 4]>>| -> ArrayRef {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 4)
                    .unwrap(),
            )
        };
        // The last value is too large for the precision, so the writer truncates
        // it. No value is 0, which a wrong truncation could produce.
        let decimal32 = |precision| -> ArrayRef {
            let values = with_null([-12345, 1, 12345, i32::MAX / 3]);
            Arc::new(
                Decimal32Array::from(values)
                    .with_precision_and_scale(precision, 2)
                    .unwrap(),
            )
        };
        let decimal64 = |precision| -> ArrayRef {
            let values = with_null([-12345, 1, 12345, i64::MAX / 3]);
            Arc::new(
                Decimal64Array::from(values)
                    .with_precision_and_scale(precision, 2)
                    .unwrap(),
            )
        };
        let decimal128 = |precision| -> ArrayRef {
            let values = with_null([-12345, 1, 12345, i128::MAX / 3]);
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, 2)
                    .unwrap(),
            )
        };
        let decimal256 = |precision| -> ArrayRef {
            let large = i256::MAX.wrapping_div(i256::from(3));
            let values = with_null([i256::from(-12345), i256::from(1), i256::from(12345), large]);
            Arc::new(
                Decimal256Array::from(values)
                    .with_precision_and_scale(precision, 2)
                    .unwrap(),
            )
        };
        let strings = with_null(["", "a", "longer than twelve bytes", "é"]);
        let binaries: Vec<_> = strings.iter().map(|s| s.map(str::as_bytes)).collect();
        let interval = IntervalDayTime::new;
        let day_times = with_null([
            interval(1, 2),
            interval(-3, 4),
            interval(0, 0),
            interval(i32::MAX, i32::MIN),
        ]);
        let int16s = Int16Array::from(vec![
            Some(9),
            Some(-7),
            None,
            Some(3),
            Some(i16::MIN),
            Some(5),
        ]);
        let keys = Int32Array::from(with_null([0, 1, 0, 1]));
        let ree_int8 = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3, 5]),
            &Int8Array::from(vec![Some(-3), None, Some(i8::MAX)]),
        );
        let ree_utf8 = RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3, 5]),
            &StringArray::from(vec![Some("a"), None, Some("b")]),
        );

        let time32s = with_null([0, 1, 3600, 86399]);
        let time64s = with_null([0, 1, 3600, 86399]);
        let i64s = with_null([-1, 0, 1, i64::MAX]);
        let float16s = with_null([f16(-1.5), f16(0.0), f16(-0.0), half::f16::NAN]);
        // Stored as days, rounded towards zero, with coerce_types
        let date64s = with_null([-1, 0, 5 * day + 1, -5 * day - 1]);
        let timestamp_ms = TimestampMillisecondArray::from(i64s.clone()).with_timezone("+01:00");

        vec![
            (
                "int8",
                Arc::new(Int8Array::from(with_null([i8::MIN, -1, 1, i8::MAX]))),
            ),
            (
                "int16",
                Arc::new(Int16Array::from(with_null([i16::MIN, -1, 1, i16::MAX]))),
            ),
            (
                "int32",
                Arc::new(Int32Array::from(with_null([i32::MIN, -1, 1, i32::MAX]))),
            ),
            (
                "int64",
                Arc::new(Int64Array::from(with_null([i64::MIN, -1, 1, i64::MAX]))),
            ),
            (
                "uint8",
                Arc::new(UInt8Array::from(with_null([0, 1, 128, u8::MAX]))),
            ),
            (
                "uint16",
                Arc::new(UInt16Array::from(with_null([0, 1, 32768, u16::MAX]))),
            ),
            (
                "uint32",
                Arc::new(UInt32Array::from(with_null([0, 1, 1 << 31, u32::MAX]))),
            ),
            (
                "uint64",
                Arc::new(UInt64Array::from(with_null([0, 1, 1 << 63, u64::MAX]))),
            ),
            ("float16", Arc::new(Float16Array::from(float16s))),
            (
                "float32",
                Arc::new(Float32Array::from(with_null([-1.5, 0.0, -0.0, f32::NAN]))),
            ),
            (
                "float64",
                Arc::new(Float64Array::from(with_null([
                    -1.5,
                    0.0,
                    f64::INFINITY,
                    f64::NAN,
                ]))),
            ),
            (
                "bool",
                Arc::new(BooleanArray::from(with_null([true, false, true, false]))),
            ),
            (
                "date32",
                Arc::new(Date32Array::from(with_null([-1, 0, 1, 20000]))),
            ),
            ("date64", Arc::new(Date64Array::from(date64s))),
            (
                "time32_s",
                Arc::new(Time32SecondArray::from(time32s.clone())),
            ),
            ("time32_ms", Arc::new(Time32MillisecondArray::from(time32s))),
            (
                "time64_us",
                Arc::new(Time64MicrosecondArray::from(time64s.clone())),
            ),
            ("time64_ns", Arc::new(Time64NanosecondArray::from(time64s))),
            (
                "timestamp_s",
                Arc::new(TimestampSecondArray::from(i64s.clone())),
            ),
            ("timestamp_ms", Arc::new(timestamp_ms)),
            (
                "timestamp_us",
                Arc::new(TimestampMicrosecondArray::from(i64s.clone())),
            ),
            (
                "timestamp_ns",
                Arc::new(TimestampNanosecondArray::from(i64s.clone())),
            ),
            (
                "duration_s",
                Arc::new(DurationSecondArray::from(i64s.clone())),
            ),
            (
                "duration_ms",
                Arc::new(DurationMillisecondArray::from(i64s.clone())),
            ),
            (
                "duration_us",
                Arc::new(DurationMicrosecondArray::from(i64s.clone())),
            ),
            ("duration_ns", Arc::new(DurationNanosecondArray::from(i64s))),
            // Stored as INT32, INT64 or FIXED_LEN_BYTE_ARRAY depending on precision
            ("decimal32_5", decimal32(5)),
            ("decimal32_9", decimal32(9)),
            ("decimal64_5", decimal64(5)),
            ("decimal64_18", decimal64(18)),
            ("decimal128_5", decimal128(5)),
            ("decimal128_15", decimal128(15)),
            ("decimal128_20", decimal128(20)),
            ("decimal128_38", decimal128(38)),
            ("decimal256_5", decimal256(5)),
            ("decimal256_15", decimal256(15)),
            ("decimal256_30", decimal256(30)),
            ("decimal256_76", decimal256(76)),
            (
                "interval_year_month",
                Arc::new(IntervalYearMonthArray::from(with_null([
                    -1,
                    0,
                    13,
                    i32::MAX,
                ]))),
            ),
            (
                "interval_day_time",
                Arc::new(IntervalDayTimeArray::from(day_times)),
            ),
            (
                "fixed_size_binary",
                fsb(with_null([
                    b"abcd",
                    b"\0\0\0\0",
                    b"\xff\xfe\xfd\xfc",
                    b"abce",
                ])),
            ),
            ("utf8", Arc::new(StringArray::from(strings.clone()))),
            (
                "large_utf8",
                Arc::new(LargeStringArray::from(strings.clone())),
            ),
            ("utf8_view", Arc::new(StringViewArray::from(strings))),
            ("binary", Arc::new(BinaryArray::from(binaries.clone()))),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from(binaries.clone())),
            ),
            ("binary_view", Arc::new(BinaryViewArray::from(binaries))),
            ("sliced_int16", Arc::new(int16s.slice(1, 5))),
            // Dictionary and run-end encoded arrays are written as their values
            (
                "dictionary_int16",
                Arc::new(DictionaryArray::new(
                    keys.clone(),
                    Arc::new(Int16Array::from(vec![-1, i16::MAX])),
                )),
            ),
            (
                "dictionary_decimal",
                Arc::new(DictionaryArray::new(
                    keys.clone(),
                    decimal128(30).slice(0, 2),
                )),
            ),
            (
                "dictionary_fixed_size_binary",
                Arc::new(DictionaryArray::new(
                    keys.clone(),
                    fsb(vec![Some(b"abcd"), Some(b"wxyz")]),
                )),
            ),
            (
                "dictionary_utf8",
                Arc::new(DictionaryArray::new(
                    keys,
                    Arc::new(StringArray::from(vec!["a", "longer than twelve bytes"])),
                )),
            ),
            ("ree_int8", Arc::new(ree_int8.unwrap())),
            ("ree_utf8", Arc::new(ree_utf8.unwrap())),
        ]
    }

    /// Writes a column of each Arrow type that [`ArrowWriter`] can write to a
    /// primitive column, and checks that [`ArrowSbbf`] finds every written value
    #[test]
    fn every_writer_type() {
        let columns = writer_type_columns();
        let batch = RecordBatch::try_from_iter(columns.clone()).unwrap();
        for coerce_types in [false, true] {
            let filters = write_and_read_bloom_filters(&batch, coerce_types);
            assert_eq!(filters.len(), columns.len());
            for ((name, array), (sbbf, column)) in columns.iter().zip(&filters) {
                let context = format!("{name} with coerce_types {coerce_types}");
                let found = ArrowSbbf::new(sbbf, column)
                    .check(array.as_ref())
                    .unwrap_or_else(|e| panic!("{context}: {e}"));
                assert_eq!(found.len(), array.len(), "{context}");
                // Every value is found, and nulls are null
                let nulls = array.logical_nulls();
                for (i, found) in found.iter().enumerate() {
                    let valid = nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i));
                    assert_eq!(found, valid.then_some(true), "{context}: index {i}");
                }
            }
        }
    }
}

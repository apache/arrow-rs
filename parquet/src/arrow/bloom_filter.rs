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

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, BooleanArray};
use arrow_buffer::BooleanBuffer;
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
#[derive(Debug, Clone, Copy)]
pub struct ArrowSbbf<'a> {
    sbbf: &'a Sbbf,
    physical_type: PhysicalType,
    /// The length of the column's values if it is a `FIXED_LEN_BYTE_ARRAY` column
    type_length: i32,
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
            type_length: column.type_length(),
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
    /// for a `FIXED_LEN_BYTE_ARRAY` column, to the column's length; a value too
    /// large for that length is reported as absent.
    ///
    /// Returns an error if [`ArrowWriter`] cannot write `array`'s data type to a
    /// column of this physical type, if `array` is a decimal array and the
    /// column is not a decimal column of the same scale, or if `array` is a
    /// `FixedSizeBinary` array whose values have a different length than the
    /// column's.
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
            DataType::Decimal32(_, scale)
            | DataType::Decimal64(_, scale)
            | DataType::Decimal128(_, scale)
            | DataType::Decimal256(_, scale) => self.validate_decimal_scale(*scale)?,
            _ => {}
        }

        let sbbf = self.sbbf;
        let found = match (self.physical_type, array.data_type()) {
            (PhysicalType::INT32, _) => visit_i32(array, CheckPrimitive { sbbf })?,
            (PhysicalType::INT64, _) => visit_i64(array, CheckPrimitive { sbbf })?,
            (PhysicalType::FLOAT, DataType::Float32) => {
                CheckPrimitive { sbbf }.slice(array.as_primitive::<Float32Type>().values())
            }
            (PhysicalType::DOUBLE, DataType::Float64) => {
                CheckPrimitive { sbbf }.slice(array.as_primitive::<Float64Type>().values())
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
                    // Never negative for a FIXED_LEN_BYTE_ARRAY column, which the schema checks
                    type_length: self.type_length as usize,
                };
                visit_fixed_len_byte_array(array, visitor)??
            }
            (physical_type, data_type) => {
                return Err(ParquetError::NYI(format!(
                    "Checking a bloom filter of a {physical_type} column with {data_type} values is not supported"
                )));
            }
        };
        Ok(BooleanArray::new(found, array.logical_nulls()))
    }

    /// Returns an error unless the column is a decimal column of `scale`
    ///
    /// Decimal values are stored as unscaled integers, so a value of another
    /// scale is a different integer. Converting it to the column's scale could
    /// lose digits, so it is not supported.
    fn validate_decimal_scale(&self, scale: i8) -> Result<()> {
        match self.decimal_scale {
            Some(column_scale) if column_scale == i32::from(scale) => Ok(()),
            Some(column_scale) => Err(ParquetError::General(format!(
                "Cannot check a bloom filter of a decimal column with scale {column_scale} with decimal values with scale {scale}"
            ))),
            None => Err(ParquetError::General(
                "Cannot check a bloom filter of a column that is not a decimal with decimal values"
                    .to_string(),
            )),
        }
    }
}

/// Checks the `INT32`, `INT64`, `FLOAT` or `DOUBLE` values passed to it against a bloom filter
struct CheckPrimitive<'a> {
    sbbf: &'a Sbbf,
}

impl<T: AsBytes> PrimitiveVisitor<T> for CheckPrimitive<'_> {
    type Output = BooleanBuffer;

    fn slice(self, values: &[T]) -> BooleanBuffer {
        values.iter().map(|v| self.sbbf.check(v)).collect()
    }

    fn map<S: Copy>(self, values: &[S], convert: impl Fn(S) -> T) -> BooleanBuffer {
        values
            .iter()
            .map(|v| self.sbbf.check(&convert(*v)))
            .collect()
    }
}

/// Checks the `FIXED_LEN_BYTE_ARRAY` values of `array` passed to it against a bloom filter
struct CheckFixedLenByteArray<'a> {
    sbbf: &'a Sbbf,
    array: &'a dyn Array,
    type_length: usize,
}

impl FixedLenByteArrayVisitor for CheckFixedLenByteArray<'_> {
    type Output = Result<BooleanBuffer>;

    fn decimal_length(&self, _precision: u8) -> usize {
        // Decimals of any precision are converted to the column's length
        self.type_length
    }

    fn values(
        self,
        length: usize,
        fill: impl Fn(usize, &mut [u8]) -> bool,
    ) -> Result<BooleanBuffer> {
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
                // A value that does not fit in the column cannot be in it
                fill(i, &mut value) && self.sbbf.check(value.as_slice())
            })
            .collect())
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
    use crate::schema::types::ColumnDescPtr;
    use arrow_array::types::Int32Type;
    use arrow_array::*;
    use arrow_buffer::{IntervalDayTime, i256};
    use bytes::Bytes;
    use std::sync::Arc;

    /// Writes `array` with a bloom filter and returns the filter and column descriptor
    fn write_and_read_bloom_filter(array: ArrayRef, coerce_types: bool) -> (Sbbf, ColumnDescPtr) {
        let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_coerce_types(coerce_types)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buffer), options).unwrap();
        let sbbf = reader
            .get_row_group(0)
            .unwrap()
            .get_column_bloom_filter(0)
            .unwrap()
            .clone();
        let column = reader.metadata().row_group(0).column(0).column_descr_ptr();
        (sbbf, column)
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

        // Values that need more than 13 bytes cannot be in the column. Their low
        // 13 bytes are the bytes of 12345 and -12345, so truncating them would
        // report them as present
        let wide = 1_i128 << 104;
        let found = sbbf
            .check(&decimal128(vec![12345 + wide, -12345 - wide], 38, 2))
            .unwrap();
        assert_eq!(found, BooleanArray::from(vec![false, false]));
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
    fn unsupported_type() {
        let (sbbf, column) =
            write_and_read_bloom_filter(Arc::new(Int8Array::from(vec![1, 2, 3])), false);
        let err = ArrowSbbf::new(&sbbf, &column)
            .check(&StringArray::from(vec!["1"]))
            .unwrap_err();
        assert_eq!(err.to_string(), "Parquet error: Cannot coerce Utf8 to I32");
    }
}

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

//! Insert and check whole Arrow arrays in an [`Sbbf`].
//!
//! Each value is hashed as the Parquet physical value that
//! [`ArrowWriter`](crate::arrow::ArrowWriter) writes for it with the default
//! options. Thus a filter filled from Arrow arrays gives the same answers as a
//! bloom filter that `ArrowWriter` writes for the same values, and the reverse.

use super::{SEED, Sbbf, hash_as_bytes};
use crate::arrow::decimal_length_from_precision;
use crate::errors::{ParquetError, Result};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrowPrimitiveType};
use arrow_buffer::{BooleanBuffer, i256};
use arrow_schema::{DataType, TimeUnit};

/// Receives a function that returns the hash of slot `i` of an array.
trait HashVisitor {
    type Output;
    fn visit(self, len: usize, hash: impl Fn(usize) -> u64) -> Self::Output;
}

/// Calls `visitor` with the hash function for the type of `array`.
fn visit_hashes<V: HashVisitor>(array: &dyn Array, visitor: V) -> Result<V::Output> {
    use DataType::*;

    fn prim<T: ArrowPrimitiveType, V: HashVisitor>(
        array: &dyn Array,
        visitor: V,
        hash: impl Fn(T::Native) -> u64,
    ) -> V::Output {
        let values = array.as_primitive::<T>().values();
        visitor.visit(values.len(), |i| hash(values[i]))
    }
    fn bytes<'a, V: HashVisitor>(
        len: usize,
        visitor: V,
        value: impl Fn(usize) -> &'a [u8],
    ) -> V::Output {
        visitor.visit(len, |i| hash_as_bytes(value(i)))
    }
    // Decimals are written as INT32, INT64 or FIXED_LEN_BYTE_ARRAY (big endian,
    // truncated to the minimum length for the precision).
    fn decimal<T: ArrowPrimitiveType, V: HashVisitor>(
        array: &dyn Array,
        visitor: V,
        precision: u8,
        to_i256: impl Fn(T::Native) -> i256,
    ) -> V::Output {
        let values = array.as_primitive::<T>().values();
        if (1..=9).contains(&precision) {
            visitor.visit(values.len(), |i| {
                hash_i32(to_i256(values[i]).as_i128() as i32)
            })
        } else if precision <= 18 {
            visitor.visit(values.len(), |i| {
                hash_i64(to_i256(values[i]).as_i128() as i64)
            })
        } else {
            let len = decimal_length_from_precision(precision);
            visitor.visit(values.len(), |i| {
                let be = to_i256(values[i]).to_be_bytes();
                hash_as_bytes(&be[32 - len..])
            })
        }
    }

    let output = match array.data_type() {
        // INT32
        Int8 => prim::<Int8Type, _>(array, visitor, |v| hash_i32(v as i32)),
        Int16 => prim::<Int16Type, _>(array, visitor, |v| hash_i32(v as i32)),
        Int32 => prim::<Int32Type, _>(array, visitor, hash_i32),
        UInt8 => prim::<UInt8Type, _>(array, visitor, |v| hash_i32(v as i32)),
        UInt16 => prim::<UInt16Type, _>(array, visitor, |v| hash_i32(v as i32)),
        UInt32 => prim::<UInt32Type, _>(array, visitor, |v| hash_i32(v as i32)),
        Date32 => prim::<Date32Type, _>(array, visitor, hash_i32),
        Time32(TimeUnit::Second) => prim::<Time32SecondType, _>(array, visitor, hash_i32),
        Time32(TimeUnit::Millisecond) => prim::<Time32MillisecondType, _>(array, visitor, hash_i32),
        // INT64
        Int64 => prim::<Int64Type, _>(array, visitor, hash_i64),
        UInt64 => prim::<UInt64Type, _>(array, visitor, |v| hash_i64(v as i64)),
        Date64 => prim::<Date64Type, _>(array, visitor, hash_i64),
        Time64(TimeUnit::Microsecond) => prim::<Time64MicrosecondType, _>(array, visitor, hash_i64),
        Time64(TimeUnit::Nanosecond) => prim::<Time64NanosecondType, _>(array, visitor, hash_i64),
        Timestamp(TimeUnit::Second, _) => prim::<TimestampSecondType, _>(array, visitor, hash_i64),
        Timestamp(TimeUnit::Millisecond, _) => {
            prim::<TimestampMillisecondType, _>(array, visitor, hash_i64)
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            prim::<TimestampMicrosecondType, _>(array, visitor, hash_i64)
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            prim::<TimestampNanosecondType, _>(array, visitor, hash_i64)
        }
        Duration(TimeUnit::Second) => prim::<DurationSecondType, _>(array, visitor, hash_i64),
        Duration(TimeUnit::Millisecond) => {
            prim::<DurationMillisecondType, _>(array, visitor, hash_i64)
        }
        Duration(TimeUnit::Microsecond) => {
            prim::<DurationMicrosecondType, _>(array, visitor, hash_i64)
        }
        Duration(TimeUnit::Nanosecond) => {
            prim::<DurationNanosecondType, _>(array, visitor, hash_i64)
        }
        // Decimals
        Decimal32(p, _) => decimal::<Decimal32Type, _>(array, visitor, *p, i256::from),
        Decimal64(p, _) => decimal::<Decimal64Type, _>(array, visitor, *p, i256::from),
        Decimal128(p, _) => decimal::<Decimal128Type, _>(array, visitor, *p, i256::from_i128),
        Decimal256(p, _) => decimal::<Decimal256Type, _>(array, visitor, *p, |v| v),
        // BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY
        Utf8 => {
            let a = array.as_string::<i32>();
            bytes(a.len(), visitor, |i| a.value(i).as_bytes())
        }
        LargeUtf8 => {
            let a = array.as_string::<i64>();
            bytes(a.len(), visitor, |i| a.value(i).as_bytes())
        }
        Utf8View => {
            let a = array.as_string_view();
            bytes(a.len(), visitor, |i| a.value(i).as_bytes())
        }
        Binary => {
            let a = array.as_binary::<i32>();
            bytes(a.len(), visitor, |i| a.value(i))
        }
        LargeBinary => {
            let a = array.as_binary::<i64>();
            bytes(a.len(), visitor, |i| a.value(i))
        }
        BinaryView => {
            let a = array.as_binary_view();
            bytes(a.len(), visitor, |i| a.value(i))
        }
        FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            bytes(a.len(), visitor, |i| a.value(i))
        }
        other => {
            return Err(ParquetError::NYI(format!(
                "Sbbf: Arrow data type {other} is not supported"
            )));
        }
    };
    Ok(output)
}

impl Sbbf {
    /// Return `true` if [`Self::hash_array`], [`Self::insert_array`] and
    /// [`Self::check_array`] support arrays of `data_type`.
    ///
    /// Supported: integers, dates, times, timestamps, durations, decimals,
    /// strings and binaries (including the view and fixed size variants).
    /// Not supported: floats (`-0.0`/`0.0` and `NaN` have more than one bit
    /// pattern), booleans, dictionaries and nested types.
    pub fn supports_data_type(data_type: &DataType) -> bool {
        use DataType::*;
        matches!(
            data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Date32
                | Date64
                | Time32(TimeUnit::Second | TimeUnit::Millisecond)
                | Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
                | Timestamp(_, _)
                | Duration(_)
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Utf8
                | LargeUtf8
                | Utf8View
                | Binary
                | LargeBinary
                | BinaryView
                | FixedSizeBinary(_)
        )
    }

    /// Write [`Self::hash_value`] of each slot of `array` into `hashes`.
    ///
    /// Each value is hashed as the Parquet physical value that
    /// [`ArrowWriter`](crate::arrow::ArrowWriter) writes for it (for example an
    /// `Int8` is hashed as an `INT32`). `hashes` is cleared first and has
    /// `array.len()` items after the call. The hashes of null slots are not
    /// defined; the caller must skip them.
    ///
    /// Returns an error if [`Self::supports_data_type`] is `false` for the type
    /// of `array`.
    pub fn hash_array(array: &dyn Array, hashes: &mut Vec<u64>) -> Result<()> {
        struct Extend<'a>(&'a mut Vec<u64>);
        impl HashVisitor for Extend<'_> {
            type Output = ();
            fn visit(self, len: usize, hash: impl Fn(usize) -> u64) {
                self.0.extend((0..len).map(hash));
            }
        }
        hashes.clear();
        hashes.reserve(array.len());
        visit_hashes(array, Extend(hashes))
    }

    /// Insert each non-null value of `array` into the filter.
    ///
    /// Equal to [`Self::insert`] of the Parquet physical value of each slot (see
    /// [`Self::hash_array`]). Returns an error, and does not change the filter, if
    /// [`Self::supports_data_type`] is `false` for the type of `array`.
    pub fn insert_array(&mut self, array: &dyn Array) -> Result<()> {
        struct Insert<'a> {
            filter: &'a mut Sbbf,
            nulls: Option<arrow_buffer::NullBuffer>,
        }
        impl HashVisitor for Insert<'_> {
            type Output = ();
            fn visit(self, len: usize, hash: impl Fn(usize) -> u64) {
                match self.nulls {
                    Some(nulls) => nulls
                        .valid_indices()
                        .for_each(|i| self.filter.insert_hash(hash(i))),
                    None => (0..len).for_each(|i| self.filter.insert_hash(hash(i))),
                }
            }
        }
        let nulls = array.logical_nulls();
        visit_hashes(
            array,
            Insert {
                filter: self,
                nulls,
            },
        )
    }

    /// Check each slot of `array`.
    ///
    /// Bit `i` of the result is `true` if the value at slot `i` is probably in the
    /// filter, and `false` if it is definitely not in the filter or the slot is
    /// null. Equal to [`Self::check`] of the Parquet physical value of each slot
    /// (see [`Self::hash_array`]), but faster: the type dispatch is done once for
    /// the array, and the block test has no data-dependent branches.
    ///
    /// Returns an error if [`Self::supports_data_type`] is `false` for the type of
    /// `array`.
    pub fn check_array(&self, array: &dyn Array) -> Result<BooleanBuffer> {
        struct Check<'a>(&'a Sbbf);
        impl HashVisitor for Check<'_> {
            type Output = BooleanBuffer;
            fn visit(self, len: usize, hash: impl Fn(usize) -> u64) -> BooleanBuffer {
                BooleanBuffer::collect_bool(len, |i| self.0.check_hash_branch_free(hash(i)))
            }
        }
        let result = visit_hashes(array, Check(self))?;
        Ok(match array.logical_nulls() {
            Some(nulls) => &result & nulls.inner(),
            None => result,
        })
    }
}

// xxHash64 (seed 0) of the 4 or 8 little-endian bytes of a value, written out so
// the compiler can inline it into the loops above. Equal to `hash_as_bytes` of
// the value (see the tests).
const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline(always)]
fn xxh64_avalanche(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^ (h >> 32)
}

#[inline(always)]
fn hash_i64(v: i64) -> u64 {
    let k1 = (v as u64).wrapping_mul(P2).rotate_left(31).wrapping_mul(P1);
    let h = SEED.wrapping_add(P5).wrapping_add(8) ^ k1;
    xxh64_avalanche(h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4))
}

#[inline(always)]
fn hash_i32(v: i32) -> u64 {
    let h = SEED.wrapping_add(P5).wrapping_add(4) ^ (v as u32 as u64).wrapping_mul(P1);
    xxh64_avalanche(h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::file::properties::{ReaderProperties, WriterProperties};
    use crate::file::reader::FileReader;
    use crate::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
    use arrow_array::*;
    use arrow_schema::{Field, Schema};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn inlined_xxh64_matches_twox() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for v in [0i64, 1, -1, 42, i64::MIN, i64::MAX] {
            assert_eq!(hash_i64(v), hash_as_bytes(&v));
            assert_eq!(hash_i32(v as i32), hash_as_bytes(&(v as i32)));
        }
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            assert_eq!(hash_i64(x as i64), hash_as_bytes(&(x as i64)));
            assert_eq!(hash_i32(x as i32), hash_as_bytes(&(x as i32)));
        }
    }

    fn test_arrays() -> Vec<ArrayRef> {
        vec![
            Arc::new(Int8Array::from(vec![Some(-3), None, Some(7)])),
            Arc::new(Int16Array::from(vec![Some(-300), None, Some(7)])),
            Arc::new(Int32Array::from(vec![Some(-3), None, Some(i32::MAX)])),
            Arc::new(Int64Array::from(vec![Some(-3), None, Some(i64::MIN)])),
            Arc::new(UInt8Array::from(vec![Some(200), None, Some(7)])),
            Arc::new(UInt16Array::from(vec![Some(60000), None, Some(7)])),
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None, Some(7)])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(7)])),
            Arc::new(Date32Array::from(vec![Some(19000), None, Some(-1)])),
            Arc::new(Date64Array::from(vec![Some(86_400_000), None, Some(0)])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(5),
                None,
                Some(-5),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None, Some(-1)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(123_456_789_012), None, Some(-1)])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(i128::MAX / 3), None, Some(-1)])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("long string value"),
            ])),
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("b")])),
            Arc::new(StringViewArray::from(vec![
                Some("short"),
                None,
                Some("a string longer than twelve bytes"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&b"x"[..]),
                None,
                Some(&b"yz"[..]),
            ])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(b"abcd"), None, Some(b"efgh")].into_iter(),
                    4,
                )
                .unwrap(),
            ),
        ]
    }

    /// The batch API finds every non-null value and rejects null slots.
    #[test]
    fn insert_array_then_check_array() {
        for array in test_arrays() {
            assert!(Sbbf::supports_data_type(array.data_type()));
            let mut filter = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
            filter.insert_array(array.as_ref()).unwrap();
            let result = filter.check_array(array.as_ref()).unwrap();
            for i in 0..array.len() {
                assert_eq!(result.value(i), array.is_valid(i), "{array:?} slot {i}");
            }
            // Values that were not inserted are (with this seed and size) absent
            let empty = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
            let result = empty.check_array(array.as_ref()).unwrap();
            assert_eq!(result.count_set_bits(), 0);
        }
    }

    /// `hash_array` matches `hash_value` of the parquet physical value.
    #[test]
    fn hash_array_matches_scalar_hash() {
        let mut hashes = vec![];
        Sbbf::hash_array(&Int8Array::from(vec![-3i8]), &mut hashes).unwrap();
        assert_eq!(hashes, vec![Sbbf::hash_value(&-3i32)]);
        Sbbf::hash_array(&UInt64Array::from(vec![u64::MAX]), &mut hashes).unwrap();
        assert_eq!(hashes, vec![Sbbf::hash_value(&-1i64)]);
        Sbbf::hash_array(&StringArray::from(vec!["abc"]), &mut hashes).unwrap();
        assert_eq!(hashes, vec![Sbbf::hash_value("abc")]);
        let dec = Decimal128Array::from(vec![12345])
            .with_precision_and_scale(5, 2)
            .unwrap();
        Sbbf::hash_array(&dec, &mut hashes).unwrap();
        assert_eq!(hashes, vec![Sbbf::hash_value(&12345i32)]);
    }

    #[test]
    fn unsupported_types_return_error() {
        let floats = Float64Array::from(vec![1.0]);
        assert!(!Sbbf::supports_data_type(floats.data_type()));
        let mut filter = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        assert!(filter.insert_array(&floats).is_err());
        assert!(filter.check_array(&floats).is_err());
        assert!(Sbbf::hash_array(&floats, &mut vec![]).is_err());
    }

    /// A filter that `ArrowWriter` writes for a column finds every value of the
    /// column with `check_array`, and a filter from `insert_array` equals the
    /// written filter.
    #[test]
    fn compatible_with_arrow_writer() {
        for array in test_arrays() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "c",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
            let props = WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_max_ndv(100)
                .set_bloom_filter_fpp(0.01)
                .build();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let bytes = Bytes::from(buf);
            // The file reads back as the same Arrow type
            let read = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(read.column(0).data_type(), array.data_type());

            let reader = SerializedFileReader::new_with_options(
                bytes,
                ReadOptionsBuilder::new()
                    .with_reader_properties(
                        ReaderProperties::builder()
                            .set_read_bloom_filter(true)
                            .build(),
                    )
                    .build(),
            )
            .unwrap();
            let row_group = reader.get_row_group(0).unwrap();
            let written = row_group.get_column_bloom_filter(0).unwrap();
            let result = written.check_array(array.as_ref()).unwrap();
            for i in 0..array.len() {
                assert_eq!(result.value(i), array.is_valid(i), "{array:?} slot {i}");
            }

            let mut ours = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
            ours.insert_array(array.as_ref()).unwrap();
            // The writer folds its filter to the target fpp
            ours.fold_to_target_fpp(0.01);
            let (mut a, mut b) = (vec![], vec![]);
            ours.write_bitset(&mut a).unwrap();
            written.write_bitset(&mut b).unwrap();
            assert_eq!(a, b, "{array:?}");
        }
    }
}

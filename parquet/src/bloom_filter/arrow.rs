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
//! [`ArrowWriter`](crate::arrow::ArrowWriter) writes for it. The conversion from
//! Arrow to Parquet values is the writer's own ([`visit_i32`], [`visit_i64`] and
//! [`visit_fixed_len_byte_array`]), so a filter filled from Arrow arrays gives the
//! same answers as a bloom filter that `ArrowWriter` writes for the same values,
//! and the reverse.

use std::cell::RefCell;

use super::{SEED, Sbbf, hash_as_bytes};
use crate::arrow::arrow_to_parquet_type;
use crate::arrow::arrow_writer::{
    FixedLenByteArrayVisitor, PrimitiveVisitor, visit_fixed_len_byte_array, visit_i32, visit_i64,
};
use crate::basic::Type as PhysicalType;
use crate::errors::{ParquetError, Result};
use crate::schema::types::Type;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, ArrayAccessor};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field, TimeUnit};

/// Receives a function that returns the hash of slot `i` of an array
pub(crate) trait HashVisitor {
    type Output;
    fn visit(self, len: usize, hash: impl Fn(usize) -> u64) -> Self::Output;
}

/// The Parquet column whose values an Arrow array is hashed as
#[derive(Debug, Clone, Copy)]
pub(crate) struct HashTarget {
    pub(crate) physical_type: PhysicalType,
    /// The length of the values of a `FIXED_LEN_BYTE_ARRAY` column
    pub(crate) type_length: usize,
}

impl HashTarget {
    /// The column that `ArrowWriter` writes for `data_type` with the default options
    fn for_data_type(data_type: &DataType) -> Result<Self> {
        match arrow_to_parquet_type(&Field::new("c", data_type.clone(), true), false)? {
            Type::PrimitiveType {
                physical_type,
                type_length,
                ..
            } => Ok(Self {
                physical_type,
                type_length: type_length.max(0) as usize,
            }),
            Type::GroupType { .. } => Err(unsupported(data_type)),
        }
    }
}

fn unsupported(data_type: &DataType) -> ParquetError {
    ParquetError::NYI(format!(
        "Sbbf: Arrow data type {data_type} is not supported"
    ))
}

/// Calls `visitor` with the hash of the value that `ArrowWriter` writes for each
/// slot of `array` in a column of `target`
pub(crate) fn visit_hashes<V: HashVisitor>(
    array: &dyn Array,
    target: HashTarget,
    visitor: V,
) -> Result<V::Output> {
    match (target.physical_type, array.data_type()) {
        (PhysicalType::INT32, _) => visit_i32(array, HashPrimitive(visitor)),
        (PhysicalType::INT64, _) => visit_i64(array, HashPrimitive(visitor)),
        (PhysicalType::FLOAT, DataType::Float32) => {
            Ok(HashPrimitive(visitor).slice(array.as_primitive::<Float32Type>().values()))
        }
        (PhysicalType::DOUBLE, DataType::Float64) => {
            Ok(HashPrimitive(visitor).slice(array.as_primitive::<Float64Type>().values()))
        }
        (PhysicalType::BOOLEAN, DataType::Boolean) => {
            let array = array.as_boolean();
            Ok(visitor.visit(array.len(), |i| hash_as_bytes(&array.value(i))))
        }
        (PhysicalType::BYTE_ARRAY, DataType::Utf8) => {
            Ok(hash_bytes(array.as_string::<i32>(), visitor))
        }
        (PhysicalType::BYTE_ARRAY, DataType::LargeUtf8) => {
            Ok(hash_bytes(array.as_string::<i64>(), visitor))
        }
        (PhysicalType::BYTE_ARRAY, DataType::Utf8View) => {
            Ok(hash_bytes(array.as_string_view(), visitor))
        }
        (PhysicalType::BYTE_ARRAY, DataType::Binary) => {
            Ok(hash_bytes(array.as_binary::<i32>(), visitor))
        }
        (PhysicalType::BYTE_ARRAY, DataType::LargeBinary) => {
            Ok(hash_bytes(array.as_binary::<i64>(), visitor))
        }
        (PhysicalType::BYTE_ARRAY, DataType::BinaryView) => {
            Ok(hash_bytes(array.as_binary_view(), visitor))
        }
        (PhysicalType::FIXED_LEN_BYTE_ARRAY, _) => visit_fixed_len_byte_array(
            array,
            HashFixedLenByteArray {
                visitor,
                len: array.len(),
                type_length: target.type_length,
                data_type: array.data_type(),
            },
        )?,
        (physical_type, data_type) => Err(ParquetError::NYI(format!(
            "Sbbf: hashing {data_type} values for a {physical_type} column is not supported"
        ))),
    }
}

fn hash_bytes<'a, T, V>(array: impl ArrayAccessor<Item = &'a T>, visitor: V) -> V::Output
where
    T: AsRef<[u8]> + ?Sized + 'a,
    V: HashVisitor,
{
    visitor.visit(array.len(), |i| hash_as_bytes(array.value(i).as_ref()))
}

/// A value of an `INT32`, `INT64`, `FLOAT` or `DOUBLE` column
trait HashNative: Copy {
    fn hash(self) -> u64;
}

impl HashNative for i32 {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_i32(self)
    }
}

impl HashNative for i64 {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_i64(self)
    }
}

impl HashNative for f32 {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_i32(self.to_bits() as i32)
    }
}

impl HashNative for f64 {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_i64(self.to_bits() as i64)
    }
}

/// Hashes the `INT32`, `INT64`, `FLOAT` or `DOUBLE` values passed to it
struct HashPrimitive<V>(V);

impl<T: HashNative, V: HashVisitor> PrimitiveVisitor<T> for HashPrimitive<V> {
    type Output = V::Output;

    fn slice(self, values: &[T]) -> V::Output {
        self.0.visit(values.len(), |i| values[i].hash())
    }

    fn map<S: Copy>(self, values: &[S], convert: impl Fn(S) -> T) -> V::Output {
        self.0.visit(values.len(), |i| convert(values[i]).hash())
    }
}

/// Hashes the `FIXED_LEN_BYTE_ARRAY` values passed to it
struct HashFixedLenByteArray<'a, V> {
    visitor: V,
    len: usize,
    type_length: usize,
    data_type: &'a DataType,
}

impl<V: HashVisitor> FixedLenByteArrayVisitor for HashFixedLenByteArray<'_, V> {
    type Output = Result<V::Output>;

    fn decimal_length(&self, _precision: u8) -> usize {
        // Decimals of any precision are converted to the column's length
        self.type_length
    }

    fn values(self, length: usize, fill: impl Fn(usize, &mut [u8])) -> Result<V::Output> {
        if length != self.type_length {
            return Err(ParquetError::General(format!(
                "Sbbf: cannot hash {} values, which have length {length}, for a FIXED_LEN_BYTE_ARRAY column of length {}",
                self.data_type, self.type_length
            )));
        }
        let value = RefCell::new(vec![0; length]);
        Ok(self.visitor.visit(self.len, |i| {
            let mut value = value.borrow_mut();
            value.fill(0);
            fill(i, &mut value);
            hash_as_bytes(value.as_slice())
        }))
    }
}

/// Checks the hashes passed to it against a filter
struct CheckHashes<'a>(&'a Sbbf);

impl HashVisitor for CheckHashes<'_> {
    type Output = BooleanBuffer;

    fn visit(self, len: usize, hash: impl Fn(usize) -> u64) -> BooleanBuffer {
        BooleanBuffer::collect_bool(len, |i| self.0.check_hash_branch_free(hash(i)))
    }
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

    /// Calls `visitor` with the hashes of `array` for the column that
    /// `ArrowWriter` writes for it with the default options
    fn visit_array_hashes<V: HashVisitor>(array: &dyn Array, visitor: V) -> Result<V::Output> {
        let data_type = array.data_type();
        if !Self::supports_data_type(data_type) {
            return Err(unsupported(data_type));
        }
        visit_hashes(array, HashTarget::for_data_type(data_type)?, visitor)
    }

    /// Write [`Self::hash_value`] of each slot of `array` into `hashes`.
    ///
    /// Each value is hashed as the Parquet physical value that
    /// [`ArrowWriter`](crate::arrow::ArrowWriter) writes for it with the default
    /// options (for example an `Int8` is hashed as an `INT32`). `hashes` is
    /// cleared first and has `array.len()` items after the call. The hashes of
    /// null slots are not defined; the caller must skip them.
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
        Self::visit_array_hashes(array, Extend(hashes))
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
        Self::visit_array_hashes(
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
    /// To check a bloom filter read from a Parquet file, use
    /// [`ArrowSbbf`](crate::arrow::bloom_filter::ArrowSbbf), which converts the
    /// values to the column's physical type instead of the default one.
    ///
    /// Returns an error if [`Self::supports_data_type`] is `false` for the type of
    /// `array`.
    pub fn check_array(&self, array: &dyn Array) -> Result<BooleanBuffer> {
        let result = Self::visit_array_hashes(array, CheckHashes(self))?;
        Ok(match array.logical_nulls() {
            Some(nulls) => &result & nulls.inner(),
            None => result,
        })
    }

    /// Check each slot of `array`, hashed for a column of `target`
    ///
    /// The result of null slots is not defined.
    pub(crate) fn check_array_for(
        &self,
        array: &dyn Array,
        target: HashTarget,
    ) -> Result<BooleanBuffer> {
        visit_hashes(array, target, CheckHashes(self))
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

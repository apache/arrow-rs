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

//! Numeric physical storage and Arrow writer bridges.

use super::*;

pub(super) type Float32Storage<'a> = &'a [f32];
pub(super) type Float64Storage<'a> = &'a [f64];

/// Already-downcast storage for Arrow types represented by Parquet INT32.
#[derive(Clone, Copy)]
pub(crate) enum Int32Storage<'a> {
    Null(usize),
    Identity(&'a [i32]),
    Int8(&'a [i8]),
    Int16(&'a [i16]),
    UInt8(&'a [u8]),
    UInt16(&'a [u16]),
    Date64(&'a [i64]),
    Int64(&'a [i64]),
    Int128(&'a [i128]),
    Int256(&'a [arrow_buffer::i256]),
}

macro_rules! write_numeric {
    ($enc:expr, $values:expr, $selection:expr) => {
        $enc.write_numeric_source(PhysicalNumericSource::new(
            $values,
            Some($values),
            $selection,
            |value| value,
        ))
    };
    ($enc:expr, $values:expr, $selection:expr, $cast:expr) => {
        $enc.write_numeric_source(PhysicalNumericSource::new($values, None, $selection, $cast))
    };
}

impl<'a> ArrowPhysicalBridge<'a> for Int32Storage<'a> {
    type ColumnEncoder = TypedColumnChunkEncoder<ParquetInt32Type>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        Ok(match column.data_type() {
            ArrowDataType::Null => Self::Null(column.len()),
            ArrowDataType::Int8 => Self::Int8(primitive_values::<Int8Type>(column)),
            ArrowDataType::Int16 => Self::Int16(primitive_values::<Int16Type>(column)),
            ArrowDataType::Int32 => Self::Identity(primitive_values::<Int32Type>(column)),
            ArrowDataType::UInt8 => Self::UInt8(primitive_values::<UInt8Type>(column)),
            ArrowDataType::UInt16 => Self::UInt16(primitive_values::<UInt16Type>(column)),
            // Matches the C++ implementation by reinterpreting the u32 bits.
            ArrowDataType::UInt32 => Self::Identity(
                column
                    .as_primitive::<UInt32Type>()
                    .values()
                    .inner()
                    .typed_data(),
            ),
            ArrowDataType::Date32 => Self::Identity(primitive_values::<Date32Type>(column)),
            ArrowDataType::Date64 => Self::Date64(primitive_values::<Date64Type>(column)),
            ArrowDataType::Time32(TimeUnit::Second) => {
                Self::Identity(primitive_values::<Time32SecondType>(column))
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => {
                Self::Identity(primitive_values::<Time32MillisecondType>(column))
            }
            ArrowDataType::Decimal32(_, _) => {
                Self::Identity(primitive_values::<Decimal32Type>(column))
            }
            ArrowDataType::Decimal64(_, _) => {
                Self::Int64(primitive_values::<Decimal64Type>(column))
            }
            ArrowDataType::Decimal128(_, _) => {
                Self::Int128(primitive_values::<Decimal128Type>(column))
            }
            ArrowDataType::Decimal256(_, _) => {
                Self::Int256(primitive_values::<Decimal256Type>(column))
            }
            d => return Err(ParquetError::General(format!("Cannot coerce {d} to I32"))),
        })
    }

    fn write_values(
        self,
        enc: &mut Self::ColumnEncoder,
        sel: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        visit_i32(
            self,
            WriteInt32 {
                encoder: enc,
                selection: sel,
            },
        )
    }
}

/// Already-downcast storage for Arrow types represented by Parquet INT64.
#[derive(Clone, Copy)]
pub(crate) enum Int64Storage<'a> {
    Identity(&'a [i64]),
    Int128(&'a [i128]),
    Int256(&'a [arrow_buffer::i256]),
}

impl<'a> ArrowPhysicalBridge<'a> for Int64Storage<'a> {
    type ColumnEncoder = TypedColumnChunkEncoder<ParquetInt64Type>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        Ok(match column.data_type() {
            ArrowDataType::Int64 => Self::Identity(primitive_values::<Int64Type>(column)),
            // Matches the C++ implementation by reinterpreting the u64 bits.
            ArrowDataType::UInt64 => Self::Identity(
                column
                    .as_primitive::<UInt64Type>()
                    .values()
                    .inner()
                    .typed_data(),
            ),
            ArrowDataType::Date64 => Self::Identity(primitive_values::<Date64Type>(column)),
            ArrowDataType::Time64(TimeUnit::Microsecond) => {
                Self::Identity(primitive_values::<Time64MicrosecondType>(column))
            }
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                Self::Identity(primitive_values::<Time64NanosecondType>(column))
            }
            ArrowDataType::Timestamp(unit, _) => Self::Identity(match unit {
                TimeUnit::Second => primitive_values::<TimestampSecondType>(column),
                TimeUnit::Millisecond => primitive_values::<TimestampMillisecondType>(column),
                TimeUnit::Microsecond => primitive_values::<TimestampMicrosecondType>(column),
                TimeUnit::Nanosecond => primitive_values::<TimestampNanosecondType>(column),
            }),
            ArrowDataType::Duration(unit) => Self::Identity(match unit {
                TimeUnit::Second => primitive_values::<DurationSecondType>(column),
                TimeUnit::Millisecond => primitive_values::<DurationMillisecondType>(column),
                TimeUnit::Microsecond => primitive_values::<DurationMicrosecondType>(column),
                TimeUnit::Nanosecond => primitive_values::<DurationNanosecondType>(column),
            }),
            ArrowDataType::Decimal64(_, _) => {
                Self::Identity(primitive_values::<Decimal64Type>(column))
            }
            ArrowDataType::Decimal128(_, _) => {
                Self::Int128(primitive_values::<Decimal128Type>(column))
            }
            ArrowDataType::Decimal256(_, _) => {
                Self::Int256(primitive_values::<Decimal256Type>(column))
            }
            d => return Err(ParquetError::General(format!("Cannot coerce {d} to I64"))),
        })
    }

    fn write_values(
        self,
        enc: &mut Self::ColumnEncoder,
        sel: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        visit_i64(
            self,
            WriteInt64 {
                encoder: enc,
                selection: sel,
            },
        )
    }
}

/// Receives borrowed physical values or a conversion over borrowed Arrow values.
///
/// Conversion rules are shared by the selected writer and other physical-value
/// consumers, such as Bloom-filter probes. Binding is separate so page windows
/// do not repeat Arrow downcasts, and visitors need not materialize converted arrays.
pub(crate) trait PrimitiveVisitor<T> {
    type Output;

    fn slice(self, values: &[T]) -> Result<Self::Output>;
    fn map<S: Copy>(self, values: &[S], convert: impl Fn(S) -> T + Copy) -> Result<Self::Output>;
    /// All-null input has a logical length but no selected physical values.
    fn nulls(self, len: usize) -> Result<Self::Output>;
}

/// Visit the INT32 representation of already-bound Arrow storage.
pub(crate) fn visit_i32<V: PrimitiveVisitor<i32>>(
    storage: Int32Storage<'_>,
    visitor: V,
) -> Result<V::Output> {
    match storage {
        Int32Storage::Null(len) => visitor.nulls(len),
        Int32Storage::Identity(values) => visitor.slice(values),
        Int32Storage::Int8(values) => visitor.map(values, |v| v as i32),
        Int32Storage::Int16(values) => visitor.map(values, |v| v as i32),
        Int32Storage::UInt8(values) => visitor.map(values, |v| v as i32),
        Int32Storage::UInt16(values) => visitor.map(values, |v| v as i32),
        // Arrow Date64 stores milliseconds; Parquet DATE truncates to whole days.
        Int32Storage::Date64(values) => visitor.map(values, |v| (v / 86_400_000) as i32),
        // Schema conversion selects INT32 for decimal precision <= 9.
        Int32Storage::Int64(values) => visitor.map(values, |v| v as i32),
        Int32Storage::Int128(values) => visitor.map(values, |v| v as i32),
        Int32Storage::Int256(values) => visitor.map(values, |v| v.as_i128() as i32),
    }
}

/// Visit the INT64 representation of already-bound Arrow storage.
pub(crate) fn visit_i64<V: PrimitiveVisitor<i64>>(
    storage: Int64Storage<'_>,
    visitor: V,
) -> Result<V::Output> {
    match storage {
        Int64Storage::Identity(values) => visitor.slice(values),
        // Schema conversion selects INT64 for decimal precision <= 18.
        Int64Storage::Int128(values) => visitor.map(values, |v| v as i64),
        Int64Storage::Int256(values) => visitor.map(values, |v| v.as_i128() as i64),
    }
}

macro_rules! selected_numeric_visitor {
    ($name:ident, $native:ty, $physical:ty) => {
        struct $name<'encoder, 'source> {
            encoder: &'encoder mut TypedColumnChunkEncoder<$physical>,
            selection: PhysicalValueSelection<'source>,
        }

        impl PrimitiveVisitor<$native> for $name<'_, '_> {
            type Output = ();

            fn slice(self, values: &[$native]) -> Result<()> {
                write_numeric!(self.encoder, values, self.selection)
            }

            fn map<S: Copy>(
                self,
                values: &[S],
                convert: impl Fn(S) -> $native + Copy,
            ) -> Result<()> {
                write_numeric!(self.encoder, values, self.selection, convert)
            }

            fn nulls(self, _len: usize) -> Result<()> {
                debug_assert_eq!(self.selection.len(), 0);
                self.slice(&[])
            }
        }
    };
}

selected_numeric_visitor!(WriteInt32, i32, ParquetInt32Type);
selected_numeric_visitor!(WriteInt64, i64, ParquetInt64Type);

impl<'a> ArrowPhysicalBridge<'a> for &'a [f32] {
    type ColumnEncoder = TypedColumnChunkEncoder<ParquetFloatType>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        Ok(primitive_values::<Float32Type>(column))
    }

    fn write_values(
        self,
        enc: &mut Self::ColumnEncoder,
        sel: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        write_numeric!(enc, self, sel)
    }
}

impl<'a> ArrowPhysicalBridge<'a> for &'a [f64] {
    type ColumnEncoder = TypedColumnChunkEncoder<ParquetDoubleType>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        Ok(primitive_values::<Float64Type>(column))
    }

    fn write_values(
        self,
        enc: &mut Self::ColumnEncoder,
        sel: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        write_numeric!(enc, self, sel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;

    struct Collect;

    impl<T: Copy + Default> PrimitiveVisitor<T> for Collect {
        type Output = Vec<T>;

        fn slice(self, values: &[T]) -> Result<Self::Output> {
            Ok(values.to_vec())
        }

        fn map<S: Copy>(
            self,
            values: &[S],
            convert: impl Fn(S) -> T + Copy,
        ) -> Result<Self::Output> {
            Ok(values.iter().copied().map(convert).collect())
        }

        fn nulls(self, len: usize) -> Result<Self::Output> {
            Ok(vec![T::default(); len])
        }
    }

    #[test]
    fn shared_integer_visitors_preserve_physical_conversions() {
        let arrays: Vec<(ArrayRef, Vec<i32>)> = vec![
            (
                Arc::new(Int8Array::from(vec![-128, -1, 127])),
                vec![-128, -1, 127],
            ),
            (
                Arc::new(UInt32Array::from(vec![0, 1 << 31, u32::MAX])),
                vec![0, i32::MIN, -1],
            ),
            (
                Arc::new(Date64Array::from(vec![-86_400_001, -1, 86_400_001])),
                vec![-1, 0, 1],
            ),
            (
                Arc::new(Decimal32Array::from(vec![-123, 456])),
                vec![-123, 456],
            ),
            (
                Arc::new(Decimal64Array::from(vec![-123, 456])),
                vec![-123, 456],
            ),
            (
                Arc::new(Decimal128Array::from(vec![-123, 456])),
                vec![-123, 456],
            ),
            (
                Arc::new(Decimal256Array::from(vec![
                    arrow_buffer::i256::from_i128(-123),
                    arrow_buffer::i256::from_i128(456),
                ])),
                vec![-123, 456],
            ),
            (Arc::new(NullArray::new(3)), vec![0; 3]),
        ];
        for (array, expected) in arrays {
            let storage = Int32Storage::bind(array.as_ref()).unwrap();
            assert_eq!(
                visit_i32(storage, Collect).unwrap(),
                expected,
                "{:?}",
                array.data_type()
            );
        }
        let arrays: Vec<(ArrayRef, Vec<i64>)> = vec![
            (
                Arc::new(UInt64Array::from(vec![0, 1 << 63, u64::MAX])),
                vec![0, i64::MIN, -1],
            ),
            (
                Arc::new(Date64Array::from(vec![-86_400_001, -1, 86_400_001])),
                vec![-86_400_001, -1, 86_400_001],
            ),
            (
                Arc::new(Decimal64Array::from(vec![-123, 456])),
                vec![-123, 456],
            ),
            (
                Arc::new(Decimal128Array::from(vec![-123, 456])),
                vec![-123, 456],
            ),
            (
                Arc::new(Decimal256Array::from(vec![
                    arrow_buffer::i256::from_i128(-123),
                    arrow_buffer::i256::from_i128(456),
                ])),
                vec![-123, 456],
            ),
        ];
        for (array, expected) in arrays {
            let storage = Int64Storage::bind(array.as_ref()).unwrap();
            assert_eq!(
                visit_i64(storage, Collect).unwrap(),
                expected,
                "{:?}",
                array.data_type()
            );
        }
    }
}

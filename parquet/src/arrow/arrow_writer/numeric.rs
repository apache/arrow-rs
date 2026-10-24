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
pub(super) enum Int32Storage<'a> {
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
            ArrowDataType::Null => Self::Identity(&[]),
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
        match self {
            Self::Identity(values) => write_numeric!(enc, values, sel),
            Self::Int8(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::Int16(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::UInt8(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::UInt16(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::Date64(values) => {
                // Arrow Date64 stores milliseconds; Parquet DATE stores days,
                // so truncate to whole days.
                write_numeric!(enc, values, sel, |v| (v / 86_400_000) as i32)
            }
            // Schema conversion selects INT32 only for decimal precision <= 9,
            // so valid higher-width decimal values narrow losslessly.
            Self::Int64(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::Int128(values) => write_numeric!(enc, values, sel, |v| v as i32),
            Self::Int256(values) => {
                write_numeric!(enc, values, sel, |v| v.as_i128() as i32)
            }
        }
    }
}

/// Already-downcast storage for Arrow types represented by Parquet INT64.
#[derive(Clone, Copy)]
pub(super) enum Int64Storage<'a> {
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
        match self {
            Self::Identity(values) => write_numeric!(enc, values, sel),
            // Schema conversion selects INT64 only for decimal precision <= 18,
            // so valid higher-width decimal values narrow losslessly.
            Self::Int128(values) => write_numeric!(enc, values, sel, |v| v as i64),
            Self::Int256(values) => {
                write_numeric!(enc, values, sel, |v| v.as_i128() as i64)
            }
        }
    }
}

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

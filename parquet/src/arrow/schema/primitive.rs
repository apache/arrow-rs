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

use crate::basic::{ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType};
use crate::errors::{ParquetError, Result};
use crate::schema::types::{BasicTypeInfo, Type};
use arrow_schema::{DataType, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION};

/// Converts [`Type`] to [`DataType`] with an optional `arrow_type_hint`
/// provided by the arrow schema
///
/// Note: the values embedded in the schema are advisory,
pub fn convert_primitive(
    parquet_type: &Type,
    arrow_type_hint: Option<DataType>,
) -> Result<DataType> {
    let physical_type = from_parquet(parquet_type)?;
    Ok(match arrow_type_hint {
        Some(hint) => apply_hint(physical_type, hint),
        None => physical_type,
    })
}

/// Uses an type hint from the embedded arrow schema to aid in faithfully
/// reproducing the data as it was written into parquet
fn apply_hint(parquet: DataType, hint: DataType) -> DataType {
    match (&parquet, &hint) {
        // Not all time units can be represented as LogicalType / ConvertedType
        (DataType::Int32 | DataType::Int64, DataType::Timestamp(_, _)) => hint,
        (DataType::Int32, DataType::Time32(_)) => hint,
        (DataType::Int64, DataType::Time64(_)) => hint,

        // Date64 doesn't have a corresponding LogicalType / ConvertedType
        (DataType::Int64, DataType::Date64) => hint,

        // Coerce Date32 back to Date64 (#1666)
        (DataType::Date32, DataType::Date64) => hint,

        // Determine timezone
        (DataType::Timestamp(p, _), DataType::Timestamp(h, Some(_))) if p == h => hint,

        // Determine offset size
        (DataType::Utf8, DataType::LargeUtf8) => hint,
        (DataType::Binary, DataType::LargeBinary) => hint,

        // Read as Utf8
        (DataType::Binary, DataType::Utf8) => hint,
        (DataType::Binary, DataType::LargeUtf8) => hint,
        (DataType::Binary, DataType::Utf8View) => hint,

        // Determine view type
        (DataType::Utf8, DataType::Utf8View) => hint,
        (DataType::Binary, DataType::BinaryView) => hint,

        // Determine interval time unit (#1666)
        (DataType::Interval(_), DataType::Interval(_)) => hint,

        // Promote to Decimal256
        (DataType::Decimal128(_, _), DataType::Decimal256(_, _)) => hint,

        // Potentially preserve dictionary encoding
        (_, DataType::Dictionary(_, value)) => {
            // Apply hint to inner type
            let hinted = apply_hint(parquet, value.as_ref().clone());

            // If matches dictionary value - preserve dictionary
            // otherwise use hinted inner type
            match &hinted == value.as_ref() {
                true => hint,
                false => hinted,
            }
        }
        _ => parquet,
    }
}

fn from_parquet(parquet_type: &Type) -> Result<DataType> {
    match parquet_type {
        Type::PrimitiveType {
            physical_type,
            basic_info,
            type_length,
            scale,
            precision,
            ..
        } => match physical_type {
            PhysicalType::BOOLEAN => Ok(DataType::Boolean),
            PhysicalType::INT32 => from_int32(basic_info, *scale, *precision),
            PhysicalType::INT64 => from_int64(basic_info, *scale, *precision),
            PhysicalType::INT96 => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            PhysicalType::FLOAT => Ok(DataType::Float32),
            PhysicalType::DOUBLE => Ok(DataType::Float64),
            PhysicalType::BYTE_ARRAY => from_byte_array(basic_info, *precision, *scale),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                from_fixed_len_byte_array(basic_info, *scale, *precision, *type_length)
            }
        },
        Type::GroupType { .. } => unreachable!(),
    }
}

fn decimal_type(scale: i32, precision: i32) -> Result<DataType> {
    if precision <= DECIMAL128_MAX_PRECISION as _ {
        decimal_128_type(scale, precision)
    } else {
        decimal_256_type(scale, precision)
    }
}

fn decimal_128_type(scale: i32, precision: i32) -> Result<DataType> {
    let scale = scale
        .try_into()
        .map_err(|_| arrow_err!("scale cannot be negative: {}", scale))?;

    let precision = precision
        .try_into()
        .map_err(|_| arrow_err!("precision cannot be negative: {}", precision))?;

    Ok(DataType::Decimal128(precision, scale))
}

fn decimal_256_type(scale: i32, precision: i32) -> Result<DataType> {
    let scale = scale
        .try_into()
        .map_err(|_| arrow_err!("scale cannot be negative: {}", scale))?;

    let precision = precision
        .try_into()
        .map_err(|_| arrow_err!("precision cannot be negative: {}", precision))?;

    Ok(DataType::Decimal256(precision, scale))
}

fn from_int32(info: &BasicTypeInfo, scale: i32, precision: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataType::Int32),
        (
            Some(
                ref t @ LogicalType::Integer {
                    bit_width,
                    is_signed,
                },
            ),
            _,
        ) => match (bit_width, is_signed) {
            (8, true) => Ok(DataType::Int8),
            (16, true) => Ok(DataType::Int16),
            (32, true) => Ok(DataType::Int32),
            (8, false) => Ok(DataType::UInt8),
            (16, false) => Ok(DataType::UInt16),
            (32, false) => Ok(DataType::UInt32),
            _ => Err(arrow_err!("Cannot create INT32 physical type from {:?}", t)),
        },
        (Some(LogicalType::Decimal { scale, precision }), _) => decimal_128_type(scale, precision),
        (Some(LogicalType::Date), _) => Ok(DataType::Date32),
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            ParquetTimeUnit::MILLIS(_) => Ok(DataType::Time32(TimeUnit::Millisecond)),
            _ => Err(arrow_err!(
                "Cannot create INT32 physical type from {:?}",
                unit
            )),
        },
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null
        (Some(LogicalType::Unknown), _) => Ok(DataType::Null),
        (None, ConvertedType::UINT_8) => Ok(DataType::UInt8),
        (None, ConvertedType::UINT_16) => Ok(DataType::UInt16),
        (None, ConvertedType::UINT_32) => Ok(DataType::UInt32),
        (None, ConvertedType::INT_8) => Ok(DataType::Int8),
        (None, ConvertedType::INT_16) => Ok(DataType::Int16),
        (None, ConvertedType::INT_32) => Ok(DataType::Int32),
        (None, ConvertedType::DATE) => Ok(DataType::Date32),
        (None, ConvertedType::TIME_MILLIS) => Ok(DataType::Time32(TimeUnit::Millisecond)),
        (None, ConvertedType::DECIMAL) => decimal_128_type(scale, precision),
        (logical, converted) => Err(arrow_err!(
            "Unable to convert parquet INT32 logical type {:?} or converted type {}",
            logical,
            converted
        )),
    }
}

fn from_int64(info: &BasicTypeInfo, scale: i32, precision: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (None, ConvertedType::NONE) => Ok(DataType::Int64),
        (
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed,
            }),
            _,
        ) => match is_signed {
            true => Ok(DataType::Int64),
            false => Ok(DataType::UInt64),
        },
        (Some(LogicalType::Time { unit, .. }), _) => match unit {
            ParquetTimeUnit::MILLIS(_) => {
                Err(arrow_err!("Cannot create INT64 from MILLIS time unit",))
            }
            ParquetTimeUnit::MICROS(_) => Ok(DataType::Time64(TimeUnit::Microsecond)),
            ParquetTimeUnit::NANOS(_) => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        },
        (
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            }),
            _,
        ) => Ok(DataType::Timestamp(
            match unit {
                ParquetTimeUnit::MILLIS(_) => TimeUnit::Millisecond,
                ParquetTimeUnit::MICROS(_) => TimeUnit::Microsecond,
                ParquetTimeUnit::NANOS(_) => TimeUnit::Nanosecond,
            },
            if is_adjusted_to_u_t_c {
                Some("UTC".into())
            } else {
                None
            },
        )),
        (None, ConvertedType::INT_64) => Ok(DataType::Int64),
        (None, ConvertedType::UINT_64) => Ok(DataType::UInt64),
        (None, ConvertedType::TIME_MICROS) => Ok(DataType::Time64(TimeUnit::Microsecond)),
        (None, ConvertedType::TIMESTAMP_MILLIS) => Ok(DataType::Timestamp(
            TimeUnit::Millisecond,
            Some("UTC".into()),
        )),
        (None, ConvertedType::TIMESTAMP_MICROS) => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        (Some(LogicalType::Decimal { scale, precision }), _) => decimal_128_type(scale, precision),
        (None, ConvertedType::DECIMAL) => decimal_128_type(scale, precision),
        (logical, converted) => Err(arrow_err!(
            "Unable to convert parquet INT64 logical type {:?} or converted type {}",
            logical,
            converted
        )),
    }
}

fn from_byte_array(info: &BasicTypeInfo, precision: i32, scale: i32) -> Result<DataType> {
    match (info.logical_type(), info.converted_type()) {
        (Some(LogicalType::String), _) => Ok(DataType::Utf8),
        (Some(LogicalType::Json), _) => Ok(DataType::Utf8),
        (Some(LogicalType::Bson), _) => Ok(DataType::Binary),
        (Some(LogicalType::Enum), _) => Ok(DataType::Binary),
        (None, ConvertedType::NONE) => Ok(DataType::Binary),
        (None, ConvertedType::JSON) => Ok(DataType::Utf8),
        (None, ConvertedType::BSON) => Ok(DataType::Binary),
        (None, ConvertedType::ENUM) => Ok(DataType::Binary),
        (None, ConvertedType::UTF8) => Ok(DataType::Utf8),
        (
            Some(LogicalType::Decimal {
                scale: s,
                precision: p,
            }),
            _,
        ) => decimal_type(s, p),
        (None, ConvertedType::DECIMAL) => decimal_type(scale, precision),
        (logical, converted) => Err(arrow_err!(
            "Unable to convert parquet BYTE_ARRAY logical type {:?} or converted type {}",
            logical,
            converted
        )),
    }
}

fn from_fixed_len_byte_array(
    info: &BasicTypeInfo,
    scale: i32,
    precision: i32,
    type_length: i32,
) -> Result<DataType> {
    // TODO: This should check the type length for the decimal and interval types
    match (info.logical_type(), info.converted_type()) {
        (Some(LogicalType::Decimal { scale, precision }), _) => {
            if type_length <= 16 {
                decimal_128_type(scale, precision)
            } else {
                decimal_256_type(scale, precision)
            }
        }
        (None, ConvertedType::DECIMAL) => {
            if type_length <= 16 {
                decimal_128_type(scale, precision)
            } else {
                decimal_256_type(scale, precision)
            }
        }
        (None, ConvertedType::INTERVAL) => {
            // There is currently no reliable way of determining which IntervalUnit
            // to return. Thus without the original Arrow schema, the results
            // would be incorrect if all 12 bytes of the interval are populated
            Ok(DataType::Interval(IntervalUnit::DayTime))
        }
        (Some(LogicalType::Float16), _) => {
            if type_length == 2 {
                Ok(DataType::Float16)
            } else {
                Err(ParquetError::General(
                    "FLOAT16 logical type must be Fixed Length Byte Array with length 2"
                        .to_string(),
                ))
            }
        }
        _ => Ok(DataType::FixedSizeBinary(type_length)),
    }
}

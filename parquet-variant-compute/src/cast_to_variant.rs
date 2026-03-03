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

use crate::arrow_to_variant::make_arrow_to_variant_row_builder;
use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::Array;
use arrow::compute::CastOptions;
use arrow_schema::ArrowError;

/// Casts a typed arrow [`Array`] to a [`VariantArray`]. This is useful when you
/// need to convert a specific data type
///
/// # Arguments
/// * `input` - A reference to the input [`Array`] to cast
///
/// # Notes
/// If the input array element is null, the corresponding element in the
/// output `VariantArray` will also be null (not `Variant::Null`).
///
/// # Example
/// ```
/// # use arrow::array::{Array, ArrayRef, Int64Array};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::cast_to_variant;
/// // input is an Int64Array, which will be cast to a VariantArray
/// let input = Int64Array::from(vec![Some(1), None, Some(3)]);
/// let result = cast_to_variant(&input).unwrap();
/// assert_eq!(result.len(), 3);
/// assert_eq!(result.value(0), Variant::Int64(1));
/// assert!(result.is_null(1)); // note null, not Variant::Null
/// assert_eq!(result.value(2), Variant::Int64(3));
/// ```
///
/// For `DataType::Timestamp`s: if the timestamp has any level of precision
/// greater than a microsecond, it will be truncated. For example
/// `1970-01-01T00:00:01.234567890Z`
/// will be truncated to
/// `1970-01-01T00:00:01.234567Z`
///
/// # Arguments
/// * `input` - The array to convert to VariantArray
/// * `options` - Options controlling conversion behavior
pub fn cast_to_variant_with_options(
    input: &dyn Array,
    options: &CastOptions,
) -> Result<VariantArray, ArrowError> {
    // Create row builder for the input array type
    let mut row_builder = make_arrow_to_variant_row_builder(input.data_type(), input, options)?;

    // Create output array builder
    let mut array_builder = VariantArrayBuilder::new(input.len());

    // Process each row using the row builder
    for i in 0..input.len() {
        row_builder.append_row(&mut array_builder, i)?;
    }

    Ok(array_builder.build())
}

/// Convert an array to a [`VariantArray`] with strict mode enabled (returns errors on conversion
/// failures).
///
/// This function provides backward compatibility. For non-strict behavior,
/// use [`cast_to_variant_with_options`] with `CastOptions { safe: true, ..Default::default() }`.
pub fn cast_to_variant(input: &dyn Array) -> Result<VariantArray, ArrowError> {
    cast_to_variant_with_options(
        input,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
        Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
        DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
        DurationSecondArray, FixedSizeBinaryBuilder, FixedSizeListBuilder, Float16Array,
        Float32Array, Float64Array, GenericByteBuilder, GenericByteViewBuilder, Int8Array,
        Int16Array, Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
        IntervalYearMonthArray, LargeListArray, LargeListViewBuilder, LargeStringArray, ListArray,
        ListViewBuilder, MapArray, NullArray, StringArray, StringRunBuilder, StringViewArray,
        StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{
        BinaryType, BinaryViewType, Date32Type, Date64Type, Int8Type, Int32Type, Int64Type,
        IntervalDayTime, IntervalMonthDayNano, LargeBinaryType, i256,
    };
    use arrow::temporal_conversions::timestamp_s_to_datetime;
    use arrow_schema::{
        DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION, DECIMAL128_MAX_PRECISION,
    };
    use arrow_schema::{DataType, Field, Fields, UnionFields};
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use half::f16;
    use parquet_variant::{
        Variant, VariantBuilder, VariantBuilderExt, VariantDecimal4, VariantDecimal8,
        VariantDecimal16,
    };
    use std::{sync::Arc, vec};

    macro_rules! max_unscaled_value {
        (32, $precision:expr) => {
            (u32::pow(10, $precision as u32) - 1) as i32
        };
        (64, $precision:expr) => {
            (u64::pow(10, $precision as u32) - 1) as i64
        };
        (128, $precision:expr) => {
            (u128::pow(10, $precision as u32) - 1) as i128
        };
    }

    #[test]
    fn test_cast_to_variant_null() {
        run_test(Arc::new(NullArray::new(2)), vec![None, None])
    }

    #[test]
    fn test_cast_to_variant_bool() {
        run_test(
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            vec![
                Some(Variant::BooleanTrue),
                None,
                Some(Variant::BooleanFalse),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_int8() {
        run_test(
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i8::MAX),
            ])),
            vec![
                Some(Variant::Int8(i8::MIN)),
                None,
                Some(Variant::Int8(-1)),
                Some(Variant::Int8(1)),
                Some(Variant::Int8(i8::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int16() {
        run_test(
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i16::MAX),
            ])),
            vec![
                Some(Variant::Int16(i16::MIN)),
                None,
                Some(Variant::Int16(-1)),
                Some(Variant::Int16(1)),
                Some(Variant::Int16(i16::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int32() {
        run_test(
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i32::MAX),
            ])),
            vec![
                Some(Variant::Int32(i32::MIN)),
                None,
                Some(Variant::Int32(-1)),
                Some(Variant::Int32(1)),
                Some(Variant::Int32(i32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int64() {
        run_test(
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i64::MAX),
            ])),
            vec![
                Some(Variant::Int64(i64::MIN)),
                None,
                Some(Variant::Int64(-1)),
                Some(Variant::Int64(1)),
                Some(Variant::Int64(i64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint8() {
        run_test(
            Arc::new(UInt8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(127),
                Some(u8::MAX),
            ])),
            vec![
                Some(Variant::Int8(0)),
                None,
                Some(Variant::Int8(1)),
                Some(Variant::Int8(127)),
                Some(Variant::Int16(255)), // u8::MAX cannot fit in Int8
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint16() {
        run_test(
            Arc::new(UInt16Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(32767),
                Some(u16::MAX),
            ])),
            vec![
                Some(Variant::Int16(0)),
                None,
                Some(Variant::Int16(1)),
                Some(Variant::Int16(32767)),
                Some(Variant::Int32(65535)), // u16::MAX cannot fit in Int16
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint32() {
        run_test(
            Arc::new(UInt32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2147483647),
                Some(u32::MAX),
            ])),
            vec![
                Some(Variant::Int32(0)),
                None,
                Some(Variant::Int32(1)),
                Some(Variant::Int32(2147483647)),
                Some(Variant::Int64(4294967295)), // u32::MAX cannot fit in Int32
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint64() {
        run_test(
            Arc::new(UInt64Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(9223372036854775807),
                Some(u64::MAX),
            ])),
            vec![
                Some(Variant::Int64(0)),
                None,
                Some(Variant::Int64(1)),
                Some(Variant::Int64(9223372036854775807)),
                Some(Variant::Decimal16(
                    // u64::MAX cannot fit in Int64
                    VariantDecimal16::try_from(18446744073709551615).unwrap(),
                )),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float16() {
        run_test(
            Arc::new(Float16Array::from(vec![
                Some(f16::MIN),
                None,
                Some(f16::from_f32(-1.5)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(1.5)),
                Some(f16::MAX),
            ])),
            vec![
                Some(Variant::Float(f16::MIN.into())),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f16::MAX.into())),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float32() {
        run_test(
            Arc::new(Float32Array::from(vec![
                Some(f32::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f32::MAX),
            ])),
            vec![
                Some(Variant::Float(f32::MIN)),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float64() {
        run_test(
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f64::MAX),
            ])),
            vec![
                Some(Variant::Double(f64::MIN)),
                None,
                Some(Variant::Double(-1.5)),
                Some(Variant::Double(0.0)),
                Some(Variant::Double(1.5)),
                Some(Variant::Double(f64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The min of Decimal32 with positive scale that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The max of Decimal32 with positive scale that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32_negative_scale() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The min of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The max of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(
                        -max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(
                        max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32_overflow_strict_mode() {
        run_test_in_strict_mode(
            Arc::new(
                Decimal32Array::from(vec![Some(i32::MIN)])
                    .with_precision_and_scale(DECIMAL32_MAX_PRECISION, 3)
                    .unwrap(),
            ),
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_decimal64() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The min of Decimal64 with positive scale that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The max of Decimal64 with positive scale that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64_negative_scale() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The min of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The max of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(
                        -max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(
                        max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64_overflow_strict_mode() {
        run_test_in_strict_mode(
            Arc::new(
                Decimal64Array::from(vec![Some(i64::MAX)])
                    .with_precision_and_scale(DECIMAL64_MAX_PRECISION, 3)
                    .unwrap(),
            ),
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_decimal128() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The min of Decimal128 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The max of Decimal128 with positive scale that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128_negative_scale() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The min of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The max of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128_overflow_strict_mode() {
        run_test_in_strict_mode(
            Arc::new(
                Decimal128Array::from(vec![Some(
                    -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1,
                )])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_decimal256() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The min of Decimal256 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The max of Decimal256 with positive scale that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256_negative_scale() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The min of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The max of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256_overflow_strict_mode() {
        run_test_in_strict_mode(
            Arc::new(
                Decimal256Array::from(vec![Some(i256::from_i128(
                    max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1,
                ))])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_timestamp() {
        let run_array_tests =
            |microseconds: i64, array_ntz: Arc<dyn Array>, array_tz: Arc<dyn Array>| {
                let timestamp = DateTime::from_timestamp_nanos(microseconds * 1000);
                run_test(
                    array_tz,
                    vec![Some(Variant::TimestampMicros(timestamp)), None],
                );
                run_test(
                    array_ntz,
                    vec![
                        Some(Variant::TimestampNtzMicros(timestamp.naive_utc())),
                        None,
                    ],
                );
            };

        let nanosecond = 1234567890;
        let microsecond = 1234567;
        let millisecond = 1234;
        let second = 1;

        let second_array = TimestampSecondArray::from(vec![Some(second), None]);
        run_array_tests(
            second * 1000 * 1000,
            Arc::new(second_array.clone()),
            Arc::new(second_array.with_timezone("+01:00".to_string())),
        );

        let millisecond_array = TimestampMillisecondArray::from(vec![Some(millisecond), None]);
        run_array_tests(
            millisecond * 1000,
            Arc::new(millisecond_array.clone()),
            Arc::new(millisecond_array.with_timezone("+01:00".to_string())),
        );

        let microsecond_array = TimestampMicrosecondArray::from(vec![Some(microsecond), None]);
        run_array_tests(
            microsecond,
            Arc::new(microsecond_array.clone()),
            Arc::new(microsecond_array.with_timezone("+01:00".to_string())),
        );

        let timestamp = DateTime::from_timestamp_nanos(nanosecond);
        let nanosecond_array = TimestampNanosecondArray::from(vec![Some(nanosecond), None]);
        run_test(
            Arc::new(nanosecond_array.clone()),
            vec![
                Some(Variant::TimestampNtzNanos(timestamp.naive_utc())),
                None,
            ],
        );
        run_test(
            Arc::new(nanosecond_array.with_timezone("+01:00".to_string())),
            vec![Some(Variant::TimestampNanos(timestamp)), None],
        );
    }

    #[test]
    fn test_cast_to_variant_timestamp_overflow_strict_mode() {
        let ts_array = TimestampSecondArray::from(vec![Some(i64::MAX), Some(0), Some(1609459200)])
            .with_timezone_opt(None::<&str>);

        let values = Arc::new(ts_array);
        run_test_in_strict_mode(
            values,
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_timestamp_overflow_non_strict_mode() {
        let ts_array = TimestampSecondArray::from(vec![Some(i64::MAX), Some(0), Some(1609459200)])
            .with_timezone_opt(None::<&str>);

        let values = Arc::new(ts_array);
        run_test(
            values,
            vec![
                Some(Variant::Null), // Invalid timestamp becomes null
                Some(Variant::TimestampNtzMicros(
                    timestamp_s_to_datetime(0).unwrap(),
                )),
                Some(Variant::TimestampNtzMicros(
                    timestamp_s_to_datetime(1609459200).unwrap(),
                )),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_date() {
        // Date32Array
        run_test(
            Arc::new(Date32Array::from(vec![
                Some(Date32Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date32Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date32Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );

        // Date64Array
        run_test(
            Arc::new(Date64Array::from(vec![
                Some(Date64Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date64Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date64Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_date64_strict_mode() {
        let date64_values = Date64Array::from(vec![Some(i64::MAX), Some(0), Some(i64::MIN)]);

        let values = Arc::new(date64_values);
        run_test_in_strict_mode(
            values,
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_date64_non_strict_mode() {
        let date64_values = Date64Array::from(vec![Some(i64::MAX), Some(0), Some(i64::MIN)]);

        let values = Arc::new(date64_values);
        run_test(
            values,
            vec![
                Some(Variant::Null),
                Some(Variant::Date(Date64Type::to_naive_date_opt(0).unwrap())),
                Some(Variant::Null),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_time32_second() {
        let array: Time32SecondArray = vec![Some(1), Some(86_399), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(1, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(86_399, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time32_millisecond() {
        let array: Time32MillisecondArray = vec![Some(123_456), Some(456_000), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_000_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(456, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time64_micro() {
        let array: Time64MicrosecondArray = vec![Some(1), Some(123_456_789), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time64_nano() {
        let array: Time64NanosecondArray =
            vec![Some(1), Some(1001), Some(123_456_789_012), None].into();
        run_test(
            Arc::new(array),
            // as we can only present with micro second, so the nano second will round donw to 0
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_time32_strict_mode() {
        let time32_array = Time32SecondArray::from(vec![Some(90000), Some(3600), Some(-1)]);

        let values = Arc::new(time32_array);
        run_test_in_strict_mode(
            values,
            Err(ArrowError::ComputeError(
                "Failed to convert value at index 0: conversion failed".to_string(),
            )),
        );
    }

    #[test]
    fn test_cast_to_variant_time32_non_strict_mode() {
        let time32_array = Time32SecondArray::from(vec![Some(90000), Some(3600), Some(-1)]);

        let values = Arc::new(time32_array);
        run_test(
            values,
            vec![
                Some(Variant::Null),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(3600, 0).unwrap(),
                )),
                Some(Variant::Null),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_duration_or_interval_errors() {
        let arrays: Vec<Box<dyn Array>> = vec![
            // Duration types
            Box::new(DurationSecondArray::from(vec![Some(10), None, Some(-5)])),
            Box::new(DurationMillisecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            Box::new(DurationMicrosecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            Box::new(DurationNanosecondArray::from(vec![
                Some(10),
                None,
                Some(-5),
            ])),
            // Interval types
            Box::new(IntervalYearMonthArray::from(vec![Some(12), None, Some(-6)])),
            Box::new(IntervalDayTimeArray::from(vec![
                Some(IntervalDayTime::new(12, 0)),
                None,
                Some(IntervalDayTime::new(-6, 0)),
            ])),
            Box::new(IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(12, 0, 0)),
                None,
                Some(IntervalMonthDayNano::new(-6, 0, 0)),
            ])),
        ];

        for array in arrays {
            let result = cast_to_variant(array.as_ref());
            assert!(result.is_err());
            match result.unwrap_err() {
                ArrowError::InvalidArgumentError(msg) => {
                    assert!(
                        msg.contains("Casting duration/interval types to Variant is not supported")
                    );
                    assert!(
                        msg.contains("The Variant format does not define duration/interval types")
                    );
                }
                _ => panic!("Expected InvalidArgumentError"),
            }
        }
    }

    #[test]
    fn test_cast_to_variant_binary() {
        // BinaryType
        let mut builder = GenericByteBuilder::<BinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let binary_array = builder.finish();
        run_test(
            Arc::new(binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // LargeBinaryType
        let mut builder = GenericByteBuilder::<LargeBinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let large_binary_array = builder.finish();
        run_test(
            Arc::new(large_binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // BinaryViewType
        let mut builder = GenericByteViewBuilder::<BinaryViewType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let byte_view_array = builder.finish();
        run_test(
            Arc::new(byte_view_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_fixed_size_binary() {
        let v1 = vec![1, 2];
        let v2 = vec![3, 4];
        let v3 = vec![5, 6];

        let mut builder = FixedSizeBinaryBuilder::new(2);
        builder.append_value(&v1).unwrap();
        builder.append_value(&v2).unwrap();
        builder.append_null();
        builder.append_value(&v3).unwrap();
        let array = builder.finish();

        run_test(
            Arc::new(array),
            vec![
                Some(Variant::Binary(&v1)),
                Some(Variant::Binary(&v2)),
                None,
                Some(Variant::Binary(&v3)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world"), Some("test")];
        let string_array = StringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
                Some(Variant::from("test")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "a".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = StringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_large_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_array = LargeStringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "b".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = LargeStringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_utf8_view() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_view_array = StringViewArray::from(short_strings.clone());

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "c".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_view_array = StringViewArray::from(long_strings);

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_list() {
        // List Array
        let data = vec![Some(vec![Some(0), Some(1), Some(2)]), None];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0);
            list.append_value(1);
            list.append_value(2);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(list_array), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_sliced_list() {
        // List Array
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            None,
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3);
            list.append_value(4);
            list.append_value(5);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(list_array.slice(1, 2)), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_large_list() {
        // Large List Array
        let data = vec![Some(vec![Some(0), Some(1), Some(2)]), None];
        let large_list_array = LargeListArray::from_iter_primitive::<Int64Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0i64);
            list.append_value(1i64);
            list.append_value(2i64);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(Arc::new(large_list_array), vec![Some(variant), None]);
    }

    #[test]
    fn test_cast_to_variant_sliced_large_list() {
        // List Array
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            None,
        ];
        let large_list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(data);

        // Expected value
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i64);
            list.append_value(4i64);
            list.append_value(5i64);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(
            Arc::new(large_list_array.slice(1, 2)),
            vec![Some(variant), None],
        );
    }

    #[test]
    fn test_cast_to_variant_list_view() {
        // Create a ListViewArray with some data
        let mut builder = ListViewBuilder::new(Int32Array::builder(0));
        builder.append_value(&Int32Array::from(vec![Some(0), None, Some(2)]));
        builder.append_value(&Int32Array::from(vec![Some(3), Some(4)]));
        builder.append_null();
        builder.append_value(&Int32Array::from(vec![None, None]));
        let list_view_array = builder.finish();

        // Expected values
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0i32);
            list.append_null();
            list.append_value(2i32);
            list.finish();
            builder.finish()
        };
        let variant0 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i32);
            list.append_value(4i32);
            list.finish();
            builder.finish()
        };
        let variant1 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_null();
            list.append_null();
            list.finish();
            builder.finish()
        };
        let variant3 = Variant::new(&metadata, &value);

        run_test(
            Arc::new(list_view_array),
            vec![Some(variant0), Some(variant1), None, Some(variant3)],
        );
    }

    #[test]
    fn test_cast_to_variant_sliced_list_view() {
        // Create a ListViewArray with some data
        let mut builder = ListViewBuilder::new(Int32Array::builder(0));
        builder.append_value(&Int32Array::from(vec![Some(0), Some(1), Some(2)]));
        builder.append_value(&Int32Array::from(vec![Some(3), None]));
        builder.append_null();
        let list_view_array = builder.finish();

        // Expected value for slice(1, 2) - should get the second and third elements
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i32);
            list.append_null();
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(
            Arc::new(list_view_array.slice(1, 2)),
            vec![Some(variant), None],
        );
    }

    #[test]
    fn test_cast_to_variant_large_list_view() {
        // Create a LargeListViewArray with some data
        let mut builder = LargeListViewBuilder::new(Int64Array::builder(0));
        builder.append_value(&Int64Array::from(vec![Some(0), None, Some(2)]));
        builder.append_value(&Int64Array::from(vec![Some(3), Some(4)]));
        builder.append_null();
        builder.append_value(&Int64Array::from(vec![None, None]));
        let large_list_view_array = builder.finish();

        // Expected values
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0i64);
            list.append_null();
            list.append_value(2i64);
            list.finish();
            builder.finish()
        };
        let variant0 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i64);
            list.append_value(4i64);
            list.finish();
            builder.finish()
        };
        let variant1 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_null();
            list.append_null();
            list.finish();
            builder.finish()
        };
        let variant3 = Variant::new(&metadata, &value);

        run_test(
            Arc::new(large_list_view_array),
            vec![Some(variant0), Some(variant1), None, Some(variant3)],
        );
    }

    #[test]
    fn test_cast_to_variant_sliced_large_list_view() {
        // Create a LargeListViewArray with some data
        let mut builder = LargeListViewBuilder::new(Int64Array::builder(0));
        builder.append_value(&Int64Array::from(vec![Some(0), Some(1), Some(2)]));
        builder.append_value(&Int64Array::from(vec![Some(3), None]));
        builder.append_null();
        let large_list_view_array = builder.finish();

        // Expected value for slice(1, 2) - should get the second and third elements
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(3i64);
            list.append_null();
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(
            Arc::new(large_list_view_array.slice(1, 2)),
            vec![Some(variant), None],
        );
    }

    #[test]
    fn test_cast_to_variant_fixed_size_list() {
        let mut builder = FixedSizeListBuilder::new(Int32Array::builder(0), 2);
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.append(true); // First list: [0, 1]

        builder.values().append_null();
        builder.values().append_value(3);
        builder.append(true); // Second list: [null, 3]

        builder.values().append_value(4);
        builder.values().append_null();
        builder.append(false); // Third list: null

        builder.values().append_nulls(2);
        builder.append(true); // Last list: [null, null]

        let fixed_size_list_array = builder.finish();

        // Expected values
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_value(0i32);
            list.append_value(1i32);
            list.finish();
            builder.finish()
        };
        let variant0 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_null();
            list.append_value(3i32);
            list.finish();
            builder.finish()
        };
        let variant1 = Variant::new(&metadata, &value);

        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_null();
            list.append_null();
            list.finish();
            builder.finish()
        };
        let variant3 = Variant::new(&metadata, &value);

        run_test(
            Arc::new(fixed_size_list_array),
            vec![Some(variant0), Some(variant1), None, Some(variant3)],
        );
    }

    #[test]
    fn test_cast_to_variant_sliced_fixed_size_list() {
        // Create a FixedSizeListArray with size 2
        let mut builder = FixedSizeListBuilder::new(Int64Array::builder(0), 2);
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.append(true); // First list: [0, 1]

        builder.values().append_null();
        builder.values().append_value(3);
        builder.append(true); // Second list: [null, 3]

        builder.values().append_value(4);
        builder.values().append_null();
        builder.append(false); // Third list: null

        let fixed_size_list_array = builder.finish();

        // Expected value for slice(1, 2) - should get the second and third elements
        let (metadata, value) = {
            let mut builder = VariantBuilder::new();
            let mut list = builder.new_list();
            list.append_null();
            list.append_value(3i64);
            list.finish();
            builder.finish()
        };
        let variant = Variant::new(&metadata, &value);

        run_test(
            Arc::new(fixed_size_list_array.slice(1, 2)),
            vec![Some(variant), None],
        );
    }

    #[test]
    fn test_cast_to_variant_struct() {
        // Test a simple struct with two fields: id (int64) and age (int32)
        let id_array = Int64Array::from(vec![Some(1001), Some(1002), None, Some(1003)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30), Some(35), None]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("age", DataType::Int32, true),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            None, // no nulls at the struct level
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 4);

        // Check first row: {"id": 1001, "age": 25}
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: {"id": 1002, "age": 30}
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));
        assert_eq!(obj2.get("age"), Some(Variant::from(30i32)));

        // Check third row: {"age": 35} (id is null, so omitted)
        let variant3 = result.value(2);
        let obj3 = variant3.as_object().unwrap();
        assert_eq!(obj3.get("id"), None);
        assert_eq!(obj3.get("age"), Some(Variant::from(35i32)));

        // Check fourth row: {"id": 1003} (age is null, so omitted)
        let variant4 = result.value(3);
        let obj4 = variant4.as_object().unwrap();
        assert_eq!(obj4.get("id"), Some(Variant::from(1003i64)));
        assert_eq!(obj4.get("age"), None);
    }

    #[test]
    fn test_cast_to_variant_struct_with_nulls() {
        // Test struct with null values at the struct level
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30)]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create null buffer to make second row null
        let null_buffer = NullBuffer::from(vec![true, false]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            Some(null_buffer),
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row: {"id": 1001, "age": 25}
        assert!(!result.is_null(0));
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: null struct
        assert!(result.is_null(1));
    }

    #[test]
    fn test_cast_to_variant_struct_performance() {
        // Test with a larger struct to demonstrate performance optimization
        // This test ensures that field arrays are only converted once, not per row
        let size = 1000;

        let id_array = Int64Array::from((0..size).map(|i| Some(i as i64)).collect::<Vec<_>>());
        let age_array = Int32Array::from(
            (0..size)
                .map(|i| Some((i % 100) as i32))
                .collect::<Vec<_>>(),
        );
        let score_array =
            Float64Array::from((0..size).map(|i| Some(i as f64 * 0.1)).collect::<Vec<_>>());

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(id_array),
                Arc::new(age_array),
                Arc::new(score_array),
            ],
            None,
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a few sample rows
        let variant0 = result.value(0);
        let obj0 = variant0.as_object().unwrap();
        assert_eq!(obj0.get("id"), Some(Variant::from(0i64)));
        assert_eq!(obj0.get("age"), Some(Variant::from(0i32)));
        assert_eq!(obj0.get("score"), Some(Variant::from(0.0f64)));

        let variant999 = result.value(999);
        let obj999 = variant999.as_object().unwrap();
        assert_eq!(obj999.get("id"), Some(Variant::from(999i64)));
        assert_eq!(obj999.get("age"), Some(Variant::from(99i32))); // 999 % 100 = 99
        assert_eq!(obj999.get("score"), Some(Variant::from(99.9f64)));
    }

    #[test]
    fn test_cast_to_variant_struct_performance_large() {
        // Test with even larger struct and more fields to demonstrate optimization benefits
        let size = 10000;
        let num_fields = 10;

        // Create arrays for many fields
        let mut field_arrays: Vec<ArrayRef> = Vec::new();
        let mut fields = Vec::new();

        for field_idx in 0..num_fields {
            match field_idx % 4 {
                0 => {
                    // Int64 fields
                    let array = Int64Array::from(
                        (0..size)
                            .map(|i| Some(i as i64 + field_idx as i64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int_field_{}", field_idx),
                        DataType::Int64,
                        false,
                    ));
                }
                1 => {
                    // Int32 fields
                    let array = Int32Array::from(
                        (0..size)
                            .map(|i| Some((i % 1000) as i32 + field_idx as i32))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int32_field_{}", field_idx),
                        DataType::Int32,
                        false,
                    ));
                }
                2 => {
                    // Float64 fields
                    let array = Float64Array::from(
                        (0..size)
                            .map(|i| Some(i as f64 * 0.1 + field_idx as f64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("float_field_{}", field_idx),
                        DataType::Float64,
                        false,
                    ));
                }
                _ => {
                    // Binary fields
                    let binary_data: Vec<Option<&[u8]>> = (0..size)
                        .map(|i| {
                            // Use static data to avoid lifetime issues in tests
                            match i % 3 {
                                0 => Some(b"test_data_0" as &[u8]),
                                1 => Some(b"test_data_1" as &[u8]),
                                _ => Some(b"test_data_2" as &[u8]),
                            }
                        })
                        .collect();
                    let array = BinaryArray::from(binary_data);
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("binary_field_{}", field_idx),
                        DataType::Binary,
                        false,
                    ));
                }
            }
        }

        let struct_array = StructArray::new(Fields::from(fields), field_arrays, None);

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a sample of rows
        for sample_idx in [0, size / 4, size / 2, size - 1] {
            let variant = result.value(sample_idx);
            let obj = variant.as_object().unwrap();

            // Should have all fields
            assert_eq!(obj.len(), num_fields);

            // Verify a few field values
            if let Some(int_field_0) = obj.get("int_field_0") {
                assert_eq!(int_field_0, Variant::from(sample_idx as i64));
            }
            if let Some(float_field_2) = obj.get("float_field_2") {
                assert_eq!(float_field_2, Variant::from(sample_idx as f64 * 0.1 + 2.0));
            }
        }
    }

    #[test]
    fn test_cast_to_variant_nested_struct() {
        // Test nested struct: person with location struct
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let x_array = Float64Array::from(vec![Some(40.7), Some(37.8)]);
        let y_array = Float64Array::from(vec![Some(-74.0), Some(-122.4)]);

        // Create location struct
        let location_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]);
        let location_struct = StructArray::new(
            location_fields.clone(),
            vec![Arc::new(x_array), Arc::new(y_array)],
            None,
        );

        // Create person struct containing location
        let person_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("location", DataType::Struct(location_fields), true),
        ]);
        let person_struct = StructArray::new(
            person_fields,
            vec![Arc::new(id_array), Arc::new(location_struct)],
            None,
        );

        let result = cast_to_variant(&person_struct).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));

        let location_variant1 = obj1.get("location").unwrap();
        let location_obj1 = location_variant1.as_object().unwrap();
        assert_eq!(location_obj1.get("x"), Some(Variant::from(40.7f64)));
        assert_eq!(location_obj1.get("y"), Some(Variant::from(-74.0f64)));

        // Check second row
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));

        let location_variant2 = obj2.get("location").unwrap();
        let location_obj2 = location_variant2.as_object().unwrap();
        assert_eq!(location_obj2.get("x"), Some(Variant::from(37.8f64)));
        assert_eq!(location_obj2.get("y"), Some(Variant::from(-122.4f64)));
    }

    #[test]
    fn test_cast_to_variant_map() {
        let keys = vec!["key1", "key2", "key3"];
        let values_data = Int32Array::from(vec![1, 2, 3]);
        let entry_offsets = vec![0, 1, 3];
        let map_array =
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap();

        let result = cast_to_variant(&map_array).unwrap();
        // [{"key1":1}]
        let variant1 = result.value(0);
        assert_eq!(
            variant1.as_object().unwrap().get("key1").unwrap(),
            Variant::from(1)
        );

        // [{"key2":2},{"key3":3}]
        let variant2 = result.value(1);
        assert_eq!(
            variant2.as_object().unwrap().get("key2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant2.as_object().unwrap().get("key3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_map_with_nulls_and_empty() {
        use arrow::array::{Int32Array, MapArray, StringArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, Fields};
        use std::sync::Arc;

        // Create entries struct array
        let keys = StringArray::from(vec!["key1", "key2", "key3"]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let entries_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        // Create offsets for 4 maps: [0..1], [1..1], [1..1], [1..3]
        let offsets = OffsetBuffer::new(vec![0, 1, 1, 1, 3].into());

        // Create null buffer - map at index 2 is NULL
        let null_buffer = Some(NullBuffer::from(vec![true, true, false, true]));

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false,
        ));

        let map_array = MapArray::try_new(map_field, offsets, entries, null_buffer, false).unwrap();

        let result = cast_to_variant(&map_array).unwrap();

        // Map 0: {"key1": 1}
        let variant0 = result.value(0);
        assert_eq!(
            variant0.as_object().unwrap().get("key1").unwrap(),
            Variant::from(1)
        );

        // Map 1: {} (empty, not null)
        let variant1 = result.value(1);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.len(), 0); // Empty object

        // Map 2: null (actual NULL)
        assert!(result.is_null(2));

        // Map 3: {"key2": 2, "key3": 3}
        let variant3 = result.value(3);
        assert_eq!(
            variant3.as_object().unwrap().get("key2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant3.as_object().unwrap().get("key3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_map_with_non_string_keys() {
        let offsets = OffsetBuffer::new(vec![0, 1, 3].into());
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("values", DataType::Int32, false),
        ]);
        let columns = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
        ];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));

        let map_array = MapArray::new(field.clone(), offsets.clone(), entries.clone(), None, false);

        let result = cast_to_variant(&map_array).unwrap();

        let variant1 = result.value(0);
        assert_eq!(
            variant1.as_object().unwrap().get("1").unwrap(),
            Variant::from(1)
        );

        let variant2 = result.value(1);
        assert_eq!(
            variant2.as_object().unwrap().get("2").unwrap(),
            Variant::from(2)
        );
        assert_eq!(
            variant2.as_object().unwrap().get("3").unwrap(),
            Variant::from(3)
        );
    }

    #[test]
    fn test_cast_to_variant_union_sparse() {
        // Create a sparse union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), None, None, None, Some(34), None]);
        let float_array = Float64Array::from(vec![None, Some(3.2), None, Some(32.5), None, None]);
        let string_array = StringArray::from(vec![None, None, Some("hello"), None, None, None]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = UnionFields::from_fields(vec![
            Field::new("int_field", DataType::Int32, false),
            Field::new("float_field", DataType::Float64, false),
            Field::new("string_field", DataType::Utf8, false),
        ]);

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            None, // Sparse union
            children,
        )
        .unwrap();

        run_test(
            Arc::new(union_array),
            vec![
                Some(Variant::Int32(1)),
                Some(Variant::Double(3.2)),
                Some(Variant::from("hello")),
                Some(Variant::Double(32.5)),
                Some(Variant::Int32(34)),
                None,
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_union_dense() {
        // Create a dense union array with mixed types (int, float, string)
        let int_array = Int32Array::from(vec![Some(1), Some(34), None]);
        let float_array = Float64Array::from(vec![3.2, 32.5]);
        let string_array = StringArray::from(vec!["hello"]);
        let type_ids = [0, 1, 2, 1, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 0, 0, 1, 1, 2]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let union_fields = UnionFields::from_fields(vec![
            Field::new("int_field", DataType::Int32, false),
            Field::new("float_field", DataType::Float64, false),
            Field::new("string_field", DataType::Utf8, false),
        ]);

        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(int_array),
            Arc::new(float_array),
            Arc::new(string_array),
        ];

        let union_array = UnionArray::try_new(
            union_fields,
            type_ids,
            Some(offsets), // Dense union
            children,
        )
        .unwrap();

        run_test(
            Arc::new(union_array),
            vec![
                Some(Variant::Int32(1)),
                Some(Variant::Double(3.2)),
                Some(Variant::from("hello")),
                Some(Variant::Double(32.5)),
                Some(Variant::Int32(34)),
                None,
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary() {
        let values = StringArray::from(vec!["apple", "banana", "cherry", "date"]);
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0), Some(3)]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                None,
                Some(Variant::from("cherry")),
                Some(Variant::from("apple")),
                Some(Variant::from("date")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary_with_nulls() {
        // Test dictionary with null values in the values array
        let values = StringArray::from(vec![Some("a"), None, Some("c")]);
        let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("a")),
                None, // key 1 points to null value
                Some(Variant::from("c")),
                Some(Variant::from("a")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded() {
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_value("apple");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("cherry");
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("cherry")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded_with_nulls() {
        use arrow::array::StringRunBuilder;
        use arrow::datatypes::Int32Type;

        // Test run-end encoded array with nulls
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_null();
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_null();
        builder.append_null();
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                None,
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                None,
                None,
            ],
        );
    }

    /// Converts the given `Array` to a `VariantArray` and tests the conversion
    /// against the expected values. It also tests the handling of nulls by
    /// setting one element to null and verifying the output.
    fn run_test_with_options(
        values: ArrayRef,
        expected: Vec<Option<Variant>>,
        options: CastOptions,
    ) {
        let variant_array = cast_to_variant_with_options(&values, &options).unwrap();
        assert_eq!(variant_array.len(), expected.len());
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(!variant_array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(variant_array.value(i), *value, "mismatch at index {i}");
                }
                None => {
                    assert!(variant_array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }

    fn run_test(values: ArrayRef, expected: Vec<Option<Variant>>) {
        run_test_with_options(values, expected, CastOptions::default());
    }

    fn run_test_in_strict_mode(
        values: ArrayRef,
        expected: Result<Vec<Option<Variant>>, ArrowError>,
    ) {
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        match expected {
            Ok(expected) => run_test_with_options(values, expected, options),
            Err(_) => {
                let result = cast_to_variant_with_options(values.as_ref(), &options);
                assert!(result.is_err());
                assert_eq!(
                    result.unwrap_err().to_string(),
                    expected.unwrap_err().to_string()
                );
            }
        }
    }
}

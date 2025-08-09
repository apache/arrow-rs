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

use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    i256, BinaryType, BinaryViewType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    LargeBinaryType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_schema::{ArrowError, DataType};
use half::f16;
use parquet_variant::{Variant, VariantDecimal16, VariantDecimal4, VariantDecimal8};

/// Convert the input array of a specific primitive type to a `VariantArray`
/// row by row
macro_rules! primitive_conversion {
    ($t:ty, $input:expr, $builder:expr) => {{
        let array = $input.as_primitive::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            $builder.append_variant(Variant::from(array.value(i)));
        }
    }};
}

/// Convert the input array to a `VariantArray` row by row, using `method`
/// to downcast the generic array to a specific array type and `cast_fn`
/// to transform each element to a type compatible with Variant
macro_rules! cast_conversion {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

macro_rules! cast_conversion_nongeneric {
    ($method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Convert a decimal value to a `VariantDecimal`
macro_rules! decimal_to_variant_decimal {
    ($v:ident, $scale:expr, $value_type:ty, $variant_type:ty) => {
        if *$scale < 0 {
            // For negative scale, we need to multiply the value by 10^|scale|
            // For example: 123 with scale -2 becomes 12300
            let multiplier = (10 as $value_type).pow((-*$scale) as u32);
            // Check for overflow
            if $v > 0 && $v > <$value_type>::MAX / multiplier {
                return Variant::Null;
            }
            if $v < 0 && $v < <$value_type>::MIN / multiplier {
                return Variant::Null;
            }
            <$variant_type>::try_new($v * multiplier, 0)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        } else {
            <$variant_type>::try_new($v, *$scale as u8)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        }
    };
}

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
/// # use parquet_variant_compute::cast_to_variant::cast_to_variant;
/// // input is an Int64Array, which will be cast to a VariantArray
/// let input = Int64Array::from(vec![Some(1), None, Some(3)]);
/// let result = cast_to_variant(&input).unwrap();
/// assert_eq!(result.len(), 3);
/// assert_eq!(result.value(0), Variant::Int64(1));
/// assert!(result.is_null(1)); // note null, not Variant::Null
/// assert_eq!(result.value(2), Variant::Int64(3));
/// ```
pub fn cast_to_variant(input: &dyn Array) -> Result<VariantArray, ArrowError> {
    let mut builder = VariantArrayBuilder::new(input.len());

    let input_type = input.data_type();
    // todo: handle other types like Boolean, Strings, Date, Timestamp, etc.
    match input_type {
        DataType::Binary => {
            cast_conversion!(BinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::LargeBinary => {
            cast_conversion!(LargeBinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::BinaryView => {
            cast_conversion!(BinaryViewType, as_byte_view, |v| v, input, builder);
        }
        DataType::Int8 => {
            primitive_conversion!(Int8Type, input, builder);
        }
        DataType::Int16 => {
            primitive_conversion!(Int16Type, input, builder);
        }
        DataType::Int32 => {
            primitive_conversion!(Int32Type, input, builder);
        }
        DataType::Int64 => {
            primitive_conversion!(Int64Type, input, builder);
        }
        DataType::UInt8 => {
            primitive_conversion!(UInt8Type, input, builder);
        }
        DataType::UInt16 => {
            primitive_conversion!(UInt16Type, input, builder);
        }
        DataType::UInt32 => {
            primitive_conversion!(UInt32Type, input, builder);
        }
        DataType::UInt64 => {
            primitive_conversion!(UInt64Type, input, builder);
        }
        DataType::Float16 => {
            cast_conversion!(
                Float16Type,
                as_primitive,
                |v: f16| -> f32 { v.into() },
                input,
                builder
            );
        }
        DataType::Float32 => {
            primitive_conversion!(Float32Type, input, builder);
        }
        DataType::Float64 => {
            primitive_conversion!(Float64Type, input, builder);
        }
        DataType::Decimal32(_, scale) => {
            cast_conversion!(
                Decimal32Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i32, VariantDecimal4),
                input,
                builder
            );
        }
        DataType::Decimal64(_, scale) => {
            cast_conversion!(
                Decimal64Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i64, VariantDecimal8),
                input,
                builder
            );
        }
        DataType::Decimal128(_, scale) => {
            cast_conversion!(
                Decimal128Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i128, VariantDecimal16),
                input,
                builder
            );
        }
        DataType::Decimal256(_, scale) => {
            cast_conversion!(
                Decimal256Type,
                as_primitive,
                |v: i256| {
                    // Since `i128::MAX` is larger than the max value of `VariantDecimal16`,
                    // we can safely convert `i256` to `i128` first and process it like `VariantDecimal16`
                    if let Some(v) = v.to_i128() {
                        decimal_to_variant_decimal!(v, scale, i128, VariantDecimal16)
                    } else {
                        Variant::Null
                    }
                },
                input,
                builder
            );
        }
        DataType::FixedSizeBinary(_) => {
            cast_conversion_nongeneric!(as_fixed_size_binary, |v| v, input, builder);
        }
        dt => {
            return Err(ArrowError::CastError(format!(
                "Unsupported data type for casting to Variant: {dt:?}",
            )));
        }
    };
    Ok(builder.build())
}

// TODO do we need a cast_with_options to allow specifying conversion behavior,
// e.g. how to handle overflows, whether to convert to Variant::Null or return
// an error, etc. ?

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array,
        FixedSizeBinaryBuilder, Float16Array, Float32Array, Float64Array, GenericByteBuilder,
        GenericByteViewBuilder, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::{
        DECIMAL128_MAX_PRECISION, DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION,
    };
    use parquet_variant::{Variant, VariantDecimal16};
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
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)),
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
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
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32_negative_scale() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)),
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                None,
                Some(VariantDecimal4::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(123_000, 0).unwrap().into()),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)),
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
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
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64_negative_scale() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)),
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                None,
                Some(VariantDecimal8::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(123_000, 0).unwrap().into()),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)),
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
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
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128_negative_scale() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)),
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)),
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))),
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))),
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
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
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256_negative_scale() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))),
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))),
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    /// Converts the given `Array` to a `VariantArray` and tests the conversion
    /// against the expected values. It also tests the handling of nulls by
    /// setting one element to null and verifying the output.
    fn run_test(values: ArrayRef, expected: Vec<Option<Variant>>) {
        // test without nulls
        let variant_array = cast_to_variant(&values).unwrap();
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
}

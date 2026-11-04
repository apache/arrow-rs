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

use std::sync::Arc;

use arrow_array::{cast::*, types::*, *};
use arrow_buffer::i256;
use arrow_cast::display::FormatOptions;
use arrow_cast::parse::parse_decimal;
use arrow_cast::{
    CastOptions, DecimalCast, can_cast_types, cast, cast_with_options,
    single_decimal_to_float_lossy,
};
use arrow_schema::{ArrowError, DataType};
use half::f16;

#[derive(Clone)]
struct DecimalCastTestConfig {
    input_prec: u8,
    input_scale: i8,
    input_repr: i128,
    output_prec: u8,
    output_scale: i8,
    expected_output_repr: Result<i128, String>, // the error variant can contain a string
                                                // template where the "{}" will be
                                                // replaced with the decimal type name
                                                // (e.g. Decimal128)
}

macro_rules! generate_cast_test_case {
    ($INPUT_ARRAY: expr, $OUTPUT_TYPE_ARRAY: ident, $OUTPUT_TYPE: expr, $OUTPUT_VALUES: expr) => {
        let output = $OUTPUT_TYPE_ARRAY::from($OUTPUT_VALUES).with_data_type($OUTPUT_TYPE.clone());

        // assert cast type
        let input_array_type = $INPUT_ARRAY.data_type();
        assert!(can_cast_types(input_array_type, $OUTPUT_TYPE));
        let result = cast($INPUT_ARRAY, $OUTPUT_TYPE).unwrap();
        assert_eq!($OUTPUT_TYPE, result.data_type());
        assert_eq!(result.as_ref(), &output);

        let cast_option = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let result = cast_with_options($INPUT_ARRAY, $OUTPUT_TYPE, &cast_option).unwrap();
        assert_eq!($OUTPUT_TYPE, result.data_type());
        assert_eq!(result.as_ref(), &output);
    };
}

fn run_decimal_cast_test_case<I, O>(t: DecimalCastTestConfig)
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast,
    O::Native: DecimalCast,
{
    let array = vec![I::Native::from_decimal(t.input_repr)];
    let array = array
        .into_iter()
        .collect::<PrimitiveArray<I>>()
        .with_precision_and_scale(t.input_prec, t.input_scale)
        .unwrap();
    let input_type = array.data_type();
    let output_type = O::TYPE_CONSTRUCTOR(t.output_prec, t.output_scale);
    assert!(can_cast_types(input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options);

    match t.expected_output_repr {
        Ok(v) => {
            let expected_array = vec![O::Native::from_decimal(v)];
            let expected_array = expected_array
                .into_iter()
                .collect::<PrimitiveArray<O>>()
                .with_precision_and_scale(t.output_prec, t.output_scale)
                .unwrap();
            assert_eq!(*result.unwrap(), expected_array);
        }
        Err(expected_output_message_template) => {
            assert!(result.is_err());
            let expected_error_message = expected_output_message_template.replace("{}", O::PREFIX);
            assert_eq!(result.unwrap_err().to_string(), expected_error_message);
        }
    }
}

fn create_decimal32_array(
    array: Vec<Option<i32>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal32Array, ArrowError> {
    array
        .into_iter()
        .collect::<Decimal32Array>()
        .with_precision_and_scale(precision, scale)
}

fn create_decimal64_array(
    array: Vec<Option<i64>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal64Array, ArrowError> {
    array
        .into_iter()
        .collect::<Decimal64Array>()
        .with_precision_and_scale(precision, scale)
}

fn create_decimal128_array(
    array: Vec<Option<i128>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, ArrowError> {
    array
        .into_iter()
        .collect::<Decimal128Array>()
        .with_precision_and_scale(precision, scale)
}

fn create_decimal256_array(
    array: Vec<Option<i256>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal256Array, ArrowError> {
    array
        .into_iter()
        .collect::<Decimal256Array>()
        .with_precision_and_scale(precision, scale)
}

#[test]
#[should_panic(
    expected = "Cannot cast to Decimal128(20, 3). Overflowing on 57896044618658097711785492504343953926634992332820282019728792003956564819967"
)]
fn test_cast_decimal_to_decimal_round_with_error() {
    // decimal256 to decimal128 overflow
    let array = vec![
        Some(i256::from_i128(1123454)),
        Some(i256::from_i128(2123456)),
        Some(i256::from_i128(-3123453)),
        Some(i256::from_i128(-3123456)),
        None,
        Some(i256::MAX),
        Some(i256::MIN),
    ];
    let input_decimal_array = create_decimal256_array(array, 76, 4).unwrap();
    let array = Arc::new(input_decimal_array) as ArrayRef;
    let input_type = DataType::Decimal256(76, 4);
    let output_type = DataType::Decimal128(20, 3);
    assert!(can_cast_types(&input_type, &output_type));
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(112345_i128),
            Some(212346_i128),
            Some(-312345_i128),
            Some(-312346_i128),
            None,
            None,
            None,
        ]
    );
}

#[test]
fn test_cast_decimal_to_decimal_round() {
    let array = vec![
        Some(1123454),
        Some(2123456),
        Some(-3123453),
        Some(-3123456),
        None,
    ];
    let array = create_decimal128_array(array, 20, 4).unwrap();
    // decimal128 to decimal128
    let input_type = DataType::Decimal128(20, 4);
    let output_type = DataType::Decimal128(20, 3);
    assert!(can_cast_types(&input_type, &output_type));
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(112345_i128),
            Some(212346_i128),
            Some(-312345_i128),
            Some(-312346_i128),
            None
        ]
    );

    // decimal128 to decimal256
    let input_type = DataType::Decimal128(20, 4);
    let output_type = DataType::Decimal256(20, 3);
    assert!(can_cast_types(&input_type, &output_type));
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(112345_i128)),
            Some(i256::from_i128(212346_i128)),
            Some(i256::from_i128(-312345_i128)),
            Some(i256::from_i128(-312346_i128)),
            None
        ]
    );

    // decimal256
    let array = vec![
        Some(i256::from_i128(1123454)),
        Some(i256::from_i128(2123456)),
        Some(i256::from_i128(-3123453)),
        Some(i256::from_i128(-3123456)),
        None,
    ];
    let array = create_decimal256_array(array, 20, 4).unwrap();

    // decimal256 to decimal256
    let input_type = DataType::Decimal256(20, 4);
    let output_type = DataType::Decimal256(20, 3);
    assert!(can_cast_types(&input_type, &output_type));
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(112345_i128)),
            Some(i256::from_i128(212346_i128)),
            Some(i256::from_i128(-312345_i128)),
            Some(i256::from_i128(-312346_i128)),
            None
        ]
    );
    // decimal256 to decimal128
    let input_type = DataType::Decimal256(20, 4);
    let output_type = DataType::Decimal128(20, 3);
    assert!(can_cast_types(&input_type, &output_type));
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(112345_i128),
            Some(212346_i128),
            Some(-312345_i128),
            Some(-312346_i128),
            None
        ]
    );
}

#[test]
fn test_cast_decimal32_to_decimal32() {
    // test changing precision
    let input_type = DataType::Decimal32(9, 3);
    let output_type = DataType::Decimal32(9, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal32_array(array, 9, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal32Array,
        &output_type,
        vec![
            Some(11234560_i32),
            Some(21234560_i32),
            Some(31234560_i32),
            None
        ]
    );
    // negative test
    let array = vec![Some(123456), None];
    let array = create_decimal32_array(array, 9, 0).unwrap();
    let result_safe = cast(&array, &DataType::Decimal32(2, 2));
    assert!(result_safe.is_ok());
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let result_unsafe = cast_with_options(&array, &DataType::Decimal32(2, 2), &options);
    assert_eq!(
        "Invalid argument error: 123456.00 is too large to store in a Decimal32 of precision 2. Max is 0.99",
        result_unsafe.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal64_to_decimal64() {
    // test changing precision
    let input_type = DataType::Decimal64(17, 3);
    let output_type = DataType::Decimal64(17, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal64_array(array, 17, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal64Array,
        &output_type,
        vec![
            Some(11234560_i64),
            Some(21234560_i64),
            Some(31234560_i64),
            None
        ]
    );
    // negative test
    let array = vec![Some(123456), None];
    let array = create_decimal64_array(array, 9, 0).unwrap();
    let result_safe = cast(&array, &DataType::Decimal64(2, 2));
    assert!(result_safe.is_ok());
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let result_unsafe = cast_with_options(&array, &DataType::Decimal64(2, 2), &options);
    assert_eq!(
        "Invalid argument error: 123456.00 is too large to store in a Decimal64 of precision 2. Max is 0.99",
        result_unsafe.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal128_to_decimal128() {
    // test changing precision
    let input_type = DataType::Decimal128(20, 3);
    let output_type = DataType::Decimal128(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal128_array(array, 20, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(11234560_i128),
            Some(21234560_i128),
            Some(31234560_i128),
            None
        ]
    );
    // negative test
    let array = vec![Some(123456), None];
    let array = create_decimal128_array(array, 10, 0).unwrap();
    let result_safe = cast(&array, &DataType::Decimal128(2, 2));
    assert!(result_safe.is_ok());
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let result_unsafe = cast_with_options(&array, &DataType::Decimal128(2, 2), &options);
    assert_eq!(
        "Invalid argument error: 123456.00 is too large to store in a Decimal128 of precision 2. Max is 0.99",
        result_unsafe.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal32_to_decimal32_dict() {
    let p = 9;
    let s = 3;
    let input_type = DataType::Decimal32(p, s);
    let output_type = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Decimal32(p, s)),
    );
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal32_array(array, p, s).unwrap();
    let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
    assert_eq!(cast_array.data_type(), &output_type);
}

#[test]
fn test_cast_decimal64_to_decimal64_dict() {
    let p = 15;
    let s = 3;
    let input_type = DataType::Decimal64(p, s);
    let output_type = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Decimal64(p, s)),
    );
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal64_array(array, p, s).unwrap();
    let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
    assert_eq!(cast_array.data_type(), &output_type);
}

#[test]
fn test_cast_decimal128_to_decimal128_dict() {
    let p = 20;
    let s = 3;
    let input_type = DataType::Decimal128(p, s);
    let output_type = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Decimal128(p, s)),
    );
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal128_array(array, p, s).unwrap();
    let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
    assert_eq!(cast_array.data_type(), &output_type);
}

#[test]
fn test_cast_decimal256_to_decimal256_dict() {
    let p = 20;
    let s = 3;
    let input_type = DataType::Decimal256(p, s);
    let output_type = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Decimal256(p, s)),
    );
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal128_array(array, p, s).unwrap();
    let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
    assert_eq!(cast_array.data_type(), &output_type);
}

#[test]
fn test_cast_decimal32_to_decimal32_overflow() {
    let input_type = DataType::Decimal32(9, 3);
    let output_type = DataType::Decimal32(9, 9);
    assert!(can_cast_types(&input_type, &output_type));

    let array = vec![Some(i32::MAX)];
    let array = create_decimal32_array(array, 9, 3).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal32(9, 9). Overflowing on 2147483647",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal32_to_decimal32_large_scale_reduction() {
    let array = vec![Some(-999999999), Some(0), Some(999999999), None];
    let array = create_decimal32_array(array, 9, 3).unwrap();

    // Divide out all digits of precision -- rounding could still produce +/- 1
    let output_type = DataType::Decimal32(9, -6);
    assert!(can_cast_types(array.data_type(), &output_type));
    generate_cast_test_case!(
        &array,
        Decimal32Array,
        &output_type,
        vec![Some(-1), Some(0), Some(1), None]
    );

    // Divide out more digits than we have precision -- all-zero result
    let output_type = DataType::Decimal32(9, -7);
    assert!(can_cast_types(array.data_type(), &output_type));
    generate_cast_test_case!(
        &array,
        Decimal32Array,
        &output_type,
        vec![Some(0), Some(0), Some(0), None]
    );
}

#[test]
fn test_cast_decimal64_to_decimal64_overflow() {
    let input_type = DataType::Decimal64(18, 3);
    let output_type = DataType::Decimal64(18, 18);
    assert!(can_cast_types(&input_type, &output_type));

    let array = vec![Some(i64::MAX)];
    let array = create_decimal64_array(array, 18, 3).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal64(18, 18). Overflowing on 9223372036854775807",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal64_to_decimal64_large_scale_reduction() {
    let array = vec![
        Some(-999999999999999999),
        Some(0),
        Some(999999999999999999),
        None,
    ];
    let array = create_decimal64_array(array, 18, 3).unwrap();

    // Divide out all digits of precision -- rounding could still produce +/- 1
    let output_type = DataType::Decimal64(18, -15);
    assert!(can_cast_types(array.data_type(), &output_type));
    generate_cast_test_case!(
        &array,
        Decimal64Array,
        &output_type,
        vec![Some(-1), Some(0), Some(1), None]
    );

    // Divide out more digits than we have precision -- all-zero result
    let output_type = DataType::Decimal64(18, -16);
    assert!(can_cast_types(array.data_type(), &output_type));
    generate_cast_test_case!(
        &array,
        Decimal64Array,
        &output_type,
        vec![Some(0), Some(0), Some(0), None]
    );
}

#[test]
fn test_cast_floating_to_decimals() {
    for output_type in [
        DataType::Decimal32(9, 3),
        DataType::Decimal64(9, 3),
        DataType::Decimal128(9, 3),
        DataType::Decimal256(9, 3),
    ] {
        let input_type = DataType::Float64;
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(1.1_f64)];
        let array = PrimitiveArray::<Float64Type>::from_iter(array);
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(
            result.is_ok(),
            "Failed to cast to {output_type} with: {}",
            result.unwrap_err()
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_float16_to_decimals() {
    let array = Float16Array::from(vec![
        Some(f16::from_f32(1.25)),
        Some(f16::from_f32(-2.5)),
        Some(f16::from_f32(1.125)),
        Some(f16::from_f32(-1.125)),
        Some(f16::from_f32(0.0)),
        None,
    ]);

    generate_cast_test_case!(
        &array,
        Decimal32Array,
        &DataType::Decimal32(9, 2),
        vec![
            Some(125_i32),
            Some(-250_i32),
            Some(113_i32),
            Some(-113_i32),
            Some(0_i32),
            None
        ]
    );
    generate_cast_test_case!(
        &array,
        Decimal64Array,
        &DataType::Decimal64(18, 2),
        vec![
            Some(125_i64),
            Some(-250_i64),
            Some(113_i64),
            Some(-113_i64),
            Some(0_i64),
            None
        ]
    );
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &DataType::Decimal128(38, 2),
        vec![
            Some(125_i128),
            Some(-250_i128),
            Some(113_i128),
            Some(-113_i128),
            Some(0_i128),
            None
        ]
    );
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &DataType::Decimal256(76, 2),
        vec![
            Some(i256::from_i128(125_i128)),
            Some(i256::from_i128(-250_i128)),
            Some(i256::from_i128(113_i128)),
            Some(i256::from_i128(-113_i128)),
            Some(i256::from_i128(0_i128)),
            None
        ]
    );

    let array = Float16Array::from(vec![
        Some(f16::from_f32(1250.0)),
        Some(f16::from_f32(-1250.0)),
        Some(f16::from_f32(1249.0)),
        None,
    ]);
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &DataType::Decimal128(5, -2),
        vec![Some(13_i128), Some(-13_i128), Some(12_i128), None]
    );
}

#[test]
fn test_cast_decimal128_to_decimal128_overflow() {
    let input_type = DataType::Decimal128(38, 3);
    let output_type = DataType::Decimal128(38, 38);
    assert!(can_cast_types(&input_type, &output_type));

    let array = vec![Some(i128::MAX)];
    let array = create_decimal128_array(array, 38, 3).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 170141183460469231731687303715884105727",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal128_to_decimal256_overflow() {
    let input_type = DataType::Decimal128(38, 3);
    let output_type = DataType::Decimal256(76, 76);
    assert!(can_cast_types(&input_type, &output_type));

    let array = vec![Some(i128::MAX)];
    let array = create_decimal128_array(array, 38, 3).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal256(76, 76). Overflowing on 170141183460469231731687303715884105727",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal32_to_decimal256() {
    let input_type = DataType::Decimal32(8, 3);
    let output_type = DataType::Decimal256(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal32_array(array, 8, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(11234560_i128)),
            Some(i256::from_i128(21234560_i128)),
            Some(i256::from_i128(31234560_i128)),
            None
        ]
    );
}
#[test]
fn test_cast_decimal64_to_decimal256() {
    let input_type = DataType::Decimal64(12, 3);
    let output_type = DataType::Decimal256(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal64_array(array, 12, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(11234560_i128)),
            Some(i256::from_i128(21234560_i128)),
            Some(i256::from_i128(31234560_i128)),
            None
        ]
    );
}
#[test]
fn test_cast_decimal128_to_decimal256() {
    let input_type = DataType::Decimal128(20, 3);
    let output_type = DataType::Decimal256(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
    let array = create_decimal128_array(array, 20, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(11234560_i128)),
            Some(i256::from_i128(21234560_i128)),
            Some(i256::from_i128(31234560_i128)),
            None
        ]
    );
}

#[test]
fn test_cast_decimal256_to_decimal128_overflow() {
    let input_type = DataType::Decimal256(76, 5);
    let output_type = DataType::Decimal128(38, 7);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(i256::from_i128(i128::MAX))];
    let array = create_decimal256_array(array, 76, 5).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal128(38, 7). Overflowing on 170141183460469231731687303715884105727",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal256_to_decimal256_overflow() {
    let input_type = DataType::Decimal256(76, 5);
    let output_type = DataType::Decimal256(76, 55);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(i256::from_i128(i128::MAX))];
    let array = create_decimal256_array(array, 76, 5).unwrap();
    let result = cast_with_options(
        &array,
        &output_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast to Decimal256(76, 55). Overflowing on 170141183460469231731687303715884105727",
        result.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_decimal256_to_decimal128() {
    let input_type = DataType::Decimal256(20, 3);
    let output_type = DataType::Decimal128(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![
        Some(i256::from_i128(1123456)),
        Some(i256::from_i128(2123456)),
        Some(i256::from_i128(3123456)),
        None,
    ];
    let array = create_decimal256_array(array, 20, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(11234560_i128),
            Some(21234560_i128),
            Some(31234560_i128),
            None
        ]
    );
}

#[test]
fn test_cast_decimal256_to_decimal256() {
    let input_type = DataType::Decimal256(20, 3);
    let output_type = DataType::Decimal256(20, 4);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![
        Some(i256::from_i128(1123456)),
        Some(i256::from_i128(2123456)),
        Some(i256::from_i128(3123456)),
        None,
    ];
    let array = create_decimal256_array(array, 20, 3).unwrap();
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(11234560_i128)),
            Some(i256::from_i128(21234560_i128)),
            Some(i256::from_i128(31234560_i128)),
            None
        ]
    );
}

fn generate_decimal_to_numeric_cast_test_case<T>(array: &PrimitiveArray<T>)
where
    T: ArrowPrimitiveType + DecimalType,
{
    // u8
    generate_cast_test_case!(
        array,
        UInt8Array,
        &DataType::UInt8,
        vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
    );
    // u16
    generate_cast_test_case!(
        array,
        UInt16Array,
        &DataType::UInt16,
        vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
    );
    // u32
    generate_cast_test_case!(
        array,
        UInt32Array,
        &DataType::UInt32,
        vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
    );
    // u64
    generate_cast_test_case!(
        array,
        UInt64Array,
        &DataType::UInt64,
        vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
    );
    // i8
    generate_cast_test_case!(
        array,
        Int8Array,
        &DataType::Int8,
        vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
    );
    // i16
    generate_cast_test_case!(
        array,
        Int16Array,
        &DataType::Int16,
        vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
    );
    // i32
    generate_cast_test_case!(
        array,
        Int32Array,
        &DataType::Int32,
        vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
    );
    // i64
    generate_cast_test_case!(
        array,
        Int64Array,
        &DataType::Int64,
        vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
    );
    // f16
    generate_cast_test_case!(
        array,
        Float16Array,
        &DataType::Float16,
        vec![
            Some(f16::from_f32(1.25)),
            Some(f16::from_f32(2.25)),
            Some(f16::from_f32(3.25)),
            None,
            Some(f16::from_f32(5.25))
        ]
    );
    // f32
    generate_cast_test_case!(
        array,
        Float32Array,
        &DataType::Float32,
        vec![
            Some(1.25_f32),
            Some(2.25_f32),
            Some(3.25_f32),
            None,
            Some(5.25_f32)
        ]
    );
    // f64
    generate_cast_test_case!(
        array,
        Float64Array,
        &DataType::Float64,
        vec![
            Some(1.25_f64),
            Some(2.25_f64),
            Some(3.25_f64),
            None,
            Some(5.25_f64)
        ]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_decimal32_to_numeric() {
    let value_array: Vec<Option<i32>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal32_array(value_array, 8, 2).unwrap();

    generate_decimal_to_numeric_cast_test_case(&array);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_decimal64_to_numeric() {
    let value_array: Vec<Option<i64>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal64_array(value_array, 8, 2).unwrap();

    generate_decimal_to_numeric_cast_test_case(&array);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_decimal128_to_numeric() {
    let value_array: Vec<Option<i128>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal128_array(value_array, 38, 2).unwrap();

    generate_decimal_to_numeric_cast_test_case(&array);

    // overflow test: out of range of max u8
    let value_array: Vec<Option<i128>> = vec![Some(51300)];
    let array = create_decimal128_array(value_array, 38, 2).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::UInt8,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: value of 513 is out of range UInt8".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::UInt8,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    // overflow test: out of range of max i8
    let value_array: Vec<Option<i128>> = vec![Some(24400)];
    let array = create_decimal128_array(value_array, 38, 2).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: value of 244 is out of range Int8".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    // loss the precision: convert decimal to f32、f64
    // f32
    // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
    let value_array: Vec<Option<i128>> = vec![
        Some(125),
        Some(225),
        Some(325),
        None,
        Some(525),
        Some(112345678),
        Some(112345679),
    ];
    let array = create_decimal128_array(value_array, 38, 2).unwrap();
    generate_cast_test_case!(
        &array,
        Float32Array,
        &DataType::Float32,
        vec![
            Some(1.25_f32),
            Some(2.25_f32),
            Some(3.25_f32),
            None,
            Some(5.25_f32),
            Some(1_123_456.7_f32),
            Some(1_123_456.7_f32)
        ]
    );

    // f64
    // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
    let value_array: Vec<Option<i128>> = vec![
        Some(125),
        Some(225),
        Some(325),
        None,
        Some(525),
        Some(112345678901234568),
        Some(112345678901234560),
    ];
    let array = create_decimal128_array(value_array, 38, 2).unwrap();
    generate_cast_test_case!(
        &array,
        Float64Array,
        &DataType::Float64,
        vec![
            Some(1.25_f64),
            Some(2.25_f64),
            Some(3.25_f64),
            None,
            Some(5.25_f64),
            Some(1_123_456_789_012_345.6_f64),
            Some(1_123_456_789_012_345.6_f64),
        ]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_decimal256_to_numeric() {
    let value_array: Vec<Option<i256>> = vec![
        Some(i256::from_i128(125)),
        Some(i256::from_i128(225)),
        Some(i256::from_i128(325)),
        None,
        Some(i256::from_i128(525)),
    ];
    let array = create_decimal256_array(value_array, 38, 2).unwrap();
    // u8
    generate_cast_test_case!(
        &array,
        UInt8Array,
        &DataType::UInt8,
        vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
    );
    // u16
    generate_cast_test_case!(
        &array,
        UInt16Array,
        &DataType::UInt16,
        vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
    );
    // u32
    generate_cast_test_case!(
        &array,
        UInt32Array,
        &DataType::UInt32,
        vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
    );
    // u64
    generate_cast_test_case!(
        &array,
        UInt64Array,
        &DataType::UInt64,
        vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
    );
    // i8
    generate_cast_test_case!(
        &array,
        Int8Array,
        &DataType::Int8,
        vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
    );
    // i16
    generate_cast_test_case!(
        &array,
        Int16Array,
        &DataType::Int16,
        vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
    );
    // i32
    generate_cast_test_case!(
        &array,
        Int32Array,
        &DataType::Int32,
        vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
    );
    // i64
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
    );
    // f16
    generate_cast_test_case!(
        &array,
        Float16Array,
        &DataType::Float16,
        vec![
            Some(f16::from_f32(1.25)),
            Some(f16::from_f32(2.25)),
            Some(f16::from_f32(3.25)),
            None,
            Some(f16::from_f32(5.25))
        ]
    );
    // f32
    generate_cast_test_case!(
        &array,
        Float32Array,
        &DataType::Float32,
        vec![
            Some(1.25_f32),
            Some(2.25_f32),
            Some(3.25_f32),
            None,
            Some(5.25_f32)
        ]
    );
    // f64
    generate_cast_test_case!(
        &array,
        Float64Array,
        &DataType::Float64,
        vec![
            Some(1.25_f64),
            Some(2.25_f64),
            Some(3.25_f64),
            None,
            Some(5.25_f64)
        ]
    );

    // overflow test: out of range of max i8
    let value_array: Vec<Option<i256>> = vec![Some(i256::from_i128(24400))];
    let array = create_decimal256_array(value_array, 38, 2).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: value of 244 is out of range Int8".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    // overflow test: values whose low 64 bits fit the target type
    // https://github.com/apache/arrow-rs/issues/10855
    let value_array: Vec<Option<i256>> = vec![Some(i256::from_i128((1i128 << 64) + 5))];
    let array = create_decimal256_array(value_array, 76, 0).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::Int64,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: value of 18446744073709551621 is out of range Int64".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::Int64,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    // loss the precision: convert decimal to f32、f64
    // f32
    // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
    let value_array: Vec<Option<i256>> = vec![
        Some(i256::from_i128(125)),
        Some(i256::from_i128(225)),
        Some(i256::from_i128(325)),
        None,
        Some(i256::from_i128(525)),
        Some(i256::from_i128(112345678)),
        Some(i256::from_i128(112345679)),
    ];
    let array = create_decimal256_array(value_array, 76, 2).unwrap();
    generate_cast_test_case!(
        &array,
        Float32Array,
        &DataType::Float32,
        vec![
            Some(1.25_f32),
            Some(2.25_f32),
            Some(3.25_f32),
            None,
            Some(5.25_f32),
            Some(1_123_456.7_f32),
            Some(1_123_456.7_f32)
        ]
    );

    // f64
    // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
    let value_array: Vec<Option<i256>> = vec![
        Some(i256::from_i128(125)),
        Some(i256::from_i128(225)),
        Some(i256::from_i128(325)),
        None,
        Some(i256::from_i128(525)),
        Some(i256::from_i128(112345678901234568)),
        Some(i256::from_i128(112345678901234560)),
    ];
    let array = create_decimal256_array(value_array, 76, 2).unwrap();
    generate_cast_test_case!(
        &array,
        Float64Array,
        &DataType::Float64,
        vec![
            Some(1.25_f64),
            Some(2.25_f64),
            Some(3.25_f64),
            None,
            Some(5.25_f64),
            Some(1_123_456_789_012_345.6_f64),
            Some(1_123_456_789_012_345.6_f64),
        ]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_decimal128_to_float16_overflow() {
    let array = create_decimal128_array(
        vec![
            Some(6_550_400_i128),
            Some(100_000_000_i128),
            Some(-100_000_000_i128),
            None,
        ],
        10,
        2,
    )
    .unwrap();

    generate_cast_test_case!(
        &array,
        Float16Array,
        &DataType::Float16,
        vec![
            Some(f16::from_f64(65504.0)),
            Some(f16::INFINITY),
            Some(f16::NEG_INFINITY),
            None
        ]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_decimal256_to_float16_overflow() {
    let array = create_decimal256_array(
        vec![
            Some(i256::from_i128(6_550_400_i128)),
            Some(i256::from_i128(100_000_000_i128)),
            Some(i256::from_i128(-100_000_000_i128)),
            None,
        ],
        10,
        2,
    )
    .unwrap();

    generate_cast_test_case!(
        &array,
        Float16Array,
        &DataType::Float16,
        vec![
            Some(f16::from_f64(65504.0)),
            Some(f16::INFINITY),
            Some(f16::NEG_INFINITY),
            None
        ]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_decimal_to_numeric_negative_scale() {
    let value_array: Vec<Option<i256>> = vec![
        Some(i256::from_i128(125)),
        Some(i256::from_i128(225)),
        Some(i256::from_i128(325)),
        None,
        Some(i256::from_i128(525)),
    ];
    let array = create_decimal256_array(value_array, 38, -1).unwrap();

    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![Some(1_250), Some(2_250), Some(3_250), None, Some(5_250)]
    );

    let value_array: Vec<Option<i128>> = vec![Some(12), Some(-12), None];
    let array = create_decimal128_array(value_array, 10, -2).unwrap();
    generate_cast_test_case!(
        &array,
        Float16Array,
        &DataType::Float16,
        vec![
            Some(f16::from_f32(1200.0)),
            Some(f16::from_f32(-1200.0)),
            None
        ]
    );

    let value_array: Vec<Option<i32>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal32_array(value_array, 8, -2).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![Some(12_500), Some(22_500), Some(32_500), None, Some(52_500)]
    );

    let value_array: Vec<Option<i32>> = vec![Some(2), Some(1), None];
    let array = create_decimal32_array(value_array, 9, -9).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![Some(2_000_000_000), Some(1_000_000_000), None]
    );

    let value_array: Vec<Option<i64>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal64_array(value_array, 18, -3).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![
            Some(125_000),
            Some(225_000),
            Some(325_000),
            None,
            Some(525_000)
        ]
    );

    let value_array: Vec<Option<i64>> = vec![Some(12), Some(34), None];
    let array = create_decimal64_array(value_array, 18, -10).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![Some(120_000_000_000), Some(340_000_000_000), None]
    );

    let value_array: Vec<Option<i128>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
    let array = create_decimal128_array(value_array, 38, -4).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![
            Some(1_250_000),
            Some(2_250_000),
            Some(3_250_000),
            None,
            Some(5_250_000)
        ]
    );

    let value_array: Vec<Option<i128>> = vec![Some(9), Some(1), None];
    let array = create_decimal128_array(value_array, 38, -18).unwrap();
    generate_cast_test_case!(
        &array,
        Int64Array,
        &DataType::Int64,
        vec![
            Some(9_000_000_000_000_000_000),
            Some(1_000_000_000_000_000_000),
            None
        ]
    );

    let array = create_decimal32_array(vec![Some(999_999_999)], 9, -1).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::Int64,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Arithmetic overflow: Overflow happened on: 999999999 * 10".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::Int64,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let array = create_decimal64_array(vec![Some(13)], 18, -1).unwrap();
    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: value of 130 is out of range Int8".to_string(),
        casted_array.unwrap_err().to_string()
    );

    let casted_array = cast_with_options(
        &array,
        &DataType::Int8,
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));
}

#[test]
fn test_cast_numeric_to_decimal128() {
    let decimal_type = DataType::Decimal128(38, 6);
    // u8, u16, u32, u64
    let input_arrays = vec![
        Arc::new(UInt8Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u8
        Arc::new(UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u16
        Arc::new(UInt32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u32
        Arc::new(UInt64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u64
    ];

    for array in input_arrays {
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1000000_i128),
                Some(2000000_i128),
                Some(3000000_i128),
                None,
                Some(5000000_i128)
            ]
        );
    }

    // i8, i16, i32, i64
    let input_arrays = vec![
        Arc::new(Int8Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i8
        Arc::new(Int16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i16
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i32
        Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i64
    ];
    for array in input_arrays {
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1000000_i128),
                Some(2000000_i128),
                Some(3000000_i128),
                None,
                Some(5000000_i128)
            ]
        );
    }

    // test u8 to decimal type with overflow the result type
    // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
    let array = UInt8Array::from(vec![1, 2, 3, 4, 100]);
    let casted_array = cast(&array, &DataType::Decimal128(3, 1));
    assert!(casted_array.is_ok());
    let array = casted_array.unwrap();
    let array: &Decimal128Array = array.as_primitive();
    assert!(array.is_null(4));

    // test i8 to decimal type with overflow the result type
    // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
    let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
    let casted_array = cast(&array, &DataType::Decimal128(3, 1));
    assert!(casted_array.is_ok());
    let array = casted_array.unwrap();
    let array: &Decimal128Array = array.as_primitive();
    assert!(array.is_null(4));

    // test f32 to decimal type
    let array = Float32Array::from(vec![
        Some(1.1),
        Some(2.2),
        Some(4.4),
        None,
        Some(1.123_456_4), // round down
        Some(1.123_456_7), // round up
    ]);
    let array = Arc::new(array) as ArrayRef;
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &decimal_type,
        vec![
            Some(1100000_i128),
            Some(2200000_i128),
            Some(4400000_i128),
            None,
            Some(1123456_i128), // round down
            Some(1123457_i128), // round up
        ]
    );

    // test f64 to decimal type
    let array = Float64Array::from(vec![
        Some(1.1),
        Some(2.2),
        Some(4.4),
        None,
        Some(1.123_456_489_123_4),     // round up
        Some(1.123_456_789_123_4),     // round up
        Some(1.123_456_489_012_345_6), // round down
        Some(1.123_456_789_012_345_6), // round up
    ]);
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &decimal_type,
        vec![
            Some(1100000_i128),
            Some(2200000_i128),
            Some(4400000_i128),
            None,
            Some(1123456_i128), // round down
            Some(1123457_i128), // round up
            Some(1123456_i128), // round down
            Some(1123457_i128), // round up
        ]
    );
}

#[test]
fn test_cast_numeric_to_decimal256() {
    let decimal_type = DataType::Decimal256(76, 6);
    // u8, u16, u32, u64
    let input_arrays = vec![
        Arc::new(UInt8Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u8
        Arc::new(UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u16
        Arc::new(UInt32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u32
        Arc::new(UInt64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // u64
    ];

    for array in input_arrays {
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1000000_i128)),
                Some(i256::from_i128(2000000_i128)),
                Some(i256::from_i128(3000000_i128)),
                None,
                Some(i256::from_i128(5000000_i128))
            ]
        );
    }

    // i8, i16, i32, i64
    let input_arrays = vec![
        Arc::new(Int8Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i8
        Arc::new(Int16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i16
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i32
        Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef, // i64
    ];
    for array in input_arrays {
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1000000_i128)),
                Some(i256::from_i128(2000000_i128)),
                Some(i256::from_i128(3000000_i128)),
                None,
                Some(i256::from_i128(5000000_i128))
            ]
        );
    }

    // test i8 to decimal type with overflow the result type
    // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
    let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast(&array, &DataType::Decimal256(3, 1));
    assert!(casted_array.is_ok());
    let array = casted_array.unwrap();
    let array: &Decimal256Array = array.as_primitive();
    assert!(array.is_null(4));

    // test f32 to decimal type
    let array = Float32Array::from(vec![
        Some(1.1),
        Some(2.2),
        Some(4.4),
        None,
        Some(1.123_456_4), // round down
        Some(1.123_456_7), // round up
    ]);
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &decimal_type,
        vec![
            Some(i256::from_i128(1100000_i128)),
            Some(i256::from_i128(2200000_i128)),
            Some(i256::from_i128(4400000_i128)),
            None,
            Some(i256::from_i128(1123456_i128)), // round down
            Some(i256::from_i128(1123457_i128)), // round up
        ]
    );

    // test f64 to decimal type
    let array = Float64Array::from(vec![
        Some(1.1),
        Some(2.2),
        Some(4.4),
        None,
        Some(1.123_456_489_123_4),     // round down
        Some(1.123_456_789_123_4),     // round up
        Some(1.123_456_489_012_345_6), // round down
        Some(1.123_456_789_012_345_6), // round up
    ]);
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &decimal_type,
        vec![
            Some(i256::from_i128(1100000_i128)),
            Some(i256::from_i128(2200000_i128)),
            Some(i256::from_i128(4400000_i128)),
            None,
            Some(i256::from_i128(1123456_i128)), // round down
            Some(i256::from_i128(1123457_i128)), // round up
            Some(i256::from_i128(1123456_i128)), // round down
            Some(i256::from_i128(1123457_i128)), // round up
        ]
    );
}

#[test]
fn test_cast_utf8view_to_decimal128() {
    let array = StringViewArray::from(vec![None, Some("4"), Some("5.6"), Some("7.89")]);
    let arr = Arc::new(array) as ArrayRef;
    generate_cast_test_case!(
        &arr,
        Decimal128Array,
        &DataType::Decimal128(4, 2),
        vec![None, Some(400_i128), Some(560_i128), Some(789_i128)]
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_cast_f64_to_decimal128() {
    // to reproduce https://github.com/apache/arrow-rs/issues/2997

    let decimal_type = DataType::Decimal128(18, 2);
    let array = Float64Array::from(vec![
        Some(0.0699999999),
        Some(0.0659999999),
        Some(0.0650000000),
        Some(0.0649999999),
    ]);
    let array = Arc::new(array) as ArrayRef;
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &decimal_type,
        vec![
            Some(7_i128), // round up
            Some(7_i128), // round up
            Some(7_i128), // round up
            Some(6_i128), // round down
        ]
    );

    let decimal_type = DataType::Decimal128(18, 3);
    let array = Float64Array::from(vec![
        Some(0.0699999999),
        Some(0.0659999999),
        Some(0.0650000000),
        Some(0.0649999999),
    ]);
    let array = Arc::new(array) as ArrayRef;
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &decimal_type,
        vec![
            Some(70_i128), // round up
            Some(66_i128), // round up
            Some(65_i128), // round down
            Some(65_i128), // round up
        ]
    );
}

/// Casts that cannot overflow, whether the source type or the data decides,
/// must match the checked conversion at the extremes, and a value beyond the
/// bound must still become null or an error.
#[test]
fn test_cast_integer_to_decimal_bounds() {
    // Every i32 has at most ten digits, so the type decides.
    let array = Int32Array::from(vec![Some(i32::MIN), None, Some(-1), Some(i32::MAX)]);
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &DataType::Decimal128(12, 2),
        vec![
            Some(i32::MIN as i128 * 100),
            None,
            Some(-100),
            Some(i32::MAX as i128 * 100)
        ]
    );
    // An i64 can exceed sixteen digits, so the data decides.
    let bound = 9_999_999_999_999_999_i64;
    let array = Int64Array::from(vec![Some(-bound), None, Some(bound)]);
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &DataType::Decimal128(18, 2),
        vec![Some(-bound as i128 * 100), None, Some(bound as i128 * 100)]
    );
    // A negative scale divides first, truncating toward zero.
    let array = Int32Array::from(vec![Some(-1_234_567), Some(9_999_999)]);
    generate_cast_test_case!(
        &array,
        Decimal32Array,
        &DataType::Decimal32(5, -2),
        vec![Some(-12_345), Some(99_999)]
    );
    // One value beyond the bound sends the whole array down the checked path.
    let array = Int64Array::from(vec![Some(bound + 1), Some(1)]);
    let result = cast(&array, &DataType::Decimal128(18, 2)).unwrap();
    let result = result.as_primitive::<Decimal128Type>();
    assert!(result.is_null(0));
    assert_eq!(result.value(1), 100);
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    assert!(cast_with_options(&array, &DataType::Decimal128(18, 2), &options).is_err());
    // A precision beyond the target type is an error even when the values fit it.
    let array = Int64Array::from(vec![5_000_000_000]);
    for safe in [true, false] {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        assert!(cast_with_options(&array, &DataType::Decimal32(10, 0), &options).is_err());
    }
}

#[test]
fn test_cast_numeric_to_decimal128_overflow() {
    let array = Int64Array::from(vec![i64::MAX]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 30),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 30),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_err());
}

#[test]
fn test_cast_numeric_to_decimal256_overflow() {
    let array = Int64Array::from(vec![i64::MAX]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(76, 76),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(76, 76),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_err());
}

#[test]
fn test_cast_integer_to_decimal32_does_not_truncate() {
    let array = Int64Array::from(vec![5_000_000_000i64, 10_000_000_000, 42]);
    let safe = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };
    let unsafe_opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    let result = cast_with_options(&array, &DataType::Decimal32(9, 0), &safe).unwrap();
    let result = result.as_primitive::<Decimal32Type>();
    assert!(
        result.is_null(0),
        "5e9 must not wrap to {}",
        result.value(0)
    );
    assert!(result.is_null(1));
    assert_eq!(result.value(2), 42);

    let err = cast_with_options(&array, &DataType::Decimal32(9, 0), &unsafe_opts)
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Cast error: Cannot cast to Decimal32(9, 0). Overflowing on 5000000000"
    );

    let result = cast_with_options(&array, &DataType::Decimal128(9, 0), &safe).unwrap();
    let result = result.as_primitive::<Decimal128Type>();
    assert!(result.is_null(0));
    assert!(result.is_null(1));
    assert_eq!(result.value(2), 42);
}

#[test]
fn test_cast_integer_to_decimal32_scales_before_narrowing() {
    let array = Int64Array::from(vec![5_000_000_000i64]);
    let safe = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };
    let unsafe_opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let data_type = DataType::Decimal32(9, -1);

    let result = cast_with_options(&array, &data_type, &safe).unwrap();
    let result = result.as_primitive::<Decimal32Type>();
    assert_eq!(result.value(0), 500_000_000);

    let result = cast_with_options(&array, &data_type, &unsafe_opts).unwrap();
    let result = result.as_primitive::<Decimal32Type>();
    assert_eq!(result.value(0), 500_000_000);
}

#[test]
fn test_cast_uint_to_decimal32_does_not_wrap() {
    let array = UInt32Array::from(vec![4_000_000_000u32]);
    let safe = CastOptions {
        safe: true,
        format_options: FormatOptions::default(),
    };
    let unsafe_opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    let result = cast_with_options(&array, &DataType::Decimal32(9, 0), &safe).unwrap();
    let result = result.as_primitive::<Decimal32Type>();
    assert!(
        result.is_null(0),
        "u32 4e9 must not wrap to {}",
        result.value(0)
    );

    let err = cast_with_options(&array, &DataType::Decimal32(9, 0), &unsafe_opts)
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Cast error: Cannot cast to Decimal32(9, 0). Overflowing on 4000000000"
    );

    let result = cast_with_options(&array, &DataType::Decimal128(9, 0), &safe).unwrap();
    assert!(result.is_null(0));
    assert!(cast_with_options(&array, &DataType::Decimal128(9, 0), &unsafe_opts).is_err());
}

#[test]
fn test_cast_uint64_max_to_decimal64_does_not_wrap() {
    let array = UInt64Array::from(vec![u64::MAX]);
    let unsafe_opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };

    let err = cast_with_options(&array, &DataType::Decimal64(18, 0), &unsafe_opts)
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Cast error: Cannot cast to Decimal64(18, 0). Overflowing on 18446744073709551615"
    );

    assert!(cast_with_options(&array, &DataType::Decimal128(18, 0), &unsafe_opts).is_err());
}

#[test]
fn test_cast_floating_point_to_decimal128_precision_overflow() {
    let array = Float64Array::from(vec![1.1]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(2, 2),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(2, 2),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Invalid argument error: 1.10 is too large to store in a Decimal128 of precision 2. Max is 0.99";
    assert!(
        err.contains(expected_error),
        "did not find expected error '{expected_error}' in actual error '{err}'"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_float16_to_decimal128_precision_overflow() {
    let array = Float16Array::from(vec![f16::from_f32(1.1)]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(2, 2),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(2, 2),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Invalid argument error: 1.10 is too large to store in a Decimal128 of precision 2. Max is 0.99";
    assert_eq!(err, expected_error);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_float16_to_decimal256_precision_overflow() {
    let array = Float16Array::from(vec![f16::from_f32(1.1)]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(2, 2),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(2, 2),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Invalid argument error: 1.10 is too large to store in a Decimal256 of precision 2. Max is 0.99";
    assert_eq!(err, expected_error);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_cast_float16_to_decimal128_non_finite() {
    let array = Float16Array::from(vec![f16::NAN, f16::INFINITY, f16::NEG_INFINITY]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 2),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    )
    .unwrap();

    assert!(casted_array.is_null(0));
    assert!(casted_array.is_null(1));
    assert!(casted_array.is_null(2));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 2),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Cannot cast to Decimal128(38, 2)";
    assert!(
        err.contains(expected_error),
        "did not find expected error '{expected_error}' in actual error '{err}'"
    );
}

#[test]
fn test_cast_floating_point_to_decimal256_precision_overflow() {
    let array = Float64Array::from(vec![1.1]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(2, 2),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(2, 2),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Invalid argument error: 1.10 is too large to store in a Decimal256 of precision 2. Max is 0.99";
    assert_eq!(err, expected_error);
}

#[test]
fn test_cast_floating_point_to_decimal128_overflow() {
    let array = Float64Array::from(vec![f64::MAX]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 30),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(38, 30),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Cast error: Cannot cast to Decimal128(38, 30)";
    assert!(
        err.contains(expected_error),
        "did not find expected error '{expected_error}' in actual error '{err}'"
    );
}

#[test]
fn test_cast_floating_point_to_decimal256_overflow() {
    let array = Float64Array::from(vec![f64::MAX]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(76, 50),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(76, 50),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    let err = casted_array.unwrap_err().to_string();
    let expected_error = "Cast error: Cannot cast to Decimal256(76, 50)";
    assert!(
        err.contains(expected_error),
        "did not find expected error '{expected_error}' in actual error '{err}'"
    );
}
#[test]
fn test_cast_decimal256_to_f64_no_overflow() {
    // Test casting i256::MAX: should produce a large finite positive value
    let array = vec![Some(i256::MAX)];
    let array = create_decimal256_array(array, 76, 2).unwrap();
    let array = Arc::new(array) as ArrayRef;

    let result = cast(&array, &DataType::Float64).unwrap();
    let result = result.as_primitive::<Float64Type>();
    assert!(result.value(0).is_finite());
    assert!(result.value(0) > 0.0); // Positive result

    // Test casting i256::MIN: should produce a large finite negative value
    let array = vec![Some(i256::MIN)];
    let array = create_decimal256_array(array, 76, 2).unwrap();
    let array = Arc::new(array) as ArrayRef;

    let result = cast(&array, &DataType::Float64).unwrap();
    let result = result.as_primitive::<Float64Type>();
    assert!(result.value(0).is_finite());
    assert!(result.value(0) < 0.0); // Negative result
}

#[test]
fn test_cast_decimal256_negative_scale_metadata() {
    let input = Decimal256Array::from(vec![
        Some(i256::ZERO),
        Some(i256::ONE),
        Some(i256::MINUS_ONE),
        None,
    ])
    .with_precision_and_scale(76, -51)
    .unwrap();
    let value = i256::from_i128(10).pow_wrapping(51);
    let expected = Decimal256Array::from(vec![Some(i256::ZERO), Some(value), Some(-value), None])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let output_type = DataType::Decimal256(76, 0);
    let result = cast(&input, &output_type).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
    let result = cast_with_options(&input, &output_type, &options).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);

    // The precision plus scale increase crosses i8::MAX at -52.
    let input = input.with_precision_and_scale(76, -52).unwrap();
    let value = i256::from_i128(10).pow_wrapping(52);
    let expected = Decimal256Array::from(vec![Some(i256::ZERO), Some(value), Some(-value), None])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let result = cast(&input, &output_type).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
    let result = cast_with_options(&input, &output_type, &options).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
}

#[test]
fn test_cast_decimal256_negative_scale_precision_overflow() {
    let input = Decimal256Array::from(vec![Some(i256::ZERO), Some(i256::ONE), None])
        .with_precision_and_scale(76, -76)
        .unwrap();
    let output_type = DataType::Decimal256(76, 0);
    let expected = Decimal256Array::from(vec![Some(i256::ZERO), None, None])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let result = cast(&input, &output_type).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let error = cast_with_options(&input, &output_type, &options).unwrap_err();
    let value = i256::from_i128(10).pow_wrapping(76);
    assert_eq!(
        error.to_string(),
        format!(
            "Invalid argument error: {value} is too large to store in a Decimal256 of precision 76. Max is {}",
            value - i256::ONE
        )
    );
}

#[test]
fn test_cast_decimal256_negative_scale_native_overflow() {
    let input = Decimal256Array::from(vec![Some(i256::from_i128(6)), None])
        .with_precision_and_scale(76, -76)
        .unwrap();
    let output_type = DataType::Decimal256(76, 0);
    let expected = Decimal256Array::from(vec![None, None])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let result = cast(&input, &output_type).unwrap();
    assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let error = cast_with_options(&input, &output_type, &options).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Cast error: Cannot cast to Decimal256(76, 0). Overflowing on 6"
    );
}

#[test]
fn test_cast_decimal256_extreme_scale_difference() {
    let input = Decimal256Array::from(vec![Some(i256::ONE), Some(i256::MINUS_ONE), None])
        .with_precision_and_scale(76, i8::MIN)
        .unwrap();
    for safe in [true, false] {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        let error = cast_with_options(&input, &DataType::Decimal256(76, 76), &options).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Cast error: Cannot cast to Decimal256(76, 76). Value overflows for output scale"
        );
    }

    let input = input.with_precision_and_scale(76, 76).unwrap();
    let expected = Decimal256Array::from(vec![Some(i256::ZERO), Some(i256::ZERO), None])
        .with_precision_and_scale(76, i8::MIN)
        .unwrap();
    for safe in [true, false] {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        let result =
            cast_with_options(&input, &DataType::Decimal256(76, i8::MIN), &options).unwrap();
        assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
    }
}

#[test]
fn test_cast_decimal128_to_decimal128_negative_scale() {
    let input_type = DataType::Decimal128(20, 0);
    let output_type = DataType::Decimal128(20, -1);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(1123450), Some(2123455), Some(3123456), None];
    let input_decimal_array = create_decimal128_array(array, 20, 0).unwrap();
    let array = Arc::new(input_decimal_array) as ArrayRef;
    generate_cast_test_case!(
        &array,
        Decimal128Array,
        &output_type,
        vec![
            Some(112345_i128),
            Some(212346_i128),
            Some(312346_i128),
            None
        ]
    );

    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("1123450", decimal_arr.value_as_string(0));
    assert_eq!("2123460", decimal_arr.value_as_string(1));
    assert_eq!("3123460", decimal_arr.value_as_string(2));
}

#[test]
fn decimal128_min_max_to_f64() {
    // Ensure Decimal128 i128::MIN/MAX round-trip cast
    let min128 = i128::MIN;
    let max128 = i128::MAX;
    assert_eq!(min128 as f64, min128 as f64);
    assert_eq!(max128 as f64, max128 as f64);
}

#[test]
fn test_cast_numeric_to_decimal128_negative() {
    let decimal_type = DataType::Decimal128(38, -1);
    let array = Arc::new(Int32Array::from(vec![
        Some(1123456),
        Some(2123456),
        Some(3123456),
    ])) as ArrayRef;

    let casted_array = cast(&array, &decimal_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("1123450", decimal_arr.value_as_string(0));
    assert_eq!("2123450", decimal_arr.value_as_string(1));
    assert_eq!("3123450", decimal_arr.value_as_string(2));

    let array = Arc::new(Float32Array::from(vec![
        Some(1123.456),
        Some(2123.456),
        Some(3123.456),
    ])) as ArrayRef;

    let casted_array = cast(&array, &decimal_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("1120", decimal_arr.value_as_string(0));
    assert_eq!("2120", decimal_arr.value_as_string(1));
    assert_eq!("3120", decimal_arr.value_as_string(2));
}

#[test]
fn test_cast_decimal128_to_decimal128_negative() {
    let input_type = DataType::Decimal128(10, -1);
    let output_type = DataType::Decimal128(10, -2);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(123)];
    let input_decimal_array = create_decimal128_array(array, 10, -1).unwrap();
    let array = Arc::new(input_decimal_array) as ArrayRef;
    generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(12_i128),]);

    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("1200", decimal_arr.value_as_string(0));

    let array = vec![Some(125)];
    let input_decimal_array = create_decimal128_array(array, 10, -1).unwrap();
    let array = Arc::new(input_decimal_array) as ArrayRef;
    generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(13_i128),]);

    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("1300", decimal_arr.value_as_string(0));
}

#[test]
fn test_cast_decimal128_to_decimal256_negative() {
    let input_type = DataType::Decimal128(10, 3);
    let output_type = DataType::Decimal256(10, 5);
    assert!(can_cast_types(&input_type, &output_type));
    let array = vec![Some(123456), Some(-123456)];
    let input_decimal_array = create_decimal128_array(array, 10, 3).unwrap();
    let array = Arc::new(input_decimal_array) as ArrayRef;

    let hundred = i256::from_i128(100);
    generate_cast_test_case!(
        &array,
        Decimal256Array,
        &output_type,
        vec![
            Some(i256::from_i128(123456).mul_wrapping(hundred)),
            Some(i256::from_i128(-123456).mul_wrapping(hundred))
        ]
    );
}

#[test]
fn test_parse_decimal_and_format() {
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>("123.45", 38, 2).unwrap(),
            38,
            2,
        ),
        "123.45"
    );
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>("12345", 38, 2).unwrap(),
            38,
            2,
        ),
        "12345.00"
    );
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>("0.12345", 38, 2).unwrap(),
            38,
            2,
        ),
        "0.12"
    );
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>(".12345", 38, 2).unwrap(),
            38,
            2,
        ),
        "0.12"
    );
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>(".1265", 38, 2).unwrap(),
            38,
            2,
        ),
        "0.13"
    );
    assert_eq!(
        Decimal128Type::format_decimal(
            parse_decimal::<Decimal128Type>(".1265", 38, 2).unwrap(),
            38,
            2,
        ),
        "0.13"
    );

    assert_eq!(
        Decimal256Type::format_decimal(
            parse_decimal::<Decimal256Type>("123.45", 76, 3).unwrap(),
            38,
            3,
        ),
        "123.450"
    );
    assert_eq!(
        Decimal256Type::format_decimal(
            parse_decimal::<Decimal256Type>("12345", 76, 3).unwrap(),
            38,
            3,
        ),
        "12345.000"
    );
    assert_eq!(
        Decimal256Type::format_decimal(
            parse_decimal::<Decimal256Type>("0.12345", 76, 3).unwrap(),
            38,
            3,
        ),
        "0.123"
    );
    assert_eq!(
        Decimal256Type::format_decimal(
            parse_decimal::<Decimal256Type>(".12345", 76, 3).unwrap(),
            38,
            3,
        ),
        "0.123"
    );
    assert_eq!(
        Decimal256Type::format_decimal(
            parse_decimal::<Decimal256Type>(".1265", 76, 3).unwrap(),
            38,
            3,
        ),
        "0.127"
    );
}

fn test_cast_string_to_decimal(array: ArrayRef) {
    // Decimal128
    let output_type = DataType::Decimal128(38, 2);
    assert!(can_cast_types(array.data_type(), &output_type));

    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert_eq!("123.45", decimal_arr.value_as_string(0));
    assert_eq!("1.23", decimal_arr.value_as_string(1));
    assert_eq!("0.12", decimal_arr.value_as_string(2));
    assert_eq!("0.13", decimal_arr.value_as_string(3));
    assert_eq!("1.26", decimal_arr.value_as_string(4));
    assert_eq!("12345.00", decimal_arr.value_as_string(5));
    assert_eq!("12345.00", decimal_arr.value_as_string(6));
    assert_eq!("0.12", decimal_arr.value_as_string(7));
    assert_eq!("12.23", decimal_arr.value_as_string(8));
    assert!(decimal_arr.is_null(9));
    assert!(decimal_arr.is_null(10));
    assert!(decimal_arr.is_null(11));
    assert!(decimal_arr.is_null(12));
    assert_eq!("-1.23", decimal_arr.value_as_string(13));
    assert_eq!("-1.24", decimal_arr.value_as_string(14));
    assert_eq!("0.00", decimal_arr.value_as_string(15));
    assert_eq!("-123.00", decimal_arr.value_as_string(16));
    assert_eq!("-123.23", decimal_arr.value_as_string(17));
    assert_eq!("-0.12", decimal_arr.value_as_string(18));
    assert_eq!("1.23", decimal_arr.value_as_string(19));
    assert_eq!("1.24", decimal_arr.value_as_string(20));
    assert_eq!("0.00", decimal_arr.value_as_string(21));
    assert_eq!("123.00", decimal_arr.value_as_string(22));
    assert_eq!("123.23", decimal_arr.value_as_string(23));
    assert_eq!("0.12", decimal_arr.value_as_string(24));
    assert!(decimal_arr.is_null(25));
    assert!(decimal_arr.is_null(26));
    assert!(decimal_arr.is_null(27));
    assert_eq!("0.00", decimal_arr.value_as_string(28));
    assert_eq!("0.00", decimal_arr.value_as_string(29));
    assert_eq!("12345.00", decimal_arr.value_as_string(30));
    assert_eq!(decimal_arr.len(), 31);

    // Decimal256
    let output_type = DataType::Decimal256(76, 3);
    assert!(can_cast_types(array.data_type(), &output_type));

    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

    assert_eq!("123.450", decimal_arr.value_as_string(0));
    assert_eq!("1.235", decimal_arr.value_as_string(1));
    assert_eq!("0.123", decimal_arr.value_as_string(2));
    assert_eq!("0.127", decimal_arr.value_as_string(3));
    assert_eq!("1.263", decimal_arr.value_as_string(4));
    assert_eq!("12345.000", decimal_arr.value_as_string(5));
    assert_eq!("12345.000", decimal_arr.value_as_string(6));
    assert_eq!("0.123", decimal_arr.value_as_string(7));
    assert_eq!("12.234", decimal_arr.value_as_string(8));
    assert!(decimal_arr.is_null(9));
    assert!(decimal_arr.is_null(10));
    assert!(decimal_arr.is_null(11));
    assert!(decimal_arr.is_null(12));
    assert_eq!("-1.235", decimal_arr.value_as_string(13));
    assert_eq!("-1.236", decimal_arr.value_as_string(14));
    assert_eq!("0.000", decimal_arr.value_as_string(15));
    assert_eq!("-123.000", decimal_arr.value_as_string(16));
    assert_eq!("-123.234", decimal_arr.value_as_string(17));
    assert_eq!("-0.123", decimal_arr.value_as_string(18));
    assert_eq!("1.235", decimal_arr.value_as_string(19));
    assert_eq!("1.236", decimal_arr.value_as_string(20));
    assert_eq!("0.000", decimal_arr.value_as_string(21));
    assert_eq!("123.000", decimal_arr.value_as_string(22));
    assert_eq!("123.234", decimal_arr.value_as_string(23));
    assert_eq!("0.123", decimal_arr.value_as_string(24));
    assert!(decimal_arr.is_null(25));
    assert!(decimal_arr.is_null(26));
    assert!(decimal_arr.is_null(27));
    assert_eq!("0.000", decimal_arr.value_as_string(28));
    assert_eq!("0.000", decimal_arr.value_as_string(29));
    assert_eq!("12345.000", decimal_arr.value_as_string(30));
    assert_eq!(decimal_arr.len(), 31);
}

#[test]
fn test_cast_utf8_to_decimal() {
    let str_array = StringArray::from(vec![
        Some("123.45"),
        Some("1.2345"),
        Some("0.12345"),
        Some("0.1267"),
        Some("1.263"),
        Some("12345.0"),
        Some("12345"),
        Some("000.123"),
        Some("12.234000"),
        None,
        Some(""),
        Some(" "),
        None,
        Some("-1.23499999"),
        Some("-1.23599999"),
        Some("-0.00001"),
        Some("-123"),
        Some("-123.234000"),
        Some("-000.123"),
        Some("+1.23499999"),
        Some("+1.23599999"),
        Some("+0.00001"),
        Some("+123"),
        Some("+123.234000"),
        Some("+000.123"),
        Some("1.-23499999"),
        Some("-1.-23499999"),
        Some("--1.23499999"),
        Some("0"),
        Some("000.000"),
        Some("0000000000000000012345.000"),
    ]);
    let array = Arc::new(str_array) as ArrayRef;

    test_cast_string_to_decimal(array);

    let test_cases = [
        (None, None),
        (Some(""), None),
        (Some("   "), None),
        (Some("0"), Some("0")),
        (Some("000.000"), Some("0")),
        (Some("12345"), Some("12345")),
        (Some("000000000000000000000000000012345"), Some("12345")),
        (Some("-123"), Some("-123")),
        (Some("+123"), Some("123")),
    ];
    let inputs = test_cases.iter().map(|entry| entry.0).collect::<Vec<_>>();
    let expected = test_cases.iter().map(|entry| entry.1).collect::<Vec<_>>();

    let array = Arc::new(StringArray::from(inputs)) as ArrayRef;
    test_cast_string_to_decimal_scale_zero(array, &expected);
}

#[test]
fn test_cast_large_utf8_to_decimal() {
    let str_array = LargeStringArray::from(vec![
        Some("123.45"),
        Some("1.2345"),
        Some("0.12345"),
        Some("0.1267"),
        Some("1.263"),
        Some("12345.0"),
        Some("12345"),
        Some("000.123"),
        Some("12.234000"),
        None,
        Some(""),
        Some(" "),
        None,
        Some("-1.23499999"),
        Some("-1.23599999"),
        Some("-0.00001"),
        Some("-123"),
        Some("-123.234000"),
        Some("-000.123"),
        Some("+1.23499999"),
        Some("+1.23599999"),
        Some("+0.00001"),
        Some("+123"),
        Some("+123.234000"),
        Some("+000.123"),
        Some("1.-23499999"),
        Some("-1.-23499999"),
        Some("--1.23499999"),
        Some("0"),
        Some("000.000"),
        Some("0000000000000000012345.000"),
    ]);
    let array = Arc::new(str_array) as ArrayRef;

    test_cast_string_to_decimal(array);

    let test_cases = [
        (None, None),
        (Some(""), None),
        (Some("   "), None),
        (Some("0"), Some("0")),
        (Some("000.000"), Some("0")),
        (Some("12345"), Some("12345")),
        (Some("000000000000000000000000000012345"), Some("12345")),
        (Some("-123"), Some("-123")),
        (Some("+123"), Some("123")),
    ];
    let inputs = test_cases.iter().map(|entry| entry.0).collect::<Vec<_>>();
    let expected = test_cases.iter().map(|entry| entry.1).collect::<Vec<_>>();

    let array = Arc::new(LargeStringArray::from(inputs)) as ArrayRef;
    test_cast_string_to_decimal_scale_zero(array, &expected);
}

fn test_cast_string_to_decimal_scale_zero(array: ArrayRef, expected_as_string: &[Option<&str>]) {
    // Decimal128
    let output_type = DataType::Decimal128(38, 0);
    assert!(can_cast_types(array.data_type(), &output_type));
    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();
    assert_decimal_array_contents(decimal_arr, expected_as_string);

    // Decimal256
    let output_type = DataType::Decimal256(76, 0);
    assert!(can_cast_types(array.data_type(), &output_type));
    let casted_array = cast(&array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal256Type>();
    assert_decimal_array_contents(decimal_arr, expected_as_string);
}

fn assert_decimal_array_contents<T>(array: &PrimitiveArray<T>, expected_as_string: &[Option<&str>])
where
    T: DecimalType + ArrowPrimitiveType,
{
    assert_eq!(array.len(), expected_as_string.len());
    for (i, expected) in expected_as_string.iter().enumerate() {
        let actual = if array.is_null(i) {
            None
        } else {
            Some(array.value_as_string(i))
        };
        let actual = actual.as_ref().map(|s| s.as_ref());
        assert_eq!(*expected, actual, "Expected at position {i}");
    }
}

#[test]
fn test_cast_invalid_utf8_to_decimal() {
    let str_array = StringArray::from(vec!["4.4.5", ". 0.123"]);
    let array = Arc::new(str_array) as ArrayRef;

    // Safe cast
    let output_type = DataType::Decimal128(38, 2);
    let casted_array = cast(&array, &output_type).unwrap();
    assert!(casted_array.is_null(0));
    assert!(casted_array.is_null(1));

    let output_type = DataType::Decimal256(76, 2);
    let casted_array = cast(&array, &output_type).unwrap();
    assert!(casted_array.is_null(0));
    assert!(casted_array.is_null(1));

    // Non-safe cast
    let output_type = DataType::Decimal128(38, 2);
    let str_array = StringArray::from(vec!["4.4.5"]);
    let array = Arc::new(str_array) as ArrayRef;
    let option = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
    assert!(
        casted_err
            .to_string()
            .contains("Cannot cast string '4.4.5' to value of Decimal128(38, 2) type")
    );

    let str_array = StringArray::from(vec![". 0.123"]);
    let array = Arc::new(str_array) as ArrayRef;
    let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
    assert!(
        casted_err
            .to_string()
            .contains("Cannot cast string '. 0.123' to value of Decimal128(38, 2) type")
    );

    let str_array = StringArray::from(vec![""]);
    let array = Arc::new(str_array) as ArrayRef;
    let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
    assert!(
        casted_err
            .to_string()
            .contains("Cannot cast string '' to value of Decimal128(38, 2) type")
    );
}

fn test_cast_string_to_decimal128_overflow(overflow_array: ArrayRef) {
    let output_type = DataType::Decimal128(38, 2);
    let casted_array = cast(&overflow_array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

    assert!(decimal_arr.is_null(0));
    assert!(decimal_arr.is_null(1));
    assert!(decimal_arr.is_null(2));
    assert_eq!(
        "999999999999999999999999999999999999.99",
        decimal_arr.value_as_string(3)
    );
    assert_eq!(
        "100000000000000000000000000000000000.00",
        decimal_arr.value_as_string(4)
    );
}

#[test]
fn test_cast_string_to_decimal128_precision_overflow() {
    let array = StringArray::from(vec!["1000".to_string()]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(10, 8),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let err = cast_with_options(
        &array,
        &DataType::Decimal128(10, 8),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast string '1000' to value of Decimal128(10, 8) type: value does not fit",
        err.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_utf8_to_decimal128_overflow() {
    let overflow_str_array = StringArray::from(vec![
        i128::MAX.to_string(),
        i128::MIN.to_string(),
        "99999999999999999999999999999999999999".to_string(),
        "999999999999999999999999999999999999.99".to_string(),
        "99999999999999999999999999999999999.999".to_string(),
    ]);
    let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

    test_cast_string_to_decimal128_overflow(overflow_array);
}

#[test]
fn test_cast_large_utf8_to_decimal128_overflow() {
    let overflow_str_array = LargeStringArray::from(vec![
        i128::MAX.to_string(),
        i128::MIN.to_string(),
        "99999999999999999999999999999999999999".to_string(),
        "999999999999999999999999999999999999.99".to_string(),
        "99999999999999999999999999999999999.999".to_string(),
    ]);
    let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

    test_cast_string_to_decimal128_overflow(overflow_array);
}

fn test_cast_string_to_decimal256_overflow(overflow_array: ArrayRef) {
    let output_type = DataType::Decimal256(76, 2);
    let casted_array = cast(&overflow_array, &output_type).unwrap();
    let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

    assert_eq!(
        "170141183460469231731687303715884105727.00",
        decimal_arr.value_as_string(0)
    );
    assert_eq!(
        "-170141183460469231731687303715884105728.00",
        decimal_arr.value_as_string(1)
    );
    assert_eq!(
        "99999999999999999999999999999999999999.00",
        decimal_arr.value_as_string(2)
    );
    assert_eq!(
        "999999999999999999999999999999999999.99",
        decimal_arr.value_as_string(3)
    );
    assert_eq!(
        "100000000000000000000000000000000000.00",
        decimal_arr.value_as_string(4)
    );
    assert!(decimal_arr.is_null(5));
    assert!(decimal_arr.is_null(6));
}

#[test]
fn test_cast_string_to_decimal256_precision_overflow() {
    let array = StringArray::from(vec!["1000".to_string()]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(10, 8),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let err = cast_with_options(
        &array,
        &DataType::Decimal256(10, 8),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Cast error: Cannot cast string '1000' to value of Decimal256(10, 8) type: value does not fit",
        err.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_utf8_to_decimal256_overflow() {
    let overflow_str_array = StringArray::from(vec![
        i128::MAX.to_string(),
        i128::MIN.to_string(),
        "99999999999999999999999999999999999999".to_string(),
        "999999999999999999999999999999999999.99".to_string(),
        "99999999999999999999999999999999999.999".to_string(),
        i256::MAX.to_string(),
        i256::MIN.to_string(),
    ]);
    let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

    test_cast_string_to_decimal256_overflow(overflow_array);
}

#[test]
fn test_cast_large_utf8_to_decimal256_overflow() {
    let overflow_str_array = LargeStringArray::from(vec![
        i128::MAX.to_string(),
        i128::MIN.to_string(),
        "99999999999999999999999999999999999999".to_string(),
        "999999999999999999999999999999999999.99".to_string(),
        "99999999999999999999999999999999999.999".to_string(),
        i256::MAX.to_string(),
        i256::MIN.to_string(),
    ]);
    let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

    test_cast_string_to_decimal256_overflow(overflow_array);
}

#[test]
fn test_cast_out_of_precision_decimal_to_string() {
    // Decimal values are not validated against their type's declared
    // precision by default. Check that out-of-precision values are rendered
    // in full when cast to strings, rather than truncated to the declared
    // precision (https://github.com/apache/arrow-rs/issues/10866)
    let array = create_decimal128_array(vec![Some(123456789), Some(-123456789)], 7, 3).unwrap();
    let b = cast(&array, &DataType::Utf8).unwrap();
    let c = b.as_string::<i32>();
    assert_eq!("123456.789", c.value(0));
    assert_eq!("-123456.789", c.value(1));
}

#[test]
fn test_cast_decimal_to_string() {
    assert!(can_cast_types(
        &DataType::Decimal32(9, 4),
        &DataType::Utf8View
    ));
    assert!(can_cast_types(
        &DataType::Decimal64(16, 4),
        &DataType::Utf8View
    ));
    assert!(can_cast_types(
        &DataType::Decimal128(10, 4),
        &DataType::Utf8View
    ));
    assert!(can_cast_types(
        &DataType::Decimal256(38, 10),
        &DataType::Utf8View
    ));

    macro_rules! assert_decimal_values {
        ($array:expr) => {
            let c = $array;
            assert_eq!("1123.454", c.value(0));
            assert_eq!("2123.456", c.value(1));
            assert_eq!("-3123.453", c.value(2));
            assert_eq!("-3123.456", c.value(3));
            assert_eq!("0.000", c.value(4));
            assert_eq!("0.123", c.value(5));
            assert!(c.is_null(6));
        };
    }

    fn test_decimal_to_string<IN: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
        output_type: DataType,
        array: PrimitiveArray<IN>,
    ) {
        let b = cast(&array, &output_type).unwrap();

        assert_eq!(b.data_type(), &output_type);
        match b.data_type() {
            DataType::Utf8View => {
                let c = b.as_string_view();
                assert_decimal_values!(c);
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let c = b.as_string::<OffsetSize>();
                assert_decimal_values!(c);
            }
            _ => (),
        }
    }

    let array32: Vec<Option<i32>> = vec![
        Some(1123454),
        Some(2123456),
        Some(-3123453),
        Some(-3123456),
        Some(0),
        Some(123),
        None,
    ];
    let array64: Vec<Option<i64>> = array32.iter().map(|num| num.map(|x| x as i64)).collect();
    let array128: Vec<Option<i128>> = array64.iter().map(|num| num.map(|x| x as i128)).collect();
    let array256: Vec<Option<i256>> = array128
        .iter()
        .map(|num| num.map(i256::from_i128))
        .collect();

    test_decimal_to_string::<Decimal32Type, i32>(
        DataType::Utf8View,
        create_decimal32_array(array32.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal32Type, i32>(
        DataType::Utf8,
        create_decimal32_array(array32.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal32Type, i64>(
        DataType::LargeUtf8,
        create_decimal32_array(array32, 7, 3).unwrap(),
    );

    test_decimal_to_string::<Decimal64Type, i32>(
        DataType::Utf8View,
        create_decimal64_array(array64.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal64Type, i32>(
        DataType::Utf8,
        create_decimal64_array(array64.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal64Type, i64>(
        DataType::LargeUtf8,
        create_decimal64_array(array64, 7, 3).unwrap(),
    );

    test_decimal_to_string::<Decimal128Type, i32>(
        DataType::Utf8View,
        create_decimal128_array(array128.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal128Type, i32>(
        DataType::Utf8,
        create_decimal128_array(array128.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal128Type, i64>(
        DataType::LargeUtf8,
        create_decimal128_array(array128, 7, 3).unwrap(),
    );

    test_decimal_to_string::<Decimal256Type, i32>(
        DataType::Utf8View,
        create_decimal256_array(array256.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal256Type, i32>(
        DataType::Utf8,
        create_decimal256_array(array256.clone(), 7, 3).unwrap(),
    );
    test_decimal_to_string::<Decimal256Type, i64>(
        DataType::LargeUtf8,
        create_decimal256_array(array256, 7, 3).unwrap(),
    );
}

#[test]
fn test_cast_numeric_to_decimal128_precision_overflow() {
    let array = Int64Array::from(vec![1234567]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal128(7, 3),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let err = cast_with_options(
        &array,
        &DataType::Decimal128(7, 3),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Invalid argument error: 1234567.000 is too large to store in a Decimal128 of precision 7. Max is 9999.999",
        err.unwrap_err().to_string()
    );
}

#[test]
fn test_cast_numeric_to_decimal256_precision_overflow() {
    let array = Int64Array::from(vec![1234567]);
    let array = Arc::new(array) as ArrayRef;
    let casted_array = cast_with_options(
        &array,
        &DataType::Decimal256(7, 3),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        },
    );
    assert!(casted_array.is_ok());
    assert!(casted_array.unwrap().is_null(0));

    let err = cast_with_options(
        &array,
        &DataType::Decimal256(7, 3),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    );
    assert_eq!(
        "Invalid argument error: 1234567.000 is too large to store in a Decimal256 of precision 7. Max is 9999.999",
        err.unwrap_err().to_string()
    );
}

fn run_decimal_cast_test_case_between_multiple_types(t: DecimalCastTestConfig) {
    run_decimal_cast_test_case::<Decimal128Type, Decimal128Type>(t.clone());
    run_decimal_cast_test_case::<Decimal128Type, Decimal256Type>(t.clone());
    run_decimal_cast_test_case::<Decimal256Type, Decimal128Type>(t.clone());
    run_decimal_cast_test_case::<Decimal256Type, Decimal256Type>(t.clone());
}

#[test]
fn test_decimal_to_decimal_coverage() {
    let test_cases = [
        // increase precision, increase scale, infallible
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 1,
            input_repr: 99999, // 9999.9
            output_prec: 10,
            output_scale: 6,
            expected_output_repr: Ok(9999900000), // 9999.900000
        },
        // increase precision, increase scale, fallible, safe
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 1,
            input_repr: 99, // 9999.9
            output_prec: 7,
            output_scale: 6,
            expected_output_repr: Ok(9900000), // 9.900000
        },
        // increase precision, increase scale, fallible, unsafe
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 1,
            input_repr: 99999, // 9999.9
            output_prec: 7,
            output_scale: 6,
            expected_output_repr: Err("Invalid argument error: 9999.900000 is too large to store in a {} of precision 7. Max is 9.999999".to_string()) // max is 9.999999
        },
        // increase precision, decrease scale, always infallible
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 3,
            input_repr: 99999, // 99.999
            output_prec: 10,
            output_scale: 2,
            expected_output_repr: Ok(10000), // 100.00
        },
        // increase precision, decrease scale, no rounding
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 3,
            input_repr: 99994, // 99.994
            output_prec: 10,
            output_scale: 2,
            expected_output_repr: Ok(9999), // 99.99
        },
        // increase precision, don't change scale, always infallible
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 3,
            input_repr: 99999, // 99.999
            output_prec: 10,
            output_scale: 3,
            expected_output_repr: Ok(99999), // 99.999
        },
        // decrease precision, increase scale, safe
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 999999, // 9.99999
            output_prec: 8,
            output_scale: 7,
            expected_output_repr: Ok(99999900), // 9.9999900
        },
        // decrease precision, increase scale, unsafe
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 9999999, // 99.99999
            output_prec: 8,
            output_scale: 7,
            expected_output_repr: Err("Invalid argument error: 99.9999900 is too large to store in a {} of precision 8. Max is 9.9999999".to_string()) // max is 9.9999999
        },
        // decrease precision, decrease scale, safe, infallible
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 4,
            input_repr: 9999999, // 999.9999
            output_prec: 6,
            output_scale: 2,
            expected_output_repr: Ok(100000),
        },
        // decrease precision, decrease scale, safe, fallible
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 12345678, // 123.45678
            output_prec: 8,
            output_scale: 3,
            expected_output_repr: Ok(123457), // 123.457
        },
        // decrease precision, decrease scale, unsafe
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 9999999, // 99.99999
            output_prec: 4,
            output_scale: 3,
            expected_output_repr: Err("Invalid argument error: 100.000 is too large to store in a {} of precision 4. Max is 9.999".to_string()) // max is 9.999
        },
        // decrease precision, same scale, safe
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 999999, // 9.99999
            output_prec: 6,
            output_scale: 5,
            expected_output_repr: Ok(999999), // 9.99999
        },
        // decrease precision, same scale, unsafe
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 5,
            input_repr: 9999999, // 99.99999
            output_prec: 6,
            output_scale: 5,
            expected_output_repr: Err("Invalid argument error: 99.99999 is too large to store in a {} of precision 6. Max is 9.99999".to_string()) // max is 9.99999
        },
        // same precision, increase scale, safe
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 4,
            input_repr: 12345, // 1.2345
            output_prec: 7,
            output_scale: 6,
            expected_output_repr: Ok(1234500), // 1.234500
        },
        // same precision, increase scale, unsafe
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 4,
            input_repr: 123456, // 12.3456
            output_prec: 7,
            output_scale: 6,
            expected_output_repr: Err("Invalid argument error: 12.345600 is too large to store in a {} of precision 7. Max is 9.999999".to_string()) // max is 9.99999
        },
        // same precision, decrease scale, infallible
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 5,
            input_repr: 1234567, // 12.34567
            output_prec: 7,
            output_scale: 4,
            expected_output_repr: Ok(123457), // 12.3457
        },
        // same precision, same scale, infallible
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 5,
            input_repr: 9999999, // 99.99999
            output_prec: 7,
            output_scale: 5,
            expected_output_repr: Ok(9999999), // 99.99999
        },
        // precision increase, input scale & output scale = 0, infallible
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 0,
            input_repr: 1234567, // 1234567
            output_prec: 8,
            output_scale: 0,
            expected_output_repr: Ok(1234567), // 1234567
        },
        // precision decrease, input scale & output scale = 0, failure
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 0,
            input_repr: 1234567, // 1234567
            output_prec: 6,
            output_scale: 0,
            expected_output_repr: Err("Invalid argument error: 1234567 is too large to store in a {} of precision 6. Max is 999999".to_string())
        },
        // precision decrease, input scale & output scale = 0, success
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 0,
            input_repr: 123456, // 123456
            output_prec: 6,
            output_scale: 0,
            expected_output_repr: Ok(123456), // 123456
        },
    ];

    for t in test_cases {
        run_decimal_cast_test_case_between_multiple_types(t);
    }
}

#[test]
fn test_decimal_to_decimal_increase_scale_and_precision_unchecked() {
    let test_cases = [
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 0,
            input_repr: 99999,
            output_prec: 10,
            output_scale: 5,
            expected_output_repr: Ok(9999900000),
        },
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 0,
            input_repr: -99999,
            output_prec: 10,
            output_scale: 5,
            expected_output_repr: Ok(-9999900000),
        },
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 2,
            input_repr: 99999,
            output_prec: 10,
            output_scale: 5,
            expected_output_repr: Ok(99999000),
        },
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: -2,
            input_repr: -99999,
            output_prec: 10,
            output_scale: 3,
            expected_output_repr: Ok(-9999900000),
        },
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 3,
            input_repr: -12345,
            output_prec: 6,
            output_scale: 5,
            expected_output_repr: Err("Invalid argument error: -12.34500 is too small to store in a {} of precision 6. Min is -9.99999".to_string())
        },
    ];

    for t in test_cases {
        run_decimal_cast_test_case_between_multiple_types(t);
    }
}

#[test]
fn test_decimal_to_decimal_decrease_scale_and_precision_unchecked() {
    let test_cases = [
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 0,
            input_repr: 99999,
            output_scale: -3,
            output_prec: 3,
            expected_output_repr: Ok(100),
        },
        DecimalCastTestConfig {
            input_prec: 5,
            input_scale: 0,
            input_repr: -99999,
            output_prec: 1,
            output_scale: -5,
            expected_output_repr: Ok(-1),
        },
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 2,
            input_repr: 123456789,
            output_prec: 5,
            output_scale: -2,
            expected_output_repr: Ok(12346),
        },
        DecimalCastTestConfig {
            input_prec: 10,
            input_scale: 4,
            input_repr: -9876543210,
            output_prec: 7,
            output_scale: 0,
            expected_output_repr: Ok(-987654),
        },
        DecimalCastTestConfig {
            input_prec: 7,
            input_scale: 4,
            input_repr: 9999999,
            output_prec: 6,
            output_scale: 3,
            expected_output_repr:
                Err("Invalid argument error: 1000.000 is too large to store in a {} of precision 6. Max is 999.999".to_string()),
        },
    ];
    for t in test_cases {
        run_decimal_cast_test_case_between_multiple_types(t);
    }
}

#[test]
fn test_decimal_to_decimal_throw_error_on_precision_overflow_same_scale() {
    let array = vec![Some(123456789)];
    let array = create_decimal128_array(array, 24, 2).unwrap();
    let input_type = DataType::Decimal128(24, 2);
    let output_type = DataType::Decimal128(6, 2);
    assert!(can_cast_types(&input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options);
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid argument error: 1234567.89 is too large to store in a Decimal128 of precision 6. Max is 9999.99"
    );
}

#[test]
fn test_decimal_to_decimal_same_scale() {
    let array = vec![Some(520)];
    let array = create_decimal128_array(array, 4, 2).unwrap();
    let input_type = DataType::Decimal128(4, 2);
    let output_type = DataType::Decimal128(3, 2);
    assert!(can_cast_types(&input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options);
    assert_eq!(
        result.unwrap().as_primitive::<Decimal128Type>().value(0),
        520
    );

    // Cast 0 of decimal(3, 0) type to decimal(2, 0)
    assert_eq!(
        &cast(
            &create_decimal128_array(vec![Some(0)], 3, 0).unwrap(),
            &DataType::Decimal128(2, 0)
        )
        .unwrap(),
        &(Arc::new(create_decimal128_array(vec![Some(0)], 2, 0).unwrap()) as ArrayRef)
    );
}

#[test]
fn test_decimal_to_decimal_throw_error_on_precision_overflow_lower_scale() {
    let array = vec![Some(123456789)];
    let array = create_decimal128_array(array, 24, 4).unwrap();
    let input_type = DataType::Decimal128(24, 4);
    let output_type = DataType::Decimal128(6, 2);
    assert!(can_cast_types(&input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options);
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid argument error: 12345.68 is too large to store in a Decimal128 of precision 6. Max is 9999.99"
    );
}

#[test]
fn test_decimal_to_decimal_throw_error_on_precision_overflow_greater_scale() {
    let array = vec![Some(123456789)];
    let array = create_decimal128_array(array, 24, 2).unwrap();
    let input_type = DataType::Decimal128(24, 2);
    let output_type = DataType::Decimal128(6, 3);
    assert!(can_cast_types(&input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options);
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid argument error: 1234567.890 is too large to store in a Decimal128 of precision 6. Max is 999.999"
    );
}

#[test]
fn test_decimal_to_decimal_throw_error_on_precision_overflow_diff_type() {
    let array = vec![Some(123456789)];
    let array = create_decimal128_array(array, 24, 2).unwrap();
    let input_type = DataType::Decimal128(24, 2);
    let output_type = DataType::Decimal256(6, 2);
    assert!(can_cast_types(&input_type, &output_type));

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = cast_with_options(&array, &output_type, &options).unwrap_err();
    assert_eq!(
        result.to_string(),
        "Invalid argument error: 1234567.89 is too large to store in a Decimal256 of precision 6. Max is 9999.99"
    );
}

#[test]
fn test_cast_decimal_error_output() {
    let array = Int64Array::from(vec![1]);
    let error = cast_with_options(
        &array,
        &DataType::Decimal32(1, 1),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: 1.0 is too large to store in a Decimal32 of precision 1. Max is 0.9"
    );

    let array = Int64Array::from(vec![-1]);
    let error = cast_with_options(
        &array,
        &DataType::Decimal32(1, 1),
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: -1.0 is too small to store in a Decimal32 of precision 1. Min is -0.9"
    );
}

fn decimal128(unscaled: i128, precision: u8, scale: i8) -> Decimal128Array {
    Decimal128Array::from(vec![unscaled])
        .with_precision_and_scale(precision, scale)
        .unwrap()
}

#[test]
fn test_cast_decimal_to_float_is_correctly_rounded() {
    // (unscaled, scale, nearest double). The last four already passed.
    for (unscaled, scale, expected) in [
        (12345678901234567890i128, 2i8, 1.2345678901234568e17f64),
        (10i128.pow(37), 37, 1.0),
        (123456789012345678901, 20, 1.2345678901234567),
        (1, 37, 1e-37),
        (1, 1, 0.1),
        (15, 1, 1.5),
        (123456, 3, 123.456),
        (-123456, 3, -123.456),
    ] {
        let out = cast(&decimal128(unscaled, 38, scale), &DataType::Float64).unwrap();
        assert_eq!(
            out.as_primitive::<Float64Type>().value(0),
            expected,
            "Decimal128({unscaled}, scale={scale}) -> Float64"
        );
    }

    // (unscaled, scale, nearest float). The first three differ from
    // `(f32) (f64) value`, so fixing the f64 path alone leaves them wrong.
    for (unscaled, scale, expected) in [
        (13631072500000000514758830i128, 18i8, 13631073.0f32),
        (72073620000000000000000582908005, 24, 72073624.0),
        (-3273316900000000000957536840, 20, -32733170.0),
        (123456, 3, 123.456),
        (-12345678, 3, -12345.678),
        (1, 10, 1e-10),
    ] {
        let out = cast(&decimal128(unscaled, 38, scale), &DataType::Float32).unwrap();
        assert_eq!(
            out.as_primitive::<Float32Type>().value(0),
            expected,
            "Decimal128({unscaled}, scale={scale}) -> Float32"
        );
    }
}

// Both sides of every boundary the conversion switches on.
#[test]
fn test_cast_decimal_to_float_matches_parsing_its_own_text() {
    for (unscaled, precision, scale) in [
        (9007199254740991i128, 38u8, 6i8), // last integer exact in an f64
        (9007199254740992, 38, 6),         // 2^53, the first that is not
        (9007199254740993, 38, 6),         // one past that
        (16777215, 38, 3),                 // last integer exact in an f32
        (16777216, 38, 3),                 // 2^24, the first that is not
        (16777217, 38, 3),                 // one past that
        (123456789, 38, 10),               // last exact power of ten for an f32
        (123456789, 38, 11),               // one past it
        (123456789, 38, 22),               // last exact power of ten for an f64
        (123456789, 38, 23),               // one past it
        (12345, 38, 0),                    // lowest scale the division handles
        (12345, 38, -1),                   // one below it: negative scales
        (12345, 38, -23),                  //   are all rescaled as text
        // only the upper bound of a scale is validated, so this is legal
        (i128::MAX, 38, i8::MIN),
        (999999999999999, 15, 3), // widest value precision 15 allows
        (-999999999999999, 15, 15),
        (999999999999999, 16, 3), // one precision past the skipped test
        (9999999, 7, 3),          // widest value precision 7 allows
        (-9999999, 7, 7),
        (9999999, 8, 3), // one precision past it, for `f32`
        (-99999999999999999999999999999999999999, 38, 20),
        (1, 38, 38),
    ] {
        let text = Decimal128Type::format_decimal(unscaled, u8::MAX, scale);
        let array = decimal128(unscaled, precision, scale);
        let label = format!("Decimal128({unscaled}, precision={precision}, scale={scale})");

        let as_f64 = cast(&array, &DataType::Float64).unwrap();
        assert_eq!(
            as_f64.as_primitive::<Float64Type>().value(0),
            text.parse::<f64>().unwrap(),
            "{label} -> Float64 disagrees with parsing {text:?}"
        );

        let as_f32 = cast(&array, &DataType::Float32).unwrap();
        assert_eq!(
            as_f32.as_primitive::<Float32Type>().value(0),
            text.parse::<f32>().unwrap(),
            "{label} -> Float32 disagrees with parsing {text:?}"
        );
    }
}

#[test]
fn test_cast_decimal256_to_float_is_correctly_rounded() {
    for (unscaled, scale) in [
        ("12345678901234567890", 2i8),
        ("10000000000000000000000000000000000000", 37),
        ("13631072500000000514758830", 18),
        ("123456", 3),
    ] {
        let value = i256::from_string(unscaled).unwrap();
        let array = Decimal256Array::from(vec![value])
            .with_precision_and_scale(76, scale)
            .unwrap();
        let text = Decimal256Type::format_decimal(value, u8::MAX, scale);

        let as_f64 = cast(&array, &DataType::Float64).unwrap();
        assert_eq!(
            as_f64.as_primitive::<Float64Type>().value(0),
            text.parse::<f64>().unwrap(),
            "Decimal256({unscaled}, scale={scale}) -> Float64"
        );

        let as_f32 = cast(&array, &DataType::Float32).unwrap();
        assert_eq!(
            as_f32.as_primitive::<Float32Type>().value(0),
            text.parse::<f32>().unwrap(),
            "Decimal256({unscaled}, scale={scale}) -> Float32"
        );
    }
}

#[test]
fn test_cast_decimal_to_float_saturates_out_of_range() {
    // 1.7e39: past f32::MAX, still far inside f64.
    for (unscaled, want_f32) in [(i128::MAX, f32::INFINITY), (i128::MIN, f32::NEG_INFINITY)] {
        let array = decimal128(unscaled, 38, -1);

        let as_f32 = cast(&array, &DataType::Float32).unwrap();
        assert_eq!(as_f32.as_primitive::<Float32Type>().value(0), want_f32);

        let text = Decimal128Type::format_decimal(unscaled, u8::MAX, -1);
        let as_f64 = cast(&array, &DataType::Float64).unwrap();
        assert_eq!(
            as_f64.as_primitive::<Float64Type>().value(0),
            text.parse::<f64>().unwrap()
        );
    }

    // 1e-76: below the smallest f32 subnormal, representable in an f64.
    let array = Decimal256Array::from(vec![i256::from(1)])
        .with_precision_and_scale(76, 76)
        .unwrap();

    let as_f32 = cast(&array, &DataType::Float32).unwrap();
    assert_eq!(as_f32.as_primitive::<Float32Type>().value(0), 0.0);

    let as_f64 = cast(&array, &DataType::Float64).unwrap();
    assert_eq!(as_f64.as_primitive::<Float64Type>().value(0), 1e-76);
}

#[test]
fn test_single_decimal_to_float_lossy_scale_outside_i8() {
    // `format_decimal` takes an `i8` scale; narrowing 128 into one wraps it to
    // -128 and turns 1e-128 into 1e128. The edges of the `i8` range still round
    // once, and anything past them falls back to the division.
    let as_float = |x: i128| x as f64;
    let lossy =
        |scale: i32| single_decimal_to_float_lossy::<Decimal128Type, _>(&as_float, 1, scale);
    for scale in [i8::MAX, i8::MIN] {
        let text = Decimal128Type::format_decimal(1, u8::MAX, scale);
        assert_eq!(
            lossy(scale.into()),
            text.parse::<f64>().unwrap(),
            "scale={scale}"
        );
    }
    for scale in [128, -129] {
        assert_eq!(lossy(scale), 1.0 / 10_f64.powi(scale), "scale={scale}");
    }
}

// Keeping only `MAX_PRECISION` digits would scale the result by ten.
#[test]
fn test_cast_decimal_to_float_keeps_digits_beyond_declared_precision() {
    // 39 digits, one more than `Decimal128Type::MAX_PRECISION`.
    let array = decimal128(i128::MAX, 38, 0);

    let as_f64 = cast(&array, &DataType::Float64).unwrap();
    assert_eq!(
        as_f64.as_primitive::<Float64Type>().value(0),
        1.7014118346046923e38 // 2^127, the double nearest i128::MAX
    );

    let as_f32 = cast(&array, &DataType::Float32).unwrap();
    assert_eq!(
        as_f32.as_primitive::<Float32Type>().value(0),
        1.7014118e38 // 2^127, the float nearest i128::MAX
    );

    // A precision of 15 (7 for `f32`) promises an integer the float holds
    // exactly, but values are not validated against the precision they declare.
    for (unscaled, precision, scale) in [(12345678901234567890i128, 15u8, 2i8), (16777217, 7, 1)] {
        let text = Decimal128Type::format_decimal(unscaled, u8::MAX, scale);
        let array = decimal128(unscaled, precision, scale);

        let as_f64 = cast(&array, &DataType::Float64).unwrap();
        assert_eq!(
            as_f64.as_primitive::<Float64Type>().value(0),
            text.parse::<f64>().unwrap(),
            "{text} at precision {precision} -> Float64"
        );

        let as_f32 = cast(&array, &DataType::Float32).unwrap();
        assert_eq!(
            as_f32.as_primitive::<Float32Type>().value(0),
            text.parse::<f32>().unwrap(),
            "{text} at precision {precision} -> Float32"
        );
    }
}

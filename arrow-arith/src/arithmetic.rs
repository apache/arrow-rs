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

//! Defines basic arithmetic kernels for `PrimitiveArrays`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use crate::arity::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::*;
use std::cmp::min;
use std::sync::Arc;

/// Returns the precision and scale of the result of a multiplication of two decimal types,
/// and the divisor for fixed point multiplication, if it fits in `i256`.
fn get_fixed_point_info(
    left: (u8, i8),
    right: (u8, i8),
    required_scale: i8,
) -> Result<(u8, i16, Option<i256>), ArrowError> {
    let product_scale = i16::from(left.1) + i16::from(right.1);
    let precision = min(left.0 + right.0 + 1, DECIMAL128_MAX_PRECISION);

    if i16::from(required_scale) > product_scale {
        return Err(ArrowError::ComputeError(format!(
            "Required scale {required_scale} is greater than product scale {product_scale}",
        )));
    }

    let scale_reduction = product_scale - i16::from(required_scale);
    // Every i128 product has magnitude at most 2^254, less than half of 10^77.
    let divisor =
        (scale_reduction <= 76).then(|| i256::from_i128(10).pow_wrapping(scale_reduction as u32));

    Ok((precision, product_scale, divisor))
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply_dyn` or `multiply_dyn_checked` instead.
pub fn multiply_fixed_point_dyn(
    left: &dyn Array,
    right: &dyn Array,
    required_scale: i8,
) -> Result<ArrayRef, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
            let left = left.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let right = right.as_any().downcast_ref::<Decimal128Array>().unwrap();

            multiply_fixed_point(left, right, required_scale).map(|a| Arc::new(a) as ArrayRef)
        }
        (_, _) => Err(ArrowError::CastError(format!(
            "Unsupported data type {}, {}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply` or `multiply_checked` instead.
pub fn multiply_fixed_point_checked(
    left: &PrimitiveArray<Decimal128Type>,
    right: &PrimitiveArray<Decimal128Type>,
    required_scale: i8,
) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let (precision, product_scale, divisor) = get_fixed_point_info(
        (left.precision(), left.scale()),
        (right.precision(), right.scale()),
        required_scale,
    )?;

    if i16::from(required_scale) == product_scale {
        return try_binary::<_, _, _, Decimal128Type>(left, right, |a, b| a.mul_checked(b))?
            .with_precision_and_scale(precision, required_scale);
    }

    let Some(divisor) = divisor else {
        return try_binary::<_, _, _, Decimal128Type>(left, right, |_, _| Ok(0))?
            .with_precision_and_scale(precision, required_scale);
    };

    let round = divide_and_round(divisor);
    let product = try_binary::<_, _, _, Decimal128Type>(left, right, |a, b| {
        let a = i256::from_i128(a);
        let b = i256::from_i128(b);

        let mut mul = a.wrapping_mul(b);
        mul = round(mul);
        mul.to_i128().ok_or_else(|| {
            ArrowError::ArithmeticOverflow(format!("Overflow happened on: {a:?} * {b:?}"))
        })
    })?;
    product.with_precision_and_scale(precision, required_scale)
}

/// Perform `left * right` operation on two decimal arrays. If either left or right value is
/// null then the result is also null.
///
/// This performs decimal multiplication which allows precision loss if an exact representation
/// is not possible for the result, according to the required scale. In the case, the result
/// will be rounded to the required scale.
///
/// If the required scale is greater than the product scale, an error is returned.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `multiply_fixed_point_checked` instead.
///
/// It is implemented for compatibility with precision loss `multiply` function provided by
/// other data processing engines. For multiplication with precision loss detection, use
/// `multiply` or `multiply_checked` instead.
pub fn multiply_fixed_point(
    left: &PrimitiveArray<Decimal128Type>,
    right: &PrimitiveArray<Decimal128Type>,
    required_scale: i8,
) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let (precision, product_scale, divisor) = get_fixed_point_info(
        (left.precision(), left.scale()),
        (right.precision(), right.scale()),
        required_scale,
    )?;

    if i16::from(required_scale) == product_scale {
        return binary(left, right, |a, b| a.mul_wrapping(b))?
            .with_precision_and_scale(precision, required_scale);
    }

    let Some(divisor) = divisor else {
        return binary::<_, _, _, Decimal128Type>(left, right, |_, _| 0)?
            .with_precision_and_scale(precision, required_scale);
    };

    let round = divide_and_round(divisor);
    let product = binary::<_, _, _, Decimal128Type>(left, right, |a, b| {
        let a = i256::from_i128(a);
        let b = i256::from_i128(b);

        let mut mul = a.wrapping_mul(b);
        mul = round(mul);
        mul.as_i128()
    })?;
    product.with_precision_and_scale(precision, required_scale)
}

/// Returns a function that divides a value by `div` and rounds the result half away from zero
fn divide_and_round(div: i256) -> impl Fn(i256) -> i256 {
    let half = div.div_wrapping(i256::from_i128(2));
    let half_neg = half.neg_wrapping();

    move |input| {
        let (d, r) = input.wrapping_div_rem(div);

        // Round result
        match input >= i256::ZERO {
            true if r >= half => d.add_wrapping(i256::ONE),
            false if r <= half_neg => d.sub_wrapping(i256::ONE),
            _ => d,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numeric::mul;

    #[test]
    fn test_decimal_multiply_allow_precision_loss() {
        // Overflow happening as i128 cannot hold multiplying result.
        // [123456789]
        let a = Decimal128Array::from(vec![123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [10]
        let b = Decimal128Array::from(vec![10000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let err = mul(&a, &b).unwrap_err();
        assert!(
            err.to_string().contains(
                "Overflow happened on: 123456789000000000000000000 * 10000000000000000000"
            )
        );

        // Allow precision loss.
        let result = multiply_fixed_point_checked(&a, &b, 28).unwrap();
        // [1234567890]
        let expected = Decimal128Array::from(vec![12345678900000000000000000000000000000])
            .with_precision_and_scale(38, 28)
            .unwrap();

        assert_eq!(&expected, &result);
        assert_eq!(
            result.value_as_string(0),
            "1234567890.0000000000000000000000000000"
        );

        // Rounding case
        // [0.000000000000000001, 123456789.555555555555555555, 1.555555555555555555]
        let a = Decimal128Array::from(vec![1, 123456789555555555555555555, 1555555555555555555])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [1.555555555555555555, 11.222222222222222222, 0.000000000000000001]
        let b = Decimal128Array::from(vec![1555555555555555555, 11222222222222222222, 1])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let result = multiply_fixed_point_checked(&a, &b, 28).unwrap();
        // [
        //    0.0000000000000000015555555556,
        //    1385459527.2345679012071330528765432099,
        //    0.0000000000000000015555555556
        // ]
        let expected = Decimal128Array::from(vec![
            15555555556,
            13854595272345679012071330528765432099,
            15555555556,
        ])
        .with_precision_and_scale(38, 28)
        .unwrap();

        assert_eq!(&expected, &result);

        // Rounded the value "1385459527.234567901207133052876543209876543210".
        assert_eq!(
            result.value_as_string(1),
            "1385459527.2345679012071330528765432099"
        );
        assert_eq!(result.value_as_string(0), "0.0000000000000000015555555556");
        assert_eq!(result.value_as_string(2), "0.0000000000000000015555555556");

        let a = Decimal128Array::from(vec![1230])
            .with_precision_and_scale(4, 2)
            .unwrap();

        let b = Decimal128Array::from(vec![1000])
            .with_precision_and_scale(4, 2)
            .unwrap();

        // Required scale is same as the product of the input scales. Behavior is same as multiply.
        let result = multiply_fixed_point_checked(&a, &b, 4).unwrap();
        assert_eq!(result.precision(), 9);
        assert_eq!(result.scale(), 4);

        let expected = mul(&a, &b).unwrap();
        assert_eq!(expected.as_ref(), &result);

        // Required scale cannot be larger than the product of the input scales.
        let result = multiply_fixed_point_checked(&a, &b, 5).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("Required scale 5 is greater than product scale 4")
        );
    }

    #[test]
    fn test_decimal_multiply_allow_precision_loss_overflow() {
        // [99999999999123456789]
        let a = Decimal128Array::from(vec![99999999999123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [9999999999910]
        let b = Decimal128Array::from(vec![9999999999910000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        let err = multiply_fixed_point_checked(&a, &b, 28).unwrap_err();
        assert!(err.to_string().contains(
            "Overflow happened on: 99999999999123456789000000000000000000 * 9999999999910000000000000000000"
        ));

        let result = multiply_fixed_point(&a, &b, 28).unwrap();
        let expected = Decimal128Array::from(vec![62946009661555981610246871926660136960])
            .with_precision_and_scale(38, 28)
            .unwrap();

        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_decimal_multiply_fixed_point() {
        // [123456789]
        let a = Decimal128Array::from(vec![123456789000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // [10]
        let b = Decimal128Array::from(vec![10000000000000000000])
            .with_precision_and_scale(38, 18)
            .unwrap();

        // `multiply` overflows on this case.
        let err = mul(&a, &b).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Arithmetic overflow: Overflow happened on: 123456789000000000000000000 * 10000000000000000000"
        );

        // Avoid overflow by reducing the scale.
        let result = multiply_fixed_point(&a, &b, 28).unwrap();
        // [1234567890]
        let expected = Decimal128Array::from(vec![12345678900000000000000000000000000000])
            .with_precision_and_scale(38, 28)
            .unwrap();

        assert_eq!(&expected, &result);
        assert_eq!(
            result.value_as_string(0),
            "1234567890.0000000000000000000000000000"
        );
    }

    #[test]
    fn test_decimal_multiply_fixed_point_rounding() {
        // Products below, at and above half for each sign, rounded from scale 1 to scale 0
        let a = Decimal128Array::from(vec![
            Some(14),
            Some(15),
            Some(16),
            Some(-14),
            Some(-15),
            Some(-16),
            Some(25),
            Some(-25),
            None,
        ])
        .with_precision_and_scale(38, 1)
        .unwrap();
        let b = Decimal128Array::from(vec![1, 1, 1, 1, 1, 1, -1, -1, 1])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let expected = Decimal128Array::from(vec![
            Some(1),
            Some(2),
            Some(2),
            Some(-1),
            Some(-2),
            Some(-2),
            Some(-3),
            Some(3),
            None,
        ])
        .with_precision_and_scale(38, 0)
        .unwrap();

        // The slice without the null covers the paths without nulls
        for (a, b, expected) in [
            (a.clone(), b.clone(), expected.clone()),
            (a.slice(0, 8), b.slice(0, 8), expected.slice(0, 8)),
        ] {
            assert_eq!(multiply_fixed_point(&a, &b, 0).unwrap(), expected);
            assert_eq!(multiply_fixed_point_checked(&a, &b, 0).unwrap(), expected);
        }
    }

    fn assert_large_scale_reduction(right_scale: i8, scale: i8, precision: u8) {
        let a = Decimal128Array::from(vec![Some(0), Some(5), Some(5), None, Some(1)])
            .with_precision_and_scale(1, 0)
            .unwrap();
        let b = Decimal128Array::from(vec![Some(0), Some(5), Some(-5), Some(1), None])
            .with_precision_and_scale(if right_scale == 0 { 1 } else { 38 }, right_scale)
            .unwrap();
        let expected = Decimal128Array::from(vec![Some(0), Some(0), Some(0), None, None])
            .with_precision_and_scale(precision, scale)
            .unwrap();
        for len in [5, 3, 0] {
            let (a, b, expected) = (a.slice(0, len), b.slice(0, len), expected.slice(0, len));
            assert_eq!(multiply_fixed_point(&a, &b, scale).unwrap(), expected);
            assert_eq!(
                multiply_fixed_point_checked(&a, &b, scale).unwrap(),
                expected
            );
            assert_eq!(
                multiply_fixed_point_dyn(&a, &b, scale).unwrap().as_ref(),
                &expected
            );
        }
        assert!(multiply_fixed_point(&a, &b.slice(0, 4), scale).is_err());
        assert!(multiply_fixed_point_checked(&a, &b.slice(0, 4), scale).is_err());
        assert!(multiply_fixed_point_dyn(&a, &b.slice(0, 4), scale).is_err());
    }

    #[test]
    fn test_decimal_multiply_fixed_point_large_scale_reduction_77() {
        assert_large_scale_reduction(0, -77, 3);
    }

    #[test]
    fn test_decimal_multiply_fixed_point_large_scale_reduction_128() {
        assert_large_scale_reduction(38, -90, 38);
    }

    #[test]
    fn test_decimal_multiply_fixed_point_scale_reduction_76() {
        let m = 10_i128.pow(38) - 1;
        let a = Decimal128Array::from(vec![m, m])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let b = Decimal128Array::from(vec![m, -m])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let expected = Decimal128Array::from(vec![1, -1])
            .with_precision_and_scale(38, -76)
            .unwrap();
        assert_eq!(multiply_fixed_point(&a, &b, -76).unwrap(), expected);
        assert_eq!(multiply_fixed_point_checked(&a, &b, -76).unwrap(), expected);
        assert_eq!(
            multiply_fixed_point_dyn(&a, &b, -76).unwrap().as_ref(),
            &expected
        );
    }

    #[test]
    fn test_decimal_multiply_fixed_point_negative_product_scale() {
        for (left_scale, right_scale) in [(-128, -1), (-128, -128)] {
            let a = Decimal128Array::from(vec![2])
                .with_precision_and_scale(1, left_scale)
                .unwrap();
            let b = Decimal128Array::from(vec![3])
                .with_precision_and_scale(1, right_scale)
                .unwrap();
            let product_scale = i16::from(left_scale) + i16::from(right_scale);
            for result in [
                multiply_fixed_point(&a, &b, -128),
                multiply_fixed_point_checked(&a, &b, -128),
            ] {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    format!(
                        "Compute error: Required scale -128 is greater than product scale {product_scale}"
                    )
                );
            }
            assert_eq!(
                multiply_fixed_point_dyn(&a, &b, -128)
                    .unwrap_err()
                    .to_string(),
                format!(
                    "Compute error: Required scale -128 is greater than product scale {product_scale}"
                )
            );
        }
        let a = Decimal128Array::from(vec![2])
            .with_precision_and_scale(1, -64)
            .unwrap();
        let b = Decimal128Array::from(vec![3])
            .with_precision_and_scale(1, -64)
            .unwrap();
        let expected = Decimal128Array::from(vec![6])
            .with_precision_and_scale(3, -128)
            .unwrap();
        assert_eq!(multiply_fixed_point(&a, &b, -128).unwrap(), expected);
        assert_eq!(
            multiply_fixed_point_checked(&a, &b, -128).unwrap(),
            expected
        );
        assert_eq!(
            multiply_fixed_point_dyn(&a, &b, -128).unwrap().as_ref(),
            &expected
        );
    }
}

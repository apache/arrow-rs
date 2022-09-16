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

use crate::array::PrimitiveArray;
use crate::compute::{binary, unary};
use crate::datatypes::ArrowNumericType;
use crate::error::Result;
use std::ops::{BitAnd, BitOr, BitXor, Not};

// The helper function for bitwise operation with two array
fn bitwise_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    binary(left, right, op)
}

/// Perform `left & right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_and<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitAnd<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a & b)
}

/// Perform `left | right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_or<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitOr<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a | b)
}

/// Perform `left ^ right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_xor<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitXor<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a ^ b)
}

/// Perform `!array` operation on array. If array value is null
/// then the result is also null.
pub fn bitwise_not<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: Not<Output = T::Native>,
{
    Ok(unary(array, |value| !value))
}

/// Perform bitwise `and` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_and_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitAnd<Output = T::Native>,
{
    Ok(unary(array, |value| value & scalar))
}

/// Perform bitwise `or` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_or_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitOr<Output = T::Native>,
{
    Ok(unary(array, |value| value | scalar))
}

/// Perform bitwise `xor` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_xor_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: BitXor<Output = T::Native>,
{
    Ok(unary(array, |value| value ^ scalar))
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, UInt64Array};
    use crate::compute::kernels::bitwise::{
        bitwise_and, bitwise_and_scalar, bitwise_not, bitwise_or, bitwise_or_scalar,
        bitwise_xor, bitwise_xor_scalar,
    };
    use crate::error::Result;

    #[test]
    fn test_bitwise_and_array() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(5), Some(10), Some(8), Some(12)]);
        let expected = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and(&left, &right)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(5), Some(-10), Some(8), Some(12)]);
        let expected = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and(&left, &right)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_and_array_scalar() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(7), Some(2), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = -20;
        let expected = Int32Array::from(vec![Some(0), Some(0), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_or_array() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(7), Some(5), Some(8), Some(13)]);
        let expected = UInt64Array::from(vec![Some(7), Some(7), None, Some(13)]);
        let result = bitwise_or(&left, &right)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(-7), Some(-5), Some(8), Some(13)]);
        let expected = Int32Array::from(vec![Some(-7), Some(-5), None, Some(13)]);
        let result = bitwise_or(&left, &right)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_not_array() -> Result<()> {
        // unsigned value
        let array = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let expected = UInt64Array::from(vec![
            Some(18446744073709551614),
            Some(18446744073709551613),
            None,
            Some(18446744073709551611),
        ]);
        let result = bitwise_not(&array)?;
        assert_eq!(expected, result);
        // signed value
        let array = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let expected = Int32Array::from(vec![Some(-2), Some(-3), None, Some(-5)]);
        let result = bitwise_not(&array)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_or_array_scalar() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(15), Some(7), None, Some(7)]);
        let result = bitwise_or_scalar(&left, scalar)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = 20;
        let expected = Int32Array::from(vec![Some(21), Some(22), None, Some(20)]);
        let result = bitwise_or_scalar(&left, scalar)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_xor_array() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(7), Some(5), Some(8), Some(13)]);
        let expected = UInt64Array::from(vec![Some(6), Some(7), None, Some(9)]);
        let result = bitwise_xor(&left, &right)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(-7), Some(5), Some(8), Some(-13)]);
        let expected = Int32Array::from(vec![Some(-8), Some(7), None, Some(-9)]);
        let result = bitwise_xor(&left, &right)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_xor_array_scalar() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(8), Some(5), None, Some(3)]);
        let result = bitwise_xor_scalar(&left, scalar)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = -20;
        let expected = Int32Array::from(vec![Some(-19), Some(-18), None, Some(-24)]);
        let result = bitwise_xor_scalar(&left, scalar)?;
        assert_eq!(expected, result);
        Ok(())
    }
}

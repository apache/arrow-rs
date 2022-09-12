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
use crate::error::{ArrowError, Result};
use std::ops::BitAnd;

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
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }
    Ok(binary(left, right, op))
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

/// Perform bitwise and  every value in an array with the scalar. If any value in the array is null then the
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

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, UInt64Array};
    use crate::compute::kernels::bitwise::{bitwise_and, bitwise_and_scalar};
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
        let right = Int32Array::from(vec![Some(5), Some(10), Some(8), Some(12)]);
        let expected = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and(&left, &right)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_and_array_scalar() -> Result<()> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar)?;
        assert_eq!(expected, result);
        Ok(())
    }
}

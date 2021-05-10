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

//! Defines kernels suitable to perform operations to primitive arrays.

use crate::array::{Array, ArrayData, PrimitiveArray};
use crate::buffer::Buffer;
use crate::datatypes::ArrowPrimitiveType;
use std::iter;

#[inline]
fn into_primitive_array_data<I: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    array: &PrimitiveArray<I>,
    buffer: Buffer,
) -> ArrayData {
    ArrayData::new(
        O::DATA_TYPE,
        array.len(),
        None,
        array.data_ref().null_buffer().cloned(),
        0,
        vec![buffer],
        vec![],
    )
}

/// Applies an unary and infalible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweights the cost of branching nulls and non-nulls.
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infalible for any value of the corresponding type
/// or this function may panic.
/// # Example
/// ```rust
/// # use arrow::array::Int32Array;
/// # use arrow::datatypes::Int32Type;
/// # use arrow::compute::kernels::arity::unary;
/// # fn main() {
/// let array = Int32Array::from(vec![Some(5), Some(7), None]);
/// let c = unary::<_, _, Int32Type>(&array, |x| x * 2 + 1);
/// assert_eq!(c, Int32Array::from(vec![Some(11), Some(15), None]));
/// # }
/// ```
pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F) -> PrimitiveArray<O>
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(I::Native) -> O::Native,
{
    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = into_primitive_array_data::<_, O>(array, buffer);
    PrimitiveArray::<O>::from(data)
}

/// Applies a nullary and infalible function to generate a primitive array.
/// You should use this instead of the vectorized version when each generated value is different.
pub fn nullary<F, O>(size: usize, op: F) -> PrimitiveArray<O>
where
    O: ArrowPrimitiveType,
    F: Fn() -> O::Native,
{
    let values = iter::repeat_with(op).take(size);
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };
    let data = ArrayData::new(O::DATA_TYPE, size, None, None, 0, vec![buffer], vec![]);
    PrimitiveArray::<O>::from(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

    #[test]
    fn test_unary() {
        let original = Int32Array::from(vec![1, 2, 3]);
        let mapped: Int32Array = unary(&original, |i| i * 2);
        assert_eq!(mapped.len(), original.len());
        assert_eq!(mapped.value(1), 4);
    }

    #[test]
    fn test_nullary() {
        let generated: Int32Array = nullary(1, || 1i32);
        assert_eq!(generated.len(), 1);
        assert_eq!(generated.value(0), 1);

        let generated: Int32Array = nullary(0, || 1i32);
        assert_eq!(generated.len(), 0);
    }
}

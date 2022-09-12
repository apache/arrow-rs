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

use crate::array::{
    Array, ArrayData, ArrayIter, ArrayRef, BufferBuilder, DictionaryArray, PrimitiveArray,
};
use crate::buffer::Buffer;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{ArrowNumericType, ArrowPrimitiveType};
use crate::downcast_dictionary_array;
use crate::error::{ArrowError, Result};
use crate::util::bit_iterator::try_for_each_valid_idx;
use std::sync::Arc;

#[inline]
unsafe fn build_primitive_array<O: ArrowPrimitiveType>(
    len: usize,
    buffer: Buffer,
    null_count: usize,
    null_buffer: Option<Buffer>,
) -> PrimitiveArray<O> {
    PrimitiveArray::from(ArrayData::new_unchecked(
        O::DATA_TYPE,
        len,
        Some(null_count),
        null_buffer,
        0,
        vec![buffer],
        vec![],
    ))
}

/// Applies an unary and infallible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweigh the cost of branching nulls and non-nulls.
///
/// # Implementation
///
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infallible for any value of the corresponding type
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
    let data = array.data();
    let len = data.len();
    let null_count = data.null_count();

    let null_buffer = data
        .null_buffer()
        .map(|b| b.bit_slice(data.offset(), data.len()));

    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };
    unsafe { build_primitive_array(len, buffer, null_count, null_buffer) }
}

/// Applies a unary and fallible function to all valid values in a primitive array
///
/// This is unlike [`unary`] which will apply an infallible function to all rows regardless
/// of validity, in many cases this will be significantly faster and should be preferred
/// if `op` is infallible.
///
/// Note: LLVM is currently unable to effectively vectorize fallible operations
pub fn try_unary<I, F, O>(array: &PrimitiveArray<I>, op: F) -> Result<PrimitiveArray<O>>
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(I::Native) -> Result<O::Native>,
{
    let len = array.len();
    let null_count = array.null_count();

    let mut buffer = BufferBuilder::<O::Native>::new(len);
    buffer.append_n_zeroed(array.len());
    let slice = buffer.as_slice_mut();

    let null_buffer = array
        .data_ref()
        .null_buffer()
        .map(|b| b.bit_slice(array.offset(), array.len()));

    try_for_each_valid_idx(array.len(), 0, null_count, null_buffer.as_deref(), |idx| {
        unsafe { *slice.get_unchecked_mut(idx) = op(array.value_unchecked(idx))? };
        Ok::<_, ArrowError>(())
    })?;

    Ok(unsafe { build_primitive_array(len, buffer.finish(), null_count, null_buffer) })
}

/// A helper function that applies an unary function to a dictionary array with primitive value type.
fn unary_dict<K, F, T>(array: &DictionaryArray<K>, op: F) -> Result<ArrayRef>
where
    K: ArrowNumericType,
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    let dict_values = array.values().as_any().downcast_ref().unwrap();
    let values = unary::<T, F, T>(dict_values, op).into_data();
    let data = array.data().clone().into_builder().child_data(vec![values]);

    let new_dict: DictionaryArray<K> = unsafe { data.build_unchecked() }.into();
    Ok(Arc::new(new_dict))
}

/// Applies an unary function to an array with primitive values.
pub fn unary_dyn<F, T>(array: &dyn Array, op: F) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    downcast_dictionary_array! {
        array => unary_dict::<_, F, T>(array, op),
        t => {
            if t == &T::DATA_TYPE {
                Ok(Arc::new(unary::<T, F, T>(
                    array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
                    op,
                )))
            } else {
                Err(ArrowError::NotYetImplemented(format!(
                    "Cannot perform unary operation on array of type {}",
                    t
                )))
            }
        }
    }
}

/// Given two arrays of length `len`, calls `op(a[i], b[i])` for `i` in `0..len`, collecting
/// the results in a [`PrimitiveArray`]. If any index is null in either `a` or `b`, the
/// corresponding index in the result will also be null
///
/// Like [`unary`] the provided function is evaluated for every index, ignoring validity. This
/// is beneficial when the cost of the operation is low compared to the cost of branching, and
/// especially when the operation can be vectorised, however, requires `op` to be infallible
/// for all possible values of its inputs
///
/// # Panic
///
/// Panics if the arrays have different lengths
pub fn binary<A, B, F, O>(
    a: &PrimitiveArray<A>,
    b: &PrimitiveArray<B>,
    op: F,
) -> PrimitiveArray<O>
where
    A: ArrowPrimitiveType,
    B: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(A::Native, B::Native) -> O::Native,
{
    assert_eq!(a.len(), b.len());
    let len = a.len();

    if a.is_empty() {
        return PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE));
    }

    let null_buffer = combine_option_bitmap(&[a.data(), b.data()], len).unwrap();
    let null_count = null_buffer
        .as_ref()
        .map(|x| len - x.count_set_bits())
        .unwrap_or_default();

    let values = a.values().iter().zip(b.values()).map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size from a PrimitiveArray
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    unsafe { build_primitive_array(len, buffer, null_count, null_buffer) }
}

/// Applies the provided fallible binary operation across `a` and `b`, returning any error,
/// and collecting the results into a [`PrimitiveArray`]. If any index is null in either `a`
/// or `b`, the corresponding index in the result will also be null
///
/// Like [`try_unary`] the function is only evaluated for non-null indices
///
/// # Panic
///
/// Panics if the arrays have different lengths
pub fn try_binary<A, B, F, O>(
    a: &PrimitiveArray<A>,
    b: &PrimitiveArray<B>,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    A: ArrowPrimitiveType,
    B: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(A::Native, B::Native) -> Result<O::Native>,
{
    assert_eq!(a.len(), b.len());
    let len = a.len();

    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }

    let null_buffer = combine_option_bitmap(&[a.data(), b.data()], len).unwrap();
    let null_count = null_buffer
        .as_ref()
        .map(|x| len - x.count_set_bits())
        .unwrap_or_default();

    let mut buffer = BufferBuilder::<O::Native>::new(len);
    buffer.append_n_zeroed(len);
    let slice = buffer.as_slice_mut();

    try_for_each_valid_idx(len, 0, null_count, null_buffer.as_deref(), |idx| {
        unsafe {
            *slice.get_unchecked_mut(idx) =
                op(a.value_unchecked(idx), b.value_unchecked(idx))?
        };
        Ok::<_, ArrowError>(())
    })?;

    Ok(unsafe { build_primitive_array(len, buffer.finish(), null_count, null_buffer) })
}

/// Applies the provided binary operation across `a` and `b`, collecting the optional results
/// into a [`PrimitiveArray`]. If any index is null in either `a` or `b`, the corresponding
/// index in the result will also be null. The binary operation could return `None` which
/// results in a new null in the collected [`PrimitiveArray`].
///
/// The function is only evaluated for non-null indices
///
/// # Panic
///
/// Panics if the arrays have different lengths
pub(crate) fn binary_opt<A, B, F, O>(
    a: &PrimitiveArray<A>,
    b: &PrimitiveArray<B>,
    op: F,
) -> PrimitiveArray<O>
where
    A: ArrowPrimitiveType,
    B: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(A::Native, B::Native) -> Option<O::Native>,
{
    assert_eq!(a.len(), b.len());

    if a.is_empty() {
        return PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE));
    }

    let iter_a = ArrayIter::new(a);
    let iter_b = ArrayIter::new(b);

    let values = iter_a
        .into_iter()
        .zip(iter_b.into_iter())
        .map(|(item_a, item_b)| {
            if let (Some(a), Some(b)) = (item_a, item_b) {
                op(a, b)
            } else {
                None
            }
        });

    values.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{as_primitive_array, Float64Array, PrimitiveDictionaryBuilder};
    use crate::datatypes::{Float64Type, Int32Type, Int8Type};

    #[test]
    fn test_unary_f64_slice() {
        let input =
            Float64Array::from(vec![Some(5.1f64), None, Some(6.8), None, Some(7.2)]);
        let input_slice = input.slice(1, 4);
        let input_slice: &Float64Array = as_primitive_array(&input_slice);
        let result = unary(input_slice, |n| n.round());
        assert_eq!(
            result,
            Float64Array::from(vec![None, Some(7.0), None, Some(7.0)])
        );

        let result = unary_dyn::<_, Float64Type>(input_slice, |n| n + 1.0).unwrap();

        assert_eq!(
            result.as_any().downcast_ref::<Float64Array>().unwrap(),
            &Float64Array::from(vec![None, Some(7.8), None, Some(8.2)])
        );
    }

    #[test]
    fn test_unary_dict_and_unary_dyn() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(5).unwrap();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null();
        builder.append(9).unwrap();
        let dictionary_array = builder.finish();

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append(9).unwrap();
        builder.append_null();
        builder.append(10).unwrap();
        let expected = builder.finish();

        let result = unary_dict::<_, _, Int32Type>(&dictionary_array, |n| n + 1).unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap(),
            &expected
        );

        let result = unary_dyn::<_, Int32Type>(&dictionary_array, |n| n + 1).unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap(),
            &expected
        );
    }
}

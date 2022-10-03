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
    Array, ArrayAccessor, ArrayData, ArrayIter, ArrayRef, BufferBuilder, DictionaryArray,
    PrimitiveArray,
};
use crate::buffer::Buffer;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{ArrowNumericType, ArrowPrimitiveType};
use crate::downcast_dictionary_array;
use crate::error::{ArrowError, Result};
use crate::util::bit_iterator::try_for_each_valid_idx;
use arrow_buffer::MutableBuffer;
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

/// See [`PrimitiveArray::unary`]
pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F) -> PrimitiveArray<O>
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(I::Native) -> O::Native,
{
    array.unary(op)
}

/// See [`PrimitiveArray::try_unary`]
pub fn try_unary<I, F, O>(array: &PrimitiveArray<I>, op: F) -> Result<PrimitiveArray<O>>
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(I::Native) -> Result<O::Native>,
{
    array.try_unary(op)
}

/// A helper function that applies an infallible unary function to a dictionary array with primitive value type.
fn unary_dict<K, F, T>(array: &DictionaryArray<K>, op: F) -> Result<ArrayRef>
where
    K: ArrowNumericType,
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    let dict_values = array.values().as_any().downcast_ref().unwrap();
    let values = unary::<T, F, T>(dict_values, op);
    Ok(Arc::new(array.with_values(&values)))
}

/// A helper function that applies a fallible unary function to a dictionary array with primitive value type.
fn try_unary_dict<K, F, T>(array: &DictionaryArray<K>, op: F) -> Result<ArrayRef>
where
    K: ArrowNumericType,
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> Result<T::Native>,
{
    if array.value_type() != T::DATA_TYPE {
        return Err(ArrowError::CastError(format!(
            "Cannot perform the unary operation on dictionary array of value type {}",
            array.value_type()
        )));
    }

    let dict_values = array.values().as_any().downcast_ref().unwrap();
    let values = try_unary::<T, F, T>(dict_values, op)?;
    Ok(Arc::new(array.with_values(&values)))
}

/// Applies an infallible unary function to an array with primitive values.
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

/// Applies a fallible unary function to an array with primitive values.
pub fn try_unary_dyn<F, T>(array: &dyn Array, op: F) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> Result<T::Native>,
{
    downcast_dictionary_array! {
        array => if array.values().data_type() == &T::DATA_TYPE {
            try_unary_dict::<_, F, T>(array, op)
        } else {
            Err(ArrowError::NotYetImplemented(format!(
                "Cannot perform unary operation on dictionary array of type {}",
                array.data_type()
            )))
        },
        t => {
            if t == &T::DATA_TYPE {
                Ok(Arc::new(try_unary::<T, F, T>(
                    array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
                    op,
                )?))
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
/// # Error
///
/// This function gives error if the arrays have different lengths
pub fn binary<A, B, F, O>(
    a: &PrimitiveArray<A>,
    b: &PrimitiveArray<B>,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    A: ArrowPrimitiveType,
    B: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(A::Native, B::Native) -> O::Native,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform binary operation on arrays of different length".to_string(),
        ));
    }
    let len = a.len();

    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
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

    Ok(unsafe { build_primitive_array(len, buffer, null_count, null_buffer) })
}

/// Applies the provided fallible binary operation across `a` and `b`, returning any error,
/// and collecting the results into a [`PrimitiveArray`]. If any index is null in either `a`
/// or `b`, the corresponding index in the result will also be null
///
/// Like [`try_unary`] the function is only evaluated for non-null indices
///
/// # Error
///
/// Return an error if the arrays have different lengths or
/// the operation is under erroneous
pub fn try_binary<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Result<O::Native>,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }
    let len = a.len();

    if a.null_count() == 0 && b.null_count() == 0 {
        try_binary_no_nulls(len, a, b, op)
    } else {
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

        Ok(unsafe {
            build_primitive_array(len, buffer.finish(), null_count, null_buffer)
        })
    }
}

/// This intentional inline(never) attribute helps LLVM optimize the loop.
#[inline(never)]
fn try_binary_no_nulls<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    len: usize,
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Result<O::Native>,
{
    let mut buffer = MutableBuffer::new(len * O::get_byte_width());
    for idx in 0..len {
        unsafe {
            buffer.push_unchecked(op(a.value_unchecked(idx), b.value_unchecked(idx))?);
        };
    }
    Ok(unsafe { build_primitive_array(len, buffer.into(), 0, None) })
}

#[inline(never)]
fn try_binary_opt_no_nulls<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    len: usize,
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Option<O::Native>,
{
    let mut buffer = Vec::with_capacity(10);
    for idx in 0..len {
        unsafe {
            buffer.push(op(a.value_unchecked(idx), b.value_unchecked(idx)));
        };
    }
    Ok(buffer.iter().collect())
}

/// Applies the provided binary operation across `a` and `b`, collecting the optional results
/// into a [`PrimitiveArray`]. If any index is null in either `a` or `b`, the corresponding
/// index in the result will also be null. The binary operation could return `None` which
/// results in a new null in the collected [`PrimitiveArray`].
///
/// The function is only evaluated for non-null indices
///
/// # Error
///
/// This function gives error if the arrays have different lengths
pub(crate) fn binary_opt<A: ArrayAccessor + Array, B: ArrayAccessor + Array, F, O>(
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Option<O::Native>,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform binary operation on arrays of different length".to_string(),
        ));
    }

    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }

    if a.null_count() == 0 && b.null_count() == 0 {
        return try_binary_opt_no_nulls(a.len(), a, b, op);
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

    Ok(values.collect())
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

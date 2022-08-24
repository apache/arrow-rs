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

use crate::array::{Array, ArrayData, ArrayRef, DictionaryArray, PrimitiveArray};
use crate::buffer::Buffer;
use crate::datatypes::{
    ArrowNumericType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::error::{ArrowError, Result};
use std::sync::Arc;

#[inline]
fn into_primitive_array_data<I: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    array: &PrimitiveArray<I>,
    buffer: Buffer,
) -> ArrayData {
    unsafe {
        ArrayData::new_unchecked(
            O::DATA_TYPE,
            array.len(),
            None,
            array
                .data_ref()
                .null_buffer()
                .map(|b| b.bit_slice(array.offset(), array.len())),
            0,
            vec![buffer],
            vec![],
        )
    }
}

/// Applies an unary and infallible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweights the cost of branching nulls and non-nulls.
/// # Implementation
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

/// A helper function that applies an unary function to a dictionary array with primitive value type.
fn unary_dict<K, F, T>(array: &DictionaryArray<K>, op: F) -> Result<ArrayRef>
where
    K: ArrowNumericType,
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    let dict_values = array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();

    let values = dict_values
        .iter()
        .map(|v| v.map(&op))
        .collect::<PrimitiveArray<T>>();

    let keys = array.keys();

    let mut data = ArrayData::builder(array.data_type().clone())
        .len(keys.len())
        .add_buffer(keys.data().buffers()[0].clone())
        .add_child_data(values.data().clone());

    match keys.data().null_buffer() {
        Some(buffer) if keys.data().null_count() > 0 => {
            data = data
                .null_bit_buffer(Some(buffer.clone()))
                .null_count(keys.data().null_count());
        }
        _ => data = data.null_count(0),
    }

    let new_dict: DictionaryArray<K> = unsafe { data.build_unchecked() }.into();
    Ok(Arc::new(new_dict))
}

/// Applies an unary function to an array with primitive values.
pub fn unary_dyn<F, T>(array: &dyn Array, op: F) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    match array.data_type() {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int8Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int16 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int16Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int32 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int64 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int64Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt8 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt8Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt16 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt32 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt64 => unary_dict::<_, F, T>(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt64Type>>()
                    .unwrap(),
                op,
            ),
            t => Err(ArrowError::NotYetImplemented(format!(
                "Cannot perform unary operation on dictionary array of key type {}.",
                t
            ))),
        },
        _ => Ok(Arc::new(unary::<T, F, T>(
            array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
            op,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        as_primitive_array, Float64Array, PrimitiveBuilder, PrimitiveDictionaryBuilder,
    };
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
        let key_builder = PrimitiveBuilder::<Int8Type>::with_capacity(3);
        let value_builder = PrimitiveBuilder::<Int32Type>::with_capacity(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(5).unwrap();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null();
        builder.append(9).unwrap();
        let dictionary_array = builder.finish();

        let key_builder = PrimitiveBuilder::<Int8Type>::with_capacity(3);
        let value_builder = PrimitiveBuilder::<Int32Type>::with_capacity(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
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

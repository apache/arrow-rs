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
    Array, ArrayData, ArrayRef, DictionaryArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use crate::buffer::Buffer;
use crate::datatypes::{
    ArrowNumericType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::error::{ArrowError, Result};

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

macro_rules! unary_dict_op {
    ($array: expr, $op: expr, $value_ty: ty) => {{
        // Safety justification: Since the inputs are valid Arrow arrays, all values are
        // valid indexes into the dictionary (which is verified during construction)

        let array_iter = unsafe {
            $array
                .values()
                .as_any()
                .downcast_ref::<$value_ty>()
                .unwrap()
                .take_iter_unchecked($array.keys_iter())
        };

        let values = array_iter
            .map(|v| {
                if let Some(value) = v {
                    Some($op(value))
                } else {
                    None
                }
            })
            .collect();

        Ok(values)
    }};
}

/// Applies an unary function to a dictionary array with primitive value type.
pub fn unary_dict<K, F, T>(array: &DictionaryArray<K>, op: F) -> Result<PrimitiveArray<T>>
where
    K: ArrowNumericType,
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    unary_dict_op!(array, op, PrimitiveArray<T>)
}

macro_rules! unary_op {
    ($array: expr, $op: expr, $op_type: ty, $value_ty: ty) => {{
        match $array.data_type() {
            DataType::Int8 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Int16 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Int32 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Int64 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::UInt8 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::UInt16 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::UInt32 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::UInt64 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Float16 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Float32 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            DataType::Float64 => Ok(unary::<$value_ty, $op_type, $value_ty>(
                $array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$value_ty>>()
                    .unwrap(),
                $op,
            )),
            t => Err(ArrowError::NotYetImplemented(format!(
                "Cannot perform unary operation on array of type {}.",
                t
            ))),
        }
    }};
}

pub fn unary_dyn<F, T>(array: &dyn Array, op: F) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    match array.data_type() {
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int8Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int16 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int16Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int32 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap(),
                op,
            ),
            DataType::Int64 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int64Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt8 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt8Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt16 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt32 => unary_dict(
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap(),
                op,
            ),
            DataType::UInt64 => unary_dict(
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
        _ => Ok(unary::<T, F, T>(
            array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
            op,
        )), // unary_op!(array, op, F, T),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        as_primitive_array, Float64Array, Int32Array, PrimitiveBuilder,
        PrimitiveDictionaryBuilder,
    };
    use crate::datatypes::{Int32Type, Int8Type};

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
        )
    }

    #[test]
    fn test_unary_dict() {
        let key_builder = PrimitiveBuilder::<Int8Type>::new(3);
        let value_builder = PrimitiveBuilder::<Int32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(5).unwrap();
        builder.append(6).unwrap();
        builder.append(7).unwrap();
        builder.append(8).unwrap();
        builder.append_null().unwrap();
        builder.append(9).unwrap();
        let dictionary_array = builder.finish();

        let result = unary_dict(&dictionary_array, |n| n + 1).unwrap();
        assert_eq!(
            result,
            Int32Array::from(vec![Some(6), Some(7), Some(8), Some(9), None, Some(10)])
        )
    }
}

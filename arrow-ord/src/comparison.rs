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

//! Comparison kernels for `Array`s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.
//!

use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::buffer::buffer_unary_not;
use arrow_buffer::{bit_util, Buffer, MutableBuffer};
use arrow_data::bit_mask::combine_option_bitmap;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};
use arrow_select::take::take;

/// Helper function to perform boolean lambda function on values from two array accessors, this
/// version does not attempt to use SIMD.
fn compare_op<T: ArrayAccessor, S: ArrayAccessor, F>(
    left: T,
    right: S,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(T::Item, S::Item) -> bool,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    Ok(BooleanArray::from_binary(left, right, op))
}

/// Helper function to perform boolean lambda function on values from array accessor, this
/// version does not attempt to use SIMD.
fn compare_op_scalar<T: ArrayAccessor, F>(
    left: T,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(T::Item) -> bool,
{
    Ok(BooleanArray::from_unary(left, op))
}

/// Evaluate `op(left, right)` for [`PrimitiveArray`]s using a specified
/// comparison function.
pub fn no_simd_compare_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op(left, right, op)
}

/// Evaluate `op(left, right)` for [`PrimitiveArray`] and scalar using
/// a specified comparison function.
pub fn no_simd_compare_op_scalar<T, F>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op_scalar(left, |l| op(l, right))
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a == b)
}

fn utf8_empty<OffsetSize: OffsetSizeTrait, const EQ: bool>(
    left: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    let null_bit_buffer = left
        .data()
        .null_buffer()
        .map(|b| b.bit_slice(left.offset(), left.len()));

    let buffer = unsafe {
        MutableBuffer::from_trusted_len_iter_bool(left.value_offsets().windows(2).map(
            |offset| {
                if EQ {
                    offset[1].as_usize() == offset[0].as_usize()
                } else {
                    offset[1].as_usize() > offset[0].as_usize()
                }
            },
        ))
    };

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from(buffer)],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    if right.is_empty() {
        return utf8_empty::<_, true>(left);
    }
    compare_op_scalar(left, |a| a == right)
}

/// Perform `left == right` operation on [`BooleanArray`]
pub fn eq_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| !(a ^ b))
}

/// Perform `left != right` operation on [`BooleanArray`]
pub fn neq_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| (a ^ b))
}

/// Perform `left < right` operation on [`BooleanArray`]
pub fn lt_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| ((!a) & b))
}

/// Perform `left <= right` operation on [`BooleanArray`]
pub fn lt_eq_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| !(a & (!b)))
}

/// Perform `left > right` operation on [`BooleanArray`]
pub fn gt_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| (a & (!b)))
}

/// Perform `left >= right` operation on [`BooleanArray`]
pub fn gt_eq_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| !((!a) & b))
}

/// Perform `left == right` operation on [`BooleanArray`] and a scalar
pub fn eq_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let len = left.len();
    let left_offset = left.offset();

    let values = if right {
        left.values().bit_slice(left_offset, len)
    } else {
        buffer_unary_not(left.values(), left.offset(), left.len())
    };

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            left.data_ref()
                .null_bitmap()
                .as_ref()
                .map(|b| b.buffer().bit_slice(left_offset, len)),
            0,
            vec![values],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

/// Perform `left < right` operation on [`BooleanArray`] and a scalar
pub fn lt_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a: bool| !a & right)
}

/// Perform `left <= right` operation on [`BooleanArray`] and a scalar
pub fn lt_eq_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a <= right)
}

/// Perform `left > right` operation on [`BooleanArray`] and a scalar
pub fn gt_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a: bool| a & !right)
}

/// Perform `left >= right` operation on [`BooleanArray`] and a scalar
pub fn gt_eq_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a >= right)
}

/// Perform `left != right` operation on [`BooleanArray`] and a scalar
pub fn neq_bool_scalar(
    left: &BooleanArray,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    eq_bool_scalar(left, !right)
}

/// Perform `left == right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a == b)
}

/// Perform `left == right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar
pub fn eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a == right)
}

/// Perform `left != right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn neq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a != b)
}

/// Perform `left != right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
pub fn neq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a != right)
}

/// Perform `left < right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn lt_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a < b)
}

/// Perform `left < right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
pub fn lt_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a < right)
}

/// Perform `left <= right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn lt_eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a <= b)
}

/// Perform `left <= right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
pub fn lt_eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a <= right)
}

/// Perform `left > right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn gt_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a > b)
}

/// Perform `left > right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
pub fn gt_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a > right)
}

/// Perform `left >= right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
pub fn gt_eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a >= b)
}

/// Perform `left >= right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
pub fn gt_eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a >= right)
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn neq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a != b)
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn neq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    if right.is_empty() {
        return utf8_empty::<_, false>(left);
    }
    compare_op_scalar(left, |a| a != right)
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn lt_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a < b)
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn lt_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a < right)
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn lt_eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a <= b)
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn lt_eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a <= right)
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn gt_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a > b)
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn gt_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a > right)
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`].
pub fn gt_eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |a, b| a >= b)
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
pub fn gt_eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |a| a >= right)
}

// Avoids creating a closure for each combination of `$RIGHT` and `$TY`
fn try_to_type_result<T>(
    value: Option<T>,
    right: &str,
    ty: &str,
) -> Result<T, ArrowError> {
    value.ok_or_else(|| {
        ArrowError::ComputeError(format!("Could not convert {} with {}", right, ty,))
    })
}

/// Calls $RIGHT.$TY() (e.g. `right.to_i128()`) with a nice error message.
/// Type of expression is `Result<.., ArrowError>`
macro_rules! try_to_type {
    ($RIGHT: expr, $TY: ident) => {
        try_to_type_result($RIGHT.$TY(), stringify!($RIGHT), stringify!($TYPE))
    };
}

macro_rules! dyn_compare_scalar {
    // Applies `LEFT OP RIGHT` when `LEFT` is a `PrimitiveArray`
    ($LEFT: expr, $RIGHT: expr, $OP: ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => {
                let right = try_to_type!($RIGHT, to_i8)?;
                let left = as_primitive_array::<Int8Type>($LEFT);
                $OP::<Int8Type>(left, right)
            }
            DataType::Int16 => {
                let right = try_to_type!($RIGHT, to_i16)?;
                let left = as_primitive_array::<Int16Type>($LEFT);
                $OP::<Int16Type>(left, right)
            }
            DataType::Int32 => {
                let right = try_to_type!($RIGHT, to_i32)?;
                let left = as_primitive_array::<Int32Type>($LEFT);
                $OP::<Int32Type>(left, right)
            }
            DataType::Int64 => {
                let right = try_to_type!($RIGHT, to_i64)?;
                let left = as_primitive_array::<Int64Type>($LEFT);
                $OP::<Int64Type>(left, right)
            }
            DataType::UInt8 => {
                let right = try_to_type!($RIGHT, to_u8)?;
                let left = as_primitive_array::<UInt8Type>($LEFT);
                $OP::<UInt8Type>(left, right)
            }
            DataType::UInt16 => {
                let right = try_to_type!($RIGHT, to_u16)?;
                let left = as_primitive_array::<UInt16Type>($LEFT);
                $OP::<UInt16Type>(left, right)
            }
            DataType::UInt32 => {
                let right = try_to_type!($RIGHT, to_u32)?;
                let left = as_primitive_array::<UInt32Type>($LEFT);
                $OP::<UInt32Type>(left, right)
            }
            DataType::UInt64 => {
                let right = try_to_type!($RIGHT, to_u64)?;
                let left = as_primitive_array::<UInt64Type>($LEFT);
                $OP::<UInt64Type>(left, right)
            }
            DataType::Float32 => {
                let right = try_to_type!($RIGHT, to_f32)?;
                let left = as_primitive_array::<Float32Type>($LEFT);
                $OP::<Float32Type>(left, right)
            }
            DataType::Float64 => {
                let right = try_to_type!($RIGHT, to_f64)?;
                let left = as_primitive_array::<Float64Type>($LEFT);
                $OP::<Float64Type>(left, right)
            }
            DataType::Decimal128(_, _) => {
                let right = try_to_type!($RIGHT, to_i128)?;
                let left = as_primitive_array::<Decimal128Type>($LEFT);
                $OP::<Decimal128Type>(left, right)
            }
            _ => Err(ArrowError::ComputeError(format!(
                "Unsupported data type {:?} for comparison {} with {:?}",
                $LEFT.data_type(),
                stringify!($OP),
                $RIGHT
            ))),
        }
    }};
    // Applies `LEFT OP RIGHT` when `LEFT` is a `DictionaryArray` with keys of type `KT`
    ($LEFT: expr, $RIGHT: expr, $KT: ident, $OP: ident) => {{
        match $KT.as_ref() {
            DataType::UInt8 => {
                let left = as_dictionary_array::<UInt8Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::UInt16 => {
                let left = as_dictionary_array::<UInt16Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::UInt32 => {
                let left = as_dictionary_array::<UInt32Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::UInt64 => {
                let left = as_dictionary_array::<UInt64Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::Int8 => {
                let left = as_dictionary_array::<Int8Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::Int16 => {
                let left = as_dictionary_array::<Int16Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::Int32 => {
                let left = as_dictionary_array::<Int32Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            DataType::Int64 => {
                let left = as_dictionary_array::<Int64Type>($LEFT);
                unpack_dict_comparison(left, $OP(left.values(), $RIGHT)?)
            }
            _ => Err(ArrowError::ComputeError(format!(
                "Unsupported dictionary key type {:?}",
                $KT.as_ref()
            ))),
        }
    }};
}

macro_rules! dyn_compare_utf8_scalar {
    ($LEFT: expr, $RIGHT: expr, $KT: ident, $OP: ident) => {{
        match $KT.as_ref() {
            DataType::UInt8 => {
                let left = as_dictionary_array::<UInt8Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::UInt16 => {
                let left = as_dictionary_array::<UInt16Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::UInt32 => {
                let left = as_dictionary_array::<UInt32Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::UInt64 => {
                let left = as_dictionary_array::<UInt64Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::Int8 => {
                let left = as_dictionary_array::<Int8Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::Int16 => {
                let left = as_dictionary_array::<Int16Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::Int32 => {
                let left = as_dictionary_array::<Int32Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            DataType::Int64 => {
                let left = as_dictionary_array::<Int64Type>($LEFT);
                let values = as_string_array(left.values());
                unpack_dict_comparison(left, $OP(values, $RIGHT)?)
            }
            _ => Err(ArrowError::ComputeError(String::from("Unknown key type"))),
        }
    }};
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, eq_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, eq_scalar),
    }
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, lt_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, lt_scalar),
    }
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt_eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, lt_eq_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, lt_eq_scalar),
    }
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, gt_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, gt_scalar),
    }
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt_eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, gt_eq_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, gt_eq_scalar),
    }
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn neq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    match left.data_type() {
        DataType::Dictionary(key_type, _value_type) => {
            dyn_compare_scalar!(left, right, key_type, neq_dyn_scalar)
        }
        _ => dyn_compare_scalar!(left, right, neq_scalar),
    }
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn eq_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            eq_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            eq_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "eq_dyn_binary_scalar only supports Binary or LargeBinary arrays".to_string(),
        )),
    }
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn neq_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            neq_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            neq_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "neq_dyn_binary_scalar only supports Binary or LargeBinary arrays"
                .to_string(),
        )),
    }
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn lt_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            lt_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            lt_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_dyn_binary_scalar only supports Binary or LargeBinary arrays".to_string(),
        )),
    }
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn lt_eq_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            lt_eq_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            lt_eq_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_eq_dyn_binary_scalar only supports Binary or LargeBinary arrays"
                .to_string(),
        )),
    }
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn gt_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            gt_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            gt_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_dyn_binary_scalar only supports Binary or LargeBinary arrays".to_string(),
        )),
    }
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
pub fn gt_eq_dyn_binary_scalar(
    left: &dyn Array,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Binary => {
            let left = as_generic_binary_array::<i32>(left);
            gt_eq_binary_scalar(left, right)
        }
        DataType::LargeBinary => {
            let left = as_generic_binary_array::<i64>(left);
            gt_eq_binary_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_eq_dyn_binary_scalar only supports Binary or LargeBinary arrays"
                .to_string(),
        )),
    }
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn eq_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, eq_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            eq_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            eq_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn lt_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, lt_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "lt_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            lt_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            lt_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn gt_eq_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, gt_eq_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "gt_eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            gt_eq_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            gt_eq_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn lt_eq_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, lt_eq_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "lt_eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            lt_eq_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            lt_eq_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_eq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn gt_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, gt_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "gt_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            gt_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            gt_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
pub fn neq_dyn_utf8_scalar(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                dyn_compare_utf8_scalar!(left, right, key_type, neq_utf8_scalar)
            }
            _ => Err(ArrowError::ComputeError(
                "neq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            )),
        },
        DataType::Utf8 => {
            let left = as_string_array(left);
            neq_utf8_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            neq_utf8_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "neq_dyn_utf8_scalar only supports Utf8 or LargeUtf8 arrays".to_string(),
        )),
    };
    result
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value.
pub fn eq_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            eq_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "eq_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
pub fn lt_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            lt_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
pub fn gt_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            gt_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
pub fn lt_eq_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            lt_eq_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "lt_eq_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
pub fn gt_eq_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            gt_eq_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "gt_eq_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
pub fn neq_dyn_bool_scalar(
    left: &dyn Array,
    right: bool,
) -> Result<BooleanArray, ArrowError> {
    let result = match left.data_type() {
        DataType::Boolean => {
            let left = as_boolean_array(left);
            neq_bool_scalar(left, right)
        }
        _ => Err(ArrowError::ComputeError(
            "neq_dyn_bool_scalar only supports BooleanArray".to_string(),
        )),
    };
    result
}

/// unpacks the results of comparing left.values (as a boolean)
///
/// TODO add example
///
fn unpack_dict_comparison<K>(
    dict: &DictionaryArray<K>,
    dict_comparison: BooleanArray,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    K::Native: num::ToPrimitive,
{
    // TODO: Use take_boolean (#2967)
    let array = take(&dict_comparison, dict.keys(), None)?;
    Ok(BooleanArray::from(array.data().clone()))
}

/// Helper function to perform boolean lambda function on values from two arrays using
/// SIMD.
#[cfg(feature = "simd")]
fn simd_compare_op<T, SI, SC>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    simd_op: SI,
    scalar_op: SC,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    SI: Fn(T::Simd, T::Simd) -> T::SimdMask,
    SC: Fn(T::Native, T::Native) -> bool,
{
    use std::borrow::BorrowMut;

    let len = left.len();
    if len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(&[left.data_ref(), right.data_ref()], len);

    // we process the data in chunks so that each iteration results in one u64 of comparison result bits
    const CHUNK_SIZE: usize = 64;
    let lanes = T::lanes();

    // this is currently the case for all our datatypes and allows us to always append full bytes
    assert!(
        lanes <= CHUNK_SIZE,
        "Number of vector lanes must be at most 64"
    );

    let buffer_size = bit_util::ceil(len, 8);
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    let mut left_chunks = left.values().chunks_exact(CHUNK_SIZE);
    let mut right_chunks = right.values().chunks_exact(CHUNK_SIZE);

    let result_chunks = result.typed_data_mut();
    let result_remainder = left_chunks
        .borrow_mut()
        .zip(right_chunks.borrow_mut())
        .fold(result_chunks, |result_slice, (left_slice, right_slice)| {
            let mut i = 0;
            let mut bitmask = 0_u64;
            while i < CHUNK_SIZE {
                let simd_left = T::load(&left_slice[i..]);
                let simd_right = T::load(&right_slice[i..]);
                let simd_result = simd_op(simd_left, simd_right);

                let m = T::mask_to_u64(&simd_result);
                bitmask |= m << i;

                i += lanes;
            }
            let bytes = bitmask.to_le_bytes();
            result_slice[0..8].copy_from_slice(&bytes);

            &mut result_slice[8..]
        });

    let left_remainder = left_chunks.remainder();
    let right_remainder = right_chunks.remainder();

    assert_eq!(left_remainder.len(), right_remainder.len());

    if !left_remainder.is_empty() {
        let remainder_bitmask = left_remainder
            .iter()
            .zip(right_remainder.iter())
            .enumerate()
            .fold(0_u64, |mut mask, (i, (scalar_left, scalar_right))| {
                let bit = scalar_op(*scalar_left, *scalar_right) as u64;
                mask |= bit << i;
                mask
            });
        let remainder_mask_as_bytes =
            &remainder_bitmask.to_le_bytes()[0..bit_util::ceil(left_remainder.len(), 8)];
        result_remainder.copy_from_slice(remainder_mask_as_bytes);
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            None,
            null_bit_buffer,
            0,
            vec![result.into()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Helper function to perform boolean lambda function on values from an array and a scalar value using
/// SIMD.
#[cfg(feature = "simd")]
fn simd_compare_op_scalar<T, SI, SC>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    simd_op: SI,
    scalar_op: SC,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    SI: Fn(T::Simd, T::Simd) -> T::SimdMask,
    SC: Fn(T::Native, T::Native) -> bool,
{
    use std::borrow::BorrowMut;

    let len = left.len();

    // we process the data in chunks so that each iteration results in one u64 of comparison result bits
    const CHUNK_SIZE: usize = 64;
    let lanes = T::lanes();

    // this is currently the case for all our datatypes and allows us to always append full bytes
    assert!(
        lanes <= CHUNK_SIZE,
        "Number of vector lanes must be at most 64"
    );

    let buffer_size = bit_util::ceil(len, 8);
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    let mut left_chunks = left.values().chunks_exact(CHUNK_SIZE);
    let simd_right = T::init(right);

    let result_chunks = result.typed_data_mut();
    let result_remainder =
        left_chunks
            .borrow_mut()
            .fold(result_chunks, |result_slice, left_slice| {
                let mut i = 0;
                let mut bitmask = 0_u64;
                while i < CHUNK_SIZE {
                    let simd_left = T::load(&left_slice[i..]);
                    let simd_result = simd_op(simd_left, simd_right);

                    let m = T::mask_to_u64(&simd_result);
                    bitmask |= m << i;

                    i += lanes;
                }
                let bytes = bitmask.to_le_bytes();
                result_slice[0..8].copy_from_slice(&bytes);

                &mut result_slice[8..]
            });

    let left_remainder = left_chunks.remainder();

    if !left_remainder.is_empty() {
        let remainder_bitmask = left_remainder.iter().enumerate().fold(
            0_u64,
            |mut mask, (i, scalar_left)| {
                let bit = scalar_op(*scalar_left, right) as u64;
                mask |= bit << i;
                mask
            },
        );
        let remainder_mask_as_bytes =
            &remainder_bitmask.to_le_bytes()[0..bit_util::ceil(left_remainder.len(), 8)];
        result_remainder.copy_from_slice(remainder_mask_as_bytes);
    }

    let null_bit_buffer = left
        .data_ref()
        .null_buffer()
        .map(|b| b.bit_slice(left.offset(), left.len()));

    // null count is the same as in the input since the right side of the scalar comparison cannot be null
    let null_count = left.null_count();

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            len,
            Some(null_count),
            null_bit_buffer,
            0,
            vec![result.into()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

fn cmp_primitive_array<T: ArrowPrimitiveType, F>(
    left: &dyn Array,
    right: &dyn Array,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(T::Native, T::Native) -> bool,
{
    let left_array = as_primitive_array::<T>(left);
    let right_array = as_primitive_array::<T>(right);
    compare_op(left_array, right_array, op)
}

#[cfg(feature = "dyn_cmp_dict")]
macro_rules! typed_dict_non_dict_cmp {
    ($LEFT: expr, $RIGHT: expr, $LEFT_KEY_TYPE: expr, $RIGHT_TYPE: tt, $OP_BOOL: expr, $OP: expr) => {{
        match $LEFT_KEY_TYPE {
            DataType::Int8 => {
                let left = as_dictionary_array::<Int8Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int16 => {
                let left = as_dictionary_array::<Int16Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int32 => {
                let left = as_dictionary_array::<Int32Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int64 => {
                let left = as_dictionary_array::<Int64Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt8 => {
                let left = as_dictionary_array::<UInt8Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt16 => {
                let left = as_dictionary_array::<UInt16Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt32 => {
                let left = as_dictionary_array::<UInt32Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt64 => {
                let left = as_dictionary_array::<UInt64Type>($LEFT);
                cmp_dict_primitive::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            t => Err(ArrowError::NotYetImplemented(format!(
                "Cannot compare dictionary array of key type {}",
                t
            ))),
        }
    }};
}

#[cfg(feature = "dyn_cmp_dict")]
macro_rules! typed_dict_string_array_cmp {
    ($LEFT: expr, $RIGHT: expr, $LEFT_KEY_TYPE: expr, $RIGHT_TYPE: tt, $OP: expr) => {{
        match $LEFT_KEY_TYPE {
            DataType::Int8 => {
                let left = as_dictionary_array::<Int8Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int16 => {
                let left = as_dictionary_array::<Int16Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int32 => {
                let left = as_dictionary_array::<Int32Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::Int64 => {
                let left = as_dictionary_array::<Int64Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt8 => {
                let left = as_dictionary_array::<UInt8Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt16 => {
                let left = as_dictionary_array::<UInt16Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt32 => {
                let left = as_dictionary_array::<UInt32Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            DataType::UInt64 => {
                let left = as_dictionary_array::<UInt64Type>($LEFT);
                cmp_dict_string_array::<_, $RIGHT_TYPE, _>(left, $RIGHT, $OP)
            }
            t => Err(ArrowError::NotYetImplemented(format!(
                "Cannot compare dictionary array of key type {}",
                t
            ))),
        }
    }};
}

#[cfg(feature = "dyn_cmp_dict")]
macro_rules! typed_cmp_dict_non_dict {
    ($LEFT: expr, $RIGHT: expr, $OP_BOOL: expr, $OP: expr, $OP_FLOAT: expr) => {{
       match ($LEFT.data_type(), $RIGHT.data_type()) {
        (DataType::Dictionary(left_key_type, left_value_type), right_type) => {
            match (left_value_type.as_ref(), right_type) {
                (DataType::Boolean, DataType::Boolean) => {
                    let left = $LEFT;
                    downcast_dictionary_array!(
                        left => {
                            cmp_dict_boolean_array::<_, _>(left, $RIGHT, $OP)
                        }
                        _ => Err(ArrowError::NotYetImplemented(format!(
                            "Cannot compare dictionary array of key type {}",
                            left_key_type.as_ref()
                        ))),
                    )
                }
                (DataType::Int8, DataType::Int8) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Int8Type, $OP_BOOL, $OP)
                }
                (DataType::Int16, DataType::Int16) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Int16Type, $OP_BOOL, $OP)
                }
                (DataType::Int32, DataType::Int32) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Int32Type, $OP_BOOL, $OP)
                }
                (DataType::Int64, DataType::Int64) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Int64Type, $OP_BOOL, $OP)
                }
                (DataType::UInt8, DataType::UInt8) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), UInt8Type, $OP_BOOL, $OP)
                }
                (DataType::UInt16, DataType::UInt16) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), UInt16Type, $OP_BOOL, $OP)
                }
                (DataType::UInt32, DataType::UInt32) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), UInt32Type, $OP_BOOL, $OP)
                }
                (DataType::UInt64, DataType::UInt64) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), UInt64Type, $OP_BOOL, $OP)
                }
                (DataType::Float32, DataType::Float32) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Float32Type, $OP_BOOL, $OP_FLOAT)
                }
                (DataType::Float64, DataType::Float64) => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Float64Type, $OP_BOOL, $OP_FLOAT)
                }
                (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2)) if s1 == s2 => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Decimal128Type, $OP_BOOL, $OP)
                }
                (DataType::Decimal256(_, s1), DataType::Decimal256(_, s2)) if s1 == s2 => {
                    typed_dict_non_dict_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), Decimal256Type, $OP_BOOL, $OP)
                }
                (DataType::Utf8, DataType::Utf8) => {
                    typed_dict_string_array_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), i32, $OP)
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    typed_dict_string_array_cmp!($LEFT, $RIGHT, left_key_type.as_ref(), i64, $OP)
                }
                (DataType::Binary, DataType::Binary) => {
                    let left = $LEFT;
                    downcast_dictionary_array!(
                        left => {
                            cmp_dict_binary_array::<_, i32, _>(left, $RIGHT, $OP)
                        }
                        _ => Err(ArrowError::NotYetImplemented(format!(
                            "Cannot compare dictionary array of key type {}",
                            left_key_type.as_ref()
                        ))),
                    )
                }
                (DataType::LargeBinary, DataType::LargeBinary) => {
                    let left = $LEFT;
                    downcast_dictionary_array!(
                        left => {
                            cmp_dict_binary_array::<_, i64, _>(left, $RIGHT, $OP)
                        }
                        _ => Err(ArrowError::NotYetImplemented(format!(
                            "Cannot compare dictionary array of key type {}",
                            left_key_type.as_ref()
                        ))),
                    )
                }
                (t1, t2) if t1 == t2 => Err(ArrowError::NotYetImplemented(format!(
                    "Comparing dictionary array of type {} with array of type {} is not yet implemented",
                    t1, t2
                ))),
                (t1, t2) => Err(ArrowError::CastError(format!(
                    "Cannot compare dictionary array with array of different value types ({} and {})",
                    t1, t2
                ))),
            }
        }
        _ => unreachable!("Should not reach this branch"),
    }
    }};
}

#[cfg(not(feature = "dyn_cmp_dict"))]
macro_rules! typed_cmp_dict_non_dict {
    ($LEFT: expr, $RIGHT: expr, $OP_BOOL: expr, $OP: expr, $OP_FLOAT: expr) => {{
        Err(ArrowError::CastError(format!(
            "Comparing dictionary array of type {} with array of type {} requires \"dyn_cmp_dict\" feature",
            $LEFT.data_type(), $RIGHT.data_type()
        )))
    }}
}

macro_rules! typed_compares {
    ($LEFT: expr, $RIGHT: expr, $OP_BOOL: expr, $OP: expr, $OP_FLOAT: expr) => {{
        match ($LEFT.data_type(), $RIGHT.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                compare_op(as_boolean_array($LEFT), as_boolean_array($RIGHT), $OP_BOOL)
            }
            (DataType::Int8, DataType::Int8) => {
                cmp_primitive_array::<Int8Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int16, DataType::Int16) => {
                cmp_primitive_array::<Int16Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int32, DataType::Int32) => {
                cmp_primitive_array::<Int32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int64, DataType::Int64) => {
                cmp_primitive_array::<Int64Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt8, DataType::UInt8) => {
                cmp_primitive_array::<UInt8Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt16, DataType::UInt16) => {
                cmp_primitive_array::<UInt16Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt32, DataType::UInt32) => {
                cmp_primitive_array::<UInt32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt64, DataType::UInt64) => {
                cmp_primitive_array::<UInt64Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Float32, DataType::Float32) => {
                cmp_primitive_array::<Float32Type, _>($LEFT, $RIGHT, $OP_FLOAT)
            }
            (DataType::Float64, DataType::Float64) => {
                cmp_primitive_array::<Float64Type, _>($LEFT, $RIGHT, $OP_FLOAT)
            }
            (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2)) if s1 == s2 => {
                cmp_primitive_array::<Decimal128Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Decimal256(_, s1), DataType::Decimal256(_, s2)) if s1 == s2 => {
                cmp_primitive_array::<Decimal256Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Utf8, DataType::Utf8) => {
                compare_op(as_string_array($LEFT), as_string_array($RIGHT), $OP)
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => compare_op(
                as_largestring_array($LEFT),
                as_largestring_array($RIGHT),
                $OP,
            ),
            (DataType::FixedSizeBinary(_), DataType::FixedSizeBinary(_)) => {
                let lhs = $LEFT
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let rhs = $RIGHT
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();

                compare_op(lhs, rhs, $OP)
            }
            (DataType::Binary, DataType::Binary) => compare_op(
                as_generic_binary_array::<i32>($LEFT),
                as_generic_binary_array::<i32>($RIGHT),
                $OP,
            ),
            (DataType::LargeBinary, DataType::LargeBinary) => compare_op(
                as_generic_binary_array::<i64>($LEFT),
                as_generic_binary_array::<i64>($RIGHT),
                $OP,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, _),
                DataType::Timestamp(TimeUnit::Nanosecond, _),
            ) => cmp_primitive_array::<TimestampNanosecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Timestamp(TimeUnit::Microsecond, _),
            ) => cmp_primitive_array::<TimestampMicrosecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Timestamp(TimeUnit::Millisecond, _),
                DataType::Timestamp(TimeUnit::Millisecond, _),
            ) => cmp_primitive_array::<TimestampMillisecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Timestamp(TimeUnit::Second, _),
                DataType::Timestamp(TimeUnit::Second, _),
            ) => cmp_primitive_array::<TimestampSecondType, _>($LEFT, $RIGHT, $OP),
            (DataType::Date32, DataType::Date32) => {
                cmp_primitive_array::<Date32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Date64, DataType::Date64) => {
                cmp_primitive_array::<Date64Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Time32(TimeUnit::Second), DataType::Time32(TimeUnit::Second)) => {
                cmp_primitive_array::<Time32SecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time32(TimeUnit::Millisecond),
            ) => cmp_primitive_array::<Time32MillisecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Time64(TimeUnit::Microsecond),
            ) => cmp_primitive_array::<Time64MicrosecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Time64(TimeUnit::Nanosecond),
            ) => cmp_primitive_array::<Time64NanosecondType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::YearMonth),
            ) => cmp_primitive_array::<IntervalYearMonthType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Interval(IntervalUnit::DayTime),
                DataType::Interval(IntervalUnit::DayTime),
            ) => cmp_primitive_array::<IntervalDayTimeType, _>($LEFT, $RIGHT, $OP),
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ) => cmp_primitive_array::<IntervalMonthDayNanoType, _>($LEFT, $RIGHT, $OP),
            (t1, t2) if t1 == t2 => Err(ArrowError::NotYetImplemented(format!(
                "Comparing arrays of type {} is not yet implemented",
                t1
            ))),
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot compare two arrays of different types ({} and {})",
                t1, t2
            ))),
        }
    }};
}

/// Applies $OP to $LEFT and $RIGHT which are two dictionaries which have (the same) key type $KT
#[cfg(feature = "dyn_cmp_dict")]
macro_rules! typed_dict_cmp {
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $OP_FLOAT: expr, $OP_BOOL: expr, $KT: tt) => {{
        match ($LEFT.value_type(), $RIGHT.value_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                cmp_dict_bool::<$KT, _>($LEFT, $RIGHT, $OP_BOOL)
            }
            (DataType::Int8, DataType::Int8) => {
                cmp_dict::<$KT, Int8Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int16, DataType::Int16) => {
                cmp_dict::<$KT, Int16Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int32, DataType::Int32) => {
                cmp_dict::<$KT, Int32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Int64, DataType::Int64) => {
                cmp_dict::<$KT, Int64Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt8, DataType::UInt8) => {
                cmp_dict::<$KT, UInt8Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt16, DataType::UInt16) => {
                cmp_dict::<$KT, UInt16Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt32, DataType::UInt32) => {
                cmp_dict::<$KT, UInt32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::UInt64, DataType::UInt64) => {
                cmp_dict::<$KT, UInt64Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Float32, DataType::Float32) => {
                cmp_dict::<$KT, Float32Type, _>($LEFT, $RIGHT, $OP_FLOAT)
            }
            (DataType::Float64, DataType::Float64) => {
                cmp_dict::<$KT, Float64Type, _>($LEFT, $RIGHT, $OP_FLOAT)
            }
            (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2)) if s1 == s2 => {
                cmp_dict::<$KT, Decimal128Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Decimal256(_, s1), DataType::Decimal256(_, s2)) if s1 == s2 => {
                cmp_dict::<$KT, Decimal256Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Utf8, DataType::Utf8) => {
                cmp_dict_utf8::<$KT, i32, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                cmp_dict_utf8::<$KT, i64, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Binary, DataType::Binary) => {
               cmp_dict_binary::<$KT, i32, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::LargeBinary, DataType::LargeBinary) => {
                cmp_dict_binary::<$KT, i64, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Timestamp(TimeUnit::Nanosecond, _),
                DataType::Timestamp(TimeUnit::Nanosecond, _),
            ) => {
                cmp_dict::<$KT, TimestampNanosecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Timestamp(TimeUnit::Microsecond, _),
            ) => {
                cmp_dict::<$KT, TimestampMicrosecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Timestamp(TimeUnit::Millisecond, _),
                DataType::Timestamp(TimeUnit::Millisecond, _),
            ) => {
                cmp_dict::<$KT, TimestampMillisecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Timestamp(TimeUnit::Second, _),
                DataType::Timestamp(TimeUnit::Second, _),
            ) => {
                cmp_dict::<$KT, TimestampSecondType, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Date32, DataType::Date32) => {
                cmp_dict::<$KT, Date32Type, _>($LEFT, $RIGHT, $OP)
            }
            (DataType::Date64, DataType::Date64) => {
                cmp_dict::<$KT, Date64Type, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Time32(TimeUnit::Second),
                DataType::Time32(TimeUnit::Second),
            ) => {
                cmp_dict::<$KT, Time32SecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time32(TimeUnit::Millisecond),
            ) => {
                cmp_dict::<$KT, Time32MillisecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Time64(TimeUnit::Microsecond),
            ) => {
                cmp_dict::<$KT, Time64MicrosecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Time64(TimeUnit::Nanosecond),
                DataType::Time64(TimeUnit::Nanosecond),
            ) => {
                cmp_dict::<$KT, Time64NanosecondType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::YearMonth),
            ) => {
                cmp_dict::<$KT, IntervalYearMonthType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Interval(IntervalUnit::DayTime),
                DataType::Interval(IntervalUnit::DayTime),
            ) => {
                cmp_dict::<$KT, IntervalDayTimeType, _>($LEFT, $RIGHT, $OP)
            }
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ) => {
                cmp_dict::<$KT, IntervalMonthDayNanoType, _>($LEFT, $RIGHT, $OP)
            }
            (t1, t2) if t1 == t2 => Err(ArrowError::NotYetImplemented(format!(
                "Comparing dictionary arrays of value type {} is not yet implemented",
                t1
            ))),
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot compare two dictionary arrays of different value types ({} and {})",
                t1, t2
            ))),
        }
    }};
}

#[cfg(feature = "dyn_cmp_dict")]
macro_rules! typed_dict_compares {
   // Applies `LEFT OP RIGHT` when `LEFT` and `RIGHT` both are `DictionaryArray`
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $OP_FLOAT: expr, $OP_BOOL: expr) => {{
        match ($LEFT.data_type(), $RIGHT.data_type()) {
            (DataType::Dictionary(left_key_type, _), DataType::Dictionary(right_key_type, _))=> {
                match (left_key_type.as_ref(), right_key_type.as_ref()) {
                    (DataType::Int8, DataType::Int8) => {
                        let left = as_dictionary_array::<Int8Type>($LEFT);
                        let right = as_dictionary_array::<Int8Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, Int8Type)
                    }
                    (DataType::Int16, DataType::Int16) => {
                        let left = as_dictionary_array::<Int16Type>($LEFT);
                        let right = as_dictionary_array::<Int16Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, Int16Type)
                    }
                    (DataType::Int32, DataType::Int32) => {
                        let left = as_dictionary_array::<Int32Type>($LEFT);
                        let right = as_dictionary_array::<Int32Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, Int32Type)
                    }
                    (DataType::Int64, DataType::Int64) => {
                        let left = as_dictionary_array::<Int64Type>($LEFT);
                        let right = as_dictionary_array::<Int64Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, Int64Type)
                    }
                    (DataType::UInt8, DataType::UInt8) => {
                        let left = as_dictionary_array::<UInt8Type>($LEFT);
                        let right = as_dictionary_array::<UInt8Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, UInt8Type)
                    }
                    (DataType::UInt16, DataType::UInt16) => {
                        let left = as_dictionary_array::<UInt16Type>($LEFT);
                        let right = as_dictionary_array::<UInt16Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, UInt16Type)
                    }
                    (DataType::UInt32, DataType::UInt32) => {
                        let left = as_dictionary_array::<UInt32Type>($LEFT);
                        let right = as_dictionary_array::<UInt32Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, UInt32Type)
                    }
                    (DataType::UInt64, DataType::UInt64) => {
                        let left = as_dictionary_array::<UInt64Type>($LEFT);
                        let right = as_dictionary_array::<UInt64Type>($RIGHT);
                        typed_dict_cmp!(left, right, $OP, $OP_FLOAT, $OP_BOOL, UInt64Type)
                    }
                    (t1, t2) if t1 == t2 => Err(ArrowError::NotYetImplemented(format!(
                        "Comparing dictionary arrays of type {} is not yet implemented",
                        t1
                    ))),
                    (t1, t2) => Err(ArrowError::CastError(format!(
                        "Cannot compare two dictionary arrays of different key types ({} and {})",
                        t1, t2
                    ))),
                }
            }
            (t1, t2) => Err(ArrowError::CastError(format!(
                "Cannot compare dictionary array with non-dictionary array ({} and {})",
                t1, t2
            ))),
        }
    }};
}

#[cfg(not(feature = "dyn_cmp_dict"))]
macro_rules! typed_dict_compares {
    ($LEFT: expr, $RIGHT: expr, $OP: expr, $OP_FLOAT: expr, $OP_BOOL: expr) => {{
        Err(ArrowError::CastError(format!(
            "Comparing array of type {} with array of type {} requires \"dyn_cmp_dict\" feature",
            $LEFT.data_type(), $RIGHT.data_type()
        )))
    }}
}

/// Perform given operation on `DictionaryArray` and `PrimitiveArray`. The value
/// type of `DictionaryArray` is same as `PrimitiveArray`'s type.
#[cfg(feature = "dyn_cmp_dict")]
fn cmp_dict_primitive<K, T, F>(
    left: &DictionaryArray<K>,
    right: &dyn Array,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    T: ArrowPrimitiveType + Sync + Send,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op(
        left.downcast_dict::<PrimitiveArray<T>>().unwrap(),
        as_primitive_array::<T>(right),
        op,
    )
}

/// Perform given operation on `DictionaryArray` and `GenericStringArray`. The value
/// type of `DictionaryArray` is same as `GenericStringArray`'s type.
#[cfg(feature = "dyn_cmp_dict")]
fn cmp_dict_string_array<K, OffsetSize: OffsetSizeTrait, F>(
    left: &DictionaryArray<K>,
    right: &dyn Array,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(&str, &str) -> bool,
{
    compare_op(
        left.downcast_dict::<GenericStringArray<OffsetSize>>()
            .unwrap(),
        right
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap(),
        op,
    )
}

/// Perform given operation on `DictionaryArray` and `BooleanArray`. The value
/// type of `DictionaryArray` is same as `BooleanArray`'s type.
#[cfg(feature = "dyn_cmp_dict")]
fn cmp_dict_boolean_array<K, F>(
    left: &DictionaryArray<K>,
    right: &dyn Array,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(bool, bool) -> bool,
{
    compare_op(
        left.downcast_dict::<BooleanArray>().unwrap(),
        right.as_any().downcast_ref::<BooleanArray>().unwrap(),
        op,
    )
}

/// Perform given operation on `DictionaryArray` and `GenericBinaryArray`. The value
/// type of `DictionaryArray` is same as `GenericBinaryArray`'s type.
#[cfg(feature = "dyn_cmp_dict")]
fn cmp_dict_binary_array<K, OffsetSize: OffsetSizeTrait, F>(
    left: &DictionaryArray<K>,
    right: &dyn Array,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(&[u8], &[u8]) -> bool,
{
    compare_op(
        left.downcast_dict::<GenericBinaryArray<OffsetSize>>()
            .unwrap(),
        right
            .as_any()
            .downcast_ref::<GenericBinaryArray<OffsetSize>>()
            .unwrap(),
        op,
    )
}

/// Perform given operation on two `DictionaryArray`s which value type is
/// primitive type. Returns an error if the two arrays have different value
/// type
#[cfg(feature = "dyn_cmp_dict")]
pub fn cmp_dict<K, T, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    T: ArrowPrimitiveType + Sync + Send,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op(
        left.downcast_dict::<PrimitiveArray<T>>().unwrap(),
        right.downcast_dict::<PrimitiveArray<T>>().unwrap(),
        op,
    )
}

/// Perform the given operation on two `DictionaryArray`s which value type is
/// `DataType::Boolean`.
#[cfg(feature = "dyn_cmp_dict")]
pub fn cmp_dict_bool<K, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(bool, bool) -> bool,
{
    compare_op(
        left.downcast_dict::<BooleanArray>().unwrap(),
        right.downcast_dict::<BooleanArray>().unwrap(),
        op,
    )
}

/// Perform the given operation on two `DictionaryArray`s which value type is
/// `DataType::Utf8` or `DataType::LargeUtf8`.
#[cfg(feature = "dyn_cmp_dict")]
pub fn cmp_dict_utf8<K, OffsetSize: OffsetSizeTrait, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(&str, &str) -> bool,
{
    compare_op(
        left.downcast_dict::<GenericStringArray<OffsetSize>>()
            .unwrap(),
        right
            .downcast_dict::<GenericStringArray<OffsetSize>>()
            .unwrap(),
        op,
    )
}

/// Perform the given operation on two `DictionaryArray`s which value type is
/// `DataType::Binary` or `DataType::LargeBinary`.
#[cfg(feature = "dyn_cmp_dict")]
pub fn cmp_dict_binary<K, OffsetSize: OffsetSizeTrait, F>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    K: ArrowPrimitiveType,
    F: Fn(&[u8], &[u8]) -> bool,
{
    compare_op(
        left.downcast_dict::<GenericBinaryArray<OffsetSize>>()
            .unwrap(),
        right
            .downcast_dict::<GenericBinaryArray<OffsetSize>>()
            .unwrap(),
        op,
    )
}

/// Perform `left == right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::{StringArray, BooleanArray};
/// use arrow_ord::comparison::eq_dyn;
/// let array1 = StringArray::from(vec![Some("foo"), None, Some("bar")]);
/// let array2 = StringArray::from(vec![Some("foo"), None, Some("baz")]);
/// let result = eq_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(true), None, Some(false)]), result);
/// ```
pub fn eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a == b, |a, b| a.is_eq(b), |a, b| a
                == b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a == b, |a, b| a == b, |a, b| a
                .is_eq(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a == b, |a, b| a == b, |a, b| b
                .is_eq(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| !(a ^ b), |a, b| a == b, |a, b| a
                .is_eq(b))
        }
    }
}

/// Perform `left != right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::{BinaryArray, BooleanArray};
/// use arrow_ord::comparison::neq_dyn;
/// let values1: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36])];
/// let values2: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x00])];
/// let array1 = BinaryArray::from(values1);
/// let array2 = BinaryArray::from(values2);
/// let result = neq_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(false), None, Some(true)]), result);
/// ```
pub fn neq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a != b, |a, b| a.is_ne(b), |a, b| a
                != b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a != b, |a, b| a != b, |a, b| a
                .is_ne(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a != b, |a, b| a != b, |a, b| b
                .is_ne(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| (a ^ b), |a, b| a != b, |a, b| a
                .is_ne(b))
        }
    }
}

/// Perform `left < right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::{PrimitiveArray, BooleanArray};
/// use arrow_array::types::Int32Type;
/// use arrow_ord::comparison::lt_dyn;
/// let array1: PrimitiveArray<Int32Type> = PrimitiveArray::from(vec![Some(0), Some(1), Some(2)]);
/// let array2: PrimitiveArray<Int32Type> = PrimitiveArray::from(vec![Some(1), Some(1), None]);
/// let result = lt_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(true), Some(false), None]), result);
/// ```
#[allow(clippy::bool_comparison)]
pub fn lt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a < b, |a, b| a.is_lt(b), |a, b| a
                < b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a < b, |a, b| a < b, |a, b| a
                .is_lt(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a > b, |a, b| a > b, |a, b| b
                .is_lt(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| ((!a) & b), |a, b| a < b, |a, b| a
                .is_lt(b))
        }
    }
}

/// Perform `left <= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::{PrimitiveArray, BooleanArray};
/// use arrow_array::types::Date32Type;
/// use arrow_ord::comparison::lt_eq_dyn;
/// let array1: PrimitiveArray<Date32Type> = vec![Some(12356), Some(13548), Some(-365), Some(365)].into();
/// let array2: PrimitiveArray<Date32Type> = vec![Some(12355), Some(13548), Some(-364), None].into();
/// let result = lt_eq_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(false), Some(true), Some(true), None]), result);
/// ```
pub fn lt_eq_dyn(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a <= b, |a, b| a.is_le(b), |a, b| a
                <= b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a <= b, |a, b| a <= b, |a, b| a
                .is_le(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a >= b, |a, b| a >= b, |a, b| b
                .is_le(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| !(a & (!b)), |a, b| a <= b, |a, b| a
                .is_le(b))
        }
    }
}

/// Perform `left > right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::BooleanArray;
/// use arrow_ord::comparison::gt_dyn;
/// let array1 = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let array2 = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let result = gt_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(true), Some(false), None]), result);
/// ```
#[allow(clippy::bool_comparison)]
pub fn gt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a > b, |a, b| a.is_gt(b), |a, b| a
                > b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a > b, |a, b| a > b, |a, b| a
                .is_gt(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a < b, |a, b| a < b, |a, b| b
                .is_gt(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| (a & (!b)), |a, b| a > b, |a, b| a
                .is_gt(b))
        }
    }
}

/// Perform `left >= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
///
/// # Example
/// ```
/// use arrow_array::{BooleanArray, StringArray};
/// use arrow_ord::comparison::gt_eq_dyn;
/// let array1 = StringArray::from(vec![Some(""), Some("aaa"), None]);
/// let array2 = StringArray::from(vec![Some(" "), Some("aa"), None]);
/// let result = gt_eq_dyn(&array1, &array2).unwrap();
/// assert_eq!(BooleanArray::from(vec![Some(false), Some(true), None]), result);
/// ```
pub fn gt_eq_dyn(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Dictionary(_, _)
            if matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_dict_compares!(left, right, |a, b| a >= b, |a, b| a.is_ge(b), |a, b| a
                >= b)
        }
        DataType::Dictionary(_, _)
            if !matches!(right.data_type(), DataType::Dictionary(_, _)) =>
        {
            typed_cmp_dict_non_dict!(left, right, |a, b| a >= b, |a, b| a >= b, |a, b| a
                .is_ge(b))
        }
        _ if matches!(right.data_type(), DataType::Dictionary(_, _)) => {
            typed_cmp_dict_non_dict!(right, left, |a, b| a <= b, |a, b| a <= b, |a, b| b
                .is_ge(a))
        }
        _ => {
            typed_compares!(left, right, |a, b| !((!a) & b), |a, b| a >= b, |a, b| a
                .is_ge(b))
        }
    }
}

/// Perform `left == right` operation on two [`PrimitiveArray`]s.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_eq(b));
}

/// Perform `left == right` operation on a [`PrimitiveArray`] and a scalar value.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn eq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::eq, |a, b| a == b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_eq(right));
}

/// Applies an unary and infallible comparison function to a primitive array.
pub fn unary_cmp<T, F>(
    left: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native) -> bool,
{
    compare_op_scalar(left, op)
}

/// Perform `left != right` operation on two [`PrimitiveArray`]s.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn neq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_ne(b));
}

/// Perform `left != right` operation on a [`PrimitiveArray`] and a scalar value.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn neq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::ne, |a, b| a != b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_ne(right));
}

/// Perform `left < right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
/// values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::lt, |a, b| a < b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_lt(b));
}

/// Perform `left < right` operation on a [`PrimitiveArray`] and a scalar value.
/// Null values are less than non-null values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::lt, |a, b| a < b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_lt(right));
}

/// Perform `left <= right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
/// values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::le, |a, b| a <= b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_le(b));
}

/// Perform `left <= right` operation on a [`PrimitiveArray`] and a scalar value.
/// Null values are less than non-null values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn lt_eq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::le, |a, b| a <= b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_le(right));
}

/// Perform `left > right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
/// values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::gt, |a, b| a > b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_gt(b));
}

/// Perform `left > right` operation on a [`PrimitiveArray`] and a scalar value.
/// Non-null values are greater than null values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::gt, |a, b| a > b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_gt(right));
}

/// Perform `left >= right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
/// values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op(left, right, T::ge, |a, b| a >= b);
    #[cfg(not(feature = "simd"))]
    return compare_op(left, right, |a, b| a.is_ge(b));
}

/// Perform `left >= right` operation on a [`PrimitiveArray`] and a scalar value.
/// Non-null values are greater than null values.
///
/// If `simd` feature flag is not enabled:
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
pub fn gt_eq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    #[cfg(feature = "simd")]
    return simd_compare_op_scalar(left, right, T::ge, |a, b| a >= b);
    #[cfg(not(feature = "simd"))]
    return compare_op_scalar(left, |a| a.is_ge(right));
}

/// Checks if a [`GenericListArray`] contains a value in the [`PrimitiveArray`]
pub fn contains<T, OffsetSize>(
    left: &PrimitiveArray<T>,
    right: &GenericListArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(&[left.data_ref(), right.data_ref()], left_len) {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.as_slice();

    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = bool_buf.as_slice_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            None,
            0,
            vec![bool_buf.into()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Checks if a [`GenericListArray`] contains a value in the [`GenericStringArray`]
pub fn contains_utf8<OffsetSize>(
    left: &GenericStringArray<OffsetSize>,
    right: &ListArray,
) -> Result<BooleanArray, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(&[left.data_ref(), right.data_ref()], left_len) {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.as_slice();

    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = &mut bool_buf;

    for i in 0..left_len {
        // contains(null, null) = false
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list
                .as_any()
                .downcast_ref::<GenericStringArray<OffsetSize>>()
                .unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            None,
            0,
            vec![bool_buf.into()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

// create a buffer and fill it with valid bits
#[inline]
fn new_all_set_buffer(len: usize) -> Buffer {
    let buffer = MutableBuffer::new(len);
    let buffer = buffer.with_bitset(len, true);

    buffer.into()
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{
        ListBuilder, PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder,
    };
    use arrow_buffer::i256;
    use arrow_schema::Field;

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<T>` or `Vec<Option<T>>` where `T` is the native
    /// type of the data type of the Arrow array element.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_vec {
        ($KERNEL:ident, $DYN_KERNEL:ident, $ARRAY:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = $ARRAY::from($A_VEC);
            let b = $ARRAY::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // slice and test if the dynamic array works
            let a = a.slice(0, a.len());
            let b = b.slice(0, b.len());
            let c = $DYN_KERNEL(a.as_ref(), b.as_ref()).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // test with a larger version of the same data to ensure we cover the chunked part of the comparison
            let mut a = vec![];
            let mut b = vec![];
            let mut e = vec![];
            for _i in 0..10 {
                a.extend($A_VEC);
                b.extend($B_VEC);
                e.extend($EXPECTED);
            }
            let a = $ARRAY::from(a);
            let b = $ARRAY::from(b);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from(e), c);
        };
    }

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64 {
        ($KERNEL:ident, $DYN_KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            cmp_vec!($KERNEL, $DYN_KERNEL, Int64Array, $A_VEC, $B_VEC, $EXPECTED);
        };
    }

    /// Evaluate `KERNEL` with one vectors and one scalar as inputs and assert against the expected output.
    /// `A_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let c = $KERNEL(&a, $B).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);

            // test with a larger version of the same data to ensure we cover the chunked part of the comparison
            let mut a = vec![];
            let mut e = vec![];
            for _i in 0..10 {
                a.extend($A_VEC);
                e.extend($EXPECTED);
            }
            let a = Int64Array::from(a);
            let c = $KERNEL(&a, $B).unwrap();
            assert_eq!(BooleanArray::from(e), c);

        };
    }

    #[test]
    fn test_primitive_array_eq() {
        cmp_i64!(
            eq,
            eq_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            TimestampSecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            Time32SecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            Time32MillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            Time64MicrosecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            Time64NanosecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_scalar() {
        cmp_i64_scalar!(
            eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_with_slice() {
        let a = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let b_slice = b.slice(5, 5);
        let c = b_slice.as_any().downcast_ref().unwrap();
        let d = eq(c, &a).unwrap();
        assert!(d.value(0));
        assert!(d.value(1));
        assert!(d.value(2));
        assert!(!d.value(3));
        assert!(d.value(4));
    }

    #[test]
    fn test_primitive_array_eq_scalar_with_slice() {
        let a = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let a = a.slice(1, 3);
        let a: &Int32Array = as_primitive_array(&a);
        let a_eq = eq_scalar(a, 2).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
    }

    #[test]
    fn test_primitive_array_neq() {
        cmp_i64!(
            neq,
            neq_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );

        cmp_vec!(
            neq,
            neq_dyn,
            TimestampMillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_neq_scalar() {
        cmp_i64_scalar!(
            neq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_boolean_array_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_neq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = neq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = lt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = lt_eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(true), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = gt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(false), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None]
                .into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)]
                .into();

        let res: Vec<Option<bool>> = gt_eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let res2: Vec<Option<bool>> = eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_neq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            neq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);

        let res2: Vec<Option<bool>> = neq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = lt_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(false), None]);

        let res2: Vec<Option<bool>> = lt_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_lt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            lt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let res2: Vec<Option<bool>> =
            lt_eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(true), None]);
    }

    #[test]
    fn test_boolean_array_gt_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> = gt_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(false), None]);

        let res2: Vec<Option<bool>> = gt_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(false), Some(false), None]);
    }

    #[test]
    fn test_boolean_array_gt_eq_scalar() {
        let a: BooleanArray = vec![Some(true), Some(false), None].into();

        let res1: Vec<Option<bool>> =
            gt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(true), None]);

        let res2: Vec<Option<bool>> =
            gt_eq_bool_scalar(&a, true).unwrap().iter().collect();

        assert_eq!(res2, vec![Some(true), Some(false), None]);
    }

    #[test]
    fn test_primitive_array_lt() {
        cmp_i64!(
            lt,
            lt_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );

        cmp_vec!(
            lt,
            lt_dyn,
            TimestampMillisecondArray,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar() {
        cmp_i64_scalar!(
            lt_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_nulls() {
        cmp_i64!(
            lt,
            lt_dyn,
            vec![None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            vec![None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );

        cmp_vec!(
            lt,
            lt_dyn,
            TimestampMillisecondArray,
            vec![None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            vec![None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar_nulls() {
        cmp_i64_scalar!(
            lt_scalar,
            vec![None, Some(1), Some(2), Some(3), None, Some(1), Some(2), Some(3), Some(2), None],
            2,
            vec![None, Some(true), Some(false), Some(false), None, Some(true), Some(false), Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq() {
        cmp_i64!(
            lt_eq,
            lt_eq_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar() {
        cmp_i64_scalar!(
            lt_eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_nulls() {
        cmp_i64!(
            lt_eq,
            lt_eq_dyn,
            vec![None, None, Some(1), None, None, Some(1), None, None, Some(1)],
            vec![None, Some(1), Some(0), None, Some(1), Some(2), None, None, Some(3)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            lt_eq_scalar,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt() {
        cmp_i64!(
            gt,
            gt_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar() {
        cmp_i64_scalar!(
            gt_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_nulls() {
        cmp_i64!(
            gt,
            gt_dyn,
            vec![None, None, Some(1), None, None, Some(2), None, None, Some(3)],
            vec![None, Some(1), Some(1), None, Some(1), Some(1), None, Some(1), Some(1)],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar_nulls() {
        cmp_i64_scalar!(
            gt_scalar,
            vec![None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(false), Some(true), None, Some(false), Some(true), None, Some(false), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq() {
        cmp_i64!(
            gt_eq,
            gt_eq_dyn,
            vec![8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar() {
        cmp_i64_scalar!(
            gt_eq_scalar,
            vec![6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        cmp_i64!(
            gt_eq,
            gt_eq_dyn,
            vec![None, None, Some(1), None, Some(1), Some(2), None, None, Some(1)],
            vec![None, Some(1), None, None, Some(1), Some(1), None, Some(2), Some(2)],
            vec![None, None, None, None, Some(true), Some(true), None, None, Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar_nulls() {
        cmp_i64_scalar!(
            gt_eq_scalar,
            vec![None, Some(1), Some(2), None, Some(2), Some(3), None, Some(3), Some(4)],
            2,
            vec![None, Some(false), Some(true), None, Some(true), Some(true), None, Some(true), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_compare_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b: Int32Array = (100..200).map(Some).collect();
        let b = b.slice(50, 50);
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = lt(a, b).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_compare_scalar_slice() {
        let a: Int32Array = (0..100).map(Some).collect();
        let a = a.slice(50, 50);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = lt_scalar(a, 200).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_length_of_result_buffer() {
        // `item_count` is chosen to not be a multiple of the number of SIMD lanes for this
        // type (`Int8Type`), 64.
        let item_count = 130;

        let select_mask: BooleanArray = vec![true; item_count].into();

        let array_a: PrimitiveArray<Int8Type> = vec![1; item_count].into();
        let array_b: PrimitiveArray<Int8Type> = vec![2; item_count].into();
        let result_mask = gt_eq(&array_a, &array_b).unwrap();

        assert_eq!(
            result_mask.data().buffers()[0].len(),
            select_mask.data().buffers()[0].len()
        );
    }

    // Expected behaviour:
    // contains(1, [1, 2, null]) = true
    // contains(3, [1, 2, null]) = false
    // contains(null, [1, 2, null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains() {
        let value_data = Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            None,
            Some(7),
        ])
        .data()
        .clone();
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 6, 9]);
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from([0b00001011])))
            .build()
            .unwrap();

        //  [[0, 1, 2], [3, 4, 5], null, [6, null, 7]]
        let list_array = LargeListArray::from(list_data);

        let nulls = Int32Array::from(vec![None, None, None, None]);
        let nulls_result = contains(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = Int32Array::from(vec![Some(0), Some(0), Some(0), Some(0)]);
        let values_result = contains(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, false, false, false]),
        );
    }

    #[test]
    fn test_interval_array() {
        let a = IntervalDayTimeArray::from(
            vec![Some(0), Some(6), Some(834), None, Some(3), None],
        );
        let b = IntervalDayTimeArray::from(
            vec![Some(70), Some(6), Some(833), Some(6), Some(3), None],
        );
        let res = eq(&a, &b).unwrap();
        let res_dyn = eq_dyn(&a, &b).unwrap();
        assert_eq!(res, res_dyn);
        assert_eq!(
            &res_dyn,
            &BooleanArray::from(
                vec![Some(false), Some(true), Some(false), None, Some(true), None]
            )
        );

        let a = IntervalMonthDayNanoArray::from(
            vec![Some(0), Some(6), Some(834), None, Some(3), None],
        );
        let b = IntervalMonthDayNanoArray::from(
            vec![Some(86), Some(5), Some(8), Some(6), Some(3), None],
        );
        let res = lt(&a, &b).unwrap();
        let res_dyn = lt_dyn(&a, &b).unwrap();
        assert_eq!(res, res_dyn);
        assert_eq!(
            &res_dyn,
            &BooleanArray::from(
                vec![Some(true), Some(false), Some(false), None, Some(false), None]
            )
        );

        let a = IntervalYearMonthArray::from(
            vec![Some(0), Some(623), Some(834), None, Some(3), None],
        );
        let b = IntervalYearMonthArray::from(
            vec![Some(86), Some(5), Some(834), Some(6), Some(86), None],
        );
        let res = gt_eq(&a, &b).unwrap();
        let res_dyn = gt_eq_dyn(&a, &b).unwrap();
        assert_eq!(res, res_dyn);
        assert_eq!(
            &res_dyn,
            &BooleanArray::from(
                vec![Some(false), Some(true), Some(true), None, Some(false), None]
            )
        );
    }

    macro_rules! test_binary {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = BinaryArray::from_vec($left);
                let right = BinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }

                let left = LargeBinaryArray::from_vec($left);
                let right = LargeBinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    #[test]
    fn test_binary_eq_scalar_on_slice() {
        let a = BinaryArray::from_opt_vec(
            vec![Some(b"hi"), None, Some(b"hello"), Some(b"world")],
        );
        let a = a.slice(1, 3);
        let a = as_generic_binary_array::<i32>(&a);
        let a_eq = eq_binary_scalar(a, b"hello").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false)])
        );
    }

    macro_rules! test_binary_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = BinaryArray::from_vec($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {:?} at position {} to {:?} ",
                        left.value(i),
                        i,
                        $right
                    );
                }

                let left = LargeBinaryArray::from_vec($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {:?} at position {} to {:?} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
    }

    test_binary!(
        test_binary_array_eq,
        vec![b"arrow", b"arrow", b"arrow", b"arrow", &[0xff, 0xf8]],
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        eq_binary,
        vec![true, false, false, false, true]
    );

    test_binary_scalar!(
        test_binary_array_eq_scalar,
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        "arrow".as_bytes(),
        eq_binary_scalar,
        vec![true, false, false, false, false]
    );

    test_binary!(
        test_binary_array_neq,
        vec![b"arrow", b"arrow", b"arrow", b"arrow", &[0xff, 0xf8]],
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf9]],
        neq_binary,
        vec![false, true, true, true, true]
    );
    test_binary_scalar!(
        test_binary_array_neq_scalar,
        vec![b"arrow", b"parquet", b"datafusion", b"flight", &[0xff, 0xf8]],
        "arrow".as_bytes(),
        neq_binary_scalar,
        vec![false, true, true, true, true]
    );

    test_binary!(
        test_binary_array_lt,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf9]],
        lt_binary,
        vec![true, true, false, false, true]
    );
    test_binary_scalar!(
        test_binary_array_lt_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        "flight".as_bytes(),
        lt_binary_scalar,
        vec![true, true, false, false, false]
    );

    test_binary!(
        test_binary_array_lt_eq,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8, 0xf9]],
        lt_eq_binary,
        vec![true, true, true, false, true]
    );
    test_binary_scalar!(
        test_binary_array_lt_eq_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        "flight".as_bytes(),
        lt_eq_binary_scalar,
        vec![true, true, true, false, false]
    );

    test_binary!(
        test_binary_array_gt,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf9]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8]],
        gt_binary,
        vec![false, false, false, true, true]
    );
    test_binary_scalar!(
        test_binary_array_gt_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        "flight".as_bytes(),
        gt_binary_scalar,
        vec![false, false, false, true, true]
    );

    test_binary!(
        test_binary_array_gt_eq,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        vec![b"flight", b"flight", b"flight", b"flight", &[0xff, 0xf8]],
        gt_eq_binary,
        vec![false, false, true, true, true]
    );
    test_binary_scalar!(
        test_binary_array_gt_eq_scalar,
        vec![b"arrow", b"datafusion", b"flight", b"parquet", &[0xff, 0xf8]],
        "flight".as_bytes(),
        gt_eq_binary_scalar,
        vec![false, false, true, true, true]
    );

    // Expected behaviour:
    // contains("ab", ["ab", "cd", null]) = true
    // contains("ef", ["ab", "cd", null]) = false
    // contains(null, ["ab", "cd", null]) = false
    // contains(null, null) = false
    #[test]
    fn test_contains_utf8() {
        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_value("Lorem");
        builder.values().append_value("ipsum");
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value("sit");
        builder.values().append_value("amet");
        builder.values().append_value("Lorem");
        builder.append(true);
        builder.append(false);
        builder.values().append_value("ipsum");
        builder.append(true);

        //  [["Lorem", "ipsum", null], ["sit", "amet", "Lorem"], null, ["ipsum"]]
        // value_offsets = [0, 3, 6, 6]
        let list_array = builder.finish();

        let nulls = StringArray::from(vec![None, None, None, None]);
        let nulls_result = contains_utf8(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = StringArray::from(vec![
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
        ]);
        let values_result = contains_utf8(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, true, false, false]),
        );
    }

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    #[test]
    fn test_utf8_eq_scalar_on_slice() {
        let a = StringArray::from(
            vec![Some("hi"), None, Some("hello"), Some("world"), Some("")],
        );
        let a = a.slice(1, 4);
        let a = as_string_array(&a);
        let a_eq = eq_utf8_scalar(a, "hello").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false), Some(false)])
        );

        let a_eq2 = eq_utf8_scalar(a, "").unwrap();

        assert_eq!(
            a_eq2,
            BooleanArray::from(vec![None, Some(false), Some(false), Some(true)])
        );
    }

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }

                let left = LargeStringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
        ($test_name:ident, $test_name_dyn:ident, $left:expr, $right:expr, $op:expr, $op_dyn:expr, $expected:expr) => {
            test_utf8_scalar!($test_name, $left, $right, $op, $expected);
            test_utf8_scalar!($test_name_dyn, $left, $right, $op_dyn, $expected);
        };
    }

    test_utf8!(
        test_utf8_array_eq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        eq_utf8,
        vec![true, false, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_eq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        eq_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8!(
        test_utf8_array_neq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        neq_utf8,
        vec![false, true, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_neq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        neq_utf8_scalar,
        vec![false, true, true, true]
    );

    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        vec![true, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_eq_utf8_scalar,
        vec![true, true, true, false]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        vec![false, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_utf8_scalar,
        vec![false, false, false, true]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        vec![false, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_eq_utf8_scalar,
        vec![false, false, true, true]
    );

    #[test]
    fn test_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), Some(false), Some(true), Some(true), Some(false)]
            )
        );
    }

    #[test]
    fn test_eq_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = eq_dyn_scalar(&array, 123).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(eq_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = lt_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), Some(true), Some(false), Some(false), Some(false)]
            )
        );
    }

    #[test]
    fn test_lt_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = lt_dyn_scalar(&array, 123).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_lt_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(lt_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_lt_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = lt_eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), Some(true), Some(true), Some(true), Some(false)]
            )
        );
    }

    #[test]
    fn test_lt_eq_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = lt_eq_dyn_scalar(&array, 23).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false)],
        );
        assert_eq!(lt_eq_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(lt_eq_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = gt_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), Some(false), Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_gt_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(123).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = gt_dyn_scalar(&array, 23).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_gt_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true)],
        );
        assert_eq!(gt_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(gt_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_gt_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = gt_eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), Some(false), Some(true), Some(true), Some(true)]
            )
        );
    }

    #[test]
    fn test_gt_eq_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(22).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = gt_eq_dyn_scalar(&array, 23).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(gt_eq_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_neq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = neq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), Some(true), Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_neq_dyn_scalar_with_dict() {
        let mut builder =
            PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
        builder.append(22).unwrap();
        builder.append_null();
        builder.append(23).unwrap();
        let array = builder.finish();
        let a_eq = neq_dyn_scalar(&array, 23).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_neq_dyn_scalar_float() {
        let array: Float32Array = vec![6.0, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(true)],
        );
        assert_eq!(neq_dyn_scalar(&array, 8).unwrap(), expected);

        let array = array.unary::<_, Float64Type>(|x| x as f64);
        assert_eq!(neq_dyn_scalar(&array, 8).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = "flight".as_bytes();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), None],
        );

        assert_eq!(eq_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            eq_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_neq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = "flight".as_bytes();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), None],
        );

        assert_eq!(neq_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            neq_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_lt_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = "flight".as_bytes();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), None],
        );

        assert_eq!(lt_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            lt_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_lt_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = "flight".as_bytes();
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(false), Some(false), None],
        );

        assert_eq!(lt_eq_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            lt_eq_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_gt_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = "flight".as_bytes();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), None],
        );

        assert_eq!(gt_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            gt_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_gt_eq_dyn_binary_scalar() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"arrow"), Some(b"datafusion"), Some(b"flight"), Some(b"parquet"), Some(&[0xff, 0xf8]), None];
        let array = BinaryArray::from(data.clone());
        let large_array = LargeBinaryArray::from(data);
        let scalar = &[0xff, 0xf8];
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), None],
        );

        assert_eq!(gt_eq_dyn_binary_scalar(&array, scalar).unwrap(), expected);
        assert_eq!(
            gt_eq_dyn_binary_scalar(&large_array, scalar).unwrap(),
            expected
        );
    }

    #[test]
    fn test_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = eq_dyn_utf8_scalar(&array, "xyz").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let a_eq = eq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), None, Some(true), Some(true), Some(false)]
            )
        );
    }

    #[test]
    fn test_lt_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = lt_dyn_utf8_scalar(&array, "xyz").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let a_eq = lt_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), None, Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_lt_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = lt_eq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let a_eq = lt_eq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), None, Some(true), Some(true), Some(false)]
            )
        );
    }

    #[test]
    fn test_gt_eq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = gt_eq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let a_eq = gt_eq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), None, Some(true), Some(true), Some(true)]
            )
        );
    }

    #[test]
    fn test_gt_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = gt_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_gt_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("xyz").unwrap();
        let array = builder.finish();
        let a_eq = gt_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(false), None, Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_neq_dyn_utf8_scalar() {
        let array = StringArray::from(vec!["abc", "def", "xyz"]);
        let a_eq = neq_dyn_utf8_scalar(&array, "xyz").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_neq_dyn_utf8_scalar_with_dict() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("abc").unwrap();
        builder.append_null();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();
        let a_eq = neq_dyn_utf8_scalar(&array, "def").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(
                vec![Some(true), None, Some(false), Some(false), Some(true)]
            )
        );
    }

    #[test]
    fn test_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let a_eq = eq_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let a_eq = lt_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(false), None])
        );
    }

    #[test]
    fn test_gt_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let a_eq = gt_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_lt_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let a_eq = lt_eq_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let a_eq = gt_eq_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_neq_dyn_bool_scalar() {
        let array = BooleanArray::from(vec![true, false, true]);
        let a_eq = neq_dyn_bool_scalar(&array, false).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_fixed_size_binary() {
        let values1: Vec<Option<&[u8]>> =
            vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x01])];
        let values2: Vec<Option<&[u8]>> =
            vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x00])];

        let array1 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values1.into_iter(), 2)
                .unwrap();
        let array2 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values2.into_iter(), 2)
                .unwrap();

        let result = eq_dyn(&array1, &array2).unwrap();
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false)]),
            result
        );

        let result = neq_dyn(&array1, &array2).unwrap();
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true)]),
            result
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);

        let keys1 = Int8Array::from_iter_values([2_i8, 3, 4]);
        let keys2 = Int8Array::from_iter_values([2_i8, 4, 4]);
        let dict_array1 = DictionaryArray::try_new(&keys1, &values).unwrap();
        let dict_array2 = DictionaryArray::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_u64_array() {
        let values = UInt64Array::from_iter_values([10_u64, 11, 12, 13, 14, 15, 16, 17]);

        let keys1 = UInt64Array::from_iter_values([1_u64, 3, 4]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 3, 5]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_utf8_array() {
        let test1 = vec!["a", "a", "b", "c"];
        let test2 = vec!["a", "b", "b", "c"];

        let dict_array1: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        let dict_array2: DictionaryArray<Int8Type> = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys1 = UInt64Array::from_iter_values([0_u64, 1, 2]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 2, 1]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_interval_array() {
        let values = IntervalDayTimeArray::from(vec![1, 6, 10, 2, 3, 5]);

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 0, 3]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_date_array() {
        let values = Date32Array::from(vec![1, 6, 10, 2, 3, 5]);

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 0, 3]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 1]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 0]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_gt_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);

        let keys1 = Int8Array::from_iter_values([3_i8, 4, 4]);
        let keys2 = Int8Array::from_iter_values([4_i8, 3, 4]);
        let dict_array1 = DictionaryArray::try_new(&keys1, &values).unwrap();
        let dict_array2 = DictionaryArray::try_new(&keys2, &values).unwrap();

        let result = lt_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = lt_eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));

        let result = gt_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = gt_eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_gt_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 0]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 1]);
        let dict_array1 =
            DictionaryArray::<UInt64Type>::try_new(&keys1, &values).unwrap();
        let dict_array2 =
            DictionaryArray::<UInt64Type>::try_new(&keys2, &values).unwrap();

        let result = lt_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = lt_eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, true, false]));

        let result = gt_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, false, true])
        );

        let result = gt_eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_unary_cmp() {
        let a = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let values = vec![1_i32, 3];

        let a_eq = unary_cmp(&a, |a| values.contains(&a)).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::try_new(&keys, &values).unwrap();

        let array = Int8Array::from_iter([Some(12_i8), None, Some(14)]);

        let result = eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = neq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = neq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::try_new(&keys, &values).unwrap();

        let array = Int8Array::from_iter([Some(12_i8), None, Some(11)]);

        let result = lt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = lt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let result = lt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );

        let result = lt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = gt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );

        let result = gt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        let result = gt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        let result = gt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float32Array = vec![f32::NAN, f32::NAN, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(neq(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float64Array = vec![f64::NAN, f64::NAN, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(neq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float32Array = vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]
            .into_iter()
            .map(Some)
            .collect();

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(lt_eq(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float64Array = vec![f64::NAN, f64::NAN, 8.0, 9.0, 10.0, 1.0]
            .into_iter()
            .map(Some)
            .collect();

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(lt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_gt_eq_dyn_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float32Array = vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]
            .into_iter()
            .map(Some)
            .collect();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(gt_eq(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let array2: Float64Array = vec![f64::NAN, f64::NAN, 8.0, 9.0, 10.0, 1.0]
            .into_iter()
            .map(Some)
            .collect();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        #[cfg(not(feature = "simd"))]
        assert_eq!(gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_scalar_neq_dyn_scalar_float_nan() {
        let array: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(true)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(neq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(true)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(neq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_scalar_lt_eq_dyn_scalar_float_nan() {
        let array: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_scalar_gt_eq_dyn_scalar_float_nan() {
        let array: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        #[cfg(feature = "simd")]
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        #[cfg(not(feature = "simd"))]
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_to_utf8_array() {
        let test1 = vec!["a", "a", "b", "c"];
        let test2 = vec!["a", "b", "b", "d"];

        let dict_array: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let array: StringArray = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = neq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = neq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_utf8_array() {
        let test1 = vec!["abc", "abc", "b", "cde"];
        let test2 = vec!["abc", "b", "b", "def"];

        let dict_array: DictionaryArray<Int8Type> = test1
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let array: StringArray = test2
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();

        let result = lt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = lt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = lt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = lt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = gt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = gt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = gt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = gt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_to_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys = UInt64Array::from(vec![Some(0_u64), None, Some(2), Some(2)]);
        let dict_array = DictionaryArray::<UInt64Type>::try_new(&keys, &values).unwrap();

        let array: BinaryArray = ["hello", "", "parquet", "test"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let result = eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = neq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = neq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys = UInt64Array::from(vec![Some(0_u64), None, Some(2), Some(2)]);
        let dict_array = DictionaryArray::<UInt64Type>::try_new(&keys, &values).unwrap();

        let array: BinaryArray = ["hello", "", "parquet", "test"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let result = lt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = lt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false)])
        );

        let result = lt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true)])
        );

        let result = lt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = gt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false)])
        );

        let result = gt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)])
        );

        let result = gt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)])
        );

        let result = gt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dict_non_dict_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 10.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float64Array::from(vec![f64::NAN, 8.0, 10.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_lt_eq_dyn_dict_non_dict_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float64Array::from(vec![f64::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_gt_dyn_gt_eq_dyn_dict_non_dict_float_nan() {
        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float64Array = vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float64Array::from(vec![f64::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_eq_dyn_neq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::try_new(&keys, &values).unwrap();

        let array: BooleanArray = test2.iter().collect();

        let result = eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = neq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = neq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::try_new(&keys, &values).unwrap();

        let array: BooleanArray = test2.iter().collect();

        let result = lt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = lt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = lt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );

        let result = lt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = gt_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let result = gt_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(false), None, None, Some(true)])
        );

        let result = gt_eq_dyn(&dict_array, &array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(false)])
        );

        let result = gt_eq_dyn(&array, &dict_array);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_cmp_dict_decimal128() {
        let values = Decimal128Array::from_iter_values([0, 1, 2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::try_new(&keys, &values).unwrap();

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), Some(false)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), Some(true)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(true), Some(false)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_cmp_dict_non_dict_decimal128() {
        let array1: Decimal128Array =
            Decimal128Array::from_iter_values([1, 2, 5, 4, 3, 0]);

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), Some(false)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), Some(true)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(true), Some(false)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_cmp_dict_decimal256() {
        let values = Decimal256Array::from_iter_values(
            [0, 1, 2, 3, 4, 5].into_iter().map(i256::from_i128),
        );
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::try_new(&keys, &values).unwrap();

        let values = Decimal256Array::from_iter_values(
            [7, -3, 4, 3, 5].into_iter().map(i256::from_i128),
        );
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), Some(false)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), Some(true)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(true), Some(false)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    #[cfg(feature = "dyn_cmp_dict")]
    fn test_cmp_dict_non_dict_decimal256() {
        let array1: Decimal256Array = Decimal256Array::from_iter_values(
            [1, 2, 5, 4, 3, 0].into_iter().map(i256::from_i128),
        );

        let values = Decimal256Array::from_iter_values(
            [7, -3, 4, 3, 5].into_iter().map(i256::from_i128),
        );
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::try_new(&keys, &values).unwrap();

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(true), Some(true), Some(false)],
        );
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(false), Some(false), Some(true)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), Some(true), Some(true), Some(true)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(true), Some(true), Some(false)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_decimal128() {
        let a = Decimal128Array::from_iter_values([1, 2, 4, 5]);
        let b = Decimal128Array::from_iter_values([7, -3, 4, 3]);
        let e = BooleanArray::from(vec![false, false, true, false]);
        let r = eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, false, false]);
        let r = lt(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = lt_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, true, false]);
        let r = lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = lt_eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, false, true]);
        let r = gt(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = gt_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, true, true]);
        let r = gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = gt_eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal128_scalar() {
        let a = Decimal128Array::from(
            vec![Some(1), Some(2), Some(3), None, Some(4), Some(5)],
        );
        let b = 3_i128;
        // array eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), None, Some(false), Some(false)],
        );
        let r = eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array neq scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), None, Some(true), Some(true)],
        );
        let r = neq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = neq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array lt scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(false), None, Some(false), Some(false)],
        );
        let r = lt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array lt_eq scalar
        let e = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), None, Some(false), Some(false)],
        );
        let r = lt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array gt scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), None, Some(true), Some(true)],
        );
        let r = gt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array gt_eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), None, Some(true), Some(true)],
        );
        let r = gt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256() {
        let a = Decimal256Array::from_iter_values(
            [1, 2, 4, 5].into_iter().map(i256::from_i128),
        );
        let b = Decimal256Array::from_iter_values(
            [7, -3, 4, 3].into_iter().map(i256::from_i128),
        );
        let e = BooleanArray::from(vec![false, false, true, false]);
        let r = eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, false, false]);
        let r = lt(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = lt_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![true, false, true, false]);
        let r = lt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = lt_eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, false, true]);
        let r = gt(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = gt_dyn(&a, &b).unwrap();
        assert_eq!(e, r);

        let e = BooleanArray::from(vec![false, true, true, true]);
        let r = gt_eq(&a, &b).unwrap();
        assert_eq!(e, r);

        let r = gt_eq_dyn(&a, &b).unwrap();
        assert_eq!(e, r);
    }
}

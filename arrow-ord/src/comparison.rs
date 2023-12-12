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

use half::f16;
use std::sync::Arc;

use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_buffer::{bit_util, BooleanBuffer, MutableBuffer, NullBuffer};
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

/// Calls $RIGHT.$TY() (e.g. `right.to_i128()`) with a nice error message.
/// Type of expression is `Result<.., ArrowError>`
macro_rules! try_to_type {
    ($RIGHT: expr, $TY: ident) => {
        try_to_type_result($RIGHT.$TY(), &format!("{:?}", $RIGHT), stringify!($TY))
    };
}

// Avoids creating a closure for each combination of `$RIGHT` and `$TY`
fn try_to_type_result<T>(value: Option<T>, right: &str, ty: &str) -> Result<T, ArrowError> {
    value.ok_or_else(|| ArrowError::ComputeError(format!("Could not convert {right} with {ty}",)))
}

fn make_primitive_scalar<T: num::ToPrimitive + std::fmt::Debug>(
    d: &DataType,
    scalar: T,
) -> Result<ArrayRef, ArrowError> {
    match d {
        DataType::Int8 => {
            let right = try_to_type!(scalar, to_i8)?;
            Ok(Arc::new(PrimitiveArray::<Int8Type>::from(vec![right])))
        }
        DataType::Int16 => {
            let right = try_to_type!(scalar, to_i16)?;
            Ok(Arc::new(PrimitiveArray::<Int16Type>::from(vec![right])))
        }
        DataType::Int32 => {
            let right = try_to_type!(scalar, to_i32)?;
            Ok(Arc::new(PrimitiveArray::<Int32Type>::from(vec![right])))
        }
        DataType::Int64 => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<Int64Type>::from(vec![right])))
        }
        DataType::UInt8 => {
            let right = try_to_type!(scalar, to_u8)?;
            Ok(Arc::new(PrimitiveArray::<UInt8Type>::from(vec![right])))
        }
        DataType::UInt16 => {
            let right = try_to_type!(scalar, to_u16)?;
            Ok(Arc::new(PrimitiveArray::<UInt16Type>::from(vec![right])))
        }
        DataType::UInt32 => {
            let right = try_to_type!(scalar, to_u32)?;
            Ok(Arc::new(PrimitiveArray::<UInt32Type>::from(vec![right])))
        }
        DataType::UInt64 => {
            let right = try_to_type!(scalar, to_u64)?;
            Ok(Arc::new(PrimitiveArray::<UInt64Type>::from(vec![right])))
        }
        DataType::Float16 => {
            let right = try_to_type!(scalar, to_f32)?;
            Ok(Arc::new(PrimitiveArray::<Float16Type>::from(vec![
                f16::from_f32(right),
            ])))
        }
        DataType::Float32 => {
            let right = try_to_type!(scalar, to_f32)?;
            Ok(Arc::new(PrimitiveArray::<Float32Type>::from(vec![right])))
        }
        DataType::Float64 => {
            let right = try_to_type!(scalar, to_f64)?;
            Ok(Arc::new(PrimitiveArray::<Float64Type>::from(vec![right])))
        }
        DataType::Decimal128(_, _) => {
            let right = try_to_type!(scalar, to_i128)?;
            Ok(Arc::new(
                PrimitiveArray::<Decimal128Type>::from(vec![right]).with_data_type(d.clone()),
            ))
        }
        DataType::Decimal256(_, _) => {
            let right = try_to_type!(scalar, to_i128)?;
            Ok(Arc::new(
                PrimitiveArray::<Decimal256Type>::from(vec![i256::from_i128(right)])
                    .with_data_type(d.clone()),
            ))
        }
        DataType::Date32 => {
            let right = try_to_type!(scalar, to_i32)?;
            Ok(Arc::new(PrimitiveArray::<Date32Type>::from(vec![right])))
        }
        DataType::Date64 => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<Date64Type>::from(vec![right])))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(
                PrimitiveArray::<TimestampNanosecondType>::from(vec![right])
                    .with_data_type(d.clone()),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::from(vec![right])
                    .with_data_type(d.clone()),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(
                PrimitiveArray::<TimestampMillisecondType>::from(vec![right])
                    .with_data_type(d.clone()),
            ))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(
                PrimitiveArray::<TimestampSecondType>::from(vec![right]).with_data_type(d.clone()),
            ))
        }
        DataType::Time32(TimeUnit::Second) => {
            let right = try_to_type!(scalar, to_i32)?;
            Ok(Arc::new(PrimitiveArray::<Time32SecondType>::from(vec![
                right,
            ])))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let right = try_to_type!(scalar, to_i32)?;
            Ok(Arc::new(PrimitiveArray::<Time32MillisecondType>::from(
                vec![right],
            )))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<Time64MicrosecondType>::from(
                vec![right],
            )))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<Time64NanosecondType>::from(
                vec![right],
            )))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let right = try_to_type!(scalar, to_i32)?;
            Ok(Arc::new(PrimitiveArray::<IntervalYearMonthType>::from(
                vec![right],
            )))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<IntervalDayTimeType>::from(vec![
                right,
            ])))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let right = try_to_type!(scalar, to_i128)?;
            Ok(Arc::new(PrimitiveArray::<IntervalMonthDayNanoType>::from(
                vec![right],
            )))
        }
        DataType::Duration(TimeUnit::Second) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<DurationSecondType>::from(vec![
                right,
            ])))
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<DurationMillisecondType>::from(
                vec![right],
            )))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<DurationMicrosecondType>::from(
                vec![right],
            )))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            let right = try_to_type!(scalar, to_i64)?;
            Ok(Arc::new(PrimitiveArray::<DurationNanosecondType>::from(
                vec![right],
            )))
        }
        DataType::Dictionary(_, v) => make_primitive_scalar(v.as_ref(), scalar),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported primitive scalar data type {d:?}",
        ))),
    }
}

fn make_binary_scalar(d: &DataType, scalar: &[u8]) -> Result<ArrayRef, ArrowError> {
    match d {
        DataType::Binary => Ok(Arc::new(BinaryArray::from_iter_values([scalar]))),
        DataType::FixedSizeBinary(_) => Ok(Arc::new(FixedSizeBinaryArray::try_from_iter(
            [scalar].into_iter(),
        )?)),
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from_iter_values([scalar]))),
        DataType::Dictionary(_, v) => make_binary_scalar(v.as_ref(), scalar),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported binary scalar data type {d:?}",
        ))),
    }
}

fn make_utf8_scalar(d: &DataType, scalar: &str) -> Result<ArrayRef, ArrowError> {
    match d {
        DataType::Utf8 => Ok(Arc::new(StringArray::from_iter_values([scalar]))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from_iter_values([scalar]))),
        DataType::Dictionary(_, v) => make_utf8_scalar(v.as_ref(), scalar),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Unsupported utf8 scalar data type {d:?}",
        ))),
    }
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::eq(left, right)
}

/// Perform `left == right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from(vec![right]);
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::eq(&left, &right)
}

/// Perform `left != right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::neq(&left, &right)
}

/// Perform `left < right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt(&left, &right)
}

/// Perform `left <= right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt_eq(&left, &right)
}

/// Perform `left > right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt(&left, &right)
}

/// Perform `left >= right` operation on [`BooleanArray`]
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt_eq(&left, &right)
}

/// Perform `left == right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Perform `left != right` operation on [`BooleanArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_bool_scalar(left: &BooleanArray, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::eq(left, right)
}

/// Perform `left == right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::eq(left, &Scalar::new(&right))
}

/// Perform `left != right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::neq(left, right)
}

/// Perform `left != right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::neq(left, &Scalar::new(&right))
}

/// Perform `left < right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt(left, right)
}

/// Perform `left < right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::lt(left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt_eq(left, right)
}

/// Perform `left <= right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::lt_eq(left, &Scalar::new(&right))
}

/// Perform `left > right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt(left, right)
}

/// Perform `left > right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::gt(left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on [`BinaryArray`] / [`LargeBinaryArray`].
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt_eq(left, right)
}

/// Perform `left >= right` operation on [`BinaryArray`] / [`LargeBinaryArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_binary_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &[u8],
) -> Result<BooleanArray, ArrowError> {
    let right = GenericBinaryArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::gt_eq(left, &Scalar::new(&right))
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::neq(left, right)
}

/// Perform `left != right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::neq(left, &Scalar::new(&right))
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt(left, right)
}

/// Perform `left < right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::lt(left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt_eq(left, right)
}

/// Perform `left <= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::lt_eq(left, &Scalar::new(&right))
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt(left, right)
}

/// Perform `left > right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::gt(left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`].
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt_eq(left, right)
}

/// Perform `left >= right` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    let right = GenericStringArray::<OffsetSize>::from_iter_values([right]);
    crate::cmp::gt_eq(left, &Scalar::new(&right))
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports PrimitiveArrays, and DictionaryArrays that have primitive values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_dyn_scalar<T>(left: &dyn Array, right: T) -> Result<BooleanArray, ArrowError>
where
    T: num::ToPrimitive + std::fmt::Debug,
{
    let right = make_primitive_scalar(left.data_type(), right)?;
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports BinaryArray and LargeBinaryArray
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_dyn_binary_scalar(left: &dyn Array, right: &[u8]) -> Result<BooleanArray, ArrowError> {
    let right = make_binary_scalar(left.data_type(), right)?;
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports StringArrays, and DictionaryArrays that have string values
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_dyn_utf8_scalar(left: &dyn Array, right: &str) -> Result<BooleanArray, ArrowError> {
    let right = make_utf8_scalar(left.data_type(), right)?;
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on an array and a numeric scalar
/// value.
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Perform `left != right` operation on an array and a numeric scalar
/// value. Supports BooleanArrays.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_dyn_bool_scalar(left: &dyn Array, right: bool) -> Result<BooleanArray, ArrowError> {
    let right = BooleanArray::from(vec![right]);
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left == right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::eq(&left, &right)
}

/// Perform `left != right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::neq(&left, &right)
}

/// Perform `left < right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt(&left, &right)
}

/// Perform `left <= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::lt_eq(&left, &right)
}

/// Perform `left > right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt(&left, &right)
}

/// Perform `left >= right` operation on two (dynamic) [`Array`]s.
///
/// Only when two arrays are of the same type the comparison will happen otherwise it will err
/// with a casting error.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
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
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    crate::cmp::gt_eq(&left, &right)
}

/// Perform `left == right` operation on two [`PrimitiveArray`]s.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::eq(&left, &right)
}

/// Perform `left == right` operation on a [`PrimitiveArray`] and a scalar value.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::eq")]
pub fn eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::eq(&left, &Scalar::new(&right))
}

/// Applies an unary and infallible comparison function to a primitive array.
#[deprecated(note = "Use BooleanArray::from_unary")]
pub fn unary_cmp<T, F>(left: &PrimitiveArray<T>, op: F) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native) -> bool,
{
    Ok(BooleanArray::from_unary(left, op))
}

/// Perform `left != right` operation on two [`PrimitiveArray`]s.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::neq(&left, &right)
}

/// Perform `left != right` operation on a [`PrimitiveArray`] and a scalar value.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::neq")]
pub fn neq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::neq(&left, &Scalar::new(&right))
}

/// Perform `left < right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
/// values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::lt(&left, &right)
}

/// Perform `left < right` operation on a [`PrimitiveArray`] and a scalar value.
/// Null values are less than non-null values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt")]
pub fn lt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::lt(&left, &Scalar::new(&right))
}

/// Perform `left <= right` operation on two [`PrimitiveArray`]s. Null values are less than non-null
/// values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::lt_eq(&left, &right)
}

/// Perform `left <= right` operation on a [`PrimitiveArray`] and a scalar value.
/// Null values are less than non-null values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::lt_eq")]
pub fn lt_eq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::lt_eq(&left, &Scalar::new(&right))
}

/// Perform `left > right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
/// values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::gt(&left, &right)
}

/// Perform `left > right` operation on a [`PrimitiveArray`] and a scalar value.
/// Non-null values are greater than null values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt")]
pub fn gt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::gt(&left, &Scalar::new(&right))
}

/// Perform `left >= right` operation on two [`PrimitiveArray`]s. Non-null values are greater than null
/// values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    crate::cmp::gt_eq(&left, &right)
}

/// Perform `left >= right` operation on a [`PrimitiveArray`] and a scalar value.
/// Non-null values are greater than null values.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros are different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
/// Please refer to `f32::total_cmp` and `f64::total_cmp`.
#[deprecated(note = "Use arrow_ord::cmp::gt_eq")]
pub fn gt_eq_scalar<T>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> Result<BooleanArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let right = PrimitiveArray::<T>::new(vec![right].into(), None);
    crate::cmp::gt_eq(&left, &Scalar::new(&right))
}

/// Checks if a [`GenericListArray`] contains a value in the [`PrimitiveArray`]
pub fn in_list<T, OffsetSize>(
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
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = bool_buf.as_slice_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
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

    let values = BooleanBuffer::new(bool_buf.into(), 0, left_len);
    Ok(BooleanArray::new(values, None))
}

/// Checks if a [`GenericListArray`] contains a value in the [`GenericStringArray`]
pub fn in_list_utf8<OffsetSize>(
    left: &GenericStringArray<OffsetSize>,
    right: &ListArray,
) -> Result<BooleanArray, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let mut bool_buf = MutableBuffer::from_len_zeroed(num_bytes);
    let bool_slice = &mut bool_buf;

    for i in 0..left_len {
        // contains(null, null) = false
        if nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true) {
            let list = right.value(i);
            let list = list.as_string::<OffsetSize>();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }
    let values = BooleanBuffer::new(bool_buf.into(), 0, left_len);
    Ok(BooleanArray::new(values, None))
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        ListBuilder, PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder,
    };
    use arrow_buffer::{i256, Buffer};
    use arrow_data::ArrayData;
    use arrow_schema::Field;

    use super::*;

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
            let c = $DYN_KERNEL(&a, &b).unwrap();
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

        cmp_vec!(
            eq,
            eq_dyn,
            IntervalYearMonthArray,
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                // 1 year
                IntervalYearMonthType::make_value(1, 0),
            ],
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(1, 2),
                // NB 12 months is treated as equal to a year (as the underlying
                // type stores number of months)
                IntervalYearMonthType::make_value(0, 12),
            ],
            vec![true, false, true]
        );

        cmp_vec!(
            eq,
            eq_dyn,
            IntervalMonthDayNanoArray,
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(3, 2, 1),
                // 1 month
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                IntervalMonthDayNanoType::make_value(1, 0, 0),
            ],
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                // 30 days is not treated as a month
                IntervalMonthDayNanoType::make_value(0, 30, 0),
                // 100 days
                IntervalMonthDayNanoType::make_value(0, 100, 0),
            ],
            vec![true, false, false, false]
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
        let a_eq = eq_scalar(&a, 2).unwrap();
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
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(false), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_neq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = neq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = lt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(true), Some(false), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_lt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = lt_eq_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(true), Some(true), Some(true), Some(false), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

        let res: Vec<Option<bool>> = gt_bool(&a, &b).unwrap().iter().collect();

        assert_eq!(
            res,
            vec![Some(false), Some(false), Some(false), Some(true), None, None]
        )
    }

    #[test]
    fn test_boolean_array_gt_eq() {
        let a: BooleanArray =
            vec![Some(true), Some(false), Some(false), Some(true), Some(true), None].into();
        let b: BooleanArray =
            vec![Some(true), Some(true), Some(false), Some(false), None, Some(false)].into();

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

        let res1: Vec<Option<bool>> = neq_bool_scalar(&a, false).unwrap().iter().collect();

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

        let res1: Vec<Option<bool>> = lt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(false), Some(true), None]);

        let res2: Vec<Option<bool>> = lt_eq_bool_scalar(&a, true).unwrap().iter().collect();

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

        let res1: Vec<Option<bool>> = gt_eq_bool_scalar(&a, false).unwrap().iter().collect();

        assert_eq!(res1, vec![Some(true), Some(true), None]);

        let res2: Vec<Option<bool>> = gt_eq_bool_scalar(&a, true).unwrap().iter().collect();

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

        cmp_vec!(
            lt,
            lt_dyn,
            IntervalDayTimeArray,
            vec![
                IntervalDayTimeType::make_value(1, 0),
                IntervalDayTimeType::make_value(0, 1000),
                IntervalDayTimeType::make_value(1, 1000),
                IntervalDayTimeType::make_value(1, 3000),
                // 90M milliseconds
                IntervalDayTimeType::make_value(0, 90_000_000),
            ],
            vec![
                IntervalDayTimeType::make_value(0, 1000),
                IntervalDayTimeType::make_value(1, 0),
                IntervalDayTimeType::make_value(10, 0),
                IntervalDayTimeType::make_value(2, 1),
                // NB even though 1 day is less than 90M milliseconds long,
                // it compares as greater because the underlying type stores
                // days and milliseconds as different fields
                IntervalDayTimeType::make_value(0, 12),
            ],
            vec![false, true, true, true ,false]
        );

        cmp_vec!(
            lt,
            lt_dyn,
            IntervalYearMonthArray,
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                IntervalYearMonthType::make_value(1, 2),
                // 1 year
                IntervalYearMonthType::make_value(1, 0),
            ],
            vec![
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(1, 2),
                IntervalYearMonthType::make_value(2, 1),
                // NB 12 months is treated as equal to a year (as the underlying
                // type stores number of months)
                IntervalYearMonthType::make_value(0, 12),
            ],
            vec![false, false, true, false]
        );

        cmp_vec!(
            lt,
            lt_dyn,
            IntervalMonthDayNanoArray,
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(3, 2, 1),
                // 1 month
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                IntervalMonthDayNanoType::make_value(1, 2, 0),
                IntervalMonthDayNanoType::make_value(1, 0, 0),
            ],
            vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(1, 2, 3),
                IntervalMonthDayNanoType::make_value(2, 0, 0),
                // 30 days is not treated as a month
                IntervalMonthDayNanoType::make_value(0, 30, 0),
                // 100 days (note is treated as greater than 1 month as the underlying integer representation)
                IntervalMonthDayNanoType::make_value(0, 100, 0),
            ],
            vec![false, false, true, false, false]
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

        assert_eq!(result_mask.values().len(), select_mask.values().len());
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
        .into_data();
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 6, 9]);
        let list_data_type =
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
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
        let nulls_result = in_list(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = Int32Array::from(vec![Some(0), Some(0), Some(0), Some(0)]);
        let values_result = in_list(&values, &list_array).unwrap();
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
        let a = IntervalDayTimeArray::from(vec![Some(0), Some(6), Some(834), None, Some(3), None]);
        let b =
            IntervalDayTimeArray::from(vec![Some(70), Some(6), Some(833), Some(6), Some(3), None]);
        let res = eq(&a, &b).unwrap();
        let res_dyn = eq_dyn(&a, &b).unwrap();
        assert_eq!(res, res_dyn);
        assert_eq!(
            &res_dyn,
            &BooleanArray::from(vec![Some(false), Some(true), Some(false), None, Some(true), None])
        );

        let a =
            IntervalMonthDayNanoArray::from(vec![Some(0), Some(6), Some(834), None, Some(3), None]);
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

        let a =
            IntervalYearMonthArray::from(vec![Some(0), Some(623), Some(834), None, Some(3), None]);
        let b = IntervalYearMonthArray::from(
            vec![Some(86), Some(5), Some(834), Some(6), Some(86), None],
        );
        let res = gt_eq(&a, &b).unwrap();
        let res_dyn = gt_eq_dyn(&a, &b).unwrap();
        assert_eq!(res, res_dyn);
        assert_eq!(
            &res_dyn,
            &BooleanArray::from(vec![Some(false), Some(true), Some(true), None, Some(false), None])
        );
    }

    macro_rules! test_binary {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = BinaryArray::from_vec($left);
                let right = BinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from_vec($left);
                let right = LargeBinaryArray::from_vec($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    #[test]
    fn test_binary_eq_scalar_on_slice() {
        let a = BinaryArray::from_opt_vec(vec![Some(b"hi"), None, Some(b"hello"), Some(b"world")]);
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
                let expected = BooleanArray::from($expected);

                let left = BinaryArray::from_vec($left);
                let res = $op(&left, $right).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from_vec($left);
                let res = $op(&left, $right).unwrap();
                assert_eq!(res, expected);
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

        let v: Vec<Option<&str>> = vec![None, None, None, None];
        let nulls = StringArray::from(v);
        let nulls_result = in_list_utf8(&nulls, &list_array).unwrap();
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
        let values_result = in_list_utf8(&values, &list_array).unwrap();
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
        let a = StringArray::from(vec![Some("hi"), None, Some("hello"), Some("world"), Some("")]);
        let a = a.slice(1, 4);
        let a_eq = eq_utf8_scalar(&a, "hello").unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![None, Some(true), Some(false), Some(false)])
        );

        let a_eq2 = eq_utf8_scalar(&a, "").unwrap();

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
        [true, false, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_eq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        eq_utf8_scalar,
        [true, false, false, false]
    );

    test_utf8!(
        test_utf8_array_neq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        neq_utf8,
        [false, true, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_neq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        neq_utf8_scalar,
        [false, true, true, true]
    );

    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        [true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_utf8_scalar,
        [true, true, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        [true, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_eq_utf8_scalar,
        [true, true, true, false]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        [false, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_utf8_scalar,
        [false, false, false, true]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        [false, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_eq_utf8_scalar,
        [false, false, true, true]
    );

    #[test]
    fn test_eq_dyn_scalar() {
        let array = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let a_eq = eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)])
        );
    }

    #[test]
    fn test_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
        let expected =
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(false)]);
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
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(false)])
        );
    }

    #[test]
    fn test_lt_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(false)]);
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
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(false)])
        );
    }

    fn test_primitive_dyn_scalar<T: ArrowPrimitiveType>(array: PrimitiveArray<T>) {
        let a_eq = eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), None, Some(false)])
        );

        let a_eq = gt_eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(true), None, Some(true)])
        );

        let a_eq = gt_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(false), None, Some(false), None, Some(true)])
        );

        let a_eq = lt_eq_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(true), None, Some(false)])
        );

        let a_eq = lt_dyn_scalar(&array, 8).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), None, Some(false)])
        );
    }

    #[test]
    fn test_timestamp_dyn_scalar() {
        let array = TimestampSecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = TimestampNanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_date32_dyn_scalar() {
        let array = Date32Array::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_date64_dyn_scalar() {
        let array = Date64Array::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_time32_dyn_scalar() {
        let array = Time32SecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = Time32MillisecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_time64_dyn_scalar() {
        let array = Time64MicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = Time64NanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_interval_dyn_scalar() {
        let array = IntervalDayTimeArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = IntervalMonthDayNanoArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = IntervalYearMonthArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_duration_dyn_scalar() {
        let array = DurationSecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationMicrosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationMillisecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);

        let array = DurationNanosecondArray::from(vec![Some(1), None, Some(8), None, Some(10)]);
        test_primitive_dyn_scalar(array);
    }

    #[test]
    fn test_lt_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(false)]);
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
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(true)])
        );
    }

    #[test]
    fn test_gt_eq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
        let expected =
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(true)]);
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
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(true)])
        );
    }

    #[test]
    fn test_neq_dyn_scalar_with_dict() {
        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::with_capacity(3, 2);
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
        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(true)]);
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

        let fsb_array = FixedSizeBinaryArray::try_from_iter(
            vec![vec![0u8], vec![0u8], vec![0u8], vec![1u8]].into_iter(),
        )
        .unwrap();
        let scalar = &[1u8];
        let expected = BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]);
        assert_eq!(eq_dyn_binary_scalar(&fsb_array, scalar).unwrap(), expected);
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

        let fsb_array = FixedSizeBinaryArray::try_from_iter(
            vec![vec![0u8], vec![0u8], vec![0u8], vec![1u8]].into_iter(),
        )
        .unwrap();
        let scalar = &[1u8];
        let expected = BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]);
        assert_eq!(neq_dyn_binary_scalar(&fsb_array, scalar).unwrap(), expected);
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
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true), Some(false)])
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
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false), Some(true)])
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
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true), Some(false)])
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
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true), Some(true)])
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
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false), Some(true)])
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
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false), Some(true)])
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
        let values1: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x01])];
        let values2: Vec<Option<&[u8]>> = vec![Some(&[0xfc, 0xa9]), None, Some(&[0x36, 0x00])];

        let array1 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values1.into_iter(), 2).unwrap();
        let array2 =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values2.into_iter(), 2).unwrap();

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
    fn test_eq_dyn_neq_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = Int8Array::from_iter_values([2_i8, 3, 4]);
        let keys2 = Int8Array::from_iter_values([2_i8, 4, 4]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_u64_array() {
        let values = UInt64Array::from_iter_values([10_u64, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 3, 4]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 3, 5]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_utf8_array() {
        let test1 = ["a", "a", "b", "c"];
        let test2 = ["a", "b", "b", "c"];

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
    fn test_eq_dyn_neq_dyn_dictionary_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([0_u64, 1, 2]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 2, 1]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_interval_array() {
        let values = IntervalDayTimeArray::from(vec![1, 6, 10, 2, 3, 5]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 0, 3]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_date_array() {
        let values = Date32Array::from(vec![1, 6, 10, 2, 3, 5]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 0, 3]);
        let keys2 = UInt64Array::from_iter_values([2_u64, 0, 3]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![false, true, true]));

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 1]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 0]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

        let result = eq_dyn(&dict_array1, &dict_array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![false, true, false])
        );

        let result = neq_dyn(&dict_array1, &dict_array2);
        assert_eq!(result.unwrap(), BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    fn test_lt_dyn_gt_dyn_dictionary_i8_array() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = Int8Array::from_iter_values([3_i8, 4, 4]);
        let keys2 = Int8Array::from_iter_values([4_i8, 3, 4]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

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
    fn test_lt_dyn_gt_dyn_dictionary_bool_array() {
        let values = BooleanArray::from(vec![true, false]);
        let values = Arc::new(values) as ArrayRef;

        let keys1 = UInt64Array::from_iter_values([1_u64, 1, 0]);
        let keys2 = UInt64Array::from_iter_values([0_u64, 1, 1]);
        let dict_array1 = DictionaryArray::new(keys1, values.clone());
        let dict_array2 = DictionaryArray::new(keys2, values.clone());

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
        let values = [1_i32, 3];

        let a_eq = unary_cmp(&a, |a| values.contains(&a)).unwrap();
        assert_eq!(
            a_eq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)])
        );
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_i8_i8_array() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

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
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);
        let array2 = Float16Array::from(
            vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)],
        );
        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(neq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 8.0, 10.0]);
        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(neq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let array2 = Float64Array::from(vec![f64::NAN, f64::NAN, 8.0, 8.0, 10.0]);

        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(eq(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(neq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let array2 = Float16Array::from(vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(lt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

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

        assert_eq!(lt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(lt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_gt_eq_dyn_float_nan() {
        let array1 = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]);
        let array2 = Float16Array::from(vec![f16::NAN, f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]);
        let array2 = Float32Array::from(vec![f32::NAN, f32::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt_eq(&array1, &array2).unwrap(), expected);

        let array1 = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 11.0, f64::NAN]);
        let array2 = Float64Array::from(vec![f64::NAN, f64::NAN, 8.0, 9.0, 10.0, 1.0]);

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        assert_eq!(gt_eq(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_scalar_neq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(neq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(neq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(neq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_scalar_lt_eq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]);

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_dyn_scalar(&array, f16::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_eq_dyn_scalar(&array, f16::NAN).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);

        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let expected =
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        let expected =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        assert_eq!(lt_eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    fn test_gt_dyn_scalar_gt_eq_dyn_scalar_float_nan() {
        let array = Float16Array::from(vec![
           f16::NAN,
           f16::from_f32(7.0),
           f16::from_f32(8.0),
           f16::from_f32(8.0),
           f16::from_f32(10.0),
       ]);
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn_scalar(&array, f16::NAN).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, f16::NAN).unwrap(), expected);

        let array = Float32Array::from(vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, f32::NAN).unwrap(), expected);

        let array = Float64Array::from(vec![f64::NAN, 7.0, 8.0, 8.0, 10.0]);
        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_dyn_scalar(&array, f64::NAN).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(false), Some(false), Some(false)],
        );
        assert_eq!(gt_eq_dyn_scalar(&array, f64::NAN).unwrap(), expected);
    }

    #[test]
    fn test_eq_dyn_neq_dyn_dictionary_to_utf8_array() {
        let test1 = ["a", "a", "b", "c"];
        let test2 = ["a", "b", "b", "d"];

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
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_utf8_array() {
        let test1 = ["abc", "abc", "b", "cde"];
        let test2 = ["abc", "b", "b", "def"];

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
    fn test_eq_dyn_neq_dyn_dictionary_to_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys = UInt64Array::from(vec![Some(0_u64), None, Some(2), Some(2)]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_binary_array() {
        let values: BinaryArray = ["hello", "", "parquet"]
            .into_iter()
            .map(|b| Some(b.as_bytes()))
            .collect();

        let keys = UInt64Array::from(vec![Some(0_u64), None, Some(2), Some(2)]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_eq_dyn_neq_dyn_dict_non_dict_float_nan() {
        let array1: Float16Array = vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(10.0)]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(10.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 10.0]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 10.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 1, 2]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
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
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected =
            BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), Some(true)]);
        assert_eq!(eq_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(false), Some(false)],
        );
        assert_eq!(neq_dyn(&array1, &array2).unwrap(), expected);
    }

    #[test]
    fn test_lt_dyn_lt_eq_dyn_dict_non_dict_float_nan() {
        let array1: Float16Array = vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(
            vec![Some(false), Some(true), Some(false), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(true), Some(true), Some(true), Some(false), Some(false)],
        );
        assert_eq!(lt_eq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_gt_dyn_gt_eq_dyn_dict_non_dict_float_nan() {
        let array1: Float16Array = vec![f16::NAN, f16::from_f32(7.0), f16::from_f32(8.0), f16::from_f32(8.0), f16::from_f32(11.0), f16::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float16Array::from(vec![f16::NAN, f16::from_f32(8.0), f16::from_f32(9.0), f16::from_f32(10.0), f16::from_f32(1.0)]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

        let expected = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_dyn(&array1, &array2).unwrap(), expected);

        let expected = BooleanArray::from(
            vec![Some(true), Some(false), Some(true), Some(false), Some(true), Some(true)],
        );
        assert_eq!(gt_eq_dyn(&array1, &array2).unwrap(), expected);

        let array1: Float32Array = vec![f32::NAN, 7.0, 8.0, 8.0, 11.0, f32::NAN]
            .into_iter()
            .map(Some)
            .collect();
        let values = Float32Array::from(vec![f32::NAN, 8.0, 9.0, 10.0, 1.0]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_eq_dyn_neq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = BooleanArray::from(test2);

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
    fn test_lt_dyn_lt_eq_dyn_gt_dyn_gt_eq_dyn_dictionary_to_boolean_array() {
        let test1 = vec![Some(true), None, Some(false)];
        let test2 = vec![Some(true), None, None, Some(true)];

        let values = BooleanArray::from(test1);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let array = BooleanArray::from(test2);

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
    fn test_cmp_dict_decimal128() {
        let values = Decimal128Array::from_iter_values([0, 1, 2, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_cmp_dict_non_dict_decimal128() {
        let array1: Decimal128Array = Decimal128Array::from_iter_values([1, 2, 5, 4, 3, 0]);

        let values = Decimal128Array::from_iter_values([7, -3, 4, 3, 5]);
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_cmp_dict_decimal256() {
        let values =
            Decimal256Array::from_iter_values([0, 1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([1_i8, 2, 5, 4, 3, 0]);
        let array1 = DictionaryArray::new(keys, Arc::new(values));

        let values =
            Decimal256Array::from_iter_values([7, -3, 4, 3, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
    fn test_cmp_dict_non_dict_decimal256() {
        let array1: Decimal256Array =
            Decimal256Array::from_iter_values([1, 2, 5, 4, 3, 0].into_iter().map(i256::from_i128));

        let values =
            Decimal256Array::from_iter_values([7, -3, 4, 3, 5].into_iter().map(i256::from_i128));
        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 3, 4]);
        let array2 = DictionaryArray::new(keys, Arc::new(values));

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
        let a = Decimal128Array::from(vec![Some(1), Some(2), Some(3), None, Some(4), Some(5)]);
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
        let a = Decimal256Array::from_iter_values([1, 2, 4, 5].into_iter().map(i256::from_i128));
        let b = Decimal256Array::from_iter_values([7, -3, 4, 3].into_iter().map(i256::from_i128));
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
    fn test_decimal256_scalar_i128() {
        let a = Decimal256Array::from_iter_values([1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let b = i256::from_i128(3);
        // array eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(true), Some(false), Some(false)],
        );
        let r = eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array neq scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(true), Some(true)]);
        let r = neq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = neq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array lt scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(false), Some(false)]);
        let r = lt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array lt_eq scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false), Some(false)]);
        let r = lt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array gt scalar
        let e =
            BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true), Some(true)]);
        let r = gt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);

        // array gt_eq scalar
        let e =
            BooleanArray::from(vec![Some(false), Some(false), Some(true), Some(true), Some(true)]);
        let r = gt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_eq_dyn_scalar(&a, b).unwrap();
        assert_eq!(e, r);
    }

    #[test]
    fn test_decimal256_scalar_i256() {
        let a = Decimal256Array::from_iter_values([1, 2, 3, 4, 5].into_iter().map(i256::from_i128));
        let b = i256::MAX;
        // array eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        let r = eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = eq_dyn_scalar(&a, b).is_err();
        assert!(r);

        // array neq scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        let r = neq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = neq_dyn_scalar(&a, b).is_err();
        assert!(r);

        // array lt scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        let r = lt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_dyn_scalar(&a, b).is_err();
        assert!(r);

        // array lt_eq scalar
        let e =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), Some(true)]);
        let r = lt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = lt_eq_dyn_scalar(&a, b).is_err();
        assert!(r);

        // array gt scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        let r = gt_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_dyn_scalar(&a, b).is_err();
        assert!(r);

        // array gt_eq scalar
        let e = BooleanArray::from(
            vec![Some(false), Some(false), Some(false), Some(false), Some(false)],
        );
        let r = gt_eq_scalar(&a, b).unwrap();
        assert_eq!(e, r);
        let r = gt_eq_dyn_scalar(&a, b).is_err();
        assert!(r);
    }

    #[test]
    fn test_floating_zeros() {
        let a = Float32Array::from(vec![0.0_f32, -0.0]);
        let b = Float32Array::from(vec![-0.0_f32, 0.0]);

        let result = eq_dyn(&a, &b).unwrap();
        let excepted = BooleanArray::from(vec![false, false]);
        assert_eq!(excepted, result);

        let result = eq_dyn_scalar(&a, 0.0).unwrap();
        let excepted = BooleanArray::from(vec![true, false]);
        assert_eq!(excepted, result);

        let result = eq_dyn_scalar(&a, -0.0).unwrap();
        let excepted = BooleanArray::from(vec![false, true]);
        assert_eq!(excepted, result);
    }

    #[derive(Debug)]
    struct ToType {}

    impl ToType {
        fn to_i128(&self) -> Option<i128> {
            None
        }
    }

    #[test]
    fn test_try_to_type() {
        let a = ToType {};
        let to_type = try_to_type!(a, to_i128).unwrap_err();
        assert!(to_type
            .to_string()
            .contains("Could not convert ToType with to_i128"));
    }

    #[test]
    fn test_dictionary_nested_nulls() {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let v1 = Arc::new(Int32Array::from(vec![Some(0), None, Some(2)]));
        let a = DictionaryArray::new(keys.clone(), v1);
        let v2 = Arc::new(Int32Array::from(vec![None, Some(0), Some(2)]));
        let b = DictionaryArray::new(keys, v2);

        let r = eq_dyn(&a, &b).unwrap();
        assert_eq!(r.null_count(), 2);
        assert!(r.is_valid(2));
    }
}

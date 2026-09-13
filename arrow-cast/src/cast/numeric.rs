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

//! Cast support for numeric and boolean arrays.

use arrow_array::{builder::*, cast::*, types::*, *};
use arrow_schema::ArrowError;
use num_traits::NumCast;
use std::sync::Arc;

use super::CastOptions;

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
pub(crate) fn cast_numeric_arrays<FROM, TO>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
    TO: ArrowPrimitiveType,
    FROM::Native: NumCast,
    TO::Native: NumCast,
{
    if cast_options.safe {
        // If the value can't be casted to the `TO::Native`, return null
        Ok(Arc::new(numeric_cast::<FROM, TO>(
            from.as_primitive::<FROM>(),
        )))
    } else {
        // If the value can't be casted to the `TO::Native`, return error
        Ok(Arc::new(try_numeric_cast::<FROM, TO>(
            from.as_primitive::<FROM>(),
        )?))
    }
}

// Natural cast between numeric types
// If the value of T can't be casted to R, will throw error
fn try_numeric_cast<T, R>(from: &PrimitiveArray<T>) -> Result<PrimitiveArray<R>, ArrowError>
where
    T: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    T::Native: NumCast,
    R::Native: NumCast,
{
    from.try_unary(|value| {
        num_cast::<T::Native, R::Native>(value).ok_or_else(|| {
            ArrowError::CastError(format!(
                "Can't cast value {:?} to type {}",
                value,
                R::DATA_TYPE
            ))
        })
    })
}

/// Natural cast between numeric types
/// Return None if the input `value` can't be casted to type `O`.
#[inline]
pub fn num_cast<I, O>(value: I) -> Option<O>
where
    I: NumCast,
    O: NumCast,
{
    num_traits::cast::cast::<I, O>(value)
}

// Natural cast between numeric types
// If the value of T can't be casted to R, it will be converted to null
fn numeric_cast<T, R>(from: &PrimitiveArray<T>) -> PrimitiveArray<R>
where
    T: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    T::Native: NumCast,
    R::Native: NumCast,
{
    from.unary_opt::<_, R>(num_cast::<T::Native, R::Native>)
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
pub(crate) fn cast_numeric_to_bool<FROM>(from: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
{
    numeric_to_bool_cast::<FROM>(from.as_primitive::<FROM>()).map(|to| Arc::new(to) as ArrayRef)
}

fn numeric_to_bool_cast<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray, ArrowError>
where
    T: ArrowPrimitiveType,
{
    let mut b = BooleanBuilder::with_capacity(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null();
        } else {
            b.append_value(cast_num_to_bool::<T::Native>(from.value(i)));
        }
    }

    Ok(b.finish())
}

/// Cast numeric types to boolean
#[inline]
pub fn cast_num_to_bool<I>(value: I) -> bool
where
    I: Default + PartialEq,
{
    value != I::default()
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
pub(crate) fn cast_bool_to_numeric<TO>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    TO: ArrowPrimitiveType,
    TO::Native: num_traits::cast::NumCast,
{
    Ok(Arc::new(bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
        cast_options,
    )))
}

fn bool_to_numeric_cast<T>(from: &BooleanArray, _cast_options: &CastOptions) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::NumCast,
{
    let iter = (0..from.len()).map(|i| {
        if from.is_null(i) {
            None
        } else {
            single_bool_to_numeric::<T::Native>(from.value(i))
        }
    });
    // Benefit:
    //     20% performance improvement
    // Soundness:
    //     The iterator is trustedLen because it comes from a Range
    unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) }
}

/// Cast single bool value to numeric value.
#[inline]
pub fn single_bool_to_numeric<O>(value: bool) -> Option<O>
where
    O: num_traits::NumCast + Default,
{
    if value {
        // a workaround to cast a primitive to type O, infallible
        num_traits::cast::cast(1)
    } else {
        Some(O::default())
    }
}

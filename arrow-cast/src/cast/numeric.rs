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

use arrow_array::{cast::*, types::*, *};
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
    FROM::Native: NumCast + NumericNative,
    TO::Native: NumCast + NumericNative,
{
    let from = from.as_primitive::<FROM>();
    let array: PrimitiveArray<TO> = if const {
        is_infallible_numeric_cast(
            <FROM::Native as NumericNative>::KIND,
            <TO::Native as NumericNative>::KIND,
        )
    } {
        // This cast cannot fail, so the fastest kernel, `unary`, can be used.
        from.unary(|v| num_cast(v).expect("numeric cast is infallible"))
    } else if cast_options.safe {
        // If the value can't be cast to the `TO::Native`, return null
        from.unary_opt(num_cast)
    } else {
        // If the value can't be cast to the `TO::Native`, return error
        from.try_unary(|v| {
            num_cast(v).ok_or_else(|| {
                ArrowError::CastError(format!("Can't cast value {v:?} to type {}", TO::DATA_TYPE))
            })
        })?
    };
    Ok(Arc::new(array))
}

/// Properties of a numeric native type that decide whether [`num_cast`] to
/// another numeric type can fail.
#[derive(Clone, Copy)]
pub(crate) struct NumericKind {
    float: bool,
    signed: bool,
    bits: u32,
}

/// A native type with a [`NumericKind`].
pub(crate) trait NumericNative {
    const KIND: NumericKind;
}

macro_rules! numeric_native {
    ($($t:ty => $float:literal, $signed:literal;)*) => {$(
        impl NumericNative for $t {
            const KIND: NumericKind = NumericKind {
                float: $float,
                signed: $signed,
                bits: 8 * std::mem::size_of::<$t>() as u32,
            };
        }
    )*};
}

numeric_native! {
    u8 => false, false;
    u16 => false, false;
    u32 => false, false;
    u64 => false, false;
    i8 => false, true;
    i16 => false, true;
    i32 => false, true;
    i64 => false, true;
    half::f16 => true, true;
    f32 => true, true;
    f64 => true, true;
}

/// Returns true if [`num_cast`] succeeds for every value of `from` when casting
/// to `to`: every cast to a float, and integer casts whose target holds every
/// source value.
///
/// This must hold for every bit pattern of the source type, not only for the
/// valid values of a particular array, because [`PrimitiveArray::unary`] applies
/// the conversion to null slots as well, and their contents are arbitrary.
pub(crate) const fn is_infallible_numeric_cast(from: NumericKind, to: NumericKind) -> bool {
    if to.float {
        true
    } else if from.float {
        false
    } else if from.signed == to.signed {
        to.bits >= from.bits
    } else {
        // Only an unsigned source fits a strictly wider signed target.
        !from.signed && to.bits > from.bits
    }
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

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
pub(crate) fn cast_numeric_to_bool<FROM>(from: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
{
    Ok(Arc::new(BooleanArray::from_unary(
        from.as_primitive::<FROM>(),
        cast_num_to_bool,
    )))
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

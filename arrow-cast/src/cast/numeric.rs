// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Cast support for numeric and boolean arrays.

use crate::cast::*;

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

/// Rounds to `f32` by round-to-odd so the `f16` rounding is the only one (`f16::from_f64` rounds twice with F16C).
pub(crate) fn f64_to_f16(v: f64) -> half::f16 {
    let nearest = v as f32;
    if v.is_nan() || (nearest as f64) == v {
        return half::f16::from_f32(nearest);
    }
    let toward_zero = if (nearest as f64).abs() > v.abs() {
        f32::from_bits(nearest.to_bits() - 1)
    } else {
        nearest
    };
    half::f16::from_f32(f32::from_bits(toward_zero.to_bits() | 1))
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
{
    Ok(Arc::new(bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
        cast_options,
    )))
}

fn bool_to_numeric_cast<T>(from: &BooleanArray, _cast_options: &CastOptions) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    let to_numeric = |bits: u64, i: usize| {
        if bits & (1 << i) != 0 {
            T::Native::ONE
        } else {
            T::Native::ZERO
        }
    };
    // Unpack a 64-bit word at a time: the fixed-size inner loop helps the
    // compiler vectorize the conversion.
    let chunks = from.values().bit_chunks();
    let mut values = Vec::with_capacity(from.len());
    for bits in &chunks {
        values.extend((0..64).map(|i| to_numeric(bits, i)));
    }
    let bits = chunks.remainder_bits();
    values.extend((0..chunks.remainder_len()).map(|i| to_numeric(bits, i)));
    PrimitiveArray::new(values.into(), from.nulls().cloned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::*;
    use half::f16;

    /// Checks the fast-path classification against `num_cast` for every pair of
    /// numeric types. `num_cast` fails only on values outside the target's range,
    /// so each type's extremes decide, plus NaN for floats.
    #[test]
    fn test_is_infallible_numeric_cast() {
        macro_rules! check {
            ($from:ty => $values:expr, [$($to:ty),*]) => {$(
                let infallible = is_infallible_numeric_cast(
                    <<$from as ArrowPrimitiveType>::Native as NumericNative>::KIND,
                    <<$to as ArrowPrimitiveType>::Native as NumericNative>::KIND,
                );
                let succeeds = $values
                    .iter()
                    .all(|&v| num_cast::<_, <$to as ArrowPrimitiveType>::Native>(v).is_some());
                assert_eq!(
                    infallible,
                    succeeds,
                    "{} to {}",
                    <$from as ArrowPrimitiveType>::DATA_TYPE,
                    <$to as ArrowPrimitiveType>::DATA_TYPE
                );
            )*};
        }
        macro_rules! check_all {
            ($($from:ty => $values:expr),* $(,)?) => {$(
                check!($from => $values, [
                    UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                    Int32Type, Int64Type, Float16Type, Float32Type, Float64Type
                ]);
            )*};
        }
        check_all! {
            UInt8Type => [u8::MIN, u8::MAX],
            UInt16Type => [u16::MIN, u16::MAX],
            UInt32Type => [u32::MIN, u32::MAX],
            UInt64Type => [u64::MIN, u64::MAX],
            Int8Type => [i8::MIN, i8::MAX],
            Int16Type => [i16::MIN, i16::MAX],
            Int32Type => [i32::MIN, i32::MAX],
            Int64Type => [i64::MIN, i64::MAX],
            Float16Type => [f16::MIN, f16::MAX, f16::NAN],
            Float32Type => [f32::MIN, f32::MAX, f32::NAN],
            Float64Type => [f64::MIN, f64::MAX, f64::NAN],
        }
    }
}

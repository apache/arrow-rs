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

//! Cast kernels to convert [`ArrayRef`]  between supported datatypes.
//!
//! See [`cast_with_options`] for more information on specific conversions.
//!
//! Example:
//!
//! ```
//! # use arrow_array::*;
//! # use arrow_cast::cast;
//! # use arrow_schema::DataType;
//! # use std::sync::Arc;
//! # use arrow_array::types::Float64Type;
//! # use arrow_array::cast::AsArray;
//! // int32 to float64
//! let a = Int32Array::from(vec![5, 6, 7]);
//! let b = cast(&a, &DataType::Float64).unwrap();
//! let c = b.as_primitive::<Float64Type>();
//! assert_eq!(5.0, c.value(0));
//! assert_eq!(6.0, c.value(1));
//! assert_eq!(7.0, c.value(2));
//! ```

mod binary;
mod decimal;
mod dictionary;
mod list;
mod map;
mod numeric;
mod run_array;
mod string;
mod structs;
mod temporal;
mod union;

use crate::cast::binary::*;
use crate::cast::decimal::*;
use crate::cast::dictionary::*;
use crate::cast::list::*;
use crate::cast::map::*;
pub(crate) use crate::cast::numeric::{
    cast_bool_to_numeric, cast_numeric_arrays, cast_numeric_to_bool,
};
pub use crate::cast::numeric::{cast_num_to_bool, num_cast, single_bool_to_numeric};
use crate::cast::run_array::*;
use crate::cast::string::*;
use crate::cast::structs::*;
use crate::cast::temporal::*;
pub use crate::cast::union::*;

use chrono::{TimeZone, Utc};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::display::{ArrayFormatter, FormatOptions};
use crate::parse::{
    Parser, parse_interval_day_time, parse_interval_month_day_nano, parse_interval_year_month,
    string_to_datetime,
};
use arrow_array::{builder::*, cast::*, temporal_conversions::*, timezone::Tz, types::*, *};
use arrow_buffer::{ArrowNativeType, OffsetBuffer, i256};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::*;
use arrow_select::take::take;
use num_traits::{NumCast, ToPrimitive, cast::AsPrimitive};

pub use decimal::{DecimalCast, float_to_decimal, float_to_decimal_opt, rescale_decimal};
#[expect(deprecated)]
pub use decimal::{parse_string_to_decimal_native, single_float_to_decimal};
pub use string::cast_single_string_to_boolean_default;

/// Integers below 2^53 convert to an `f64` exactly.
const F64_EXACT_INT_LIMIT: f64 = 9_007_199_254_740_992.0;
/// Integers below 2^24 convert to an `f32` exactly.
const F32_EXACT_INT_LIMIT: f64 = 16_777_216.0;

/// Lossy conversion from decimal to float.
///
/// Returns the `f64` nearest to the decimal's exact value, rounding once, for any
/// scale that fits in an `i8` -- as every Arrow decimal's does.
///
/// Conversion is lossy and follows standard floating point semantics. Values
/// that exceed the representable range become `INFINITY` or `-INFINITY` without
/// returning an error.
#[inline(always)]
pub fn single_decimal_to_float_lossy<D, F>(f: &F, x: D::Native, scale: i32) -> f64
where
    D: DecimalType,
    F: Fn(D::Native) -> f64,
{
    let unscaled = f(x);
    // Both operands are exact in this range, so this rounds once. A negative scale
    // multiplies, since `10^-scale` is the exact power of ten there.
    if (-22..=22).contains(&scale) && unscaled.abs() < F64_EXACT_INT_LIMIT {
        return if scale >= 0 {
            unscaled / 10_f64.powi(scale)
        } else {
            unscaled * 10_f64.powi(-scale)
        };
    }
    match i8::try_from(scale) {
        Ok(scale) => decimal_to_f64_rounded_once::<D>(x, scale, unscaled),
        Err(_) => unscaled / 10_f64.powi(scale),
    }
}

/// Rounds once for the values the arithmetic cannot convert exactly, by parsing
/// the decimal's own text.
#[cold]
#[inline(never)]
fn decimal_to_f64_rounded_once<D: DecimalType>(x: D::Native, scale: i8, unscaled: f64) -> f64 {
    // `format_decimal` always writes a decimal literal, so the parse cannot fail.
    // The fallback is there to keep that from being a panic, not because it runs.
    D::format_decimal(x, u8::MAX, scale)
        .parse::<f64>()
        .unwrap_or_else(|_| unscaled / 10_f64.powi(scale.into()))
}

/// As [`decimal_to_f64_rounded_once`], but narrowing to `f32` in one step.
/// Rounding to `f64` first and then to `f32` rounds twice: a decimal just above an
/// `f32` midpoint can collapse onto that midpoint in `f64`, and round-half-even
/// then sends it the wrong way.
#[cold]
#[inline(never)]
fn decimal_to_f32_rounded_once<D: DecimalType>(x: D::Native, scale: i8, unscaled: f64) -> f32 {
    // Unreachable, as in [`decimal_to_f64_rounded_once`].
    D::format_decimal(x, u8::MAX, scale)
        .parse::<f32>()
        .unwrap_or_else(|_| unscaled as f32 / 10_f32.powi(scale.into()))
}

/// Casts a decimal array to `Float64`, rounding each value once.
fn cast_decimal_to_f64<D, F>(
    array: &dyn Array,
    as_float: &F,
    scale: i8,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType,
    F: Fn(D::Native) -> f64,
{
    let array = array.as_primitive::<D>();
    // No power of ten outside this range is exactly representable.
    if !(-22..=22).contains(&scale) {
        let values = array
            .unary::<_, Float64Type>(|x| decimal_to_f64_rounded_once::<D>(x, scale, as_float(x)));
        return Ok(Arc::new(values));
    }
    let pow = 10_f64.powi(scale.unsigned_abs().into());
    let values = if scale >= 0 {
        array.unary::<_, Float64Type>(|x| {
            let unscaled = as_float(x);
            if unscaled.abs() < F64_EXACT_INT_LIMIT {
                unscaled / pow
            } else {
                decimal_to_f64_rounded_once::<D>(x, scale, unscaled)
            }
        })
    } else {
        array.unary::<_, Float64Type>(|x| {
            let unscaled = as_float(x);
            if unscaled.abs() < F64_EXACT_INT_LIMIT {
                unscaled * pow
            } else {
                decimal_to_f64_rounded_once::<D>(x, scale, unscaled)
            }
        })
    };
    Ok(Arc::new(values))
}

/// Casts a decimal array to `Float32`, rounding each value once. Same shape as
/// [`cast_decimal_to_f64`], with the smaller bounds an `f32` allows: integers are
/// exact below 2^24, and `10^k` only up to `k = 10`.
fn cast_decimal_to_f32<D, F>(
    array: &dyn Array,
    as_float: &F,
    scale: i8,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType,
    F: Fn(D::Native) -> f64,
{
    let array = array.as_primitive::<D>();
    // No power of ten outside this range is exactly representable.
    if !(-10..=10).contains(&scale) {
        let values = array
            .unary::<_, Float32Type>(|x| decimal_to_f32_rounded_once::<D>(x, scale, as_float(x)));
        return Ok(Arc::new(values));
    }
    let pow = 10_f32.powi(scale.unsigned_abs().into());
    let values = if scale >= 0 {
        array.unary::<_, Float32Type>(|x| {
            let unscaled = as_float(x);
            if unscaled.abs() < F32_EXACT_INT_LIMIT {
                unscaled as f32 / pow
            } else {
                decimal_to_f32_rounded_once::<D>(x, scale, unscaled)
            }
        })
    } else {
        array.unary::<_, Float32Type>(|x| {
            let unscaled = as_float(x);
            if unscaled.abs() < F32_EXACT_INT_LIMIT {
                unscaled as f32 * pow
            } else {
                decimal_to_f32_rounded_once::<D>(x, scale, unscaled)
            }
        })
    };
    Ok(Arc::new(values))
}

/// CastOptions provides a way to override the default cast behaviors
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastOptions<'a> {
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    pub safe: bool,
    /// Formatting options when casting from temporal types to string
    pub format_options: FormatOptions<'a>,
}

impl Default for CastOptions<'_> {
    fn default() -> Self {
        Self {
            safe: true,
            format_options: FormatOptions::default(),
        }
    }
}

/// Return true if a value of type `from_type` can be cast into a value of `to_type`.
///
/// See [`cast_with_options`] for more information
pub fn can_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    use self::DataType::*;
    use self::IntervalUnit::*;
    use self::TimeUnit::*;
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (Null, _) => true,
        // Dictionary/List conditions should be put in front of others
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (Union(fields, _), _) => union::resolve_child_array(fields, to_type).is_some(),
        (_, Union(_, _)) => false,
        (RunEndEncoded(_, value_type), _) => can_cast_types(value_type.data_type(), to_type),
        (_, RunEndEncoded(_, value_type)) => can_cast_types(from_type, value_type.data_type()),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),
        (
            List(list_from) | LargeList(list_from) | ListView(list_from) | LargeListView(list_from),
            List(list_to) | LargeList(list_to) | ListView(list_to) | LargeListView(list_to),
        ) => can_cast_types(list_from.data_type(), list_to.data_type()),
        (
            List(list_from) | LargeList(list_from) | ListView(list_from) | LargeListView(list_from),
            Utf8 | LargeUtf8 | Utf8View,
        ) => can_cast_types(list_from.data_type(), to_type),
        (
            FixedSizeList(list_from, _),
            List(list_to) | LargeList(list_to) | ListView(list_to) | LargeListView(list_to),
        ) => can_cast_types(list_from.data_type(), list_to.data_type()),
        (
            List(list_from) | LargeList(list_from) | ListView(list_from) | LargeListView(list_from),
            FixedSizeList(list_to, _),
        ) => can_cast_types(list_from.data_type(), list_to.data_type()),
        (FixedSizeList(inner, size), FixedSizeList(inner_to, size_to)) if size == size_to => {
            can_cast_types(inner.data_type(), inner_to.data_type())
        }
        (_, List(list_to) | LargeList(list_to) | ListView(list_to) | LargeListView(list_to)) => {
            can_cast_types(from_type, list_to.data_type())
        }
        (_, FixedSizeList(list_to, size)) if *size == 1 => {
            can_cast_types(from_type, list_to.data_type())
        }
        (FixedSizeList(list_from, size), _) if *size == 1 => {
            can_cast_types(list_from.data_type(), to_type)
        }
        (Map(from_entries, ordered_from), Map(to_entries, ordered_to))
            if ordered_from == ordered_to =>
        {
            match (
                key_field(from_entries),
                key_field(to_entries),
                value_field(from_entries),
                value_field(to_entries),
            ) {
                (Some(from_key), Some(to_key), Some(from_value), Some(to_value)) => {
                    can_cast_types(from_key.data_type(), to_key.data_type())
                        && can_cast_types(from_value.data_type(), to_value.data_type())
                }
                _ => false,
            }
        }
        // cast one decimal type to another decimal type
        (
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
        ) => true,
        // unsigned integer to decimal
        (
            UInt8 | UInt16 | UInt32 | UInt64,
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
        ) => true,
        // signed numeric to decimal
        (
            Int8 | Int16 | Int32 | Int64 | Float16 | Float32 | Float64,
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
        ) => true,
        // decimal to unsigned numeric
        (
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
            UInt8 | UInt16 | UInt32 | UInt64,
        ) => true,
        // decimal to signed numeric
        (
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
            Null | Int8 | Int16 | Int32 | Int64 | Float16 | Float32 | Float64,
        ) => true,
        // decimal to string
        (
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
            Utf8View | Utf8 | LargeUtf8,
        ) => true,
        // string to decimal
        (
            Utf8View | Utf8 | LargeUtf8,
            Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _),
        ) => true,
        (Struct(from_fields), Struct(to_fields)) => {
            if from_fields.len() != to_fields.len() {
                return false;
            }

            // fast path, all field names are in the same order and same number of fields
            if from_fields
                .iter()
                .zip(to_fields.iter())
                .all(|(f1, f2)| f1.name() == f2.name())
            {
                return from_fields.iter().zip(to_fields.iter()).all(|(f1, f2)| {
                    // Assume that nullability between two structs are compatible, if not,
                    // cast kernel will return error.
                    can_cast_types(f1.data_type(), f2.data_type())
                });
            }

            // slow path, we match the fields by name
            if to_fields.iter().all(|to_field| {
                from_fields
                    .iter()
                    .find(|from_field| from_field.name() == to_field.name())
                    .is_some_and(|from_field| {
                        // Assume that nullability between two structs are compatible, if not,
                        // cast kernel will return error.
                        can_cast_types(from_field.data_type(), to_field.data_type())
                    })
            }) {
                return true;
            }

            // if we couldn't match by name, we try to see if they can be matched by position
            from_fields
                .iter()
                .zip(to_fields.iter())
                .all(|(f1, f2)| can_cast_types(f1.data_type(), f2.data_type()))
        }
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (_, Boolean) => from_type.is_integer() || from_type.is_floating() || from_type.is_string(),
        (Boolean, _) => to_type.is_integer() || to_type.is_floating() || to_type.is_string(),

        (Binary, LargeBinary | Utf8 | LargeUtf8 | FixedSizeBinary(_) | BinaryView | Utf8View) => {
            true
        }
        (LargeBinary, Binary | Utf8 | LargeUtf8 | FixedSizeBinary(_) | BinaryView | Utf8View) => {
            true
        }
        (FixedSizeBinary(_), Binary | LargeBinary | BinaryView) => true,
        (
            Utf8 | LargeUtf8 | Utf8View,
            Binary
            | LargeBinary
            | Utf8
            | LargeUtf8
            | Date32
            | Date64
            | Time32(Second | Millisecond)
            | Time64(Microsecond | Nanosecond)
            | Timestamp(Second | Millisecond | Microsecond | Nanosecond, _)
            | Interval(_)
            | BinaryView,
        ) => true,
        (Utf8 | LargeUtf8, Utf8View) => true,
        (BinaryView, Binary | LargeBinary | Utf8 | LargeUtf8 | Utf8View) => true,
        (Utf8View | Utf8 | LargeUtf8, _) => to_type.is_numeric(),
        (_, Utf8 | Utf8View | LargeUtf8) => from_type.is_primitive(),

        (_, Binary | LargeBinary) => from_type.is_integer(),

        // start numeric casts
        (
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float16 | Float32
            | Float64,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float16 | Float32
            | Float64,
        ) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32 | Date64 | Time32(_)) => true,
        (Date32, Int32 | Int64) => true,
        (Time32(_), Int32 | Int64) => true,
        (Int64, Date64 | Date32 | Time64(_)) => true,
        (Date64, Int64 | Int32) => true,
        (Time64(_), Int64) => true,
        (Date32 | Date64, Date32 | Date64) => true,
        // time casts
        (Time32(_), Time32(_)) => true,
        (Time32(_), Time64(_)) => true,
        (Time64(_), Time64(_)) => true,
        (Time64(_), Time32(to_unit)) => {
            matches!(to_unit, Second | Millisecond)
        }
        (Timestamp(_, _), _) if to_type.is_numeric() => true,
        (_, Timestamp(_, _)) if from_type.is_numeric() => true,
        (Date64, Timestamp(_, _)) => true,
        (Date32, Timestamp(_, _)) => true,
        (
            Timestamp(_, _),
            Timestamp(_, _)
            | Date32
            | Date64
            | Time32(Second | Millisecond)
            | Time64(Microsecond | Nanosecond),
        ) => true,
        (_, Duration(_)) if from_type.is_numeric() => true,
        (Duration(_), _) if to_type.is_numeric() => true,
        (Duration(_), Duration(_)) => true,
        // No `(Interval(_), Int64)` arm: `cast_with_options` implements no such cast.
        (Int32, Interval(to_type)) => match to_type {
            YearMonth => true,
            DayTime => false,
            MonthDayNano => false,
        },
        (Duration(_), Interval(MonthDayNano)) => true,
        (Interval(MonthDayNano), Duration(_)) => true,
        (Interval(YearMonth), Interval(MonthDayNano)) => true,
        (Interval(DayTime), Interval(MonthDayNano)) => true,
        (_, _) => false,
    }
}

/// Cast `array` to the provided data type and return a new Array with type `to_type`, if possible.
///
/// See [`cast_with_options`] for more information
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to_type, &CastOptions::default())
}

/// Convert an integer to a decimal native value without wrapping.
///
/// `AsPrimitive` / `as` silently truncates when the source is wider than `M`
/// (for example `5_000_000_000i64 as i32`). All integer sources fit in `i128`
/// losslessly, which [`DecimalCast`] then converts to the decimal native type
/// with a range check. For types that always fit (e.g. `i64` to `Decimal128`) this
/// should get optimized to being equivalent to `i64 as i128`.
fn integer_to_decimal_native<I, M>(value: I) -> Option<M>
where
    I: Into<i128>,
    M: DecimalCast,
{
    M::from_decimal(value.into())
}

/// Returns true if every value of `array` has at most `digits` decimal digits.
///
/// If the source type cannot hold that many digits, every value fits;
/// otherwise the data is scanned, including null slots, since `unary`
/// converts those as well.
fn integers_fit_digits<T>(array: &PrimitiveArray<T>, digits: u32) -> bool
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp,
{
    let Ok(bound) = T::Native::usize_as(10).pow_checked(digits) else {
        // The type's maximum is below the bound, and its minimum, one larger
        // in magnitude, is a power of two that no power of ten can equal.
        return true;
    };
    let hi = bound.sub_wrapping(T::Native::ONE);
    // Unsigned types have no negative values to bound.
    let lo = T::Native::ZERO.sub_checked(hi).unwrap_or(T::Native::ZERO);

    // The scan works in chunks, so an array that does not fit is rejected after
    // its first failing chunk. Each chunk is checked without branching, so it
    // vectorizes.
    array
        .values()
        .chunks(64)
        .all(|chunk| chunk.iter().fold(true, |ok, &v| ok & (v >= lo) & (v <= hi)))
}

fn cast_integer_to_decimal<
    T: ArrowPrimitiveType,
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M,
>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + Into<i128>,
    M: ArrowNativeTypeOp + DecimalCast,
{
    let overflow = |v: T::Native| {
        ArrowError::CastError(format!(
            "Cannot cast to {}({precision}, {scale}). Overflowing on {v:?}",
            D::PREFIX,
        ))
    };

    // An invalid precision or scale would let the bound below admit values the
    // decimal native type cannot hold, so reject it before choosing a kernel.
    validate_decimal_precision_and_scale::<D>(precision, scale)?;

    // Scan for values that don't fit in the target type before choosing a
    // kernel: in the common case that all values fit, the cast is infallible
    // and we can use the `unary` kernel, which is very fast. Empirically, the
    // cost of doing the out-of-range scan is small relative to the win from
    // using a faster kernel.
    //
    // A value fits when it has at most `precision - scale` decimal digits: a
    // nonnegative scale appends that many trailing zeros, and a negative scale
    // removes digits first. Validation guarantees the difference is not
    // negative.
    let digits = (precision as i32 - scale as i32) as u32;
    let all_fit = integers_fit_digits(array, digits);

    let array = if scale < 0 {
        // Compute the scale factor once in the source type. Scaling before the
        // checked conversion permits values that only fit the decimal native
        // type after scaling.
        //
        // scale_factor is always > 0, so div_wrapping(scale_factor) can never
        // overflow/panic
        let scale_factor = T::Native::usize_as(10)
            .pow_checked(scale.unsigned_abs() as u32)
            .ok();

        match (scale_factor, cast_options.safe) {
            (Some(scale_factor), _) if all_fit => array.unary::<_, D>(|v| {
                integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))
                    .expect("value fits the decimal")
            }),
            (Some(scale_factor), true) => array.unary_opt::<_, D>(|v| {
                let v = integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))?;
                (D::is_valid_decimal_precision(v, precision)).then_some(v)
            }),
            (Some(scale_factor), false) => array.try_unary::<_, D, _>(|v| {
                let v = integer_to_decimal_native::<_, M>(v.div_wrapping(scale_factor))
                    .ok_or_else(|| overflow(v))?;
                D::validate_decimal_precision(v, precision, scale).map(|()| v)
            })?,
            // A scale factor that overflows the source type is larger than all
            // source values, so integer division produces zero.
            //
            // For a well formed decimal scale, this path should never be reachable.
            (None, _) => array.unary::<_, D>(|_| M::ZERO),
        }
    } else {
        let scale_factor = M::usize_as(10)
            .pow_checked(scale.unsigned_abs() as u32)
            .map_err(|_| {
                ArrowError::CastError(format!(
                    "Cannot cast to {:?}({}, {}). The scale causes overflow.",
                    D::PREFIX,
                    precision,
                    scale,
                ))
            })?;

        if all_fit {
            array.unary::<_, D>(|v| {
                integer_to_decimal_native::<_, M>(v)
                    .expect("value fits the decimal")
                    .mul_wrapping(scale_factor)
            })
        } else if cast_options.safe {
            array.unary_opt::<_, D>(|v| {
                let v = integer_to_decimal_native::<_, M>(v)
                    .and_then(|v| v.mul_checked(scale_factor).ok())?;
                (D::is_valid_decimal_precision(v, precision)).then_some(v)
            })
        } else {
            array.try_unary::<_, D, _>(|v| {
                let v = integer_to_decimal_native::<_, M>(v)
                    .ok_or_else(|| overflow(v))
                    .and_then(|v| v.mul_checked(scale_factor))?;
                D::validate_decimal_precision(v, precision, scale).map(|()| v)
            })?
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(precision, scale)?))
}

/// Cast the primitive array using [`PrimitiveArray::reinterpret_cast`]
fn cast_reinterpret_arrays<I: ArrowPrimitiveType, O: ArrowPrimitiveType<Native = I::Native>>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(array.as_primitive::<I>().reinterpret_cast::<O>()))
}

/// Try to cast `array` to `to_type` if possible.
///
/// Returns a new Array with type `to_type` if possible.
///
/// Accepts [`CastOptions`] to specify cast behavior. See also [`cast()`].
///
/// # Behavior
/// * `Boolean` to `Utf8`, `LargeUtf8`, or `Utf8View`: `true` => `"true"`, `false` => `"false"`
/// * `Utf8` to `Boolean`: `true`, `yes`, `on`, `1` => `true`, `false`, `no`, `off`, `0` => `false`,
///   short variants are accepted, other strings return null or error
/// * `Utf8` to Numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to `Boolean`: 0 returns `false`, any other value returns `true`
/// * `List` to `List`: the underlying data type is cast
/// * `List` to `FixedSizeList`: the underlying data type is cast. If safe is true and a list element
///   has the wrong length it will be replaced with NULL, otherwise an error will be returned
/// * Primitive to `List`: a list array with 1 value per slot is created
/// * `Date32` and `Date64`: precision lost when going to higher interval
/// * `Time32` and `Time64`: precision lost when going to higher interval
/// * `Timestamp` and `Date{32|64}`: precision lost when going to higher interval
/// * `Timestamp` to `Date32`, `Time32`, or `Time64`: timestamps without a timezone
///   support the full `i64` range; timestamps with a timezone must lie within Chrono's
///   date range. If safe is true, a timestamp outside that range or a `Date32` day
///   count that does not fit in `i32` becomes NULL, otherwise an error is returned
/// * Temporal to/from backing Primitive: zero-copy with data type change
/// * `Float16/Float32/Float64` to `Decimal(precision, scale)` rounds to the `scale` decimals
///   (i.e. casting `6.4999` to `Decimal(10, 1)` becomes `6.5`).
/// * `Decimal` to `Float16/Float32/Float64` is lossy and values outside the representable
///   range become `INFINITY` or `-INFINITY` without error.
///
/// Unsupported Casts (check with `can_cast_types` before calling):
/// * To or from `StructArray`
/// * `List` to `Primitive`
/// * `Interval` and `Duration`
///
/// # Durations and Intervals
///
/// Casting integer types directly to interval types such as
/// [`IntervalMonthDayNano`] is not supported because the meaning of the integer
/// is ambiguous. For example, the integer  could represent either nanoseconds
/// or months.
///
/// To cast an integer type to an interval type, first convert to a Duration
/// type, and then cast that to the desired interval type.
///
/// For example, to convert an `Int64` representing nanoseconds to an
/// `IntervalMonthDayNano` you would first convert the `Int64` to a
/// `DurationNanoseconds`, and then cast that to `IntervalMonthDayNano`.
///
/// # Timestamps and Timezones
///
/// Timestamps are stored with an optional timezone in Arrow.
///
/// ## Casting timestamps to a timestamp without timezone / UTC
/// ```
/// # use arrow_array::Int64Array;
/// # use arrow_array::types::TimestampSecondType;
/// # use arrow_cast::{cast, display};
/// # use arrow_array::cast::AsArray;
/// # use arrow_schema::{DataType, TimeUnit};
/// // can use "UTC" if chrono-tz feature is enabled, here use offset based timezone
/// let data_type = DataType::Timestamp(TimeUnit::Second, None);
/// let a = Int64Array::from(vec![1_000_000_000, 2_000_000_000, 3_000_000_000]);
/// let b = cast(&a, &data_type).unwrap();
/// let b = b.as_primitive::<TimestampSecondType>(); // downcast to result type
/// assert_eq!(2_000_000_000, b.value(1)); // values are the same as the type has no timezone
/// // use display to show them (note has no trailing Z)
/// assert_eq!("2033-05-18T03:33:20", display::array_value_to_string(&b, 1).unwrap());
/// ```
///
/// ## Casting timestamps to a timestamp with timezone
///
/// Similarly to the previous example, if you cast numeric values to a timestamp
/// with timezone, the cast kernel will not change the underlying values
/// but display and other functions will interpret them as being in the provided timezone.
///
/// ```
/// # use arrow_array::Int64Array;
/// # use arrow_array::types::TimestampSecondType;
/// # use arrow_cast::{cast, display};
/// # use arrow_array::cast::AsArray;
/// # use arrow_schema::{DataType, TimeUnit};
/// // can use "Americas/New_York" if chrono-tz feature is enabled, here use offset based timezone
/// let data_type = DataType::Timestamp(TimeUnit::Second, Some("-05:00".into()));
/// let a = Int64Array::from(vec![1_000_000_000, 2_000_000_000, 3_000_000_000]);
/// let b = cast(&a, &data_type).unwrap();
/// let b = b.as_primitive::<TimestampSecondType>(); // downcast to result type
/// assert_eq!(2_000_000_000, b.value(1)); // values are still the same
/// // displayed in the target timezone (note the offset -05:00)
/// assert_eq!("2033-05-17T22:33:20-05:00", display::array_value_to_string(&b, 1).unwrap());
/// ```
/// # Casting timestamps without timezone to timestamps with timezone
///
/// When casting from a timestamp without timezone to a timestamp with
/// timezone, the cast kernel interprets the timestamp values as being in
/// the destination timezone and then adjusts the underlying value to UTC as required
///
/// However, note that when casting from a timestamp with timezone BACK to a
/// timestamp without timezone the cast kernel does not adjust the values.
///
/// Thus round trip casting a timestamp without timezone to a timestamp with
/// timezone and back to a timestamp without timezone results in different
/// values than the starting values.
///
/// ```
/// # use arrow_array::Int64Array;
/// # use arrow_array::types::{TimestampSecondType};
/// # use arrow_cast::{cast, display};
/// # use arrow_array::cast::AsArray;
/// # use arrow_schema::{DataType, TimeUnit};
/// let data_type  = DataType::Timestamp(TimeUnit::Second, None);
/// let data_type_tz = DataType::Timestamp(TimeUnit::Second, Some("-05:00".into()));
/// let a = Int64Array::from(vec![1_000_000_000, 2_000_000_000, 3_000_000_000]);
/// let b = cast(&a, &data_type).unwrap(); // cast to timestamp without timezone
/// let b = b.as_primitive::<TimestampSecondType>(); // downcast to result type
/// assert_eq!(2_000_000_000, b.value(1)); // values are still the same
/// // displayed without a timezone (note lack of offset or Z)
/// assert_eq!("2033-05-18T03:33:20", display::array_value_to_string(&b, 1).unwrap());
///
/// // Convert timestamps without a timezone to timestamps with a timezone
/// let c = cast(&b, &data_type_tz).unwrap();
/// let c = c.as_primitive::<TimestampSecondType>(); // downcast to result type
/// assert_eq!(2_000_018_000, c.value(1)); // value has been adjusted by offset
/// // displayed with the target timezone offset (-05:00)
/// assert_eq!("2033-05-18T03:33:20-05:00", display::array_value_to_string(&c, 1).unwrap());
///
/// // Convert from timestamp with timezone back to timestamp without timezone
/// let d = cast(&c, &data_type).unwrap();
/// let d = d.as_primitive::<TimestampSecondType>(); // downcast to result type
/// assert_eq!(2_000_018_000, d.value(1)); // value has not been adjusted
/// // NOTE: the timestamp is adjusted (08:33:20 instead of 03:33:20 as in previous example)
/// assert_eq!("2033-05-18T08:33:20", display::array_value_to_string(&d, 1).unwrap());
/// ```
pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    let from_type = array.data_type();
    // clone array if types are the same
    if from_type == to_type {
        return Ok(make_array(array.to_data()));
    }
    match (from_type, to_type) {
        (Null, _) => Ok(new_null_array(to_type, array.len())),
        (RunEndEncoded(index_type, _), _) => match index_type.data_type() {
            Int16 => run_end_encoded_cast::<Int16Type>(array, to_type, cast_options),
            Int32 => run_end_encoded_cast::<Int32Type>(array, to_type, cast_options),
            Int64 => run_end_encoded_cast::<Int64Type>(array, to_type, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from run end encoded type {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (_, RunEndEncoded(index_type, value_type)) => {
            let array_ref = make_array(array.to_data());
            match index_type.data_type() {
                Int16 => cast_to_run_end_encoded::<Int16Type>(
                    &array_ref,
                    value_type.data_type(),
                    cast_options,
                ),
                Int32 => cast_to_run_end_encoded::<Int32Type>(
                    &array_ref,
                    value_type.data_type(),
                    cast_options,
                ),
                Int64 => cast_to_run_end_encoded::<Int64Type>(
                    &array_ref,
                    value_type.data_type(),
                    cast_options,
                ),
                _ => Err(ArrowError::CastError(format!(
                    "Casting from type {from_type:?} to run end encoded type {to_type:?} not supported",
                ))),
            }
        }
        (Union(_, _), _) => union_extract_by_type(
            array.as_any().downcast_ref::<UnionArray>().unwrap(),
            to_type,
            cast_options,
        ),
        (_, Union(_, _)) => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
        (Dictionary(index_type, _), _) => match **index_type {
            Int8 => dictionary_cast::<Int8Type>(array, to_type, cast_options),
            Int16 => dictionary_cast::<Int16Type>(array, to_type, cast_options),
            Int32 => dictionary_cast::<Int32Type>(array, to_type, cast_options),
            Int64 => dictionary_cast::<Int64Type>(array, to_type, cast_options),
            UInt8 => dictionary_cast::<UInt8Type>(array, to_type, cast_options),
            UInt16 => dictionary_cast::<UInt16Type>(array, to_type, cast_options),
            UInt32 => dictionary_cast::<UInt32Type>(array, to_type, cast_options),
            UInt64 => dictionary_cast::<UInt64Type>(array, to_type, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from dictionary type {from_type} to {to_type} not supported",
            ))),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            Int8 => cast_to_dictionary::<Int8Type>(array, value_type, cast_options),
            Int16 => cast_to_dictionary::<Int16Type>(array, value_type, cast_options),
            Int32 => cast_to_dictionary::<Int32Type>(array, value_type, cast_options),
            Int64 => cast_to_dictionary::<Int64Type>(array, value_type, cast_options),
            UInt8 => cast_to_dictionary::<UInt8Type>(array, value_type, cast_options),
            UInt16 => cast_to_dictionary::<UInt16Type>(array, value_type, cast_options),
            UInt32 => cast_to_dictionary::<UInt32Type>(array, value_type, cast_options),
            UInt64 => cast_to_dictionary::<UInt64Type>(array, value_type, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from type {from_type} to dictionary type {to_type} not supported",
            ))),
        },
        // Casting between lists of same types (cast inner values)
        (List(_), List(to)) => cast_list_values::<i32>(array, to, cast_options),
        (LargeList(_), LargeList(to)) => cast_list_values::<i64>(array, to, cast_options),
        (FixedSizeList(_, size_from), FixedSizeList(list_to, size_to)) => {
            if size_from != size_to {
                return Err(ArrowError::CastError(
                    "cannot cast fixed-size-list to fixed-size-list with different size".into(),
                ));
            }
            let array = array.as_fixed_size_list();
            let values = cast_with_options(array.values(), list_to.data_type(), cast_options)?;
            let nulls = array.nulls().cloned();
            let len = array.len();
            Ok(Arc::new(FixedSizeListArray::try_new_with_length(
                list_to.clone(),
                *size_from,
                values,
                nulls,
                len,
            )?))
        }
        (ListView(_), ListView(to)) => cast_list_view_values::<i32>(array, to, cast_options),
        (LargeListView(_), LargeListView(to)) => {
            cast_list_view_values::<i64>(array, to, cast_options)
        }
        // Casting between different types of lists
        // List
        (List(_), LargeList(list_to)) => cast_list::<i32, i64>(array, list_to, cast_options),
        (List(_), FixedSizeList(field, size)) => {
            cast_list_to_fixed_size_list::<i32>(array, field, *size, cast_options)
        }
        (List(_), ListView(list_to)) => {
            cast_list_to_list_view::<i32, i32>(array, list_to, cast_options)
        }
        (List(_), LargeListView(list_to)) => {
            cast_list_to_list_view::<i32, i64>(array, list_to, cast_options)
        }
        // LargeList
        (LargeList(_), List(list_to)) => cast_list::<i64, i32>(array, list_to, cast_options),
        (LargeList(_), FixedSizeList(field, size)) => {
            cast_list_to_fixed_size_list::<i64>(array, field, *size, cast_options)
        }
        (LargeList(_), ListView(list_to)) => {
            cast_list_to_list_view::<i64, i32>(array, list_to, cast_options)
        }
        (LargeList(_), LargeListView(list_to)) => {
            cast_list_to_list_view::<i64, i64>(array, list_to, cast_options)
        }
        // ListView
        (ListView(_), List(list_to)) => {
            cast_list_view_to_list::<i32, Int32Type>(array, list_to, cast_options)
        }
        (ListView(_), LargeList(list_to)) => {
            cast_list_view_to_list::<i32, Int64Type>(array, list_to, cast_options)
        }
        (ListView(_), LargeListView(list_to)) => {
            cast_list_view::<i32, i64>(array, list_to, cast_options)
        }
        (ListView(_), FixedSizeList(field, size)) => {
            cast_list_view_to_fixed_size_list::<i32>(array, field, *size, cast_options)
        }
        // LargeListView
        (LargeListView(_), LargeList(list_to)) => {
            cast_list_view_to_list::<i64, Int64Type>(array, list_to, cast_options)
        }
        (LargeListView(_), List(list_to)) => {
            cast_list_view_to_list::<i64, Int32Type>(array, list_to, cast_options)
        }
        (LargeListView(_), ListView(list_to)) => {
            cast_list_view::<i64, i32>(array, list_to, cast_options)
        }
        (LargeListView(_), FixedSizeList(field, size)) => {
            cast_list_view_to_fixed_size_list::<i64>(array, field, *size, cast_options)
        }
        // FixedSizeList
        (FixedSizeList(_, _), List(list_to)) => {
            cast_fixed_size_list_to_list::<i32>(array, list_to, cast_options)
        }
        (FixedSizeList(_, _), LargeList(list_to)) => {
            cast_fixed_size_list_to_list::<i64>(array, list_to, cast_options)
        }
        (FixedSizeList(_, _), ListView(list_to)) => {
            cast_fixed_size_list_to_list_view::<i32>(array, list_to, cast_options)
        }
        (FixedSizeList(_, _), LargeListView(list_to)) => {
            cast_fixed_size_list_to_list_view::<i64>(array, list_to, cast_options)
        }
        // List to/from other types
        (FixedSizeList(_, size), _) if *size == 1 => {
            cast_single_element_fixed_size_list_to_values(array, to_type, cast_options)
        }
        // NOTE: we could support FSL to string here too but might be confusing
        //       since behaviour for size 1 would be different (see arm above)
        (List(_) | LargeList(_) | ListView(_) | LargeListView(_), _) => match to_type {
            Utf8 => value_to_string::<i32>(array, cast_options),
            LargeUtf8 => value_to_string::<i64>(array, cast_options),
            Utf8View => value_to_string_view(array, cast_options),
            dt => Err(ArrowError::CastError(format!(
                "Cannot cast LIST to non-list data type {dt}"
            ))),
        },
        (_, List(to)) => cast_values_to_list::<i32>(array, to, cast_options),
        (_, LargeList(to)) => cast_values_to_list::<i64>(array, to, cast_options),
        (_, ListView(to)) => cast_values_to_list_view::<i32>(array, to, cast_options),
        (_, LargeListView(to)) => cast_values_to_list_view::<i64>(array, to, cast_options),
        (_, FixedSizeList(to, size)) if *size == 1 => {
            let values = cast_with_options(array, to.data_type(), cast_options)?;
            let list = FixedSizeListArray::try_new(to.clone(), 1, values, None)?;
            Ok(Arc::new(list))
        }
        // Map
        (Map(_, ordered1), Map(_, ordered2)) if ordered1 == ordered2 => {
            cast_map_values(array.as_map(), to_type, cast_options, ordered1.to_owned())
        }
        // Decimal to decimal, same width
        (Decimal32(p1, s1), Decimal32(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal32Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal64(p1, s1), Decimal64(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal64Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal128Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(p1, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal256Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        // Decimal to decimal, different width
        (Decimal32(p1, s1), Decimal64(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal32Type, Decimal64Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal32(p1, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal32Type, Decimal128Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal32(p1, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal32Type, Decimal256Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal64(p1, s1), Decimal32(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal64Type, Decimal32Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal64(p1, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal64Type, Decimal128Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal64(p1, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal64Type, Decimal256Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(p1, s1), Decimal32(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal128Type, Decimal32Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(p1, s1), Decimal64(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal128Type, Decimal64Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(p1, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal128Type, Decimal256Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(p1, s1), Decimal32(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal256Type, Decimal32Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(p1, s1), Decimal64(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal256Type, Decimal64Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(p1, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal256Type, Decimal128Type>(
                array.as_primitive(),
                *p1,
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        // Decimal to non-decimal
        (Decimal32(_, scale), _) if !to_type.is_temporal() => {
            cast_from_decimal::<Decimal32Type, _>(
                array,
                scale,
                from_type,
                to_type,
                |x: i32| x as f64,
                cast_options,
            )
        }
        (Decimal64(_, scale), _) if !to_type.is_temporal() => {
            cast_from_decimal::<Decimal64Type, _>(
                array,
                scale,
                from_type,
                to_type,
                |x: i64| x as f64,
                cast_options,
            )
        }
        (Decimal128(_, scale), _) if !to_type.is_temporal() => {
            cast_from_decimal::<Decimal128Type, _>(
                array,
                scale,
                from_type,
                to_type,
                |x: i128| x as f64,
                cast_options,
            )
        }
        (Decimal256(_, scale), _) if !to_type.is_temporal() => {
            cast_from_decimal::<Decimal256Type, _>(
                array,
                scale,
                from_type,
                to_type,
                |x: i256| x.to_f64().expect("All i256 values fit in f64"),
                cast_options,
            )
        }
        // Non-decimal to decimal
        (_, Decimal32(precision, scale)) if !from_type.is_temporal() => {
            cast_to_decimal::<Decimal32Type, _>(
                array,
                precision,
                scale,
                from_type,
                to_type,
                cast_options,
            )
        }
        (_, Decimal64(precision, scale)) if !from_type.is_temporal() => {
            cast_to_decimal::<Decimal64Type, _>(
                array,
                precision,
                scale,
                from_type,
                to_type,
                cast_options,
            )
        }
        (_, Decimal128(precision, scale)) if !from_type.is_temporal() => {
            cast_to_decimal::<Decimal128Type, _>(
                array,
                precision,
                scale,
                from_type,
                to_type,
                cast_options,
            )
        }
        (_, Decimal256(precision, scale)) if !from_type.is_temporal() => {
            cast_to_decimal::<Decimal256Type, _>(
                array,
                precision,
                scale,
                from_type,
                to_type,
                cast_options,
            )
        }
        (Struct(from_fields), Struct(to_fields)) => cast_struct_to_struct(
            array.as_struct(),
            from_fields.clone(),
            to_fields.clone(),
            cast_options,
        ),
        (Struct(_), _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
        (_, Struct(_)) => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
        (_, Boolean) => match from_type {
            UInt8 => cast_numeric_to_bool::<UInt8Type>(array),
            UInt16 => cast_numeric_to_bool::<UInt16Type>(array),
            UInt32 => cast_numeric_to_bool::<UInt32Type>(array),
            UInt64 => cast_numeric_to_bool::<UInt64Type>(array),
            Int8 => cast_numeric_to_bool::<Int8Type>(array),
            Int16 => cast_numeric_to_bool::<Int16Type>(array),
            Int32 => cast_numeric_to_bool::<Int32Type>(array),
            Int64 => cast_numeric_to_bool::<Int64Type>(array),
            Float16 => cast_numeric_to_bool::<Float16Type>(array),
            Float32 => cast_numeric_to_bool::<Float32Type>(array),
            Float64 => cast_numeric_to_bool::<Float64Type>(array),
            Utf8View => cast_utf8view_to_boolean(array, cast_options),
            Utf8 => cast_utf8_to_boolean::<i32>(array, cast_options),
            LargeUtf8 => cast_utf8_to_boolean::<i64>(array, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => cast_bool_to_numeric::<UInt8Type>(array, cast_options),
            UInt16 => cast_bool_to_numeric::<UInt16Type>(array, cast_options),
            UInt32 => cast_bool_to_numeric::<UInt32Type>(array, cast_options),
            UInt64 => cast_bool_to_numeric::<UInt64Type>(array, cast_options),
            Int8 => cast_bool_to_numeric::<Int8Type>(array, cast_options),
            Int16 => cast_bool_to_numeric::<Int16Type>(array, cast_options),
            Int32 => cast_bool_to_numeric::<Int32Type>(array, cast_options),
            Int64 => cast_bool_to_numeric::<Int64Type>(array, cast_options),
            Float16 => cast_bool_to_numeric::<Float16Type>(array, cast_options),
            Float32 => cast_bool_to_numeric::<Float32Type>(array, cast_options),
            Float64 => cast_bool_to_numeric::<Float64Type>(array, cast_options),
            Utf8View => cast_bool_to_string::<StringViewArray>(array),
            Utf8 => cast_bool_to_string::<StringArray>(array),
            LargeUtf8 => cast_bool_to_string::<LargeStringArray>(array),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (Utf8, _) => match to_type {
            UInt8 => parse_string::<UInt8Type, i32>(array, cast_options),
            UInt16 => parse_string::<UInt16Type, i32>(array, cast_options),
            UInt32 => parse_string::<UInt32Type, i32>(array, cast_options),
            UInt64 => parse_string::<UInt64Type, i32>(array, cast_options),
            Int8 => parse_string::<Int8Type, i32>(array, cast_options),
            Int16 => parse_string::<Int16Type, i32>(array, cast_options),
            Int32 => parse_string::<Int32Type, i32>(array, cast_options),
            Int64 => parse_string::<Int64Type, i32>(array, cast_options),
            Float16 => parse_string::<Float16Type, i32>(array, cast_options),
            Float32 => parse_string::<Float32Type, i32>(array, cast_options),
            Float64 => parse_string::<Float64Type, i32>(array, cast_options),
            Date32 => parse_string::<Date32Type, i32>(array, cast_options),
            Date64 => parse_string::<Date64Type, i32>(array, cast_options),
            Binary => Ok(Arc::new(BinaryArray::from(
                array.as_string::<i32>().clone(),
            ))),
            LargeBinary => {
                let binary = BinaryArray::from(array.as_string::<i32>().clone());
                cast_byte_container::<BinaryType, LargeBinaryType>(&binary)
            }
            Utf8View => Ok(Arc::new(StringViewArray::from(array.as_string::<i32>()))),
            BinaryView => Ok(Arc::new(
                StringViewArray::from(array.as_string::<i32>()).to_binary_view(),
            )),
            LargeUtf8 => cast_byte_container::<Utf8Type, LargeUtf8Type>(array),
            Time32(TimeUnit::Second) => parse_string::<Time32SecondType, i32>(array, cast_options),
            Time32(TimeUnit::Millisecond) => {
                parse_string::<Time32MillisecondType, i32>(array, cast_options)
            }
            Time64(TimeUnit::Microsecond) => {
                parse_string::<Time64MicrosecondType, i32>(array, cast_options)
            }
            Time64(TimeUnit::Nanosecond) => {
                parse_string::<Time64NanosecondType, i32>(array, cast_options)
            }
            Timestamp(TimeUnit::Second, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampSecondType,
            >(array, to_tz.as_ref(), cast_options),
            Timestamp(TimeUnit::Millisecond, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampMillisecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Microsecond, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampMicrosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Nanosecond, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampNanosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Interval(IntervalUnit::YearMonth) => {
                cast_string_to_year_month_interval::<i32>(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => {
                cast_string_to_day_time_interval::<i32>(array, cast_options)
            }
            Interval(IntervalUnit::MonthDayNano) => {
                cast_string_to_month_day_nano_interval::<i32>(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (Utf8View, _) => match to_type {
            UInt8 => parse_string_view::<UInt8Type>(array, cast_options),
            UInt16 => parse_string_view::<UInt16Type>(array, cast_options),
            UInt32 => parse_string_view::<UInt32Type>(array, cast_options),
            UInt64 => parse_string_view::<UInt64Type>(array, cast_options),
            Int8 => parse_string_view::<Int8Type>(array, cast_options),
            Int16 => parse_string_view::<Int16Type>(array, cast_options),
            Int32 => parse_string_view::<Int32Type>(array, cast_options),
            Int64 => parse_string_view::<Int64Type>(array, cast_options),
            Float16 => parse_string_view::<Float16Type>(array, cast_options),
            Float32 => parse_string_view::<Float32Type>(array, cast_options),
            Float64 => parse_string_view::<Float64Type>(array, cast_options),
            Date32 => parse_string_view::<Date32Type>(array, cast_options),
            Date64 => parse_string_view::<Date64Type>(array, cast_options),
            Binary => cast_view_to_byte::<StringViewType, GenericBinaryType<i32>>(array),
            LargeBinary => cast_view_to_byte::<StringViewType, GenericBinaryType<i64>>(array),
            BinaryView => Ok(Arc::new(array.as_string_view().clone().to_binary_view())),
            Utf8 => cast_view_to_byte::<StringViewType, GenericStringType<i32>>(array),
            LargeUtf8 => cast_view_to_byte::<StringViewType, GenericStringType<i64>>(array),
            Time32(TimeUnit::Second) => parse_string_view::<Time32SecondType>(array, cast_options),
            Time32(TimeUnit::Millisecond) => {
                parse_string_view::<Time32MillisecondType>(array, cast_options)
            }
            Time64(TimeUnit::Microsecond) => {
                parse_string_view::<Time64MicrosecondType>(array, cast_options)
            }
            Time64(TimeUnit::Nanosecond) => {
                parse_string_view::<Time64NanosecondType>(array, cast_options)
            }
            Timestamp(TimeUnit::Second, to_tz) => {
                cast_view_to_timestamp::<TimestampSecondType>(array, to_tz.as_ref(), cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => cast_view_to_timestamp::<
                TimestampMillisecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Microsecond, to_tz) => cast_view_to_timestamp::<
                TimestampMicrosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Nanosecond, to_tz) => cast_view_to_timestamp::<
                TimestampNanosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Interval(IntervalUnit::YearMonth) => {
                cast_view_to_year_month_interval(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => cast_view_to_day_time_interval(array, cast_options),
            Interval(IntervalUnit::MonthDayNano) => {
                cast_view_to_month_day_nano_interval(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (LargeUtf8, _) => match to_type {
            UInt8 => parse_string::<UInt8Type, i64>(array, cast_options),
            UInt16 => parse_string::<UInt16Type, i64>(array, cast_options),
            UInt32 => parse_string::<UInt32Type, i64>(array, cast_options),
            UInt64 => parse_string::<UInt64Type, i64>(array, cast_options),
            Int8 => parse_string::<Int8Type, i64>(array, cast_options),
            Int16 => parse_string::<Int16Type, i64>(array, cast_options),
            Int32 => parse_string::<Int32Type, i64>(array, cast_options),
            Int64 => parse_string::<Int64Type, i64>(array, cast_options),
            Float16 => parse_string::<Float16Type, i64>(array, cast_options),
            Float32 => parse_string::<Float32Type, i64>(array, cast_options),
            Float64 => parse_string::<Float64Type, i64>(array, cast_options),
            Date32 => parse_string::<Date32Type, i64>(array, cast_options),
            Date64 => parse_string::<Date64Type, i64>(array, cast_options),
            Utf8 => cast_byte_container::<LargeUtf8Type, Utf8Type>(array),
            Binary => {
                let large_binary = LargeBinaryArray::from(array.as_string::<i64>().clone());
                cast_byte_container::<LargeBinaryType, BinaryType>(&large_binary)
            }
            LargeBinary => Ok(Arc::new(LargeBinaryArray::from(
                array.as_string::<i64>().clone(),
            ))),
            Utf8View => Ok(Arc::new(StringViewArray::from(array.as_string::<i64>()))),
            BinaryView => Ok(Arc::new(BinaryViewArray::from(
                array
                    .as_string::<i64>()
                    .into_iter()
                    .map(|x| x.map(|x| x.as_bytes()))
                    .collect::<Vec<_>>(),
            ))),
            Time32(TimeUnit::Second) => parse_string::<Time32SecondType, i64>(array, cast_options),
            Time32(TimeUnit::Millisecond) => {
                parse_string::<Time32MillisecondType, i64>(array, cast_options)
            }
            Time64(TimeUnit::Microsecond) => {
                parse_string::<Time64MicrosecondType, i64>(array, cast_options)
            }
            Time64(TimeUnit::Nanosecond) => {
                parse_string::<Time64NanosecondType, i64>(array, cast_options)
            }
            Timestamp(TimeUnit::Second, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampSecondType,
            >(array, to_tz.as_ref(), cast_options),
            Timestamp(TimeUnit::Millisecond, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampMillisecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Microsecond, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampMicrosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Timestamp(TimeUnit::Nanosecond, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampNanosecondType,
            >(
                array, to_tz.as_ref(), cast_options
            ),
            Interval(IntervalUnit::YearMonth) => {
                cast_string_to_year_month_interval::<i64>(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => {
                cast_string_to_day_time_interval::<i64>(array, cast_options)
            }
            Interval(IntervalUnit::MonthDayNano) => {
                cast_string_to_month_day_nano_interval::<i64>(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (Binary, _) => match to_type {
            Utf8 => cast_binary_to_string::<i32>(array, cast_options),
            LargeUtf8 => {
                let array = cast_binary_to_string::<i32>(array, cast_options)?;
                cast_byte_container::<Utf8Type, LargeUtf8Type>(array.as_ref())
            }
            LargeBinary => cast_byte_container::<BinaryType, LargeBinaryType>(array),
            FixedSizeBinary(size) => {
                cast_binary_to_fixed_size_binary::<i32>(array, *size, cast_options)
            }
            BinaryView => Ok(Arc::new(BinaryViewArray::from(array.as_binary::<i32>()))),
            Utf8View => Ok(Arc::new(StringViewArray::from(
                cast_binary_to_string::<i32>(array, cast_options)?.as_string::<i32>(),
            ))),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (LargeBinary, _) => match to_type {
            Utf8 => {
                let array = cast_binary_to_string::<i64>(array, cast_options)?;
                cast_byte_container::<LargeUtf8Type, Utf8Type>(array.as_ref())
            }
            LargeUtf8 => cast_binary_to_string::<i64>(array, cast_options),
            Binary => cast_byte_container::<LargeBinaryType, BinaryType>(array),
            FixedSizeBinary(size) => {
                cast_binary_to_fixed_size_binary::<i64>(array, *size, cast_options)
            }
            BinaryView => Ok(Arc::new(BinaryViewArray::from(array.as_binary::<i64>()))),
            Utf8View => {
                let array = cast_binary_to_string::<i64>(array, cast_options)?;
                Ok(Arc::new(StringViewArray::from(array.as_string::<i64>())))
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (FixedSizeBinary(size), _) => match to_type {
            Binary => cast_fixed_size_binary_to_binary::<i32>(array, *size),
            LargeBinary => cast_fixed_size_binary_to_binary::<i64>(array, *size),
            BinaryView => cast_fixed_size_binary_to_binary_view(array, *size),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (BinaryView, Binary) => cast_view_to_byte::<BinaryViewType, GenericBinaryType<i32>>(array),
        (BinaryView, LargeBinary) => {
            cast_view_to_byte::<BinaryViewType, GenericBinaryType<i64>>(array)
        }
        (BinaryView, Utf8) => {
            let binary_arr = cast_view_to_byte::<BinaryViewType, GenericBinaryType<i32>>(array)?;
            cast_binary_to_string::<i32>(&binary_arr, cast_options)
        }
        (BinaryView, LargeUtf8) => {
            let binary_arr = cast_view_to_byte::<BinaryViewType, GenericBinaryType<i64>>(array)?;
            cast_binary_to_string::<i64>(&binary_arr, cast_options)
        }
        (BinaryView, Utf8View) => cast_binary_view_to_string_view(array, cast_options),
        (BinaryView, _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported",
        ))),
        (from_type, Utf8View) if from_type.is_primitive() => {
            value_to_string_view(array, cast_options)
        }
        (from_type, LargeUtf8) if from_type.is_primitive() => {
            value_to_string::<i64>(array, cast_options)
        }
        (from_type, Utf8) if from_type.is_primitive() => {
            value_to_string::<i32>(array, cast_options)
        }
        (from_type, Binary) if from_type.is_integer() => match from_type {
            UInt8 => cast_numeric_to_binary::<UInt8Type, i32>(array),
            UInt16 => cast_numeric_to_binary::<UInt16Type, i32>(array),
            UInt32 => cast_numeric_to_binary::<UInt32Type, i32>(array),
            UInt64 => cast_numeric_to_binary::<UInt64Type, i32>(array),
            Int8 => cast_numeric_to_binary::<Int8Type, i32>(array),
            Int16 => cast_numeric_to_binary::<Int16Type, i32>(array),
            Int32 => cast_numeric_to_binary::<Int32Type, i32>(array),
            Int64 => cast_numeric_to_binary::<Int64Type, i32>(array),
            _ => unreachable!(),
        },
        (from_type, LargeBinary) if from_type.is_integer() => match from_type {
            UInt8 => cast_numeric_to_binary::<UInt8Type, i64>(array),
            UInt16 => cast_numeric_to_binary::<UInt16Type, i64>(array),
            UInt32 => cast_numeric_to_binary::<UInt32Type, i64>(array),
            UInt64 => cast_numeric_to_binary::<UInt64Type, i64>(array),
            Int8 => cast_numeric_to_binary::<Int8Type, i64>(array),
            Int16 => cast_numeric_to_binary::<Int16Type, i64>(array),
            Int32 => cast_numeric_to_binary::<Int32Type, i64>(array),
            Int64 => cast_numeric_to_binary::<Int64Type, i64>(array),
            _ => unreachable!(),
        },
        // start numeric casts
        (UInt8, UInt16) => cast_numeric_arrays::<UInt8Type, UInt16Type>(array, cast_options),
        (UInt8, UInt32) => cast_numeric_arrays::<UInt8Type, UInt32Type>(array, cast_options),
        (UInt8, UInt64) => cast_numeric_arrays::<UInt8Type, UInt64Type>(array, cast_options),
        (UInt8, Int8) => cast_numeric_arrays::<UInt8Type, Int8Type>(array, cast_options),
        (UInt8, Int16) => cast_numeric_arrays::<UInt8Type, Int16Type>(array, cast_options),
        (UInt8, Int32) => cast_numeric_arrays::<UInt8Type, Int32Type>(array, cast_options),
        (UInt8, Int64) => cast_numeric_arrays::<UInt8Type, Int64Type>(array, cast_options),
        (UInt8, Float16) => cast_numeric_arrays::<UInt8Type, Float16Type>(array, cast_options),
        (UInt8, Float32) => cast_numeric_arrays::<UInt8Type, Float32Type>(array, cast_options),
        (UInt8, Float64) => cast_numeric_arrays::<UInt8Type, Float64Type>(array, cast_options),

        (UInt16, UInt8) => cast_numeric_arrays::<UInt16Type, UInt8Type>(array, cast_options),
        (UInt16, UInt32) => cast_numeric_arrays::<UInt16Type, UInt32Type>(array, cast_options),
        (UInt16, UInt64) => cast_numeric_arrays::<UInt16Type, UInt64Type>(array, cast_options),
        (UInt16, Int8) => cast_numeric_arrays::<UInt16Type, Int8Type>(array, cast_options),
        (UInt16, Int16) => cast_numeric_arrays::<UInt16Type, Int16Type>(array, cast_options),
        (UInt16, Int32) => cast_numeric_arrays::<UInt16Type, Int32Type>(array, cast_options),
        (UInt16, Int64) => cast_numeric_arrays::<UInt16Type, Int64Type>(array, cast_options),
        (UInt16, Float16) => cast_numeric_arrays::<UInt16Type, Float16Type>(array, cast_options),
        (UInt16, Float32) => cast_numeric_arrays::<UInt16Type, Float32Type>(array, cast_options),
        (UInt16, Float64) => cast_numeric_arrays::<UInt16Type, Float64Type>(array, cast_options),

        (UInt32, UInt8) => cast_numeric_arrays::<UInt32Type, UInt8Type>(array, cast_options),
        (UInt32, UInt16) => cast_numeric_arrays::<UInt32Type, UInt16Type>(array, cast_options),
        (UInt32, UInt64) => cast_numeric_arrays::<UInt32Type, UInt64Type>(array, cast_options),
        (UInt32, Int8) => cast_numeric_arrays::<UInt32Type, Int8Type>(array, cast_options),
        (UInt32, Int16) => cast_numeric_arrays::<UInt32Type, Int16Type>(array, cast_options),
        (UInt32, Int32) => cast_numeric_arrays::<UInt32Type, Int32Type>(array, cast_options),
        (UInt32, Int64) => cast_numeric_arrays::<UInt32Type, Int64Type>(array, cast_options),
        (UInt32, Float16) => cast_numeric_arrays::<UInt32Type, Float16Type>(array, cast_options),
        (UInt32, Float32) => cast_numeric_arrays::<UInt32Type, Float32Type>(array, cast_options),
        (UInt32, Float64) => cast_numeric_arrays::<UInt32Type, Float64Type>(array, cast_options),

        (UInt64, UInt8) => cast_numeric_arrays::<UInt64Type, UInt8Type>(array, cast_options),
        (UInt64, UInt16) => cast_numeric_arrays::<UInt64Type, UInt16Type>(array, cast_options),
        (UInt64, UInt32) => cast_numeric_arrays::<UInt64Type, UInt32Type>(array, cast_options),
        (UInt64, Int8) => cast_numeric_arrays::<UInt64Type, Int8Type>(array, cast_options),
        (UInt64, Int16) => cast_numeric_arrays::<UInt64Type, Int16Type>(array, cast_options),
        (UInt64, Int32) => cast_numeric_arrays::<UInt64Type, Int32Type>(array, cast_options),
        (UInt64, Int64) => cast_numeric_arrays::<UInt64Type, Int64Type>(array, cast_options),
        (UInt64, Float16) => cast_numeric_arrays::<UInt64Type, Float16Type>(array, cast_options),
        (UInt64, Float32) => cast_numeric_arrays::<UInt64Type, Float32Type>(array, cast_options),
        (UInt64, Float64) => cast_numeric_arrays::<UInt64Type, Float64Type>(array, cast_options),

        (Int8, UInt8) => cast_numeric_arrays::<Int8Type, UInt8Type>(array, cast_options),
        (Int8, UInt16) => cast_numeric_arrays::<Int8Type, UInt16Type>(array, cast_options),
        (Int8, UInt32) => cast_numeric_arrays::<Int8Type, UInt32Type>(array, cast_options),
        (Int8, UInt64) => cast_numeric_arrays::<Int8Type, UInt64Type>(array, cast_options),
        (Int8, Int16) => cast_numeric_arrays::<Int8Type, Int16Type>(array, cast_options),
        (Int8, Int32) => cast_numeric_arrays::<Int8Type, Int32Type>(array, cast_options),
        (Int8, Int64) => cast_numeric_arrays::<Int8Type, Int64Type>(array, cast_options),
        (Int8, Float16) => cast_numeric_arrays::<Int8Type, Float16Type>(array, cast_options),
        (Int8, Float32) => cast_numeric_arrays::<Int8Type, Float32Type>(array, cast_options),
        (Int8, Float64) => cast_numeric_arrays::<Int8Type, Float64Type>(array, cast_options),

        (Int16, UInt8) => cast_numeric_arrays::<Int16Type, UInt8Type>(array, cast_options),
        (Int16, UInt16) => cast_numeric_arrays::<Int16Type, UInt16Type>(array, cast_options),
        (Int16, UInt32) => cast_numeric_arrays::<Int16Type, UInt32Type>(array, cast_options),
        (Int16, UInt64) => cast_numeric_arrays::<Int16Type, UInt64Type>(array, cast_options),
        (Int16, Int8) => cast_numeric_arrays::<Int16Type, Int8Type>(array, cast_options),
        (Int16, Int32) => cast_numeric_arrays::<Int16Type, Int32Type>(array, cast_options),
        (Int16, Int64) => cast_numeric_arrays::<Int16Type, Int64Type>(array, cast_options),
        (Int16, Float16) => cast_numeric_arrays::<Int16Type, Float16Type>(array, cast_options),
        (Int16, Float32) => cast_numeric_arrays::<Int16Type, Float32Type>(array, cast_options),
        (Int16, Float64) => cast_numeric_arrays::<Int16Type, Float64Type>(array, cast_options),

        (Int32, UInt8) => cast_numeric_arrays::<Int32Type, UInt8Type>(array, cast_options),
        (Int32, UInt16) => cast_numeric_arrays::<Int32Type, UInt16Type>(array, cast_options),
        (Int32, UInt32) => cast_numeric_arrays::<Int32Type, UInt32Type>(array, cast_options),
        (Int32, UInt64) => cast_numeric_arrays::<Int32Type, UInt64Type>(array, cast_options),
        (Int32, Int8) => cast_numeric_arrays::<Int32Type, Int8Type>(array, cast_options),
        (Int32, Int16) => cast_numeric_arrays::<Int32Type, Int16Type>(array, cast_options),
        (Int32, Int64) => cast_numeric_arrays::<Int32Type, Int64Type>(array, cast_options),
        (Int32, Float16) => cast_numeric_arrays::<Int32Type, Float16Type>(array, cast_options),
        (Int32, Float32) => cast_numeric_arrays::<Int32Type, Float32Type>(array, cast_options),
        (Int32, Float64) => cast_numeric_arrays::<Int32Type, Float64Type>(array, cast_options),

        (Int64, UInt8) => cast_numeric_arrays::<Int64Type, UInt8Type>(array, cast_options),
        (Int64, UInt16) => cast_numeric_arrays::<Int64Type, UInt16Type>(array, cast_options),
        (Int64, UInt32) => cast_numeric_arrays::<Int64Type, UInt32Type>(array, cast_options),
        (Int64, UInt64) => cast_numeric_arrays::<Int64Type, UInt64Type>(array, cast_options),
        (Int64, Int8) => cast_numeric_arrays::<Int64Type, Int8Type>(array, cast_options),
        (Int64, Int16) => cast_numeric_arrays::<Int64Type, Int16Type>(array, cast_options),
        (Int64, Int32) => cast_numeric_arrays::<Int64Type, Int32Type>(array, cast_options),
        (Int64, Float16) => cast_numeric_arrays::<Int64Type, Float16Type>(array, cast_options),
        (Int64, Float32) => cast_numeric_arrays::<Int64Type, Float32Type>(array, cast_options),
        (Int64, Float64) => cast_numeric_arrays::<Int64Type, Float64Type>(array, cast_options),

        (Float16, UInt8) => cast_numeric_arrays::<Float16Type, UInt8Type>(array, cast_options),
        (Float16, UInt16) => cast_numeric_arrays::<Float16Type, UInt16Type>(array, cast_options),
        (Float16, UInt32) => cast_numeric_arrays::<Float16Type, UInt32Type>(array, cast_options),
        (Float16, UInt64) => cast_numeric_arrays::<Float16Type, UInt64Type>(array, cast_options),
        (Float16, Int8) => cast_numeric_arrays::<Float16Type, Int8Type>(array, cast_options),
        (Float16, Int16) => cast_numeric_arrays::<Float16Type, Int16Type>(array, cast_options),
        (Float16, Int32) => cast_numeric_arrays::<Float16Type, Int32Type>(array, cast_options),
        (Float16, Int64) => cast_numeric_arrays::<Float16Type, Int64Type>(array, cast_options),
        (Float16, Float32) => cast_numeric_arrays::<Float16Type, Float32Type>(array, cast_options),
        (Float16, Float64) => cast_numeric_arrays::<Float16Type, Float64Type>(array, cast_options),

        (Float32, UInt8) => cast_numeric_arrays::<Float32Type, UInt8Type>(array, cast_options),
        (Float32, UInt16) => cast_numeric_arrays::<Float32Type, UInt16Type>(array, cast_options),
        (Float32, UInt32) => cast_numeric_arrays::<Float32Type, UInt32Type>(array, cast_options),
        (Float32, UInt64) => cast_numeric_arrays::<Float32Type, UInt64Type>(array, cast_options),
        (Float32, Int8) => cast_numeric_arrays::<Float32Type, Int8Type>(array, cast_options),
        (Float32, Int16) => cast_numeric_arrays::<Float32Type, Int16Type>(array, cast_options),
        (Float32, Int32) => cast_numeric_arrays::<Float32Type, Int32Type>(array, cast_options),
        (Float32, Int64) => cast_numeric_arrays::<Float32Type, Int64Type>(array, cast_options),
        (Float32, Float16) => cast_numeric_arrays::<Float32Type, Float16Type>(array, cast_options),
        (Float32, Float64) => cast_numeric_arrays::<Float32Type, Float64Type>(array, cast_options),

        (Float64, UInt8) => cast_numeric_arrays::<Float64Type, UInt8Type>(array, cast_options),
        (Float64, UInt16) => cast_numeric_arrays::<Float64Type, UInt16Type>(array, cast_options),
        (Float64, UInt32) => cast_numeric_arrays::<Float64Type, UInt32Type>(array, cast_options),
        (Float64, UInt64) => cast_numeric_arrays::<Float64Type, UInt64Type>(array, cast_options),
        (Float64, Int8) => cast_numeric_arrays::<Float64Type, Int8Type>(array, cast_options),
        (Float64, Int16) => cast_numeric_arrays::<Float64Type, Int16Type>(array, cast_options),
        (Float64, Int32) => cast_numeric_arrays::<Float64Type, Int32Type>(array, cast_options),
        (Float64, Int64) => cast_numeric_arrays::<Float64Type, Int64Type>(array, cast_options),
        (Float64, Float16) => cast_numeric_arrays::<Float64Type, Float16Type>(array, cast_options),
        (Float64, Float32) => cast_numeric_arrays::<Float64Type, Float32Type>(array, cast_options),
        // end numeric casts

        // temporal casts
        (Int32, Date32) => cast_reinterpret_arrays::<Int32Type, Date32Type>(array),
        (Int32, Date64) => cast_with_options(
            &cast_with_options(array, &Date32, cast_options)?,
            &Date64,
            cast_options,
        ),
        (Int32, Time32(TimeUnit::Second)) => {
            cast_reinterpret_arrays::<Int32Type, Time32SecondType>(array)
        }
        (Int32, Time32(TimeUnit::Millisecond)) => {
            cast_reinterpret_arrays::<Int32Type, Time32MillisecondType>(array)
        }
        // No support for microsecond/nanosecond with i32
        (Date32, Int32) => cast_reinterpret_arrays::<Date32Type, Int32Type>(array),
        (Date32, Int64) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Int64,
            cast_options,
        ),
        (Time32(TimeUnit::Second), Int32) => {
            cast_reinterpret_arrays::<Time32SecondType, Int32Type>(array)
        }
        (Time32(TimeUnit::Millisecond), Int32) => {
            cast_reinterpret_arrays::<Time32MillisecondType, Int32Type>(array)
        }
        (Time32(TimeUnit::Second), Int64) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Int64,
            cast_options,
        ),
        (Time32(TimeUnit::Millisecond), Int64) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Int64,
            cast_options,
        ),
        (Int64, Date64) => cast_reinterpret_arrays::<Int64Type, Date64Type>(array),
        (Int64, Date32) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Date32,
            cast_options,
        ),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => {
            cast_reinterpret_arrays::<Int64Type, Time64MicrosecondType>(array)
        }
        (Int64, Time64(TimeUnit::Nanosecond)) => {
            cast_reinterpret_arrays::<Int64Type, Time64NanosecondType>(array)
        }

        (Date64, Int64) => cast_reinterpret_arrays::<Date64Type, Int64Type>(array),
        (Date64, Int32) => cast_with_options(
            &cast_with_options(array, &Int64, cast_options)?,
            &Int32,
            cast_options,
        ),
        (Time64(TimeUnit::Microsecond), Int64) => {
            cast_reinterpret_arrays::<Time64MicrosecondType, Int64Type>(array)
        }
        (Time64(TimeUnit::Nanosecond), Int64) => {
            cast_reinterpret_arrays::<Time64NanosecondType, Int64Type>(array)
        }
        (Date32, Date64) => Ok(Arc::new(
            array
                .as_primitive::<Date32Type>()
                .unary::<_, Date64Type>(|x| x as i64 * MILLISECONDS_IN_DAY),
        )),
        (Date64, Date32) => {
            let array = array.as_primitive::<Date64Type>();
            let result = if cast_options.safe {
                array.unary_opt::<_, Date32Type>(|x| i32::try_from(x / MILLISECONDS_IN_DAY).ok())
            } else {
                array.try_unary::<_, Date32Type, _>(|x| {
                    i32::try_from(x / MILLISECONDS_IN_DAY).map_err(|_| {
                        ArrowError::CastError(format!(
                            "Cannot cast Date64 value {x} to Date32 without overflow"
                        ))
                    })
                })?
            };
            Ok(Arc::new(result))
        }

        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => {
            let array = array.as_primitive::<Time32SecondType>();
            let result = if cast_options.safe {
                array.unary_opt::<_, Time32MillisecondType>(|x| x.checked_mul(MILLISECONDS as i32))
            } else {
                array.try_unary::<_, Time32MillisecondType, _>(|x| {
                    x.mul_checked(MILLISECONDS as i32)
                })?
            };
            Ok(Arc::new(result))
        }
        (Time32(TimeUnit::Second), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time32SecondType>()
                .unary::<_, Time64MicrosecondType>(|x| x as i64 * MICROSECONDS),
        )),
        (Time32(TimeUnit::Second), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time32SecondType>()
                .unary::<_, Time64NanosecondType>(|x| x as i64 * NANOSECONDS),
        )),

        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array
                .as_primitive::<Time32MillisecondType>()
                .unary::<_, Time32SecondType>(|x| x / MILLISECONDS as i32),
        )),
        (Time32(TimeUnit::Millisecond), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time32MillisecondType>()
                .unary::<_, Time64MicrosecondType>(|x| x as i64 * (MICROSECONDS / MILLISECONDS)),
        )),
        (Time32(TimeUnit::Millisecond), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time32MillisecondType>()
                .unary::<_, Time64NanosecondType>(|x| x as i64 * (NANOSECONDS / MILLISECONDS)),
        )),

        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Second)) => {
            cast_time64_to_time32::<Time64MicrosecondType, Time32SecondType>(
                array,
                MICROSECONDS,
                cast_options,
            )
        }
        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Millisecond)) => {
            cast_time64_to_time32::<Time64MicrosecondType, Time32MillisecondType>(
                array,
                MICROSECONDS / MILLISECONDS,
                cast_options,
            )
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => {
            let array = array.as_primitive::<Time64MicrosecondType>();
            let result = if cast_options.safe {
                array.unary_opt::<_, Time64NanosecondType>(|x| {
                    x.checked_mul(NANOSECONDS / MICROSECONDS)
                })
            } else {
                array.try_unary::<_, Time64NanosecondType, _>(|x| {
                    x.mul_checked(NANOSECONDS / MICROSECONDS)
                })?
            };
            Ok(Arc::new(result))
        }

        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Second)) => {
            cast_time64_to_time32::<Time64NanosecondType, Time32SecondType>(
                array,
                NANOSECONDS,
                cast_options,
            )
        }
        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Millisecond)) => {
            cast_time64_to_time32::<Time64NanosecondType, Time32MillisecondType>(
                array,
                NANOSECONDS / MILLISECONDS,
                cast_options,
            )
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, Time64MicrosecondType>(|x| x / (NANOSECONDS / MICROSECONDS)),
        )),

        // Timestamp to integer/floating/decimals
        (Timestamp(TimeUnit::Second, _), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<TimestampSecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Millisecond, _), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<TimestampMillisecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Microsecond, _), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<TimestampMicrosecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Nanosecond, _), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<TimestampNanosecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }

        (_, Timestamp(unit, tz)) if from_type.is_numeric() => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            Ok(make_timestamp_array(
                array.as_primitive(),
                *unit,
                tz.clone(),
            ))
        }

        (Timestamp(from_unit, from_tz), Timestamp(to_unit, to_tz)) => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            let time_array = array.as_primitive::<Int64Type>();
            let from_size = time_unit_multiple(from_unit);
            let to_size = time_unit_multiple(to_unit);
            // we either divide or multiply, depending on size of each unit
            // units are never the same when the types are the same
            let converted = match from_size.cmp(&to_size) {
                Ordering::Greater => {
                    let divisor = from_size / to_size;
                    time_array.unary::<_, Int64Type>(|o| o / divisor)
                }
                Ordering::Equal => time_array.clone(),
                Ordering::Less => {
                    let mul = to_size / from_size;
                    if cast_options.safe {
                        time_array.unary_opt::<_, Int64Type>(|o| o.checked_mul(mul))
                    } else {
                        time_array.try_unary::<_, Int64Type, _>(|o| o.mul_checked(mul))?
                    }
                }
            };
            // Normalize timezone
            let adjusted = match (from_tz, to_tz) {
                // Only this case needs to be adjusted because we're casting from
                // unknown time offset to some time offset, we want the time to be
                // unchanged.
                //
                // i.e. Timestamp('2001-01-01T00:00', None) -> Timestamp('2001-01-01T00:00', '+0700')
                //
                // For IANA timezones a wall clock reading is not always a unique
                // instant: a "fall back" DST transition makes an hour occur twice
                // and a "spring forward" transition skips an hour entirely. See
                // `resolve_local_offset` for how those readings are resolved.
                (None, Some(to_tz)) => {
                    let to_tz: Tz = to_tz.parse()?;
                    match to_unit {
                        TimeUnit::Second => adjust_timestamp_to_timezone::<TimestampSecondType>(
                            converted,
                            &to_tz,
                            cast_options,
                        )?,
                        TimeUnit::Millisecond => adjust_timestamp_to_timezone::<
                            TimestampMillisecondType,
                        >(
                            converted, &to_tz, cast_options
                        )?,
                        TimeUnit::Microsecond => adjust_timestamp_to_timezone::<
                            TimestampMicrosecondType,
                        >(
                            converted, &to_tz, cast_options
                        )?,
                        TimeUnit::Nanosecond => adjust_timestamp_to_timezone::<
                            TimestampNanosecondType,
                        >(
                            converted, &to_tz, cast_options
                        )?,
                    }
                }
                _ => converted,
            };
            Ok(make_timestamp_array(&adjusted, *to_unit, to_tz.clone()))
        }
        (Timestamp(TimeUnit::Microsecond, _), Date32) => timestamp_to_date32(
            array.as_primitive::<TimestampMicrosecondType>(),
            cast_options,
        ),
        (Timestamp(TimeUnit::Millisecond, _), Date32) => timestamp_to_date32(
            array.as_primitive::<TimestampMillisecondType>(),
            cast_options,
        ),
        (Timestamp(TimeUnit::Second, _), Date32) => {
            timestamp_to_date32(array.as_primitive::<TimestampSecondType>(), cast_options)
        }
        (Timestamp(TimeUnit::Nanosecond, _), Date32) => timestamp_to_date32(
            array.as_primitive::<TimestampNanosecondType>(),
            cast_options,
        ),
        (Timestamp(TimeUnit::Second, _), Date64) => Ok(Arc::new(match cast_options.safe {
            true => {
                // change error to None
                array
                    .as_primitive::<TimestampSecondType>()
                    .unary_opt::<_, Date64Type>(|x| x.checked_mul(MILLISECONDS))
            }
            false => array
                .as_primitive::<TimestampSecondType>()
                .try_unary::<_, Date64Type, _>(|x| x.mul_checked(MILLISECONDS))?,
        })),
        (Timestamp(TimeUnit::Millisecond, _), Date64) => {
            cast_reinterpret_arrays::<TimestampMillisecondType, Date64Type>(array)
        }
        (Timestamp(TimeUnit::Microsecond, _), Date64) => Ok(Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .unary::<_, Date64Type>(|x| x / (MICROSECONDS / MILLISECONDS)),
        )),
        (Timestamp(TimeUnit::Nanosecond, _), Date64) => Ok(Arc::new(
            array
                .as_primitive::<TimestampNanosecondType>()
                .unary::<_, Date64Type>(|x| x / (NANOSECONDS / MILLISECONDS)),
        )),
        (Timestamp(TimeUnit::Second, _), Time32(_) | Time64(_)) => {
            cast_timestamp_to_time::<TimestampSecondType>(array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Millisecond, _), Time32(_) | Time64(_)) => {
            cast_timestamp_to_time::<TimestampMillisecondType>(array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Microsecond, _), Time32(_) | Time64(_)) => {
            cast_timestamp_to_time::<TimestampMicrosecondType>(array, to_type, cast_options)
        }
        (Timestamp(TimeUnit::Nanosecond, _), Time32(_) | Time64(_)) => {
            cast_timestamp_to_time::<TimestampNanosecondType>(array, to_type, cast_options)
        }
        (Date64, Timestamp(TimeUnit::Second, _)) => {
            let array = array
                .as_primitive::<Date64Type>()
                .unary::<_, TimestampSecondType>(|x| x / MILLISECONDS);

            cast_with_options(&array, to_type, cast_options)
        }
        (Date64, Timestamp(TimeUnit::Millisecond, _)) => {
            let array = array
                .as_primitive::<Date64Type>()
                .reinterpret_cast::<TimestampMillisecondType>();

            cast_with_options(&array, to_type, cast_options)
        }

        (Date64, Timestamp(TimeUnit::Microsecond, _)) => {
            let array = array
                .as_primitive::<Date64Type>()
                .reinterpret_cast::<TimestampMillisecondType>();

            cast_with_options(&array, to_type, cast_options)
        }
        (Date64, Timestamp(TimeUnit::Nanosecond, _)) => {
            let array = array
                .as_primitive::<Date64Type>()
                .reinterpret_cast::<TimestampMillisecondType>();

            cast_with_options(&array, to_type, cast_options)
        }
        (Date32, Timestamp(TimeUnit::Second, _)) => {
            let array = array
                .as_primitive::<Date32Type>()
                .unary::<_, TimestampSecondType>(|x| (x as i64) * SECONDS_IN_DAY);

            cast_with_options(&array, to_type, cast_options)
        }
        (Date32, Timestamp(TimeUnit::Millisecond, _)) => {
            let array = array
                .as_primitive::<Date32Type>()
                .unary::<_, TimestampMillisecondType>(|x| (x as i64) * MILLISECONDS_IN_DAY);

            cast_with_options(&array, to_type, cast_options)
        }
        (Date32, Timestamp(TimeUnit::Microsecond, _)) => {
            let date_array = array.as_primitive::<Date32Type>();
            let converted = if cast_options.safe {
                date_array.unary_opt::<_, TimestampMicrosecondType>(|x| {
                    (x as i64).checked_mul(MICROSECONDS_IN_DAY)
                })
            } else {
                date_array.try_unary::<_, TimestampMicrosecondType, _>(|x| {
                    (x as i64).mul_checked(MICROSECONDS_IN_DAY)
                })?
            };
            cast_with_options(&converted, to_type, cast_options)
        }
        (Date32, Timestamp(TimeUnit::Nanosecond, _)) => {
            let date_array = array.as_primitive::<Date32Type>();
            let converted = if cast_options.safe {
                date_array.unary_opt::<_, TimestampNanosecondType>(|x| {
                    (x as i64).checked_mul(NANOSECONDS_IN_DAY)
                })
            } else {
                date_array.try_unary::<_, TimestampNanosecondType, _>(|x| {
                    (x as i64).mul_checked(NANOSECONDS_IN_DAY)
                })?
            };
            cast_with_options(&converted, to_type, cast_options)
        }

        (_, Duration(unit)) if from_type.is_numeric() => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            Ok(make_duration_array(array.as_primitive(), *unit))
        }
        (Duration(TimeUnit::Second), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<DurationSecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Duration(TimeUnit::Millisecond), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<DurationMillisecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Duration(TimeUnit::Microsecond), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<DurationMicrosecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }
        (Duration(TimeUnit::Nanosecond), _) if to_type.is_numeric() => {
            let array = cast_reinterpret_arrays::<DurationNanosecondType, Int64Type>(array)?;
            cast_with_options(&array, to_type, cast_options)
        }

        (Duration(from_unit), Duration(to_unit)) => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            let time_array = array.as_primitive::<Int64Type>();
            let from_size = time_unit_multiple(from_unit);
            let to_size = time_unit_multiple(to_unit);
            // we either divide or multiply, depending on size of each unit
            // units are never the same when the types are the same
            let converted = match from_size.cmp(&to_size) {
                Ordering::Greater => {
                    let divisor = from_size / to_size;
                    time_array.unary::<_, Int64Type>(|o| o / divisor)
                }
                Ordering::Equal => time_array.clone(),
                Ordering::Less => {
                    let mul = to_size / from_size;
                    if cast_options.safe {
                        time_array.unary_opt::<_, Int64Type>(|o| o.checked_mul(mul))
                    } else {
                        time_array.try_unary::<_, Int64Type, _>(|o| o.mul_checked(mul))?
                    }
                }
            };
            Ok(make_duration_array(&converted, *to_unit))
        }

        (Duration(TimeUnit::Second), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationSecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Millisecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationMillisecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Microsecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationMicrosecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Nanosecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationNanosecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::MonthDayNano), Duration(TimeUnit::Second)) => {
            cast_month_day_nano_to_duration::<DurationSecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::MonthDayNano), Duration(TimeUnit::Millisecond)) => {
            cast_month_day_nano_to_duration::<DurationMillisecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::MonthDayNano), Duration(TimeUnit::Microsecond)) => {
            cast_month_day_nano_to_duration::<DurationMicrosecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::MonthDayNano), Duration(TimeUnit::Nanosecond)) => {
            cast_month_day_nano_to_duration::<DurationNanosecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::YearMonth), Interval(IntervalUnit::MonthDayNano)) => {
            cast_interval_year_month_to_interval_month_day_nano(array, cast_options)
        }
        (Interval(IntervalUnit::DayTime), Interval(IntervalUnit::MonthDayNano)) => {
            cast_interval_day_time_to_interval_month_day_nano(array, cast_options)
        }
        (Int32, Interval(IntervalUnit::YearMonth)) => {
            cast_reinterpret_arrays::<Int32Type, IntervalYearMonthType>(array)
        }
        (_, _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported",
        ))),
    }
}

fn cast_from_decimal<D, F>(
    array: &dyn Array,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    as_float: F,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ToPrimitive,
    F: Fn(D::Native) -> f64,
{
    use DataType::*;
    // cast decimal to other type
    match to_type {
        UInt8 => cast_decimal_to_integer::<D, UInt8Type>(array, *scale, cast_options),
        UInt16 => cast_decimal_to_integer::<D, UInt16Type>(array, *scale, cast_options),
        UInt32 => cast_decimal_to_integer::<D, UInt32Type>(array, *scale, cast_options),
        UInt64 => cast_decimal_to_integer::<D, UInt64Type>(array, *scale, cast_options),
        Int8 => cast_decimal_to_integer::<D, Int8Type>(array, *scale, cast_options),
        Int16 => cast_decimal_to_integer::<D, Int16Type>(array, *scale, cast_options),
        Int32 => cast_decimal_to_integer::<D, Int32Type>(array, *scale, cast_options),
        Int64 => cast_decimal_to_integer::<D, Int64Type>(array, *scale, cast_options),
        Float16 => cast_decimal_to_float::<D, Float16Type, _>(array, |x| {
            half::f16::from_f64(single_decimal_to_float_lossy::<D, F>(
                &as_float,
                x,
                <i32 as From<i8>>::from(*scale),
            ))
        }),
        Float32 => cast_decimal_to_f32::<D, F>(array, &as_float, *scale),
        Float64 => cast_decimal_to_f64::<D, F>(array, &as_float, *scale),
        Utf8View => value_to_string_view(array, cast_options),
        Utf8 => value_to_string::<i32>(array, cast_options),
        LargeUtf8 => value_to_string::<i64>(array, cast_options),
        Null => Ok(new_null_array(to_type, array.len())),
        _ => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
    }
}

fn cast_to_decimal<D, M>(
    array: &dyn Array,
    precision: &u8,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M: ArrowNativeTypeOp + DecimalCast,
{
    use DataType::*;
    // cast data to decimal
    match from_type {
        UInt8 => cast_integer_to_decimal::<_, D, M>(
            array.as_primitive::<UInt8Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        UInt64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int8 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int8Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Int64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float16 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float16Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float32 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float32Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Float64 => cast_floating_point_to_decimal::<_, D>(
            array.as_primitive::<Float64Type>(),
            *precision,
            *scale,
            cast_options,
        ),
        Utf8View | Utf8 => {
            cast_string_to_decimal::<D, i32>(array, *precision, *scale, cast_options)
        }
        LargeUtf8 => cast_string_to_decimal::<D, i64>(array, *precision, *scale, cast_options),
        Null => Ok(new_null_array(to_type, array.len())),
        _ => Err(ArrowError::CastError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
    }
}

/// Number of `T` units in one day
const fn units_per_day<T: ArrowTimestampType>() -> i64 {
    SECONDS_IN_DAY * time_unit_multiple(&T::UNIT)
}

/// Get the time unit as a multiple of a second
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

fn cast_time64_to_time32<FROM, TO>(
    array: &dyn Array,
    divisor: i64,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType<Native = i64>,
    TO: ArrowPrimitiveType<Native = i32>,
{
    let array = array.as_primitive::<FROM>();
    let result = if cast_options.safe {
        array.unary_opt::<_, TO>(|value| i32::try_from(value / divisor).ok())
    } else {
        array.try_unary::<_, TO, _>(|value| {
            let value = value / divisor;
            i32::try_from(value).map_err(|_| {
                ArrowError::CastError(format!(
                    "Can't cast value {value:?} to type {}",
                    TO::DATA_TYPE
                ))
            })
        })?
    };

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::numeric::{NumericNative, is_infallible_numeric_cast};
    use DataType::*;
    use arrow_array::{Int64Array, RunArray, StringArray};
    use arrow_buffer::{BooleanBuffer, IntervalDayTime, NullBuffer, i256};
    use arrow_schema::{DataType, Field};
    use chrono::{FixedOffset, NaiveDate, NaiveTime};
    use half::f16;
    use std::sync::Arc;

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

    #[test]
    fn test_cast_i32_to_f64() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_primitive::<Float64Type>();
        assert_eq!(5.0, c.value(0));
        assert_eq!(6.0, c.value(1));
        assert_eq!(7.0, c.value(2));
        assert_eq!(8.0, c.value(3));
        assert_eq!(9.0, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_u8() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_primitive::<UInt8Type>();
        assert!(!c.is_valid(0));
        assert_eq!(6, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8, c.value(3));
        // overflows return None
        assert!(!c.is_valid(4));
    }

    #[test]
    #[should_panic(expected = "Can't cast value -5 to type UInt8")]
    fn test_cast_int32_to_u8_with_error() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        // overflow with the error
        let cast_option = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let result = cast_with_options(&array, &DataType::UInt8, &cast_option);
        assert!(result.is_err());
        result.unwrap();
    }

    #[test]
    fn test_cast_i32_to_u8_sliced() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        assert_eq!(0, array.offset());
        let array = array.slice(2, 3);
        let b = cast(&array, &DataType::UInt8).unwrap();
        assert_eq!(3, b.len());
        let c = b.as_primitive::<UInt8Type>();
        assert!(!c.is_valid(0));
        assert_eq!(8, c.value(1));
        // overflows return None
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_i32_to_i32() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_int_to_utf8view() {
        let inputs = vec![
            Arc::new(Int8Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(Int16Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(UInt16Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![None, Some(8), Some(9), Some(10)])) as ArrayRef,
        ];
        let expected: ArrayRef = Arc::new(StringViewArray::from(vec![
            None,
            Some("8"),
            Some("9"),
            Some("10"),
        ]));

        for array in inputs {
            assert!(can_cast_types(array.data_type(), &DataType::Utf8View));
            let arr = cast(&array, &DataType::Utf8View).unwrap();
            assert_eq!(expected.as_ref(), arr.as_ref());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_float_to_utf8view() {
        let inputs = vec![
            Arc::new(Float16Array::from(vec![
                Some(f16::from_f64(1.5)),
                Some(f16::from_f64(2.5)),
                None,
            ])) as ArrayRef,
            Arc::new(Float32Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
        ];

        let expected: ArrayRef =
            Arc::new(StringViewArray::from(vec![Some("1.5"), Some("2.5"), None]));

        for array in inputs {
            assert!(can_cast_types(array.data_type(), &DataType::Utf8View));
            let arr = cast(&array, &DataType::Utf8View).unwrap();
            assert_eq!(expected.as_ref(), arr.as_ref());
        }
    }

    #[test]
    fn test_cast_utf8_to_i32() {
        let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8, c.value(3));
        assert!(!c.is_valid(4));
    }

    #[test]
    fn test_cast_utf8view_to_i32() {
        let array = StringViewArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8, c.value(3));
        assert!(!c.is_valid(4));
    }

    #[test]
    fn test_cast_utf8view_to_f32() {
        let array = StringViewArray::from(vec!["3", "4.56", "seven", "8.9"]);
        let b = cast(&array, &DataType::Float32).unwrap();
        let c = b.as_primitive::<Float32Type>();
        assert_eq!(3.0, c.value(0));
        assert_eq!(4.56, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8.9, c.value(3));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_string_to_f16() {
        let arrays = [
            Arc::new(StringViewArray::from(vec!["3", "4.56", "seven", "8.9"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["3", "4.56", "seven", "8.9"])),
            Arc::new(LargeStringArray::from(vec!["3", "4.56", "seven", "8.9"])),
        ];
        for array in arrays {
            let b = cast(&array, &DataType::Float16).unwrap();
            let c = b.as_primitive::<Float16Type>();
            assert_eq!(half::f16::from_f32(3.0), c.value(0));
            assert_eq!(half::f16::from_f32(4.56), c.value(1));
            assert!(!c.is_valid(2));
            assert_eq!(half::f16::from_f32(8.9), c.value(3));
        }
    }

    #[test]
    fn test_cast_with_options_utf8_to_i32() {
        let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let result = cast_with_options(
            &array,
            &DataType::Int32,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("Cast error: Cannot cast string 'seven' to value of Int32 type",),
                    "Error: {e}"
                )
            }
        }
    }

    #[test]
    fn test_cast_utf8_to_bool() {
        let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
        let casted = cast(&strings, &DataType::Boolean).unwrap();
        let expected = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None]);
        assert_eq!(*as_boolean_array(&casted), expected);
    }

    #[test]
    fn test_cast_utf8view_to_bool() {
        let strings = StringViewArray::from(vec!["true", "false", "invalid", " Y ", ""]);
        let casted = cast(&strings, &DataType::Boolean).unwrap();
        let expected = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None]);
        assert_eq!(*as_boolean_array(&casted), expected);
    }

    #[test]
    fn test_cast_with_options_utf8_to_bool() {
        let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
        let casted = cast_with_options(
            &strings,
            &DataType::Boolean,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        match casted {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(
                    e.to_string().contains(
                        "Cast error: Cannot cast value 'invalid' to value of Boolean type"
                    )
                )
            }
        }
    }

    #[test]
    fn test_cast_bool_to_i32() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(1, c.value(0));
        assert_eq!(0, c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_numeric_sliced() {
        for nulls in [
            None,
            Some(NullBuffer::from(
                [false, true, true, false, true, true].repeat(12),
            )),
            Some(NullBuffer::new_null(72)),
        ] {
            // Include true values under nulls and slices crossing a bitmap word boundary.
            let booleans = BooleanArray::new(
                BooleanBuffer::from([true, false, true, false, true, false].repeat(12)),
                nulls.clone(),
            );
            let numbers = Int32Array::new([1, 0, 1, 0, 1, 0].repeat(12).into(), nulls);
            for data_type in [
                Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64,
            ] {
                let expected = cast(&numbers, &data_type).unwrap();
                for offset in [0, 3, 63, 72] {
                    let input = booleans.slice(offset, 72 - offset);
                    let expected = expected.slice(offset, 72 - offset);
                    let actual = cast(&input, &data_type).unwrap();
                    assert_eq!(actual.as_ref(), expected.as_ref());

                    let actual = cast(expected.as_ref(), &Boolean).unwrap();
                    assert_eq!(actual.as_ref(), &input);
                }
            }
        }
    }

    #[test]
    fn test_cast_float_to_bool_special_values() {
        let numbers = Float64Array::from(vec![
            0.0,
            -0.0,
            1.5,
            -1.5,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]);
        let expected = BooleanArray::from(vec![false, false, true, true, true, true, true]);
        for data_type in [Float16, Float32, Float64] {
            let input = cast(&numbers, &data_type).unwrap();
            let actual = cast(input.as_ref(), &Boolean).unwrap();
            assert_eq!(actual.as_ref(), &expected);
        }
    }

    #[test]
    fn test_cast_bool_to_string() {
        for nulls in [
            None,
            Some(NullBuffer::from([false, false, true, true].repeat(18))),
            Some(NullBuffer::new_null(72)),
        ] {
            // Include both Boolean values under nulls, an unaligned slice, and an empty slice.
            let array = BooleanArray::new(BooleanBuffer::from([true, false].repeat(36)), nulls);
            let expected: Vec<_> = ["true", "false"]
                .repeat(36)
                .into_iter()
                .enumerate()
                .map(|(i, value)| array.is_valid(i).then_some(value))
                .collect();
            for (offset, len) in [(0, 72), (3, 65), (72, 0)] {
                let input = array.slice(offset, len);
                for data_type in [Utf8, LargeUtf8, Utf8View] {
                    let actual = cast(&input, &data_type).unwrap();
                    let values: Vec<_> = match data_type {
                        Utf8 => actual.as_string::<i32>().iter().collect(),
                        LargeUtf8 => actual.as_string::<i64>().iter().collect(),
                        Utf8View => actual.as_string_view().iter().collect(),
                        _ => unreachable!(),
                    };
                    assert_eq!(values, expected[offset..offset + len]);
                }
            }
        }
    }

    #[test]
    fn test_cast_bool_to_f64() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_primitive::<Float64Type>();
        assert_eq!(1.0, c.value(0));
        assert_eq!(0.0, c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_integer_to_timestamp() {
        let array = Int64Array::from(vec![Some(2), Some(10), None]);
        let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        let array = Int8Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = Int16Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = Int32Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = UInt8Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = UInt16Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = UInt32Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = UInt64Array::from(vec![Some(2), Some(10), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);
    }

    #[test]
    fn test_cast_timestamp_to_integer() {
        let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
            .with_timezone("UTC".to_string());
        let expected = cast(&array, &DataType::Int64).unwrap();

        let actual = cast(&cast(&array, &DataType::Int8).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::Int16).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::Int32).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::UInt8).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::UInt16).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::UInt32).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::UInt64).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_floating_to_timestamp() {
        let array = Int64Array::from(vec![Some(2), Some(10), None]);
        let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        let array = Float16Array::from(vec![
            Some(f16::from_f32(2.0)),
            Some(f16::from_f32(10.6)),
            None,
        ]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = Float32Array::from(vec![Some(2.0), Some(10.6), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = Float64Array::from(vec![Some(2.1), Some(10.2), None]);
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_timestamp_to_floating() {
        let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
            .with_timezone("UTC".to_string());
        let expected = cast(&array, &DataType::Int64).unwrap();

        let actual = cast(&cast(&array, &DataType::Float16).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::Float32).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(&cast(&array, &DataType::Float64).unwrap(), &DataType::Int64).unwrap();
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn test_cast_decimal_to_timestamp() {
        let array = Int64Array::from(vec![Some(2), Some(10), None]);
        let expected = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        let array = Decimal128Array::from(vec![Some(200), Some(1000), None])
            .with_precision_and_scale(4, 2)
            .unwrap();
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);

        let array = Decimal256Array::from(vec![
            Some(i256::from_i128(2000)),
            Some(i256::from_i128(10000)),
            None,
        ])
        .with_precision_and_scale(5, 3)
        .unwrap();
        let actual = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();

        assert_eq!(&actual, &expected);
    }

    #[test]
    fn test_cast_timestamp_to_decimal() {
        let array = TimestampMillisecondArray::from(vec![Some(5), Some(1), None])
            .with_timezone("UTC".to_string());
        let expected = cast(&array, &DataType::Int64).unwrap();

        let actual = cast(
            &cast(&array, &DataType::Decimal128(5, 2)).unwrap(),
            &DataType::Int64,
        )
        .unwrap();
        assert_eq!(&actual, &expected);

        let actual = cast(
            &cast(&array, &DataType::Decimal256(10, 5)).unwrap(),
            &DataType::Int64,
        )
        .unwrap();
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn test_cast_date32_to_date64() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_primitive::<Date64Type>();
        assert_eq!(864000000000, c.value(0));
        assert_eq!(1545696000000, c.value(1));
    }

    #[test]
    fn test_cast_date64_to_date32() {
        let a = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_date32_overflow() {
        let a = Date64Array::from(vec![i64::MAX]);
        let array = Arc::new(a) as ArrayRef;

        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert!(c.is_null(0));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let err = cast_with_options(&array, &DataType::Date32, &options).unwrap_err();
        assert!(
            err.to_string().contains("Cannot cast Date64 value"),
            "{err}"
        );
    }

    #[test]
    fn test_cast_string_to_integral_overflow() {
        let str = Arc::new(StringArray::from(vec![
            Some("123"),
            Some("-123"),
            Some("86374"),
            Some("-40000"),
            None,
        ])) as ArrayRef;

        let options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };
        let res = cast_with_options(&str, &DataType::Int16, &options).expect("should cast to i16");
        let expected = Arc::new(Int16Array::from(vec![
            Some(123),
            Some(-123),
            None,
            None,
            None,
        ])) as ArrayRef;
        assert_eq!(&res, &expected);
    }

    #[test]
    fn test_cast_string_to_timestamp() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("2020-09-08T12:00:00.123456789+00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("2020-09-08T12:00:00.123456789+00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2020-09-08T12:00:00.123456789+00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            for time_unit in &[
                TimeUnit::Second,
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
                TimeUnit::Nanosecond,
            ] {
                let to_type = DataType::Timestamp(*time_unit, None);
                let b = cast(array, &to_type).unwrap();

                match time_unit {
                    TimeUnit::Second => {
                        let c = b.as_primitive::<TimestampSecondType>();
                        assert_eq!(1599566400, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Millisecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Microsecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123456, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Nanosecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123456789, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                }

                let options = CastOptions {
                    safe: false,
                    format_options: FormatOptions::default(),
                };
                let err = cast_with_options(array, &to_type, &options).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Parser error: Error parsing timestamp from 'Not a valid date': error parsing date"
                );
            }
        }
    }

    #[test]
    fn test_cast_string_to_timestamp_overflow() {
        let array = StringArray::from(vec!["9800-09-08T12:00:00.123456789"]);
        let result = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();
        assert_eq!(result.values(), &[247112596800]);
    }

    #[test]
    fn test_cast_string_to_date32_with_surrounding_whitespace() {
        let array = StringArray::from(vec![" 2026-09-29 ", "2026-09-29"]);

        let result = cast(&array, &DataType::Date32).unwrap();
        let result = result.as_primitive::<Date32Type>();

        assert!(!result.is_null(0));
        assert_eq!(result.value(0), result.value(1));
    }

    #[test]
    fn test_cast_string_to_date32() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("2018-12-25"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("2018-12-25"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2018-12-25"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Date32;
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Date32Type>();
            assert_eq!(17890, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string 'Not a valid date' to value of Date32 type"
            );
        }
    }

    #[test]
    fn test_cast_string_with_large_date_to_date32() {
        let array = Arc::new(StringArray::from(vec![
            Some("+10999-12-31"),
            Some("-0010-02-28"),
            Some("0010-02-28"),
            Some("0000-01-01"),
            Some("-0000-01-01"),
            Some("-0001-01-01"),
        ])) as ArrayRef;
        let to_type = DataType::Date32;
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let b = cast_with_options(&array, &to_type, &options).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert_eq!(3298139, c.value(0)); // 10999-12-31
        assert_eq!(-723122, c.value(1)); // -0010-02-28
        assert_eq!(-715817, c.value(2)); // 0010-02-28
        assert_eq!(c.value(3), c.value(4)); // Expect 0000-01-01 and -0000-01-01 to be parsed the same
        assert_eq!(-719528, c.value(3)); // 0000-01-01
        assert_eq!(-719528, c.value(4)); // -0000-01-01
        assert_eq!(-719893, c.value(5)); // -0001-01-01
    }

    #[test]
    fn test_cast_invalid_string_with_large_date_to_date32() {
        // Large dates need to be prefixed with a + or - sign, otherwise they are not parsed correctly
        let array = Arc::new(StringArray::from(vec![Some("10999-12-31")])) as ArrayRef;
        let to_type = DataType::Date32;
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let err = cast_with_options(&array, &to_type, &options).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cast error: Cannot cast string '10999-12-31' to value of Date32 type"
        );
    }

    #[test]
    fn test_cast_string_format_yyyymmdd_to_date32() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("2020-12-25"),
            Some("20201117"),
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("2020-12-25"),
            Some("20201117"),
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2020-12-25"),
            Some("20201117"),
        ])) as ArrayRef;

        for array in &[a0, a1, a2] {
            let to_type = DataType::Date32;
            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let result = cast_with_options(&array, &to_type, &options).unwrap();
            let c = result.as_primitive::<Date32Type>();
            assert_eq!(
                chrono::NaiveDate::from_ymd_opt(2020, 12, 25),
                c.value_as_date(0)
            );
            assert_eq!(
                chrono::NaiveDate::from_ymd_opt(2020, 11, 17),
                c.value_as_date(1)
            );
        }
    }

    #[test]
    fn test_cast_string_to_time32second() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Time32(TimeUnit::Second);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Time32SecondType>();
            assert_eq!(29315, c.value(0));
            assert_eq!(29340, c.value(1));
            assert!(c.is_null(2));
            assert!(c.is_null(3));
            assert!(c.is_null(4));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(s) type"
            );
        }
    }

    #[test]
    fn test_cast_string_to_time32millisecond() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Time32(TimeUnit::Millisecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Time32MillisecondType>();
            assert_eq!(29315091, c.value(0));
            assert_eq!(29340091, c.value(1));
            assert!(c.is_null(2));
            assert!(c.is_null(3));
            assert!(c.is_null(4));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(ms) type"
            );
        }
    }

    #[test]
    fn test_cast_string_to_time64microsecond() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Time64(TimeUnit::Microsecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Time64MicrosecondType>();
            assert_eq!(29315091323, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string 'Not a valid time' to value of Time64(µs) type"
            );
        }
    }

    #[test]
    fn test_cast_string_to_time64nanosecond() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Time64(TimeUnit::Nanosecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Time64NanosecondType>();
            assert_eq!(29315091323414, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string 'Not a valid time' to value of Time64(ns) type"
            );
        }
    }

    #[test]
    fn test_cast_string_to_date64() {
        let a0 = Arc::new(StringViewArray::from(vec![
            Some("2020-09-08T12:00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a1 = Arc::new(StringArray::from(vec![
            Some("2020-09-08T12:00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2020-09-08T12:00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a0, a1, a2] {
            let to_type = DataType::Date64;
            let b = cast(array, &to_type).unwrap();
            let c = b.as_primitive::<Date64Type>();
            assert_eq!(1599566400000, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Cast error: Cannot cast string 'Not a valid date' to value of Date64 type"
            );
        }
    }

    macro_rules! test_safe_string_to_interval {
        ($data_vec:expr, $interval_unit:expr, $array_ty:ty, $expect_vec:expr) => {
            let source_string_array = Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;

            let options = CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            };

            let target_interval_array = cast_with_options(
                &source_string_array.clone(),
                &DataType::Interval($interval_unit),
                &options,
            )
            .unwrap()
            .as_any()
            .downcast_ref::<$array_ty>()
            .unwrap()
            .clone() as $array_ty;

            let target_string_array =
                cast_with_options(&target_interval_array, &DataType::Utf8, &options)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();

            let expect_string_array = StringArray::from($expect_vec);

            assert_eq!(target_string_array, expect_string_array);

            let target_large_string_array =
                cast_with_options(&target_interval_array, &DataType::LargeUtf8, &options)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .clone();

            let expect_large_string_array = LargeStringArray::from($expect_vec);

            assert_eq!(target_large_string_array, expect_large_string_array);
        };
    }

    #[test]
    fn test_cast_string_to_interval_year_month() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month"),
                Some("1.5 years 13 month"),
                Some("30 days"),
                Some("31 days"),
                Some("2 months 31 days"),
                Some("2 months 31 days 1 second"),
                Some("foobar"),
            ],
            IntervalUnit::YearMonth,
            IntervalYearMonthArray,
            vec![
                Some("1 years 1 mons"),
                Some("2 years 7 mons"),
                None,
                None,
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_cast_string_to_interval_day_time() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month"),
                Some("1.5 years 13 month"),
                Some("30 days"),
                Some("1 day 2 second 3.5 milliseconds"),
                Some("foobar"),
            ],
            IntervalUnit::DayTime,
            IntervalDayTimeArray,
            vec![
                Some("390 days"),
                Some("930 days"),
                Some("30 days"),
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_cast_string_to_interval_month_day_nano() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month 1 day"),
                None,
                Some("1.5 years 13 month 35 days 1.4 milliseconds"),
                Some("3 days"),
                Some("8 seconds"),
                None,
                Some("1 day 29800 milliseconds"),
                Some("3 months 1 second"),
                Some("6 minutes 120 second"),
                Some("2 years 39 months 9 days 19 hours 1 minute 83 seconds 399222 milliseconds"),
                Some("foobar"),
            ],
            IntervalUnit::MonthDayNano,
            IntervalMonthDayNanoArray,
            vec![
                Some("13 mons 1 days"),
                None,
                Some("31 mons 35 days 0.001400000 secs"),
                Some("3 days"),
                Some("8.000000000 secs"),
                None,
                Some("1 days 29.800000000 secs"),
                Some("3 mons 1.000000000 secs"),
                Some("8 mins"),
                Some("63 mons 9 days 19 hours 9 mins 2.222000000 secs"),
                None,
            ]
        );
    }

    macro_rules! test_unsafe_string_to_interval_err {
        ($data_vec:expr, $interval_unit:expr, $error_msg:expr) => {
            let string_array = Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;
            let options = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let arrow_err = cast_with_options(
                &string_array.clone(),
                &DataType::Interval($interval_unit),
                &options,
            )
            .unwrap_err();
            assert_eq!($error_msg, arrow_err.to_string());
        };
    }

    #[test]
    fn test_cast_string_to_interval_err() {
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::YearMonth,
            r#"Parser error: Invalid input syntax for type interval: "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::DayTime,
            r#"Parser error: Invalid input syntax for type interval: "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::MonthDayNano,
            r#"Parser error: Invalid input syntax for type interval: "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("2 months 31 days 1 second")],
            IntervalUnit::YearMonth,
            "Cast error: Cannot cast 2 months 31 days 1 second to IntervalYearMonth. Only year and month fields are allowed."
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("1 day 1.5 milliseconds")],
            IntervalUnit::DayTime,
            "Cast error: Cannot cast 1 day 1.5 milliseconds to IntervalDayTime because the nanos part isn't multiple of milliseconds"
        );

        // overflow
        test_unsafe_string_to_interval_err!(
            vec![Some(format!(
                "{} century {} year {} month",
                i64::MAX - 2,
                i64::MAX - 2,
                i64::MAX - 2
            ))],
            IntervalUnit::DayTime,
            format!(
                "Arithmetic overflow: Overflow happened on: {} * 100",
                i64::MAX - 2
            )
        );
        test_unsafe_string_to_interval_err!(
            vec![Some(format!(
                "{} year {} month {} day",
                i64::MAX - 2,
                i64::MAX - 2,
                i64::MAX - 2
            ))],
            IntervalUnit::MonthDayNano,
            format!(
                "Arithmetic overflow: Overflow happened on: {} * 12",
                i64::MAX - 2
            )
        );
    }

    #[test]
    fn test_cast_binary_to_fixed_size_binary() {
        let bytes_1 = b"Hiiii".as_slice();
        let bytes_2 = b"Hello".as_slice();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(BinaryArray::from(binary_data.clone())) as ArrayRef;
        let a2 = Arc::new(LargeBinaryArray::from(binary_data)) as ArrayRef;

        let array_ref = cast(&a1, &DataType::FixedSizeBinary(5)).unwrap();
        let down_cast = array_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        let array_ref = cast(&a2, &DataType::FixedSizeBinary(5)).unwrap();
        let down_cast = array_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        // test error cases when the length of binary are not same
        let bytes_1 = b"Hi".as_slice();
        let bytes_2 = b"Hello".as_slice();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(BinaryArray::from(binary_data.clone())) as ArrayRef;
        let a2 = Arc::new(LargeBinaryArray::from(binary_data)) as ArrayRef;

        let array_ref = cast_with_options(
            &a1,
            &DataType::FixedSizeBinary(5),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(array_ref.is_err());

        let array_ref = cast_with_options(
            &a2,
            &DataType::FixedSizeBinary(5),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(array_ref.is_err());
    }

    #[test]
    fn test_fixed_size_binary_to_binary() {
        let bytes_1 = b"Hiiii".as_slice();
        let bytes_2 = b"Hello".as_slice();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(FixedSizeBinaryArray::try_from(binary_data.clone()).unwrap()) as ArrayRef;

        let array_ref = cast(&a1, &DataType::Binary).unwrap();
        let down_cast = array_ref.as_binary::<i32>();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        let array_ref = cast(&a1, &DataType::LargeBinary).unwrap();
        let down_cast = array_ref.as_binary::<i64>();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        let array_ref = cast(&a1, &DataType::BinaryView).unwrap();
        let down_cast = array_ref.as_binary_view();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));
    }

    #[test]
    fn test_numeric_to_binary() {
        let a = Int16Array::from(vec![Some(1), Some(511), None]);

        let array_ref = cast(&a, &DataType::Binary).unwrap();
        let down_cast = array_ref.as_binary::<i32>();
        assert_eq!(&1_i16.to_le_bytes(), down_cast.value(0));
        assert_eq!(&511_i16.to_le_bytes(), down_cast.value(1));
        assert!(down_cast.is_null(2));

        let a = Int64Array::from(vec![Some(-1), Some(123456789), None]);

        let array_ref = cast(&a, &DataType::Binary).unwrap();
        let down_cast = array_ref.as_binary::<i32>();
        assert_eq!(&(-1_i64).to_le_bytes(), down_cast.value(0));
        assert_eq!(&123456789_i64.to_le_bytes(), down_cast.value(1));
        assert!(down_cast.is_null(2));
    }

    #[test]
    fn test_numeric_to_large_binary() {
        let a = Int16Array::from(vec![Some(1), Some(511), None]);

        let array_ref = cast(&a, &DataType::LargeBinary).unwrap();
        let down_cast = array_ref.as_binary::<i64>();
        assert_eq!(&1_i16.to_le_bytes(), down_cast.value(0));
        assert_eq!(&511_i16.to_le_bytes(), down_cast.value(1));
        assert!(down_cast.is_null(2));

        let a = Int64Array::from(vec![Some(-1), Some(123456789), None]);

        let array_ref = cast(&a, &DataType::LargeBinary).unwrap();
        let down_cast = array_ref.as_binary::<i64>();
        assert_eq!(&(-1_i64).to_le_bytes(), down_cast.value(0));
        assert_eq!(&123456789_i64.to_le_bytes(), down_cast.value(1));
        assert!(down_cast.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_int32() {
        let array = Date32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_int32_to_date32() {
        let array = Int32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    /// Compares arithmetic casts with Chrono for representable dates, including
    /// negative and fractional boundaries, nulls, and nonzero-offset slices.
    fn check_timestamp_casts<T: ArrowTimestampType>() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        let units = time_unit_multiple(&T::UNIT);
        let around = |x: i64| [x - 1, x, x + 1, -x - 1, -x, -x + 1];
        let mut values = vec![i64::MIN, i64::MAX];
        for base in [0, 60, 3_600, 86_400] {
            for seconds in around(base) {
                for fraction in [-1, 0, 1, units - 1] {
                    values.push(seconds * units + fraction);
                }
            }
        }
        for boundary in [1_000, 1_000_000] {
            values.extend(around(boundary));
        }
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..256 {
            values.push(rng.random());
            let seconds = rng.random_range(-2_000_000_000..2_000_000_000);
            values.push(seconds * units + rng.random_range(0..units));
        }
        values.retain(|&v| as_datetime::<T>(v).is_some());
        let no_nulls = PrimitiveArray::<T>::new(values.clone().into(), None);
        let mixed = PrimitiveArray::<T>::from_iter(
            values
                .iter()
                .enumerate()
                .map(|(i, &v)| (i % 5 != 0).then_some(v)),
        );
        for array in [no_nulls, mixed.slice(1, mixed.len() - 2)] {
            let dates: Vec<_> = array
                .iter()
                .map(|v| v.map(|v| as_datetime::<T>(v).unwrap()))
                .collect();
            let expected: [ArrayRef; 5] = [
                Arc::new(Date32Array::from_iter(
                    dates
                        .iter()
                        .map(|d| d.map(|d| Date32Type::from_naive_date(d.date()))),
                )),
                Arc::new(Time32SecondArray::from_iter(
                    dates.iter().map(|d| d.map(|d| time_to_time32s(d.time()))),
                )),
                Arc::new(Time32MillisecondArray::from_iter(
                    dates.iter().map(|d| d.map(|d| time_to_time32ms(d.time()))),
                )),
                Arc::new(Time64MicrosecondArray::from_iter(
                    dates.iter().map(|d| d.map(|d| time_to_time64us(d.time()))),
                )),
                Arc::new(Time64NanosecondArray::from_iter(
                    dates.iter().map(|d| d.map(|d| time_to_time64ns(d.time()))),
                )),
            ];
            for expected in expected {
                let actual = cast(&array, expected.data_type()).unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref(), "{}", T::DATA_TYPE);
            }
        }
    }

    #[test]
    fn test_cast_timestamp_without_timezone_matches_chrono() {
        check_timestamp_casts::<TimestampSecondType>();
        check_timestamp_casts::<TimestampMillisecondType>();
        check_timestamp_casts::<TimestampMicrosecondType>();
        check_timestamp_casts::<TimestampNanosecondType>();
    }

    #[test]
    fn test_cast_timestamp_to_time_extremes() {
        fn check<T: ArrowTimestampType>(min: (u32, u32, u32, u32), max: (u32, u32, u32, u32)) {
            let time = |(h, m, s, ns)| NaiveTime::from_hms_nano_opt(h, m, s, ns).unwrap();
            let times = [Some(time(min)), None, Some(time(max))];
            let array = PrimitiveArray::<T>::from_iter([Some(i64::MIN), None, Some(i64::MAX)]);
            let expected: [ArrayRef; 4] = [
                Arc::new(Time32SecondArray::from_iter(
                    times.map(|v| v.map(time_to_time32s)),
                )),
                Arc::new(Time32MillisecondArray::from_iter(
                    times.map(|v| v.map(time_to_time32ms)),
                )),
                Arc::new(Time64MicrosecondArray::from_iter(
                    times.map(|v| v.map(time_to_time64us)),
                )),
                Arc::new(Time64NanosecondArray::from_iter(
                    times.map(|v| v.map(time_to_time64ns)),
                )),
            ];
            for expected in expected {
                assert_eq!(
                    cast(&array, expected.data_type()).unwrap().as_ref(),
                    expected.as_ref()
                );
            }
        }
        check::<TimestampSecondType>((8, 29, 52, 0), (15, 30, 7, 0));
        check::<TimestampMillisecondType>((16, 47, 4, 192_000_000), (7, 12, 55, 807_000_000));
        check::<TimestampMicrosecondType>((19, 59, 5, 224_192_000), (4, 0, 54, 775_807_000));
        // Nanosecond extremes are representable in Chrono and covered above.
    }

    #[test]
    fn test_cast_timestamp_to_date32_range() {
        fn check<T: ArrowTimestampType>() {
            let day = SECONDS_IN_DAY * time_unit_multiple(&T::UNIT);
            let first = i32::MIN as i64 * day;
            let last = (i32::MAX as i64 + 1) * day - 1;
            let array = PrimitiveArray::<T>::new(
                vec![first, i64::MAX, last].into(),
                Some(NullBuffer::from(vec![true, false, true])),
            );
            let expected = Date32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)]);
            for safe in [true, false] {
                let options = CastOptions {
                    safe,
                    ..Default::default()
                };
                assert_eq!(
                    cast_with_options(&array, &Date32, &options)
                        .unwrap()
                        .as_ref(),
                    &expected
                );
            }
            // Day counts that do not fit in i32 become null when safe, otherwise an error.
            let array = PrimitiveArray::<T>::from_iter_values([
                first - 1,
                last,
                last + 1,
                i64::MIN,
                i64::MAX,
            ]);
            let expected = Date32Array::from(vec![None, Some(i32::MAX), None, None, None]);
            assert_eq!(cast(&array, &Date32).unwrap().as_ref(), &expected);
            let options = CastOptions {
                safe: false,
                ..Default::default()
            };
            assert!(cast_with_options(&array, &Date32, &options).is_err());
        }
        check::<TimestampSecondType>();
        check::<TimestampMillisecondType>();
        // The entire microsecond domain fits in Date32, including dates outside Chrono's range.
        let array = TimestampMicrosecondArray::from(vec![Some(i64::MIN), None, Some(i64::MAX)]);
        assert_eq!(
            cast(&array, &Date32).unwrap().as_ref(),
            &Date32Array::from(vec![Some(-106_751_992), None, Some(106_751_991)]),
        );
    }

    #[test]
    fn test_cast_timestamp_date_time_timezone_validation() {
        let invalid = TimestampSecondArray::from(vec![0]).with_timezone("invalid timezone");
        let extreme = TimestampSecondArray::from(vec![0, i64::MAX]).with_timezone("+00:00");
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        for target in [
            Date32,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
        ] {
            // A timezone that fails to parse is an error even when the cast is safe.
            assert!(cast(&invalid, &target).is_err());
            // A timestamp outside Chrono's date range is null when safe, otherwise an error.
            let actual = cast(&extreme, &target).unwrap();
            assert!(actual.is_valid(0));
            assert!(actual.is_null(1));
            assert!(cast_with_options(&extreme, &target, &options).is_err());
        }
    }

    #[test]
    fn test_cast_timestamp_to_date32() {
        let array =
            TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None])
                .with_timezone("+00:00".to_string());
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_primitive::<Date32Type>();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }
    #[test]
    fn test_cast_timestamp_to_date32_zone() {
        let strings = StringArray::from_iter([
            Some("1970-01-01T00:00:01"),
            Some("1970-01-01T23:59:59"),
            None,
            Some("2020-03-01T02:00:23+00:00"),
        ]);
        let dt = DataType::Timestamp(TimeUnit::Millisecond, Some("-07:00".into()));
        let timestamps = cast(&strings, &dt).unwrap();
        let dates = cast(timestamps.as_ref(), &DataType::Date32).unwrap();

        let c = dates.as_primitive::<Date32Type>();
        let expected = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(c.value_as_date(0).unwrap(), expected);
        assert_eq!(c.value_as_date(1).unwrap(), expected);
        assert!(c.is_null(2));
        let expected = NaiveDate::from_ymd_opt(2020, 2, 29).unwrap();
        assert_eq!(c.value_as_date(3).unwrap(), expected);
    }
    #[test]
    fn test_cast_timestamp_to_date64() {
        let array =
            TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_primitive::<Date64Type>();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));

        let array = TimestampSecondArray::from(vec![Some(864000000005), Some(1545696000001)]);
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_primitive::<Date64Type>();
        assert_eq!(864000000005000, c.value(0));
        assert_eq!(1545696000001000, c.value(1));

        // test overflow, safe cast
        let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
        let b = cast(&array, &DataType::Date64).unwrap();
        assert!(b.is_null(0));
        // test overflow, unsafe cast
        let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let b = cast_with_options(&array, &DataType::Date64, &options);
        assert!(b.is_err());
    }

    #[test]
    fn test_cast_timestamp_to_time64() {
        // test timestamp secs
        let array = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
            .with_timezone("+01:00".to_string());
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp milliseconds
        let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp microseconds
        let a = TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // Test fractional timestamps on both sides of the epoch.
        let a = TimestampNanosecondArray::from(vec![Some(86_405_123_456_789), Some(-1), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(3_605_123_456, c.value(0));
        assert_eq!(3_599_999_999, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(3_605_123_456_789, c.value(0));
        assert_eq!(3_599_999_999_999, c.value(1));
        assert!(c.is_null(2));

        // test overflow, safe cast
        let a =
            TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        assert!(b.is_null(0));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        assert!(b.is_null(0));
        // test overflow, unsafe cast
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let b = cast_with_options(&array, &DataType::Time64(TimeUnit::Microsecond), &options);
        assert!(b.is_err());
        let b = cast_with_options(&array, &DataType::Time64(TimeUnit::Nanosecond), &options);
        assert!(b.is_err());
        // Time64 only supports microseconds and nanoseconds
        let b = cast(&array, &DataType::Time64(TimeUnit::Millisecond));
        assert!(b.is_err());
    }

    #[test]
    fn test_cast_timestamp_to_time32() {
        // test timestamp secs
        let a = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_primitive::<Time32SecondType>();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp milliseconds
        let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_primitive::<Time32SecondType>();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp microseconds
        let a = TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_primitive::<Time32SecondType>();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // Test fractional timestamps on both sides of the epoch.
        let a = TimestampNanosecondArray::from(vec![Some(86_405_123_456_789), Some(-1), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_primitive::<Time32SecondType>();
        assert_eq!(3605, c.value(0));
        assert_eq!(3599, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert_eq!(3_605_123, c.value(0));
        assert_eq!(3_599_999, c.value(1));
        assert!(c.is_null(2));

        // test overflow, safe cast
        let a =
            TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        assert!(b.is_null(0));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        assert!(b.is_null(0));
        // test overflow, unsafe cast
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let b = cast_with_options(&array, &DataType::Time32(TimeUnit::Second), &options);
        assert!(b.is_err());
        let b = cast_with_options(&array, &DataType::Time32(TimeUnit::Millisecond), &options);
        assert!(b.is_err());
    }

    // Cast Timestamp(_, None) -> Timestamp(_, Some(timezone))
    #[test]
    fn test_cast_timestamp_with_timezone_1() {
        let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("2000-01-01T00:00:00.123456789"),
            Some("2010-01-01T00:00:00.123456789"),
            None,
        ]));
        let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let timestamp_array = cast(&string_array, &to_type).unwrap();

        let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+0700".into()));
        let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

        let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2000-01-01T00:00:00.123456+07:00", result.value(0));
        assert_eq!("2010-01-01T00:00:00.123456+07:00", result.value(1));
        assert!(result.is_null(2));
    }

    // Cast Timestamp(_, Some(timezone)) -> Timestamp(_, None)
    #[test]
    fn test_cast_timestamp_with_timezone_2() {
        let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("2000-01-01T07:00:00.123456789"),
            Some("2010-01-01T07:00:00.123456789"),
            None,
        ]));
        let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some("+0700".into()));
        let timestamp_array = cast(&string_array, &to_type).unwrap();

        // Check intermediate representation is correct
        let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2000-01-01T07:00:00.123+07:00", result.value(0));
        assert_eq!("2010-01-01T07:00:00.123+07:00", result.value(1));
        assert!(result.is_null(2));

        let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

        let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2000-01-01T00:00:00.123", result.value(0));
        assert_eq!("2010-01-01T00:00:00.123", result.value(1));
        assert!(result.is_null(2));
    }

    // Cast Timestamp(_, Some(timezone)) -> Timestamp(_, Some(timezone))
    #[test]
    fn test_cast_timestamp_with_timezone_3() {
        let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("2000-01-01T07:00:00.123456789"),
            Some("2010-01-01T07:00:00.123456789"),
            None,
        ]));
        let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+0700".into()));
        let timestamp_array = cast(&string_array, &to_type).unwrap();

        // Check intermediate representation is correct
        let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2000-01-01T07:00:00.123456+07:00", result.value(0));
        assert_eq!("2010-01-01T07:00:00.123456+07:00", result.value(1));
        assert!(result.is_null(2));

        let to_type = DataType::Timestamp(TimeUnit::Second, Some("-08:00".into()));
        let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

        let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("1999-12-31T16:00:00-08:00", result.value(0));
        assert_eq!("2009-12-31T16:00:00-08:00", result.value(1));
        assert!(result.is_null(2));
    }

    /// The i64 value of a `Timestamp(Second, None)` holding the given wall clock
    /// reading (a naive timestamp is stored as if it were UTC).
    fn naive_seconds(y: i32, m: u32, d: u32, h: u32, min: u32) -> i64 {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    /// The instant, in seconds since the epoch, denoted by the given wall clock
    /// reading at a fixed offset of `offset_hours`.
    fn instant_seconds(y: i32, m: u32, d: u32, h: u32, min: u32, offset_hours: i32) -> i64 {
        let offset = FixedOffset::east_opt(offset_hours * 3600).unwrap();
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
            .and_local_timezone(offset)
            .unwrap()
            .timestamp()
    }

    // Cast Timestamp(_, None) -> Timestamp(_, Some(IANA timezone)) across DST
    // transitions. See `resolve_local_offset`.
    #[test]
    fn test_cast_timestamp_to_named_timezone_dst() {
        // Unambiguous, EDT (-04:00) is in effect.
        let unambiguous = naive_seconds(2024, 11, 1, 0, 0);
        // Ambiguous: the clocks go back at 2024-11-03T02:00 EDT, so 01:30
        // happens twice, first at -04:00 and then at -05:00.
        let ambiguous = naive_seconds(2024, 11, 3, 1, 30);
        // Nonexistent: the clocks go forward at 2024-03-10T02:00 EST, so 02:30
        // never happens.
        let nonexistent = naive_seconds(2024, 3, 10, 2, 30);
        assert_eq!(
            [unambiguous, ambiguous, nonexistent],
            [1_730_419_200, 1_730_597_400, 1_710_037_800]
        );

        let array = TimestampSecondArray::from(vec![
            Some(unambiguous),
            Some(ambiguous),
            Some(nonexistent),
            None,
        ]);
        let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampSecondType>();

        assert_eq!(c.value(0), instant_seconds(2024, 11, 1, 0, 0, -4));
        assert_eq!(c.value(0), 1_730_433_600);
        // The later of the two candidates, i.e. EST rather than EDT.
        assert_eq!(c.value(1), instant_seconds(2024, 11, 3, 1, 30, -5));
        assert_eq!(c.value(1), 1_730_615_400);
        // Shifted forward by the one hour gap: 02:30 EST is 03:30 EDT.
        assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 2, 30, -5));
        assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 3, 30, -4));
        assert_eq!(c.value(2), 1_710_055_800);
        assert!(c.is_null(3));
    }

    // The same values must not become null when `safe` casting is requested.
    #[test]
    fn test_cast_timestamp_to_named_timezone_dst_safe() {
        let array = TimestampSecondArray::from(vec![
            Some(naive_seconds(2024, 11, 1, 0, 0)),
            Some(naive_seconds(2024, 11, 3, 1, 30)),
            Some(naive_seconds(2024, 3, 10, 2, 30)),
            None,
        ]);
        let to_type = DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()));
        let options = CastOptions {
            safe: true,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(c.null_count(), 1);
        assert_eq!(c.value(0), 1_730_433_600);
        assert_eq!(c.value(1), 1_730_615_400);
        assert_eq!(c.value(2), 1_710_055_800);
        assert!(c.is_null(3));
    }

    // Southern hemisphere: the transitions run the other way around.
    #[test]
    fn test_cast_timestamp_to_named_timezone_dst_southern_hemisphere() {
        // Ambiguous: clocks go back at 2024-04-07T03:00 AEDT (+11:00 -> +10:00).
        let ambiguous = naive_seconds(2024, 4, 7, 2, 30);
        // Nonexistent: clocks go forward at 2024-10-06T02:00 AEST (+10:00 -> +11:00).
        let nonexistent = naive_seconds(2024, 10, 6, 2, 30);

        let array = TimestampSecondArray::from(vec![Some(ambiguous), Some(nonexistent)]);
        let to_type = DataType::Timestamp(TimeUnit::Second, Some("Australia/Sydney".into()));
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampSecondType>();

        // The later of the two candidates, i.e. AEST (+10:00).
        assert_eq!(c.value(0), instant_seconds(2024, 4, 7, 2, 30, 10));
        assert_eq!(c.value(0), 1_712_421_000);
        // Shifted forward by the one hour gap: 02:30 AEST is 03:30 AEDT.
        assert_eq!(c.value(1), instant_seconds(2024, 10, 6, 2, 30, 10));
        assert_eq!(c.value(1), instant_seconds(2024, 10, 6, 3, 30, 11));
        assert_eq!(c.value(1), 1_728_145_800);
    }

    // The resolution is independent of the time unit.
    #[test]
    fn test_cast_timestamp_to_named_timezone_dst_nanosecond() {
        let ambiguous = naive_seconds(2024, 11, 3, 1, 30) * 1_000_000_000 + 123_456_789;
        let nonexistent = naive_seconds(2024, 3, 10, 2, 30) * 1_000_000_000 + 123_456_789;
        let array = TimestampNanosecondArray::from(vec![Some(ambiguous), Some(nonexistent)]);
        let to_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()));
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampNanosecondType>();
        assert_eq!(c.value(0), 1_730_615_400 * 1_000_000_000 + 123_456_789);
        assert_eq!(c.value(1), 1_710_055_800 * 1_000_000_000 + 123_456_789);
    }

    // Unit conversion still composes with the timezone adjustment.
    #[test]
    fn test_cast_timestamp_to_named_timezone_dst_changing_unit() {
        let array = TimestampSecondArray::from(vec![
            Some(naive_seconds(2024, 11, 3, 1, 30)),
            Some(naive_seconds(2024, 3, 10, 2, 30)),
        ]);
        let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()));
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampMillisecondType>();
        assert_eq!(c.value(0), 1_730_615_400_000);
        assert_eq!(c.value(1), 1_710_055_800_000);
    }

    // A fixed offset has no transitions, so the same readings are unaffected.
    #[test]
    fn test_cast_timestamp_to_fixed_offset_timezone_unaffected() {
        let array = TimestampSecondArray::from(vec![
            Some(naive_seconds(2024, 11, 1, 0, 0)),
            Some(naive_seconds(2024, 11, 3, 1, 30)),
            Some(naive_seconds(2024, 3, 10, 2, 30)),
            None,
        ]);
        let to_type = DataType::Timestamp(TimeUnit::Second, Some("+08:00".into()));
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let b = cast_with_options(&array, &to_type, &options).unwrap();
        assert_eq!(b.data_type(), &to_type);
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(c.value(0), instant_seconds(2024, 11, 1, 0, 0, 8));
        assert_eq!(c.value(1), instant_seconds(2024, 11, 3, 1, 30, 8));
        assert_eq!(c.value(2), instant_seconds(2024, 3, 10, 2, 30, 8));
        assert!(c.is_null(3));
    }

    #[test]
    fn test_cast_date64_to_timestamp() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(864000000, c.value(0));
        assert_eq!(1545696000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_ms() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_us() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(864000000005000, c.value(0));
        assert_eq!(1545696000001000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_ns() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(864000000005000000, c.value(0));
        assert_eq!(1545696000001000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_i64() {
        let array =
            TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None])
                .with_timezone("UTC".to_string());
        let b = cast(&array, &DataType::Int64).unwrap();
        let c = b.as_primitive::<Int64Type>();
        assert_eq!(&DataType::Int64, c.data_type());
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    macro_rules! assert_cast {
        ($array:expr, $datatype:expr, $output_array_type: ty, $expected:expr) => {{
            assert!(can_cast_types($array.data_type(), &$datatype));
            let out = cast(&$array, &$datatype).unwrap();
            let actual = out
                .as_any()
                .downcast_ref::<$output_array_type>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, $expected);
        }};
        ($array:expr, $datatype:expr, $output_array_type: ty, $options:expr, $expected:expr) => {{
            assert!(can_cast_types($array.data_type(), &$datatype));
            let out = cast_with_options(&$array, &$datatype, &$options).unwrap();
            let actual = out
                .as_any()
                .downcast_ref::<$output_array_type>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, $expected);
        }};
    }

    #[test]
    fn test_cast_date32_to_string() {
        let array = Date32Array::from(vec![Some(0), Some(10000), Some(13036), Some(17890), None]);
        let expected = vec![
            Some("1970-01-01"),
            Some("1997-05-19"),
            Some("2005-09-10"),
            Some("2018-12-25"),
            None,
        ];

        assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
        assert_cast!(array, DataType::Utf8, StringArray, expected);
        assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
    }

    #[test]
    fn test_cast_date64_to_string() {
        let array = Date64Array::from(vec![
            Some(0),
            Some(10000 * 86400000),
            Some(13036 * 86400000),
            Some(17890 * 86400000),
            None,
        ]);
        let expected = vec![
            Some("1970-01-01T00:00:00"),
            Some("1997-05-19T00:00:00"),
            Some("2005-09-10T00:00:00"),
            Some("2018-12-25T00:00:00"),
            None,
        ];

        assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
        assert_cast!(array, DataType::Utf8, StringArray, expected);
        assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
    }

    #[test]
    fn test_cast_date32_to_timestamp_and_timestamp_with_timezone() {
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let a = Date32Array::from(vec![Some(18628), None, None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;

        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
        )
        .unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));

        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00", result.value(0));
    }

    #[test]
    fn test_cast_date32_to_timestamp_with_timezone() {
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
        )
        .unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(1609438500, c.value(0));
        assert_eq!(1640974500, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
        assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date32_to_timestamp_with_timezone_ms() {
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Millisecond, Some(tz.into())),
        )
        .unwrap();
        let c = b.as_primitive::<TimestampMillisecondType>();
        assert_eq!(1609438500000, c.value(0));
        assert_eq!(1640974500000, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
        assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date32_to_timestamp_with_timezone_us() {
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some(tz.into())),
        )
        .unwrap();
        let c = b.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(1609438500000000, c.value(0));
        assert_eq!(1640974500000000, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
        assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date32_to_timestamp_with_timezone_ns() {
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
        )
        .unwrap();
        let c = b.as_primitive::<TimestampNanosecondType>();
        assert_eq!(1609438500000000000, c.value(0));
        assert_eq!(1640974500000000000, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("2021-01-01T00:00:00+05:45", result.value(0));
        assert_eq!("2022-01-01T00:00:00+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date64_to_timestamp_with_timezone() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Second, Some(tz.into())),
        )
        .unwrap();

        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(863979300, c.value(0));
        assert_eq!(1545675300, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("1997-05-19T00:00:00+05:45", result.value(0));
        assert_eq!("2018-12-25T00:00:00+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date64_to_timestamp_with_timezone_ms() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Millisecond, Some(tz.into())),
        )
        .unwrap();

        let c = b.as_primitive::<TimestampMillisecondType>();
        assert_eq!(863979300005, c.value(0));
        assert_eq!(1545675300001, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
        assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date64_to_timestamp_with_timezone_us() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some(tz.into())),
        )
        .unwrap();

        let c = b.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(863979300005000, c.value(0));
        assert_eq!(1545675300001000, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
        assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
    }

    #[test]
    fn test_cast_date64_to_timestamp_with_timezone_ns() {
        let array = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
        )
        .unwrap();

        let c = b.as_primitive::<TimestampNanosecondType>();
        assert_eq!(863979300005000000, c.value(0));
        assert_eq!(1545675300001000000, c.value(1));
        assert!(c.is_null(2));

        let string_array = cast(&c, &DataType::Utf8).unwrap();
        let result = string_array.as_string::<i32>();
        assert_eq!("1997-05-19T00:00:00.005+05:45", result.value(0));
        assert_eq!("2018-12-25T00:00:00.001+05:45", result.value(1));
    }

    #[test]
    fn test_cast_timestamp_to_strings() {
        // "2018-12-25T00:00:02.001", "1997-05-19T00:00:03.005", None
        let array =
            TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
        let expected = vec![
            Some("1997-05-19T00:00:03.005"),
            Some("2018-12-25T00:00:02.001"),
            None,
        ];

        assert_cast!(array, DataType::Utf8View, StringViewArray, expected);
        assert_cast!(array, DataType::Utf8, StringArray, expected);
        assert_cast!(array, DataType::LargeUtf8, LargeStringArray, expected);
    }

    #[test]
    fn test_cast_timestamp_to_strings_opt() {
        let ts_format = "%Y-%m-%d %H:%M:%S%.6f";
        let tz = "+0545"; // UTC + 0545 is Asia/Kathmandu
        let cast_options = CastOptions {
            safe: true,
            format_options: FormatOptions::default()
                .with_timestamp_format(Some(ts_format))
                .with_timestamp_tz_format(Some(ts_format)),
        };

        // "2018-12-25T00:00:02.001", "1997-05-19T00:00:03.005", None
        let array_without_tz =
            TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
        let expected = vec![
            Some("1997-05-19 00:00:03.005000"),
            Some("2018-12-25 00:00:02.001000"),
            None,
        ];
        assert_cast!(
            array_without_tz,
            DataType::Utf8View,
            StringViewArray,
            cast_options,
            expected
        );
        assert_cast!(
            array_without_tz,
            DataType::Utf8,
            StringArray,
            cast_options,
            expected
        );
        assert_cast!(
            array_without_tz,
            DataType::LargeUtf8,
            LargeStringArray,
            cast_options,
            expected
        );

        let array_with_tz =
            TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None])
                .with_timezone(tz.to_string());
        let expected = vec![
            Some("1997-05-19 05:45:03.005000"),
            Some("2018-12-25 05:45:02.001000"),
            None,
        ];
        assert_cast!(
            array_with_tz,
            DataType::Utf8View,
            StringViewArray,
            cast_options,
            expected
        );
        assert_cast!(
            array_with_tz,
            DataType::Utf8,
            StringArray,
            cast_options,
            expected
        );
        assert_cast!(
            array_with_tz,
            DataType::LargeUtf8,
            LargeStringArray,
            cast_options,
            expected
        );
    }

    #[test]
    fn test_cast_between_timestamps() {
        let array =
            TimestampMillisecondArray::from(vec![Some(864000003005), Some(1545696002001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(864000003, c.value(0));
        assert_eq!(1545696002, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_duration_to_i64() {
        let base = vec![5, 6, 7, 8, 100000000];

        let duration_arrays = vec![
            Arc::new(DurationNanosecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationMicrosecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationMillisecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationSecondArray::from(base.clone())) as ArrayRef,
        ];

        for arr in duration_arrays {
            assert!(can_cast_types(arr.data_type(), &DataType::Int64));
            let result = cast(&arr, &DataType::Int64).unwrap();
            let result = result.as_primitive::<Int64Type>();
            assert_eq!(base.as_slice(), result.values());
        }
    }

    #[test]
    fn test_cast_between_durations_and_numerics() {
        fn test_cast_between_durations<FromType, ToType>()
        where
            FromType: ArrowPrimitiveType<Native = i64>,
            ToType: ArrowPrimitiveType<Native = i64>,
            PrimitiveArray<FromType>: From<Vec<Option<i64>>>,
        {
            let DataType::Duration(from_unit) = FromType::DATA_TYPE else {
                panic!("Expected a duration type")
            };
            let DataType::Duration(to_unit) = ToType::DATA_TYPE else {
                panic!("Expected a duration type")
            };
            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);

            let (v1_before, v2_before) = (8640003005, 1696002001);
            let (v1_after, v2_after) = if from_size >= to_size {
                (
                    v1_before / (from_size / to_size),
                    v2_before / (from_size / to_size),
                )
            } else {
                (
                    v1_before * (to_size / from_size),
                    v2_before * (to_size / from_size),
                )
            };

            let array =
                PrimitiveArray::<FromType>::from(vec![Some(v1_before), Some(v2_before), None]);
            let b = cast(&array, &ToType::DATA_TYPE).unwrap();
            let c = b.as_primitive::<ToType>();
            assert_eq!(v1_after, c.value(0));
            assert_eq!(v2_after, c.value(1));
            assert!(c.is_null(2));
        }

        // between each individual duration type
        test_cast_between_durations::<DurationSecondType, DurationMillisecondType>();
        test_cast_between_durations::<DurationSecondType, DurationMicrosecondType>();
        test_cast_between_durations::<DurationSecondType, DurationNanosecondType>();
        test_cast_between_durations::<DurationMillisecondType, DurationSecondType>();
        test_cast_between_durations::<DurationMillisecondType, DurationMicrosecondType>();
        test_cast_between_durations::<DurationMillisecondType, DurationNanosecondType>();
        test_cast_between_durations::<DurationMicrosecondType, DurationSecondType>();
        test_cast_between_durations::<DurationMicrosecondType, DurationMillisecondType>();
        test_cast_between_durations::<DurationMicrosecondType, DurationNanosecondType>();
        test_cast_between_durations::<DurationNanosecondType, DurationSecondType>();
        test_cast_between_durations::<DurationNanosecondType, DurationMillisecondType>();
        test_cast_between_durations::<DurationNanosecondType, DurationMicrosecondType>();

        // cast failed
        let array = DurationSecondArray::from(vec![
            Some(i64::MAX),
            Some(8640203410378005),
            Some(10241096),
            None,
        ]);
        let b = cast(&array, &DataType::Duration(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<DurationNanosecondType>();
        assert!(c.is_null(0));
        assert!(c.is_null(1));
        assert_eq!(10241096000000000, c.value(2));
        assert!(c.is_null(3));

        // durations to numerics
        let array = DurationSecondArray::from(vec![
            Some(i64::MAX),
            Some(8640203410378005),
            Some(10241096),
            None,
        ]);
        let b = cast(&array, &DataType::Int64).unwrap();
        let c = b.as_primitive::<Int64Type>();
        assert_eq!(i64::MAX, c.value(0));
        assert_eq!(8640203410378005, c.value(1));
        assert_eq!(10241096, c.value(2));
        assert!(c.is_null(3));

        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_primitive::<Int32Type>();
        assert_eq!(0, c.value(0));
        assert_eq!(0, c.value(1));
        assert_eq!(10241096, c.value(2));
        assert!(c.is_null(3));

        // numerics to durations
        let array = Int32Array::from(vec![Some(i32::MAX), Some(802034103), Some(10241096), None]);
        let b = cast(&array, &DataType::Duration(TimeUnit::Second)).unwrap();
        let c = b.as_any().downcast_ref::<DurationSecondArray>().unwrap();
        assert_eq!(i32::MAX as i64, c.value(0));
        assert_eq!(802034103, c.value(1));
        assert_eq!(10241096, c.value(2));
        assert!(c.is_null(3));
    }

    #[test]
    fn test_cast_to_strings() {
        let a = Int32Array::from(vec![1, 2, 3]);
        let out = cast(&a, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
        let out = cast(&a, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
    }

    #[test]
    fn test_str_to_str_casts() {
        for data in [
            vec![Some("foo"), Some("bar"), Some("ham")],
            vec![Some("foo"), None, Some("bar")],
        ] {
            let a = LargeStringArray::from(data.clone());
            let to = cast(&a, &DataType::Utf8).unwrap();
            let expect = a
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            let out = to
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(expect, out);

            let a = StringArray::from(data);
            let to = cast(&a, &DataType::LargeUtf8).unwrap();
            let expect = a
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            let out = to
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(expect, out);
        }
    }

    const VIEW_TEST_DATA: [Option<&str>; 5] = [
        Some("hello"),
        Some("repeated"),
        None,
        Some("large payload over 12 bytes"),
        Some("repeated"),
    ];

    #[test]
    fn test_string_view_to_binary_view() {
        let string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);

        assert!(can_cast_types(
            string_view_array.data_type(),
            &DataType::BinaryView
        ));

        let binary_view_array = cast(&string_view_array, &DataType::BinaryView).unwrap();
        assert_eq!(binary_view_array.data_type(), &DataType::BinaryView);

        let expect_binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(binary_view_array.as_ref(), &expect_binary_view_array);
    }

    #[test]
    fn test_binary_view_to_string_view() {
        let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);

        assert!(can_cast_types(
            binary_view_array.data_type(),
            &DataType::Utf8View
        ));

        let string_view_array = cast(&binary_view_array, &DataType::Utf8View).unwrap();
        assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

        let expect_string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(string_view_array.as_ref(), &expect_string_view_array);
    }

    #[test]
    fn test_binary_view_to_string_view_with_invalid_utf8() {
        let binary_view_array = BinaryViewArray::from_iter(vec![
            Some(b"valid".as_slice()),
            Some(&[0xff]),
            Some(b"utf8".as_slice()),
            None,
        ]);

        let strict_options = CastOptions {
            safe: false,
            ..Default::default()
        };

        assert!(
            cast_with_options(&binary_view_array, &DataType::Utf8View, &strict_options).is_err()
        );

        let safe_options = CastOptions {
            safe: true,
            ..Default::default()
        };

        let string_view_array =
            cast_with_options(&binary_view_array, &DataType::Utf8View, &safe_options).unwrap();
        assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

        let values: Vec<_> = string_view_array.as_string_view().iter().collect();

        assert_eq!(values, vec![Some("valid"), None, Some("utf8"), None]);
    }

    #[test]
    fn test_string_to_view() {
        _test_string_to_view::<i32>();
        _test_string_to_view::<i64>();
    }

    fn _test_string_to_view<O>()
    where
        O: OffsetSizeTrait,
    {
        let string_array = GenericStringArray::<O>::from_iter(VIEW_TEST_DATA);

        assert!(can_cast_types(
            string_array.data_type(),
            &DataType::Utf8View
        ));

        assert!(can_cast_types(
            string_array.data_type(),
            &DataType::BinaryView
        ));

        let string_view_array = cast(&string_array, &DataType::Utf8View).unwrap();
        assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

        let binary_view_array = cast(&string_array, &DataType::BinaryView).unwrap();
        assert_eq!(binary_view_array.data_type(), &DataType::BinaryView);

        let expect_string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(string_view_array.as_ref(), &expect_string_view_array);

        let expect_binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(binary_view_array.as_ref(), &expect_binary_view_array);
    }

    #[test]
    fn test_binary_to_view() {
        _test_binary_to_view::<i32>();
        _test_binary_to_view::<i64>();
    }

    fn _test_binary_to_view<O>()
    where
        O: OffsetSizeTrait,
    {
        let binary_array = GenericBinaryArray::<O>::from_iter(VIEW_TEST_DATA);

        assert!(can_cast_types(
            binary_array.data_type(),
            &DataType::Utf8View
        ));

        assert!(can_cast_types(
            binary_array.data_type(),
            &DataType::BinaryView
        ));

        let string_view_array = cast(&binary_array, &DataType::Utf8View).unwrap();
        assert_eq!(string_view_array.data_type(), &DataType::Utf8View);

        let binary_view_array = cast(&binary_array, &DataType::BinaryView).unwrap();
        assert_eq!(binary_view_array.data_type(), &DataType::BinaryView);

        let expect_string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(string_view_array.as_ref(), &expect_string_view_array);

        let expect_binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
        assert_eq!(binary_view_array.as_ref(), &expect_binary_view_array);
    }

    #[test]
    fn test_dict_to_view() {
        let values = StringArray::from_iter(VIEW_TEST_DATA);
        let keys = Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
        let string_dict_array =
            DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
        let typed_dict = string_dict_array.downcast_dict::<StringArray>().unwrap();

        let string_view_array = {
            let mut builder = StringViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
            for v in typed_dict {
                builder.append_option(v);
            }
            builder.finish()
        };
        let expected_string_array_type = string_view_array.data_type();
        let casted_string_array = cast(&string_dict_array, expected_string_array_type).unwrap();
        assert_eq!(casted_string_array.data_type(), expected_string_array_type);
        assert_eq!(casted_string_array.as_ref(), &string_view_array);

        let binary_buffer = cast(&typed_dict.values(), &DataType::Binary).unwrap();
        let binary_dict_array =
            DictionaryArray::<Int8Type>::new(typed_dict.keys().clone(), binary_buffer);
        let typed_binary_dict = binary_dict_array.downcast_dict::<BinaryArray>().unwrap();

        let binary_view_array = {
            let mut builder = BinaryViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
            for v in typed_binary_dict {
                builder.append_option(v);
            }
            builder.finish()
        };
        let expected_binary_array_type = binary_view_array.data_type();
        let casted_binary_array = cast(&binary_dict_array, expected_binary_array_type).unwrap();
        assert_eq!(casted_binary_array.data_type(), expected_binary_array_type);
        assert_eq!(casted_binary_array.as_ref(), &binary_view_array);
    }

    #[test]
    fn test_dict_to_view_null_dictionary_value_is_null() {
        // Ensure we preserve nulls in the values
        let keys = Int32Array::from_iter([Some(0), Some(1), Some(2), None, Some(1)]);

        let values = StringArray::from(vec![Some("aa"), None, Some("a value over twelve bytes")]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys.clone(), Arc::new(values)).unwrap();
        let casted = cast(&dict, &DataType::Utf8View).unwrap();
        assert_eq!(
            casted.as_string_view().iter().collect::<Vec<_>>(),
            vec![
                Some("aa"),
                None,
                Some("a value over twelve bytes"),
                None,
                None
            ]
        );
        // the same input cast to Utf8 goes through `unpack_dictionary` and always agreed
        let reference = cast(&dict, &DataType::Utf8).unwrap();
        assert_eq!(
            casted.as_string_view().iter().collect::<Vec<_>>(),
            reference.as_string::<i32>().iter().collect::<Vec<_>>()
        );

        let values = BinaryArray::from_opt_vec(vec![
            Some(b"aa".as_slice()),
            None,
            Some(b"a value over twelve bytes"),
        ]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let casted = cast(&dict, &DataType::BinaryView).unwrap();
        assert_eq!(
            casted.as_binary_view().iter().collect::<Vec<_>>(),
            vec![
                Some(b"aa".as_slice()),
                None,
                Some(b"a value over twelve bytes"),
                None,
                None
            ]
        );
    }

    #[test]
    fn test_view_to_dict() {
        let string_view_array = StringViewArray::from_iter(VIEW_TEST_DATA);
        let string_dict_array: DictionaryArray<Int8Type> = VIEW_TEST_DATA.into_iter().collect();
        let casted_type = string_dict_array.data_type();
        let casted_dict_array = cast(&string_view_array, casted_type).unwrap();
        assert_eq!(casted_dict_array.data_type(), casted_type);
        assert_eq!(casted_dict_array.as_ref(), &string_dict_array);

        let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);
        let binary_dict_array = string_dict_array.downcast_dict::<StringArray>().unwrap();
        let binary_buffer = cast(&binary_dict_array.values(), &DataType::Binary).unwrap();
        let binary_dict_array =
            DictionaryArray::<Int8Type>::new(binary_dict_array.keys().clone(), binary_buffer);
        let casted_type = binary_dict_array.data_type();
        let casted_binary_array = cast(&binary_view_array, casted_type).unwrap();
        assert_eq!(casted_binary_array.data_type(), casted_type);
        assert_eq!(casted_binary_array.as_ref(), &binary_dict_array);
    }

    #[test]
    fn test_view_to_string() {
        _test_view_to_string::<i32>();
        _test_view_to_string::<i64>();
    }

    fn _test_view_to_string<O>()
    where
        O: OffsetSizeTrait,
    {
        let string_view_array = {
            let mut builder = StringViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
            for s in &VIEW_TEST_DATA {
                builder.append_option(*s);
            }
            builder.finish()
        };

        let binary_view_array = BinaryViewArray::from_iter(VIEW_TEST_DATA);

        let expected_string_array = GenericStringArray::<O>::from_iter(VIEW_TEST_DATA);
        let expected_type = expected_string_array.data_type();

        assert!(can_cast_types(string_view_array.data_type(), expected_type));
        assert!(can_cast_types(binary_view_array.data_type(), expected_type));

        let string_view_casted_array = cast(&string_view_array, expected_type).unwrap();
        assert_eq!(string_view_casted_array.data_type(), expected_type);
        assert_eq!(string_view_casted_array.as_ref(), &expected_string_array);

        let binary_view_casted_array = cast(&binary_view_array, expected_type).unwrap();
        assert_eq!(binary_view_casted_array.data_type(), expected_type);
        assert_eq!(binary_view_casted_array.as_ref(), &expected_string_array);
    }

    #[test]
    fn test_view_to_binary() {
        _test_view_to_binary::<i32>();
        _test_view_to_binary::<i64>();
    }

    fn _test_view_to_binary<O>()
    where
        O: OffsetSizeTrait,
    {
        let view_array = {
            let mut builder = BinaryViewBuilder::new().with_fixed_block_size(8); // multiple buffers.
            for s in &VIEW_TEST_DATA {
                builder.append_option(*s);
            }
            builder.finish()
        };

        let expected_binary_array = GenericBinaryArray::<O>::from_iter(VIEW_TEST_DATA);
        let expected_type = expected_binary_array.data_type();

        assert!(can_cast_types(view_array.data_type(), expected_type));

        let binary_array = cast(&view_array, expected_type).unwrap();
        assert_eq!(binary_array.data_type(), expected_type);

        assert_eq!(binary_array.as_ref(), &expected_binary_array);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_f64() {
        let f64_values: Vec<f64> = vec![
            i64::MIN as f64,
            i32::MIN as f64,
            i16::MIN as f64,
            i8::MIN as f64,
            0_f64,
            u8::MAX as f64,
            u16::MAX as f64,
            u32::MAX as f64,
            u64::MAX as f64,
        ];
        let f64_array: ArrayRef = Arc::new(Float64Array::from(f64_values));

        let f64_expected = vec![
            -9223372036854776000.0,
            -2147483648.0,
            -32768.0,
            -128.0,
            0.0,
            255.0,
            65535.0,
            4294967295.0,
            18446744073709552000.0,
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected = vec![
            -9223372000000000000.0,
            -2147483600.0,
            -32768.0,
            -128.0,
            0.0,
            255.0,
            65535.0,
            4294967300.0,
            18446744000000000000.0,
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let f16_expected = vec![
            f16::from_f64(-9223372000000000000.0),
            f16::from_f64(-2147483600.0),
            f16::from_f64(-32768.0),
            f16::from_f64(-128.0),
            f16::from_f64(0.0),
            f16::from_f64(255.0),
            f16::from_f64(65535.0),
            f16::from_f64(4294967300.0),
            f16::from_f64(18446744000000000000.0),
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&f64_array, &DataType::Float16)
                .iter()
                .map(|i| i.parse::<f16>().unwrap())
                .collect::<Vec<f16>>()
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f64_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_f32() {
        let f32_values: Vec<f32> = vec![
            i32::MIN as f32,
            i32::MIN as f32,
            i16::MIN as f32,
            i8::MIN as f32,
            0_f32,
            u8::MAX as f32,
            u16::MAX as f32,
            u32::MAX as f32,
            u32::MAX as f32,
        ];
        let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967296.0",
            "4294967296.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "4294967300.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f32_array, &DataType::Float32)
        );

        let f16_expected = vec![
            "-inf", "-inf", "-32768.0", "-128.0", "0.0", "255.0", "inf", "inf", "inf",
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&f32_array, &DataType::Float16)
        );

        let i64_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f32_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f32_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f32_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f32_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f32_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f32_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f32_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f32_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_uint64() {
        let u64_values: Vec<u64> = vec![
            0,
            u8::MAX as u64,
            u16::MAX as u64,
            u32::MAX as u64,
            u64::MAX,
        ];
        let u64_array: ArrayRef = Arc::new(UInt64Array::from(u64_values));

        let f64_expected = vec![0.0, 255.0, 65535.0, 4294967295.0, 18446744073709552000.0];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected = vec![0.0, 255.0, 65535.0, 4294967300.0, 18446744000000000000.0];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let f16_expected = vec![
            f16::from_f64(0.0),
            f16::from_f64(255.0),
            f16::from_f64(65535.0),
            f16::from_f64(4294967300.0),
            f16::from_f64(18446744000000000000.0),
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&u64_array, &DataType::Float16)
                .iter()
                .map(|i| i.parse::<f16>().unwrap())
                .collect::<Vec<f16>>()
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u64_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u64_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u64_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u64_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535", "4294967295", "18446744073709551615"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u64_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u64_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u64_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u64_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_uint32() {
        let u32_values: Vec<u32> = vec![0, u8::MAX as u32, u16::MAX as u32, u32::MAX];
        let u32_array: ArrayRef = Arc::new(UInt32Array::from(u32_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0", "4294967295.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u32_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0", "4294967300.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u32_array, &DataType::Float32)
        );

        let f16_expected = vec!["0.0", "255.0", "inf", "inf"];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&u32_array, &DataType::Float16)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u32_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u32_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u32_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u32_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u32_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u32_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_uint16() {
        let u16_values: Vec<u16> = vec![0, u8::MAX as u16, u16::MAX];
        let u16_array: ArrayRef = Arc::new(UInt16Array::from(u16_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u16_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u16_array, &DataType::Float32)
        );

        let f16_expected = vec!["0.0", "255.0", "inf"];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&u16_array, &DataType::Float16)
        );

        let i64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u16_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u16_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u16_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u16_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u16_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_uint8() {
        let u8_values: Vec<u8> = vec![0, u8::MAX];
        let u8_array: ArrayRef = Arc::new(UInt8Array::from(u8_values));

        let f64_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u8_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u8_array, &DataType::Float32)
        );

        let f16_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&u8_array, &DataType::Float16)
        );

        let i64_expected = vec!["0", "255"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u8_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u8_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u8_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u8_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u8_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_int64() {
        let i64_values: Vec<i64> = vec![
            i64::MIN,
            i32::MIN as i64,
            i16::MIN as i64,
            i8::MIN as i64,
            0,
            i8::MAX as i64,
            i16::MAX as i64,
            i32::MAX as i64,
            i64::MAX,
        ];
        let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_values));

        let f64_expected = vec![
            -9223372036854776000.0,
            -2147483648.0,
            -32768.0,
            -128.0,
            0.0,
            127.0,
            32767.0,
            2147483647.0,
            9223372036854776000.0,
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected = vec![
            -9223372000000000000.0,
            -2147483600.0,
            -32768.0,
            -128.0,
            0.0,
            127.0,
            32767.0,
            2147483600.0,
            9223372000000000000.0,
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let f16_expected = vec![
            f16::from_f64(-9223372000000000000.0),
            f16::from_f64(-2147483600.0),
            f16::from_f64(-32768.0),
            f16::from_f64(-128.0),
            f16::from_f64(0.0),
            f16::from_f64(127.0),
            f16::from_f64(32767.0),
            f16::from_f64(2147483600.0),
            f16::from_f64(9223372000000000000.0),
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&i64_array, &DataType::Float16)
                .iter()
                .map(|i| i.parse::<f16>().unwrap())
                .collect::<Vec<f16>>()
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i64_array, &DataType::Int32)
        );

        assert_eq!(
            i32_expected,
            get_cast_values::<Date32Type>(&i64_array, &DataType::Date32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i64_array, &DataType::UInt8)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_int32() {
        let i32_values: Vec<i32> = vec![
            i32::MIN,
            i16::MIN as i32,
            i8::MIN as i32,
            0,
            i8::MAX as i32,
            i16::MAX as i32,
            i32::MAX,
        ];
        let i32_array: ArrayRef = Arc::new(Int32Array::from(i32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i32_array, &DataType::Float32)
        );

        let f16_expected = vec![
            f16::from_f64(-2147483600.0),
            f16::from_f64(-32768.0),
            f16::from_f64(-128.0),
            f16::from_f64(0.0),
            f16::from_f64(127.0),
            f16::from_f64(32767.0),
            f16::from_f64(2147483600.0),
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&i32_array, &DataType::Float16)
                .iter()
                .map(|i| i.parse::<f16>().unwrap())
                .collect::<Vec<f16>>()
        );

        let i16_expected = vec!["null", "-32768", "-128", "0", "127", "32767", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i32_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "null", "-128", "0", "127", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i32_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i32_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "null", "0", "127", "32767", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "null", "0", "127", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i32_array, &DataType::UInt8)
        );

        // The date32 to date64 cast increases the numerical values in order to keep the same dates.
        let i64_expected = vec![
            "-185542587187200000",
            "-2831155200000",
            "-11059200000",
            "0",
            "10972800000",
            "2831068800000",
            "185542587100800000",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Date64Type>(&i32_array, &DataType::Date64)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_int16() {
        let i16_values: Vec<i16> = vec![i16::MIN, i8::MIN as i16, 0, i8::MAX as i16, i16::MAX];
        let i16_array: ArrayRef = Arc::new(Int16Array::from(i16_values));

        let f64_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i16_array, &DataType::Float64)
        );

        let f32_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i16_array, &DataType::Float32)
        );

        let f16_expected = vec![
            f16::from_f64(-32768.0),
            f16::from_f64(-128.0),
            f16::from_f64(0.0),
            f16::from_f64(127.0),
            f16::from_f64(32767.0),
        ];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&i16_array, &DataType::Float16)
                .iter()
                .map(|i| i.parse::<f16>().unwrap())
                .collect::<Vec<f16>>()
        );

        let i64_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i16_array, &DataType::Int64)
        );

        let i32_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i16_array, &DataType::Int32)
        );

        let i16_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i16_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "-128", "0", "127", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i16_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "0", "127", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_date32() {
        let i32_values: Vec<i32> = vec![
            i32::MIN,
            i16::MIN as i32,
            i8::MIN as i32,
            0,
            i8::MAX as i32,
            i16::MAX as i32,
            i32::MAX,
        ];
        let date32_array: ArrayRef = Arc::new(Date32Array::from(i32_values));

        let i64_expected = vec![
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&date32_array, &DataType::Int64)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unsupported inline assembly
    fn test_cast_from_int8() {
        let i8_values: Vec<i8> = vec![i8::MIN, 0, i8::MAX];
        let i8_array = Int8Array::from(i8_values);

        let f64_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i8_array, &DataType::Float64)
        );

        let f32_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i8_array, &DataType::Float32)
        );

        let f16_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f16_expected,
            get_cast_values::<Float16Type>(&i8_array, &DataType::Float16)
        );

        let i64_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i8_array, &DataType::Int64)
        );

        let i32_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i8_array, &DataType::Int32)
        );

        let i16_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i8_array, &DataType::Int16)
        );

        let i8_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i8_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "0", "127"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "0", "127"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "0", "127"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "0", "127"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i8_array, &DataType::UInt8)
        );
    }

    /// Convert `array` into a vector of strings by casting to data type dt
    fn get_cast_values<T>(array: &dyn Array, dt: &DataType) -> Vec<String>
    where
        T: ArrowPrimitiveType,
    {
        let c = cast(array, dt).unwrap();
        let a = c.as_primitive::<T>();
        let mut v: Vec<String> = vec![];
        for i in 0..array.len() {
            if a.is_null(i) {
                v.push("null".to_string())
            } else {
                v.push(format!("{:?}", a.value(i)));
            }
        }
        v
    }

    #[test]
    fn test_cast_null_array_to_from_decimal_array() {
        let data_type = DataType::Decimal128(12, 4);
        let array = new_null_array(&DataType::Null, 4);
        assert_eq!(array.data_type(), &DataType::Null);
        let cast_array = cast(&array, &data_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &data_type);
        for i in 0..4 {
            assert!(cast_array.is_null(i));
        }

        let array = new_null_array(&data_type, 4);
        assert_eq!(array.data_type(), &data_type);
        let cast_array = cast(&array, &DataType::Null).expect("cast failed");
        assert_eq!(cast_array.data_type(), &DataType::Null);
        assert_eq!(cast_array.len(), 4);
        assert_eq!(cast_array.logical_nulls().unwrap().null_count(), 4);
    }

    #[test]
    fn test_cast_null_array_from_and_to_primitive_array() {
        macro_rules! typed_test {
            ($ARR_TYPE:ident, $DATATYPE:ident, $TYPE:tt) => {{
                {
                    let array = Arc::new(NullArray::new(6)) as ArrayRef;
                    let expected = $ARR_TYPE::from(vec![None; 6]);
                    let cast_type = DataType::$DATATYPE;
                    let cast_array = cast(&array, &cast_type).expect("cast failed");
                    let cast_array = cast_array.as_primitive::<$TYPE>();
                    assert_eq!(cast_array.data_type(), &cast_type);
                    assert_eq!(cast_array, &expected);
                }
            }};
        }

        typed_test!(Int16Array, Int16, Int16Type);
        typed_test!(Int32Array, Int32, Int32Type);
        typed_test!(Int64Array, Int64, Int64Type);

        typed_test!(UInt16Array, UInt16, UInt16Type);
        typed_test!(UInt32Array, UInt32, UInt32Type);
        typed_test!(UInt64Array, UInt64, UInt64Type);

        typed_test!(Float16Array, Float16, Float16Type);
        typed_test!(Float32Array, Float32, Float32Type);
        typed_test!(Float64Array, Float64, Float64Type);

        typed_test!(Date32Array, Date32, Date32Type);
        typed_test!(Date64Array, Date64, Date64Type);
    }

    fn cast_from_null_to_other_base(data_type: &DataType, is_complex: bool) {
        // Cast from null to data_type
        let array = new_null_array(&DataType::Null, 4);
        assert_eq!(array.data_type(), &DataType::Null);
        let cast_array = cast(&array, data_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), data_type);
        for i in 0..4 {
            if is_complex {
                assert!(cast_array.logical_nulls().unwrap().is_null(i));
            } else {
                assert!(cast_array.is_null(i));
            }
        }
    }

    fn cast_from_null_to_other(data_type: &DataType) {
        cast_from_null_to_other_base(data_type, false);
    }

    fn cast_from_null_to_other_complex(data_type: &DataType) {
        cast_from_null_to_other_base(data_type, true);
    }

    #[test]
    fn test_cast_null_from_and_to_variable_sized() {
        cast_from_null_to_other(&DataType::Utf8);
        cast_from_null_to_other(&DataType::LargeUtf8);
        cast_from_null_to_other(&DataType::Binary);
        cast_from_null_to_other(&DataType::LargeBinary);
    }

    #[test]
    fn test_cast_null_from_and_to_nested_type() {
        // Cast null from and to map
        let data_type = DataType::Map(
            Arc::new(Field::new_struct(
                Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
                vec![
                    Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                    Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Int32, true),
                ],
                false,
            )),
            false,
        );
        cast_from_null_to_other(&data_type);

        // Cast null from and to list
        let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        cast_from_null_to_other(&data_type);
        let data_type = DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        cast_from_null_to_other(&data_type);
        let data_type =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 4);
        cast_from_null_to_other(&data_type);

        // Cast null from and to dictionary
        let values = vec![None, None, None, None] as Vec<Option<&str>>;
        let array: DictionaryArray<Int8Type> = values.into_iter().collect();
        let array = Arc::new(array) as ArrayRef;
        let data_type = array.data_type().to_owned();
        cast_from_null_to_other(&data_type);

        // Cast null from and to struct
        let data_type = DataType::Struct(vec![Field::new("data", DataType::Int64, false)].into());
        cast_from_null_to_other(&data_type);

        let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
        cast_from_null_to_other(&target_type);

        let target_type =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
        cast_from_null_to_other(&target_type);

        let fields = UnionFields::from_fields(vec![Field::new("a", DataType::Int64, false)]);
        let target_type = DataType::Union(fields, UnionMode::Sparse);
        cast_from_null_to_other_complex(&target_type);

        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new("item", DataType::Int32, true)),
            Arc::new(Field::new("item", DataType::Int32, true)),
        );
        cast_from_null_to_other_complex(&target_type);
    }

    #[test]
    fn test_cast_utf8_to_date32() {
        use chrono::NaiveDate;
        let from_ymd = chrono::NaiveDate::from_ymd_opt;
        let since = chrono::NaiveDate::signed_duration_since;

        let a = StringArray::from(vec![
            "2000-01-01",          // valid date with leading 0s
            "2000-01-01T12:00:00", // valid datetime, will throw away the time part
            "2000-2-2",            // valid date without leading 0s
            "2000-00-00",          // invalid month and day
            "2000",                // just a year is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_primitive::<Date32Type>();

        // test valid inputs
        let date_value = since(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            from_ymd(1970, 1, 1).unwrap(),
        )
        .num_days() as i32;
        assert!(c.is_valid(0)); // "2000-01-01"
        assert_eq!(date_value, c.value(0));

        assert!(c.is_valid(1)); // "2000-01-01T12:00:00"
        assert_eq!(date_value, c.value(1));

        let date_value = since(
            NaiveDate::from_ymd_opt(2000, 2, 2).unwrap(),
            from_ymd(1970, 1, 1).unwrap(),
        )
        .num_days() as i32;
        assert!(c.is_valid(2)); // "2000-2-2"
        assert_eq!(date_value, c.value(2));

        // test invalid inputs
        assert!(!c.is_valid(3)); // "2000-00-00"
        assert!(!c.is_valid(4)); // "2000"
    }

    #[test]
    fn test_cast_utf8_to_date64() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
            "2020-2-2T12:34:56",   // valid date time without leading 0s
            "2000-00-00T12:00:00", // invalid month and day
            "2000-01-01 12:00:00", // missing the 'T'
            "2000-01-01",          // just a date is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_primitive::<Date64Type>();

        // test valid inputs
        assert!(c.is_valid(0)); // "2000-01-01T12:00:00"
        assert_eq!(946728000000, c.value(0));
        assert!(c.is_valid(1)); // "2020-12-15T12:34:56"
        assert_eq!(1608035696000, c.value(1));
        assert!(!c.is_valid(2)); // "2020-2-2T12:34:56"

        assert!(!c.is_valid(3)); // "2000-00-00T12:00:00"
        assert!(c.is_valid(4)); // "2000-01-01 12:00:00"
        assert_eq!(946728000000, c.value(4));
        assert!(c.is_valid(5)); // "2000-01-01"
        assert_eq!(946684800000, c.value(5));
    }

    #[test]
    fn test_utf8_cast_offsets() {
        // test if offset of the array is taken into account during cast
        let str_array = StringArray::from(vec!["a", "b", "c"]);
        let str_array = str_array.slice(1, 2);

        let out = cast(&str_array, &DataType::LargeUtf8).unwrap();

        let large_str_array = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let strs = large_str_array.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(strs, &["b", "c"])
    }

    #[test]
    fn test_cast_outside_supported_range_for_nanoseconds() {
        const EXPECTED_ERROR_MESSAGE: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

        let array = StringArray::from(vec![Some("1650-01-01 01:01:01.000001")]);

        let cast_options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };

        let result =
            cast_string_to_timestamp::<i32, TimestampNanosecondType>(&array, None, &cast_options);

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Cast error: Overflow converting {} to Nanosecond. {}",
                array.value(0),
                EXPECTED_ERROR_MESSAGE
            )
        );
    }

    #[test]
    fn test_cast_date32_to_timestamp() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_primitive::<TimestampSecondType>();
        assert_eq!(1609459200, c.value(0));
        assert_eq!(1640995200, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_ms() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(1609459200000, c.value(0));
        assert_eq!(1640995200000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_us() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(1609459200000000, c.value(0));
        assert_eq!(1640995200000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_ns() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(1609459200000000000, c.value(0));
        assert_eq!(1640995200000000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_us_overflow() {
        const MAX_DAYS_MICROS: i32 = (i64::MAX / MICROSECONDS_IN_DAY) as i32;
        let a = Date32Array::from(vec![Some(MAX_DAYS_MICROS), Some(MAX_DAYS_MICROS + 1), None]);
        let array = Arc::new(a) as ArrayRef;
        let err = cast_with_options(
            &array,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(err.is_err());

        let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        let c = b.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(MAX_DAYS_MICROS as i64 * MICROSECONDS_IN_DAY, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_ns_overflow() {
        // 2262-04-11, 2062-04-12
        let upper_limit = 106_751;
        let a = Date32Array::from(vec![Some(upper_limit), Some(upper_limit + 1), None]);
        let array = Arc::new(a) as ArrayRef;
        let err = cast_with_options(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(err.is_err());

        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let c = b.as_primitive::<TimestampNanosecondType>();
        assert_eq!(upper_limit as i64 * NANOSECONDS_IN_DAY, c.value(0));
        assert!(c.is_null(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_timezone_cast() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let v = b.as_primitive::<TimestampNanosecondType>();

        assert_eq!(v.value(0), 946728000000000000);
        assert_eq!(v.value(1), 1608035696000000000);

        let b = cast(
            &b,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        )
        .unwrap();
        let v = b.as_primitive::<TimestampNanosecondType>();

        assert_eq!(v.value(0), 946728000000000000);
        assert_eq!(v.value(1), 1608035696000000000);

        let b = cast(
            &b,
            &DataType::Timestamp(TimeUnit::Millisecond, Some("+02:00".into())),
        )
        .unwrap();
        let v = b.as_primitive::<TimestampMillisecondType>();

        assert_eq!(v.value(0), 946728000000);
        assert_eq!(v.value(1), 1608035696000);
    }

    #[test]
    fn test_cast_utf8_to_timestamp() {
        fn test_tz(tz: Arc<str>) {
            let valid = StringArray::from(vec![
                "2023-01-01 04:05:06.789000-08:00",
                "2023-01-01 04:05:06.789000-07:00",
                "2023-01-01 04:05:06.789 -0800",
                "2023-01-01 04:05:06.789 -08:00",
                "2023-01-01 040506 +0730",
                "2023-01-01 040506 +07:30",
                "2023-01-01 04:05:06.789",
                "2023-01-01 04:05:06",
                "2023-01-01",
            ]);

            let array = Arc::new(valid) as ArrayRef;
            let b = cast_with_options(
                &array,
                &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.clone())),
                &CastOptions {
                    safe: false,
                    format_options: FormatOptions::default(),
                },
            )
            .unwrap();

            let tz = tz.as_ref().parse().unwrap();

            let as_tz =
                |v: i64| as_datetime_with_timezone::<TimestampNanosecondType>(v, tz).unwrap();

            let as_utc = |v: &i64| as_tz(*v).naive_utc().to_string();
            let as_local = |v: &i64| as_tz(*v).naive_local().to_string();

            let values = b.as_primitive::<TimestampNanosecondType>().values();
            let utc_results: Vec<_> = values.iter().map(as_utc).collect();
            let local_results: Vec<_> = values.iter().map(as_local).collect();

            // Absolute timestamps should be parsed preserving the same UTC instant
            assert_eq!(
                &utc_results[..6],
                &[
                    "2023-01-01 12:05:06.789".to_string(),
                    "2023-01-01 11:05:06.789".to_string(),
                    "2023-01-01 12:05:06.789".to_string(),
                    "2023-01-01 12:05:06.789".to_string(),
                    "2022-12-31 20:35:06".to_string(),
                    "2022-12-31 20:35:06".to_string(),
                ]
            );
            // Non-absolute timestamps should be parsed preserving the same local instant
            assert_eq!(
                &local_results[6..],
                &[
                    "2023-01-01 04:05:06.789".to_string(),
                    "2023-01-01 04:05:06".to_string(),
                    "2023-01-01 00:00:00".to_string()
                ]
            )
        }

        test_tz("+00:00".into());
        test_tz("+02:00".into());
    }

    #[test]
    fn test_cast_invalid_utf8() {
        let v1: &[u8] = b"\xFF invalid";
        let v2: &[u8] = b"\x00 Foo";
        let s = BinaryArray::from(vec![v1, v2]);
        let options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };
        let array = cast_with_options(&s, &DataType::Utf8, &options).unwrap();
        let a = array.as_string::<i32>();
        a.to_data().validate_full().unwrap();

        assert_eq!(a.null_count(), 1);
        assert_eq!(a.len(), 2);
        assert!(a.is_null(0));
        assert_eq!(a.value(0), "");
        assert_eq!(a.value(1), "\x00 Foo");
    }

    #[test]
    fn test_cast_sliced_binary_with_hidden_invalid_utf8() {
        // The invalid first value is outside the slice, so it must not fail the cast
        let values: Vec<&[u8]> = vec![b"\xFF", b"a", b"b", b"c", b"d", b"e", b"f"];
        let arrays: [ArrayRef; 2] = [
            Arc::new(BinaryArray::from(values.clone()).slice(1, 6)),
            Arc::new(LargeBinaryArray::from(values).slice(1, 6)),
        ];
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let expected = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        for array in &arrays {
            for to_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                let result = cast_with_options(array, &to_type, &options).unwrap();
                assert_eq!(result.as_ref(), cast(&expected, &to_type).unwrap().as_ref());
            }

            // A dictionary with fewer keys than half its values takes a separate
            // path to Utf8View
            let keys = Int32Array::from(vec![5, 0]);
            let dict = DictionaryArray::try_new(keys, Arc::clone(array)).unwrap();
            let result = cast_with_options(&dict, &DataType::Utf8View, &options).unwrap();
            assert_eq!(result.as_ref(), &StringViewArray::from(vec!["f", "a"]));
        }
    }

    #[test]
    fn test_cast_utf8_to_timestamptz() {
        let valid = StringArray::from(vec!["2023-01-01"]);

        let array = Arc::new(valid) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        )
        .unwrap();

        let expect = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));

        assert_eq!(b.data_type(), &expect);
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(1672531200000000000, c.value(0));
    }

    /// helper function to test casting from duration to interval
    fn cast_from_duration_to_interval<T: ArrowTemporalType<Native = i64>>(
        array: Vec<i64>,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
        let array = PrimitiveArray::<T>::new(array.into(), None);
        let array = Arc::new(array) as ArrayRef;
        let interval = DataType::Interval(IntervalUnit::MonthDayNano);
        let out = cast_with_options(&array, &interval, cast_options)?;
        let out = out.as_primitive::<IntervalMonthDayNanoType>().clone();
        Ok(out)
    }

    #[test]
    fn test_cast_from_duration_to_interval() {
        // from duration second to interval month day nano
        let array = vec![1234567];
        let casted_array =
            cast_from_duration_to_interval::<DurationSecondType>(array, &CastOptions::default())
                .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(0, 0, 1234567000000000)
        );

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
            array.clone(),
            &CastOptions::default(),
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
            array,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());

        // from duration millisecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array,
            &CastOptions::default(),
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(0, 0, 1234567000000)
        );

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array.clone(),
            &CastOptions::default(),
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());

        // from duration microsecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array,
            &CastOptions::default(),
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(0, 0, 1234567000)
        );

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array.clone(),
            &CastOptions::default(),
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());

        // from duration nanosecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationNanosecondType>(
            array,
            &CastOptions::default(),
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(0, 0, 1234567)
        );

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationNanosecondType>(
            array,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        )
        .unwrap();
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(0, 0, i64::MAX)
        );
    }

    /// helper function to test casting from interval to duration
    fn cast_from_interval_to_duration<T: ArrowTemporalType>(
        array: &IntervalMonthDayNanoArray,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<T>, ArrowError> {
        let casted_array = cast_with_options(&array, &T::DATA_TYPE, cast_options)?;
        casted_array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::ComputeError(format!("Failed to downcast to {}", T::DATA_TYPE))
            })
            .cloned()
    }

    #[test]
    fn test_cast_from_interval_to_duration() {
        let nullable = CastOptions::default();
        let fallible = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let v = IntervalMonthDayNano::new(0, 0, 1234567);

        // from interval month day nano to duration second
        let array = vec![v].into();
        let casted_array: DurationSecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert_eq!(casted_array.value(0), 0);

        let array = vec![IntervalMonthDayNano::MAX].into();
        let casted_array: DurationSecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert!(!casted_array.is_valid(0));

        let res = cast_from_interval_to_duration::<DurationSecondType>(&array, &fallible);
        assert!(res.is_err());

        // from interval month day nano to duration millisecond
        let array = vec![v].into();
        let casted_array: DurationMillisecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert_eq!(casted_array.value(0), 1);

        let array = vec![IntervalMonthDayNano::MAX].into();
        let casted_array: DurationMillisecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert!(!casted_array.is_valid(0));

        let res = cast_from_interval_to_duration::<DurationMillisecondType>(&array, &fallible);
        assert!(res.is_err());

        // from interval month day nano to duration microsecond
        let array = vec![v].into();
        let casted_array: DurationMicrosecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert_eq!(casted_array.value(0), 1234);

        let array = vec![IntervalMonthDayNano::MAX].into();
        let casted_array =
            cast_from_interval_to_duration::<DurationMicrosecondType>(&array, &nullable).unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array =
            cast_from_interval_to_duration::<DurationMicrosecondType>(&array, &fallible);
        assert!(casted_array.is_err());

        // from interval month day nano to duration nanosecond
        let array = vec![v].into();
        let casted_array: DurationNanosecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert_eq!(casted_array.value(0), 1234567);

        let array = vec![IntervalMonthDayNano::MAX].into();
        let casted_array: DurationNanosecondArray =
            cast_from_interval_to_duration(&array, &nullable).unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array =
            cast_from_interval_to_duration::<DurationNanosecondType>(&array, &fallible);
        assert!(casted_array.is_err());

        let array = vec![
            IntervalMonthDayNanoType::make_value(0, 1, 0),
            IntervalMonthDayNanoType::make_value(-1, 0, 0),
            IntervalMonthDayNanoType::make_value(1, 1, 0),
            IntervalMonthDayNanoType::make_value(1, 0, 1),
            IntervalMonthDayNanoType::make_value(0, 0, -1),
        ]
        .into();
        let casted_array =
            cast_from_interval_to_duration::<DurationNanosecondType>(&array, &nullable).unwrap();
        assert!(!casted_array.is_valid(0));
        assert!(!casted_array.is_valid(1));
        assert!(!casted_array.is_valid(2));
        assert!(!casted_array.is_valid(3));
        assert!(casted_array.is_valid(4));
        assert_eq!(casted_array.value(4), -1);
    }

    /// helper function to test casting from interval year month to interval month day nano
    fn cast_from_interval_year_month_to_interval_month_day_nano(
        array: Vec<i32>,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
        let array = PrimitiveArray::<IntervalYearMonthType>::from(array);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Interval(IntervalUnit::MonthDayNano),
            cast_options,
        )?;
        casted_array
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                ArrowError::ComputeError(
                    "Failed to downcast to IntervalMonthDayNanoArray".to_string(),
                )
            })
            .cloned()
    }

    #[test]
    fn test_cast_from_interval_year_month_to_interval_month_day_nano() {
        // from interval year month to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_interval_year_month_to_interval_month_day_nano(
            array,
            &CastOptions::default(),
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            casted_array.value(0),
            IntervalMonthDayNano::new(1234567, 0, 0)
        );
    }

    /// helper function to test casting from interval day time to interval month day nano
    fn cast_from_interval_day_time_to_interval_month_day_nano(
        array: Vec<IntervalDayTime>,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError> {
        let array = PrimitiveArray::<IntervalDayTimeType>::from(array);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Interval(IntervalUnit::MonthDayNano),
            cast_options,
        )?;
        Ok(casted_array
            .as_primitive::<IntervalMonthDayNanoType>()
            .clone())
    }

    #[test]
    fn test_cast_from_interval_day_time_to_interval_month_day_nano() {
        // from interval day time to interval month day nano
        let array = vec![IntervalDayTime::new(123, 0)];
        let casted_array =
            cast_from_interval_day_time_to_interval_month_day_nano(array, &CastOptions::default())
                .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(casted_array.value(0), IntervalMonthDayNano::new(0, 123, 0));
    }

    #[test]
    fn test_can_cast_interval_to_int64_matches_cast() {
        // Assert the two agree for every interval unit, rather than hard coding the answer.
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(IntervalYearMonthArray::from(vec![12])),
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(1, 2)])),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(1, 2, 3),
            ])),
        ];
        for array in arrays {
            let from = array.data_type();
            assert_eq!(
                can_cast_types(from, &DataType::Int64),
                cast(&array, &DataType::Int64).is_ok(),
                "can_cast_types disagrees with cast for {from} -> Int64"
            );
        }
    }

    #[test]
    fn test_cast_union_to_int64_skips_uncastable_interval_child() {
        // `resolve_child_array` picks the first child `can_cast_types` accepts, so an
        // over-permissive answer made it pick the interval child instead of the `Utf8` one.
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("iv", DataType::Interval(IntervalUnit::YearMonth), true),
                Field::new("s", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let union = UnionArray::try_new(
            fields,
            vec![0, 1, 0].into(),
            None,
            vec![
                Arc::new(IntervalYearMonthArray::from(vec![Some(12), None, Some(24)])),
                Arc::new(StringArray::from(vec![None, Some("77"), None])),
            ],
        )
        .unwrap();

        let casted = cast(&union, &DataType::Int64).unwrap();
        let casted = casted.as_primitive::<Int64Type>();
        assert!(casted.is_null(0));
        assert_eq!(casted.value(1), 77);
        assert!(casted.is_null(2));
    }

    #[test]
    fn test_cast_below_unixtimestamp() {
        let valid = StringArray::from(vec![
            "1900-01-03 23:59:59",
            "1969-12-31 00:00:01",
            "1989-12-31 00:00:01",
        ]);

        let array = Arc::new(valid) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        )
        .unwrap();

        let ts_array = casted_array
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .iter()
            .map(|ts| ts / 1_000_000)
            .collect::<Vec<_>>();

        let array = TimestampMillisecondArray::from(ts_array).with_timezone("+00:00".to_string());
        let casted_array = cast(&array, &DataType::Date32).unwrap();
        let date_array = casted_array.as_primitive::<Date32Type>();
        let casted_array = cast(&date_array, &DataType::Utf8).unwrap();
        let string_array = casted_array.as_string::<i32>();
        assert_eq!("1900-01-03", string_array.value(0));
        assert_eq!("1969-12-31", string_array.value(1));
        assert_eq!("1989-12-31", string_array.value(2));
    }

    const CAST_OPTIONS: CastOptions<'static> = CastOptions {
        safe: true,
        format_options: FormatOptions::new(),
    };

    #[test]
    #[expect(clippy::assertions_on_constants)]
    fn test_const_options() {
        assert!(CAST_OPTIONS.safe)
    }

    #[test]
    fn test_cast_string_to_timestamp_invalid_tz() {
        // content after Z should be ignored
        let bad_timestamp = "2023-12-05T21:58:10.45ZZTOP";
        let array = StringArray::from(vec![Some(bad_timestamp)]);

        let data_types = [
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ];

        let cast_options = CastOptions {
            safe: false,
            ..Default::default()
        };

        for dt in data_types {
            // The trailing detail of the message differs depending on whether
            // `chrono-tz` is enabled, so only the common prefix is asserted.
            let err = cast_with_options(&array, &dt, &cast_options)
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("Parser error: Invalid timezone \"ZZTOP\":"),
                "{err}"
            );
        }
    }
    #[test]
    fn test_cast_struct_to_struct() {
        let struct_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Boolean, false),
                Field::new("b", DataType::Int32, false),
            ]
            .into(),
        );
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ]
            .into(),
        );
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
        ]);
        let casted_array = cast(&struct_array, &to_type).unwrap();
        let casted_array = casted_array.as_struct();
        assert_eq!(casted_array.data_type(), &to_type);
        let casted_boolean_array = casted_array
            .column(0)
            .as_string::<i32>()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let casted_int_array = casted_array
            .column(1)
            .as_string::<i32>()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(casted_boolean_array, vec!["false", "false", "true", "true"]);
        assert_eq!(casted_int_array, vec!["42", "28", "19", "31"]);

        // test for can't cast
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Date32, false),
                Field::new("b", DataType::Utf8, false),
            ]
            .into(),
        );
        assert!(!can_cast_types(&struct_type, &to_type));
        let result = cast(&struct_array, &to_type);
        assert_eq!(
            "Cast error: Casting from Boolean to Date32 not supported",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_cast_struct_to_struct_nullability() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![Some(42), None, Some(19), None]));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, true)),
                int.clone() as ArrayRef,
            ),
        ]);

        // okay: nullable to nullable
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        cast(&struct_array, &to_type).expect("Cast nullable to nullable struct field should work");

        // error: nullable to non-nullable
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ]
            .into(),
        );
        cast(&struct_array, &to_type)
            .expect_err("Cast nullable to non-nullable struct field should fail");

        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![i32::MAX, 25, 1, 100]));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
        ]);

        // okay: non-nullable to non-nullable
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ]
            .into(),
        );
        cast(&struct_array, &to_type)
            .expect("Cast non-nullable to non-nullable struct field should work");

        // err: non-nullable to non-nullable but overflowing return null during casting
        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int8, false),
            ]
            .into(),
        );
        cast(&struct_array, &to_type).expect_err(
            "Cast non-nullable to non-nullable struct field returning null should fail",
        );
    }

    #[test]
    fn test_cast_struct_to_non_struct() {
        let boolean = Arc::new(BooleanArray::from(vec![true, false]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        )]);
        let to_type = DataType::Utf8;
        let result = cast(&struct_array, &to_type);
        assert_eq!(
            r#"Cast error: Casting from Struct("a": non-null Boolean) to Utf8 not supported"#,
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_cast_non_struct_to_struct() {
        let array = StringArray::from(vec!["a", "b"]);
        let to_type = DataType::Struct(vec![Field::new("a", DataType::Boolean, false)].into());
        let result = cast(&array, &to_type);
        assert_eq!(
            r#"Cast error: Casting from Utf8 to Struct("a": non-null Boolean) not supported"#,
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_cast_struct_with_different_field_order() {
        // Test slow path: fields are in different order
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let string = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "qux"]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Utf8, false)),
                string.clone() as ArrayRef,
            ),
        ]);

        // Target has fields in different order: c, a, b instead of a, b, c
        let to_type = DataType::Struct(
            vec![
                Field::new("c", DataType::Utf8, false),
                Field::new("a", DataType::Utf8, false), // Boolean to Utf8
                Field::new("b", DataType::Utf8, false), // Int32 to Utf8
            ]
            .into(),
        );

        let result = cast(&struct_array, &to_type).unwrap();
        let result_struct = result.as_struct();

        assert_eq!(result_struct.data_type(), &to_type);
        assert_eq!(result_struct.num_columns(), 3);

        // Verify field "c" (originally position 2, now position 0) remains Utf8
        let c_column = result_struct.column(0).as_string::<i32>();
        assert_eq!(
            c_column.into_iter().flatten().collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "qux"]
        );

        // Verify field "a" (originally position 0, now position 1) was cast from Boolean to Utf8
        let a_column = result_struct.column(1).as_string::<i32>();
        assert_eq!(
            a_column.into_iter().flatten().collect::<Vec<_>>(),
            vec!["false", "false", "true", "true"]
        );

        // Verify field "b" (originally position 1, now position 2) was cast from Int32 to Utf8
        let b_column = result_struct.column(2).as_string::<i32>();
        assert_eq!(
            b_column.into_iter().flatten().collect::<Vec<_>>(),
            vec!["42", "28", "19", "31"]
        );
    }

    #[test]
    fn test_cast_struct_with_missing_field() {
        // Test that casting fails when target has a field not present in source
        let boolean = Arc::new(BooleanArray::from(vec![false, true]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        )]);

        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false), // Field "b" doesn't exist in source
            ]
            .into(),
        );

        let result = cast(&struct_array, &to_type);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid argument error: Incorrect number of arrays for StructArray fields, expected 2 got 1"
        );
    }

    #[test]
    fn test_cast_struct_with_subset_of_fields() {
        // Test casting to a struct with fewer fields (selecting a subset)
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let string = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "qux"]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Utf8, false)),
                string.clone() as ArrayRef,
            ),
        ]);

        // Target has only fields "c" and "a", omitting "b"
        let to_type = DataType::Struct(
            vec![
                Field::new("c", DataType::Utf8, false),
                Field::new("a", DataType::Utf8, false),
            ]
            .into(),
        );

        let result = cast(&struct_array, &to_type).unwrap();
        let result_struct = result.as_struct();

        assert_eq!(result_struct.data_type(), &to_type);
        assert_eq!(result_struct.num_columns(), 2);

        // Verify field "c" remains Utf8
        let c_column = result_struct.column(0).as_string::<i32>();
        assert_eq!(
            c_column.into_iter().flatten().collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "qux"]
        );

        // Verify field "a" was cast from Boolean to Utf8
        let a_column = result_struct.column(1).as_string::<i32>();
        assert_eq!(
            a_column.into_iter().flatten().collect::<Vec<_>>(),
            vec!["false", "false", "true", "true"]
        );
    }

    #[test]
    fn test_can_cast_struct_rename_field() {
        // Test that can_cast_types returns false when target has a field not in source
        let from_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]
            .into(),
        );

        let to_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("c", DataType::Boolean, false), // Field "c" not in source
            ]
            .into(),
        );

        assert!(can_cast_types(&from_type, &to_type));
    }

    #[test]
    fn test_first_none() {
        let array = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            None,
            Some(vec![Some(1), Some(2)]),
        ])) as ArrayRef;
        let data_type =
            DataType::FixedSizeList(FieldRef::new(Field::new("item", DataType::Int64, true)), 2);
        let opt = CastOptions::default();
        let r = cast_with_options(&array, &data_type, &opt).unwrap();

        let fixed_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
            vec![None, Some(vec![Some(1), Some(2)])],
            2,
        )) as ArrayRef;
        assert_eq!(*fixed_array, *r);
    }

    #[test]
    fn test_first_last_none() {
        let array = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            None,
            Some(vec![Some(1), Some(2)]),
            None,
        ])) as ArrayRef;
        let data_type =
            DataType::FixedSizeList(FieldRef::new(Field::new("item", DataType::Int64, true)), 2);
        let opt = CastOptions::default();
        let r = cast_with_options(&array, &data_type, &opt).unwrap();

        let fixed_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
            vec![None, Some(vec![Some(1), Some(2)]), None],
            2,
        )) as ArrayRef;
        assert_eq!(*fixed_array, *r);
    }

    #[test]
    fn test_run_end_encoded_to_primitive() {
        // Create a RunEndEncoded array: [1, 1, 2, 2, 2, 3]
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;
        // Cast to Int64
        let cast_result = cast(&array_ref, &DataType::Int64).unwrap();
        // Verify the result is a RunArray with Int64 values
        let result_run_array = cast_result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            result_run_array.values(),
            &[1i64, 1i64, 2i64, 2i64, 2i64, 3i64]
        );
    }

    #[test]
    fn test_sliced_run_end_encoded_to_primitive() {
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let values = Int32Array::from(vec![1, 2, 3]);
        // [1, 1, 2, 2, 2, 3]
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let run_array = run_array.slice(3, 3); // [2, 2, 3]
        let array_ref = Arc::new(run_array) as ArrayRef;

        let cast_result = cast(&array_ref, &DataType::Int64).unwrap();
        let result_run_array = cast_result.as_primitive::<Int64Type>();
        assert_eq!(result_run_array.values(), &[2, 2, 3]);
    }

    #[test]
    fn test_run_end_encoded_to_string() {
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values = Int32Array::from(vec![10, 20, 30]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;

        // Cast to String
        let cast_result = cast(&array_ref, &DataType::Utf8).unwrap();

        // Verify the result is a RunArray with String values
        let result_array = cast_result.as_any().downcast_ref::<StringArray>().unwrap();
        // Check that values are correct
        assert_eq!(result_array.value(0), "10");
        assert_eq!(result_array.value(1), "10");
        assert_eq!(result_array.value(2), "20");
    }

    #[test]
    fn test_primitive_to_run_end_encoded() {
        // Create an Int32 array with repeated values: [1, 1, 2, 2, 2, 3]
        let source_array = Int32Array::from(vec![1, 1, 2, 2, 2, 3]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, Int32>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int32,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();

        // Verify the result is a RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check run structure: runs should end at positions [2, 5, 6]
        assert_eq!(result_run_array.run_ends().values(), &[2, 5, 6]);

        // Check values: should be [1, 2, 3]
        let values_array = result_run_array.values().as_primitive::<Int32Type>();
        assert_eq!(values_array.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_primitive_to_run_end_encoded_with_nulls() {
        let source_array = Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            None,
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            None,
            None,
            Some(4),
            Some(4),
            Some(5),
            Some(5),
            None,
            None,
        ]);
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int32,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();
        assert_eq!(
            result_run_array.run_ends().values(),
            &[2, 4, 6, 8, 10, 12, 14, 16]
        );
        assert_eq!(
            result_run_array
                .values()
                .as_primitive::<Int32Type>()
                .values(),
            &[1, 0, 2, 3, 0, 4, 5, 0]
        );
        assert_eq!(result_run_array.values().null_count(), 3);
    }

    #[test]
    fn test_primitive_to_run_end_encoded_with_nulls_consecutive() {
        let source_array = Int64Array::from(vec![
            Some(1),
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(4),
            Some(20),
            Some(500),
            Some(500),
            None,
            None,
        ]);
        let array_ref = Arc::new(source_array) as ArrayRef;
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int16,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int64,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int16Type>>()
            .unwrap();
        assert_eq!(
            result_run_array.run_ends().values(),
            &[2, 10, 11, 12, 14, 16]
        );
        assert_eq!(
            result_run_array
                .values()
                .as_primitive::<Int64Type>()
                .values(),
            &[1, 0, 4, 20, 500, 0]
        );
        assert_eq!(result_run_array.values().null_count(), 2);
    }

    #[test]
    fn test_string_to_run_end_encoded() {
        // Create a String array with repeated values: ["a", "a", "b", "c", "c"]
        let source_array = StringArray::from(vec!["a", "a", "b", "c", "c"]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, String>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();

        // Verify the result is a RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check run structure: runs should end at positions [2, 3, 5]
        assert_eq!(result_run_array.run_ends().values(), &[2, 3, 5]);

        // Check values: should be ["a", "b", "c"]
        let values_array = result_run_array.values().as_string::<i32>();
        assert_eq!(values_array.value(0), "a");
        assert_eq!(values_array.value(1), "b");
        assert_eq!(values_array.value(2), "c");
    }

    #[test]
    fn test_empty_array_to_run_end_encoded() {
        // Create an empty Int32 array
        let source_array = Int32Array::from(Vec::<i32>::new());
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, Int32>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int32,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int32,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();

        // Verify the result is an empty RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check that both run_ends and values are empty
        assert_eq!(result_run_array.run_ends().len(), 0);
        assert_eq!(result_run_array.values().len(), 0);
    }

    #[test]
    fn test_run_end_encoded_with_nulls() {
        // Create a RunEndEncoded array with nulls: [1, 1, null, 2, 2]
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values = Int32Array::from(vec![Some(1), None, Some(2)]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;

        // Cast to String
        let cast_result = cast(&array_ref, &DataType::Utf8).unwrap();

        // Verify the result preserves nulls
        let result_run_array = cast_result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_run_array.value(0), "1");
        assert!(result_run_array.is_null(2));
        assert_eq!(result_run_array.value(4), "2");
    }

    #[test]
    fn test_different_index_types() {
        // Test with Int16 index type
        let source_array = Int32Array::from(vec![1, 1, 2, 3, 3]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int16,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int32,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();
        assert_eq!(cast_result.data_type(), &target_type);

        // Verify the cast worked correctly: values are [1, 2, 3]
        // and run-ends are [2, 3, 5]
        let run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int16Type>>()
            .unwrap();
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(0), 1);
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(1), 2);
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(2), 3);
        assert_eq!(run_array.run_ends().values(), &[2i16, 3i16, 5i16]);

        // Test again with Int64 index type
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Int32,
                true,
            )),
        );
        let cast_result = cast(&array_ref, &target_type).unwrap();
        assert_eq!(cast_result.data_type(), &target_type);

        // Verify the cast worked correctly: values are [1, 2, 3]
        // and run-ends are [2, 3, 5]
        let run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int64Type>>()
            .unwrap();
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(0), 1);
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(1), 2);
        assert_eq!(run_array.values().as_primitive::<Int32Type>().value(2), 3);
        assert_eq!(run_array.run_ends().values(), &[2i64, 3i64, 5i64]);
    }

    #[test]
    fn test_unsupported_cast_to_run_end_encoded() {
        // Create a Struct array - complex nested type that might not be supported
        let field = Field::new("item", DataType::Int32, false);
        let struct_array = StructArray::from(vec![(
            Arc::new(field),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )]);
        let array_ref = Arc::new(struct_array) as ArrayRef;

        // This should fail because:
        // 1. The target type is not RunEndEncoded
        // 2. The target type is not supported for casting from StructArray
        let cast_result = cast(&array_ref, &DataType::FixedSizeBinary(10));

        // Expect this to fail
        assert!(cast_result.is_err());
    }

    /// Test casting RunEndEncoded<Int64, String> to RunEndEncoded<Int16, String> should fail
    #[test]
    fn test_cast_run_end_encoded_int64_to_int16_should_fail() {
        // Construct a valid REE array with Int64 run-ends
        let run_ends = Int64Array::from(vec![100_000, 400_000, 700_000]); // values too large for Int16
        let values = StringArray::from(vec!["a", "b", "c"]);

        let ree_array = RunArray::<Int64Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(ree_array) as ArrayRef;

        // Attempt to cast to RunEndEncoded<Int16, Utf8>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int16,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_options = CastOptions {
            safe: false, // This should make it fail instead of returning nulls
            format_options: FormatOptions::default(),
        };

        // This should fail due to run-end overflow
        let result: Result<Arc<dyn Array + 'static>, ArrowError> =
            cast_with_options(&array_ref, &target_type, &cast_options);

        let e = result.expect_err("Cast should have failed but succeeded");
        assert!(
            e.to_string()
                .contains("Cast error: Can't cast value 100000 to type Int16")
        );
    }

    #[test]
    fn test_cast_run_end_encoded_int64_to_int16_with_safe_should_fail_with_null_invalid_error() {
        // Construct a valid REE array with Int64 run-ends
        let run_ends = Int64Array::from(vec![100_000, 400_000, 700_000]); // values too large for Int16
        let values = StringArray::from(vec!["a", "b", "c"]);

        let ree_array = RunArray::<Int64Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(ree_array) as ArrayRef;

        // Attempt to cast to RunEndEncoded<Int16, Utf8>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int16,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };

        // This fails even though safe is true because the run_ends array has null values
        let result: Result<Arc<dyn Array + 'static>, ArrowError> =
            cast_with_options(&array_ref, &target_type, &cast_options);
        let e = result.expect_err("Cast should have failed but succeeded");
        assert!(
            e.to_string()
                .contains("Invalid argument error: Found null values in run_ends array. The run_ends array should not have null values.")
        );
    }

    /// Test casting RunEndEncoded<Int16, String> to RunEndEncoded<Int64, String> should succeed
    #[test]
    fn test_cast_run_end_encoded_int16_to_int64_should_succeed() {
        // Construct a valid REE array with Int16 run-ends
        let run_ends = Int16Array::from(vec![2, 5, 8]); // values that fit in Int16
        let values = StringArray::from(vec!["a", "b", "c"]);

        let ree_array = RunArray::<Int16Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(ree_array) as ArrayRef;

        // Attempt to cast to RunEndEncoded<Int64, Utf8> (upcast should succeed)
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };

        // This should succeed due to valid upcast
        let result: Result<Arc<dyn Array + 'static>, ArrowError> =
            cast_with_options(&array_ref, &target_type, &cast_options);

        let array_ref = result.expect("Cast should have succeeded but failed");
        // Downcast to RunArray<Int64Type>
        let run_array = array_ref
            .as_any()
            .downcast_ref::<RunArray<Int64Type>>()
            .unwrap();

        // Verify the cast worked correctly
        // Assert the values were cast correctly
        assert_eq!(run_array.run_ends().values(), &[2i64, 5i64, 8i64]);
        assert_eq!(run_array.values().as_string::<i32>().value(0), "a");
        assert_eq!(run_array.values().as_string::<i32>().value(1), "b");
        assert_eq!(run_array.values().as_string::<i32>().value(2), "c");
    }

    #[test]
    fn test_cast_run_end_encoded_dictionary_to_run_end_encoded() {
        // Construct a valid dictionary encoded array
        let values = StringArray::from_iter([Some("a"), Some("b"), Some("c")]);
        let keys = UInt64Array::from_iter(vec![1, 1, 1, 0, 0, 0, 2, 2, 2]);
        let array_ref = Arc::new(DictionaryArray::new(keys, Arc::new(values))) as ArrayRef;

        // Attempt to cast to RunEndEncoded<Int64, Utf8>
        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };

        // This should succeed
        let result = cast_with_options(&array_ref, &target_type, &cast_options)
            .expect("Cast should have succeeded but failed");

        // Verify the cast worked correctly
        // Assert the values were cast correctly
        let run_array = result
            .as_any()
            .downcast_ref::<RunArray<Int64Type>>()
            .unwrap();
        assert_eq!(run_array.values().as_string::<i32>().value(0), "b");
        assert_eq!(run_array.values().as_string::<i32>().value(1), "a");
        assert_eq!(run_array.values().as_string::<i32>().value(2), "c");

        // Verify the run-ends were cast correctly (run ends at 3, 6, 9)
        assert_eq!(run_array.run_ends().values(), &[3i64, 6i64, 9i64]);
    }

    #[test]
    fn test_cast_time32_second_to_int64() {
        let array = Time32SecondArray::from(vec![1000, 2000, 3000]);
        let array = Arc::new(array) as Arc<dyn Array>;
        let to_type = DataType::Int64;
        let cast_options = CastOptions::default();

        assert!(can_cast_types(array.data_type(), &to_type));

        let result = cast_with_options(&array, &to_type, &cast_options);
        assert!(
            result.is_ok(),
            "Failed to cast Time32(Second) to Int64: {:?}",
            result.err()
        );

        let cast_array = result.unwrap();
        let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(cast_array.value(0), 1000);
        assert_eq!(cast_array.value(1), 2000);
        assert_eq!(cast_array.value(2), 3000);
    }

    #[test]
    fn test_cast_time32_millisecond_to_int64() {
        let array = Time32MillisecondArray::from(vec![1000, 2000, 3000]);
        let array = Arc::new(array) as Arc<dyn Array>;
        let to_type = DataType::Int64;
        let cast_options = CastOptions::default();

        assert!(can_cast_types(array.data_type(), &to_type));

        let result = cast_with_options(&array, &to_type, &cast_options);
        assert!(
            result.is_ok(),
            "Failed to cast Time32(Millisecond) to Int64: {:?}",
            result.err()
        );

        let cast_array = result.unwrap();
        let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(cast_array.value(0), 1000);
        assert_eq!(cast_array.value(1), 2000);
        assert_eq!(cast_array.value(2), 3000);
    }

    #[test]
    fn test_cast_time32_millisecond_to_time64_nanosecond() {
        let array =
            Time32MillisecondArray::from(vec![Some(1_000), Some(2_000), None, Some(43_200_000)]);
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(c.value(0), 1_000_000_000);
        assert_eq!(c.value(1), 2_000_000_000);
        assert!(c.is_null(2));
        assert_eq!(c.value(3), 43_200_000_000_000);
    }

    #[test]
    fn test_cast_time32_millisecond_to_time64_microsecond() {
        let array =
            Time32MillisecondArray::from(vec![Some(1_000), Some(2_000), None, Some(43_200_000)]);
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(c.value(0), 1_000_000);
        assert_eq!(c.value(1), 2_000_000);
        assert!(c.is_null(2));
        assert_eq!(c.value(3), 43_200_000_000);
    }

    #[test]
    fn test_cast_time32_second_to_time64_nanosecond() {
        let array = Time32SecondArray::from(vec![Some(1), Some(60), None, Some(43_200)]);
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_primitive::<Time64NanosecondType>();
        assert_eq!(c.value(0), 1_000_000_000);
        assert_eq!(c.value(1), 60_000_000_000);
        assert!(c.is_null(2));
        assert_eq!(c.value(3), 43_200_000_000_000);
    }

    #[test]
    fn test_cast_time32_second_to_time64_microsecond() {
        let array = Time32SecondArray::from(vec![Some(1), Some(60), None, Some(43_200)]);
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_primitive::<Time64MicrosecondType>();
        assert_eq!(c.value(0), 1_000_000);
        assert_eq!(c.value(1), 60_000_000);
        assert!(c.is_null(2));
        assert_eq!(c.value(3), 43_200_000_000);
    }

    #[test]
    fn test_cast_time32_second_to_time32_millisecond_overflow() {
        let array = Time32SecondArray::from(vec![i32::MAX]);

        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_primitive::<Time32MillisecondType>();
        assert!(c.is_null(0));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let err = cast_with_options(&array, &DataType::Time32(TimeUnit::Millisecond), &options)
            .unwrap_err();
        assert!(err.to_string().contains("Overflow"), "{err}");
    }

    fn assert_temporal_overflow_is_safe(array: &dyn Array, to_type: &DataType) {
        let result = cast(array, to_type).unwrap();
        assert_eq!(result.null_count(), array.len());

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        assert!(cast_with_options(array, to_type, &options).is_err());
    }

    #[test]
    fn test_cast_time64_overflow() {
        let microseconds = Time64MicrosecondArray::from(vec![i64::MIN, i64::MAX]);
        for to_type in [
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Nanosecond),
        ] {
            assert_temporal_overflow_is_safe(&microseconds, &to_type);
        }

        let nanoseconds = Time64NanosecondArray::from(vec![i64::MIN, i64::MAX]);
        for to_type in [
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
        ] {
            assert_temporal_overflow_is_safe(&nanoseconds, &to_type);
        }
    }

    #[test]
    fn test_cast_date64_to_timestamp_overflow() {
        let array = Date64Array::from(vec![i64::MIN, i64::MAX]);
        for to_type in [
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert_temporal_overflow_is_safe(&array, &to_type);
        }
    }

    #[test]
    fn test_cast_time64_to_time32_boundaries() {
        fn check<FROM, TO>(divisor: i64)
        where
            FROM: ArrowPrimitiveType<Native = i64>,
            TO: ArrowPrimitiveType<Native = i32>,
        {
            // Division truncates towards zero before the range check. Values
            // slightly outside the scaled i32 bounds can still be representable.
            let min = <i64 as From<i32>>::from(i32::MIN) * divisor - (divisor - 1);
            let max = <i64 as From<i32>>::from(i32::MAX) * divisor + (divisor - 1);
            let array = PrimitiveArray::<FROM>::new(
                vec![i64::MAX, min - 1, min, -divisor + 1, i64::MAX, max, max + 1].into(),
                Some(vec![true, true, true, true, false, true, true].into()),
            )
            .slice(1, 6);
            let expected = PrimitiveArray::<TO>::from_iter([
                None,
                Some(i32::MIN),
                Some(0),
                None,
                Some(i32::MAX),
                None,
            ]);
            let result = cast(&array, &TO::DATA_TYPE).unwrap();
            assert_eq!(result.as_primitive::<TO>(), &expected);

            let options = CastOptions {
                safe: false,
                ..Default::default()
            };
            assert!(cast_with_options(&array, &TO::DATA_TYPE, &options).is_err());
            // The invalid physical value under the null must not cause an error.
            let result = cast_with_options(&array.slice(1, 4), &TO::DATA_TYPE, &options).unwrap();
            assert_eq!(result.as_primitive::<TO>(), &expected.slice(1, 4));
        }
        check::<Time64MicrosecondType, Time32SecondType>(MICROSECONDS);
        check::<Time64MicrosecondType, Time32MillisecondType>(MICROSECONDS / MILLISECONDS);
        check::<Time64NanosecondType, Time32SecondType>(NANOSECONDS);
        check::<Time64NanosecondType, Time32MillisecondType>(NANOSECONDS / MILLISECONDS);
    }

    #[test]
    fn test_cast_temporal_scaling_boundaries() {
        fn check<FROM, TO>(multiplier: i64)
        where
            FROM: ArrowPrimitiveType<Native = i64>,
            TO: ArrowPrimitiveType<Native = i64>,
        {
            let min = i64::MIN / multiplier;
            let max = i64::MAX / multiplier;
            let array = PrimitiveArray::<FROM>::new(
                vec![i64::MAX, min - 1, min, -1, i64::MAX, max, max + 1].into(),
                Some(vec![true, true, true, true, false, true, true].into()),
            )
            .slice(1, 6);
            let expected = PrimitiveArray::<TO>::from_iter([
                None,
                Some(min * multiplier),
                Some(-multiplier),
                None,
                Some(max * multiplier),
                None,
            ]);
            let result = cast(&array, &TO::DATA_TYPE).unwrap();
            assert_eq!(result.as_primitive::<TO>(), &expected);
            let options = CastOptions {
                safe: false,
                ..Default::default()
            };
            assert!(cast_with_options(&array, &TO::DATA_TYPE, &options).is_err());
            let result = cast_with_options(&array.slice(1, 4), &TO::DATA_TYPE, &options).unwrap();
            assert_eq!(result.as_primitive::<TO>(), &expected.slice(1, 4));
        }
        check::<Time64MicrosecondType, Time64NanosecondType>(NANOSECONDS / MICROSECONDS);
        check::<Date64Type, TimestampMicrosecondType>(MICROSECONDS / MILLISECONDS);
        check::<Date64Type, TimestampNanosecondType>(NANOSECONDS / MILLISECONDS);
    }

    #[test]
    fn test_cast_string_to_time32_second_to_int64() {
        // Mimic: select arrow_cast('03:12:44'::time, 'Time32(Second)')::bigint;
        // raised in https://github.com/apache/datafusion/issues/19036
        let array = StringArray::from(vec!["03:12:44"]);
        let array = Arc::new(array) as Arc<dyn Array>;
        let cast_options = CastOptions::default();

        // 1. Cast String to Time32(Second)
        let time32_type = DataType::Time32(TimeUnit::Second);
        let time32_array = cast_with_options(&array, &time32_type, &cast_options).unwrap();

        // 2. Cast Time32(Second) to Int64
        let int64_type = DataType::Int64;
        assert!(can_cast_types(time32_array.data_type(), &int64_type));

        let result = cast_with_options(&time32_array, &int64_type, &cast_options);

        assert!(
            result.is_ok(),
            "Failed to cast Time32(Second) to Int64: {:?}",
            result.err()
        );

        let cast_array = result.unwrap();
        let cast_array = cast_array.as_any().downcast_ref::<Int64Array>().unwrap();

        // 03:12:44 = 3*3600 + 12*60 + 44 = 10800 + 720 + 44 = 11564
        assert_eq!(cast_array.value(0), 11564);
    }
    #[test]
    fn test_string_dicts_to_binary_view() {
        let expected = BinaryViewArray::from_iter(vec![
            VIEW_TEST_DATA[1],
            VIEW_TEST_DATA[0],
            None,
            VIEW_TEST_DATA[3],
            None,
            VIEW_TEST_DATA[1],
            VIEW_TEST_DATA[4],
        ]);

        let values_arrays: [ArrayRef; _] = [
            Arc::new(StringArray::from_iter(VIEW_TEST_DATA)),
            Arc::new(StringViewArray::from_iter(VIEW_TEST_DATA)),
            Arc::new(LargeStringArray::from_iter(VIEW_TEST_DATA)),
        ];
        for values in values_arrays {
            let keys =
                Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
            let string_dict_array = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();

            let casted = cast(&string_dict_array, &DataType::BinaryView).unwrap();
            assert_eq!(casted.as_ref(), &expected);
        }
    }

    #[test]
    fn test_binary_dicts_to_string_view() {
        let expected = StringViewArray::from_iter(vec![
            VIEW_TEST_DATA[1],
            VIEW_TEST_DATA[0],
            None,
            VIEW_TEST_DATA[3],
            None,
            VIEW_TEST_DATA[1],
            VIEW_TEST_DATA[4],
        ]);

        let values_arrays: [ArrayRef; _] = [
            Arc::new(BinaryArray::from_iter(VIEW_TEST_DATA)),
            Arc::new(BinaryViewArray::from_iter(VIEW_TEST_DATA)),
            Arc::new(LargeBinaryArray::from_iter(VIEW_TEST_DATA)),
        ];
        for values in values_arrays {
            let keys =
                Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
            let string_dict_array = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();

            let casted = cast(&string_dict_array, &DataType::Utf8View).unwrap();
            assert_eq!(casted.as_ref(), &expected);
        }
    }

    #[test]
    fn test_cast_between_sliced_run_end_encoded() {
        let run_ends = Int16Array::from(vec![2, 5, 8]);
        let values = StringArray::from(vec!["a", "b", "c"]);

        let ree_array = RunArray::<Int16Type>::try_new(&run_ends, &values).unwrap();
        let ree_array = ree_array.slice(1, 2);
        let array_ref = Arc::new(ree_array) as ArrayRef;

        let target_type = DataType::RunEndEncoded(
            Arc::new(Field::new(
                Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                Field::REE_VALUES_FIELD_DEFAULT_NAME,
                DataType::Utf8,
                true,
            )),
        );
        let cast_options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };

        let result = cast_with_options(&array_ref, &target_type, &cast_options).unwrap();
        let run_array = result.as_run::<Int64Type>();
        let run_array = run_array.downcast::<StringArray>().unwrap();

        let expected = vec!["a", "b"];
        let actual = run_array.into_iter().flatten().collect::<Vec<_>>();

        assert_eq!(expected, actual);
    }
}

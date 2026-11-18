// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
            | Duration(_)
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
/// See [`cast_with_options`] for more information.
///
/// # Errors
///
/// See [cast_with_options](cast_with_options#errors) for error conditions.
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to_type, &CastOptions::default())
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
/// # Errors
///
/// Returns [`ArrowError::InvalidArgumentError`] when the conversion between the
/// source and target types is unsupported, regardless of [`CastOptions::safe`].
/// Use [`can_cast_types`] to check whether a conversion is supported.
///
/// [`ArrowError::CastError`] reports failures to convert input values, such as
/// numeric overflow or invalid strings. See [`CastOptions::safe`] for whether
/// failed conversions produce nulls or errors.
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Casting from type {from_type:?} to run end encoded type {to_type:?} not supported",
                ))),
            }
        }
        (Union(_, _), _) => union_extract_by_type(
            array.as_any().downcast_ref::<UnionArray>().unwrap(),
            to_type,
            cast_options,
        ),
        (_, Union(_, _)) => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Casting from type {from_type} to dictionary type {to_type} not supported",
            ))),
        },
        // Casting between lists of same types (cast inner values)
        (List(_), List(to)) => cast_list_values::<i32>(array, to, cast_options),
        (LargeList(_), LargeList(to)) => cast_list_values::<i64>(array, to, cast_options),
        (FixedSizeList(_, size_from), FixedSizeList(list_to, size_to)) => {
            if size_from != size_to {
                return Err(ArrowError::InvalidArgumentError(
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
            dt => Err(ArrowError::InvalidArgumentError(format!(
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
        (Struct(_), _) => Err(ArrowError::InvalidArgumentError(format!(
            "Casting from {from_type} to {to_type} not supported"
        ))),
        (_, Struct(_)) => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            Duration(TimeUnit::Second) => {
                cast_string_to_duration::<DurationSecondType, i32>(array, cast_options)
            }
            Duration(TimeUnit::Millisecond) => {
                cast_string_to_duration::<DurationMillisecondType, i32>(array, cast_options)
            }
            Duration(TimeUnit::Microsecond) => {
                cast_string_to_duration::<DurationMicrosecondType, i32>(array, cast_options)
            }
            Duration(TimeUnit::Nanosecond) => {
                cast_string_to_duration::<DurationNanosecondType, i32>(array, cast_options)
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            Duration(TimeUnit::Second) => {
                cast_view_to_duration::<DurationSecondType>(array, cast_options)
            }
            Duration(TimeUnit::Millisecond) => {
                cast_view_to_duration::<DurationMillisecondType>(array, cast_options)
            }
            Duration(TimeUnit::Microsecond) => {
                cast_view_to_duration::<DurationMicrosecondType>(array, cast_options)
            }
            Duration(TimeUnit::Nanosecond) => {
                cast_view_to_duration::<DurationNanosecondType>(array, cast_options)
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            Duration(TimeUnit::Second) => {
                cast_string_to_duration::<DurationSecondType, i64>(array, cast_options)
            }
            Duration(TimeUnit::Millisecond) => {
                cast_string_to_duration::<DurationMillisecondType, i64>(array, cast_options)
            }
            Duration(TimeUnit::Microsecond) => {
                cast_string_to_duration::<DurationMicrosecondType, i64>(array, cast_options)
            }
            Duration(TimeUnit::Nanosecond) => {
                cast_string_to_duration::<DurationNanosecondType, i64>(array, cast_options)
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Casting from {from_type} to {to_type} not supported",
            ))),
        },
        (FixedSizeBinary(size), _) => match to_type {
            Binary => cast_fixed_size_binary_to_binary::<i32>(array, *size),
            LargeBinary => cast_fixed_size_binary_to_binary::<i64>(array, *size),
            BinaryView => cast_fixed_size_binary_to_binary_view(array, *size),
            _ => Err(ArrowError::InvalidArgumentError(format!(
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
        (BinaryView, _) => Err(ArrowError::InvalidArgumentError(format!(
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
                array.unary_opt::<_, Date32Type>(|x| {
                    i32::try_from(x.div_euclid(MILLISECONDS_IN_DAY)).ok()
                })
            } else {
                array.try_unary::<_, Date32Type, _>(|x| {
                    i32::try_from(x.div_euclid(MILLISECONDS_IN_DAY)).map_err(|_| {
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

        // reinterpret Int64 without going through ArrayData
        (Int64, Timestamp(unit, tz)) => Ok(make_timestamp_array(
            array.as_primitive(),
            *unit,
            tz.clone(),
        )),
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
        (Timestamp(TimeUnit::Second, _), Date64) => {
            timestamp_to_date64(array.as_primitive::<TimestampSecondType>(), cast_options)
        }
        (Timestamp(TimeUnit::Millisecond, _), Date64) => timestamp_to_date64(
            array.as_primitive::<TimestampMillisecondType>(),
            cast_options,
        ),
        (Timestamp(TimeUnit::Microsecond, _), Date64) => timestamp_to_date64(
            array.as_primitive::<TimestampMicrosecondType>(),
            cast_options,
        ),
        (Timestamp(TimeUnit::Nanosecond, _), Date64) => timestamp_to_date64(
            array.as_primitive::<TimestampNanosecondType>(),
            cast_options,
        ),
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

        // reinterpret Int64 without going through ArrayData
        (Int64, Duration(unit)) => Ok(make_duration_array(array.as_primitive(), *unit)),
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
        (_, _) => Err(ArrowError::InvalidArgumentError(format!(
            "Casting from {from_type} to {to_type} not supported",
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

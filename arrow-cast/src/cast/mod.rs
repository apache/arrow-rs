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

mod decimal;
mod dictionary;
mod list;
mod map;
mod string;
use crate::cast::decimal::*;
use crate::cast::dictionary::*;
use crate::cast::list::*;
use crate::cast::map::*;
use crate::cast::string::*;

use arrow_buffer::IntervalMonthDayNano;
use arrow_data::ByteView;
use chrono::{NaiveTime, Offset, TimeZone, Utc};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::display::{ArrayFormatter, FormatOptions};
use crate::parse::{
    parse_interval_day_time, parse_interval_month_day_nano, parse_interval_year_month,
    string_to_datetime, Parser,
};
use arrow_array::{builder::*, cast::*, temporal_conversions::*, timezone::Tz, types::*, *};
use arrow_buffer::{i256, ArrowNativeType, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_data::ArrayData;
use arrow_schema::*;
use arrow_select::take::take;
use num::cast::AsPrimitive;
use num::{NumCast, ToPrimitive};

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
        (
            Null,
            Boolean
            | Int8
            | UInt8
            | Int16
            | UInt16
            | Int32
            | UInt32
            | Float32
            | Date32
            | Time32(_)
            | Int64
            | UInt64
            | Float64
            | Date64
            | Timestamp(_, _)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | FixedSizeBinary(_)
            | Binary
            | Utf8
            | LargeBinary
            | LargeUtf8
            | BinaryView
            | Utf8View
            | List(_)
            | LargeList(_)
            | FixedSizeList(_, _)
            | Struct(_)
            | Map(_, _)
            | Dictionary(_, _),
        ) => true,
        // Dictionary/List conditions should be put in front of others
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),
        (List(list_from) | LargeList(list_from), List(list_to) | LargeList(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(list_from) | LargeList(list_from), Utf8 | LargeUtf8) => {
            can_cast_types(list_from.data_type(), to_type)
        }
        (List(list_from) | LargeList(list_from), FixedSizeList(list_to, _)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(_), _) => false,
        (FixedSizeList(list_from,_), List(list_to)) |
        (FixedSizeList(list_from,_), LargeList(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (FixedSizeList(inner, size), FixedSizeList(inner_to, size_to)) if size == size_to => {
            can_cast_types(inner.data_type(), inner_to.data_type())
        }
        (_, List(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (_, LargeList(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (_, FixedSizeList(list_to,size)) if *size == 1 => {
            can_cast_types(from_type, list_to.data_type())},
        (FixedSizeList(list_from,size), _) if *size == 1 => {
            can_cast_types(list_from.data_type(), to_type)},
        (Map(from_entries,ordered_from), Map(to_entries, ordered_to)) if ordered_from == ordered_to =>
            match (key_field(from_entries), key_field(to_entries), value_field(from_entries), value_field(to_entries)) {
                (Some(from_key), Some(to_key), Some(from_value), Some(to_value)) =>
                    can_cast_types(from_key.data_type(), to_key.data_type()) && can_cast_types(from_value.data_type(), to_value.data_type()),
                _ => false
            },
        // cast one decimal type to another decimal type
        (Decimal128(_, _), Decimal128(_, _)) => true,
        (Decimal256(_, _), Decimal256(_, _)) => true,
        (Decimal128(_, _), Decimal256(_, _)) => true,
        (Decimal256(_, _), Decimal128(_, _)) => true,
        // unsigned integer to decimal
        (UInt8 | UInt16 | UInt32 | UInt64, Decimal128(_, _)) |
        (UInt8 | UInt16 | UInt32 | UInt64, Decimal256(_, _)) |
        // signed numeric to decimal
        (Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64, Decimal128(_, _)) |
        (Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64, Decimal256(_, _)) |
        // decimal to unsigned numeric
        (Decimal128(_, _) | Decimal256(_, _), UInt8 | UInt16 | UInt32 | UInt64) |
        // decimal to signed numeric
        (Decimal128(_, _) | Decimal256(_, _), Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64) => true,
        // decimal to string
        (Decimal128(_, _) | Decimal256(_, _), Utf8View | Utf8 | LargeUtf8) => true,
        // string to decimal
        (Utf8View | Utf8 | LargeUtf8, Decimal128(_, _) | Decimal256(_, _)) => true,
        (Struct(from_fields), Struct(to_fields)) => {
            from_fields.len() == to_fields.len() &&
                from_fields.iter().zip(to_fields.iter()).all(|(f1, f2)| {
                    // Assume that nullability between two structs are compatible, if not,
                    // cast kernel will return error.
                    can_cast_types(f1.data_type(), f2.data_type())
                })
        }
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (_, Boolean) => {
            DataType::is_integer(from_type)
                || DataType::is_floating(from_type)
                || from_type == &Utf8View
                || from_type == &Utf8
                || from_type == &LargeUtf8
        }
        (Boolean, _) => {
            DataType::is_integer(to_type)
                || DataType::is_floating(to_type)
                || to_type == &Utf8View
                || to_type == &Utf8
                || to_type == &LargeUtf8
        }

        (Binary, LargeBinary | Utf8 | LargeUtf8 | FixedSizeBinary(_) | BinaryView | Utf8View ) => true,
        (LargeBinary, Binary | Utf8 | LargeUtf8 | FixedSizeBinary(_) | BinaryView | Utf8View ) => true,
        (FixedSizeBinary(_), Binary | LargeBinary) => true,
        (
            Utf8 | LargeUtf8 | Utf8View,
            Binary
            | LargeBinary
            | Utf8
            | LargeUtf8
            | Date32
            | Date64
            | Time32(Second)
            | Time32(Millisecond)
            | Time64(Microsecond)
            | Time64(Nanosecond)
            | Timestamp(Second, _)
            | Timestamp(Millisecond, _)
            | Timestamp(Microsecond, _)
            | Timestamp(Nanosecond, _)
            | Interval(_)
            | BinaryView,
        ) => true,
        (Utf8 | LargeUtf8, Utf8View) => true,
        (BinaryView, Binary | LargeBinary | Utf8 | LargeUtf8 | Utf8View) => true,
        (Utf8View | Utf8 | LargeUtf8, _) => to_type.is_numeric() && to_type != &Float16,
        (_, Utf8 | LargeUtf8) => from_type.is_primitive(),
        (_, Utf8View) => from_type.is_numeric(),

        (_, Binary | LargeBinary) => from_type.is_integer(),

        // start numeric casts
        (
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float16 | Float32 | Float64,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float16 | Float32 | Float64,
        ) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32 | Date64 | Time32(_)) => true,
        (Date32, Int32 | Int64) => true,
        (Time32(_), Int32) => true,
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
            | Time32(Second)
            | Time32(Millisecond)
            | Time64(Microsecond)
            | Time64(Nanosecond),
        ) => true,
        (_, Duration(_)) if from_type.is_numeric() => true,
        (Duration(_), _) if to_type.is_numeric() => true,
        (Duration(_), Duration(_)) => true,
        (Interval(from_type), Int64) => {
            match from_type {
                YearMonth => true,
                DayTime => true,
                MonthDayNano => false, // Native type is i128
            }
        }
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

fn cast_integer_to_decimal<
    T: ArrowPrimitiveType,
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M,
>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    base: M,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<M>,
    M: ArrowNativeTypeOp,
{
    let scale_factor = base.pow_checked(scale.unsigned_abs() as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}({}, {}). The scale causes overflow.",
            D::PREFIX,
            precision,
            scale,
        ))
    })?;

    let array = if scale < 0 {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_()
                    .div_checked(scale_factor)
                    .ok()
                    .and_then(|v| (D::is_valid_decimal_precision(v, precision)).then_some(v))
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .div_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    } else {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_()
                    .mul_checked(scale_factor)
                    .ok()
                    .and_then(|v| (D::is_valid_decimal_precision(v, precision)).then_some(v))
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .mul_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(precision, scale)?))
}

/// Cast the array from interval year month to month day nano
fn cast_interval_year_month_to_interval_month_day_nano(
    array: &dyn Array,
    _cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalYearMonthType>();

    Ok(Arc::new(array.unary::<_, IntervalMonthDayNanoType>(|v| {
        let months = IntervalYearMonthType::to_months(v);
        IntervalMonthDayNanoType::make_value(months, 0, 0)
    })))
}

/// Cast the array from interval day time to month day nano
fn cast_interval_day_time_to_interval_month_day_nano(
    array: &dyn Array,
    _cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalDayTimeType>();
    let mul = 1_000_000;

    Ok(Arc::new(array.unary::<_, IntervalMonthDayNanoType>(|v| {
        let (days, ms) = IntervalDayTimeType::to_parts(v);
        IntervalMonthDayNanoType::make_value(0, days, ms as i64 * mul)
    })))
}

/// Cast the array from interval to duration
fn cast_month_day_nano_to_duration<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<IntervalMonthDayNanoType>();
    let scale = match D::DATA_TYPE {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array.iter().map(|v| {
            v.and_then(|v| (v.days == 0 && v.months == 0).then_some(v.nanoseconds / scale))
        });
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| match v.days == 0 && v.months == 0 {
                    true => Ok((v.nanoseconds) / scale),
                    _ => Err(ArrowError::ComputeError(
                        "Cannot convert interval containing non-zero months or days to duration"
                            .to_string(),
                    )),
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(vec.iter())
        }))
    }
}

/// Cast the array from duration and interval
fn cast_duration_to_interval<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<D>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast duration to DurationArray of expected type"
                    .to_string(),
            )
        })?;

    let scale = match array.data_type() {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array.iter().map(|v| {
            v.and_then(|v| {
                v.checked_mul(scale)
                    .map(|v| IntervalMonthDayNano::new(0, 0, v))
            })
        });
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| {
                    if let Ok(v) = v.mul_checked(scale) {
                        Ok(IntervalMonthDayNano::new(0, 0, v))
                    } else {
                        Err(ArrowError::ComputeError(format!(
                            "Cannot cast to {:?}. Overflowing on {:?}",
                            IntervalMonthDayNanoType::DATA_TYPE,
                            v
                        )))
                    }
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(vec.iter())
        }))
    }
}

/// Cast the primitive array using [`PrimitiveArray::reinterpret_cast`]
fn cast_reinterpret_arrays<I: ArrowPrimitiveType, O: ArrowPrimitiveType<Native = I::Native>>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(array.as_primitive::<I>().reinterpret_cast::<O>()))
}

fn make_timestamp_array(
    array: &PrimitiveArray<Int64Type>,
    unit: TimeUnit,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(
            array
                .reinterpret_cast::<TimestampSecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Millisecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMillisecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Microsecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Nanosecond => Arc::new(
            array
                .reinterpret_cast::<TimestampNanosecondType>()
                .with_timezone_opt(tz),
        ),
    }
}

fn make_duration_array(array: &PrimitiveArray<Int64Type>, unit: TimeUnit) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(array.reinterpret_cast::<DurationSecondType>()),
        TimeUnit::Millisecond => Arc::new(array.reinterpret_cast::<DurationMillisecondType>()),
        TimeUnit::Microsecond => Arc::new(array.reinterpret_cast::<DurationMicrosecondType>()),
        TimeUnit::Nanosecond => Arc::new(array.reinterpret_cast::<DurationNanosecondType>()),
    }
}

fn as_time_res_with_timezone<T: ArrowPrimitiveType>(
    v: i64,
    tz: Option<Tz>,
) -> Result<NaiveTime, ArrowError> {
    let time = match tz {
        Some(tz) => as_datetime_with_timezone::<T>(v, tz).map(|d| d.time()),
        None => as_datetime::<T>(v).map(|d| d.time()),
    };

    time.ok_or_else(|| {
        ArrowError::CastError(format!(
            "Failed to create naive time with {} {}",
            std::any::type_name::<T>(),
            v
        ))
    })
}

fn timestamp_to_date32<T: ArrowTimestampType>(
    array: &PrimitiveArray<T>,
) -> Result<ArrayRef, ArrowError> {
    let err = |x: i64| {
        ArrowError::CastError(format!(
            "Cannot convert {} {x} to datetime",
            std::any::type_name::<T>()
        ))
    };

    let array: Date32Array = match array.timezone() {
        Some(tz) => {
            let tz: Tz = tz.parse()?;
            array.try_unary(|x| {
                as_datetime_with_timezone::<T>(x, tz)
                    .ok_or_else(|| err(x))
                    .map(|d| Date32Type::from_naive_date(d.date_naive()))
            })?
        }
        None => array.try_unary(|x| {
            as_datetime::<T>(x)
                .ok_or_else(|| err(x))
                .map(|d| Date32Type::from_naive_date(d.date()))
        })?,
    };
    Ok(Arc::new(array))
}

/// Try to cast `array` to `to_type` if possible.
///
/// Returns a new Array with type `to_type` if possible.
///
/// Accepts [`CastOptions`] to specify cast behavior. See also [`cast()`].
///
/// # Behavior
/// * `Boolean` to `Utf8`: `true` => '1', `false` => `0`
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
/// * `Time32 and `Time64`: precision lost when going to higher interval
/// * `Timestamp` and `Date{32|64}`: precision lost when going to higher interval
/// * Temporal to/from backing Primitive: zero-copy with data type change
/// * `Float32/Float64` to `Decimal(precision, scale)` rounds to the `scale` decimals
///   (i.e. casting `6.4999` to `Decimal(10, 1)` becomes `6.5`).
///
/// Unsupported Casts (check with `can_cast_types` before calling):
/// * To or from `StructArray`
/// * `List` to `Primitive`
/// * `Interval` and `Duration`
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
        (
            Null,
            Boolean
            | Int8
            | UInt8
            | Int16
            | UInt16
            | Int32
            | UInt32
            | Float32
            | Date32
            | Time32(_)
            | Int64
            | UInt64
            | Float64
            | Date64
            | Timestamp(_, _)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | FixedSizeBinary(_)
            | Binary
            | Utf8
            | LargeBinary
            | LargeUtf8
            | BinaryView
            | Utf8View
            | List(_)
            | LargeList(_)
            | FixedSizeList(_, _)
            | Struct(_)
            | Map(_, _)
            | Dictionary(_, _),
        ) => Ok(new_null_array(to_type, array.len())),
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
                "Casting from dictionary type {from_type:?} to {to_type:?} not supported",
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
                "Casting from type {from_type:?} to dictionary type {to_type:?} not supported",
            ))),
        },
        (List(_), List(to)) => cast_list_values::<i32>(array, to, cast_options),
        (LargeList(_), LargeList(to)) => cast_list_values::<i64>(array, to, cast_options),
        (List(_), LargeList(list_to)) => cast_list::<i32, i64>(array, list_to, cast_options),
        (LargeList(_), List(list_to)) => cast_list::<i64, i32>(array, list_to, cast_options),
        (List(_), FixedSizeList(field, size)) => {
            let array = array.as_list::<i32>();
            cast_list_to_fixed_size_list::<i32>(array, field, *size, cast_options)
        }
        (LargeList(_), FixedSizeList(field, size)) => {
            let array = array.as_list::<i64>();
            cast_list_to_fixed_size_list::<i64>(array, field, *size, cast_options)
        }
        (List(_) | LargeList(_), _) => match to_type {
            Utf8 => value_to_string::<i32>(array, cast_options),
            LargeUtf8 => value_to_string::<i64>(array, cast_options),
            _ => Err(ArrowError::CastError(
                "Cannot cast list to non-list data types".to_string(),
            )),
        },
        (FixedSizeList(list_from, size), List(list_to)) => {
            if list_to.data_type() != list_from.data_type() {
                // To transform inner type, can first cast to FSL with new inner type.
                let fsl_to = DataType::FixedSizeList(list_to.clone(), *size);
                let array = cast_with_options(array, &fsl_to, cast_options)?;
                cast_fixed_size_list_to_list::<i32>(array.as_ref())
            } else {
                cast_fixed_size_list_to_list::<i32>(array)
            }
        }
        (FixedSizeList(list_from, size), LargeList(list_to)) => {
            if list_to.data_type() != list_from.data_type() {
                // To transform inner type, can first cast to FSL with new inner type.
                let fsl_to = DataType::FixedSizeList(list_to.clone(), *size);
                let array = cast_with_options(array, &fsl_to, cast_options)?;
                cast_fixed_size_list_to_list::<i64>(array.as_ref())
            } else {
                cast_fixed_size_list_to_list::<i64>(array)
            }
        }
        (FixedSizeList(_, size_from), FixedSizeList(list_to, size_to)) => {
            if size_from != size_to {
                return Err(ArrowError::CastError(
                    "cannot cast fixed-size-list to fixed-size-list with different size".into(),
                ));
            }
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let values = cast_with_options(array.values(), list_to.data_type(), cast_options)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                list_to.clone(),
                *size_from,
                values,
                array.nulls().cloned(),
            )?))
        }
        (_, List(ref to)) => cast_values_to_list::<i32>(array, to, cast_options),
        (_, LargeList(ref to)) => cast_values_to_list::<i64>(array, to, cast_options),
        (_, FixedSizeList(ref to, size)) if *size == 1 => {
            cast_values_to_fixed_size_list(array, to, *size, cast_options)
        }
        (FixedSizeList(_, size), _) if *size == 1 => {
            cast_single_element_fixed_size_list_to_values(array, to_type, cast_options)
        }
        (Map(_, ordered1), Map(_, ordered2)) if ordered1 == ordered2 => {
            cast_map_values(array.as_map(), to_type, cast_options, ordered1.to_owned())
        }
        // Decimal to decimal, same width
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
        (Decimal128(_, scale), _) if !to_type.is_temporal() => {
            cast_from_decimal::<Decimal128Type, _>(
                array,
                10_i128,
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
                i256::from_i128(10_i128),
                scale,
                from_type,
                to_type,
                |x: i256| x.to_f64().unwrap(),
                cast_options,
            )
        }
        // Non-decimal to decimal
        (_, Decimal128(precision, scale)) if !from_type.is_temporal() => {
            cast_to_decimal::<Decimal128Type, _>(
                array,
                10_i128,
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
                i256::from_i128(10_i128),
                precision,
                scale,
                from_type,
                to_type,
                cast_options,
            )
        }
        (Struct(_), Struct(to_fields)) => {
            let array = array.as_struct();
            let fields = array
                .columns()
                .iter()
                .zip(to_fields.iter())
                .map(|(l, field)| cast_with_options(l, field.data_type(), cast_options))
                .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
            let array = StructArray::try_new(to_fields.clone(), fields, array.nulls().cloned())?;
            Ok(Arc::new(array) as ArrayRef)
        }
        (Struct(_), _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type:?} to {to_type:?} not supported"
        ))),
        (_, Struct(_)) => Err(ArrowError::CastError(format!(
            "Casting from {from_type:?} to {to_type:?} not supported"
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
                "Casting from {from_type:?} to {to_type:?} not supported",
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
            Utf8View => value_to_string_view(array, cast_options),
            Utf8 => value_to_string::<i32>(array, cast_options),
            LargeUtf8 => value_to_string::<i64>(array, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
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
            Timestamp(TimeUnit::Second, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampSecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampMillisecondType,
            >(array, to_tz, cast_options),
            Timestamp(TimeUnit::Microsecond, to_tz) => cast_string_to_timestamp::<
                i32,
                TimestampMicrosecondType,
            >(array, to_tz, cast_options),
            Timestamp(TimeUnit::Nanosecond, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampNanosecondType>(array, to_tz, cast_options)
            }
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
                "Casting from {from_type:?} to {to_type:?} not supported",
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
                cast_view_to_timestamp::<TimestampSecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => {
                cast_view_to_timestamp::<TimestampMillisecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Microsecond, to_tz) => {
                cast_view_to_timestamp::<TimestampMicrosecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Nanosecond, to_tz) => {
                cast_view_to_timestamp::<TimestampNanosecondType>(array, to_tz, cast_options)
            }
            Interval(IntervalUnit::YearMonth) => {
                cast_view_to_year_month_interval(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => cast_view_to_day_time_interval(array, cast_options),
            Interval(IntervalUnit::MonthDayNano) => {
                cast_view_to_month_day_nano_interval(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
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
            Timestamp(TimeUnit::Second, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampSecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampMillisecondType,
            >(array, to_tz, cast_options),
            Timestamp(TimeUnit::Microsecond, to_tz) => cast_string_to_timestamp::<
                i64,
                TimestampMicrosecondType,
            >(array, to_tz, cast_options),
            Timestamp(TimeUnit::Nanosecond, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampNanosecondType>(array, to_tz, cast_options)
            }
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
                "Casting from {from_type:?} to {to_type:?} not supported",
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
                "Casting from {from_type:?} to {to_type:?} not supported",
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
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (FixedSizeBinary(size), _) => match to_type {
            Binary => cast_fixed_size_binary_to_binary::<i32>(array, *size),
            LargeBinary => cast_fixed_size_binary_to_binary::<i64>(array, *size),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
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
        (BinaryView, Utf8View) => {
            Ok(Arc::new(array.as_binary_view().clone().to_string_view()?) as ArrayRef)
        }
        (BinaryView, _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type:?} to {to_type:?} not supported",
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
        (Date64, Date32) => Ok(Arc::new(
            array
                .as_primitive::<Date64Type>()
                .unary::<_, Date32Type>(|x| (x / MILLISECONDS_IN_DAY) as i32),
        )),

        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time32SecondType>()
                .unary::<_, Time32MillisecondType>(|x| x * MILLISECONDS as i32),
        )),
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
                .unary::<_, Time64NanosecondType>(|x| x as i64 * (MICROSECONDS / NANOSECONDS)),
        )),

        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array
                .as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time32SecondType>(|x| (x / MICROSECONDS) as i32),
        )),
        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time32MillisecondType>(|x| (x / (MICROSECONDS / MILLISECONDS)) as i32),
        )),
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time64NanosecondType>(|x| x * (NANOSECONDS / MICROSECONDS)),
        )),

        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, Time32SecondType>(|x| (x / NANOSECONDS) as i32),
        )),
        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, Time32MillisecondType>(|x| (x / (NANOSECONDS / MILLISECONDS)) as i32),
        )),
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
        (Timestamp(TimeUnit::Microsecond, _), Date32) => {
            timestamp_to_date32(array.as_primitive::<TimestampMicrosecondType>())
        }
        (Timestamp(TimeUnit::Millisecond, _), Date32) => {
            timestamp_to_date32(array.as_primitive::<TimestampMillisecondType>())
        }
        (Timestamp(TimeUnit::Second, _), Date32) => {
            timestamp_to_date32(array.as_primitive::<TimestampSecondType>())
        }
        (Timestamp(TimeUnit::Nanosecond, _), Date32) => {
            timestamp_to_date32(array.as_primitive::<TimestampNanosecondType>())
        }
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
        (Timestamp(TimeUnit::Second, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
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
                .unary::<_, TimestampMicrosecondType>(|x| x * (MICROSECONDS / MILLISECONDS));

            cast_with_options(&array, to_type, cast_options)
        }
        (Date64, Timestamp(TimeUnit::Nanosecond, _)) => {
            let array = array
                .as_primitive::<Date64Type>()
                .unary::<_, TimestampNanosecondType>(|x| x * (NANOSECONDS / MILLISECONDS));

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
            let array = array
                .as_primitive::<Date32Type>()
                .unary::<_, TimestampMicrosecondType>(|x| (x as i64) * MICROSECONDS_IN_DAY);

            cast_with_options(&array, to_type, cast_options)
        }
        (Date32, Timestamp(TimeUnit::Nanosecond, _)) => {
            let array = array
                .as_primitive::<Date32Type>()
                .unary::<_, TimestampNanosecondType>(|x| (x as i64) * NANOSECONDS_IN_DAY);

            cast_with_options(&array, to_type, cast_options)
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
            "Casting from {from_type:?} to {to_type:?} not supported",
        ))),
    }
}

fn cast_from_decimal<D, F>(
    array: &dyn Array,
    base: D::Native,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    as_float: F,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + ToPrimitive,
    F: Fn(D::Native) -> f64,
{
    use DataType::*;
    // cast decimal to other type
    match to_type {
        UInt8 => cast_decimal_to_integer::<D, UInt8Type>(array, base, *scale, cast_options),
        UInt16 => cast_decimal_to_integer::<D, UInt16Type>(array, base, *scale, cast_options),
        UInt32 => cast_decimal_to_integer::<D, UInt32Type>(array, base, *scale, cast_options),
        UInt64 => cast_decimal_to_integer::<D, UInt64Type>(array, base, *scale, cast_options),
        Int8 => cast_decimal_to_integer::<D, Int8Type>(array, base, *scale, cast_options),
        Int16 => cast_decimal_to_integer::<D, Int16Type>(array, base, *scale, cast_options),
        Int32 => cast_decimal_to_integer::<D, Int32Type>(array, base, *scale, cast_options),
        Int64 => cast_decimal_to_integer::<D, Int64Type>(array, base, *scale, cast_options),
        Float32 => cast_decimal_to_float::<D, Float32Type, _>(array, |x| {
            (as_float(x) / 10_f64.powi(*scale as i32)) as f32
        }),
        Float64 => cast_decimal_to_float::<D, Float64Type, _>(array, |x| {
            as_float(x) / 10_f64.powi(*scale as i32)
        }),
        Utf8View => value_to_string_view(array, cast_options),
        Utf8 => value_to_string::<i32>(array, cast_options),
        LargeUtf8 => value_to_string::<i64>(array, cast_options),
        Null => Ok(new_null_array(to_type, array.len())),
        _ => Err(ArrowError::CastError(format!(
            "Casting from {from_type:?} to {to_type:?} not supported"
        ))),
    }
}

fn cast_to_decimal<D, M>(
    array: &dyn Array,
    base: M,
    precision: &u8,
    scale: &i8,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M: ArrowNativeTypeOp + DecimalCast,
    u8: num::traits::AsPrimitive<M>,
    u16: num::traits::AsPrimitive<M>,
    u32: num::traits::AsPrimitive<M>,
    u64: num::traits::AsPrimitive<M>,
    i8: num::traits::AsPrimitive<M>,
    i16: num::traits::AsPrimitive<M>,
    i32: num::traits::AsPrimitive<M>,
    i64: num::traits::AsPrimitive<M>,
{
    use DataType::*;
    // cast data to decimal
    match from_type {
        UInt8 => cast_integer_to_decimal::<_, D, M>(
            array.as_primitive::<UInt8Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        UInt16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt16Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        UInt32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt32Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        UInt64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<UInt64Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        Int8 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int8Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        Int16 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int16Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        Int32 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int32Type>(),
            *precision,
            *scale,
            base,
            cast_options,
        ),
        Int64 => cast_integer_to_decimal::<_, D, _>(
            array.as_primitive::<Int64Type>(),
            *precision,
            *scale,
            base,
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
            "Casting from {from_type:?} to {to_type:?} not supported"
        ))),
    }
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

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
fn cast_numeric_arrays<FROM, TO>(
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
        num::cast::cast::<T::Native, R::Native>(value).ok_or_else(|| {
            ArrowError::CastError(format!(
                "Can't cast value {:?} to type {}",
                value,
                R::DATA_TYPE
            ))
        })
    })
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
    from.unary_opt::<_, R>(num::cast::cast::<T::Native, R::Native>)
}

fn cast_numeric_to_binary<FROM: ArrowPrimitiveType, O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<FROM>();
    let size = std::mem::size_of::<FROM::Native>();
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat(size).take(array.len()));
    Ok(Arc::new(GenericBinaryArray::<O>::new(
        offsets,
        array.values().inner().clone(),
        array.nulls().cloned(),
    )))
}

fn adjust_timestamp_to_timezone<T: ArrowTimestampType>(
    array: PrimitiveArray<Int64Type>,
    to_tz: &Tz,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<Int64Type>, ArrowError> {
    let adjust = |o| {
        let local = as_datetime::<T>(o)?;
        let offset = to_tz.offset_from_local_datetime(&local).single()?;
        T::make_value(local - offset.fix())
    };
    let adjusted = if cast_options.safe {
        array.unary_opt::<_, Int64Type>(adjust)
    } else {
        array.try_unary::<_, Int64Type, _>(|o| {
            adjust(o).ok_or_else(|| {
                ArrowError::CastError("Cannot cast timezone to different timezone".to_string())
            })
        })?
    };
    Ok(adjusted)
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
fn cast_numeric_to_bool<FROM>(from: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
{
    numeric_to_bool_cast::<FROM>(from.as_primitive::<FROM>()).map(|to| Arc::new(to) as ArrayRef)
}

fn numeric_to_bool_cast<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray, ArrowError>
where
    T: ArrowPrimitiveType + ArrowPrimitiveType,
{
    let mut b = BooleanBuilder::with_capacity(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null();
        } else if from.value(i) != T::default_value() {
            b.append_value(true);
        } else {
            b.append_value(false);
        }
    }

    Ok(b.finish())
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
fn cast_bool_to_numeric<TO>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    TO: ArrowPrimitiveType,
    TO::Native: num::cast::NumCast,
{
    Ok(Arc::new(bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
        cast_options,
    )))
}

fn bool_to_numeric_cast<T>(from: &BooleanArray, _cast_options: &CastOptions) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num::NumCast,
{
    let iter = (0..from.len()).map(|i| {
        if from.is_null(i) {
            None
        } else if from.value(i) {
            // a workaround to cast a primitive to T::Native, infallible
            num::cast::cast(1)
        } else {
            Some(T::default_value())
        }
    });
    // Benefit:
    //     20% performance improvement
    // Soundness:
    //     The iterator is trustedLen because it comes from a Range
    unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) }
}

/// Helper function to cast from one `BinaryArray` or 'LargeBinaryArray' to 'FixedSizeBinaryArray'.
fn cast_binary_to_fixed_size_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_binary::<O>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(array.len(), byte_width);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            match builder.append_value(array.value(i)) {
                Ok(_) => {}
                Err(e) => match cast_options.safe {
                    true => builder.append_null(),
                    false => return Err(e),
                },
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from 'FixedSizeBinaryArray' to one `BinaryArray` or 'LargeBinaryArray'.
/// If the target one is too large for the source array it will return an Error.
fn cast_fixed_size_binary_to_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let offsets: i128 = byte_width as i128 * array.len() as i128;

    let is_binary = matches!(GenericBinaryType::<O>::DATA_TYPE, DataType::Binary);
    if is_binary && offsets > i32::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to Binary array".to_string(),
        ));
    } else if !is_binary && offsets > i64::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to LargeBinary array".to_string(),
        ));
    }

    let mut builder = GenericBinaryBuilder::<O>::with_capacity(array.len(), array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i));
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from one `ByteArrayType` to another and vice versa.
/// If the target one (e.g., `LargeUtf8`) is too large for the source array it will return an Error.
fn cast_byte_container<FROM, TO>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ByteArrayType,
    TO: ByteArrayType<Native = FROM::Native>,
    FROM::Offset: OffsetSizeTrait + ToPrimitive,
    TO::Offset: OffsetSizeTrait + NumCast,
{
    let data = array.to_data();
    assert_eq!(data.data_type(), &FROM::DATA_TYPE);
    let str_values_buf = data.buffers()[1].clone();
    let offsets = data.buffers()[0].typed_data::<FROM::Offset>();

    let mut offset_builder = BufferBuilder::<TO::Offset>::new(offsets.len());
    offsets
        .iter()
        .try_for_each::<_, Result<_, ArrowError>>(|offset| {
            let offset =
                <<TO as ByteArrayType>::Offset as NumCast>::from(*offset).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "{}{} array too large to cast to {}{} array",
                        FROM::Offset::PREFIX,
                        FROM::PREFIX,
                        TO::Offset::PREFIX,
                        TO::PREFIX
                    ))
                })?;
            offset_builder.append(offset);
            Ok(())
        })?;

    let offset_buffer = offset_builder.finish();

    let dtype = TO::DATA_TYPE;

    let builder = ArrayData::builder(dtype)
        .offset(array.offset())
        .len(array.len())
        .add_buffer(offset_buffer)
        .add_buffer(str_values_buf)
        .nulls(data.nulls().cloned());

    let array_data = unsafe { builder.build_unchecked() };

    Ok(Arc::new(GenericByteArray::<TO>::from(array_data)))
}

/// Helper function to cast from one `ByteViewType` array to `ByteArrayType` array.
fn cast_view_to_byte<FROM, TO>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ByteViewType,
    TO: ByteArrayType,
    FROM::Native: AsRef<TO::Native>,
{
    let data = array.to_data();
    let view_array = GenericByteViewArray::<FROM>::from(data);

    let len = view_array.len();
    let bytes = view_array
        .views()
        .iter()
        .map(|v| ByteView::from(*v).length as usize)
        .sum::<usize>();

    let mut byte_array_builder = GenericByteBuilder::<TO>::with_capacity(len, bytes);

    for val in view_array.iter() {
        byte_array_builder.append_option(val);
    }

    Ok(Arc::new(byte_array_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::{Buffer, IntervalDayTime, NullBuffer};
    use chrono::NaiveDate;
    use half::f16;

    #[derive(Clone)]
    struct DecimalCastTestConfig {
        input_prec: u8,
        input_scale: i8,
        input_repr: i128,
        output_prec: u8,
        output_scale: i8,
        expected_output_repr: Result<i128, String>, // the error variant can contain a string
                                                    // template where the "{}" will be
                                                    // replaced with the decimal type name
                                                    // (e.g. Decimal128)
    }

    macro_rules! generate_cast_test_case {
        ($INPUT_ARRAY: expr, $OUTPUT_TYPE_ARRAY: ident, $OUTPUT_TYPE: expr, $OUTPUT_VALUES: expr) => {
            let output =
                $OUTPUT_TYPE_ARRAY::from($OUTPUT_VALUES).with_data_type($OUTPUT_TYPE.clone());

            // assert cast type
            let input_array_type = $INPUT_ARRAY.data_type();
            assert!(can_cast_types(input_array_type, $OUTPUT_TYPE));
            let result = cast($INPUT_ARRAY, $OUTPUT_TYPE).unwrap();
            assert_eq!($OUTPUT_TYPE, result.data_type());
            assert_eq!(result.as_ref(), &output);

            let cast_option = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let result = cast_with_options($INPUT_ARRAY, $OUTPUT_TYPE, &cast_option).unwrap();
            assert_eq!($OUTPUT_TYPE, result.data_type());
            assert_eq!(result.as_ref(), &output);
        };
    }

    fn run_decimal_cast_test_case<I, O>(t: DecimalCastTestConfig)
    where
        I: DecimalType,
        O: DecimalType,
        I::Native: DecimalCast,
        O::Native: DecimalCast,
    {
        let array = vec![I::Native::from_decimal(t.input_repr)];
        let array = array
            .into_iter()
            .collect::<PrimitiveArray<I>>()
            .with_precision_and_scale(t.input_prec, t.input_scale)
            .unwrap();
        let input_type = array.data_type();
        let output_type = O::TYPE_CONSTRUCTOR(t.output_prec, t.output_scale);
        assert!(can_cast_types(input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);

        match t.expected_output_repr {
            Ok(v) => {
                let expected_array = vec![O::Native::from_decimal(v)];
                let expected_array = expected_array
                    .into_iter()
                    .collect::<PrimitiveArray<O>>()
                    .with_precision_and_scale(t.output_prec, t.output_scale)
                    .unwrap();
                assert_eq!(*result.unwrap(), expected_array);
            }
            Err(expected_output_message_template) => {
                assert!(result.is_err());
                let expected_error_message =
                    expected_output_message_template.replace("{}", O::PREFIX);
                assert_eq!(result.unwrap_err().to_string(), expected_error_message);
            }
        }
    }

    fn create_decimal128_array(
        array: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal128Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
    }

    fn create_decimal256_array(
        array: Vec<Option<i256>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal256Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(precision, scale)
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    #[should_panic(
        expected = "Cannot cast to Decimal128(20, 3). Overflowing on 57896044618658097711785492504343953926634992332820282019728792003956564819967"
    )]
    fn test_cast_decimal_to_decimal_round_with_error() {
        // decimal256 to decimal128 overflow
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
            Some(i256::MAX),
            Some(i256::MIN),
        ];
        let input_decimal_array = create_decimal256_array(array, 76, 4).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        let input_type = DataType::Decimal256(76, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_cast_decimal_to_decimal_round() {
        let array = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            None,
        ];
        let array = create_decimal128_array(array, 20, 4).unwrap();
        // decimal128 to decimal128
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );

        // decimal128 to decimal256
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );

        // decimal256
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 4).unwrap();

        // decimal256 to decimal256
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );
        // decimal256 to decimal128
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal128_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
        // negative test
        let array = vec![Some(123456), None];
        let array = create_decimal128_array(array, 10, 0).unwrap();
        let result_safe = cast(&array, &DataType::Decimal128(2, 2));
        assert!(result_safe.is_ok());
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };

        let result_unsafe = cast_with_options(&array, &DataType::Decimal128(2, 2), &options);
        assert_eq!("Invalid argument error: 12345600 is too large to store in a Decimal128 of precision 2. Max is 99",
                   result_unsafe.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_dict() {
        let p = 20;
        let s = 3;
        let input_type = DataType::Decimal128(p, s);
        let output_type = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Decimal128(p, s)),
        );
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal128_array(array, p, s).unwrap();
        let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
        assert_eq!(cast_array.data_type(), &output_type);
    }

    #[test]
    fn test_cast_decimal256_to_decimal256_dict() {
        let p = 20;
        let s = 3;
        let input_type = DataType::Decimal256(p, s);
        let output_type = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Decimal256(p, s)),
        );
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal128_array(array, p, s).unwrap();
        let cast_array = cast_with_options(&array, &output_type, &CastOptions::default()).unwrap();
        assert_eq!(cast_array.data_type(), &output_type);
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal128(38, 38);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal128_array(array, 38, 3).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal256(76, 76);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal128_array(array, 38, 3).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 76). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal128_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal128_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal128(38, 7);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 7). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal256_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal256(76, 55);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result = cast_with_options(
            &array,
            &output_type,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 55). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal128() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal256() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    fn generate_decimal_to_numeric_cast_test_case<T>(array: &PrimitiveArray<T>)
    where
        T: ArrowPrimitiveType + DecimalType,
    {
        // u8
        generate_cast_test_case!(
            array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );
    }

    #[test]
    fn test_cast_decimal128_to_numeric() {
        let value_array: Vec<Option<i128>> = vec![Some(125), Some(225), Some(325), None, Some(525)];
        let array = create_decimal128_array(value_array, 38, 2).unwrap();

        generate_decimal_to_numeric_cast_test_case(&array);

        // overflow test: out of range of max u8
        let value_array: Vec<Option<i128>> = vec![Some(51300)];
        let array = create_decimal128_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::UInt8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 513 is out of range UInt8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::UInt8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i128>> = vec![Some(24400)];
        let array = create_decimal128_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678),
            Some(112345679),
        ];
        let array = create_decimal128_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678901234568),
            Some(112345678901234560),
        ];
        let array = create_decimal128_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_numeric() {
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
        ];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        // u8
        generate_cast_test_case!(
            &array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            &array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            &array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            &array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            &array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            &array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            &array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            &array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i256>> = vec![Some(i256::from_i128(24400))];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array = cast_with_options(
            &array,
            &DataType::Int8,
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678)),
            Some(i256::from_i128(112345679)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678901234568)),
            Some(i256::from_i128(112345678901234560)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128() {
        let decimal_type = DataType::Decimal128(38, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // test u8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = UInt8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round up
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal256() {
        let decimal_type = DataType::Decimal256(76, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast(&array, &DataType::Decimal256(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal256Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round down
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );
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
    fn test_cast_i32_to_list_i32() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));
        let c = arr.values().as_primitive::<Int32Type>();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_i32_nullable() {
        let array = Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        assert_eq!(0, b.null_count());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));

        let c = arr.values().as_primitive::<Int32Type>();
        assert_eq!(1, c.null_count());
        assert_eq!(5, c.value(0));
        assert!(!c.is_valid(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_f64_nullable_sliced() {
        let array = Int32Array::from(vec![Some(5), None, Some(7), Some(8), None, Some(10)]);
        let array = array.slice(2, 4);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
        )
        .unwrap();
        assert_eq!(4, b.len());
        assert_eq!(0, b.null_count());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        let c = arr.values().as_primitive::<Float64Type>();
        assert_eq!(1, c.null_count());
        assert_eq!(7.0, c.value(0));
        assert_eq!(8.0, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(10.0, c.value(3));
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
    fn test_cast_utf8view_to_decimal128() {
        let array = StringViewArray::from(vec![None, Some("4"), Some("5.6"), Some("7.89")]);
        let arr = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &arr,
            Decimal128Array,
            &DataType::Decimal128(4, 2),
            vec![None, Some(400_i128), Some(560_i128), Some(789_i128)]
        );
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
                assert!(e
                    .to_string()
                    .contains("Cast error: Cannot cast value 'invalid' to value of Boolean type"))
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
    fn test_cast_bool_to_utf8view() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Utf8View).unwrap();
        let c = b.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!("true", c.value(0));
        assert_eq!("false", c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_to_utf8() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Utf8).unwrap();
        let c = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!("true", c.value(0));
        assert_eq!("false", c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_to_large_utf8() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::LargeUtf8).unwrap();
        let c = b.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!("true", c.value(0));
        assert_eq!("false", c.value(1));
        assert!(!c.is_valid(2));
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
    fn test_cast_list_i32_to_list_u16() {
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 100000000]).into_data();

        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        // Construct a list array from the above two
        // [[0,0,0], [-1, -2, -1], [2, 100000000]]
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let cast_array = cast(
            &list_array,
            &DataType::List(Arc::new(Field::new_list_field(DataType::UInt16, true))),
        )
        .unwrap();

        // For the ListArray itself, there are no null values (as there were no nulls when they went in)
        //
        // 3 negative values should get lost when casting to unsigned,
        // 1 value should overflow
        assert_eq!(0, cast_array.null_count());

        // offsets should be the same
        let array = cast_array.as_list::<i32>();
        assert_eq!(list_array.value_offsets(), array.value_offsets());

        assert_eq!(DataType::UInt16, array.value_type());
        assert_eq!(3, array.value_length(0));
        assert_eq!(3, array.value_length(1));
        assert_eq!(2, array.value_length(2));

        // expect 4 nulls: negative numbers and overflow
        let u16arr = array.values().as_primitive::<UInt16Type>();
        assert_eq!(4, u16arr.null_count());

        // expect 4 nulls: negative numbers and overflow
        let expected: UInt16Array =
            vec![Some(0), Some(0), Some(0), None, None, None, Some(2), None]
                .into_iter()
                .collect();

        assert_eq!(u16arr, &expected);
    }

    #[test]
    fn test_cast_list_i32_to_list_timestamp() {
        // Construct a value array
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).into_data();

        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 9]);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let actual = cast(
            &list_array,
            &DataType::List(Arc::new(Field::new_list_field(
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
        )
        .unwrap();

        let expected = cast(
            &cast(
                &list_array,
                &DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            )
            .unwrap(),
            &DataType::List(Arc::new(Field::new_list_field(
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
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
    fn test_cast_string_to_integral_overflow() {
        let str = Arc::new(StringArray::from(vec![
            Some("123"),
            Some("-123"),
            Some("86374"),
            None,
        ])) as ArrayRef;

        let options = CastOptions {
            safe: true,
            format_options: FormatOptions::default(),
        };
        let res = cast_with_options(&str, &DataType::Int16, &options).expect("should cast to i16");
        let expected =
            Arc::new(Int16Array::from(vec![Some(123), Some(-123), None, None])) as ArrayRef;
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
            assert_eq!(err.to_string(), "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(Second) type");
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
            assert_eq!(err.to_string(), "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(Millisecond) type");
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
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid time' to value of Time64(Microsecond) type");
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
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid time' to value of Time64(Nanosecond) type");
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
            r#"Cast error: Cannot cast 2 months 31 days 1 second to IntervalYearMonth. Only year and month fields are allowed."#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("1 day 1.5 milliseconds")],
            IntervalUnit::DayTime,
            r#"Cast error: Cannot cast 1 day 1.5 milliseconds to IntervalDayTime because the nanos part isn't multiple of milliseconds"#
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
        let bytes_1 = "Hiiii".as_bytes();
        let bytes_2 = "Hello".as_bytes();

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
        let bytes_1 = "Hi".as_bytes();
        let bytes_2 = "Hello".as_bytes();

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
        let bytes_1 = "Hiiii".as_bytes();
        let bytes_2 = "Hello".as_bytes();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(FixedSizeBinaryArray::from(binary_data.clone())) as ArrayRef;

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

        // test timestamp nanoseconds
        let a = TimestampNanosecondArray::from(vec![Some(86405000000000), Some(1000000000), None])
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

        // test overflow
        let a =
            TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond));
        assert!(b.is_err());
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond));
        assert!(b.is_err());
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

        // test timestamp nanoseconds
        let a = TimestampNanosecondArray::from(vec![Some(86405000000000), Some(1000000000), None])
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

        // test overflow
        let a =
            TimestampSecondArray::from(vec![Some(i64::MAX)]).with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second));
        assert!(b.is_err());
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond));
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

    #[test]
    fn test_cast_date32_to_string() {
        let array = Date32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Utf8).unwrap();
        let c = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(&DataType::Utf8, c.data_type());
        assert_eq!("1997-05-19", c.value(0));
        assert_eq!("2018-12-25", c.value(1));
    }

    #[test]
    fn test_cast_date64_to_string() {
        let array = Date64Array::from(vec![10000 * 86400000, 17890 * 86400000]);
        let b = cast(&array, &DataType::Utf8).unwrap();
        let c = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(&DataType::Utf8, c.data_type());
        assert_eq!("1997-05-19T00:00:00", c.value(0));
        assert_eq!("2018-12-25T00:00:00", c.value(1));
    }

    macro_rules! assert_cast_timestamp_to_string {
        ($array:expr, $datatype:expr, $output_array_type: ty, $expected:expr) => {{
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

        assert_cast_timestamp_to_string!(array, DataType::Utf8View, StringViewArray, expected);
        assert_cast_timestamp_to_string!(array, DataType::Utf8, StringArray, expected);
        assert_cast_timestamp_to_string!(array, DataType::LargeUtf8, LargeStringArray, expected);
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
        assert_cast_timestamp_to_string!(
            array_without_tz,
            DataType::Utf8View,
            StringViewArray,
            cast_options,
            expected
        );
        assert_cast_timestamp_to_string!(
            array_without_tz,
            DataType::Utf8,
            StringArray,
            cast_options,
            expected
        );
        assert_cast_timestamp_to_string!(
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
        assert_cast_timestamp_to_string!(
            array_with_tz,
            DataType::Utf8View,
            StringViewArray,
            cast_options,
            expected
        );
        assert_cast_timestamp_to_string!(
            array_with_tz,
            DataType::Utf8,
            StringArray,
            cast_options,
            expected
        );
        assert_cast_timestamp_to_string!(
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
            let from_unit = match FromType::DATA_TYPE {
                DataType::Duration(unit) => unit,
                _ => panic!("Expected a duration type"),
            };
            let to_unit = match ToType::DATA_TYPE {
                DataType::Duration(unit) => unit,
                _ => panic!("Expected a duration type"),
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
    fn test_bianry_to_view() {
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
            for v in typed_dict.into_iter() {
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
            for v in typed_binary_dict.into_iter() {
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
            for s in VIEW_TEST_DATA.iter() {
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
            for s in VIEW_TEST_DATA.iter() {
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
    fn test_cast_utf8_dict() {
        // FROM a dictionary with of Utf8 values
        use DataType::*;

        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("one").unwrap();
        builder.append_null();
        builder.append("three").unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["one", "null", "three"];

        // Test casting TO StringArray
        let cast_type = Utf8;
        let cast_array = cast(&array, &cast_type).expect("cast to UTF-8 failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Test casting TO Dictionary (with different index sizes)

        let cast_type = Dictionary(Box::new(Int16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_primitive() {
        use DataType::*;
        // test converting from an array that has indexes of a type
        // that are out of bounds for a particular other kind of
        // index.

        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            builder.append(i).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{res:?}");
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{actual_error}' in actual error '{expected_error}'"
        );
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_utf8() {
        use DataType::*;
        // Same test as test_cast_dict_to_dict_bad_index_value but use
        // string values (and encode the expected behavior here);

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            let val = format!("val{i}");
            builder.append(&val).unwrap();
        }
        let array = builder.finish();

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{res:?}");
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{actual_error}' in actual error '{expected_error}'"
        );
    }

    #[test]
    fn test_cast_primitive_dict() {
        // FROM a dictionary with of INT32 values
        use DataType::*;

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(1).unwrap();
        builder.append_null();
        builder.append(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Test casting TO PrimitiveArray, different dictionary type
        let cast_array = cast(&array, &Utf8).expect("cast to UTF-8 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Utf8);

        let cast_array = cast(&array, &Int64).expect("cast to int64 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Int64);
    }

    #[test]
    fn test_cast_primitive_array_to_dict() {
        use DataType::*;

        let mut builder = PrimitiveBuilder::<Int32Type>::new();
        builder.append_value(1);
        builder.append_null();
        builder.append_value(3);
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Cast to a dictionary (same value type, Int32)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int32));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Cast to a dictionary (different value type, Int8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_time_array_to_dict() {
        use DataType::*;

        let array = Arc::new(Date32Array::from(vec![Some(1000), None, Some(2000)])) as ArrayRef;

        let expected = vec!["1972-09-27", "null", "1975-06-24"];

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Date32));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_timestamp_array_to_dict() {
        use DataType::*;

        let array = Arc::new(
            TimestampSecondArray::from(vec![Some(1000), None, Some(2000)]).with_timezone_utc(),
        ) as ArrayRef;

        let expected = vec!["1970-01-01T00:16:40", "null", "1970-01-01T00:33:20"];

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Timestamp(TimeUnit::Second, None)));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_string_array_to_dict() {
        use DataType::*;

        let array = Arc::new(StringArray::from(vec![Some("one"), None, Some("three")])) as ArrayRef;

        let expected = vec!["one", "null", "three"];

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
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

        typed_test!(Float32Array, Float32, Float32Type);
        typed_test!(Float64Array, Float64, Float64Type);

        typed_test!(Date32Array, Date32, Date32Type);
        typed_test!(Date64Array, Date64, Date64Type);
    }

    fn cast_from_null_to_other(data_type: &DataType) {
        // Cast from null to data_type
        {
            let array = new_null_array(&DataType::Null, 4);
            assert_eq!(array.data_type(), &DataType::Null);
            let cast_array = cast(&array, data_type).expect("cast failed");
            assert_eq!(cast_array.data_type(), data_type);
            for i in 0..4 {
                assert!(cast_array.is_null(i));
            }
        }
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
                "entry",
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
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
    }

    /// Print the `DictionaryArray` `array` as a vector of strings
    fn array_to_strings(array: &ArrayRef) -> Vec<String> {
        let options = FormatOptions::new().with_null("null");
        let formatter = ArrayFormatter::try_new(array.as_ref(), &options).unwrap();
        (0..array.len())
            .map(|i| formatter.value(i).to_string())
            .collect()
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
    fn test_can_cast_fsl_to_fsl() {
        let from_array = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                [Some([Some(1.0), Some(2.0)]), None],
                2,
            ),
        ) as ArrayRef;
        let to_array = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
                [
                    Some([Some(f16::from_f32(1.0)), Some(f16::from_f32(2.0))]),
                    None,
                ],
                2,
            ),
        ) as ArrayRef;

        assert!(can_cast_types(from_array.data_type(), to_array.data_type()));
        let actual = cast(&from_array, to_array.data_type()).unwrap();
        assert_eq!(actual.data_type(), to_array.data_type());

        let invalid_target =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Binary, true)), 2);
        assert!(!can_cast_types(from_array.data_type(), &invalid_target));

        let invalid_size =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float16, true)), 5);
        assert!(!can_cast_types(from_array.data_type(), &invalid_size));
    }

    #[test]
    fn test_can_cast_types_fixed_size_list_to_list() {
        // DataType::List
        let array1 = Arc::new(make_fixed_size_list_array()) as ArrayRef;
        assert!(can_cast_types(
            array1.data_type(),
            &DataType::List(Arc::new(Field::new("", DataType::Int32, false)))
        ));

        // DataType::LargeList
        let array2 = Arc::new(make_fixed_size_list_array_for_large_list()) as ArrayRef;
        assert!(can_cast_types(
            array2.data_type(),
            &DataType::LargeList(Arc::new(Field::new("", DataType::Int64, false)))
        ));
    }

    #[test]
    fn test_cast_fixed_size_list_to_list() {
        // Important cases:
        // 1. With/without nulls
        // 2. LargeList and List
        // 3. With and without inner casts

        let cases = [
            // fixed_size_list<i32, 2> => list<i32>
            (
                Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                    2,
                )) as ArrayRef,
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                    Some([Some(1), Some(1)]),
                    Some([Some(2), Some(2)]),
                ])) as ArrayRef,
            ),
            // fixed_size_list<i32, 2> => list<i32> (nullable)
            (
                Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    [None, Some([Some(2), Some(2)])],
                    2,
                )) as ArrayRef,
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                    None,
                    Some([Some(2), Some(2)]),
                ])) as ArrayRef,
            ),
            // fixed_size_list<i32, 2> => large_list<i64>
            (
                Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                    2,
                )) as ArrayRef,
                Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>([
                    Some([Some(1), Some(1)]),
                    Some([Some(2), Some(2)]),
                ])) as ArrayRef,
            ),
            // fixed_size_list<i32, 2> => large_list<i64> (nullable)
            (
                Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    [None, Some([Some(2), Some(2)])],
                    2,
                )) as ArrayRef,
                Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>([
                    None,
                    Some([Some(2), Some(2)]),
                ])) as ArrayRef,
            ),
        ];

        for (array, expected) in cases {
            let array = Arc::new(array) as ArrayRef;

            assert!(
                can_cast_types(array.data_type(), expected.data_type()),
                "can_cast_types claims we cannot cast {:?} to {:?}",
                array.data_type(),
                expected.data_type()
            );

            let list_array = cast(&array, expected.data_type())
                .unwrap_or_else(|_| panic!("Failed to cast {:?} to {:?}", array, expected));
            assert_eq!(
                list_array.as_ref(),
                &expected,
                "Incorrect result from casting {:?} to {:?}",
                array,
                expected
            );
        }
    }

    #[test]
    fn test_cast_utf8_to_list() {
        // DataType::List
        let array = Arc::new(StringArray::from(vec!["5"])) as ArrayRef;
        let field = Arc::new(Field::new("", DataType::Int32, false));
        let list_array = cast(&array, &DataType::List(field.clone())).unwrap();
        let actual = list_array.as_list_opt::<i32>().unwrap();
        let expect = ListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])]);
        assert_eq!(&expect.value(0), &actual.value(0));

        // DataType::LargeList
        let list_array = cast(&array, &DataType::LargeList(field.clone())).unwrap();
        let actual = list_array.as_list_opt::<i64>().unwrap();
        let expect = LargeListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])]);
        assert_eq!(&expect.value(0), &actual.value(0));

        // DataType::FixedSizeList
        let list_array = cast(&array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
        let actual = list_array.as_fixed_size_list_opt().unwrap();
        let expect =
            FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])], 1);
        assert_eq!(&expect.value(0), &actual.value(0));
    }

    #[test]
    fn test_cast_single_element_fixed_size_list() {
        // FixedSizeList<T>[1] => T
        let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
            [(Some([Some(5)]))],
            1,
        )) as ArrayRef;
        let casted_array = cast(&from_array, &DataType::Int32).unwrap();
        let actual: &Int32Array = casted_array.as_primitive();
        let expected = Int32Array::from(vec![Some(5)]);
        assert_eq!(&expected, actual);

        // FixedSizeList<T>[1] => FixedSizeList<U>[1]
        let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
            [(Some([Some(5)]))],
            1,
        )) as ArrayRef;
        let to_field = Arc::new(Field::new("dummy", DataType::Float32, false));
        let actual = cast(&from_array, &DataType::FixedSizeList(to_field.clone(), 1)).unwrap();
        let expected = Arc::new(FixedSizeListArray::new(
            to_field.clone(),
            1,
            Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
            None,
        )) as ArrayRef;
        assert_eq!(*expected, *actual);

        // FixedSizeList<T>[1] => FixedSizeList<FixdSizedList<U>[1]>[1]
        let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
            [(Some([Some(5)]))],
            1,
        )) as ArrayRef;
        let to_field_inner = Arc::new(Field::new_list_field(DataType::Float32, false));
        let to_field = Arc::new(Field::new(
            "dummy",
            DataType::FixedSizeList(to_field_inner.clone(), 1),
            false,
        ));
        let actual = cast(&from_array, &DataType::FixedSizeList(to_field.clone(), 1)).unwrap();
        let expected = Arc::new(FixedSizeListArray::new(
            to_field.clone(),
            1,
            Arc::new(FixedSizeListArray::new(
                to_field_inner.clone(),
                1,
                Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
                None,
            )) as ArrayRef,
            None,
        )) as ArrayRef;
        assert_eq!(*expected, *actual);

        // T => FixedSizeList<T>[1] (non-nullable)
        let field = Arc::new(Field::new("dummy", DataType::Float32, false));
        let from_array = Arc::new(Int8Array::from(vec![Some(5)])) as ArrayRef;
        let casted_array = cast(&from_array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
        let actual = casted_array.as_fixed_size_list();
        let expected = Arc::new(FixedSizeListArray::new(
            field.clone(),
            1,
            Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
            None,
        )) as ArrayRef;
        assert_eq!(expected.as_ref(), actual);

        // T => FixedSizeList<T>[1] (nullable)
        let field = Arc::new(Field::new("nullable", DataType::Float32, true));
        let from_array = Arc::new(Int8Array::from(vec![None])) as ArrayRef;
        let casted_array = cast(&from_array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
        let actual = casted_array.as_fixed_size_list();
        let expected = Arc::new(FixedSizeListArray::new(
            field.clone(),
            1,
            Arc::new(Float32Array::from(vec![None])) as ArrayRef,
            None,
        )) as ArrayRef;
        assert_eq!(expected.as_ref(), actual);
    }

    #[test]
    fn test_cast_list_containers() {
        // large-list to list
        let array = Arc::new(make_large_list_array()) as ArrayRef;
        let list_array = cast(
            &array,
            &DataType::List(Arc::new(Field::new("", DataType::Int32, false))),
        )
        .unwrap();
        let actual = list_array.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = array.as_any().downcast_ref::<LargeListArray>().unwrap();

        assert_eq!(&expected.value(0), &actual.value(0));
        assert_eq!(&expected.value(1), &actual.value(1));
        assert_eq!(&expected.value(2), &actual.value(2));

        // list to large-list
        let array = Arc::new(make_list_array()) as ArrayRef;
        let large_list_array = cast(
            &array,
            &DataType::LargeList(Arc::new(Field::new("", DataType::Int32, false))),
        )
        .unwrap();
        let actual = large_list_array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();
        let expected = array.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(&expected.value(0), &actual.value(0));
        assert_eq!(&expected.value(1), &actual.value(1));
        assert_eq!(&expected.value(2), &actual.value(2));
    }

    #[test]
    fn test_cast_list_to_fsl() {
        // There four noteworthy cases we should handle:
        // 1. No nulls
        // 2. Nulls that are always empty
        // 3. Nulls that have varying lengths
        // 4. Nulls that are correctly sized (same as target list size)

        // Non-null case
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let values = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5), Some(6)]),
        ];
        let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            values.clone(),
        )) as ArrayRef;
        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            values, 3,
        )) as ArrayRef;
        let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());

        // Null cases
        // Array is [[1, 2, 3], null, [4, 5, 6], null]
        let cases = [
            (
                // Zero-length nulls
                vec![1, 2, 3, 4, 5, 6],
                vec![3, 0, 3, 0],
            ),
            (
                // Varying-length nulls
                vec![1, 2, 3, 0, 0, 4, 5, 6, 0],
                vec![3, 2, 3, 1],
            ),
            (
                // Correctly-sized nulls
                vec![1, 2, 3, 0, 0, 0, 4, 5, 6, 0, 0, 0],
                vec![3, 3, 3, 3],
            ),
            (
                // Mixed nulls
                vec![1, 2, 3, 4, 5, 6, 0, 0, 0],
                vec![3, 0, 3, 3],
            ),
        ];
        let null_buffer = NullBuffer::from(vec![true, false, true, false]);

        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                None,
                Some(vec![Some(4), Some(5), Some(6)]),
                None,
            ],
            3,
        )) as ArrayRef;

        for (values, lengths) in cases.iter() {
            let array = Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::from_lengths(lengths.clone()),
                Arc::new(Int32Array::from(values.clone())),
                Some(null_buffer.clone()),
            )) as ArrayRef;
            let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
            assert_eq!(expected.as_ref(), actual.as_ref());
        }
    }

    #[test]
    fn test_cast_list_to_fsl_safety() {
        let values = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(6), Some(7), Some(8), Some(9)]),
            Some(vec![Some(3), Some(4), Some(5)]),
        ];
        let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            values.clone(),
        )) as ArrayRef;

        let res = cast_with_options(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        );
        assert!(res.is_err());
        assert!(format!("{:?}", res)
            .contains("Cannot cast to FixedSizeList(3): value at index 1 has length 2"));

        // When safe=true (default), the cast will fill nulls for lists that are
        // too short and truncate lists that are too long.
        let res = cast(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
        )
        .unwrap();
        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                None, // Too short -> replaced with null
                None, // Too long -> replaced with null
                Some(vec![Some(3), Some(4), Some(5)]),
            ],
            3,
        )) as ArrayRef;
        assert_eq!(expected.as_ref(), res.as_ref());

        // The safe option is false and the source array contains a null list.
        // issue: https://github.com/apache/arrow-rs/issues/5642
        let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
        ])) as ArrayRef;
        let res = cast_with_options(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .unwrap();
        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![Some(vec![Some(1), Some(2), Some(3)]), None],
            3,
        )) as ArrayRef;
        assert_eq!(expected.as_ref(), res.as_ref());
    }

    #[test]
    fn test_cast_large_list_to_fsl() {
        let values = vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])];
        let array = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
            values.clone(),
        )) as ArrayRef;
        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            values, 2,
        )) as ArrayRef;
        let actual = cast(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2),
        )
        .unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());
    }

    #[test]
    fn test_cast_list_to_fsl_subcast() {
        let array = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3), Some(i32::MAX)]),
            ],
        )) as ArrayRef;
        let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3), Some(i32::MAX as i64)]),
            ],
            2,
        )) as ArrayRef;
        let actual = cast(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, true)), 2),
        )
        .unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());

        let res = cast_with_options(
            array.as_ref(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int16, true)), 2),
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        );
        assert!(res.is_err());
        assert!(format!("{:?}", res).contains("Can't cast value 2147483647 to type Int16"));
    }

    #[test]
    fn test_cast_list_to_fsl_empty() {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let array = new_empty_array(&DataType::List(field.clone()));

        let target_type = DataType::FixedSizeList(field.clone(), 3);
        let expected = new_empty_array(&target_type);

        let actual = cast(array.as_ref(), &target_type).unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());
    }

    fn make_list_array() -> ListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        ListArray::from(list_data)
    }

    fn make_large_list_array() -> LargeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        LargeListArray::from(list_data)
    }

    fn make_fixed_size_list_array() -> FixedSizeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 4);
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .add_child_data(value_data)
            .build()
            .unwrap();
        FixedSizeListArray::from(list_data)
    }

    fn make_fixed_size_list_array_for_large_list() -> FixedSizeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int64)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0i64, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, true)), 4);
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .add_child_data(value_data)
            .build()
            .unwrap();
        FixedSizeListArray::from(list_data)
    }

    #[test]
    fn test_cast_map_dont_allow_change_of_order() {
        let string_builder = StringBuilder::new();
        let value_builder = StringBuilder::new();
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            string_builder,
            value_builder,
        );

        builder.keys().append_value("0");
        builder.values().append_value("test_val_1");
        builder.append(true).unwrap();
        builder.keys().append_value("1");
        builder.values().append_value("test_val_2");
        builder.append(true).unwrap();

        // map builder returns unsorted map by default
        let array = builder.finish();

        let new_ordered = true;
        let new_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            new_ordered,
        );

        let new_array_result = cast(&array, &new_type.clone());
        assert!(!can_cast_types(array.data_type(), &new_type));
        assert!(
            matches!(new_array_result, Err(ArrowError::CastError(t)) if t == r#"Casting from Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false) to Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, true) not supported"#)
        );
    }

    #[test]
    fn test_cast_map_dont_allow_when_container_cant_cast() {
        let string_builder = StringBuilder::new();
        let value_builder = IntervalDayTimeArray::builder(2);
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            string_builder,
            value_builder,
        );

        builder.keys().append_value("0");
        builder.values().append_value(IntervalDayTime::new(1, 1));
        builder.append(true).unwrap();
        builder.keys().append_value("1");
        builder.values().append_value(IntervalDayTime::new(2, 2));
        builder.append(true).unwrap();

        // map builder returns unsorted map by default
        let array = builder.finish();

        let new_ordered = true;
        let new_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Duration(TimeUnit::Second), false),
                    ]
                    .into(),
                ),
                false,
            )),
            new_ordered,
        );

        let new_array_result = cast(&array, &new_type.clone());
        assert!(!can_cast_types(array.data_type(), &new_type));
        assert!(
            matches!(new_array_result, Err(ArrowError::CastError(t)) if t == r#"Casting from Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Interval(DayTime), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false) to Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Duration(Second), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, true) not supported"#)
        );
    }

    #[test]
    fn test_cast_map_field_names() {
        let string_builder = StringBuilder::new();
        let value_builder = StringBuilder::new();
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            string_builder,
            value_builder,
        );

        builder.keys().append_value("0");
        builder.values().append_value("test_val_1");
        builder.append(true).unwrap();
        builder.keys().append_value("1");
        builder.values().append_value("test_val_2");
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        let array = builder.finish();

        let new_type = DataType::Map(
            Arc::new(Field::new(
                "entries_new",
                DataType::Struct(
                    vec![
                        Field::new("key_new", DataType::Utf8, false),
                        Field::new("value_values", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        assert_ne!(new_type, array.data_type().clone());

        let new_array = cast(&array, &new_type.clone()).unwrap();
        assert_eq!(new_type, new_array.data_type().clone());
        let map_array = new_array.as_map();

        assert_ne!(new_type, array.data_type().clone());
        assert_eq!(new_type, map_array.data_type().clone());

        let key_string = map_array
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&key_string, &vec!["0", "1"]);

        let values_string_array = cast(map_array.values(), &DataType::Utf8).unwrap();
        let values_string = values_string_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&values_string, &vec!["test_val_1", "test_val_2"]);

        assert_eq!(
            map_array.nulls(),
            Some(&NullBuffer::from(vec![true, true, false]))
        );
    }

    #[test]
    fn test_cast_map_contained_values() {
        let string_builder = StringBuilder::new();
        let value_builder = Int8Builder::new();
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            string_builder,
            value_builder,
        );

        builder.keys().append_value("0");
        builder.values().append_value(44);
        builder.append(true).unwrap();
        builder.keys().append_value("1");
        builder.values().append_value(22);
        builder.append(true).unwrap();

        let array = builder.finish();

        let new_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let new_array = cast(&array, &new_type.clone()).unwrap();
        assert_eq!(new_type, new_array.data_type().clone());
        let map_array = new_array.as_map();

        assert_ne!(new_type, array.data_type().clone());
        assert_eq!(new_type, map_array.data_type().clone());

        let key_string = map_array
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&key_string, &vec!["0", "1"]);

        let values_string_array = cast(map_array.values(), &DataType::Utf8).unwrap();
        let values_string = values_string_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&values_string, &vec!["44", "22"]);
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
    fn test_list_cast_offsets() {
        // test if offset of the array is taken into account during cast
        let array1 = make_list_array().slice(1, 2);
        let array2 = Arc::new(make_list_array()) as ArrayRef;

        let dt = DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let out1 = cast(&array1, &dt).unwrap();
        let out2 = cast(&array2, &dt).unwrap();

        assert_eq!(&out1, &out2.slice(1, 2))
    }

    #[test]
    fn test_list_to_string() {
        let str_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);
        let value_data = str_array.into_data();

        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let out = cast(&array, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[a, b, c]", "[d, e, f]", "[g, h]"]);

        let out = cast(&array, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[a, b, c]", "[d, e, f]", "[g, h]"]);

        let array = Arc::new(make_list_array()) as ArrayRef;
        let out = cast(&array, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);

        let array = Arc::new(make_large_list_array()) as ArrayRef;
        let out = cast(&array, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);
    }

    #[test]
    fn test_cast_f64_to_decimal128() {
        // to reproduce https://github.com/apache/arrow-rs/issues/2997

        let decimal_type = DataType::Decimal128(18, 2);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(6_i128), // round down
            ]
        );

        let decimal_type = DataType::Decimal128(18, 3);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(70_i128), // round up
                Some(66_i128), // round up
                Some(65_i128), // round down
                Some(65_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_numeric_to_decimal256_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_floating_point_to_decimal128_precision_overflow() {
        let array = Float64Array::from(vec![1.1]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(2, 2),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(2, 2),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Invalid argument error: 110 is too large to store in a Decimal128 of precision 2. Max is 99";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal256_precision_overflow() {
        let array = Float64Array::from(vec![1.1]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(2, 2),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(2, 2),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Invalid argument error: 110 is too large to store in a Decimal256 of precision 2. Max is 99";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal128_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal128(38, 30)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal256_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal256(76, 50)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative_scale() {
        let input_type = DataType::Decimal128(20, 0);
        let output_type = DataType::Decimal128(20, -1);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123450), Some(2123455), Some(3123456), None];
        let input_decimal_array = create_decimal128_array(array, 20, 0).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(312346_i128),
                None
            ]
        );

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123460", decimal_arr.value_as_string(1));
        assert_eq!("3123460", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_numeric_to_decimal128_negative() {
        let decimal_type = DataType::Decimal128(38, -1);
        let array = Arc::new(Int32Array::from(vec![
            Some(1123456),
            Some(2123456),
            Some(3123456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123450", decimal_arr.value_as_string(1));
        assert_eq!("3123450", decimal_arr.value_as_string(2));

        let array = Arc::new(Float32Array::from(vec![
            Some(1123.456),
            Some(2123.456),
            Some(3123.456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1120", decimal_arr.value_as_string(0));
        assert_eq!("2120", decimal_arr.value_as_string(1));
        assert_eq!("3120", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative() {
        let input_type = DataType::Decimal128(10, -1);
        let output_type = DataType::Decimal128(10, -2);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(123)];
        let input_decimal_array = create_decimal128_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(12_i128),]);

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1200", decimal_arr.value_as_string(0));

        let array = vec![Some(125)];
        let input_decimal_array = create_decimal128_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(&array, Decimal128Array, &output_type, vec![Some(13_i128),]);

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1300", decimal_arr.value_as_string(0));
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_negative() {
        let input_type = DataType::Decimal128(10, 3);
        let output_type = DataType::Decimal256(10, 5);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(123456), Some(-123456)];
        let input_decimal_array = create_decimal128_array(array, 10, 3).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;

        let hundred = i256::from_i128(100);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(123456).mul_wrapping(hundred)),
                Some(i256::from_i128(-123456).mul_wrapping(hundred))
            ]
        );
    }

    #[test]
    fn test_parse_string_to_decimal() {
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("123.45", 2).unwrap(),
                38,
                2,
            ),
            "123.45"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("12345", 2).unwrap(),
                38,
                2,
            ),
            "12345.00"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("0.12345", 2).unwrap(),
                38,
                2,
            ),
            "0.12"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".12345", 2).unwrap(),
                38,
                2,
            ),
            "0.12"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".1265", 2).unwrap(),
                38,
                2,
            ),
            "0.13"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".1265", 2).unwrap(),
                38,
                2,
            ),
            "0.13"
        );

        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("123.45", 3).unwrap(),
                38,
                3,
            ),
            "123.450"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("12345", 3).unwrap(),
                38,
                3,
            ),
            "12345.000"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("0.12345", 3).unwrap(),
                38,
                3,
            ),
            "0.123"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>(".12345", 3).unwrap(),
                38,
                3,
            ),
            "0.123"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>(".1265", 3).unwrap(),
                38,
                3,
            ),
            "0.127"
        );
    }

    fn test_cast_string_to_decimal(array: ArrayRef) {
        // Decimal128
        let output_type = DataType::Decimal128(38, 2);
        assert!(can_cast_types(array.data_type(), &output_type));

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("123.45", decimal_arr.value_as_string(0));
        assert_eq!("1.23", decimal_arr.value_as_string(1));
        assert_eq!("0.12", decimal_arr.value_as_string(2));
        assert_eq!("0.13", decimal_arr.value_as_string(3));
        assert_eq!("1.26", decimal_arr.value_as_string(4));
        assert_eq!("12345.00", decimal_arr.value_as_string(5));
        assert_eq!("12345.00", decimal_arr.value_as_string(6));
        assert_eq!("0.12", decimal_arr.value_as_string(7));
        assert_eq!("12.23", decimal_arr.value_as_string(8));
        assert!(decimal_arr.is_null(9));
        assert_eq!("0.00", decimal_arr.value_as_string(10));
        assert_eq!("0.00", decimal_arr.value_as_string(11));
        assert!(decimal_arr.is_null(12));
        assert_eq!("-1.23", decimal_arr.value_as_string(13));
        assert_eq!("-1.24", decimal_arr.value_as_string(14));
        assert_eq!("0.00", decimal_arr.value_as_string(15));
        assert_eq!("-123.00", decimal_arr.value_as_string(16));
        assert_eq!("-123.23", decimal_arr.value_as_string(17));
        assert_eq!("-0.12", decimal_arr.value_as_string(18));
        assert_eq!("1.23", decimal_arr.value_as_string(19));
        assert_eq!("1.24", decimal_arr.value_as_string(20));
        assert_eq!("0.00", decimal_arr.value_as_string(21));
        assert_eq!("123.00", decimal_arr.value_as_string(22));
        assert_eq!("123.23", decimal_arr.value_as_string(23));
        assert_eq!("0.12", decimal_arr.value_as_string(24));
        assert!(decimal_arr.is_null(25));
        assert!(decimal_arr.is_null(26));
        assert!(decimal_arr.is_null(27));
        assert_eq!("0.00", decimal_arr.value_as_string(28));
        assert_eq!("0.00", decimal_arr.value_as_string(29));
        assert_eq!("12345.00", decimal_arr.value_as_string(30));
        assert_eq!(decimal_arr.len(), 31);

        // Decimal256
        let output_type = DataType::Decimal256(76, 3);
        assert!(can_cast_types(array.data_type(), &output_type));

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

        assert_eq!("123.450", decimal_arr.value_as_string(0));
        assert_eq!("1.235", decimal_arr.value_as_string(1));
        assert_eq!("0.123", decimal_arr.value_as_string(2));
        assert_eq!("0.127", decimal_arr.value_as_string(3));
        assert_eq!("1.263", decimal_arr.value_as_string(4));
        assert_eq!("12345.000", decimal_arr.value_as_string(5));
        assert_eq!("12345.000", decimal_arr.value_as_string(6));
        assert_eq!("0.123", decimal_arr.value_as_string(7));
        assert_eq!("12.234", decimal_arr.value_as_string(8));
        assert!(decimal_arr.is_null(9));
        assert_eq!("0.000", decimal_arr.value_as_string(10));
        assert_eq!("0.000", decimal_arr.value_as_string(11));
        assert!(decimal_arr.is_null(12));
        assert_eq!("-1.235", decimal_arr.value_as_string(13));
        assert_eq!("-1.236", decimal_arr.value_as_string(14));
        assert_eq!("0.000", decimal_arr.value_as_string(15));
        assert_eq!("-123.000", decimal_arr.value_as_string(16));
        assert_eq!("-123.234", decimal_arr.value_as_string(17));
        assert_eq!("-0.123", decimal_arr.value_as_string(18));
        assert_eq!("1.235", decimal_arr.value_as_string(19));
        assert_eq!("1.236", decimal_arr.value_as_string(20));
        assert_eq!("0.000", decimal_arr.value_as_string(21));
        assert_eq!("123.000", decimal_arr.value_as_string(22));
        assert_eq!("123.234", decimal_arr.value_as_string(23));
        assert_eq!("0.123", decimal_arr.value_as_string(24));
        assert!(decimal_arr.is_null(25));
        assert!(decimal_arr.is_null(26));
        assert!(decimal_arr.is_null(27));
        assert_eq!("0.000", decimal_arr.value_as_string(28));
        assert_eq!("0.000", decimal_arr.value_as_string(29));
        assert_eq!("12345.000", decimal_arr.value_as_string(30));
        assert_eq!(decimal_arr.len(), 31);
    }

    #[test]
    fn test_cast_utf8_to_decimal() {
        let str_array = StringArray::from(vec![
            Some("123.45"),
            Some("1.2345"),
            Some("0.12345"),
            Some("0.1267"),
            Some("1.263"),
            Some("12345.0"),
            Some("12345"),
            Some("000.123"),
            Some("12.234000"),
            None,
            Some(""),
            Some(" "),
            None,
            Some("-1.23499999"),
            Some("-1.23599999"),
            Some("-0.00001"),
            Some("-123"),
            Some("-123.234000"),
            Some("-000.123"),
            Some("+1.23499999"),
            Some("+1.23599999"),
            Some("+0.00001"),
            Some("+123"),
            Some("+123.234000"),
            Some("+000.123"),
            Some("1.-23499999"),
            Some("-1.-23499999"),
            Some("--1.23499999"),
            Some("0"),
            Some("000.000"),
            Some("0000000000000000012345.000"),
        ]);
        let array = Arc::new(str_array) as ArrayRef;

        test_cast_string_to_decimal(array);

        let test_cases = [
            (None, None),
            // (Some(""), None),
            // (Some("   "), None),
            (Some("0"), Some("0")),
            (Some("000.000"), Some("0")),
            (Some("12345"), Some("12345")),
            (Some("000000000000000000000000000012345"), Some("12345")),
            (Some("-123"), Some("-123")),
            (Some("+123"), Some("123")),
        ];
        let inputs = test_cases.iter().map(|entry| entry.0).collect::<Vec<_>>();
        let expected = test_cases.iter().map(|entry| entry.1).collect::<Vec<_>>();

        let array = Arc::new(StringArray::from(inputs)) as ArrayRef;
        test_cast_string_to_decimal_scale_zero(array, &expected);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal() {
        let str_array = LargeStringArray::from(vec![
            Some("123.45"),
            Some("1.2345"),
            Some("0.12345"),
            Some("0.1267"),
            Some("1.263"),
            Some("12345.0"),
            Some("12345"),
            Some("000.123"),
            Some("12.234000"),
            None,
            Some(""),
            Some(" "),
            None,
            Some("-1.23499999"),
            Some("-1.23599999"),
            Some("-0.00001"),
            Some("-123"),
            Some("-123.234000"),
            Some("-000.123"),
            Some("+1.23499999"),
            Some("+1.23599999"),
            Some("+0.00001"),
            Some("+123"),
            Some("+123.234000"),
            Some("+000.123"),
            Some("1.-23499999"),
            Some("-1.-23499999"),
            Some("--1.23499999"),
            Some("0"),
            Some("000.000"),
            Some("0000000000000000012345.000"),
        ]);
        let array = Arc::new(str_array) as ArrayRef;

        test_cast_string_to_decimal(array);

        let test_cases = [
            (None, None),
            (Some(""), None),
            (Some("   "), None),
            (Some("0"), Some("0")),
            (Some("000.000"), Some("0")),
            (Some("12345"), Some("12345")),
            (Some("000000000000000000000000000012345"), Some("12345")),
            (Some("-123"), Some("-123")),
            (Some("+123"), Some("123")),
        ];
        let inputs = test_cases.iter().map(|entry| entry.0).collect::<Vec<_>>();
        let expected = test_cases.iter().map(|entry| entry.1).collect::<Vec<_>>();

        let array = Arc::new(LargeStringArray::from(inputs)) as ArrayRef;
        test_cast_string_to_decimal_scale_zero(array, &expected);
    }

    fn test_cast_string_to_decimal_scale_zero(
        array: ArrayRef,
        expected_as_string: &[Option<&str>],
    ) {
        // Decimal128
        let output_type = DataType::Decimal128(38, 0);
        assert!(can_cast_types(array.data_type(), &output_type));
        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();
        assert_decimal_array_contents(decimal_arr, expected_as_string);

        // Decimal256
        let output_type = DataType::Decimal256(76, 0);
        assert!(can_cast_types(array.data_type(), &output_type));
        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal256Type>();
        assert_decimal_array_contents(decimal_arr, expected_as_string);
    }

    fn assert_decimal_array_contents<T>(
        array: &PrimitiveArray<T>,
        expected_as_string: &[Option<&str>],
    ) where
        T: DecimalType + ArrowPrimitiveType,
    {
        assert_eq!(array.len(), expected_as_string.len());
        for (i, expected) in expected_as_string.iter().enumerate() {
            let actual = if array.is_null(i) {
                None
            } else {
                Some(array.value_as_string(i))
            };
            let actual = actual.as_ref().map(|s| s.as_ref());
            assert_eq!(*expected, actual, "Expected at position {}", i);
        }
    }

    #[test]
    fn test_cast_invalid_utf8_to_decimal() {
        let str_array = StringArray::from(vec!["4.4.5", ". 0.123"]);
        let array = Arc::new(str_array) as ArrayRef;

        // Safe cast
        let output_type = DataType::Decimal128(38, 2);
        let casted_array = cast(&array, &output_type).unwrap();
        assert!(casted_array.is_null(0));
        assert!(casted_array.is_null(1));

        let output_type = DataType::Decimal256(76, 2);
        let casted_array = cast(&array, &output_type).unwrap();
        assert!(casted_array.is_null(0));
        assert!(casted_array.is_null(1));

        // Non-safe cast
        let output_type = DataType::Decimal128(38, 2);
        let str_array = StringArray::from(vec!["4.4.5"]);
        let array = Arc::new(str_array) as ArrayRef;
        let option = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
        assert!(casted_err
            .to_string()
            .contains("Cannot cast string '4.4.5' to value of Decimal128(38, 10) type"));

        let str_array = StringArray::from(vec![". 0.123"]);
        let array = Arc::new(str_array) as ArrayRef;
        let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
        assert!(casted_err
            .to_string()
            .contains("Cannot cast string '. 0.123' to value of Decimal128(38, 10) type"));
    }

    fn test_cast_string_to_decimal128_overflow(overflow_array: ArrayRef) {
        let output_type = DataType::Decimal128(38, 2);
        let casted_array = cast(&overflow_array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert!(decimal_arr.is_null(0));
        assert!(decimal_arr.is_null(1));
        assert!(decimal_arr.is_null(2));
        assert_eq!(
            "999999999999999999999999999999999999.99",
            decimal_arr.value_as_string(3)
        );
        assert_eq!(
            "100000000000000000000000000000000000.00",
            decimal_arr.value_as_string(4)
        );
    }

    #[test]
    fn test_cast_string_to_decimal128_precision_overflow() {
        let array = StringArray::from(vec!["1000".to_string()]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(10, 8),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal128(10, 8),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Invalid argument error: 100000000000 is too large to store in a Decimal128 of precision 10. Max is 9999999999", err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_utf8_to_decimal128_overflow() {
        let overflow_str_array = StringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal128_overflow(overflow_array);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal128_overflow() {
        let overflow_str_array = LargeStringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal128_overflow(overflow_array);
    }

    fn test_cast_string_to_decimal256_overflow(overflow_array: ArrayRef) {
        let output_type = DataType::Decimal256(76, 2);
        let casted_array = cast(&overflow_array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

        assert_eq!(
            "170141183460469231731687303715884105727.00",
            decimal_arr.value_as_string(0)
        );
        assert_eq!(
            "-170141183460469231731687303715884105728.00",
            decimal_arr.value_as_string(1)
        );
        assert_eq!(
            "99999999999999999999999999999999999999.00",
            decimal_arr.value_as_string(2)
        );
        assert_eq!(
            "999999999999999999999999999999999999.99",
            decimal_arr.value_as_string(3)
        );
        assert_eq!(
            "100000000000000000000000000000000000.00",
            decimal_arr.value_as_string(4)
        );
        assert!(decimal_arr.is_null(5));
        assert!(decimal_arr.is_null(6));
    }

    #[test]
    fn test_cast_string_to_decimal256_precision_overflow() {
        let array = StringArray::from(vec!["1000".to_string()]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(10, 8),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal256(10, 8),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Invalid argument error: 100000000000 is too large to store in a Decimal256 of precision 10. Max is 9999999999", err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_utf8_to_decimal256_overflow() {
        let overflow_str_array = StringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
            i256::MAX.to_string(),
            i256::MIN.to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal256_overflow(overflow_array);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal256_overflow() {
        let overflow_str_array = LargeStringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
            i256::MAX.to_string(),
            i256::MIN.to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal256_overflow(overflow_array);
    }

    #[test]
    fn test_cast_outside_supported_range_for_nanoseconds() {
        const EXPECTED_ERROR_MESSAGE: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

        let array = StringArray::from(vec![Some("1650-01-01 01:01:01.000001")]);

        let cast_options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };

        let result = cast_string_to_timestamp::<i32, TimestampNanosecondType>(
            &array,
            &None::<Arc<str>>,
            &cast_options,
        );

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

    #[test]
    fn test_cast_decimal_to_string() {
        assert!(can_cast_types(
            &DataType::Decimal128(10, 4),
            &DataType::Utf8View
        ));
        assert!(can_cast_types(
            &DataType::Decimal256(38, 10),
            &DataType::Utf8View
        ));

        macro_rules! assert_decimal_values {
            ($array:expr) => {
                let c = $array;
                assert_eq!("1123.454", c.value(0));
                assert_eq!("2123.456", c.value(1));
                assert_eq!("-3123.453", c.value(2));
                assert_eq!("-3123.456", c.value(3));
                assert_eq!("0.000", c.value(4));
                assert_eq!("0.123", c.value(5));
                assert_eq!("1234.567", c.value(6));
                assert_eq!("-1234.567", c.value(7));
                assert!(c.is_null(8));
            };
        }

        fn test_decimal_to_string<IN: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
            output_type: DataType,
            array: PrimitiveArray<IN>,
        ) {
            let b = cast(&array, &output_type).unwrap();

            assert_eq!(b.data_type(), &output_type);
            match b.data_type() {
                DataType::Utf8View => {
                    let c = b.as_string_view();
                    assert_decimal_values!(c);
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let c = b.as_string::<OffsetSize>();
                    assert_decimal_values!(c);
                }
                _ => (),
            }
        }

        let array128: Vec<Option<i128>> = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            Some(0),
            Some(123),
            Some(123456789),
            Some(-123456789),
            None,
        ];
        let array256: Vec<Option<i256>> = array128
            .iter()
            .map(|num| num.map(i256::from_i128))
            .collect();

        test_decimal_to_string::<Decimal128Type, i32>(
            DataType::Utf8View,
            create_decimal128_array(array128.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<Decimal128Type, i32>(
            DataType::Utf8,
            create_decimal128_array(array128.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<Decimal128Type, i64>(
            DataType::LargeUtf8,
            create_decimal128_array(array128, 7, 3).unwrap(),
        );

        test_decimal_to_string::<Decimal256Type, i32>(
            DataType::Utf8View,
            create_decimal256_array(array256.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<Decimal256Type, i32>(
            DataType::Utf8,
            create_decimal256_array(array256.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<Decimal256Type, i64>(
            DataType::LargeUtf8,
            create_decimal256_array(array256, 7, 3).unwrap(),
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128_precision_overflow() {
        let array = Int64Array::from(vec![1234567]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(7, 3),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal128(7, 3),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Invalid argument error: 1234567000 is too large to store in a Decimal128 of precision 7. Max is 9999999", err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_numeric_to_decimal256_precision_overflow() {
        let array = Int64Array::from(vec![1234567]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(7, 3),
            &CastOptions {
                safe: true,
                format_options: FormatOptions::default(),
            },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal256(7, 3),
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        );
        assert_eq!("Invalid argument error: 1234567000 is too large to store in a Decimal256 of precision 7. Max is 9999999", err.unwrap_err().to_string());
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

    #[test]
    fn test_nested_list() {
        let mut list = ListBuilder::new(Int32Builder::new());
        list.append_value([Some(1), Some(2), Some(3)]);
        list.append_value([Some(4), None, Some(6)]);
        let list = list.finish();

        let to_field = Field::new("nested", list.data_type().clone(), false);
        let to = DataType::List(Arc::new(to_field));
        let out = cast(&list, &to).unwrap();
        let opts = FormatOptions::default().with_null("null");
        let formatted = ArrayFormatter::try_new(out.as_ref(), &opts).unwrap();

        assert_eq!(formatted.value(0).to_string(), "[[1], [2], [3]]");
        assert_eq!(formatted.value(1).to_string(), "[[4], [null], [6]]");
    }

    #[test]
    fn test_nested_list_cast() {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        builder.append_value([Some([Some(1), Some(2), None]), None]);
        builder.append_value([None, Some([]), None]);
        builder.append_null();
        builder.append_value([Some([Some(2), Some(3)])]);
        let start = builder.finish();

        let mut builder = LargeListBuilder::new(LargeListBuilder::new(Int8Builder::new()));
        builder.append_value([Some([Some(1), Some(2), None]), None]);
        builder.append_value([None, Some([]), None]);
        builder.append_null();
        builder.append_value([Some([Some(2), Some(3)])]);
        let expected = builder.finish();

        let actual = cast(&start, expected.data_type()).unwrap();
        assert_eq!(actual.as_ref(), &expected);
    }

    const CAST_OPTIONS: CastOptions<'static> = CastOptions {
        safe: true,
        format_options: FormatOptions::new(),
    };

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_const_options() {
        assert!(CAST_OPTIONS.safe)
    }

    #[test]
    fn test_list_format_options() {
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default().with_null("null"),
        };
        let array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(0), None, Some(2)]),
        ]);
        let a = cast_with_options(&array, &DataType::Utf8, &options).unwrap();
        let r: Vec<_> = a.as_string::<i32>().iter().flatten().collect();
        assert_eq!(r, &["[0, 1, 2]", "[0, null, 2]"]);
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
            assert_eq!(
                cast_with_options(&array, &dt, &cast_options)
                    .unwrap_err()
                    .to_string(),
                "Parser error: Invalid timezone \"ZZTOP\": only offset based timezones supported without chrono-tz feature"
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
            r#"Cast error: Casting from Struct([Field { name: "a", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]) to Utf8 not supported"#,
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_cast_non_struct_to_struct() {
        let array = StringArray::from(vec!["a", "b"]);
        let to_type = DataType::Struct(vec![Field::new("a", DataType::Boolean, false)].into());
        let result = cast(&array, &to_type);
        assert_eq!(
            r#"Cast error: Casting from Utf8 to Struct([Field { name: "a", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]) not supported"#,
            result.unwrap_err().to_string()
        );
    }

    fn run_decimal_cast_test_case_between_multiple_types(t: DecimalCastTestConfig) {
        run_decimal_cast_test_case::<Decimal128Type, Decimal128Type>(t.clone());
        run_decimal_cast_test_case::<Decimal128Type, Decimal256Type>(t.clone());
        run_decimal_cast_test_case::<Decimal256Type, Decimal128Type>(t.clone());
        run_decimal_cast_test_case::<Decimal256Type, Decimal256Type>(t.clone());
    }

    #[test]
    fn test_decimal_to_decimal_coverage() {
        let test_cases = [
            // increase precision, increase scale, infallible
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 1,
                input_repr: 99999, // 9999.9
                output_prec: 10,
                output_scale: 6,
                expected_output_repr: Ok(9999900000), // 9999.900000
            },
            // increase precision, increase scale, fallible, safe
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 1,
                input_repr: 99, // 9999.9
                output_prec: 7,
                output_scale: 6,
                expected_output_repr: Ok(9900000), // 9.900000
            },
            // increase precision, increase scale, fallible, unsafe
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 1,
                input_repr: 99999, // 9999.9
                output_prec: 7,
                output_scale: 6,
                expected_output_repr: Err("Invalid argument error: 9999900000 is too large to store in a {} of precision 7. Max is 9999999".to_string()) // max is 9.999999
            },
            // increase precision, decrease scale, always infallible
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 3,
                input_repr: 99999, // 99.999
                output_prec: 10,
                output_scale: 2,
                expected_output_repr: Ok(10000), // 100.00
            },
            // increase precision, decrease scale, no rouding
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 3,
                input_repr: 99994, // 99.994
                output_prec: 10,
                output_scale: 2,
                expected_output_repr: Ok(9999), // 99.99
            },
            // increase precision, don't change scale, always infallible
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 3,
                input_repr: 99999, // 99.999
                output_prec: 10,
                output_scale: 3,
                expected_output_repr: Ok(99999), // 99.999
            },
            // decrease precision, increase scale, safe
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 999999, // 9.99999
                output_prec: 8,
                output_scale: 7,
                expected_output_repr: Ok(99999900), // 9.9999900
            },
            // decrease precision, increase scale, unsafe
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 9999999, // 99.99999
                output_prec: 8,
                output_scale: 7,
                expected_output_repr: Err("Invalid argument error: 999999900 is too large to store in a {} of precision 8. Max is 99999999".to_string()) // max is 9.9999999
            },
            // decrease precision, decrease scale, safe, infallible
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 4,
                input_repr: 9999999, // 999.9999
                output_prec: 6,
                output_scale: 2,
                expected_output_repr: Ok(100000),
            },
            // decrease precision, decrease scale, safe, fallible
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 12345678, // 123.45678
                output_prec: 8,
                output_scale: 3,
                expected_output_repr: Ok(123457), // 123.457
            },
            // decrease precision, decrease scale, unsafe
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 9999999, // 99.99999
                output_prec: 4,
                output_scale: 3,
                expected_output_repr: Err("Invalid argument error: 100000 is too large to store in a {} of precision 4. Max is 9999".to_string()) // max is 9.999
            },
            // decrease precision, same scale, safe
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 999999, // 9.99999
                output_prec: 6,
                output_scale: 5,
                expected_output_repr: Ok(999999), // 9.99999
            },
            // decrease precision, same scale, unsafe
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 5,
                input_repr: 9999999, // 99.99999
                output_prec: 6,
                output_scale: 5,
                expected_output_repr: Err("Invalid argument error: 9999999 is too large to store in a {} of precision 6. Max is 999999".to_string()) // max is 9.99999
            },
            // same precision, increase scale, safe
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 4,
                input_repr: 12345, // 1.2345
                output_prec: 7,
                output_scale: 6,
                expected_output_repr: Ok(1234500), // 1.234500
            },
            // same precision, increase scale, unsafe
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 4,
                input_repr: 123456, // 12.3456
                output_prec: 7,
                output_scale: 6,
                expected_output_repr: Err("Invalid argument error: 12345600 is too large to store in a {} of precision 7. Max is 9999999".to_string()) // max is 9.99999
            },
            // same precision, decrease scale, infallible
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 5,
                input_repr: 1234567, // 12.34567
                output_prec: 7,
                output_scale: 4,
                expected_output_repr: Ok(123457), // 12.3457
            },
            // same precision, same scale, infallible
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 5,
                input_repr: 9999999, // 99.99999
                output_prec: 7,
                output_scale: 5,
                expected_output_repr: Ok(9999999), // 99.99999
            },
            // precision increase, input scale & output scale = 0, infallible
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 0,
                input_repr: 1234567, // 1234567
                output_prec: 8,
                output_scale: 0,
                expected_output_repr: Ok(1234567), // 1234567
            },
            // precision decrease, input scale & output scale = 0, failure
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 0,
                input_repr: 1234567, // 1234567
                output_prec: 6,
                output_scale: 0,
                expected_output_repr: Err("Invalid argument error: 1234567 is too large to store in a {} of precision 6. Max is 999999".to_string())
            },
            // precision decrease, input scale & output scale = 0, success
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 0,
                input_repr: 123456, // 123456
                output_prec: 6,
                output_scale: 0,
                expected_output_repr: Ok(123456), // 123456
            },
        ];

        for t in test_cases {
            run_decimal_cast_test_case_between_multiple_types(t);
        }
    }

    #[test]
    fn test_decimal_to_decimal_increase_scale_and_precision_unchecked() {
        let test_cases = [
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 0,
                input_repr: 99999,
                output_prec: 10,
                output_scale: 5,
                expected_output_repr: Ok(9999900000),
            },
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 0,
                input_repr: -99999,
                output_prec: 10,
                output_scale: 5,
                expected_output_repr: Ok(-9999900000),
            },
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 2,
                input_repr: 99999,
                output_prec: 10,
                output_scale: 5,
                expected_output_repr: Ok(99999000),
            },
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: -2,
                input_repr: -99999,
                output_prec: 10,
                output_scale: 3,
                expected_output_repr: Ok(-9999900000),
            },
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 3,
                input_repr: -12345,
                output_prec: 6,
                output_scale: 5,
                expected_output_repr: Err("Invalid argument error: -1234500 is too small to store in a {} of precision 6. Min is -999999".to_string())
            },
        ];

        for t in test_cases {
            run_decimal_cast_test_case_between_multiple_types(t);
        }
    }

    #[test]
    fn test_decimal_to_decimal_decrease_scale_and_precision_unchecked() {
        let test_cases = [
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 0,
                input_repr: 99999,
                output_scale: -3,
                output_prec: 3,
                expected_output_repr: Ok(100),
            },
            DecimalCastTestConfig {
                input_prec: 5,
                input_scale: 0,
                input_repr: -99999,
                output_prec: 1,
                output_scale: -5,
                expected_output_repr: Ok(-1),
            },
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 2,
                input_repr: 123456789,
                output_prec: 5,
                output_scale: -2,
                expected_output_repr: Ok(12346),
            },
            DecimalCastTestConfig {
                input_prec: 10,
                input_scale: 4,
                input_repr: -9876543210,
                output_prec: 7,
                output_scale: 0,
                expected_output_repr: Ok(-987654),
            },
            DecimalCastTestConfig {
                input_prec: 7,
                input_scale: 4,
                input_repr: 9999999,
                output_prec: 6,
                output_scale: 3,
                expected_output_repr:
                    Err("Invalid argument error: 1000000 is too large to store in a {} of precision 6. Max is 999999".to_string()),
            },
        ];
        for t in test_cases {
            run_decimal_cast_test_case_between_multiple_types(t);
        }
    }

    #[test]
    fn test_decimal_to_decimal_throw_error_on_precision_overflow_same_scale() {
        let array = vec![Some(123456789)];
        let array = create_decimal128_array(array, 24, 2).unwrap();
        let input_type = DataType::Decimal128(24, 2);
        let output_type = DataType::Decimal128(6, 2);
        assert!(can_cast_types(&input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);
        assert_eq!(result.unwrap_err().to_string(),
                   "Invalid argument error: 123456789 is too large to store in a Decimal128 of precision 6. Max is 999999");
    }

    #[test]
    fn test_decimal_to_decimal_same_scale() {
        let array = vec![Some(520)];
        let array = create_decimal128_array(array, 4, 2).unwrap();
        let input_type = DataType::Decimal128(4, 2);
        let output_type = DataType::Decimal128(3, 2);
        assert!(can_cast_types(&input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);
        assert_eq!(
            result.unwrap().as_primitive::<Decimal128Type>().value(0),
            520
        );

        // Cast 0 of decimal(3, 0) type to decimal(2, 0)
        assert_eq!(
            &cast(
                &create_decimal128_array(vec![Some(0)], 3, 0).unwrap(),
                &DataType::Decimal128(2, 0)
            )
            .unwrap(),
            &(Arc::new(create_decimal128_array(vec![Some(0)], 2, 0).unwrap()) as ArrayRef)
        );
    }

    #[test]
    fn test_decimal_to_decimal_throw_error_on_precision_overflow_lower_scale() {
        let array = vec![Some(123456789)];
        let array = create_decimal128_array(array, 24, 4).unwrap();
        let input_type = DataType::Decimal128(24, 4);
        let output_type = DataType::Decimal128(6, 2);
        assert!(can_cast_types(&input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);
        assert_eq!(result.unwrap_err().to_string(),
                   "Invalid argument error: 1234568 is too large to store in a Decimal128 of precision 6. Max is 999999");
    }

    #[test]
    fn test_decimal_to_decimal_throw_error_on_precision_overflow_greater_scale() {
        let array = vec![Some(123456789)];
        let array = create_decimal128_array(array, 24, 2).unwrap();
        let input_type = DataType::Decimal128(24, 2);
        let output_type = DataType::Decimal128(6, 3);
        assert!(can_cast_types(&input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);
        assert_eq!(result.unwrap_err().to_string(),
                   "Invalid argument error: 1234567890 is too large to store in a Decimal128 of precision 6. Max is 999999");
    }

    #[test]
    fn test_decimal_to_decimal_throw_error_on_precision_overflow_diff_type() {
        let array = vec![Some(123456789)];
        let array = create_decimal128_array(array, 24, 2).unwrap();
        let input_type = DataType::Decimal128(24, 2);
        let output_type = DataType::Decimal256(6, 2);
        assert!(can_cast_types(&input_type, &output_type));

        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_with_options(&array, &output_type, &options);
        assert_eq!(result.unwrap_err().to_string(),
                   "Invalid argument error: 123456789 is too large to store in a Decimal256 of precision 6. Max is 999999");
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
}

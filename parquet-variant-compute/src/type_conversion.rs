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

//! Module for transforming a typed arrow `Array` to `VariantArray`.

use arrow::array::ArrowNativeTypeOp;
use arrow::compute::{
    CastOptions, DecimalCast, cast_num_to_bool, cast_single_string_to_boolean_default, num_cast,
    parse_string_to_decimal_native, rescale_decimal, single_bool_to_numeric,
    single_decimal_to_float_lossy, single_float_to_decimal,
};
use arrow::datatypes::{
    self, ArrowPrimitiveType, ArrowTimestampType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, DecimalType, format_decimal_str,
};
use arrow::error::{ArrowError, Result};
use arrow::util::display::{write_temporal_display, write_timestamp};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use half::f16;
use lexical_core::FormattedSize;
use num_traits::NumCast;
use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};
use ryu::Float;
use std::fmt::Write;

/// Extension trait for Arrow primitive types that can extract their native value from a Variant
pub(crate) trait PrimitiveFromVariant: ArrowPrimitiveType {
    fn from_variant(variant: &Variant<'_, '_>, shred: bool) -> Option<Self::Native>;
}

/// Extension trait for Arrow timestamp types that can extract their native value from a Variant
/// We can't use [`PrimitiveFromVariant`] directly because we need _two_ implementations for each
/// timestamp type -- the `NTZ` param here.
pub(crate) trait TimestampFromVariant<const NTZ: bool>: ArrowTimestampType {
    fn from_variant(variant: &Variant<'_, '_>, shred: bool) -> Option<Self::Native>;
}

/// Cast a single `Variant` value with safe/strict semantics.
///
/// Returns `Ok(Some(_))` on successful conversion.
/// Returns `Ok(None)` when conversion fails in safe mode or the source value is `Variant::Null`.
/// Returns `Err(_)` when conversion fails in strict mode.
pub(crate) fn variant_cast_with_options<'a, 'm, 'v, T>(
    variant: &'a Variant<'m, 'v>,
    cast_options: &CastOptions<'_>,
    cast: impl FnOnce(&'a Variant<'m, 'v>) -> Option<T>,
) -> Result<Option<T>> {
    if let Some(value) = cast(variant) {
        Ok(Some(value))
    } else if matches!(variant, Variant::Null) || cast_options.safe {
        Ok(None)
    } else {
        Err(ArrowError::CastError(format!(
            "Failed to cast variant value {variant:?}"
        )))
    }
}

/// Macro to generate PrimitiveFromVariant implementations for Arrow primitive types
macro_rules! impl_primitive_from_variant {
    ($arrow_type:ty, $shred_fun:expr, $get_method:ident $(, $cast_fn:expr)?) => {
        impl PrimitiveFromVariant for $arrow_type {
            fn from_variant(variant: &Variant<'_, '_>, shred: bool) -> Option<Self::Native> {
                let value = match shred {
                    true => $shred_fun(variant),
                    false => $get_method(variant),
                };
                $( let value = value.and_then($cast_fn); )?
                value
            }
        }
    };
}

macro_rules! impl_timestamp_from_variant {
    ($timestamp_type:ty, $shred_fun:expr, $variant_method:expr, ntz=$ntz:ident, $cast_fn:expr $(,)?) => {
        impl TimestampFromVariant<{ $ntz }> for $timestamp_type {
            fn from_variant(variant: &Variant<'_, '_>, shred: bool) -> Option<Self::Native> {
                let value = match shred {
                    true => ($shred_fun)(variant),
                    false => $variant_method(variant),
                };

                value.and_then($cast_fn)
            }
        }
    };
}

fn convert_to_timestamp_nano(value: &Variant) -> Option<DateTime<Utc>> {
    match *value {
        Variant::TimestampNanos(d) | Variant::TimestampMicros(d) => Some(d),
        _ => None,
    }
}

fn convert_to_timestamp_ntz_nano(value: &Variant) -> Option<NaiveDateTime> {
    match *value {
        Variant::TimestampNtzNanos(d) | Variant::TimestampNtzMicros(d) => Some(d),
        _ => None,
    }
}

enum NumericKind {
    Integer,
    Float,
}

trait DecimalCastTarget: NumCast + Default {
    const KIND: NumericKind;
}

macro_rules! impl_decimal_cast_target {
    ($raw_type: ident, $target_kind:expr) => {
        impl DecimalCastTarget for $raw_type {
            const KIND: NumericKind = $target_kind;
        }
    };
}

impl_decimal_cast_target!(i8, NumericKind::Integer);
impl_decimal_cast_target!(i16, NumericKind::Integer);
impl_decimal_cast_target!(i32, NumericKind::Integer);
impl_decimal_cast_target!(i64, NumericKind::Integer);
impl_decimal_cast_target!(u8, NumericKind::Integer);
impl_decimal_cast_target!(u16, NumericKind::Integer);
impl_decimal_cast_target!(u32, NumericKind::Integer);
impl_decimal_cast_target!(u64, NumericKind::Integer);
impl_decimal_cast_target!(f16, NumericKind::Float);
impl_decimal_cast_target!(f32, NumericKind::Float);
impl_decimal_cast_target!(f64, NumericKind::Float);

/// Converts a boolean or numeric variant(integers, floating-point, and decimals)
/// to the specified numeric type `T`.
///
/// Uses Arrow's casting logic to perform the conversion. Returns `Some(T)` if
/// the conversion succeeds, `None` if the variant can't be casted to type `T`.
fn as_num<T>(variant: &Variant) -> Option<T>
where
    T: DecimalCastTarget,
{
    match *variant {
        Variant::BooleanFalse => single_bool_to_numeric(false),
        Variant::BooleanTrue => single_bool_to_numeric(true),
        Variant::Int8(i) => num_cast(i),
        Variant::Int16(i) => num_cast(i),
        Variant::Int32(i) => num_cast(i),
        Variant::Int64(i) => num_cast(i),
        Variant::Float(f) => num_cast(f),
        Variant::Double(d) => num_cast(d),
        Variant::Decimal4(d) => {
            cast_decimal_to_num::<Decimal32Type, T, _>(d.integer(), d.scale(), |x| x as f64)
        }
        Variant::Decimal8(d) => {
            cast_decimal_to_num::<Decimal64Type, T, _>(d.integer(), d.scale(), |x| x as f64)
        }
        Variant::Decimal16(d) => {
            cast_decimal_to_num::<Decimal128Type, T, _>(d.integer(), d.scale(), |x| x as f64)
        }
        _ => None,
    }
}

fn cast_decimal_to_num<D, T, F>(raw: D::Native, scale: u8, as_float: F) -> Option<T>
where
    D: DecimalType,
    D::Native: NumCast + ArrowNativeTypeOp,
    T: DecimalCastTarget,
    F: Fn(D::Native) -> f64,
{
    let base: D::Native = NumCast::from(10)?;

    let div = base.pow_checked(<u32 as From<u8>>::from(scale)).ok()?;
    match T::KIND {
        NumericKind::Integer => raw
            .div_checked(div)
            .ok()
            .and_then(<T as NumCast>::from::<D::Native>),
        NumericKind::Float => T::from(single_decimal_to_float_lossy::<D, _>(
            &as_float,
            raw,
            <i32 as From<u8>>::from(scale),
        )),
    }
}

fn cast_naive_date(value: &Variant<'_, '_>) -> Option<NaiveDate> {
    value.as_naive_date()
}

fn cast_time_utc(value: &Variant<'_, '_>) -> Option<NaiveTime> {
    value.as_time_utc()
}

// helper function for the types that would never be the shred target type.
fn always_none<T>(_input: &Variant) -> Option<T> {
    None
}

impl_primitive_from_variant!(datatypes::Int32Type, Variant::as_int32, as_num);
impl_primitive_from_variant!(datatypes::Int16Type, Variant::as_int16, as_num);
impl_primitive_from_variant!(datatypes::Int8Type, Variant::as_int8, as_num);
impl_primitive_from_variant!(datatypes::Int64Type, Variant::as_int64, as_num);
impl_primitive_from_variant!(datatypes::UInt8Type, always_none, as_num);
impl_primitive_from_variant!(datatypes::UInt16Type, always_none, as_num);
impl_primitive_from_variant!(datatypes::UInt32Type, always_none, as_num);
impl_primitive_from_variant!(datatypes::UInt64Type, always_none, as_num);
impl_primitive_from_variant!(datatypes::Float16Type, always_none, as_num);
impl_primitive_from_variant!(datatypes::Float32Type, Variant::as_f32, as_num);
impl_primitive_from_variant!(datatypes::Float64Type, Variant::as_f64, as_num);
impl_primitive_from_variant!(
    datatypes::Date32Type,
    Variant::as_naive_date,
    cast_naive_date,
    |v| { Some(datatypes::Date32Type::from_naive_date(v)) }
);
impl_primitive_from_variant!(
    datatypes::Date64Type,
    Variant::as_naive_date,
    cast_naive_date,
    |v| { Some(datatypes::Date64Type::from_naive_date(v)) }
);
impl_primitive_from_variant!(
    datatypes::Time32SecondType,
    always_none, // would never shred to Time32SecondType
    cast_time_utc,
    |v| {
        // Return None if there are leftover nanoseconds
        if v.nanosecond() != 0 {
            None
        } else {
            Some(v.num_seconds_from_midnight() as i32)
        }
    }
);
impl_primitive_from_variant!(
    datatypes::Time32MillisecondType,
    always_none, // would never shred to Time32MillisecondType
    cast_time_utc,
    |v| {
        // Return None if there are leftover microseconds
        if v.nanosecond() % 1_000_000 != 0 {
            None
        } else {
            Some(
                (v.num_seconds_from_midnight() * 1_000) as i32
                    + (v.nanosecond() / 1_000_000) as i32,
            )
        }
    }
);
impl_primitive_from_variant!(
    datatypes::Time64MicrosecondType,
    Variant::as_time_utc,
    cast_time_utc,
    |v| { Some(v.num_seconds_from_midnight() as i64 * 1_000_000 + v.nanosecond() as i64 / 1_000) }
);
impl_primitive_from_variant!(
    datatypes::Time64NanosecondType,
    always_none, // would never shred to Time64NanosecondType
    cast_time_utc,
    |v| {
        // convert micro to nano seconds
        Some(v.num_seconds_from_midnight() as i64 * 1_000_000_000 + v.nanosecond() as i64)
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampSecondType,
    always_none, // would never shred to TimestampSecondType
    convert_to_timestamp_ntz_nano,
    ntz = true,
    |timestamp| {
        // Return None if there are leftover nanoseconds
        if timestamp.nanosecond() != 0 {
            None
        } else {
            Self::from_naive_datetime(timestamp, None)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampSecondType,
    always_none, // would never shred to TimestampSecondType
    convert_to_timestamp_nano,
    ntz = false,
    |timestamp| {
        // Return None if there are leftover nanoseconds
        if timestamp.nanosecond() != 0 {
            None
        } else {
            Self::from_naive_datetime(timestamp.naive_utc(), None)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMillisecondType,
    always_none, // would never shred to TimestampMillisecondType
    convert_to_timestamp_ntz_nano,
    ntz = true,
    |timestamp| {
        // Return None if there are leftover microseconds
        if timestamp.nanosecond() % 1_000_000 != 0 {
            None
        } else {
            Self::from_naive_datetime(timestamp, None)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMillisecondType,
    always_none, // would never shred to TimestampMillisecondType
    convert_to_timestamp_nano,
    ntz = false,
    |timestamp| {
        // Return None if there are leftover microseconds
        if timestamp.nanosecond() % 1_000_000 != 0 {
            None
        } else {
            Self::from_naive_datetime(timestamp.naive_utc(), None)
        }
    }
);
impl_timestamp_from_variant!(
    datatypes::TimestampMicrosecondType,
    Variant::as_timestamp_ntz_micros,
    Variant::as_timestamp_ntz_micros,
    ntz = true,
    |timestamp| Self::from_naive_datetime(timestamp, None),
);
impl_timestamp_from_variant!(
    datatypes::TimestampMicrosecondType,
    Variant::as_timestamp_micros,
    Variant::as_timestamp_micros,
    ntz = false,
    |timestamp| Self::from_naive_datetime(timestamp.naive_utc(), None)
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    Variant::as_timestamp_ntz_nanos,
    convert_to_timestamp_ntz_nano,
    ntz = true,
    |timestamp| Self::from_naive_datetime(timestamp, None)
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    Variant::as_timestamp_nanos,
    convert_to_timestamp_nano,
    ntz = false,
    |timestamp| Self::from_naive_datetime(timestamp.naive_utc(), None)
);

/// Returns the unscaled integer representation for Arrow decimal type `O`
/// from a `Variant`.
///
/// - `precision` and `scale` specify the target Arrow decimal parameters
/// - Integer variants (`Int8/16/32/64`) are treated as decimals with scale 0
/// - Floating point variants (`Float/Double`) are converted to decimals with the given scale
/// - String variants (`String/ShortString`) are parsed as decimals with the given scale
/// - Decimal variants (`Decimal4/8/16`) use their embedded precision and scale
///
/// The value is rescaled to (`precision`, `scale`) using `rescale_decimal` for integers,
/// `single_float_to_decimal` for floats, and `parse_string_to_decimal_native` for strings.
/// returns `None` if it cannot fit the requested precision.
pub(crate) fn variant_to_unscaled_decimal<O>(
    variant: &Variant<'_, '_>,
    precision: u8,
    scale: i8,
) -> Option<O::Native>
where
    O: DecimalType,
    O::Native: DecimalCast,
{
    let mul = 10_f64.powi(scale as i32);

    match variant {
        Variant::Int8(i) => rescale_decimal::<Decimal32Type, O>(
            *i as i32,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int16(i) => rescale_decimal::<Decimal32Type, O>(
            *i as i32,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int32(i) => rescale_decimal::<Decimal32Type, O>(
            *i,
            VariantDecimal4::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Int64(i) => rescale_decimal::<Decimal64Type, O>(
            *i,
            VariantDecimal8::MAX_PRECISION,
            0,
            precision,
            scale,
        ),
        Variant::Float(f) => single_float_to_decimal::<O>(<f64 as From<f32>>::from(*f), mul),
        Variant::Double(f) => single_float_to_decimal::<O>(*f, mul),
        // arrow-cast only support cast string to decimal with scale >=0 for now
        // Please see `cast_string_to_decimal` in arrow-cast/src/cast/decimal.rs for more detail
        Variant::String(v) if scale >= 0 => parse_string_to_decimal_native::<O>(v, scale as _).ok(),
        Variant::ShortString(v) if scale >= 0 => {
            parse_string_to_decimal_native::<O>(v, scale as _).ok()
        }
        Variant::Decimal4(d) => rescale_decimal::<Decimal32Type, O>(
            d.integer(),
            VariantDecimal4::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        Variant::Decimal8(d) => rescale_decimal::<Decimal64Type, O>(
            d.integer(),
            VariantDecimal8::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        Variant::Decimal16(d) => rescale_decimal::<Decimal128Type, O>(
            d.integer(),
            VariantDecimal16::MAX_PRECISION,
            d.scale() as i8,
            precision,
            scale,
        ),
        _ => None,
    }
}

/// Returns the unscaled integer representation for Arrow decimal type `O` from a `Variant`.
///
/// Unlike `variant_to_unscaled_decimal`, this function only accepts integer and decimal
/// variants. Decimal values may be rescaled only when the conversion is exact, as verified
/// by converting the result back to the original scale.
pub(crate) fn shred_variant_to_unscaled_decimal<O>(
    variant: &Variant<'_, '_>,
    precision: u8,
    scale: i8,
) -> Option<O::Native>
where
    O: ShredDecimalVariant,
    O::Native: DecimalCast,
{
    match variant {
        Variant::Int8(_)
        | Variant::Int16(_)
        | Variant::Int32(_)
        | Variant::Int64(_)
        | Variant::Decimal4(_)
        | Variant::Decimal8(_)
        | Variant::Decimal16(_) => O::shred_variant(variant, precision, scale),
        _ => None,
    }
}
pub(crate) trait ShredDecimalVariant: DecimalType {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native>;
}

fn convert_to_unscaled_decimal<I, O>(
    input: I::Native,
    input_precision: u8,
    input_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Option<O::Native>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast,
    O::Native: DecimalCast,
{
    let converted = rescale_decimal::<I, O>(
        input,
        input_precision,
        input_scale,
        target_precision,
        target_scale,
    )?;

    let converted_back = rescale_decimal::<O, I>(
        converted,
        target_precision,
        target_scale,
        input_precision,
        input_scale,
    )?;
    if converted_back == input {
        return Some(converted);
    }

    None
}

impl ShredDecimalVariant for Decimal32Type {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native> {
        match *value {
            Variant::Int8(i) => convert_to_unscaled_decimal::<Decimal32Type, Decimal32Type>(
                i as i32,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int16(i) => convert_to_unscaled_decimal::<Decimal32Type, Decimal32Type>(
                i as i32,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int32(i) => convert_to_unscaled_decimal::<Decimal32Type, Decimal32Type>(
                i,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int64(i) => {
                let i32_value = <i64 as TryInto<i32>>::try_into(i).ok()?;
                convert_to_unscaled_decimal::<Decimal32Type, Decimal32Type>(
                    i32_value,
                    VariantDecimal4::MAX_PRECISION,
                    0,
                    precision,
                    scale,
                )
            }
            Variant::Decimal4(d) => convert_to_unscaled_decimal::<Decimal32Type, Decimal32Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => convert_to_unscaled_decimal::<Decimal64Type, Decimal32Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => convert_to_unscaled_decimal::<Decimal128Type, Decimal32Type>(
                d.integer(),
                VariantDecimal16::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            _ => None,
        }
    }
}

impl ShredDecimalVariant for Decimal64Type {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native> {
        match *value {
            Variant::Int8(i) => convert_to_unscaled_decimal::<Decimal64Type, Decimal64Type>(
                i as i64,
                VariantDecimal8::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int16(i) => convert_to_unscaled_decimal::<Decimal64Type, Decimal64Type>(
                i as i64,
                VariantDecimal8::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int32(i) => convert_to_unscaled_decimal::<Decimal64Type, Decimal64Type>(
                i as i64,
                VariantDecimal8::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int64(i) => convert_to_unscaled_decimal::<Decimal64Type, Decimal64Type>(
                i,
                VariantDecimal8::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Decimal4(d) => convert_to_unscaled_decimal::<Decimal32Type, Decimal64Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => convert_to_unscaled_decimal::<Decimal64Type, Decimal64Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => convert_to_unscaled_decimal::<Decimal128Type, Decimal64Type>(
                d.integer(),
                VariantDecimal16::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            _ => None,
        }
    }
}

impl ShredDecimalVariant for Decimal128Type {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native> {
        match *value {
            Variant::Int8(i) => convert_to_unscaled_decimal::<Decimal128Type, Decimal128Type>(
                i as i128,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int16(i) => convert_to_unscaled_decimal::<Decimal128Type, Decimal128Type>(
                i as i128,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int32(i) => convert_to_unscaled_decimal::<Decimal128Type, Decimal128Type>(
                i as i128,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Int64(i) => convert_to_unscaled_decimal::<Decimal128Type, Decimal128Type>(
                i as i128,
                VariantDecimal4::MAX_PRECISION,
                0,
                precision,
                scale,
            ),
            Variant::Decimal4(d) => convert_to_unscaled_decimal::<Decimal32Type, Decimal128Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => convert_to_unscaled_decimal::<Decimal64Type, Decimal128Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => convert_to_unscaled_decimal::<Decimal128Type, Decimal128Type>(
                d.integer(),
                VariantDecimal16::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            _ => None,
        }
    }
}

impl ShredDecimalVariant for Decimal256Type {
    fn shred_variant(_value: &Variant<'_, '_>, _precision: u8, _scale: i8) -> Option<Self::Native> {
        None // always return none because we'll never shred to decimal256
    }
}

pub(crate) fn variant_to_boolean(variant: &Variant<'_, '_>, shred: bool) -> Option<bool> {
    if shred {
        return variant.as_boolean();
    }

    match variant {
        Variant::BooleanTrue => Some(true),
        Variant::BooleanFalse => Some(false),
        Variant::Int8(i) => Some(cast_num_to_bool(*i)),
        Variant::Int16(i) => Some(cast_num_to_bool(*i)),
        Variant::Int32(i) => Some(cast_num_to_bool(*i)),
        Variant::Int64(i) => Some(cast_num_to_bool(*i)),
        Variant::Float(f) => Some(cast_num_to_bool(*f)),
        Variant::Double(d) => Some(cast_num_to_bool(*d)),
        Variant::ShortString(s) => cast_single_string_to_boolean_default(s.as_str()),
        Variant::String(s) => cast_single_string_to_boolean_default(s),
        _ => None,
    }
}

#[inline]
fn write_float_to_string<F: Float>(f: F, out: &mut String) {
    let mut buffer = ryu::Buffer::new();
    out.push_str(buffer.format(f));
}

// convert a variant to an owned string.
pub(crate) fn variant_to_string(
    variant: &Variant<'_, '_>,
    formats: &TemporalFormats<'_>,
) -> Option<String> {
    if matches!(variant, Variant::Null) {
        return None;
    }

    if matches!(variant, Variant::Object(_)) {
        return None;
    }
    let mut s = String::new();
    write_variant_to_string(variant, formats, &mut s).then_some(s)
}

fn write_lexical_to_string<N: lexical_core::ToLexical>(out: &mut String, n: N) {
    // i64::FORMATTED_SIZE is the upper bound for all integer types we support
    // (i8/i16/i32/i64). With power-of-two feature it's 128, otherwise 20.
    // We can't use N::FORMATTED_SIZE in a generic function (generic_const_exprs).
    let mut buf = [0u8; i64::FORMATTED_SIZE];
    let written = lexical_core::write(n, &mut buf);
    // Lexical core produces valid UTF-8
    out.push_str(unsafe { std::str::from_utf8_unchecked(written) });
}

fn write_variant_to_string(
    variant: &Variant<'_, '_>,
    formats: &TemporalFormats<'_>,
    out: &mut String,
) -> bool {
    match variant {
        Variant::Null => {
            out.push_str(formats.null());
            true
        }
        Variant::String(s) => {
            out.push_str(s);
            true
        }
        Variant::ShortString(s) => {
            out.push_str(s);
            true
        }
        Variant::BooleanTrue => {
            out.push_str("true");
            true
        }
        Variant::BooleanFalse => {
            out.push_str("false");
            true
        }
        Variant::Int8(i) => {
            write_lexical_to_string(out, *i);
            true
        }
        Variant::Int16(i) => {
            write_lexical_to_string(out, *i);
            true
        }
        Variant::Int32(i) => {
            write_lexical_to_string(out, *i);
            true
        }
        Variant::Int64(i) => {
            write_lexical_to_string(out, *i);
            true
        }
        Variant::Float(f) => {
            write_float_to_string(*f, out);
            true
        }
        Variant::Double(f) => {
            write_float_to_string(*f, out);
            true
        }
        Variant::Decimal4(d) => {
            let value_str = d.integer().to_string();
            out.push_str(&format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ));
            true
        }
        Variant::Decimal8(d) => {
            let value_str = d.integer().to_string();
            out.push_str(&format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ));
            true
        }
        Variant::Decimal16(d) => {
            let value_str = d.integer().to_string();
            out.push_str(&format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ));
            true
        }
        Variant::Date(d) => {
            // The writing is always success
            let _ = write_temporal_display(out, d, formats.date());
            true
        },
        Variant::Time(t) => {
            // The writing is always success
            let _ = write_temporal_display(out, t, formats.time());
            true
        }
        Variant::TimestampMicros(t) => {
            // The writing is always success
            let _ = write_timestamp(
                out,
                t.naive_utc(),
                "+00:00".parse().ok(),
                formats.timestamp_tz(),
            );
            true
        }
        Variant::TimestampNtzMicros(t) => {
            // The writing is always success
            let _ = write_timestamp(out, *t, None, formats.timestamp());
            true
        }
        Variant::TimestampNanos(t) => {
            // The writing is always success
            let _ = write_timestamp(
                out,
                t.naive_utc(),
                "+00:00".parse().ok(),
                formats.timestamp_tz(),
            );
            true
        }
        Variant::TimestampNtzNanos(t) => {
            // The writing is always success
            let _ = write_timestamp(out, *t, None, formats.timestamp());
            true
        }
        Variant::Uuid(u) => write!(out, "{u}").is_ok(),
        Variant::Binary(v) => match std::str::from_utf8(v) {
            Ok(s) => {
                out.push_str(s);
                true
            }
            Err(_) => false,
        },
        Variant::List(l) => {
            write_list_to_string(l.iter(), formats, out);
            true
        }
        Variant::Object(o) => {
            write_map_to_string(o.iter(), formats, out);
            true
        }
    }
}

fn write_list_to_string<'m, 'v>(
    mut iter: impl Iterator<Item = Variant<'m, 'v>>,
    formats: &TemporalFormats,
    out: &mut String,
) {
    out.push('[');
    if let Some(item) = iter.next()
        && !write_variant_to_string(&item, formats, out)
    {
        out.push_str(formats.null());
    }
    for item in iter {
        out.push_str(", ");
        if !write_variant_to_string(&item, formats, out) {
            out.push_str(formats.null());
        }
    }
    out.push(']');
}

fn write_map_to_string<'m, 'v>(
    mut iter: impl Iterator<Item = (&'m str, Variant<'m, 'v>)>,
    formats: &TemporalFormats,
    out: &mut String,
) {
    out.push('{');

    if let Some((key, value)) = iter.next() {
        out.push_str(key);
        out.push_str(": ");
        if !write_variant_to_string(&value, formats, out) {
            out.push_str(formats.null());
        }
    }

    for (key, value) in iter {
        out.push_str(", ");
        out.push_str(key);
        out.push_str(": ");
        if !write_variant_to_string(&value, formats, out) {
            out.push_str(formats.null());
        }
    }

    out.push('}');
}

pub(crate) fn variant_to_binary<'v>(variant: &Variant<'_, 'v>) -> Option<&'v [u8]> {
    match *variant {
        Variant::Binary(d) => Some(d),
        Variant::String(s) => Some(s.as_bytes()),
        Variant::ShortString(s) => Some(s.as_str().as_bytes()),
        _ => None,
    }
}

/// Convert the value at a specific index in the given array into a `Variant`.
macro_rules! non_generic_conversion_single_value {
    ($array:expr, $cast_fn:expr, $index:expr) => {{
        let array = $array;
        if array.is_null($index) {
            Ok(Variant::Null)
        } else {
            let cast_value = $cast_fn(array.value($index));
            Ok(Variant::from(cast_value))
        }
    }};
}
pub(crate) use non_generic_conversion_single_value;

/// Convert the value at a specific index in the given array into a `Variant`,
/// using `method` requiring a generic type to downcast the generic array
/// to a specific array type and `cast_fn` to transform the element.
macro_rules! generic_conversion_single_value {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        $crate::type_conversion::non_generic_conversion_single_value!(
            $input.$method::<$t>(),
            $cast_fn,
            $index
        )
    }};
}
pub(crate) use generic_conversion_single_value;

macro_rules! generic_conversion_single_value_with_result {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        let arr = $input.$method::<$t>();
        let v = arr.value($index);
        match ($cast_fn)(v) {
            Ok(var) => Ok(Variant::from(var)),
            Err(e) => Err(ArrowError::CastError(format!(
                "Cast failed at index {idx} (array type: {ty}): {e}",
                idx = $index,
                ty = <$t as ::arrow::datatypes::ArrowPrimitiveType>::DATA_TYPE
            ))),
        }
    }};
}

pub(crate) use generic_conversion_single_value_with_result;

/// Convert the value at a specific index in the given array into a `Variant`.
macro_rules! primitive_conversion_single_value {
    ($t:ty, $input:expr, $index:expr) => {{
        $crate::type_conversion::generic_conversion_single_value!(
            $t,
            as_primitive,
            |v| v,
            $input,
            $index
        )
    }};
}
use crate::variant_to_arrow::TemporalFormats;
pub(crate) use primitive_conversion_single_value;

#[cfg(test)]
mod tests {
    use crate::type_conversion::variant_to_string;
    use crate::variant_to_arrow::TemporalFormats;
    use arrow::array::{
        Array, AsArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Builder,
        ListBuilder, MapBuilder, StringBuilder, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use arrow::compute::{CastOptions, cast, cast_with_options};
    use arrow::util::display::FormatOptions;
    use arrow_schema::DataType;
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use parquet_variant::{Variant, VariantBuilder, VariantBuilderExt};
    use std::iter::zip;

    #[test]
    fn test_compatible_cast_logic_with_cast_kernel() {
        // boolean -> string
        let boolean_array = BooleanArray::from(vec![Some(true), Some(false)]);
        let cast_array = cast(&boolean_array, &DataType::Utf8).unwrap();
        let boolean_utf8_array = cast_array.as_string::<i32>();
        let expected_array = vec![
            variant_to_string(&Variant::BooleanTrue, &TemporalFormats::default()),
            variant_to_string(&Variant::BooleanFalse, &TemporalFormats::default()),
        ];
        for (a, b) in zip(boolean_utf8_array, expected_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // float -> string
        let raw_float_array = vec![1.23, 1e10, f32::NAN, f32::INFINITY];
        let float32_array = Float32Array::from(raw_float_array.clone());
        let cast_array = cast(&float32_array, &DataType::Utf8).unwrap();
        let float_utf8_array = cast_array.as_string::<i32>();
        let float_variant_array = raw_float_array
            .iter()
            .map(|f| Variant::from(*f))
            .collect::<Vec<Variant>>();
        let float_variant_as_string_array = float_variant_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();
        for (a, b) in zip(float_utf8_array, float_variant_as_string_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // double -> string
        let raw_float64_array = vec![1.23, 1e10, f64::NAN, f64::INFINITY];
        let float64_array = Float64Array::from(raw_float64_array.clone());
        let cast_array = cast(&float64_array, &DataType::Utf8).unwrap();
        let float64_utf8_array = cast_array.as_string::<i32>();
        let float64_variant_array = raw_float64_array
            .iter()
            .map(|f| Variant::from(*f))
            .collect::<Vec<Variant>>();
        let float64_variant_as_string_array = float64_variant_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();
        for (a, b) in zip(float64_utf8_array, float64_variant_as_string_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        let custom_cast_option = CastOptions {
            safe: false,
            format_options: FormatOptions::new()
                // custom format which different with the default format
                .with_date_format(Some("%Y/%m/%d"))
                .with_time_format(Some("%H-%M-%S%.f"))
                .with_timestamp_format(Some("%Y/%m/%d %H:%M:%S%.f"))
                .with_timestamp_tz_format(Some("%Y/%m/%d %H:%M:%S%.f%:z")),
        };
        let custom_temporal_formats = TemporalFormats::new(&custom_cast_option);

        // date -> string
        let epoch_days = [-10, 0, 18628];
        let date_array = epoch_days
            .iter()
            .map(|d| Variant::Date(NaiveDate::from_epoch_days(*d).unwrap()))
            .collect::<Vec<Variant>>();
        let variant_as_string_array = date_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let date32_array = Date32Array::from_iter_values(epoch_days);
        let date32_cast_array = cast(&date32_array, &DataType::Utf8).unwrap();
        let date32_utf8_array = date32_cast_array.as_string::<i32>();
        for (a, b) in zip(variant_as_string_array, date32_utf8_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        let custom_variant_date_as_string_array = date_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_date32_cast_array =
            cast_with_options(&date32_array, &DataType::Utf8, &custom_cast_option).unwrap();
        let custom_date32_utf8_array = custom_date32_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_variant_date_as_string_array,
            custom_date32_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // time -> string
        let time_tuples = [(123, 0), (123, 456789000), (12345, 456789000)];
        let time_array = time_tuples
            .iter()
            .map(|tuple| {
                Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(tuple.0, tuple.1).unwrap(),
                )
            })
            .collect::<Vec<Variant>>();
        let time_variant_as_string_array = time_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let time_micro_array = Time64MicrosecondArray::from_iter(
            time_tuples
                .iter()
                .map(|item| Some(item.0 as i64 * 1_000_000 + item.1 as i64 / 1000)),
        );

        let time_micro_cast_array = cast(&time_micro_array, &DataType::Utf8).unwrap();
        let time_micro_utf8_array = time_micro_cast_array.as_string::<i32>();

        for (a, b) in zip(time_variant_as_string_array, time_micro_utf8_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // time -> string with custom format
        let custom_time_variant_as_string_array = time_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_time_micro_cast_array =
            cast_with_options(&time_micro_array, &DataType::Utf8, &custom_cast_option).unwrap();
        let custom_time_micro_utf8_array = custom_time_micro_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_time_variant_as_string_array,
            custom_time_micro_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(micro) -> string
        let micros = [-123456, 123456, 45678];
        let timestamp_micro_array = micros
            .iter()
            .map(|m| Variant::TimestampMicros(DateTime::from_timestamp_micros(*m).unwrap()))
            .collect::<Vec<Variant>>();
        let timestamp_micro_as_string_array = timestamp_micro_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let timestamp_micro_arrow_array =
            TimestampMicrosecondArray::from_iter_values(micros).with_timezone("+00:00");
        let timestamp_micro_arrow_cast_array =
            cast(&timestamp_micro_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_micro_utf8_array = timestamp_micro_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(timestamp_micro_as_string_array, timestamp_micro_utf8_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(micro) -> string with custom format
        let custom_timestamp_micro_as_string_array = timestamp_micro_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_timestamp_micro_arrow_cast_array = cast_with_options(
            &timestamp_micro_arrow_array,
            &DataType::Utf8,
            &custom_cast_option,
        )
        .unwrap();
        let custom_timestamp_micro_utf8_array =
            custom_timestamp_micro_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_timestamp_micro_as_string_array,
            custom_timestamp_micro_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(micro) ntz -> string
        let micros_ntz = [-123456, 123456, 45678];
        let timestamp_micro_ntz_variant_array = micros_ntz
            .iter()
            .map(|m| {
                Variant::TimestampNtzMicros(
                    DateTime::from_timestamp_micros(*m).unwrap().naive_utc(),
                )
            })
            .collect::<Vec<Variant>>();
        let timestamp_micro_ntz_variant_as_string_array = timestamp_micro_ntz_variant_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let timestamp_micro_ntz_arrow_array =
            TimestampMicrosecondArray::from_iter_values(micros_ntz);
        let timestamp_micro_ntz_arrow_cast_array =
            cast(&timestamp_micro_ntz_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_micro_ntz_utf8_array =
            timestamp_micro_ntz_arrow_cast_array.as_string::<i32>();

        for (a, b) in zip(
            timestamp_micro_ntz_variant_as_string_array.clone(),
            timestamp_micro_ntz_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(micro) ntz -> string with custom format
        let custom_timestamp_micro_ntz_variant_as_string_array = timestamp_micro_ntz_variant_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_timestamp_micro_ntz_arrow_cast_array = cast_with_options(
            &timestamp_micro_ntz_arrow_array,
            &DataType::Utf8,
            &custom_cast_option,
        )
        .unwrap();
        let custom_timestamp_micro_ntz_utf8_array =
            custom_timestamp_micro_ntz_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_timestamp_micro_ntz_variant_as_string_array.clone(),
            custom_timestamp_micro_ntz_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(nano) -> string
        let nanos = [-2_208_936_075_000_000_000, 0, 1_662_921_288_000_000_000];
        let timestamp_nano_variant_array = nanos
            .iter()
            .map(|n| Variant::TimestampNanos(DateTime::from_timestamp_nanos(*n)))
            .collect::<Vec<Variant>>();
        let timestamp_nano_as_string_array = timestamp_nano_variant_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let timestamp_nano_arrow_array =
            TimestampNanosecondArray::from_iter_values(nanos).with_timezone("+00:00");
        let timestamp_nano_arrow_cast_array =
            cast(&timestamp_nano_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_nano_cast_utf8_array = timestamp_nano_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            timestamp_nano_cast_utf8_array,
            timestamp_nano_as_string_array.clone(),
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(nano) -> string with custom format
        let custom_timestamp_nano_as_string_array = timestamp_nano_variant_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_timestamp_nano_arrow_cast_array = cast_with_options(
            &timestamp_nano_arrow_array,
            &DataType::Utf8,
            &custom_cast_option,
        )
        .unwrap();
        let custom_timestamp_nano_cast_utf8_array =
            custom_timestamp_nano_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_timestamp_nano_cast_utf8_array,
            custom_timestamp_nano_as_string_array.clone(),
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // timestamp(nano) ntz -> string
        let nanos_ntz = [-2_208_936_075_000_000_000i64, 0, 1_662_921_288_000_000_000];
        let timestamp_nano_ntz_variant_array = nanos_ntz
            .iter()
            .map(|n| Variant::TimestampNtzNanos(DateTime::from_timestamp_nanos(*n).naive_utc()))
            .collect::<Vec<Variant>>();

        let timestamp_nano_ntz_variant_as_string_array = timestamp_nano_ntz_variant_array
            .iter()
            .map(|v| variant_to_string(v, &TemporalFormats::default()))
            .collect::<Vec<Option<String>>>();

        let timestamp_nano_ntz_arrow_array = TimestampNanosecondArray::from_iter_values(nanos_ntz);

        let timestamp_nano_ntz_arrow_cast_array =
            cast(&timestamp_nano_ntz_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_nano_ntz_utf8_array = timestamp_nano_ntz_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            timestamp_nano_ntz_variant_as_string_array.clone(),
            timestamp_nano_ntz_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }
        // timestamp(nano) ntz -> string with custom format
        let custom_timestamp_nano_ntz_variant_as_string_array = timestamp_nano_ntz_variant_array
            .iter()
            .map(|v| variant_to_string(v, &custom_temporal_formats))
            .collect::<Vec<Option<String>>>();
        let custom_timestamp_nano_ntz_arrow_cast_array = cast_with_options(
            &timestamp_nano_ntz_arrow_array,
            &DataType::Utf8,
            &custom_cast_option,
        )
        .unwrap();
        let custom_timestamp_nano_ntz_utf8_array =
            custom_timestamp_nano_ntz_arrow_cast_array.as_string::<i32>();
        for (a, b) in zip(
            custom_timestamp_nano_ntz_variant_as_string_array.clone(),
            custom_timestamp_nano_ntz_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // list -> string without nested map
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.append_value(123);
        list_builder.append_value(234);
        list_builder.append_null();
        list_builder.append_value(345);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);
        let variant_list_as_string =
            variant_to_string(&variant_list, &TemporalFormats::default());

        let inner_builder = Int32Builder::new();
        let mut builder = ListBuilder::new(inner_builder);
        builder.values().append_value(123);
        builder.values().append_value(234);
        builder.values().append_null();
        builder.values().append_value(345);
        builder.append(true);
        let list_arrow_array = builder.finish();
        let cast_array = cast(&list_arrow_array, &DataType::Utf8).unwrap();
        let arrow_list_cast_utf8_array = cast_array.as_string::<i32>();

        assert_eq!(arrow_list_cast_utf8_array.len(), 1);
        assert_eq!(
            variant_list_as_string.unwrap(),
            arrow_list_cast_utf8_array.value(0)
        );
    }

    #[test]
    fn test_compatible_cast_logic_for_nested_map_in_list() {
        // list -> string with nested map
        // value: [{"key1":1234, "key2": 5678}, {"key3": 91011, "key4": 121314}, null]
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        // first object {"key1": 1234, "key2": 5678}
        let mut object_builder = list_builder.new_object();
        object_builder.insert("key1", 1234);
        object_builder.insert("key2", 5678);
        object_builder.finish();
        // second object {"key3": 91011, "key4": 121314}
        let mut object_builder2 = list_builder.new_object();
        object_builder2.insert("key3", 91011);
        object_builder2.insert("key4", 121314);
        object_builder2.finish();
        // null value
        list_builder.append_null();
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);
        let variant_list_include_nest_map_as_string =
            variant_to_string(&variant_list, &TemporalFormats::default());

        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);
        // Construct `[{"key1": 1234, "key2": 5678}, {"key3": 91011, "key4": 121314}, null]`
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);
        // {"key1": 1234, "key2": 5678}
        map_builder.keys().append_value("key1");
        map_builder.values().append_value(1234);
        map_builder.keys().append_value("key2");
        map_builder.values().append_value(5678);
        map_builder.append(true).unwrap();
        // {"key3": 91011, "key4": 121314}
        map_builder.keys().append_value("key3");
        map_builder.values().append_value(91011);
        map_builder.keys().append_value("key4");
        map_builder.values().append_value(121314);
        map_builder.append(true).unwrap();
        // null
        map_builder.append(false).unwrap();
        let mut builder = ListBuilder::new(map_builder);
        builder.append(true);
        let list_arrow_array = builder.finish();
        let cast_array = cast(&list_arrow_array, &DataType::Utf8).unwrap();
        let arrow_list_include_nest_map_cast_utf8_array = cast_array.as_string::<i32>();

        assert_eq!(arrow_list_include_nest_map_cast_utf8_array.len(), 1);
        assert_eq!(
            variant_list_include_nest_map_as_string.unwrap(),
            arrow_list_include_nest_map_cast_utf8_array.value(0)
        );
    }

    #[test]
    fn test_compatible_cast_logic_for_deep_nested_map_in_list() {
        // Value: [{outer: {inner1: 1234, inner2: 5678}}, null]
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();

        let mut outer_object_builder = list_builder.new_object();
        let mut inner_object_builder = outer_object_builder.new_object("outer");
        inner_object_builder.insert("inner1", 1234);
        inner_object_builder.insert("inner2", 5678);
        inner_object_builder.finish();
        outer_object_builder.finish();

        list_builder.append_null();
        list_builder.finish();

        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);
        let variant_list_as_string =
            variant_to_string(&variant_list, &TemporalFormats::default());

        let inner_map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int32Builder::with_capacity(2));
        let mut outer_map_builder = MapBuilder::new(None, StringBuilder::new(), inner_map_builder);

        outer_map_builder.keys().append_value("outer");
        outer_map_builder.values().keys().append_value("inner1");
        outer_map_builder.values().values().append_value(1234);
        outer_map_builder.values().keys().append_value("inner2");
        outer_map_builder.values().values().append_value(5678);
        outer_map_builder.values().append(true).unwrap();
        outer_map_builder.append(true).unwrap();

        outer_map_builder.append(false).unwrap();

        let mut list_arrow_builder = ListBuilder::new(outer_map_builder);
        list_arrow_builder.append(true);
        let list_arrow_array = list_arrow_builder.finish();

        let cast_array = cast(&list_arrow_array, &DataType::Utf8).unwrap();
        let arrow_list_cast_utf8_array = cast_array.as_string::<i32>();

        assert_eq!(arrow_list_cast_utf8_array.len(), 1);
        assert_eq!(
            variant_list_as_string.unwrap(),
            arrow_list_cast_utf8_array.value(0)
        );
    }

    #[test]
    fn test_compatible_cast_logic_for_nested_list_in_list() {
        // Value: [[123, 234, null], [345, 456], null]
        let mut variant_builder = VariantBuilder::new();
        let mut outer_list_builder = variant_builder.new_list();

        let mut first_inner_list = outer_list_builder.new_list();
        first_inner_list.append_value(123);
        first_inner_list.append_value(234);
        first_inner_list.append_null();
        first_inner_list.finish();

        let mut second_inner_list = outer_list_builder.new_list();
        second_inner_list.append_value(345);
        second_inner_list.append_value(456);
        second_inner_list.finish();

        outer_list_builder.append_null();
        outer_list_builder.finish();

        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);
        let variant_list_as_string =
            variant_to_string(&variant_list, &TemporalFormats::default());

        let inner_builder = Int32Builder::new();
        let inner_list_builder = ListBuilder::new(inner_builder);
        let mut outer_list_builder = ListBuilder::new(inner_list_builder);

        outer_list_builder.values().values().append_value(123);
        outer_list_builder.values().values().append_value(234);
        outer_list_builder.values().values().append_null();
        outer_list_builder.values().append(true);

        outer_list_builder.values().values().append_value(345);
        outer_list_builder.values().values().append_value(456);
        outer_list_builder.values().append(true);

        outer_list_builder.values().append(false);
        outer_list_builder.append(true);

        let nested_list_arrow_array = outer_list_builder.finish();
        let cast_array = cast(&nested_list_arrow_array, &DataType::Utf8).unwrap();
        let arrow_nested_list_cast_utf8_array = cast_array.as_string::<i32>();

        assert_eq!(arrow_nested_list_cast_utf8_array.len(), 1);
        assert_eq!(
            variant_list_as_string.unwrap(),
            arrow_nested_list_cast_utf8_array.value(0)
        )
    }

    #[test]
    fn test_compatible_cast_with_custom_null_format() {
        let custom_cast_option = CastOptions {
            safe: false,
            format_options: FormatOptions::new().with_null("NULL"),
        };
        let custom_temporal_formats = TemporalFormats::new(&custom_cast_option);

        // Test null value
        let mut variant_builder = VariantBuilder::new();
        variant_builder.append_null();
        let (metadata, value) = variant_builder.finish();
        let variant_null = Variant::new(&metadata, &value);

        let result = variant_to_string(&variant_null, &custom_temporal_formats);
        assert!(result.is_none());

        // list -> string
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.append_value(123);
        list_builder.append_value(234);
        list_builder.append_null();
        list_builder.append_value(345);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);
        let variant_list_as_string =
            variant_to_string(&variant_list, &custom_temporal_formats).unwrap();

        let inner_builder = Int32Builder::new();
        let mut builder = ListBuilder::new(inner_builder);
        builder.values().append_value(123);
        builder.values().append_value(234);
        builder.values().append_null();
        builder.values().append_value(345);
        builder.append(true);
        let list_arrow_array = builder.finish();
        let cast_array =
            cast_with_options(&list_arrow_array, &DataType::Utf8, &custom_cast_option).unwrap();
        let arrow_list_cast_utf8_array = cast_array.as_string::<i32>();

        assert_eq!(arrow_list_cast_utf8_array.len(), 1);
        let expected_string = format!("[123, 234, {}, 345]", custom_temporal_formats.null());
        assert_eq!(expected_string, variant_list_as_string.clone());
        assert_eq!(variant_list_as_string, arrow_list_cast_utf8_array.value(0));
    }

    #[test]
    fn test_variant_to_string_list_mixed_types() {
        // Test mixed types list
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.append_value(42i32);
        list_builder.append_value("text");
        list_builder.append_value(true);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant_list = Variant::new(&metadata, &value);

        let result = variant_to_string(&variant_list, &TemporalFormats::default()).unwrap();
        assert_eq!(result, "[42, text, true]");
    }
}

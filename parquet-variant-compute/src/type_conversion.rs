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
    Decimal256Type, DecimalType,
};
use arrow::error::{ArrowError, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use half::f16;
use num_traits::NumCast;
use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};

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

/// Return the unscaled integer representation for Arrow decimal type `O` from a `Variant`.
///
/// This function is unlike `variant_to_unscaled_decim`, it would never rescale the decimal value,
/// and only return the unscaled integer representation for the specific decimal variants.
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
        Variant::Decimal4(_) | Variant::Decimal8(_) | Variant::Decimal16(_) => {
            O::shred_variant(variant, precision, scale)
        }
        _ => None,
    }
}
pub(crate) trait ShredDecimalVariant: DecimalType {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native>;
}

impl ShredDecimalVariant for Decimal32Type {
    fn shred_variant(value: &Variant<'_, '_>, precision: u8, scale: i8) -> Option<Self::Native> {
        match *value {
            Variant::Decimal4(d) => rescale_decimal::<Decimal32Type, Decimal32Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => rescale_decimal::<Decimal64Type, Decimal32Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => rescale_decimal::<Decimal128Type, Decimal32Type>(
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
            Variant::Decimal4(d) => rescale_decimal::<Decimal32Type, Decimal64Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => rescale_decimal::<Decimal64Type, Decimal64Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => rescale_decimal::<Decimal128Type, Decimal64Type>(
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
            Variant::Decimal4(d) => rescale_decimal::<Decimal32Type, Decimal128Type>(
                d.integer(),
                VariantDecimal4::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal8(d) => rescale_decimal::<Decimal64Type, Decimal128Type>(
                d.integer(),
                VariantDecimal8::MAX_PRECISION,
                d.scale() as i8,
                precision,
                scale,
            ),
            Variant::Decimal16(d) => rescale_decimal::<Decimal128Type, Decimal128Type>(
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
pub(crate) use primitive_conversion_single_value;

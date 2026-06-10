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

use arrow::compute::{
    CastOptions, DecimalCast, parse_string_to_decimal_native, rescale_decimal,
    single_float_to_decimal,
};
use arrow::datatypes::{
    self, ArrowPrimitiveType, ArrowTimestampType, Decimal32Type, Decimal64Type, Decimal128Type,
    DecimalType, format_decimal_str,
};
use arrow::error::{ArrowError, Result};
use arrow::util::display::{lexical_to_string, write_timestamp};
use chrono::Timelike;
use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};
use std::fmt::Write;

/// Extension trait for Arrow primitive types that can extract their native value from a Variant
pub(crate) trait PrimitiveFromVariant: ArrowPrimitiveType {
    fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native>;
}

/// Extension trait for Arrow timestamp types that can extract their native value from a Variant
/// We can't use [`PrimitiveFromVariant`] directly because we need _two_ implementations for each
/// timestamp type -- the `NTZ` param here.
pub(crate) trait TimestampFromVariant<const NTZ: bool>: ArrowTimestampType {
    fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native>;
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
    ($arrow_type:ty, $variant_method:ident $(, $cast_fn:expr)?) => {
        impl PrimitiveFromVariant for $arrow_type {
            fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native> {
                let value = variant.$variant_method();
                $( let value = value.and_then($cast_fn); )?
                value
            }
        }
    };
}

macro_rules! impl_timestamp_from_variant {
    ($timestamp_type:ty, $variant_method:ident, ntz=$ntz:ident, $cast_fn:expr $(,)?) => {
        impl TimestampFromVariant<{ $ntz }> for $timestamp_type {
            fn from_variant(variant: &Variant<'_, '_>) -> Option<Self::Native> {
                variant.$variant_method().and_then($cast_fn)
            }
        }
    };
}

impl_primitive_from_variant!(datatypes::Int32Type, as_int32);
impl_primitive_from_variant!(datatypes::Int16Type, as_int16);
impl_primitive_from_variant!(datatypes::Int8Type, as_int8);
impl_primitive_from_variant!(datatypes::Int64Type, as_int64);
impl_primitive_from_variant!(datatypes::UInt8Type, as_u8);
impl_primitive_from_variant!(datatypes::UInt16Type, as_u16);
impl_primitive_from_variant!(datatypes::UInt32Type, as_u32);
impl_primitive_from_variant!(datatypes::UInt64Type, as_u64);
impl_primitive_from_variant!(datatypes::Float16Type, as_f16);
impl_primitive_from_variant!(datatypes::Float32Type, as_f32);
impl_primitive_from_variant!(datatypes::Float64Type, as_f64);
impl_primitive_from_variant!(datatypes::Date32Type, as_naive_date, |v| {
    Some(datatypes::Date32Type::from_naive_date(v))
});
impl_primitive_from_variant!(datatypes::Date64Type, as_naive_date, |v| {
    Some(datatypes::Date64Type::from_naive_date(v))
});
impl_primitive_from_variant!(datatypes::Time32SecondType, as_time_utc, |v| {
    // Return None if there are leftover nanoseconds
    if v.nanosecond() != 0 {
        None
    } else {
        Some(v.num_seconds_from_midnight() as i32)
    }
});
impl_primitive_from_variant!(datatypes::Time32MillisecondType, as_time_utc, |v| {
    // Return None if there are leftover microseconds
    if v.nanosecond() % 1_000_000 != 0 {
        None
    } else {
        Some((v.num_seconds_from_midnight() * 1_000) as i32 + (v.nanosecond() / 1_000_000) as i32)
    }
});
impl_primitive_from_variant!(datatypes::Time64MicrosecondType, as_time_utc, |v| {
    Some(v.num_seconds_from_midnight() as i64 * 1_000_000 + v.nanosecond() as i64 / 1_000)
});
impl_primitive_from_variant!(datatypes::Time64NanosecondType, as_time_utc, |v| {
    // convert micro to nano seconds
    Some(v.num_seconds_from_midnight() as i64 * 1_000_000_000 + v.nanosecond() as i64)
});
impl_timestamp_from_variant!(
    datatypes::TimestampSecondType,
    as_timestamp_ntz_nanos,
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
    as_timestamp_nanos,
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
    as_timestamp_ntz_nanos,
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
    as_timestamp_nanos,
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
    as_timestamp_ntz_micros,
    ntz = true,
    |timestamp| Self::from_naive_datetime(timestamp, None),
);
impl_timestamp_from_variant!(
    datatypes::TimestampMicrosecondType,
    as_timestamp_micros,
    ntz = false,
    |timestamp| Self::from_naive_datetime(timestamp.naive_utc(), None)
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    as_timestamp_ntz_nanos,
    ntz = true,
    |timestamp| Self::from_naive_datetime(timestamp, None)
);
impl_timestamp_from_variant!(
    datatypes::TimestampNanosecondType,
    as_timestamp_nanos,
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
        Variant::Float(f) => single_float_to_decimal::<O>(f64::from(*f), mul),
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

pub(crate) fn variant_to_string(variant: &Variant<'_, '_>) -> Option<String> {
    match variant {
        Variant::String(s) => Some(s.to_string()),
        Variant::ShortString(s) => Some(s.to_string()),
        Variant::BooleanTrue => Some("true".into()),
        Variant::BooleanFalse => Some("false".into()),
        Variant::Int8(i) => Some(lexical_to_string(*i)),
        Variant::Int16(i) => Some(lexical_to_string(*i)),
        Variant::Int32(i) => Some(lexical_to_string(*i)),
        Variant::Int64(i) => Some(lexical_to_string(*i)),
        Variant::Float(f) => Some(lexical_to_string(*f)),
        Variant::Double(f) => Some(lexical_to_string(*f)),
        Variant::Decimal4(d) => {
            let value_str = d.integer().to_string();
            Some(format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ))
        }
        Variant::Decimal8(d) => {
            let value_str = d.integer().to_string();
            Some(format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ))
        }
        Variant::Decimal16(d) => {
            let value_str = d.integer().to_string();
            Some(format_decimal_str(
                &value_str,
                value_str.len(),
                d.scale() as _,
            ))
        }
        Variant::Date(d) => {
            let mut ret_string = String::new();
            let _ = write!(ret_string, "{d:?}");
            Some(ret_string)
        }
        Variant::Time(t) => {
            let mut ret_string = String::new();
            let _ = write!(ret_string, "{t:?}");
            Some(ret_string)
        }
        Variant::TimestampMicros(t) => {
            let mut out = String::new();
            let _ = write_timestamp(&mut out, t.naive_utc(), "+00:00".parse().ok(), None);
            Some(out)
        }
        Variant::TimestampNtzMicros(t) => {
            let mut out = String::new();
            let _ = write_timestamp(&mut out, *t, None, None);
            Some(out)
        }
        Variant::TimestampNanos(t) => {
            let mut out = String::new();
            let _ = write_timestamp(&mut out, t.naive_utc(), "+00:00".parse().ok(), None);
            Some(out)
        }
        Variant::TimestampNtzNanos(t) => {
            let mut out = String::new();
            let _ = write_timestamp(&mut out, *t, None, None);
            Some(out)
        }
        Variant::Uuid(u) => Some(u.to_string()),
        Variant::Binary(v) => std::str::from_utf8(v).ok().map(|s| s.to_string()),
        Variant::List(l) => Some(cast_list_to_string(l.iter())),
        _ => None,
    }
}

fn cast_list_to_string<'m, 'v>(mut iter: impl Iterator<Item = Variant<'m, 'v>>) -> String {
    let mut ret_str = String::new();
    let _ = ret_str.write_char('[');

    if let Some(item) = iter.next() {
        let _ = write!(ret_str, "{}", variant_to_string(&item).unwrap_or_default());
    }

    for item in iter {
        let _ = write!(
            ret_str,
            ", {}",
            variant_to_string(&item).unwrap_or_default()
        );
    }

    let _ = ret_str.write_char(']');

    ret_str
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

#[cfg(test)]
mod tests {
    use crate::type_conversion::variant_to_string;
    use arrow::array::{
        Array, BooleanArray, Date32Array, Int32Builder, ListBuilder, StringArray,
        Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow::compute::cast;
    use arrow_schema::DataType;
    use chrono::{DateTime, NaiveDate, NaiveTime};
    use parquet_variant::{Variant, VariantBuilder, VariantBuilderExt};
    use std::iter::zip;

    #[test]
    fn test_compatible_cast_logic_with_cast_kernel() {
        // boolean -> string
        let boolean_array = BooleanArray::from(vec![Some(true), Some(false)]);
        let cast_array = cast(&boolean_array, &DataType::Utf8).unwrap();
        let boolean_utf8_array = cast_array.as_any().downcast_ref::<StringArray>().unwrap();
        let expected_array = vec![
            variant_to_string(&Variant::BooleanTrue),
            variant_to_string(&Variant::BooleanFalse),
        ];
        for (a, b) in zip(boolean_utf8_array, expected_array) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

        // date -> string
        let epoch_days = [-10, 0, 18628];
        let date_array = epoch_days
            .iter()
            .map(|d| Variant::Date(NaiveDate::from_epoch_days(*d).unwrap()))
            .collect::<Vec<Variant>>();
        let variant_as_string_array = date_array
            .iter()
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let date32_array = Date32Array::from_iter_values(epoch_days);
        let date32_cast_array = cast(&date32_array, &DataType::Utf8).unwrap();
        let date32_utf8_array = date32_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (a, b) in zip(variant_as_string_array, date32_utf8_array) {
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
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let time_micro_array = Time64MicrosecondArray::from_iter(
            time_tuples
                .iter()
                .map(|item| Some(item.0 as i64 * 1_000_000 + item.1 as i64 / 1000)),
        );

        let time_micro_cast_array = cast(&time_micro_array, &DataType::Utf8).unwrap();
        let time_micro_utf8_array = time_micro_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for (a, b) in zip(time_variant_as_string_array, time_micro_utf8_array) {
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
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let timestamp_micro_arrow_array =
            TimestampMicrosecondArray::from_iter_values(micros).with_timezone("+00:00");
        let timestamp_micro_arrow_cast_array =
            cast(&timestamp_micro_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_micro_utf8_array = timestamp_micro_arrow_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (a, b) in zip(timestamp_micro_as_string_array, timestamp_micro_utf8_array) {
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
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let timestamp_micro_ntz_arrow_array =
            TimestampMicrosecondArray::from_iter_values(micros_ntz);
        let timestamp_micro_ntz_arrow_cast_array =
            cast(&timestamp_micro_ntz_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_micro_ntz_utf8_array = timestamp_micro_ntz_arrow_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for (a, b) in zip(
            timestamp_micro_ntz_variant_as_string_array,
            timestamp_micro_ntz_utf8_array,
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
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let timestamp_nano_arrow_array =
            TimestampNanosecondArray::from_iter_values(nanos).with_timezone("+00:00");
        let timestamp_nano_arrow_cast_array =
            cast(&timestamp_nano_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_nano_cast_utf8_array = timestamp_nano_arrow_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (a, b) in zip(
            timestamp_nano_cast_utf8_array,
            timestamp_nano_as_string_array,
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
            .map(|v| variant_to_string(v))
            .collect::<Vec<Option<String>>>();

        let timestamp_nano_ntz_arrow_array = TimestampNanosecondArray::from_iter_values(nanos_ntz);

        let timestamp_nano_ntz_arrow_cast_array =
            cast(&timestamp_nano_ntz_arrow_array, &DataType::Utf8).unwrap();
        let timestamp_nano_ntz_utf8_array = timestamp_nano_ntz_arrow_cast_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for (a, b) in zip(
            timestamp_nano_ntz_variant_as_string_array,
            timestamp_nano_ntz_utf8_array,
        ) {
            assert_eq!(a.unwrap(), b.unwrap());
        }

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
        let variant_list_as_string = variant_to_string(&variant_list);

        let inner_builder = Int32Builder::new();
        let mut builder = ListBuilder::new(inner_builder);
        builder.values().append_value(123);
        builder.values().append_value(234);
        builder.values().append_null();
        builder.values().append_value(345);
        builder.append(true);
        let list_arrow_array = builder.finish();
        let cast_array = cast(&list_arrow_array, &DataType::Utf8).unwrap();
        let arrow_list_cast_utf8_array = cast_array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(arrow_list_cast_utf8_array.len(), 1);
        assert_eq!(
            variant_list_as_string.unwrap(),
            arrow_list_cast_utf8_array.value(0)
        );
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

        let result = variant_to_string(&variant_list).unwrap();
        assert_eq!(result, "[42, text, true]");
    }
}

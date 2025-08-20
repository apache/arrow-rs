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

use std::sync::Arc;

use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{
    Array, AsArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray
};
use arrow::datatypes::{
    i256, ArrowNativeType, BinaryType, BinaryViewType, Date32Type, Date64Type, Decimal128Type,
    Decimal256Type, Decimal32Type, Decimal64Type, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, LargeBinaryType, RunEndIndexType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use half::f16;
use parquet_variant::{
    Variant, VariantBuilder, VariantDecimal16, VariantDecimal4, VariantDecimal8,
};

/// Convert the input array of a specific primitive type to a `VariantArray`
/// row by row
macro_rules! primitive_conversion {
    ($t:ty, $input:expr, $builder:expr) => {{
        let array = $input.as_primitive::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            $builder.append_variant(Variant::from(array.value(i)));
        }
    }};
}

/// Convert the input array to a `VariantArray` row by row, using `method`
/// requiring a generic type to downcast the generic array to a specific
/// array type and `cast_fn` to transform each element to a type compatible with Variant
macro_rules! generic_conversion {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Convert the input array to a `VariantArray` row by row, using `method`
/// not requiring a generic type to downcast the generic array to a specific
/// array type and `cast_fn` to transform each element to a type compatible with Variant
macro_rules! non_generic_conversion {
    ($method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

fn convert_timestamp(
    time_unit: &TimeUnit,
    time_zone: &Option<Arc<str>>,
    input: &dyn Array,
    builder: &mut VariantArrayBuilder,
) {
    let native_datetimes: Vec<Option<NaiveDateTime>> = match time_unit {
        arrow_schema::TimeUnit::Second => {
            let ts_array = input
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("Array is not TimestampSecondArray");

            ts_array
                .iter()
                .map(|x| x.map(|y| timestamp_s_to_datetime(y).unwrap()))
                .collect()
        }
        arrow_schema::TimeUnit::Millisecond => {
            let ts_array = input
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Array is not TimestampMillisecondArray");

            ts_array
                .iter()
                .map(|x| x.map(|y| timestamp_ms_to_datetime(y).unwrap()))
                .collect()
        }
        arrow_schema::TimeUnit::Microsecond => {
            let ts_array = input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("Array is not TimestampMicrosecondArray");
            ts_array
                .iter()
                .map(|x| x.map(|y| timestamp_us_to_datetime(y).unwrap()))
                .collect()
        }
        arrow_schema::TimeUnit::Nanosecond => {
            let ts_array = input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Array is not TimestampNanosecondArray");
            ts_array
                .iter()
                .map(|x| x.map(|y| timestamp_ns_to_datetime(y).unwrap()))
                .collect()
        }
    };

    for x in native_datetimes {
        match x {
            Some(ndt) => {
                if time_zone.is_none() {
                    builder.append_variant(ndt.into());
                } else {
                    let utc_dt: DateTime<Utc> = Utc.from_utc_datetime(&ndt);
                    builder.append_variant(utc_dt.into());
                }
            }
            None => {
                builder.append_null();
            }
        }
    }
}

/// Convert a decimal value to a `VariantDecimal`
macro_rules! decimal_to_variant_decimal {
    ($v:ident, $scale:expr, $value_type:ty, $variant_type:ty) => {
        if *$scale < 0 {
            // For negative scale, we need to multiply the value by 10^|scale|
            // For example: 123 with scale -2 becomes 12300
            let multiplier = (10 as $value_type).pow((-*$scale) as u32);
            // Check for overflow
            if $v > 0 && $v > <$value_type>::MAX / multiplier {
                return Variant::Null;
            }
            if $v < 0 && $v < <$value_type>::MIN / multiplier {
                return Variant::Null;
            }
            <$variant_type>::try_new($v * multiplier, 0)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        } else {
            <$variant_type>::try_new($v, *$scale as u8)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        }
    };
}

/// Convert arrays that don't need generic type parameters
macro_rules! cast_conversion_nongeneric {
    ($method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Convert string arrays using the offset size as the type parameter
macro_rules! cast_conversion_string {
    ($offset_type:ty, $method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method::<$offset_type>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Casts a typed arrow [`Array`] to a [`VariantArray`]. This is useful when you
/// need to convert a specific data type
///
/// # Arguments
/// * `input` - A reference to the input [`Array`] to cast
///
/// # Notes
/// If the input array element is null, the corresponding element in the
/// output `VariantArray` will also be null (not `Variant::Null`).
///
/// # Example
/// ```
/// # use arrow::array::{Array, ArrayRef, Int64Array};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::cast_to_variant::cast_to_variant;
/// // input is an Int64Array, which will be cast to a VariantArray
/// let input = Int64Array::from(vec![Some(1), None, Some(3)]);
/// let result = cast_to_variant(&input).unwrap();
/// assert_eq!(result.len(), 3);
/// assert_eq!(result.value(0), Variant::Int64(1));
/// assert!(result.is_null(1)); // note null, not Variant::Null
/// assert_eq!(result.value(2), Variant::Int64(3));
/// ```
///
/// For `DataType::Timestamp`s: if the timestamp has any level of precision
/// greater than a microsecond, it will be truncated. For example
/// `1970-01-01T00:00:01.234567890Z`
/// will be truncated to
/// `1970-01-01T00:00:01.234567Z`
pub fn cast_to_variant(input: &dyn Array) -> Result<VariantArray, ArrowError> {
    let mut builder = VariantArrayBuilder::new(input.len());

    let input_type = input.data_type();
    // todo: handle other types like Boolean, Date, Timestamp, etc.
    match input_type {
        DataType::Boolean => {
            non_generic_conversion!(as_boolean, |v| v, input, builder);
        }
        DataType::Binary => {
            generic_conversion!(BinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::LargeBinary => {
            generic_conversion!(LargeBinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::BinaryView => {
            generic_conversion!(BinaryViewType, as_byte_view, |v| v, input, builder);
        }
        DataType::Int8 => {
            primitive_conversion!(Int8Type, input, builder);
        }
        DataType::Int16 => {
            primitive_conversion!(Int16Type, input, builder);
        }
        DataType::Int32 => {
            primitive_conversion!(Int32Type, input, builder);
        }
        DataType::Int64 => {
            primitive_conversion!(Int64Type, input, builder);
        }
        DataType::UInt8 => {
            primitive_conversion!(UInt8Type, input, builder);
        }
        DataType::UInt16 => {
            primitive_conversion!(UInt16Type, input, builder);
        }
        DataType::UInt32 => {
            primitive_conversion!(UInt32Type, input, builder);
        }
        DataType::UInt64 => {
            primitive_conversion!(UInt64Type, input, builder);
        }
        DataType::Float16 => {
            generic_conversion!(
                Float16Type,
                as_primitive,
                |v: f16| -> f32 { v.into() },
                input,
                builder
            );
        }
        DataType::Float32 => {
            primitive_conversion!(Float32Type, input, builder);
        }
        DataType::Float64 => {
            primitive_conversion!(Float64Type, input, builder);
        }
        DataType::Decimal32(_, scale) => {
            generic_conversion!(
                Decimal32Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i32, VariantDecimal4),
                input,
                builder
            );
        }
        DataType::Decimal64(_, scale) => {
            generic_conversion!(
                Decimal64Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i64, VariantDecimal8),
                input,
                builder
            );
        }
        DataType::Decimal128(_, scale) => {
            generic_conversion!(
                Decimal128Type,
                as_primitive,
                |v| decimal_to_variant_decimal!(v, scale, i128, VariantDecimal16),
                input,
                builder
            );
        }
        DataType::Decimal256(_, scale) => {
            generic_conversion!(
                Decimal256Type,
                as_primitive,
                |v: i256| {
                    // Since `i128::MAX` is larger than the max value of `VariantDecimal16`,
                    // any `i256` value that cannot be cast to `i128` is unable to be cast to `VariantDecimal16` either.
                    // Therefore, we can safely convert `i256` to `i128` first and process it like `i128`.
                    if let Some(v) = v.to_i128() {
                        decimal_to_variant_decimal!(v, scale, i128, VariantDecimal16)
                    } else {
                        Variant::Null
                    }
                },
                input,
                builder
            );
        }
        DataType::FixedSizeBinary(_) => {
            non_generic_conversion!(as_fixed_size_binary, |v| v, input, builder);
        }
        DataType::Null => {
            for _ in 0..input.len() {
                builder.append_null();
            }
        }
        DataType::Timestamp(time_unit, time_zone) => {
            convert_timestamp(time_unit, time_zone, input, &mut builder);
        }
        DataType::Time32(unit) => {
            match *unit {
                TimeUnit::Second => {
                    generic_conversion!(
                        Time32SecondType,
                        as_primitive,
                        // nano second are always 0
                        |v| NaiveTime::from_num_seconds_from_midnight_opt(v as u32, 0u32).unwrap(),
                        input,
                        builder
                    );
                }
                TimeUnit::Millisecond => {
                    generic_conversion!(
                        Time32MillisecondType,
                        as_primitive,
                        |v| NaiveTime::from_num_seconds_from_midnight_opt(
                            v as u32 / 1000,
                            (v as u32 % 1000) * 1_000_000
                        )
                        .unwrap(),
                        input,
                        builder
                    );
                }
                _ => {
                    return Err(ArrowError::CastError(format!(
                        "Unsupported Time32 unit: {:?}",
                        unit
                    )));
                }
            };
        }
        DataType::Time64(unit) => {
            match *unit {
                TimeUnit::Microsecond => {
                    generic_conversion!(
                        Time64MicrosecondType,
                        as_primitive,
                        |v| NaiveTime::from_num_seconds_from_midnight_opt(
                            (v / 1_000_000) as u32,
                            (v % 1_000_000 * 1_000) as u32
                        )
                        .unwrap(),
                        input,
                        builder
                    );
                }
                TimeUnit::Nanosecond => {
                    generic_conversion!(
                        Time64NanosecondType,
                        as_primitive,
                        |v| NaiveTime::from_num_seconds_from_midnight_opt(
                            (v / 1_000_000_000) as u32,
                            (v % 1_000_000_000) as u32
                        )
                        .unwrap(),
                        input,
                        builder
                    );
                }
                _ => {
                    return Err(ArrowError::CastError(format!(
                        "Unsupported Time64 unit: {:?}",
                        unit
                    )));
                }
            };
        }
        DataType::Interval(_) => {
            return Err(ArrowError::InvalidArgumentError(
                "Casting interval types to Variant is not supported. \
                 The Variant format does not define interval/duration types."
                    .to_string(),
            ));
        }
        DataType::Utf8 => {
            cast_conversion_string!(i32, as_string, |v| v, input, builder);
        }
        DataType::LargeUtf8 => {
            cast_conversion_string!(i64, as_string, |v| v, input, builder);
        }
        DataType::Utf8View => {
            cast_conversion_nongeneric!(as_string_view, |v| v, input, builder);
        }
        DataType::Struct(_) => {
            let struct_array = input.as_struct();

            // Pre-convert all field arrays once for better performance
            // This avoids converting the same field array multiple times
            // Alternative approach: Use slicing per row: field_array.slice(i, 1)
            // However, pre-conversion is more efficient for typical use cases
            let field_variant_arrays: Result<Vec<_>, _> = struct_array
                .columns()
                .iter()
                .map(|field_array| cast_to_variant(field_array.as_ref()))
                .collect();
            let field_variant_arrays = field_variant_arrays?;

            // Cache column names to avoid repeated calls
            let column_names = struct_array.column_names();

            for i in 0..struct_array.len() {
                if struct_array.is_null(i) {
                    builder.append_null();
                    continue;
                }

                // Create a VariantBuilder for this struct instance
                let mut variant_builder = VariantBuilder::new();
                let mut object_builder = variant_builder.new_object();

                // Iterate through all fields in the struct
                for (field_idx, field_name) in column_names.iter().enumerate() {
                    // Use pre-converted field variant arrays for better performance
                    // Check nulls directly from the pre-converted arrays instead of accessing column again
                    if !field_variant_arrays[field_idx].is_null(i) {
                        let field_variant = field_variant_arrays[field_idx].value(i);
                        object_builder.insert(field_name, field_variant);
                    }
                    // Note: we skip null fields rather than inserting Variant::Null
                    // to match Arrow struct semantics where null fields are omitted
                }

                object_builder.finish()?;
                let (metadata, value) = variant_builder.finish();
                let variant = Variant::try_new(&metadata, &value)?;
                builder.append_variant(variant);
            }
        }
        DataType::Date32 => {
            generic_conversion!(
                Date32Type,
                as_primitive,
                |v: i32| -> NaiveDate { Date32Type::to_naive_date(v) },
                input,
                builder
            );
        }
        DataType::Date64 => {
            generic_conversion!(
                Date64Type,
                as_primitive,
                |v: i64| { Date64Type::to_naive_date_opt(v).unwrap() },
                input,
                builder
            );
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => process_run_end_encoded::<Int16Type>(input, &mut builder)?,
            DataType::Int32 => process_run_end_encoded::<Int32Type>(input, &mut builder)?,
            DataType::Int64 => process_run_end_encoded::<Int64Type>(input, &mut builder)?,
            _ => {
                return Err(ArrowError::CastError(format!(
                    "Unsupported run ends type: {:?}",
                    run_ends.data_type()
                )));
            }
        },
        DataType::Dictionary(_, _) => {
            let dict_array = input.as_any_dictionary();
            let values_variant_array = cast_to_variant(dict_array.values().as_ref())?;
            let normalized_keys = dict_array.normalized_keys();
            let keys = dict_array.keys();

            for (i, key_idx) in normalized_keys.iter().enumerate() {
                if keys.is_null(i) {
                    builder.append_null();
                    continue;
                }

                if values_variant_array.is_null(*key_idx) {
                    builder.append_null();
                    continue;
                }

                let value = values_variant_array.value(*key_idx);
                builder.append_variant(value);
            }
        }
        DataType::List(_) => {
            let list_array = input.as_list::<i32>();

            let values_variant_array = cast_to_variant(list_array.values().as_ref())?;

            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    builder.append_null();
                    continue;
                }

                let value = values_variant_array.value(i);
                let variant_list = value.as_list().expect("variant should be a list").clone();

                builder.append_variant(Variant::List(variant_list));
            }
        }
        DataType::LargeList(_) => {
            // generic_conversion!(i64, as_list, |v| Variant::List(v), input, builder);
        }
        dt => {
            return Err(ArrowError::CastError(format!(
                "Unsupported data type for casting to Variant: {dt:?}",
            )));
        }
    };
    Ok(builder.build())
}

/// Generic function to process run-end encoded arrays
fn process_run_end_encoded<R: RunEndIndexType>(
    input: &dyn Array,
    builder: &mut VariantArrayBuilder,
) -> Result<(), ArrowError> {
    let run_array = input.as_run::<R>();
    let values_variant_array = cast_to_variant(run_array.values().as_ref())?;

    // Process runs in batches for better performance
    let run_ends = run_array.run_ends().values();
    let mut logical_start = 0;

    for (physical_idx, &run_end) in run_ends.iter().enumerate() {
        let logical_end = run_end.as_usize();
        let run_length = logical_end - logical_start;

        if values_variant_array.is_null(physical_idx) {
            // Append nulls for the entire run
            for _ in 0..run_length {
                builder.append_null();
            }
        } else {
            // Get the value once and append it for the entire run
            let value = values_variant_array.value(physical_idx);
            for _ in 0..run_length {
                builder.append_variant(value.clone());
            }
        }

        logical_start = logical_end;
    }

    Ok(())
}

// TODO do we need a cast_with_options to allow specifying conversion behavior,
// e.g. how to handle overflows, whether to convert to Variant::Null or return
// an error, etc. ?

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray, FixedSizeBinaryBuilder,
        Float16Array, Float32Array, Float64Array, GenericByteBuilder, GenericByteViewBuilder,
        Int16Array, Int32Array, Int64Array, Int8Array, IntervalYearMonthArray, LargeStringArray,
        NullArray, StringArray, StringRunBuilder, StringViewArray, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::buffer::NullBuffer;
    use arrow_schema::{Field, Fields};
    use arrow_schema::{
        DECIMAL128_MAX_PRECISION, DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION,
    };
    use parquet_variant::{Variant, VariantDecimal16};
    use std::{sync::Arc, vec};

    macro_rules! max_unscaled_value {
        (32, $precision:expr) => {
            (u32::pow(10, $precision as u32) - 1) as i32
        };
        (64, $precision:expr) => {
            (u64::pow(10, $precision as u32) - 1) as i64
        };
        (128, $precision:expr) => {
            (u128::pow(10, $precision as u32) - 1) as i128
        };
    }

    #[test]
    fn test_cast_to_variant_timestamp() {
        let run_array_tests =
            |microseconds: i64, array_ntz: Arc<dyn Array>, array_tz: Arc<dyn Array>| {
                let timestamp = DateTime::from_timestamp_nanos(microseconds * 1000);
                run_test(
                    array_tz,
                    vec![Some(Variant::TimestampMicros(timestamp)), None],
                );
                run_test(
                    array_ntz,
                    vec![
                        Some(Variant::TimestampNtzMicros(timestamp.naive_utc())),
                        None,
                    ],
                );
            };

        let nanosecond = 1234567890;
        let microsecond = 1234567;
        let millisecond = 1234;
        let second = 1;

        let second_array = TimestampSecondArray::from(vec![Some(second), None]);
        run_array_tests(
            second * 1000 * 1000,
            Arc::new(second_array.clone()),
            Arc::new(second_array.with_timezone("+01:00".to_string())),
        );

        let millisecond_array = TimestampMillisecondArray::from(vec![Some(millisecond), None]);
        run_array_tests(
            millisecond * 1000,
            Arc::new(millisecond_array.clone()),
            Arc::new(millisecond_array.with_timezone("+01:00".to_string())),
        );

        let microsecond_array = TimestampMicrosecondArray::from(vec![Some(microsecond), None]);
        run_array_tests(
            microsecond,
            Arc::new(microsecond_array.clone()),
            Arc::new(microsecond_array.with_timezone("+01:00".to_string())),
        );

        let timestamp = DateTime::from_timestamp_nanos(nanosecond);
        let nanosecond_array = TimestampNanosecondArray::from(vec![Some(nanosecond), None]);
        run_test(
            Arc::new(nanosecond_array.clone()),
            vec![
                Some(Variant::TimestampNtzNanos(timestamp.naive_utc())),
                None,
            ],
        );
        run_test(
            Arc::new(nanosecond_array.with_timezone("+01:00".to_string())),
            vec![Some(Variant::TimestampNanos(timestamp)), None],
        );
    }

    #[test]
    fn test_cast_to_variant_fixed_size_binary() {
        let v1 = vec![1, 2];
        let v2 = vec![3, 4];
        let v3 = vec![5, 6];

        let mut builder = FixedSizeBinaryBuilder::new(2);
        builder.append_value(&v1).unwrap();
        builder.append_value(&v2).unwrap();
        builder.append_null();
        builder.append_value(&v3).unwrap();
        let array = builder.finish();

        run_test(
            Arc::new(array),
            vec![
                Some(Variant::Binary(&v1)),
                Some(Variant::Binary(&v2)),
                None,
                Some(Variant::Binary(&v3)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_list() {
        
    }
    #[test]
    fn test_cast_to_variant_binary() {
        // BinaryType
        let mut builder = GenericByteBuilder::<BinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let binary_array = builder.finish();
        run_test(
            Arc::new(binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // LargeBinaryType
        let mut builder = GenericByteBuilder::<LargeBinaryType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let large_binary_array = builder.finish();
        run_test(
            Arc::new(large_binary_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );

        // BinaryViewType
        let mut builder = GenericByteViewBuilder::<BinaryViewType>::new();
        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"world");
        let byte_view_array = builder.finish();
        run_test(
            Arc::new(byte_view_array),
            vec![
                Some(Variant::Binary(b"hello")),
                Some(Variant::Binary(b"")),
                None,
                Some(Variant::Binary(b"world")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_bool() {
        run_test(
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            vec![
                Some(Variant::BooleanTrue),
                None,
                Some(Variant::BooleanFalse),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_int8() {
        run_test(
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i8::MAX),
            ])),
            vec![
                Some(Variant::Int8(i8::MIN)),
                None,
                Some(Variant::Int8(-1)),
                Some(Variant::Int8(1)),
                Some(Variant::Int8(i8::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int16() {
        run_test(
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i16::MAX),
            ])),
            vec![
                Some(Variant::Int16(i16::MIN)),
                None,
                Some(Variant::Int16(-1)),
                Some(Variant::Int16(1)),
                Some(Variant::Int16(i16::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int32() {
        run_test(
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i32::MAX),
            ])),
            vec![
                Some(Variant::Int32(i32::MIN)),
                None,
                Some(Variant::Int32(-1)),
                Some(Variant::Int32(1)),
                Some(Variant::Int32(i32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int64() {
        run_test(
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i64::MAX),
            ])),
            vec![
                Some(Variant::Int64(i64::MIN)),
                None,
                Some(Variant::Int64(-1)),
                Some(Variant::Int64(1)),
                Some(Variant::Int64(i64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint8() {
        run_test(
            Arc::new(UInt8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(127),
                Some(u8::MAX),
            ])),
            vec![
                Some(Variant::Int8(0)),
                None,
                Some(Variant::Int8(1)),
                Some(Variant::Int8(127)),
                Some(Variant::Int16(255)), // u8::MAX cannot fit in Int8
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint16() {
        run_test(
            Arc::new(UInt16Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(32767),
                Some(u16::MAX),
            ])),
            vec![
                Some(Variant::Int16(0)),
                None,
                Some(Variant::Int16(1)),
                Some(Variant::Int16(32767)),
                Some(Variant::Int32(65535)), // u16::MAX cannot fit in Int16
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint32() {
        run_test(
            Arc::new(UInt32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2147483647),
                Some(u32::MAX),
            ])),
            vec![
                Some(Variant::Int32(0)),
                None,
                Some(Variant::Int32(1)),
                Some(Variant::Int32(2147483647)),
                Some(Variant::Int64(4294967295)), // u32::MAX cannot fit in Int32
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint64() {
        run_test(
            Arc::new(UInt64Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(9223372036854775807),
                Some(u64::MAX),
            ])),
            vec![
                Some(Variant::Int64(0)),
                None,
                Some(Variant::Int64(1)),
                Some(Variant::Int64(9223372036854775807)),
                Some(Variant::Decimal16(
                    // u64::MAX cannot fit in Int64
                    VariantDecimal16::try_from(18446744073709551615).unwrap(),
                )),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float16() {
        run_test(
            Arc::new(Float16Array::from(vec![
                Some(f16::MIN),
                None,
                Some(f16::from_f32(-1.5)),
                Some(f16::from_f32(0.0)),
                Some(f16::from_f32(1.5)),
                Some(f16::MAX),
            ])),
            vec![
                Some(Variant::Float(f16::MIN.into())),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f16::MAX.into())),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float32() {
        run_test(
            Arc::new(Float32Array::from(vec![
                Some(f32::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f32::MAX),
            ])),
            vec![
                Some(Variant::Float(f32::MIN)),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float64() {
        run_test(
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f64::MAX),
            ])),
            vec![
                Some(Variant::Double(f64::MIN)),
                None,
                Some(Variant::Double(-1.5)),
                Some(Variant::Double(0.0)),
                Some(Variant::Double(1.5)),
                Some(Variant::Double(f64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_interval_error() {
        let array = IntervalYearMonthArray::from(vec![Some(12), None, Some(-6)]);
        let result = cast_to_variant(&array);

        assert!(result.is_err());
        match result.unwrap_err() {
            ArrowError::InvalidArgumentError(msg) => {
                assert!(msg.contains("Casting interval types to Variant is not supported"));
                assert!(msg.contains("The Variant format does not define interval/duration types"));
            }
            _ => panic!("Expected InvalidArgumentError"),
        }
    }

    #[test]
    fn test_cast_to_variant_null() {
        run_test(Arc::new(NullArray::new(2)), vec![None, None])
    }

    #[test]
    fn test_cast_to_variant_decimal32() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The min of Decimal32 with positive scale that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION)), // The max of Decimal32 with positive scale that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal4::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal32_negative_scale() {
        run_test(
            Arc::new(
                Decimal32Array::from(vec![
                    Some(i32::MIN),
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The min of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3)), // The max of Decimal32 with scale -3 that can be cast to VariantDecimal4
                    Some(max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i32::MAX),
                ])
                .with_precision_and_scale(DECIMAL32_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal4::try_new(
                        -max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal4::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal4::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal4::try_new(
                        max_unscaled_value!(32, DECIMAL32_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The min of Decimal64 with positive scale that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION)), // The max of Decimal64 with positive scale that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal8::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION), 3)
                        .unwrap()
                        .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal64_negative_scale() {
        run_test(
            Arc::new(
                Decimal64Array::from(vec![
                    Some(i64::MIN),
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The min of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3)), // The max of Decimal64 with scale -3 that can be cast to VariantDecimal8
                    Some(max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i64::MAX),
                ])
                .with_precision_and_scale(DECIMAL64_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal8::try_new(
                        -max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal8::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal8::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal8::try_new(
                        max_unscaled_value!(64, DECIMAL64_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The min of Decimal128 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION)), // The max of Decimal128 with positive scale that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal128_negative_scale() {
        run_test(
            Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1), // Overflow value will be cast to Null
                    Some(-max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The min of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(-123),
                    Some(0),
                    Some(123),
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3)), // The max of Decimal128 with scale -3 that can be cast to VariantDecimal16
                    Some(max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1), // Overflow value will be cast to Null
                    Some(i128::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The min of Decimal256 with positive scale that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION
                    ))), // The max of Decimal256 with positive scale that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 3).unwrap().into()),
                Some(VariantDecimal16::try_new(123, 3).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION),
                        3,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_decimal256_negative_scale() {
        run_test(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::MIN),
                    Some(i256::from_i128(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) - 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::from_i128(-max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The min of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    None,
                    Some(i256::from_i128(-123)),
                    Some(i256::from_i128(0)),
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(max_unscaled_value!(
                        128,
                        DECIMAL128_MAX_PRECISION - 3
                    ))), // The max of Decimal256 with scale -3 that can be cast to VariantDecimal16
                    Some(i256::from_i128(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) + 1,
                    )), // Overflow value will be cast to Null
                    Some(i256::MAX),
                ])
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, -3)
                .unwrap(),
            ),
            vec![
                Some(Variant::Null),
                Some(Variant::Null),
                Some(
                    VariantDecimal16::try_new(
                        -max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                None,
                Some(VariantDecimal16::try_new(-123_000, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(0, 0).unwrap().into()),
                Some(VariantDecimal16::try_new(123_000, 0).unwrap().into()),
                Some(
                    VariantDecimal16::try_new(
                        max_unscaled_value!(128, DECIMAL128_MAX_PRECISION - 3) * 1000,
                        0,
                    )
                    .unwrap()
                    .into(),
                ),
                Some(Variant::Null),
                Some(Variant::Null),
            ],
        )
    }

    #[test]
    fn test_cast_time32_second_to_variant_time() {
        let array: Time32SecondArray = vec![Some(1), Some(86_399), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(1, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(86_399, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_time32_millisecond_to_variant_time() {
        let array: Time32MillisecondArray = vec![Some(123_456), Some(456_000), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_000_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(456, 0).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_time64_micro_to_variant_time() {
        let array: Time64MicrosecondArray = vec![Some(1), Some(123_456_789), None].into();
        let values = Arc::new(array);
        run_test(
            values,
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_time64_nano_to_variant_time() {
        let array: Time64NanosecondArray =
            vec![Some(1), Some(1001), Some(123_456_789_012), None].into();
        run_test(
            Arc::new(array),
            // as we can only present with micro second, so the nano second will round donw to 0
            vec![
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(0, 1_000).unwrap(),
                )),
                Some(Variant::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(123, 456_789_000).unwrap(),
                )),
                None,
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world"), Some("test")];
        let string_array = StringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
                Some(Variant::from("test")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "a".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = StringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_large_utf8() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_array = LargeStringArray::from(short_strings.clone());

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "b".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_array = LargeStringArray::from(long_strings);

        run_test(
            Arc::new(string_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_utf8_view() {
        // Test with short strings (should become ShortString variants)
        let short_strings = vec![Some("hello"), Some(""), None, Some("world")];
        let string_view_array = StringViewArray::from(short_strings.clone());

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from("hello")),
                Some(Variant::from("")),
                None,
                Some(Variant::from("world")),
            ],
        );

        // Test with a long string (should become String variant)
        let long_string = "c".repeat(100); // > 63 bytes, so will be Variant::String
        let long_strings = vec![Some(long_string.clone()), None, Some("short".to_string())];
        let string_view_array = StringViewArray::from(long_strings);

        run_test(
            Arc::new(string_view_array),
            vec![
                Some(Variant::from(long_string.as_str())),
                None,
                Some(Variant::from("short")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_struct() {
        // Test a simple struct with two fields: id (int64) and age (int32)
        let id_array = Int64Array::from(vec![Some(1001), Some(1002), None, Some(1003)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30), Some(35), None]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("age", DataType::Int32, true),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            None, // no nulls at the struct level
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 4);

        // Check first row: {"id": 1001, "age": 25}
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: {"id": 1002, "age": 30}
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));
        assert_eq!(obj2.get("age"), Some(Variant::from(30i32)));

        // Check third row: {"age": 35} (id is null, so omitted)
        let variant3 = result.value(2);
        let obj3 = variant3.as_object().unwrap();
        assert_eq!(obj3.get("id"), None);
        assert_eq!(obj3.get("age"), Some(Variant::from(35i32)));

        // Check fourth row: {"id": 1003} (age is null, so omitted)
        let variant4 = result.value(3);
        let obj4 = variant4.as_object().unwrap();
        assert_eq!(obj4.get("id"), Some(Variant::from(1003i64)));
        assert_eq!(obj4.get("age"), None);
    }

    #[test]
    fn test_cast_to_variant_struct_with_nulls() {
        // Test struct with null values at the struct level
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let age_array = Int32Array::from(vec![Some(25), Some(30)]);

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create null buffer to make second row null
        let null_buffer = NullBuffer::from(vec![true, false]);

        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(id_array), Arc::new(age_array)],
            Some(null_buffer),
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row: {"id": 1001, "age": 25}
        assert!(!result.is_null(0));
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));
        assert_eq!(obj1.get("age"), Some(Variant::from(25i32)));

        // Check second row: null struct
        assert!(result.is_null(1));
    }

    #[test]
    fn test_cast_to_variant_struct_performance() {
        // Test with a larger struct to demonstrate performance optimization
        // This test ensures that field arrays are only converted once, not per row
        let size = 1000;

        let id_array = Int64Array::from((0..size).map(|i| Some(i as i64)).collect::<Vec<_>>());
        let age_array = Int32Array::from(
            (0..size)
                .map(|i| Some((i % 100) as i32))
                .collect::<Vec<_>>(),
        );
        let score_array =
            Float64Array::from((0..size).map(|i| Some(i as f64 * 0.1)).collect::<Vec<_>>());

        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]);

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(id_array),
                Arc::new(age_array),
                Arc::new(score_array),
            ],
            None,
        );

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a few sample rows
        let variant0 = result.value(0);
        let obj0 = variant0.as_object().unwrap();
        assert_eq!(obj0.get("id"), Some(Variant::from(0i64)));
        assert_eq!(obj0.get("age"), Some(Variant::from(0i32)));
        assert_eq!(obj0.get("score"), Some(Variant::from(0.0f64)));

        let variant999 = result.value(999);
        let obj999 = variant999.as_object().unwrap();
        assert_eq!(obj999.get("id"), Some(Variant::from(999i64)));
        assert_eq!(obj999.get("age"), Some(Variant::from(99i32))); // 999 % 100 = 99
        assert_eq!(obj999.get("score"), Some(Variant::from(99.9f64)));
    }

    #[test]
    fn test_cast_to_variant_struct_performance_large() {
        // Test with even larger struct and more fields to demonstrate optimization benefits
        let size = 10000;
        let num_fields = 10;

        // Create arrays for many fields
        let mut field_arrays: Vec<ArrayRef> = Vec::new();
        let mut fields = Vec::new();

        for field_idx in 0..num_fields {
            match field_idx % 4 {
                0 => {
                    // Int64 fields
                    let array = Int64Array::from(
                        (0..size)
                            .map(|i| Some(i as i64 + field_idx as i64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int_field_{}", field_idx),
                        DataType::Int64,
                        false,
                    ));
                }
                1 => {
                    // Int32 fields
                    let array = Int32Array::from(
                        (0..size)
                            .map(|i| Some((i % 1000) as i32 + field_idx as i32))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("int32_field_{}", field_idx),
                        DataType::Int32,
                        false,
                    ));
                }
                2 => {
                    // Float64 fields
                    let array = Float64Array::from(
                        (0..size)
                            .map(|i| Some(i as f64 * 0.1 + field_idx as f64))
                            .collect::<Vec<_>>(),
                    );
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("float_field_{}", field_idx),
                        DataType::Float64,
                        false,
                    ));
                }
                _ => {
                    // Binary fields
                    let binary_data: Vec<Option<&[u8]>> = (0..size)
                        .map(|i| {
                            // Use static data to avoid lifetime issues in tests
                            match i % 3 {
                                0 => Some(b"test_data_0" as &[u8]),
                                1 => Some(b"test_data_1" as &[u8]),
                                _ => Some(b"test_data_2" as &[u8]),
                            }
                        })
                        .collect();
                    let array = BinaryArray::from(binary_data);
                    field_arrays.push(Arc::new(array));
                    fields.push(Field::new(
                        format!("binary_field_{}", field_idx),
                        DataType::Binary,
                        false,
                    ));
                }
            }
        }

        let struct_array = StructArray::new(Fields::from(fields), field_arrays, None);

        let result = cast_to_variant(&struct_array).unwrap();
        assert_eq!(result.len(), size);

        // Verify a sample of rows
        for sample_idx in [0, size / 4, size / 2, size - 1] {
            let variant = result.value(sample_idx);
            let obj = variant.as_object().unwrap();

            // Should have all fields
            assert_eq!(obj.len(), num_fields);

            // Verify a few field values
            if let Some(int_field_0) = obj.get("int_field_0") {
                assert_eq!(int_field_0, Variant::from(sample_idx as i64));
            }
            if let Some(float_field_2) = obj.get("float_field_2") {
                assert_eq!(float_field_2, Variant::from(sample_idx as f64 * 0.1 + 2.0));
            }
        }
    }

    #[test]
    fn test_cast_to_variant_nested_struct() {
        // Test nested struct: person with location struct
        let id_array = Int64Array::from(vec![Some(1001), Some(1002)]);
        let x_array = Float64Array::from(vec![Some(40.7), Some(37.8)]);
        let y_array = Float64Array::from(vec![Some(-74.0), Some(-122.4)]);

        // Create location struct
        let location_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]);
        let location_struct = StructArray::new(
            location_fields.clone(),
            vec![Arc::new(x_array), Arc::new(y_array)],
            None,
        );

        // Create person struct containing location
        let person_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("location", DataType::Struct(location_fields), true),
        ]);
        let person_struct = StructArray::new(
            person_fields,
            vec![Arc::new(id_array), Arc::new(location_struct)],
            None,
        );

        let result = cast_to_variant(&person_struct).unwrap();
        assert_eq!(result.len(), 2);

        // Check first row
        let variant1 = result.value(0);
        let obj1 = variant1.as_object().unwrap();
        assert_eq!(obj1.get("id"), Some(Variant::from(1001i64)));

        let location_variant1 = obj1.get("location").unwrap();
        let location_obj1 = location_variant1.as_object().unwrap();
        assert_eq!(location_obj1.get("x"), Some(Variant::from(40.7f64)));
        assert_eq!(location_obj1.get("y"), Some(Variant::from(-74.0f64)));

        // Check second row
        let variant2 = result.value(1);
        let obj2 = variant2.as_object().unwrap();
        assert_eq!(obj2.get("id"), Some(Variant::from(1002i64)));

        let location_variant2 = obj2.get("location").unwrap();
        let location_obj2 = location_variant2.as_object().unwrap();
        assert_eq!(location_obj2.get("x"), Some(Variant::from(37.8f64)));
        assert_eq!(location_obj2.get("y"), Some(Variant::from(-122.4f64)));
    }

    #[test]
    fn test_cast_to_variant_date() {
        // Date32Array
        run_test(
            Arc::new(Date32Array::from(vec![
                Some(Date32Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date32Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date32Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );

        // Date64Array
        run_test(
            Arc::new(Date64Array::from(vec![
                Some(Date64Type::from_naive_date(NaiveDate::MIN)),
                None,
                Some(Date64Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2025, 8, 1).unwrap(),
                )),
                Some(Date64Type::from_naive_date(NaiveDate::MAX)),
            ])),
            vec![
                Some(Variant::Date(NaiveDate::MIN)),
                None,
                Some(Variant::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap())),
                Some(Variant::Date(NaiveDate::MAX)),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded() {
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_value("apple");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_value("cherry");
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                Some(Variant::from("cherry")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_run_end_encoded_with_nulls() {
        use arrow::array::StringRunBuilder;
        use arrow::datatypes::Int32Type;

        // Test run-end encoded array with nulls
        let mut builder = StringRunBuilder::<Int32Type>::new();
        builder.append_value("apple");
        builder.append_null();
        builder.append_value("banana");
        builder.append_value("banana");
        builder.append_null();
        builder.append_null();
        let run_array = builder.finish();

        run_test(
            Arc::new(run_array),
            vec![
                Some(Variant::from("apple")),
                None,
                Some(Variant::from("banana")),
                Some(Variant::from("banana")),
                None,
                None,
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary() {
        let values = StringArray::from(vec!["apple", "banana", "cherry", "date"]);
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0), Some(3)]);
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("apple")),
                Some(Variant::from("banana")),
                None,
                Some(Variant::from("cherry")),
                Some(Variant::from("apple")),
                Some(Variant::from("date")),
            ],
        );
    }

    #[test]
    fn test_cast_to_variant_dictionary_with_nulls() {
        // Test dictionary with null values in the values array
        let values = StringArray::from(vec![Some("a"), None, Some("c")]);
        let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        run_test(
            Arc::new(dict_array),
            vec![
                Some(Variant::from("a")),
                None, // key 1 points to null value
                Some(Variant::from("c")),
                Some(Variant::from("a")),
            ],
        );
    }

    /// Converts the given `Array` to a `VariantArray` and tests the conversion
    /// against the expected values. It also tests the handling of nulls by
    /// setting one element to null and verifying the output.
    fn run_test(values: ArrayRef, expected: Vec<Option<Variant>>) {
        // test without nulls
        let variant_array = cast_to_variant(&values).unwrap();
        assert_eq!(variant_array.len(), expected.len());
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(!variant_array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(variant_array.value(i), *value, "mismatch at index {i}");
                }
                None => {
                    assert!(variant_array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }
}

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

use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{
    Array, AsArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{
    BinaryType, BinaryViewType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, LargeBinaryType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow_schema::{ArrowError, DataType};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use half::f16;
use parquet_variant::Variant;

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
/// to downcast the generic array to a specific array type and `cast_fn`
/// to transform each element to a type compatible with Variant
macro_rules! cast_conversion {
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
    // todo: handle other types like Boolean, Strings, Date, Timestamp, etc.
    match input_type {
        DataType::Binary => {
            cast_conversion!(BinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::LargeBinary => {
            cast_conversion!(LargeBinaryType, as_bytes, |v| v, input, builder);
        }
        DataType::BinaryView => {
            cast_conversion!(BinaryViewType, as_byte_view, |v| v, input, builder);
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
            cast_conversion!(
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
        DataType::FixedSizeBinary(_) => {
            cast_conversion_nongeneric!(as_fixed_size_binary, |v| v, input, builder);
        }
        DataType::Timestamp(time_unit, time_zone) => {
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
        dt => {
            return Err(ArrowError::CastError(format!(
                "Unsupported data type for casting to Variant: {dt:?}",
            )));
        }
    };
    Ok(builder.build())
}

// TODO do we need a cast_with_options to allow specifying conversion behavior,
// e.g. how to handle overflows, whether to convert to Variant::Null or return
// an error, etc. ?

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, FixedSizeBinaryBuilder, Float16Array, Float32Array, Float64Array,
        GenericByteBuilder, GenericByteViewBuilder, Int16Array, Int32Array, Int64Array, Int8Array,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use parquet_variant::{Variant, VariantDecimal16};
    use std::{sync::Arc, vec};

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

        // nanoseconds should get truncated to microseconds
        let nanosecond_array = TimestampNanosecondArray::from(vec![Some(nanosecond), None]);
        run_array_tests(
            microsecond,
            Arc::new(nanosecond_array.clone()),
            Arc::new(nanosecond_array.with_timezone("+01:00".to_string())),
        )
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

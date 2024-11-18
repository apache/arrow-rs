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

use arrow_array::{
    types::RunEndIndexType, Array, ArrayRef, ArrowPrimitiveType, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalYearMonthArray,
    PrimitiveArray, RunArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, TypedRunArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};

use crate::cast_with_options;

use super::CastOptions;

/// Attempt to cast a run-encoded array into a new type.
///
/// `K` is the *current* run end index type
pub(crate) fn run_end_cast<K: RunEndIndexType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let ree_array = array
        .as_any()
        .downcast_ref::<RunArray<K>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast run end array to RunArray of the expected type"
                    .to_string(),
            )
        })?;

    match to_type {
        // Potentially convert to a new value or run end type
        DataType::RunEndEncoded(re_t, dt) => {
            let values = cast_with_options(ree_array.values(), dt.data_type(), cast_options)?;
            let re = PrimitiveArray::<K>::new(ree_array.run_ends().inner().clone(), None);
            let re = cast_with_options(&re, re_t.data_type(), cast_options)?;

            // TODO: we shouldn't need to validate the new run length array
            // since we can assume we are converting from a valid one, but
            // there's no "unchecked" variant yet
            let result: Arc<dyn Array> = match re.data_type() {
                DataType::Int16 => Arc::new(RunArray::try_new(
                    re.as_any().downcast_ref::<Int16Array>().unwrap(),
                    &values,
                )?),
                DataType::Int32 => Arc::new(RunArray::try_new(
                    re.as_any().downcast_ref::<Int32Array>().unwrap(),
                    &values,
                )?),
                DataType::Int64 => Arc::new(RunArray::try_new(
                    re.as_any().downcast_ref::<Int64Array>().unwrap(),
                    &values,
                )?),
                _ => Err(ArrowError::ComputeError(format!(
                    "Invalid run end type requested during cast: {:?}",
                    re.data_type()
                )))?,
            };

            Ok(result.slice(ree_array.run_ends().offset(), ree_array.run_ends().len()))
        }
        // Convert to a primitive value
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => {
            // TODO this could be somewhat inefficent, since the run encoded
            // array is initially transformed into a primitive array of the same
            // type, then casted to the (potentially) new type. For example,
            // casting a run encoded array of Float32 to Float64 will first
            // create a primitive array of Float32s, then convert that primitive
            // array to Float64.
            cast_with_options(&run_array_to_primitive(ree_array)?, to_type, cast_options)
        }
        _ => todo!(),
    }
}

/// Converts a run array of primitive values into a primitive array, without changing the type
fn run_array_to_primitive<R: RunEndIndexType>(ra: &RunArray<R>) -> Result<ArrayRef, ArrowError> {
    let prim = match ra.values().data_type() {
        DataType::Int8 => typed_run_array_to_primitive(ra.downcast::<Int8Array>().unwrap()),
        DataType::Int16 => typed_run_array_to_primitive(ra.downcast::<Int16Array>().unwrap()),
        DataType::Int32 => typed_run_array_to_primitive(ra.downcast::<Int32Array>().unwrap()),
        DataType::Int64 => typed_run_array_to_primitive(ra.downcast::<Int64Array>().unwrap()),
        DataType::UInt8 => typed_run_array_to_primitive(ra.downcast::<UInt8Array>().unwrap()),
        DataType::UInt16 => typed_run_array_to_primitive(ra.downcast::<UInt16Array>().unwrap()),
        DataType::UInt32 => typed_run_array_to_primitive(ra.downcast::<UInt32Array>().unwrap()),
        DataType::UInt64 => typed_run_array_to_primitive(ra.downcast::<UInt64Array>().unwrap()),
        DataType::Float16 => typed_run_array_to_primitive(ra.downcast::<Float16Array>().unwrap()),
        DataType::Float32 => typed_run_array_to_primitive(ra.downcast::<Float32Array>().unwrap()),
        DataType::Float64 => typed_run_array_to_primitive(ra.downcast::<Float64Array>().unwrap()),
        DataType::Date32 => typed_run_array_to_primitive(ra.downcast::<Date32Array>().unwrap()),
        DataType::Date64 => typed_run_array_to_primitive(ra.downcast::<Date64Array>().unwrap()),
        DataType::Time32(arrow_schema::TimeUnit::Second) => {
            typed_run_array_to_primitive(ra.downcast::<Time32SecondArray>().unwrap())
        }
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            typed_run_array_to_primitive(ra.downcast::<Time32MillisecondArray>().unwrap())
        }
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            typed_run_array_to_primitive(ra.downcast::<Time64MicrosecondArray>().unwrap())
        }
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            typed_run_array_to_primitive(ra.downcast::<Time64NanosecondArray>().unwrap())
        }
        DataType::Decimal128(_, _) => {
            typed_run_array_to_primitive(ra.downcast::<Decimal128Array>().unwrap())
        }
        DataType::Decimal256(_, _) => {
            typed_run_array_to_primitive(ra.downcast::<Decimal256Array>().unwrap())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            typed_run_array_to_primitive(ra.downcast::<TimestampSecondArray>().unwrap())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            typed_run_array_to_primitive(ra.downcast::<TimestampMillisecondArray>().unwrap())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            typed_run_array_to_primitive(ra.downcast::<TimestampMicrosecondArray>().unwrap())
        }

        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            typed_run_array_to_primitive(ra.downcast::<TimestampNanosecondArray>().unwrap())
        }
        DataType::Duration(arrow_schema::TimeUnit::Second) => {
            typed_run_array_to_primitive(ra.downcast::<DurationSecondArray>().unwrap())
        }
        DataType::Duration(arrow_schema::TimeUnit::Millisecond) => {
            typed_run_array_to_primitive(ra.downcast::<DurationMillisecondArray>().unwrap())
        }
        DataType::Duration(arrow_schema::TimeUnit::Microsecond) => {
            typed_run_array_to_primitive(ra.downcast::<DurationMicrosecondArray>().unwrap())
        }
        DataType::Duration(arrow_schema::TimeUnit::Nanosecond) => {
            typed_run_array_to_primitive(ra.downcast::<DurationNanosecondArray>().unwrap())
        }
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => {
            typed_run_array_to_primitive(ra.downcast::<IntervalYearMonthArray>().unwrap())
        }
        DataType::Interval(arrow_schema::IntervalUnit::DayTime) => {
            typed_run_array_to_primitive(ra.downcast::<IntervalDayTimeArray>().unwrap())
        }
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
            typed_run_array_to_primitive(ra.downcast::<IntervalYearMonthArray>().unwrap())
        }
        _ => {
            return Err(ArrowError::ComputeError(format!(
                "Cannot convert run-end encoded array of type {:?} to primitive type",
                ra.values().data_type()
            )))
        }
    };

    Ok(prim)
}

/// "Unroll" a run-end encoded array of primitive values into a primitive array.
/// This function should be efficient for long run lenghts due to the use of
/// Builder's `append_value_n`
fn typed_run_array_to_primitive<R: RunEndIndexType, T: ArrowPrimitiveType>(
    arr: TypedRunArray<R, PrimitiveArray<T>>,
) -> ArrayRef {
    let mut builder = PrimitiveArray::<T>::builder(
        arr.run_ends()
            .values()
            .last()
            .map(|end| end.as_usize())
            .unwrap_or(0),
    );

    let mut last = 0;
    for (run_end, val) in arr
        .run_ends()
        .values()
        .iter()
        .zip(arr.values().values().iter().copied())
    {
        let run_end = run_end.as_usize();
        let run_length = run_end - last;
        builder.append_value_n(val, run_length);
        last = run_end;
    }

    // TODO: this slice could be optimized by only copying the relevant parts of
    // the array, but this might be tricky to get right because a slice can
    // start or end in the middle of a run.
    Arc::new(builder.finish().slice(arr.offset(), arr.len()))
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use crate::can_cast_types;

    use super::*;

    #[test]
    fn test_can_cast_run_ends() {
        let re_i64 = Arc::new(Field::new("run ends", DataType::Int64, false));
        let re_i32 = Arc::new(Field::new("run ends", DataType::Int64, false));
        let va_f64 = Arc::new(Field::new("values", DataType::Float64, true));
        let va_str = Arc::new(Field::new("values", DataType::Utf8, true));

        // can change run end type of non-primitive
        assert!(can_cast_types(
            &DataType::RunEndEncoded(re_i32.clone(), va_str.clone()),
            &DataType::RunEndEncoded(re_i64.clone(), va_str.clone())
        ));

        // can cast from primitive type to primitive
        assert!(can_cast_types(
            &DataType::RunEndEncoded(re_i32.clone(), va_f64.clone()),
            &DataType::Float64
        ));

        // cannot cast from non-primitive to flat array
        assert!(!can_cast_types(
            &DataType::RunEndEncoded(re_i32.clone(), va_str.clone()),
            &DataType::Utf8
        ));
    }

    #[test]
    fn test_run_end_to_primitive() {
        let run_ends = vec![2, 4, 5];
        let values = vec![10, 20, 30];
        let ree =
            RunArray::try_new(&Int32Array::from(run_ends), &Int32Array::from(values)).unwrap();

        let result = cast_with_options(&ree, &DataType::Int32, &CastOptions::default()).unwrap();

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values(), &[10, 10, 20, 20, 30]);
    }

    #[test]
    fn test_run_end_sliced_to_primitive() {
        let run_ends = vec![2, 4, 5];
        let values = vec![10, 20, 30];
        let ree = RunArray::try_new(&Int32Array::from(run_ends), &Int32Array::from(values))
            .unwrap()
            .slice(1, 3);

        let result = cast_with_options(&ree, &DataType::Int32, &CastOptions::default()).unwrap();

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values(), &[10, 20, 20]);
    }

    #[test]
    fn test_run_end_to_run_end() {
        let run_ends = vec![2, 4, 5];
        let values = vec![10, 20, 30];
        let ree =
            RunArray::try_new(&Int32Array::from(run_ends), &Int32Array::from(values)).unwrap();

        let new_re_type = Field::new("run ends", DataType::Int64, false);
        let new_va_type = Field::new("values", DataType::Float64, true);
        let result = cast_with_options(
            &ree,
            &DataType::RunEndEncoded(Arc::new(new_re_type), Arc::new(new_va_type)),
            &CastOptions::default(),
        )
        .unwrap();

        let result =
            cast_with_options(&result, &DataType::Float64, &CastOptions::default()).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.values(), &[10.0, 10.0, 20.0, 20.0, 30.0]);
    }
}

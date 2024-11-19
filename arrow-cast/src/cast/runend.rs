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
    builder::ArrayBuilder, make_array, types::RunEndIndexType, Array, ArrayRef, ArrowPrimitiveType,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    PrimitiveArray, RunArray, TypedRunArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{ArrowNativeType, NullBufferBuilder};
use arrow_data::transform::MutableArrayData;
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

    let curr_value_type = ree_array.values().data_type();

    match (curr_value_type, to_type) {
        // Potentially convert to a new value or run end type
        (_, DataType::RunEndEncoded(re_t, dt)) => {
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
        // We match against the native types in order to use an optimized kernel
        (DataType::Int8, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Int8Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Int16, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Int16Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Int32, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Int32Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Int64, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Int64Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::UInt8, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<UInt8Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::UInt16, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<UInt16Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::UInt32, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<UInt32Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::UInt64, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<UInt64Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Float16, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Float16Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Float32, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Float32Array>().unwrap()),
            to_type,
            cast_options,
        ),
        (DataType::Float64, _) => cast_with_options(
            &typed_run_array_to_primitive(ree_array.downcast::<Float64Array>().unwrap()),
            to_type,
            cast_options,
        ),
        // For all other types, we use an interpretation-based approach
        _ => {
            // TODO this could be somewhat inefficent, since the run encoded
            // array is initially transformed into a flat array of the same
            // type, then casted to the (potentially) new type. For example,
            // casting a run encoded array of Float32 to Float64 will first
            // create a primitive array of Float32s, then convert that primitive
            // array to Float64.
            cast_with_options(&run_array_to_flat(ree_array)?, to_type, cast_options)
        }
    }
}

/// "Unroll" a run-end encoded array of primitive values into a primitive array.
/// This function should be efficient for long run lenghts due to the use of
/// Builder's `append_value_n`. Uses `PrimitiveBuilder`, so does not do any
/// interpretation.
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

    // copy all values into the builder
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
    let mut result = builder.finish();

    // if we have null values, decode them as well
    if let Some(null_buffer) = arr.values().nulls() {
        let mut nbb = NullBufferBuilder::new(builder.len());

        let mut last = 0;
        for (run_end, val) in arr.run_ends().values().iter().zip(null_buffer.iter()) {
            let run_end = run_end.as_usize();
            let run_length = run_end - last;
            if val {
                nbb.append_n_non_nulls(run_length);
            } else {
                nbb.append_n_nulls(run_length);
            }

            last = run_end;
        }

        let nb = nbb.finish();
        result = PrimitiveArray::<T>::new(result.values().clone(), nb);
    }

    // TODO: this slice could be optimized by only copying the relevant parts of
    // the array, but this might be tricky to get right because a slice can
    // start or end in the middle of a run.
    Arc::new(result.slice(arr.offset(), arr.len()))
}

/// Converts a run array into a flat array, without changing the type, by
/// "unrolling" it. Uses `MutableArrayData`, which must use interpretation for
/// each run.
fn run_array_to_flat<R: RunEndIndexType>(ra: &RunArray<R>) -> Result<ArrayRef, ArrowError> {
    let array_data = ra.values().to_data();
    let mut builder = MutableArrayData::new(vec![&array_data], false, ra.len());

    let mut last = 0;
    for (idx, run_end) in ra.run_ends().values().iter().enumerate() {
        let run_end = run_end.as_usize();
        let run_length = run_end - last;
        if run_length == 0 {
            continue;
        }

        builder.extend_n(0, idx, idx + 1, run_length);

        last = run_end;
    }

    let result = make_array(builder.freeze());

    // TODO: this slice could be optimized by only copying the relevant parts of
    // the array, but this might be tricky to get right because a slice can
    // start or end in the middle of a run.
    Ok(result.slice(ra.offset(), ra.len()))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, StringArray};
    use arrow_schema::Field;

    use super::*;

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
    fn test_run_end_to_string() {
        let run_ends = vec![2, 4, 5];
        let values = vec!["hello", "world", "test"];
        let ree =
            RunArray::try_new(&Int32Array::from(run_ends), &StringArray::from(values)).unwrap();

        let result = cast_with_options(&ree, &DataType::Utf8, &CastOptions::default()).unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Vec<Option<&str>> = result.iter().collect();
        assert_eq!(
            result,
            &[
                Some("hello"),
                Some("hello"),
                Some("world"),
                Some("world"),
                Some("test")
            ]
        );
    }

    #[test]
    fn test_run_end_to_primitive_nulls() {
        let run_ends = vec![2, 4, 5];
        let values = vec![Some(10), None, Some(30)];
        let ree =
            RunArray::try_new(&Int32Array::from(run_ends), &Int32Array::from(values)).unwrap();

        let result = cast_with_options(&ree, &DataType::Int32, &CastOptions::default()).unwrap();

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: Vec<Option<i32>> = result.iter().collect();
        assert_eq!(result, vec![Some(10), Some(10), None, None, Some(30)]);
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

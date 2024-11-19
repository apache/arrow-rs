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
    make_array, types::RunEndIndexType, Array, ArrayRef, Int16Array, Int32Array, Int64Array,
    PrimitiveArray, RunArray,
};
use arrow_buffer::ArrowNativeType;
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

/// Converts a run array of primitive values into a primitive array, without changing the type
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
    use arrow_array::Float64Array;
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

use crate::cast::*;

pub(crate) fn run_end_encoded_cast<K: RunEndIndexType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::RunEndEncoded(_run_end_field, _values_field) => {
            let run_array = array
                .as_any()
                .downcast_ref::<RunArray<K>>()
                .ok_or_else(|| ArrowError::CastError("Expected RunArray".to_string()))?;

            let values = run_array.values();

            match to_type {
                // CASE 1: Stay as RunEndEncoded, cast only the values
                DataType::RunEndEncoded(_target_run_end_field, target_value_field) => {
                    let cast_values =
                        cast_with_options(values, target_value_field.data_type(), cast_options)?;

                    let run_ends_array = PrimitiveArray::<K>::from_iter_values(
                        run_array.run_ends().values().iter().copied(),
                    );

                    let new_run_array =
                        RunArray::<K>::try_new(&run_ends_array, cast_values.as_ref())?;
                    Ok(Arc::new(new_run_array))
                }

                // CASE 2: Expand to logical form
                _ => {
                    let total_len = run_array.len();
                    let indices = Int32Array::from_iter_values(
                        (0..total_len).map(|i| run_array.get_physical_index(i) as i32),
                    );

                    let taken = take(values.as_ref(), &indices, None)?;

                    if taken.data_type() != to_type {
                        cast_with_options(taken.as_ref(), to_type, cast_options)
                    } else {
                        Ok(taken)
                    }
                }
            }
        }

        _ => Err(ArrowError::CastError(format!(
            "Cannot cast array of type {:?} to RunEndEncodedArray",
            array.data_type()
        ))),
    }
}

/// Attempts to cast an array to a RunEndEncoded array with the specified index type K
/// and value type. This function performs run-length encoding on the input array.
///
/// # Arguments
/// * `array` - The input array to be run-length encoded
/// * `value_type` - The target data type for the values in the RunEndEncoded array
/// * `cast_options` - Options controlling the casting behavior
///
/// # Returns
/// A `Result` containing the new `RunArray` or an `ArrowError` if casting fails
///
/// # Process
/// 1. Cast the input array to the target value type if needed
/// 2. Iterate through the array to identify runs of consecutive equal values
/// 3. Build run_ends array indicating where each run terminates
/// 4. Build values array containing the unique values for each run
/// 5. Construct and return the RunArray
pub(crate) fn cast_to_run_end_encoded<K: RunEndIndexType>(
    array: &dyn Array,
    value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    // Step 1: Cast the input array to the target value type if necessary
    let cast_array = if array.data_type() == value_type {
        // No casting needed, use the array as-is
        make_array(array.to_data())
    } else {
        // Cast to the target value type
        cast_with_options(array, value_type, cast_options)?
    };

    // Step 2: Run-length encode the cast array
    // We'll use a builder to construct the RunArray efficiently
    let mut run_ends_builder = PrimitiveBuilder::<K>::new();

    if cast_array.len() == 0 {
        // Handle empty array case
        let empty_run_ends = run_ends_builder.finish();
        let empty_values = make_array(ArrayData::new_empty(value_type));
        return Ok(Arc::new(RunArray::<K>::try_new(
            &empty_run_ends,
            empty_values.as_ref(),
        )?));
    }

    // Create a temporary builder to construct the run array
    // We'll iterate through and build runs by comparing adjacent elements
    let mut run_ends_vec = Vec::new();
    let mut values_indices = Vec::new();

    let mut current_run_end = 1usize;

    // Add the first element as the start of the first run
    values_indices.push(0);

    for i in 1..cast_array.len() {
        // For simplicity, we'll use a basic comparison approach
        // In practice, you'd want more sophisticated comparison based on data type
        let values_equal = match (cast_array.is_null(i), cast_array.is_null(i - 1)) {
            (true, true) => true, // Both null
            (false, false) => {
                // Both non-null - use slice comparison as a basic approach
                // This is a simplified implementation
                cast_array.slice(i, 1).to_data() == cast_array.slice(i - 1, 1).to_data()
            }
            _ => false, // One null, one not null
        };

        if !values_equal {
            // End current run, start new run
            run_ends_vec.push(current_run_end);
            values_indices.push(i);
        }

        current_run_end += 1;
    }

    // Add the final run end
    run_ends_vec.push(current_run_end);

    // Step 4: Build the run_ends array
    for &run_end in &run_ends_vec {
        run_ends_builder.append_value(K::Native::from_usize(run_end).unwrap());
    }
    let run_ends_array = run_ends_builder.finish();

    // Step 5: Build the values array by taking elements at the run start positions
    let indices = PrimitiveArray::<UInt32Type>::from_iter_values(
        values_indices.iter().map(|&idx| idx as u32),
    );
    let values_array = take(&cast_array, &indices, None)?;

    // Step 7: Create and return the RunArray
    let run_array = RunArray::<K>::try_new(&run_ends_array, values_array.as_ref())?;
    Ok(Arc::new(run_array))
}

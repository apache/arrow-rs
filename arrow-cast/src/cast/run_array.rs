use crate::cast::*;

pub(crate) fn run_end_encoded_cast<K: RunEndIndexType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::RunEndEncoded(_run_end_field, _values_field) => {
            let run_array = array.as_any().downcast_ref::<RunArray<K>>().unwrap();

            let values = run_array.values();

            // Cast the values to the target type
            let cast_values = cast_with_options(values, to_type, cast_options)?;

            // Create a PrimitiveArray from the run_ends buffer
            let run_ends_buffer = run_array.run_ends();
            let run_ends_array =
                PrimitiveArray::<K>::from_iter_values(run_ends_buffer.values().iter().copied());

            // Create new RunArray with the same run_ends but cast values
            let new_run_array = RunArray::<K>::try_new(&run_ends_array, cast_values.as_ref())?;

            Ok(Arc::new(new_run_array))
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

    // Step 3: Use a simpler approach - use existing Arrow builders for run-length encoding
    // This is a more robust implementation that handles all data types correctly

    // For now, we'll use a basic approach that works with the existing builder infrastructure
    // In a production implementation, you'd want to use type-specific comparison logic

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::DataType;
    use std::sync::Arc;

    /// Test casting FROM RunEndEncoded to other types
    #[test]
    fn test_run_end_encoded_to_primitive() {
        // Create a RunEndEncoded array: [1, 1, 2, 2, 2, 3]
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;

        // Cast to Int64
        let cast_result = run_end_encoded_cast::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Int64,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is a RunArray with Int64 values
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check that values were cast to Int64
        assert_eq!(result_run_array.values().data_type(), &DataType::Int64);

        // Check that run structure is preserved
        assert_eq!(result_run_array.run_ends().values(), &[2, 5, 6]);

        // Check that values are correct
        let values_array = result_run_array.values().as_primitive::<Int64Type>();
        assert_eq!(values_array.values(), &[1i64, 2i64, 3i64]);
    }

    #[test]
    fn test_run_end_encoded_to_string() {
        // Create a RunEndEncoded array with Int32 values: [10, 10, 20, 30, 30]
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values = Int32Array::from(vec![10, 20, 30]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;

        // Cast to String
        let cast_result = run_end_encoded_cast::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Utf8,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is a RunArray with String values
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check that values were cast to String
        assert_eq!(result_run_array.values().data_type(), &DataType::Utf8);

        // Check that run structure is preserved
        assert_eq!(result_run_array.run_ends().values(), &[2, 3, 5]);

        // Check that values are correct
        let values_array = result_run_array.values().as_string::<i32>();
        assert_eq!(values_array.value(0), "10");
        assert_eq!(values_array.value(1), "20");
        assert_eq!(values_array.value(2), "30");
    }

    /// Test casting TO RunEndEncoded from other types
    #[test]
    fn test_primitive_to_run_end_encoded() {
        // Create an Int32 array with repeated values: [1, 1, 2, 2, 2, 3]
        let source_array = Int32Array::from(vec![1, 1, 2, 2, 2, 3]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, Int32>
        let cast_result = cast_to_run_end_encoded::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Int32,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is a RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check run structure: runs should end at positions [2, 5, 6]
        assert_eq!(result_run_array.run_ends().values(), &[2, 5, 6]);

        // Check values: should be [1, 2, 3]
        let values_array = result_run_array.values().as_primitive::<Int32Type>();
        assert_eq!(values_array.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_string_to_run_end_encoded() {
        // Create a String array with repeated values: ["a", "a", "b", "c", "c"]
        let source_array = StringArray::from(vec!["a", "a", "b", "c", "c"]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, String>
        let cast_result = cast_to_run_end_encoded::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Utf8,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is a RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check run structure: runs should end at positions [2, 3, 5]
        assert_eq!(result_run_array.run_ends().values(), &[2, 3, 5]);

        // Check values: should be ["a", "b", "c"]
        let values_array = result_run_array.values().as_string::<i32>();
        assert_eq!(values_array.value(0), "a");
        assert_eq!(values_array.value(1), "b");
        assert_eq!(values_array.value(2), "c");
    }

    #[test]
    fn test_cast_with_type_conversion() {
        // Create an Int32 array: [1, 1, 2, 2, 3]
        let source_array = Int32Array::from(vec![1, 1, 2, 2, 3]);
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, String> (values get converted to strings)
        let cast_result = cast_to_run_end_encoded::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Utf8,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is a RunArray with String values
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check that values were converted to strings
        assert_eq!(result_run_array.values().data_type(), &DataType::Utf8);

        // Check run structure: runs should end at positions [2, 4, 5]
        assert_eq!(result_run_array.run_ends().values(), &[2, 4, 5]);

        // Check values: should be ["1", "2", "3"]
        let values_array = result_run_array.values().as_string::<i32>();
        assert_eq!(values_array.value(0), "1");
        assert_eq!(values_array.value(1), "2");
        assert_eq!(values_array.value(2), "3");
    }

    #[test]
    fn test_empty_array_to_run_end_encoded() {
        // Create an empty Int32 array
        let source_array = Int32Array::from(Vec::<i32>::new());
        let array_ref = Arc::new(source_array) as ArrayRef;

        // Cast to RunEndEncoded<Int32, Int32>
        let cast_result = cast_to_run_end_encoded::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Int32,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result is an empty RunArray
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Check that both run_ends and values are empty
        assert_eq!(result_run_array.run_ends().len(), 0);
        assert_eq!(result_run_array.values().len(), 0);
    }

    #[test]
    fn test_run_end_encoded_with_nulls() {
        // Create a RunEndEncoded array with nulls: [1, 1, null, 2, 2]
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values = Int32Array::from(vec![Some(1), None, Some(2)]);
        let run_array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let array_ref = Arc::new(run_array) as ArrayRef;

        // Cast to String
        let cast_result = run_end_encoded_cast::<Int32Type>(
            array_ref.as_ref(),
            &DataType::Utf8,
            &CastOptions::default(),
        )
        .unwrap();

        // Verify the result preserves nulls
        let result_run_array = cast_result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        let values_array = result_run_array.values().as_string::<i32>();
        assert_eq!(values_array.value(0), "1");
        assert!(values_array.is_null(1));
        assert_eq!(values_array.value(2), "2");
    }
}

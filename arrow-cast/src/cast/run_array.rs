use crate::cast::*;
use arrow_ord::partition::partition;

/// Attempts to cast a Run-End Encoded array to another type, handling both REE-to-REE
/// and REE-to-other type conversions with proper validation and error handling.
///
/// # Arguments
/// * `array` - The input Run-End Encoded array to be cast
/// * `to_type` - The target data type for the casting operation
/// * `cast_options` - Options controlling the casting behavior (e.g., safe vs unsafe)
///
/// # Returns
/// A `Result` containing the new `ArrayRef` or an `ArrowError` if casting fails
///
/// # Behavior
/// This function handles two main casting scenarios:
///
/// ## Case 1: REE-to-REE Casting
/// When casting to another Run-End Encoded type:
/// - Casts both the `values` and `run_ends` to their target types
/// - Validates that run-end casting only allows upcasts (Int16→Int32, Int16→Int64, Int32→Int64)
/// - Preserves the REE structure while updating both fields
/// - Returns a new `RunArray` with the appropriate run-end type (Int16, Int32, or Int64)
///
/// ## Case 2: REE-to-Other Casting
/// When casting to a non-REE type:
/// - Expands the REE array to its logical form by unpacking all values
/// - Applies the target type casting to the expanded array
/// - Returns a regular array of the target type (e.g., StringArray, Int64Array)
///
/// # Error Handling, error occurs if:
/// - the input array is not a Run-End Encoded array
/// - run-end downcasting would cause overflow
/// - the target run-end type is unsupported
/// - Propagates errors from underlying casting operations
///
/// # Safety Considerations
/// - Run-end casting uses `safe: false` to prevent silent overflow
/// - Only upcasts are allowed for run-ends to maintain valid REE structure
/// - Unpacking preserves null values and array length
/// - Type validation ensures only supported run-end types (Int16, Int32, Int64)
///
/// # Performance Notes
/// - REE-to-REE casting is efficient as it operates on the compressed structure
/// - REE-to-other casting requires full unpacking, which may be expensive for large arrays
/// - Run-end validation adds minimal overhead for safety
pub(crate) fn run_end_encoded_cast<K: RunEndIndexType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::RunEndEncoded(_, _) => {
            let run_array = array
                .as_any()
                .downcast_ref::<RunArray<K>>()
                .ok_or_else(|| ArrowError::CastError("Expected RunArray".to_string()))?;

            let values = run_array.values();

            match to_type {
                // CASE 1: Stay as RunEndEncoded, cast only the values
                DataType::RunEndEncoded(target_index_field, target_value_field) => {
                    let cast_values =
                        cast_with_options(values, target_value_field.data_type(), cast_options)?;

                    let run_ends_array = PrimitiveArray::<K>::from_iter_values(
                        run_array.run_ends().values().iter().copied(),
                    );
                    let cast_run_ends = cast_with_options(
                        &run_ends_array,
                        target_index_field.data_type(),
                        cast_options,
                    )?;
                    let new_run_array: ArrayRef = match target_index_field.data_type() {
                        DataType::Int16 => {
                            let re = cast_run_ends.as_primitive::<Int16Type>();
                            Arc::new(RunArray::<Int16Type>::try_new(re, cast_values.as_ref())?)
                        }
                        DataType::Int32 => {
                            let re = cast_run_ends.as_primitive::<Int32Type>();
                            Arc::new(RunArray::<Int32Type>::try_new(re, cast_values.as_ref())?)
                        }
                        DataType::Int64 => {
                            let re = cast_run_ends.as_primitive::<Int64Type>();
                            Arc::new(RunArray::<Int64Type>::try_new(re, cast_values.as_ref())?)
                        }
                        _ => {
                            return Err(ArrowError::CastError(
                                "Run-end type must be i16, i32, or i64".to_string(),
                            ));
                        }
                    };
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
/// and value type. This function performs run-end encoding on the input array.
///
/// # Arguments
/// * `array` - The input array to be run-end encoded
/// * `value_type` - The target data type for the values in the RunEndEncoded array
/// * `cast_options` - Options controlling the casting behavior
///
/// # Returns
/// A `Result` containing the new `RunArray` or an `ArrowError` if casting fails
///
/// # Process
/// 1. Cast the input array to the target value type if needed
/// 2. Partition the array to identify runs of consecutive equal values
/// 3. Build run_ends array indicating where each run terminates
/// 4. Build values array containing the unique values for each run
/// 5. Construct and return the RunArray
pub(crate) fn cast_to_run_end_encoded<K: RunEndIndexType>(
    array: &ArrayRef,
    value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let mut run_ends_builder = PrimitiveBuilder::<K>::new();

    // Cast the input array to the target value type if necessary
    let cast_array = if array.data_type() == value_type {
        array
    } else {
        &cast_with_options(array, value_type, cast_options)?
    };

    // Return early if the array to cast is empty
    if cast_array.is_empty() {
        let empty_run_ends = run_ends_builder.finish();
        let empty_values = make_array(ArrayData::new_empty(value_type));
        return Ok(Arc::new(RunArray::<K>::try_new(
            &empty_run_ends,
            empty_values.as_ref(),
        )?));
    }

    // REE arrays are handled by run_end_encoded_cast
    if let DataType::RunEndEncoded(_, _) = array.data_type() {
        unreachable!()
    }

    // Partition the array to identify runs of consecutive equal values
    let partitions = partition(&[Arc::clone(array)])?;
    let mut run_ends = Vec::new();
    let mut values_indexes = Vec::new();
    let mut last_partition_end = 0;
    for partition in partitions.ranges() {
        values_indexes.push(last_partition_end);
        run_ends.push(partition.end);
        last_partition_end = partition.end;
    }

    // Build the run_ends array
    for run_end in run_ends {
        run_ends_builder.append_value(
            K::Native::from_usize(run_end)
                .ok_or_else(|| ArrowError::CastError("Run end index out of range".to_string()))?,
        );
    }
    let run_ends_array = run_ends_builder.finish();
    // Build the values array by taking elements at the run start positions
    let indices = PrimitiveArray::<UInt32Type>::from_iter_values(
        values_indexes.iter().map(|&idx| idx as u32),
    );
    let values_array = take(&cast_array, &indices, None)?;

    // Create and return the RunArray
    let run_array = RunArray::<K>::try_new(&run_ends_array, values_array.as_ref())?;
    Ok(Arc::new(run_array))
}

/// Checks if a given data type can be cast to a RunEndEncoded array.
///
/// # Arguments
/// * `from_type` - The source data type to be checked
/// * `to_type` - The target data type to be checked
pub(crate) fn can_cast_to_run_end_encoded(from_type: &DataType, to_type: &DataType) -> bool {
    match to_type {
        DataType::RunEndEncoded(_, _) => {
            // Check if from_type supports equality (can be REE-encoded)
            match from_type {
                // Primitive types - support equality
                DataType::Boolean
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
                | DataType::Float64 => true,

                // String types - support equality
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,

                // Binary types - support equality
                DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_) => true,

                // Temporal types - support equality
                DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => true,

                // Decimal types - support equality
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => true,

                // Already REE-encoded - can be re-encoded
                DataType::RunEndEncoded(_, _) => true,

                _ => false,
            }
        }
        _ => false, // Not casting to REE type
    }
}

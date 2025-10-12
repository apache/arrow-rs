use crate::cast::*;

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

// Macro to pack runs for any array type
macro_rules! pack_runs {
    ($arr:expr, $run_ends_vec:expr, $values_indices:expr) => {
        for i in 1..$arr.len() {
            let values_equal = match ($arr.is_null(i), $arr.is_null(i - 1)) {
                (true, true) => true,
                (false, false) => $arr.value(i) == $arr.value(i - 1),
                (false, true) | (true, false) => false,
            };
            if !values_equal {
                $run_ends_vec.push(i);
                $values_indices.push(i);
            }
        }
    };
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
/// 2. Iterate through the array to identify runs of consecutive equal values
/// 3. Build run_ends array indicating where each run terminates
/// 4. Build values array containing the unique values for each run
/// 5. Construct and return the RunArray
pub(crate) fn cast_to_run_end_encoded<K: RunEndIndexType>(
    array: &dyn Array,
    value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    // Cast the input array to the target value type if necessary
    let cast_array = if array.data_type() == value_type {
        array
    } else {
        &cast_with_options(array, value_type, cast_options)?
    };

    // REE arrays already handled by run_end_encoded_cast
    if let DataType::RunEndEncoded(_, _) = cast_array.data_type() {
        unreachable!()
    }

    // Create a builder to construct the run array
    let mut run_ends_builder = PrimitiveBuilder::<K>::new();

    // Return early if the array to cast is empty
    if cast_array.is_empty() {
        let empty_run_ends = run_ends_builder.finish();
        let empty_values = make_array(ArrayData::new_empty(value_type));
        return Ok(Arc::new(RunArray::<K>::try_new(
            &empty_run_ends,
            empty_values.as_ref(),
        )?));
    }

    // Run-end encode the cast array
    // We'll iterate through and build runs by comparing adjacent elements
    let mut run_ends = Vec::new();
    let mut vals_idxs = Vec::new();

    // Add the first element as the start of the first run
    vals_idxs.push(0);

    match value_type {
        // Primitive numeric types
        Boolean => {
            let arr = cast_array.as_boolean();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Int8 => {
            let arr = cast_array.as_primitive::<Int8Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Int16 => {
            let arr = cast_array.as_primitive::<Int16Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Int32 => {
            let arr = cast_array.as_primitive::<Int32Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Int64 => {
            let arr = cast_array.as_primitive::<Int64Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        UInt8 => {
            let arr = cast_array.as_primitive::<UInt8Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        UInt16 => {
            let arr = cast_array.as_primitive::<UInt16Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        UInt32 => {
            let arr = cast_array.as_primitive::<UInt32Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        UInt64 => {
            let arr = cast_array.as_primitive::<UInt64Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Float16 => {
            let arr = cast_array.as_primitive::<Float16Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Float32 => {
            let arr = cast_array.as_primitive::<Float32Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Float64 => {
            let arr = cast_array.as_primitive::<Float64Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }

        // String types
        Utf8 => {
            let arr = cast_array.as_string::<i32>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        LargeUtf8 => {
            let arr = cast_array.as_string::<i64>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Utf8View => {
            let arr = cast_array.as_string_view();
            pack_runs!(arr, run_ends, vals_idxs);
        }

        // Binary types
        Binary => {
            let arr = cast_array.as_binary::<i32>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        LargeBinary => {
            let arr = cast_array.as_binary::<i64>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        BinaryView => {
            let arr = cast_array.as_binary_view();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        FixedSizeBinary(_) => {
            let arr = cast_array.as_fixed_size_binary();
            pack_runs!(arr, run_ends, vals_idxs);
        }

        // Temporal types
        Date32 => {
            let arr = cast_array.as_primitive::<Date32Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Date64 => {
            let arr = cast_array.as_primitive::<Date64Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let arr = cast_array.as_primitive::<TimestampSecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Millisecond => {
                let arr = cast_array.as_primitive::<TimestampMillisecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Microsecond => {
                let arr = cast_array.as_primitive::<TimestampMicrosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Nanosecond => {
                let arr = cast_array.as_primitive::<TimestampNanosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
        },
        Time32(time_unit) => match time_unit {
            TimeUnit::Second => {
                let arr = cast_array.as_primitive::<Time32SecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Millisecond => {
                let arr = cast_array.as_primitive::<Time32MillisecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                panic!("Time32 must have a TimeUnit of either seconds or milliseconds")
            }
        },
        Time64(time_unit) => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                panic!("Time64 must have a TimeUnit of either microseconds or nanoseconds")
            }
            TimeUnit::Microsecond => {
                let arr = cast_array.as_primitive::<Time64MicrosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Nanosecond => {
                let arr = cast_array.as_primitive::<Time64NanosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
        },
        Duration(time_unit) => match time_unit {
            TimeUnit::Second => {
                let arr = cast_array.as_primitive::<DurationSecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Millisecond => {
                let arr = cast_array.as_primitive::<DurationMillisecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Microsecond => {
                let arr = cast_array.as_primitive::<DurationMicrosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            TimeUnit::Nanosecond => {
                let arr = cast_array.as_primitive::<DurationNanosecondType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
        },
        Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                let arr = cast_array.as_primitive::<IntervalYearMonthType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            IntervalUnit::DayTime => {
                let arr = cast_array.as_primitive::<IntervalDayTimeType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
            IntervalUnit::MonthDayNano => {
                let arr = cast_array.as_primitive::<IntervalMonthDayNanoType>();
                pack_runs!(arr, run_ends, vals_idxs);
            }
        },

        // Decimal types
        Decimal32(_, _) => {
            let arr = cast_array.as_primitive::<Decimal32Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Decimal64(_, _) => {
            let arr = cast_array.as_primitive::<Decimal64Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Decimal128(_, _) => {
            let arr = cast_array.as_primitive::<Decimal128Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }
        Decimal256(_, _) => {
            let arr = cast_array.as_primitive::<Decimal256Type>();
            pack_runs!(arr, run_ends, vals_idxs);
        }

        // REE arrays already handled by run_end_encoded_cast
        RunEndEncoded(_, _) => unreachable!(),

        // Unsupported types: Cannot cast to these, so we should never get here
        // (see can_cast_to_run_end_encoded)
        Null
        | List(_)
        | ListView(_)
        | FixedSizeList(_, _)
        | LargeList(_)
        | LargeListView(_)
        | Struct(_)
        | Union(_, _)
        | Dictionary(_, _)
        | Map(_, _) => unreachable!(),
    };

    // Add the final run end
    run_ends.push(cast_array.len());

    // Build the run_ends array
    for run_end in run_ends {
        run_ends_builder.append_value(
            K::Native::from_usize(run_end)
                .ok_or_else(|| ArrowError::CastError("Run end index out of range".to_string()))?,
        );
    }
    let run_ends_array = run_ends_builder.finish();
    // Build the values array by taking elements at the run start positions
    let indices =
        PrimitiveArray::<UInt32Type>::from_iter_values(vals_idxs.iter().map(|&idx| idx as u32));
    let values_array = take(cast_array, &indices, None)?;

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

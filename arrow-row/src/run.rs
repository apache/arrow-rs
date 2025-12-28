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

use crate::{RowConverter, Rows, SortField, variable};
use arrow_array::types::RunEndIndexType;
use arrow_array::{PrimitiveArray, RunArray};
use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use arrow_schema::{ArrowError, SortOptions};

/// Computes the lengths of each row for a RunEndEncodedArray
pub fn compute_lengths<R: RunEndIndexType>(
    lengths: &mut [usize],
    rows: &Rows,
    array: &RunArray<R>,
) {
    let run_ends = array.run_ends().values();
    let mut logical_start = 0;

    // Iterate over each run and apply the same length to all logical positions in the run
    for (physical_idx, &run_end) in run_ends.iter().enumerate() {
        let logical_end = run_end.as_usize();
        let row = rows.row(physical_idx);
        let encoded_len = variable::encoded_len(Some(row.data));

        // Add the same length for all logical positions in this run
        for length in &mut lengths[logical_start..logical_end] {
            *length += encoded_len;
        }

        logical_start = logical_end;
    }
}

/// Encodes the provided `RunEndEncodedArray` to `out` with the provided `SortOptions`
///
/// `rows` should contain the encoded values
pub fn encode<R: RunEndIndexType>(
    data: &mut [u8],
    offsets: &mut [usize],
    rows: &Rows,
    opts: SortOptions,
    array: &RunArray<R>,
) {
    let run_ends = array.run_ends();

    let mut logical_idx = 0;
    let mut offset_idx = 1; // Skip first offset

    // Iterate over each run
    for physical_idx in 0..run_ends.values().len() {
        let run_end = run_ends.values()[physical_idx].as_usize();

        // Process all elements in this run
        while logical_idx < run_end && offset_idx < offsets.len() {
            let offset = &mut offsets[offset_idx];
            let out = &mut data[*offset..];

            // Use variable-length encoding to make the data self-describing
            let row = rows.row(physical_idx);
            let bytes_written = variable::encode_one(out, Some(row.data), opts);
            *offset += bytes_written;

            logical_idx += 1;
            offset_idx += 1;
        }

        // Break if we've processed all offsets
        if offset_idx >= offsets.len() {
            break;
        }
    }
}

/// Decodes a RunEndEncodedArray from `rows` with the provided `options`
///
/// # Safety
///
/// `rows` must contain valid data for the provided `converter`
pub unsafe fn decode<R: RunEndIndexType>(
    converter: &RowConverter,
    rows: &mut [&[u8]],
    field: &SortField,
    validate_utf8: bool,
) -> Result<RunArray<R>, ArrowError> {
    if rows.is_empty() {
        let values = unsafe { converter.convert_raw(&mut [], validate_utf8) }?;
        let run_ends_array = PrimitiveArray::<R>::try_new(ScalarBuffer::from(vec![]), None)?;
        return RunArray::<R>::try_new(&run_ends_array, &values[0]);
    }

    // Decode each row's REE data and collect the decoded values
    let mut decoded_values = Vec::new();
    let mut run_ends = Vec::new();
    let mut unique_row_indices = Vec::new();

    // Process each row to extract its REE data (following decode_binary pattern)
    let mut decoded_data = Vec::new();
    for (idx, row) in rows.iter_mut().enumerate() {
        decoded_data.clear();
        // Extract the decoded value data from this row
        let consumed = variable::decode_blocks(row, field.options, |block| {
            decoded_data.extend_from_slice(block);
        });

        // Handle bit inversion for descending sort (following decode_binary pattern)
        if field.options.descending {
            decoded_data.iter_mut().for_each(|b| *b = !*b);
        }

        // Update the row to point past the consumed REE data
        *row = &row[consumed..];

        // Check if this decoded value is the same as the previous one to identify runs
        let is_new_run =
            idx == 0 || decoded_data != decoded_values[*unique_row_indices.last().unwrap()];

        if is_new_run {
            // This is a new unique value - end the previous run if any
            if idx > 0 {
                run_ends.push(R::Native::usize_as(idx));
            }
            unique_row_indices.push(decoded_values.len());
            let capacity = decoded_data.capacity();
            decoded_values.push(std::mem::replace(
                &mut decoded_data,
                Vec::with_capacity(capacity),
            ));
        }
    }
    // Add the final run end
    run_ends.push(R::Native::usize_as(rows.len()));

    // Convert the unique decoded values using the row converter
    let mut unique_rows: Vec<&[u8]> = decoded_values.iter().map(|v| v.as_slice()).collect();
    let values = if unique_rows.is_empty() {
        unsafe { converter.convert_raw(&mut [], validate_utf8) }?
    } else {
        unsafe { converter.convert_raw(&mut unique_rows, validate_utf8) }?
    };

    // Create run ends array
    let run_ends_array = PrimitiveArray::<R>::try_new(ScalarBuffer::from(run_ends), None)?;

    // Create the RunEndEncodedArray
    RunArray::<R>::try_new(&run_ends_array, &values[0])
}

#[cfg(test)]
mod tests {
    use crate::{RowConverter, SortField};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
    use arrow_array::{Array, Int64Array, PrimitiveArray, RunArray, StringArray};
    use arrow_schema::{DataType, SortOptions};
    use std::sync::Arc;

    fn assert_roundtrip<R: RunEndIndexType>(
        array: &RunArray<R>,
        run_end_type: DataType,
        values_type: DataType,
        sort_options: Option<SortOptions>,
    ) {
        let sort_field = if let Some(options) = sort_options {
            SortField::new_with_options(
                DataType::RunEndEncoded(
                    Arc::new(arrow_schema::Field::new("run_ends", run_end_type, false)),
                    Arc::new(arrow_schema::Field::new("values", values_type, true)),
                ),
                options,
            )
        } else {
            SortField::new(DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", run_end_type, false)),
                Arc::new(arrow_schema::Field::new("values", values_type, true)),
            ))
        };

        let converter = RowConverter::new(vec![sort_field]).unwrap();

        let rows = converter
            .convert_columns(&[Arc::new(array.clone())])
            .unwrap();

        let arrays = converter.convert_rows(&rows).unwrap();
        let result = arrays[0].as_any().downcast_ref::<RunArray<R>>().unwrap();

        assert_eq!(array, result);
    }

    #[test]
    fn test_run_end_encoded_supports_datatype() {
        // Test that the RowConverter correctly supports run-end encoded arrays
        assert!(RowConverter::supports_datatype(&DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
        )));
    }

    #[test]
    fn test_run_end_encoded_round_trip_int16_int64s() {
        // Test round-trip correctness for RunEndEncodedArray with Int64 values making sure it
        // doesn't just work with eg. strings (which are all the other tests).

        let values = Int64Array::from(vec![100, 200, 100, 300]);
        let run_ends = vec![2, 3, 5, 6];
        let array: RunArray<Int16Type> =
            RunArray::try_new(&PrimitiveArray::from(run_ends), &values).unwrap();

        assert_roundtrip(&array, DataType::Int16, DataType::Int64, None);
    }

    #[test]
    fn test_run_end_encoded_round_trip_int32_int64s() {
        // Test round-trip correctness for RunEndEncodedArray with Int64 values making sure it
        // doesn't just work with eg. strings (which are all the other tests).

        let values = Int64Array::from(vec![100, 200, 100, 300]);
        let run_ends = vec![2, 3, 5, 6];
        let array: RunArray<Int32Type> =
            RunArray::try_new(&PrimitiveArray::from(run_ends), &values).unwrap();

        assert_roundtrip(&array, DataType::Int32, DataType::Int64, None);
    }

    #[test]
    fn test_run_end_encoded_round_trip_int64_int64s() {
        // Test round-trip correctness for RunEndEncodedArray with Int64 values making sure it
        // doesn't just work with eg. strings (which are all the other tests).

        let values = Int64Array::from(vec![100, 200, 100, 300]);
        let run_ends = vec![2, 3, 5, 6];
        let array: RunArray<Int64Type> =
            RunArray::try_new(&PrimitiveArray::from(run_ends), &values).unwrap();

        assert_roundtrip(&array, DataType::Int64, DataType::Int64, None);
    }

    #[test]
    fn test_run_end_encoded_round_trip_strings() {
        // Test round-trip correctness for RunEndEncodedArray with strings

        let array: RunArray<Int32Type> = vec!["b", "b", "a"].into_iter().collect();

        assert_roundtrip(&array, DataType::Int32, DataType::Utf8, None);
    }

    #[test]
    fn test_run_end_encoded_round_trip_strings_with_nulls() {
        // Test round-trip correctness for RunEndEncodedArray with nulls

        let array: RunArray<Int32Type> = vec![Some("b"), Some("b"), None, Some("a")]
            .into_iter()
            .collect();

        assert_roundtrip(&array, DataType::Int32, DataType::Utf8, None);
    }

    #[test]
    fn test_run_end_encoded_ascending_descending_round_trip() {
        // Test round-trip correctness for ascending vs descending sort options

        let values_asc =
            arrow_array::StringArray::from(vec![Some("apple"), Some("banana"), Some("cherry")]);
        let run_ends_asc = vec![2, 4, 6];
        let run_array_asc: RunArray<Int32Type> = RunArray::try_new(
            &arrow_array::PrimitiveArray::from(run_ends_asc),
            &values_asc,
        )
        .unwrap();

        // Test ascending order
        assert_roundtrip(
            &run_array_asc,
            DataType::Int32,
            DataType::Utf8,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        );

        // Test descending order
        assert_roundtrip(
            &run_array_asc,
            DataType::Int32,
            DataType::Utf8,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        );
    }

    #[test]
    fn test_run_end_encoded_sort_configurations_basic() {
        // Test that different sort configurations work and can round-trip successfully

        let test_array: RunArray<Int32Type> = vec!["test"].into_iter().collect();

        // Test ascending order
        assert_roundtrip(
            &test_array,
            DataType::Int32,
            DataType::Utf8,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        );

        // Test descending order
        assert_roundtrip(
            &test_array,
            DataType::Int32,
            DataType::Utf8,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        );
    }

    #[test]
    fn test_run_end_encoded_nulls_first_last_configurations() {
        // Test that nulls_first vs nulls_last configurations work

        let simple_array: RunArray<Int32Type> = vec!["simple"].into_iter().collect();

        let converter_nulls_first = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )])
        .unwrap();

        let converter_nulls_last = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();

        // Test that both configurations can handle simple arrays
        let rows_nulls_first = converter_nulls_first
            .convert_columns(&[Arc::new(simple_array.clone())])
            .unwrap();
        let arrays_nulls_first = converter_nulls_first
            .convert_rows(&rows_nulls_first)
            .unwrap();
        let result_nulls_first = arrays_nulls_first[0]
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        let rows_nulls_last = converter_nulls_last
            .convert_columns(&[Arc::new(simple_array.clone())])
            .unwrap();
        let arrays_nulls_last = converter_nulls_last.convert_rows(&rows_nulls_last).unwrap();
        let result_nulls_last = arrays_nulls_last[0]
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        // Both should successfully convert the simple array
        assert_eq!(simple_array.len(), result_nulls_first.len());
        assert_eq!(simple_array.len(), result_nulls_last.len());
    }

    #[test]
    fn test_run_end_encoded_row_consumption() {
        // This test verifies that ALL rows are properly consumed during decoding,
        // not just the unique values. We test this by ensuring multi-column conversion
        // works correctly - if rows aren't consumed properly, the second column would fail.

        // Create a REE array with multiple runs
        let array: RunArray<Int32Type> = vec!["a", "a", "b", "b", "b", "c"].into_iter().collect();
        let string_array = StringArray::from(vec!["x", "y", "z", "w", "u", "v"]);

        let multi_converter = RowConverter::new(vec![
            SortField::new(DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            )),
            SortField::new(DataType::Utf8),
        ])
        .unwrap();

        let multi_rows = multi_converter
            .convert_columns(&[Arc::new(array.clone()), Arc::new(string_array.clone())])
            .unwrap();

        // Convert back - this will test that all rows are consumed properly
        let arrays = multi_converter.convert_rows(&multi_rows).unwrap();

        // Verify both columns round-trip correctly
        let result_ree = arrays[0]
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();

        let result_string = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        // This should pass - both arrays should be identical to originals
        assert_eq!(result_ree.values().as_ref(), array.values().as_ref());
        assert_eq!(result_ree.run_ends().values(), array.run_ends().values());
        assert_eq!(*result_string, string_array);
    }

    #[test]
    fn test_run_end_encoded_sorting_behavior() {
        // Test that the binary row encoding actually produces the correct sort order

        // Create REE arrays with different values to test sorting
        let array1: RunArray<Int32Type> = vec!["apple", "apple"].into_iter().collect();
        let array2: RunArray<Int32Type> = vec!["banana", "banana"].into_iter().collect();
        let array3: RunArray<Int32Type> = vec!["cherry", "cherry"].into_iter().collect();

        // Test ascending sort
        let converter_asc = RowConverter::new(vec![SortField::new(DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
        ))])
        .unwrap();

        let rows1_asc = converter_asc
            .convert_columns(&[Arc::new(array1.clone())])
            .unwrap();
        let rows2_asc = converter_asc
            .convert_columns(&[Arc::new(array2.clone())])
            .unwrap();
        let rows3_asc = converter_asc
            .convert_columns(&[Arc::new(array3.clone())])
            .unwrap();

        // For ascending: apple < banana < cherry
        // So row bytes should sort: rows1 < rows2 < rows3
        assert!(
            rows1_asc.row(0) < rows2_asc.row(0),
            "apple should come before banana in ascending order"
        );
        assert!(
            rows2_asc.row(0) < rows3_asc.row(0),
            "banana should come before cherry in ascending order"
        );
        assert!(
            rows1_asc.row(0) < rows3_asc.row(0),
            "apple should come before cherry in ascending order"
        );

        // Test descending sort
        let converter_desc = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            arrow_schema::SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let rows1_desc = converter_desc
            .convert_columns(&[Arc::new(array1.clone())])
            .unwrap();
        let rows2_desc = converter_desc
            .convert_columns(&[Arc::new(array2.clone())])
            .unwrap();
        let rows3_desc = converter_desc
            .convert_columns(&[Arc::new(array3.clone())])
            .unwrap();

        // For descending: cherry > banana > apple
        // So row bytes should sort: rows3 < rows2 < rows1 (because byte comparison is ascending)
        assert!(
            rows3_desc.row(0) < rows2_desc.row(0),
            "cherry should come before banana in descending order (byte-wise)"
        );
        assert!(
            rows2_desc.row(0) < rows1_desc.row(0),
            "banana should come before apple in descending order (byte-wise)"
        );
        assert!(
            rows3_desc.row(0) < rows1_desc.row(0),
            "cherry should come before apple in descending order (byte-wise)"
        );
    }

    #[test]
    fn test_run_end_encoded_null_sorting() {
        // Test null handling in sort order

        let array_with_nulls: RunArray<Int32Type> = vec![None, None].into_iter().collect();
        let array_with_values: RunArray<Int32Type> = vec!["apple", "apple"].into_iter().collect();

        // Test nulls_first = true
        let converter_nulls_first = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: true,
            },
        )])
        .unwrap();

        let rows_nulls = converter_nulls_first
            .convert_columns(&[Arc::new(array_with_nulls.clone())])
            .unwrap();
        let rows_values = converter_nulls_first
            .convert_columns(&[Arc::new(array_with_values.clone())])
            .unwrap();

        // nulls should come before values when nulls_first = true
        assert!(
            rows_nulls.row(0) < rows_values.row(0),
            "nulls should come before values when nulls_first=true"
        );

        // Test nulls_first = false
        let converter_nulls_last = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();

        let rows_nulls_last = converter_nulls_last
            .convert_columns(&[Arc::new(array_with_nulls.clone())])
            .unwrap();
        let rows_values_last = converter_nulls_last
            .convert_columns(&[Arc::new(array_with_values.clone())])
            .unwrap();

        // values should come before nulls when nulls_first = false
        assert!(
            rows_values_last.row(0) < rows_nulls_last.row(0),
            "values should come before nulls when nulls_first=false"
        );
    }

    #[test]
    fn test_run_end_encoded_mixed_sorting() {
        // Test sorting with mixed values and nulls to ensure complex scenarios work

        let array1: RunArray<Int32Type> = vec![Some("apple"), None].into_iter().collect();
        let array2: RunArray<Int32Type> = vec![None, Some("banana")].into_iter().collect();
        let array3: RunArray<Int32Type> =
            vec![Some("cherry"), Some("cherry")].into_iter().collect();

        let converter = RowConverter::new(vec![SortField::new_with_options(
            DataType::RunEndEncoded(
                Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
            ),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: true,
            },
        )])
        .unwrap();

        let rows1 = converter.convert_columns(&[Arc::new(array1)]).unwrap();
        let rows2 = converter.convert_columns(&[Arc::new(array2)]).unwrap();
        let rows3 = converter.convert_columns(&[Arc::new(array3)]).unwrap();

        // With nulls_first=true, ascending:
        // Row 0: array1[0]="apple", array2[0]=null, array3[0]="cherry" -> null < apple < cherry
        // Row 1: array1[1]=null, array2[1]="banana", array3[1]="cherry" -> null < banana < cherry

        // Compare first rows: null < apple < cherry
        assert!(rows2.row(0) < rows1.row(0), "null should come before apple");
        assert!(
            rows1.row(0) < rows3.row(0),
            "apple should come before cherry"
        );

        // Compare second rows: null < banana < cherry
        assert!(
            rows1.row(1) < rows2.row(1),
            "null should come before banana"
        );
        assert!(
            rows2.row(1) < rows3.row(1),
            "banana should come before cherry"
        );
    }

    #[test]
    fn test_run_end_encoded_empty() {
        // Test converting / decoding an empty RunEndEncodedArray
        let values: Vec<&str> = vec![];
        let array: RunArray<Int32Type> = values.into_iter().collect();

        let converter = RowConverter::new(vec![SortField::new(DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
        ))])
        .unwrap();

        let rows = converter.convert_columns(&[Arc::new(array)]).unwrap();
        assert_eq!(rows.num_rows(), 0);

        // Likewise converting empty rows should yield an empty RunEndEncodedArray
        let arrays = converter.convert_rows(&rows).unwrap();
        assert_eq!(arrays.len(), 1);
        // Verify both columns round-trip correctly
        let result_ree = arrays[0].as_run::<Int32Type>();
        assert_eq!(result_ree.len(), 0);
    }
}

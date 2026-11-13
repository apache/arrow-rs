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

//! Slice-aware logical value sizes shared by page and row group sizing.
//!
//! These measure values, not backing allocations. Row group sizing calibrates
//! them against the writer's encoded-size estimate; they do not predict encoding
//! or compression by themselves.

use std::ops::Range;

use crate::arrow::arrow_writer::fixed_byte_width;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, OffsetSizeTrait, RecordBatch, downcast_dictionary_array, downcast_run_array,
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

/// Plain BYTE_ARRAY bytes size, including a four-byte length prefix per value.
pub fn byte_array_bytes_size(payload_size: usize, values_count: usize) -> usize {
    payload_size.saturating_add(values_count.saturating_mul(size_of::<u32>()))
}

/// Payload span of offset-buffer values, without inspecting their data buffer.
pub fn offset_bytes<O: OffsetSizeTrait>(offsets: &[O], range: Range<usize>) -> usize {
    (offsets[range.end] - offsets[range.start]).as_usize()
}

/// Plain BYTE_ARRAY bytes of views size, without dereferencing their payloads.
pub fn byte_array_view_bytes_size(views: &[u128]) -> usize {
    views.iter().fold(0usize, |total, view| {
        total.saturating_add(byte_array_bytes_size((*view as u32) as usize, 1))
    })
}

pub fn batch_bytes_size(batch: &RecordBatch, rows: usize) -> usize {
    batch.columns().iter().fold(0usize, |total, array| {
        total.saturating_add(array_bytes_size(array.as_ref(), 0..rows))
    })
}

/// Calibrate logical input bytes against the encoded size observed so far.
/// Without a sample, start with a ratio of one.
pub fn input_byte_budget(limit: usize, encoded_bytes: usize, input_bytes: usize) -> usize {
    let remaining = limit.saturating_sub(encoded_bytes);
    if input_bytes == 0 || encoded_bytes == 0 {
        return remaining;
    }
    // The intermediate product can overflow usize even when the quotient fits,
    // particularly on 32-bit targets. Clamp only after division.
    ((remaining as u128 * input_bytes as u128) / encoded_bytes as u128).min(usize::MAX as u128)
        as usize
}

/// Find a prefix at a common row boundary across all columns. Flat arrays can
/// often size a whole prefix from metadata. Otherwise, grow the search window
/// exponentially, so repeatedly splitting a large batch in small chunks does
/// not scan all its remaining values at every split.
pub fn number_of_first_rows_within_budget(
    batch: &RecordBatch,
    rows: usize,
    budget: usize,
) -> usize {
    if rows == 0 {
        return 0;
    }
    let mut low = 0;
    let mut high = 1;
    while batch_bytes_size(batch, high) <= budget {
        if high == rows {
            return rows;
        }
        low = high;
        high = high.saturating_mul(2).min(rows);
    }
    while low + 1 < high {
        let mid = low + (high - low) / 2;
        if batch_bytes_size(batch, mid) <= budget {
            low = mid;
        } else {
            high = mid;
        }
    }
    low
}

fn array_bytes_size(array: &dyn Array, range: Range<usize>) -> usize {
    if range.is_empty() {
        return 0;
    }
    if matches!(array.data_type(), DataType::Null) {
        return range.len();
    }
    // A small allowance for definition levels also gives all-null input a
    // nonzero calibration size. Null payloads (including nested children) are
    // excluded, even if their buffers contain values.
    let levels = if array.nulls().is_some() {
        range.len()
    } else {
        0
    };
    if array.null_count() == 0 {
        return levels.saturating_add(contiguous_valid_array_bytes_size(array, range));
    }
    let mut total = levels;
    let mut start = range.start;
    while start < range.end {
        if array.is_null(start) {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < range.end && array.is_valid(end) {
            end += 1;
        }
        total = total.saturating_add(contiguous_valid_array_bytes_size(array, start..end));
        start = end;
    }
    total
}

fn contiguous_valid_array_bytes_size(array: &dyn Array, range: Range<usize>) -> usize {
    let len = range.len();
    match array.data_type() {
        DataType::Null => len,
        // Rounding per range can bias calibration when batch sizes change.
        DataType::Boolean => len.div_ceil(8),
        DataType::Utf8 => byte_array_bytes_size(
            offset_bytes(array.as_string::<i32>().value_offsets(), range),
            len,
        ),
        DataType::LargeUtf8 => byte_array_bytes_size(
            offset_bytes(array.as_string::<i64>().value_offsets(), range),
            len,
        ),
        DataType::Binary => byte_array_bytes_size(
            offset_bytes(array.as_binary::<i32>().value_offsets(), range),
            len,
        ),
        DataType::LargeBinary => byte_array_bytes_size(
            offset_bytes(array.as_binary::<i64>().value_offsets(), range),
            len,
        ),
        DataType::Utf8View => byte_array_view_bytes_size(&array.as_string_view().views()[range]),
        DataType::BinaryView => byte_array_view_bytes_size(&array.as_binary_view().views()[range]),
        DataType::FixedSizeBinary(width) => len.saturating_mul(*width as usize),
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .fold(0usize, |total, child| {
                total.saturating_add(array_bytes_size(child.as_ref(), range.clone()))
            }),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            len.saturating_add(array_bytes_size(
                list.values().as_ref(),
                offsets[range.start].as_usize()..offsets[range.end].as_usize(),
            ))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            len.saturating_add(array_bytes_size(
                list.values().as_ref(),
                offsets[range.start].as_usize()..offsets[range.end].as_usize(),
            ))
        }
        DataType::ListView(_) => {
            let list = array.as_list_view::<i32>();
            range.fold(len, |total, row| {
                let start = list.value_offset(row).as_usize();
                let end = start + list.value_size(row).as_usize();
                total.saturating_add(array_bytes_size(list.values().as_ref(), start..end))
            })
        }
        DataType::LargeListView(_) => {
            let list = array.as_list_view::<i64>();
            range.fold(len, |total, row| {
                let start = list.value_offset(row).as_usize();
                let end = start + list.value_size(row).as_usize();
                total.saturating_add(array_bytes_size(list.values().as_ref(), start..end))
            })
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list();
            let width = list.value_length() as usize;
            len.saturating_add(array_bytes_size(
                list.values().as_ref(),
                range.start * width..range.end * width,
            ))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            len.saturating_add(array_bytes_size(
                map.entries(),
                offsets[range.start].as_usize()..offsets[range.end].as_usize(),
            ))
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array!(array => {
            // Count referenced values with multiplicity, not the dictionary's
            // allocation or the width of its Arrow keys. Parquet may choose a
            // different dictionary or fall back to plain encoding.
            range.fold(0usize, |total, row| {
                let key = array.key(row).unwrap();
                total.saturating_add(array_bytes_size(array.values().as_ref(), key..key + 1))
            })
        }, _ => unreachable!()),
        DataType::RunEndEncoded(_, _) => downcast_run_array!(array => {
            range.fold(0usize, |total, row| {
                let index = array.run_ends().get_physical_index(row);
                total.saturating_add(array_bytes_size(array.values().as_ref(), index..index + 1))
            })
        }, _ => unreachable!()),
        // Match the physical value widths where Arrow and Parquet differ.
        DataType::Int8
        | DataType::Int16
        | DataType::UInt8
        | DataType::UInt16
        | DataType::Date64 => len.saturating_mul(4),
        DataType::Interval(_) => len.saturating_mul(12),
        data_type => len.saturating_mul(fixed_byte_width(data_type).unwrap_or(1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::types::{Int8Type, Int32Type};
    use arrow_array::{
        ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array, LargeStringArray,
        ListArray, NullArray, RunArray, StringArray, StringViewArray, StructArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;

    #[test]
    fn sliced_strings_and_views() {
        let large = "x".repeat(100_000);
        for array in [
            Arc::new(StringArray::from(vec![
                large.as_str(),
                "a",
                "bb",
                large.as_str(),
            ])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                large.as_str(),
                "a",
                "bb",
                large.as_str(),
            ])),
            Arc::new(StringViewArray::from(vec![
                large.as_str(),
                "a",
                "bb",
                large.as_str(),
            ])),
        ] {
            assert_eq!(array_bytes_size(array.as_ref(), 1..3), 11);
            assert_eq!(array_bytes_size(array.slice(1, 2).as_ref(), 0..2), 11);
        }
    }

    #[test]
    fn null_payloads_and_nested_slices() {
        // A null list slot may retain arbitrarily large child payloads.
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "x".repeat(100_000),
            "a".into(),
            "bb".into(),
        ]));
        let field = Arc::new(Field::new("item", DataType::Utf8, false));
        let list = ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            values,
            Some(NullBuffer::from(vec![false, true, true])),
        );
        assert_eq!(array_bytes_size(&list, 0..3), 16); // 3 null allowances + 2 list allowances + 11 value bytes
        assert_eq!(array_bytes_size(&list.slice(1, 2), 0..2), 15);

        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let list = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            2,
            values,
            None,
        );
        assert_eq!(array_bytes_size(&list.slice(1, 2), 0..2), 18);

        let values: ArrayRef = Arc::new(StringArray::from(vec!["hidden payload", "a"]));
        let structure = StructArray::new(
            vec![field].into(),
            vec![values],
            Some(NullBuffer::from(vec![false, true])),
        );
        assert_eq!(array_bytes_size(&structure, 0..2), 7);
        assert_eq!(array_bytes_size(&NullArray::new(5), 0..5), 5);
    }

    #[test]
    fn dictionary_and_run_values_have_logical_sizes() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("unused large value"),
            Some("abc"),
            None,
        ]));
        let dict = DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![Some(1), Some(1), None, Some(2)]),
            values,
        );
        assert_eq!(array_bytes_size(&dict, 0..4), 21); // 4 key null allowances + 8 + 8 + 1
        assert_eq!(array_bytes_size(&dict.slice(1, 3), 0..3), 12);

        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![3, 5]),
            &StringArray::from(vec!["a", "bb"]),
        )
        .unwrap();
        assert_eq!(array_bytes_size(&runs.slice(2, 3), 0..3), 17);
    }

    #[test]
    fn budget_sums_columns_and_observes_outliers() {
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(Int32Array::from(vec![0; 4])) as ArrayRef),
            (
                "b",
                Arc::new(StringArray::from(vec!["a", "bb", &"x".repeat(1000), "c"])),
            ),
        ])
        .unwrap();
        assert_eq!(number_of_first_rows_within_budget(&batch, 4, 19), 2);
        assert_eq!(number_of_first_rows_within_budget(&batch, 4, 18), 1);
        assert_eq!(number_of_first_rows_within_budget(&batch, 4, 0), 0);
        assert_eq!(number_of_first_rows_within_budget(&batch, 4, usize::MAX), 4);
    }

    #[test]
    fn calibrated_budget_avoids_overflow() {
        // This product overflows usize on both 32-bit and 64-bit targets,
        // while its quotient fits without saturation.
        let half = usize::MAX / 2;
        assert_eq!(input_byte_budget(usize::MAX, half + 1, half), half - 1);
        assert_eq!(input_byte_budget(3500, 100, 1_000_000), 34_000_000);
        assert_eq!(input_byte_budget(usize::MAX, 1, usize::MAX), usize::MAX);
        assert_eq!(input_byte_budget(3500, 100, 0), 3400);
        assert_eq!(input_byte_budget(3500, 0, 1_000_000), 3500);
        assert_eq!(input_byte_budget(3500, 4000, 1_000_000), 0);
    }
}

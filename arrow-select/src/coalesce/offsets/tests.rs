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

use super::OffsetCapacity;
use crate::coalesce::BatchCoalescer;
use crate::concat::concat_batches;
use crate::filter::filter_record_batch;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, MapArray, NullArray, RecordBatch,
    StringArray, StructArray, cast::AsArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

fn with_limit(batch: &RecordBatch, rows: usize, limit: usize) -> BatchCoalescer {
    let mut coalescer = BatchCoalescer::new(batch.schema(), rows);
    coalescer.offset_capacity = OffsetCapacity::new(batch.schema_ref()).with_limit(limit);
    coalescer
}

fn finish(mut coalescer: BatchCoalescer) -> Vec<RecordBatch> {
    coalescer.finish_buffered_batch().unwrap();
    std::iter::from_fn(|| coalescer.next_completed_batch()).collect()
}

#[test]
fn coalescing_flushes_before_actual_list_offset_overflow() {
    // Null children carry only a length, so this crosses the real i32 boundary
    // without allocating billions of values or replacing the production limit.
    let child_count = 1_usize << 30;
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Null, true)),
        OffsetBuffer::from_lengths([child_count]),
        Arc::new(NullArray::new(child_count)),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![("items", Arc::new(list) as ArrayRef)]).unwrap();
    let mut coalescer = BatchCoalescer::new(batch.schema(), 8192);
    coalescer.push_batch(batch.clone()).unwrap();
    coalescer.push_batch(batch.clone()).unwrap();

    let output = finish(coalescer);
    assert_eq!(output.len(), 2);
    for actual in output {
        assert_eq!(actual.schema(), batch.schema());
        assert_eq!(actual.num_rows(), 1);
        let items = actual.column(0).as_list::<i32>();
        assert_eq!(items.value_offsets(), &[0, child_count as i32]);
        assert_eq!(items.values().len(), child_count);
        assert_eq!(items.values().data_type(), &DataType::Null);
    }
}

#[test]
fn byte_limit_preserves_slices_nulls_and_column_alignment() {
    let source = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec![
                Some("unused prefix"),
                Some("abc"),
                None,
                Some("def"),
                Some("unused suffix"),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let first = source.slice(1, 3);
    let second = RecordBatch::try_new(
        first.schema(),
        vec![
            Arc::new(Int32Array::from(vec![5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ghi", "jk"])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut coalescer = with_limit(&first, 4, 6);
    coalescer.push_batch(first.clone()).unwrap();
    coalescer.push_batch(second.clone()).unwrap();

    assert_eq!(finish(coalescer), vec![first, second]);
}

#[test]
fn nested_map_string_offsets_flush_all_columns_together() {
    let parts = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::from_lengths([2, 0]),
        Arc::new(StringArray::from(vec!["abc", "def"])),
        None,
    );
    let values = StructArray::from(vec![(
        Arc::new(Field::new("parts", parts.data_type().clone(), true)),
        Arc::new(parts) as ArrayRef,
    )]);
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["m", "n"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", values.data_type().clone(), true)),
            Arc::new(values) as ArrayRef,
        ),
    ]);
    let maps = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths([1, 0, 1]),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("messages", Arc::new(maps) as ArrayRef),
    ])
    .unwrap();
    let mut coalescer = with_limit(&batch, 128, 10);
    coalescer.push_batch(batch.clone()).unwrap();
    coalescer.push_batch(batch.clone()).unwrap();

    assert_eq!(finish(coalescer), vec![batch.clone(), batch]);
}

#[test]
fn filtered_batches_count_only_selected_payloads() {
    let text = StringArray::from_iter((0..256).map(|index| {
        if index == 0 || index == 128 {
            Some("abc")
        } else {
            Some("unused payload")
        }
    }));
    let batch = RecordBatch::try_from_iter(vec![("text", Arc::new(text) as ArrayRef)]).unwrap();
    let filter = BooleanArray::from_iter((0..256).map(|index| Some(index == 0 || index == 128)));
    let selected = filter_record_batch(&batch, &filter).unwrap();
    let mut coalescer = with_limit(&batch, 128, 12);
    for _ in 0..3 {
        coalescer
            .push_batch_with_filter(batch.clone(), &filter)
            .unwrap();
    }

    let expected_first =
        concat_batches(&batch.schema(), &[selected.clone(), selected.clone()]).unwrap();
    assert_eq!(finish(coalescer), vec![expected_first, selected]);
}

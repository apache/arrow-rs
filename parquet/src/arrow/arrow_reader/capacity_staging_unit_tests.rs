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

use super::*;
use arrow_array::{
    BinaryArray, Int32Array, LargeBinaryArray, LargeStringArray, StringArray, StructArray,
};
use arrow_schema::{Field, Schema};
use std::collections::HashMap;

fn window(schema: &SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> RecordBatch {
    let fields = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, array)| {
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone())
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

#[test]
fn fitting_slice_shares_payload_and_preserves_schema_and_nulls() {
    let metadata = HashMap::from([("owner".into(), "example".into())]);
    let native: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("child", DataType::Utf8, false)),
        Arc::new(StringArray::from(vec!["nested-payload"; 3])) as ArrayRef,
    )]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("text", DataType::Utf8, true).with_metadata(metadata.clone()),
            Field::new("native", DataType::Int32, false),
            Field::new("nested", nested.data_type().clone(), false),
        ],
        metadata,
    ));
    let source = LargeStringArray::from(vec![
        Some("prefix"),
        None,
        Some("ab"),
        Some("c"),
        Some("tail"),
    ])
    .slice(1, 3);
    let expected_pointer = source.values()[source.value_offsets()[0] as usize..].as_ptr();
    let mut splitter = CapacitySplitter::new(schema.clone(), 3);
    splitter
        .push(window(
            &schema,
            vec![Arc::new(source), native.clone(), nested.clone()],
            3,
        ))
        .unwrap();
    let output = splitter.next_batch().unwrap().unwrap();
    let strings = output.column(0).as_string::<i32>();
    assert_eq!(output.schema(), schema);
    assert!(Arc::ptr_eq(output.column(1), &native));
    assert!(Arc::ptr_eq(output.column(2), &nested));
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![None, Some("ab"), Some("c")]
    );
    assert_eq!(strings.value_offsets(), &[0, 0, 2, 3]);
    assert_eq!(strings.values().as_ptr(), expected_pointer);
    assert!(splitter.next_batch().unwrap().is_none());
}

#[test]
fn split_limits_each_column_and_copies_only_staged_payloads() {
    let text: ArrayRef = Arc::new(LargeStringArray::from(vec!["aa", "b", "cc", "d"]));
    let binary: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        b"x".as_slice(),
        b"y",
        b"zz",
        b"ww",
    ]));
    let large: ArrayRef = Arc::new(LargeStringArray::from(vec!["already-large"; 4]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("child", DataType::Utf8, false)),
        Arc::new(StringArray::from(vec!["nested-payload"; 4])) as ArrayRef,
    )]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("binary", DataType::Binary, false),
        Field::new("large", DataType::LargeUtf8, false),
        Field::new("nested", nested.data_type().clone(), false),
    ]));
    let source_pointer = text.as_string::<i64>().values().as_ptr();
    let nested_pointer = nested
        .as_struct()
        .column(0)
        .as_string::<i32>()
        .values()
        .as_ptr();
    let weak = Arc::downgrade(&text);
    let mut splitter = CapacitySplitter::new(schema.clone(), 3);
    splitter
        .push(window(&schema, vec![text, binary, large, nested], 4))
        .unwrap();
    let mut output = vec![];
    while let Some(batch) = splitter.next_batch().unwrap() {
        assert_eq!(batch.schema(), schema);
        let text = batch.column(0).as_string::<i32>();
        let binary = batch.column(1).as_binary::<i32>();
        assert!(text.values().len() <= 3);
        assert!(binary.values().len() <= 3);
        assert_ne!(text.values().as_ptr(), source_pointer);
        assert_eq!(batch.column(2).as_string::<i64>().value(0), "already-large");
        let child = batch.column(3).as_struct().column(0);
        assert_eq!(child.as_string::<i32>().values().as_ptr(), nested_pointer);
        output.push(batch);
    }
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        vec![2, 1, 1]
    );
    let combined = arrow_select::concat::concat_batches(&schema, &output).unwrap();
    assert_eq!(
        combined.column(0).as_string::<i32>(),
        &StringArray::from(vec!["aa", "b", "cc", "d"])
    );
    assert_eq!(
        combined.column(1).as_binary::<i32>(),
        &BinaryArray::from(vec![b"x".as_slice(), b"y", b"zz", b"ww"])
    );
    assert!(weak.upgrade().is_none());
}

#[test]
fn oversized_row_releases_pending_after_an_emitted_prefix() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let source: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "wide"]));
    let weak = Arc::downgrade(&source);
    let mut splitter = CapacitySplitter::new(schema.clone(), 2);
    splitter.push(window(&schema, vec![source], 2)).unwrap();
    assert_eq!(splitter.next_batch().unwrap().unwrap().num_rows(), 1);
    let error = splitter.next_batch().unwrap_err().to_string();
    assert!(error.contains("a single row needs 4 bytes of Utf8 data for column text"));
    assert!(error.contains("Large or View"));
    assert!(weak.upgrade().is_none());
    assert!(splitter.next_batch().unwrap().is_none());
    splitter
        .push(window(
            &schema,
            vec![Arc::new(LargeStringArray::from(vec!["ok"]))],
            1,
        ))
        .unwrap();
    assert_eq!(
        splitter
            .next_batch()
            .unwrap()
            .unwrap()
            .column(0)
            .as_string::<i32>()
            .value(0),
        "ok"
    );
}

#[test]
fn restoration_allows_mixed_flat_staging_but_keeps_nested_types_opaque() {
    let nested = DataType::Struct(vec![Field::new("child", DataType::Utf8, false)].into());
    let fields = vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, false),
        Field::new("nested", nested, false),
        Field::new("view", DataType::Utf8View, false),
    ];
    let logical = DataType::Struct(fields.clone().into());
    assert!(can_restore_type(&logical, &logical));
    let mut decoded = fields;
    decoded[0] = decoded[0].clone().with_data_type(DataType::LargeUtf8);
    assert!(can_restore_type(
        &DataType::Struct(decoded.clone().into()),
        &logical
    ));
    decoded[1] = decoded[1].clone().with_data_type(DataType::LargeBinary);
    assert!(can_restore_type(
        &DataType::Struct(decoded.clone().into()),
        &logical
    ));
    for invalid in [
        decoded[0].clone().with_name("other"),
        decoded[0].clone().with_nullable(false),
        decoded[0]
            .clone()
            .with_metadata(HashMap::from([("key".into(), "value".into())])),
    ] {
        let mut mismatched = decoded.clone();
        mismatched[0] = invalid;
        assert!(!can_restore_type(
            &DataType::Struct(mismatched.into()),
            &logical
        ));
    }
    decoded[2] = decoded[2].clone().with_data_type(DataType::Struct(
        vec![Field::new("child", DataType::LargeUtf8, false)].into(),
    ));
    assert!(!can_restore_type(
        &DataType::Struct(decoded.into()),
        &logical
    ));
}

#[test]
fn empty_and_unstaged_windows_preserve_row_counts() {
    let schema = Arc::new(Schema::empty());
    for rows in [0, 5] {
        let mut splitter = CapacitySplitter::new(schema.clone(), 0);
        splitter.push(window(&schema, vec![], rows)).unwrap();
        assert_eq!(splitter.next_batch().unwrap().unwrap().num_rows(), rows);
        assert!(splitter.next_batch().unwrap().is_none());
    }
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let mut splitter = CapacitySplitter::new(schema.clone(), 0);
    splitter
        .push(window(&schema, vec![array.clone()], 2))
        .unwrap();
    let output = splitter.next_batch().unwrap().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>()
            .values()
            .as_ptr(),
        array
            .as_primitive::<arrow_array::types::Int32Type>()
            .values()
            .as_ptr()
    );
}

#[test]
fn split_shares_majority_slices_and_copies_minority_per_column() {
    let text = LargeStringArray::new(
        OffsetBuffer::new(vec![0, 6, 10].into()),
        Buffer::from_vec(b"aaaaaabbbb".to_vec()),
        None,
    );
    let binary = LargeBinaryArray::new(
        OffsetBuffer::new(vec![0, 1, 7].into()),
        Buffer::from_vec(b"xyyyyyy".to_vec()),
        None,
    );
    assert_eq!(text.values().capacity(), 10);
    assert_eq!(binary.values().capacity(), 7);
    let text_allocation = text.values().data_ptr();
    let binary_allocation = binary.values().data_ptr();
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("binary", DataType::Binary, false),
    ]));
    let mut splitter = CapacitySplitter::new(schema.clone(), 6);
    splitter
        .push(window(&schema, vec![Arc::new(text), Arc::new(binary)], 2))
        .unwrap();

    // The first row dominates text but not binary; the second reverses that.
    for (text_shared, binary_shared, text, binary) in [
        (true, false, "aaaaaa", b"x".as_slice()),
        (false, true, "bbbb", b"yyyyyy".as_slice()),
    ] {
        let output = splitter.next_batch().unwrap().unwrap();
        let strings = output.column(0).as_string::<i32>();
        let bytes = output.column(1).as_binary::<i32>();
        assert_eq!(strings.value(0), text);
        assert_eq!(bytes.value(0), binary);
        assert_eq!(strings.values().data_ptr() == text_allocation, text_shared);
        assert_eq!(
            bytes.values().data_ptr() == binary_allocation,
            binary_shared
        );
    }
    assert!(splitter.next_batch().unwrap().is_none());
}

#[test]
fn split_shares_visible_majority_with_bounded_spare_capacity() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    // Six bytes are a visible majority even at exactly half of capacity 12.
    // Capacity 20 is allowed for a ten-byte window, but 21 must be copied.
    for (capacity, prefix, shared) in [(12, 0, true), (20, 0, true), (21, 0, false), (20, 8, true)]
    {
        let mut values = Vec::with_capacity(capacity);
        values.resize(prefix, b'p');
        values.extend_from_slice(b"aaaaaabbbb");
        assert_eq!(values.capacity(), capacity);
        let values = Buffer::from_vec(values);
        assert_eq!(values.capacity(), capacity);
        let allocation = values.data_ptr();
        let prefix = prefix as i64;
        let source = LargeStringArray::new(
            OffsetBuffer::new(vec![0, prefix, prefix + 6, prefix + 10].into()),
            values,
            None,
        )
        .slice(1, 2);
        assert_eq!(source.value_offsets(), &[prefix, prefix + 6, prefix + 10]);
        let mut splitter = CapacitySplitter::new(schema.clone(), 6);
        splitter
            .push(window(&schema, vec![Arc::new(source)], 2))
            .unwrap();

        let first = splitter.next_batch().unwrap().unwrap();
        let strings = first.column(0).as_string::<i32>();
        assert_eq!(strings.value(0), "aaaaaa");
        assert_eq!(strings.value_offsets(), &[0, 6]);
        assert_eq!(strings.values().data_ptr() == allocation, shared);

        let second = splitter.next_batch().unwrap().unwrap();
        let strings = second.column(0).as_string::<i32>();
        assert_eq!(strings.value(0), "bbbb");
        assert_ne!(strings.values().data_ptr(), allocation);
        assert!(splitter.next_batch().unwrap().is_none());
    }
}

#[test]
fn split_considers_capacity_behind_a_sliced_payload_buffer() {
    let values = Buffer::from_vec(vec![b'x'; 110]).slice_with_length(100, 10);
    assert_eq!(values.len(), 10);
    assert_eq!(values.capacity(), 110);
    let allocation = values.data_ptr();
    let text = LargeStringArray::new(OffsetBuffer::new(vec![0, 6, 10].into()), values, None);
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let mut splitter = CapacitySplitter::new(schema.clone(), 6);
    splitter
        .push(window(&schema, vec![Arc::new(text)], 2))
        .unwrap();
    for len in [6, 4] {
        let output = splitter.next_batch().unwrap().unwrap();
        let strings = output.column(0).as_string::<i32>();
        assert_eq!(strings.value(0).len(), len);
        assert_ne!(strings.values().data_ptr(), allocation);
    }
}

#[test]
fn split_empty_payloads_do_not_retain_unused_backing_storage() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("binary", DataType::Binary, false),
    ]));
    for values in [Buffer::default(), Buffer::from_vec(b"unused".to_vec())] {
        let text = LargeStringArray::from(vec!["aaaaaa", "bbbb"]);
        let bytes = LargeBinaryArray::new(OffsetBuffer::new(vec![0, 0, 0].into()), values, None);
        let mut splitter = CapacitySplitter::new(schema.clone(), 6);
        splitter
            .push(window(&schema, vec![Arc::new(text), Arc::new(bytes)], 2))
            .unwrap();
        for _ in 0..2 {
            let output = splitter.next_batch().unwrap().unwrap();
            let bytes = output.column(1).as_binary::<i32>();
            assert_eq!(bytes.value_offsets(), &[0, 0]);
            assert!(bytes.values().is_empty());
            assert_eq!(bytes.values().capacity(), 0);
        }
        assert!(splitter.next_batch().unwrap().is_none());
    }
}

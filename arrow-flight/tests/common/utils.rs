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

//! Common utilities for testing flight clients and servers

use std::sync::Arc;

use arrow_array::{
    types::Int32Type, ArrayRef, BinaryViewArray, DictionaryArray, Float64Array, RecordBatch,
    StringViewArray, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};

/// Make a primitive batch for testing
///
/// Example:
/// i: 0, 1, None, 3, 4
/// f: 5.0, 4.0, None, 2.0, 1.0
#[allow(dead_code)]
pub fn make_primitive_batch(num_rows: usize) -> RecordBatch {
    let i: UInt8Array = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
                None
            } else {
                Some(i.try_into().unwrap())
            }
        })
        .collect();

    let f: Float64Array = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
                None
            } else {
                Some((num_rows - i) as f64)
            }
        })
        .collect();

    RecordBatch::try_from_iter(vec![("i", Arc::new(i) as ArrayRef), ("f", Arc::new(f))]).unwrap()
}

/// Make a dictionary batch for testing
///
/// Example:
/// a: value0, value1, value2, None, value1, value2
#[allow(dead_code)]
pub fn make_dictionary_batch(num_rows: usize) -> RecordBatch {
    let values: Vec<_> = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
                None
            } else {
                // repeat some values for low cardinality
                let v = i / 3;
                Some(format!("value{v}"))
            }
        })
        .collect();

    let a: DictionaryArray<Int32Type> = values
        .iter()
        .map(|s| s.as_ref().map(|s| s.as_str()))
        .collect();

    RecordBatch::try_from_iter(vec![("a", Arc::new(a) as ArrayRef)]).unwrap()
}

#[allow(dead_code)]
pub fn make_view_batches(num_rows: usize) -> RecordBatch {
    const LONG_TEST_STRING: &str =
        "This is a long string to make sure binary view array handles it";
    let schema = Schema::new(vec![
        Field::new("field1", DataType::BinaryView, true),
        Field::new("field2", DataType::Utf8View, true),
    ]);

    let string_view_values: Vec<Option<&str>> = (0..num_rows)
        .map(|i| match i % 3 {
            0 => None,
            1 => Some("foo"),
            2 => Some(LONG_TEST_STRING),
            _ => unreachable!(),
        })
        .collect();

    let bin_view_values: Vec<Option<&[u8]>> = (0..num_rows)
        .map(|i| match i % 3 {
            0 => None,
            1 => Some("bar".as_bytes()),
            2 => Some(LONG_TEST_STRING.as_bytes()),
            _ => unreachable!(),
        })
        .collect();

    let binary_array = BinaryViewArray::from_iter(bin_view_values);
    let utf8_array = StringViewArray::from_iter(string_view_values);
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(binary_array), Arc::new(utf8_array)],
    )
    .unwrap()
}

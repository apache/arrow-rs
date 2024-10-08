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

//! This example demonstrates using Vec<ArrayRef> as an alternative to ChunkedArray.
use arrow::array::{ArrayRef, AsArray, Float32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_array::cast::as_string_array;
use arrow_array::types::Float32Type;
use std::sync::Arc;

fn main() {
    let batches = [
        RecordBatch::try_from_iter(vec![
            (
                "label",
                Arc::new(StringArray::from(vec!["A", "B", "C"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            (
                "label",
                Arc::new(StringArray::from(vec!["D", "E"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Float32Array::from(vec![0.4, 0.5])) as ArrayRef,
            ),
        ])
        .unwrap(),
    ];

    // chunked_array_by_index is an array of two Vec<ArrayRef> where each Vec<ArrayRef> is a column
    let mut chunked_array_by_index = [Vec::new(), Vec::new()];
    for batch in &batches {
        for (i, array) in batch.columns().iter().enumerate() {
            chunked_array_by_index[i].push(array.clone());
        }
    }

    // downcast and iterate over the values - column 0 is the labels and column 1 is the values
    let labels: Vec<&str> = chunked_array_by_index[0]
        .iter()
        .flat_map(|x| as_string_array(x).iter())
        .flatten() // flatten the Option<String> to String
        .collect();

    let values: Vec<f32> = chunked_array_by_index[1]
        .iter()
        .flat_map(|x| x.as_primitive::<Float32Type>().iter())
        .flatten() // flatten the Option<f32> to f32
        .collect();

    assert_eq!(labels, ["A", "B", "C", "D", "E"]);
    assert_eq!(values, [0.1, 0.2, 0.3, 0.4, 0.5]);

    // Or you could use a struct with typed chunks and downcast as you gather them
    type ChunkedStringArray = Vec<StringArray>;
    type ChunkedFloat32Array = Vec<Float32Array>;

    #[derive(Default)]
    struct MyTable {
        labels: ChunkedStringArray,
        values: ChunkedFloat32Array,
    }

    let table = batches
        .iter()
        .fold(MyTable::default(), |mut table_acc, batch| {
            batch.columns().iter().enumerate().for_each(|(i, array)| {
                match batch.schema().field(i).name().as_str() {
                    "label" => table_acc.labels.push(array.as_string().clone()),
                    "value" => {
                        table_acc.values.push(array.as_primitive().clone());
                    }
                    _ => unreachable!(),
                }
            });
            table_acc
        });

    // first flatten is from Vec<Vec<T>> to Vec<T>, second is from Vec<Option<T>> to Vec<T>
    let labels: Vec<&str> = table.labels.iter().flatten().flatten().collect();
    let values: Vec<f32> = table.values.iter().flatten().flatten().collect();

    assert_eq!(labels, ["A", "B", "C", "D", "E"]);
    assert_eq!(values, [0.1, 0.2, 0.3, 0.4, 0.5]);
}

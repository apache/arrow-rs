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

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, UInt32Array, make_array,
};
use arrow_data::ArrayData;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field};
use arrow_select::coalesce::BatchCoalescer;
use arrow_select::filter::filter;
use arrow_select::interleave::interleave;
use arrow_select::take::{take, take_record_batch};

fn run_arrays() -> Vec<ArrayRef> {
    let mut arrays = Vec::new();
    for ends in [
        Int16Array::from(vec![2, 4]).to_data(),
        Int32Array::from(vec![2, 4]).to_data(),
        Int64Array::from(vec![2, 4]).to_data(),
    ] {
        for nullable in [false, true] {
            let values =
                StringArray::from(vec![Some("a"), if nullable { None } else { Some("b") }]);
            let values_field = Field::new("strings", DataType::Utf8, nullable)
                .with_metadata([("source".to_string(), "test".to_string())]);
            let data_type = DataType::RunEndEncoded(
                Arc::new(Field::new("ends", ends.data_type().clone(), false)),
                Arc::new(values_field),
            );
            let data = ArrayData::builder(data_type)
                .len(4)
                .child_data(vec![ends.clone(), values.to_data()])
                .build()
                .unwrap();
            let array = make_array(data);
            arrays.extend([Arc::clone(&array), array.slice(1, 3), array.slice(4, 0)]);
        }
    }
    arrays
}

#[test]
fn selection_preserves_run_end_type() {
    for array in run_arrays() {
        let selected: Vec<u32> = (0..array.len() as u32).step_by(2).collect();
        let mask = BooleanArray::from_iter((0..array.len()).map(|i| Some(i % 2 == 0)));
        let indices: Vec<_> = selected
            .iter()
            .enumerate()
            .map(|(i, row)| (i % 2, *row as usize))
            .collect();
        let selected = UInt32Array::from(selected);
        let expected = take(&array, &selected, None).unwrap();
        for actual in [
            filter(&array, &mask).unwrap(),
            interleave(&[array.as_ref(), array.as_ref()], &indices).unwrap(),
        ] {
            assert_eq!(actual.data_type(), array.data_type());
            assert_eq!(actual.to_data(), expected.to_data());
            actual.to_data().validate_full().unwrap();
        }
        let child = Arc::new(Field::new("runs", array.data_type().clone(), true));
        let structure = StructArray::from(vec![(child, array)]);
        let batch =
            RecordBatch::try_from_iter([("nested", Arc::new(structure) as ArrayRef)]).unwrap();
        let expected = take_record_batch(&batch, &selected).unwrap();
        let mut coalescer = BatchCoalescer::new(batch.schema(), 8);
        coalescer.push_batch_with_filter(batch, &mask).unwrap();
        coalescer.finish_buffered_batch().unwrap();
        if expected.num_rows() != 0 {
            let actual = coalescer.next_completed_batch().unwrap();
            assert_eq!(actual, expected);
            actual.column(0).to_data().validate_full().unwrap();
        } else {
            assert!(coalescer.next_completed_batch().is_none());
        }
    }
}

#[test]
fn row_roundtrip_preserves_run_end_type() {
    for array in run_arrays() {
        let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())]).unwrap();
        let rows = converter.convert_columns(&[Arc::clone(&array)]).unwrap();
        let actual = converter.convert_rows(&rows).unwrap();
        assert_eq!(actual[0].data_type(), array.data_type());
        assert_eq!(actual[0].to_data(), array.to_data());
        actual[0].to_data().validate_full().unwrap();
    }
}

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Content-defined chunking page boundaries for nested data.
//!
//! Regression tests for https://github.com/apache/arrow-rs/issues/12391:
//! page boundaries must depend only on the values written.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, ListArray, ListViewArray, RecordBatch,
    UInt32Array,
};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::take::take;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{CdcOptions, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;

/// Writes `array` with content-defined chunking, and returns the first row of each page
fn page_starts(array: ArrayRef) -> Vec<i64> {
    let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
    let cdc = CdcOptions {
        min_chunk_size: 256,
        max_chunk_size: 1024,
        norm_level: 0,
    };
    let props = WriterProperties::builder()
        .set_content_defined_chunking(Some(cdc))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(Bytes::from(buffer), options).unwrap();
    let pages = reader
        .metadata()
        .page_index()
        .unwrap()
        .page_locations(0, 0)
        .unwrap();
    pages.iter().map(|page| page.first_row_index).collect()
}

fn values(n: i32) -> impl Iterator<Item = Option<Vec<Option<i32>>>> {
    (0..n).map(|i| Some(vec![Some(i), (i % 7 != 0).then_some(i * 3)]))
}

#[test]
fn sliced_list() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(values(10_000));
    let sliced = list.slice(1000, 5000);
    // The same rows, with offsets that start at 0
    let copy = take(&sliced, &UInt32Array::from_iter_values(0..5000), None).unwrap();
    assert_eq!(sliced.to_data(), copy.to_data());
    assert_eq!(page_starts(Arc::new(sliced)), page_starts(copy));
}

#[test]
fn fixed_size_list_with_null_rows() {
    let item = Arc::new(Field::new_list_field(DataType::Int32, true));
    let nulls = NullBuffer::from_iter((0..10_000).map(|row| row % 3 != 0));
    // The same valid rows, and different values in the slots of null rows
    let list = |null_slot: i32| {
        let values = (0..20_000).map(|v| match v / 2 % 3 {
            0 => Some(null_slot + v),
            _ => (v % 5 != 0).then_some(v),
        });
        let values = Arc::new(Int32Array::from_iter(values));
        FixedSizeListArray::new(item.clone(), 2, values, Some(nulls.clone()))
    };
    let (a, b) = (list(0), list(1_000_000));
    assert_eq!(a.to_data(), b.to_data());
    assert_eq!(page_starts(Arc::new(a)), page_starts(Arc::new(b)));
}

#[test]
fn list_view_in_reverse() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(values(10_000));
    let item = Arc::new(Field::new_list_field(DataType::Int32, true));
    let offsets = ScalarBuffer::from_iter((0..10_000).rev().map(|row| row * 2));
    let sizes = ScalarBuffer::from_iter((0..10_000).map(|_| 2));
    let view = ListViewArray::new(item, offsets, sizes, list.values().clone(), None);
    page_starts(Arc::new(view));
}

#[test]
fn overlapping_list_views() {
    let item = Arc::new(Field::new_list_field(DataType::Int32, true));
    let offsets = ScalarBuffer::from_iter((0..10_000).map(|_| 0));
    let sizes = ScalarBuffer::from_iter((0..10_000).map(|_| 2));
    let values = Arc::new(Int32Array::from(vec![Some(1), None]));
    let view = ListViewArray::new(item, offsets, sizes, values, None);
    page_starts(Arc::new(view));
}

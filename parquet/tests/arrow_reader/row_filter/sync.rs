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

use arrow::{
    array::AsArray,
    compute::{concat_batches, kernels::cmp::eq, or},
};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
            RowSelection, RowSelectionPolicy, RowSelector,
        },
    },
    errors::Result,
    file::{metadata::PageIndexPolicy, properties::WriterProperties},
};

#[test]
fn test_row_selection_interleaved_skip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        ArrowDataType::Int32,
        false,
    )]));

    let values = Int32Array::from(vec![0, 1, 2, 3, 4]);
    let batch = RecordBatch::try_from_iter([("v", Arc::new(values) as ArrayRef)]).unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
    writer.write(&batch)?;
    writer.close()?;

    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(2),
        RowSelector::select(2),
    ]);

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))?
        .with_batch_size(4)
        .with_row_selection(selection)
        .build()?;

    let out = reader.next().unwrap()?;
    assert_eq!(out.num_rows(), 3);
    let values = out
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values();
    assert_eq!(values, &[0, 3, 4]);
    assert!(reader.next().is_none());
    Ok(())
}

#[test]
fn test_row_selection_mask_sparse_rows() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        ArrowDataType::Int32,
        false,
    )]));

    let values = Int32Array::from((0..30).collect::<Vec<i32>>());
    let batch = RecordBatch::try_from_iter([("v", Arc::new(values) as ArrayRef)])?;

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let total_rows = batch.num_rows();
    let ranges = (1..total_rows)
        .step_by(2)
        .map(|i| i..i + 1)
        .collect::<Vec<_>>();
    let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), total_rows);

    let selectors: Vec<RowSelector> = selection.clone().into();
    assert!(total_rows < selectors.len() * 8);

    let bytes = Bytes::from(buffer);

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?
        .with_batch_size(7)
        .with_row_selection(selection)
        .build()?;

    let mut collected = Vec::new();
    for batch in reader {
        let batch = batch?;
        collected.extend_from_slice(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values(),
        );
    }

    let expected: Vec<i32> = (1..total_rows).step_by(2).map(|i| i as i32).collect();
    assert_eq!(collected, expected);
    Ok(())
}

#[test]
fn test_row_filter_full_page_skip_is_handled() {
    let first_value: i64 = 1111;
    let last_value: i64 = 9999;
    let num_rows: usize = 12;

    // build data with row selection average length 4
    // The result would be (1111 XXXX) ... (4 page in the middle)... (XXXX 9999)
    // The Row Selection would be [1111, (skip 10), 9999]
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", arrow_schema::DataType::Int64, false),
        Field::new("value", arrow_schema::DataType::Int64, false),
    ]));

    let mut int_values: Vec<i64> = (0..num_rows as i64).collect();
    int_values[0] = first_value;
    int_values[num_rows - 1] = last_value;
    let keys = Int64Array::from(int_values.clone());
    let values = Int64Array::from(int_values.clone());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_write_batch_size(2)
        .set_data_page_row_count_limit(2)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(data.clone(), options).unwrap();
    let schema = builder.parquet_schema().clone();
    let filter_mask = ProjectionMask::leaves(&schema, [0]);

    let make_predicate = |mask: ProjectionMask| {
        ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            let column = batch.column(0);
            let match_first = eq(column, &Int64Array::new_scalar(first_value))?;
            let match_second = eq(column, &Int64Array::new_scalar(last_value))?;
            or(&match_first, &match_second)
        })
    };

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let predicate = make_predicate(filter_mask.clone());

    // The batch size is set to 12 to read all rows in one go after filtering
    // If the Reader chooses mask to handle filter, it might cause panic because the mid 4 pages may not be decoded.
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(data.clone(), options)
        .unwrap()
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 32 })
        .with_batch_size(12)
        .build()
        .unwrap();

    // Predicate pruning used to panic once mask-backed plans removed whole pages.
    // Collecting into batches validates the plan now downgrades to selectors instead.
    let schema = reader.schema().clone();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let result = concat_batches(&schema, &batches).unwrap();
    assert_eq!(result.num_rows(), 2);
}

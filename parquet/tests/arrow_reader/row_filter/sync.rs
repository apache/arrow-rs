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
    file::{
        metadata::PageIndexPolicy,
        properties::{WriterProperties, WriterVersion},
    },
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

/// Regression test for <https://github.com/apache/arrow-rs/issues/9370>
///
/// When `skip_records` on a list column crosses v2 data page boundaries,
/// the partial record state (`has_partial`) in the repetition level
/// decoder must be flushed before the whole-page-skip shortcut can fire.
/// Without the fix, the list column over-skips by one record, causing
/// struct children to disagree on record counts.
#[test]
fn test_row_selection_list_column_v2_page_boundary_skip() {
    use arrow_array::builder::{Int32Builder, ListBuilder};

    let num_rows = 10usize;

    // Schema: { id: int32, values: list<int32> }
    // Two top-level columns so that StructArrayReader can detect the
    // desync between a simple column (id) and a list column (values).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new(
            "values",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true))),
            false,
        ),
    ]));

    // Each row: id = i, values = [i*10, i*10+1]
    let ids = Int32Array::from((0..num_rows as i32).collect::<Vec<_>>());
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    for i in 0..num_rows as i32 {
        list_builder.values().append_value(i * 10);
        list_builder.values().append_value(i * 10 + 1);
        list_builder.append(true);
    }
    let values_array = list_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(values_array) as ArrayRef,
        ],
    )
    .unwrap();

    // Force v2 data pages with exactly 2 rows per page → 5 pages.
    // The default reader (no offset index) puts SerializedPageReader
    // in Values state where at_record_boundary() returns false for
    // non-last pages, matching the production scenario.
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_write_batch_size(2)
        .set_data_page_row_count_limit(2)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    // 1. Read without row selection — should always succeed
    let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, num_rows, "full read should return all rows");

    // 2. Read with row selection: select first and last row.
    // This produces the sequence read(1) + skip(8) + read(1).
    //
    // The skip crosses v2 page boundaries. Without the fix, the
    // list column's rep-level decoder has stale has_partial state
    // after exhausting the first page's remaining levels, causing
    // the whole-page-skip shortcut to over-count by one record.
    //
    // We must use RowSelectionPolicy::Selectors because the default
    // Auto policy would choose the Mask strategy for this small
    // selection, which reads all rows then filters (never calling
    // skip_records, thereby hiding the bug).
    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(num_rows - 2),
        RowSelector::select(1),
    ]);

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_row_selection(selection)
        .with_row_selection_policy(RowSelectionPolicy::Selectors)
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "selection should return exactly 2 rows");

    // Verify data correctness: row 0 and row 9
    let result = concat_batches(&schema, &batches).unwrap();
    let id_col = result
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(id_col.value(0), 0);
    assert_eq!(id_col.value(1), (num_rows - 1) as i32);

    let list_col = result.column(1).as_list::<i32>();
    let first = list_col
        .value(0)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec();
    assert_eq!(first, vec![0, 1]);
    let last = list_col
        .value(1)
        .as_primitive::<arrow_array::types::Int32Type>()
        .values()
        .to_vec();
    let n = (num_rows - 1) as i32;
    assert_eq!(last, vec![n * 10, n * 10 + 1]);
}

/// Regression test for <https://github.com/apache/arrow-rs/issues/9370>
///
/// When leaf columns inside a `List<Struct<…>>` have different page
/// boundaries (due to value-size differences), the `has_partial` state
/// bug causes one leaf to over-skip by one record while the other stays
/// correct.
#[test]
fn test_list_struct_page_boundary_desync_produces_length_mismatch() {
    use arrow_array::Array;
    use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder, StructBuilder};
    use arrow_schema::Fields;

    let num_rows = 14usize;
    // Long string forces the string column to flush pages much sooner
    // than the int32 column, creating different page boundaries.
    let long_prefix = "x".repeat(500);

    // Schema: { vals: List<Struct<x: Int32, y: Utf8>> }
    let struct_fields = Fields::from(vec![
        Field::new("x", ArrowDataType::Int32, false),
        Field::new("y", ArrowDataType::Utf8, false),
    ]);

    // Build data: even rows have 2 list elements, odd rows have 3.
    // This ensures different physical value counts per record, so
    // reading from the wrong position produces a different total.
    let mut list_builder = ListBuilder::new(StructBuilder::from_fields(struct_fields, 0));
    for i in 0..num_rows {
        let num_elems = if i % 2 == 0 { 2 } else { 3 };
        let sb = list_builder.values();
        for j in 0..num_elems {
            sb.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(i as i32 * 10 + j);
            sb.field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(format!("{long_prefix}_{i}_{j}"));
            sb.append(true);
        }
        list_builder.append(true);
    }
    let vals_array = list_builder.finish();

    // Derive schema from the actual array type to avoid field-name mismatches.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vals",
        vals_array.data_type().clone(),
        false,
    )]));

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(vals_array) as ArrayRef]).unwrap();

    // V2 pages + small max_data_page_size.
    // Column x (Int32): all 14 rows fit in one page (~140 bytes values).
    // Column y (Utf8, 500-byte strings): pages flush after every ~2 rows.
    // This creates the page boundary asymmetry needed to trigger the bug.
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_write_batch_size(2)
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(512)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    // 1. Read without row selection — should always succeed.
    let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, num_rows, "full read should return all rows");

    // 2. Read with selection: select(1) + skip(10) + select(1).
    //    Without the fix, the string column (y) over-skips by 1,
    //    reading a record with a different element count than the
    //    int column (x).  The inner StructArrayReader sees arrays
    //    of different lengths.
    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(10),
        RowSelector::select(1),
    ]);

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_row_selection(selection)
        .with_row_selection_policy(RowSelectionPolicy::Selectors)
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "selection should return exactly 2 rows");
}

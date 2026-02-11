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
use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowSchemaConverter, ArrowWriter, ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
            RowSelection, RowSelectionPolicy, RowSelector,
        },
    },
    column::writer::ColumnCloseResult,
    errors::Result,
    file::{
        metadata::ColumnChunkMetaData,
        metadata::PageIndexPolicy,
        properties::{WriterProperties, WriterVersion},
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter,
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

#[test]
fn test_row_selection_struct_mismatched_children_regression_9370() {
    // Reproduces https://github.com/apache/arrow-rs/issues/9370 on main by constructing
    // a synthetic file with a mixed V1/V2 page sequence for one nested leaf column.
    fn write_struct_file(
        batch: &RecordBatch,
        version: WriterVersion,
        row_count_limit: usize,
    ) -> Bytes {
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_dictionary_enabled(false)
            .set_offset_index_disabled(true)
            .set_data_page_row_count_limit(row_count_limit)
            .set_write_batch_size(1)
            .build();

        let mut sink = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut sink, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(sink)
    }

    fn extract_column_chunk(file_bytes: &Bytes, col_idx: usize) -> (Bytes, ColumnCloseResult) {
        let reader = SerializedFileReader::new(file_bytes.clone()).unwrap();
        let row_group = reader.metadata().row_group(0);
        let column = row_group.column(col_idx);

        let src_dictionary_offset = column.dictionary_page_offset();
        let src_data_offset = column.data_page_offset();
        let src_offset = src_dictionary_offset.unwrap_or(src_data_offset);
        let src_length = column.compressed_size() as usize;
        let src_start = src_offset as usize;
        let src_end = src_start + src_length;
        let chunk = file_bytes.slice(src_start..src_end);

        let mut builder = ColumnChunkMetaData::builder(column.column_descr_ptr())
            .set_compression(column.compression())
            .set_encodings_mask(*column.encodings_mask())
            .set_total_compressed_size(column.compressed_size())
            .set_total_uncompressed_size(column.uncompressed_size())
            .set_num_values(column.num_values())
            .set_data_page_offset(src_data_offset - src_offset)
            .set_dictionary_page_offset(src_dictionary_offset.map(|x| x - src_offset))
            .set_unencoded_byte_array_data_bytes(column.unencoded_byte_array_data_bytes());

        if let Some(stats) = column.statistics() {
            builder = builder.set_statistics(stats.clone());
        }
        if let Some(page_encoding_stats) = column.page_encoding_stats() {
            builder = builder.set_page_encoding_stats(page_encoding_stats.clone());
        }

        let close = ColumnCloseResult {
            bytes_written: src_length as u64,
            rows_written: row_group.num_rows() as u64,
            metadata: builder.build().unwrap(),
            bloom_filter: None,
            column_index: None,
            offset_index: None,
        };
        (chunk, close)
    }

    let list_values = [[10, 20], [30, 40], [50, 60], [70, 80]];

    let mut a_builder = ListBuilder::new(Int32Builder::new());
    for values in list_values {
        a_builder.values().append_slice(&values);
        a_builder.append(true);
    }
    let a = Arc::new(a_builder.finish()) as ArrayRef;
    let b = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let struct_array = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("a", a.data_type().clone(), false)), a),
        (
            Arc::new(Field::new("b", ArrowDataType::Int32, false)),
            b.clone(),
        ),
    ])) as ArrayRef;
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        struct_array.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![struct_array]).unwrap();

    let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();

    let file_a_v1 = write_struct_file(&batch.slice(0, 1), WriterVersion::PARQUET_1_0, 1024);
    let file_a_v2 = write_struct_file(&batch.slice(1, 3), WriterVersion::PARQUET_2_0, 2);
    let file_b_v2 = write_struct_file(&batch, WriterVersion::PARQUET_2_0, 1024);

    let (a_v1_bytes, a_v1_close) = extract_column_chunk(&file_a_v1, 0);
    let (a_v2_bytes, a_v2_close) = extract_column_chunk(&file_a_v2, 0);
    let (b_v2_bytes, b_v2_close) = extract_column_chunk(&file_b_v2, 1);

    let mut mixed_a_bytes = Vec::with_capacity(a_v1_bytes.len() + a_v2_bytes.len());
    mixed_a_bytes.extend_from_slice(&a_v1_bytes);
    mixed_a_bytes.extend_from_slice(&a_v2_bytes);

    let mixed_a_metadata = ColumnChunkMetaData::builder(a_v1_close.metadata.column_descr_ptr())
        .set_compression(a_v1_close.metadata.compression())
        .set_encodings_mask(*a_v1_close.metadata.encodings_mask())
        .set_total_compressed_size(mixed_a_bytes.len() as i64)
        .set_total_uncompressed_size(
            a_v1_close.metadata.uncompressed_size() + a_v2_close.metadata.uncompressed_size(),
        )
        .set_num_values(a_v1_close.metadata.num_values() + a_v2_close.metadata.num_values())
        .set_data_page_offset(a_v1_close.metadata.data_page_offset())
        .build()
        .unwrap();

    let mixed_a_close = ColumnCloseResult {
        bytes_written: mixed_a_bytes.len() as u64,
        rows_written: 4,
        metadata: mixed_a_metadata,
        bloom_filter: None,
        column_index: None,
        offset_index: None,
    };

    let mut parquet_bytes = Vec::new();
    let mut file_writer = SerializedFileWriter::new(
        &mut parquet_bytes,
        parquet_schema.root_schema_ptr(),
        Arc::new(
            WriterProperties::builder()
                .set_offset_index_disabled(true)
                .build(),
        ),
    )
    .unwrap();
    let mut row_group = file_writer.next_row_group().unwrap();
    row_group
        .append_column(&Bytes::from(mixed_a_bytes), mixed_a_close)
        .unwrap();
    row_group.append_column(&b_v2_bytes, b_v2_close).unwrap();
    row_group.close().unwrap();
    file_writer.close().unwrap();

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(parquet_bytes), options)
            .unwrap()
            .with_batch_size(4)
            .with_row_selection_policy(RowSelectionPolicy::Selectors)
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(2),
            ]))
            .build()
            .unwrap();

    // read results and validate the results are correct
    let batches: Result<Vec<_>, _> = reader.collect();
    let batches = batches.unwrap();
    assert_eq!(batches.len(), 0);

    // TODO verify the values
}

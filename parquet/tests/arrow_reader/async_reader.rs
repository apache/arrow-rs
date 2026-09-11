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

use super::io::TestReader;
use arrow::compute::kernels::cmp::eq;
use arrow::error::Result as ArrowResult;
use arrow_array::builder::{Float32Builder, ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, RecordBatchReader, Scalar, StringArray,
    StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::RowGroupSelection;
use parquet::arrow::{
    ArrowWriter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use parquet::errors::Result;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use rand::{RngExt, rng};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempfile;

#[tokio::test]
async fn test_async_reader() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data.clone());

    let requests = async_reader.requests();
    let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();

    let metadata = builder.metadata().clone();
    assert_eq!(metadata.num_row_groups(), 1);

    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
    let stream = builder
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .build()
        .unwrap();

    let async_batches: Vec<_> = stream.try_collect().await.unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_projection(mask)
        .with_batch_size(104)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    assert_eq!(async_batches, sync_batches);

    let requests = requests.lock().unwrap();
    let (offset_1, length_1) = metadata.row_group(0).column(1).byte_range();
    let (offset_2, length_2) = metadata.row_group(0).column(2).byte_range();

    assert_eq!(
        &requests[..],
        &[
            offset_1 as usize..(offset_1 + length_1) as usize,
            offset_2 as usize..(offset_2 + length_2) as usize
        ]
    );
}

#[tokio::test]
async fn test_async_reader_row_group_local_selections() {
    let batch = RecordBatch::try_from_iter([(
        "a",
        Arc::new(Int32Array::from_iter_values(0..6)) as ArrayRef,
    )])
    .unwrap();
    let mut data = Vec::new();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let stream = ParquetRecordBatchStreamBuilder::new(TestReader::new(data.into()))
        .await
        .unwrap()
        .with_row_group_selections(vec![
            RowGroupSelection::new(1, Some(RowSelection::from(vec![RowSelector::select(1)]))),
            RowGroupSelection::new(
                0,
                Some(RowSelection::from(vec![
                    RowSelector::skip(1),
                    RowSelector::select(2),
                ])),
            ),
        ])
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int32Type>().values(),
        &[3]
    );
    assert_eq!(
        batches[1].column(0).as_primitive::<Int32Type>().values(),
        &[1, 2]
    );
}

#[tokio::test]
async fn test_async_reader_with_next_row_group() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data.clone());

    let requests = async_reader.requests();
    let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();

    let metadata = builder.metadata().clone();
    assert_eq!(metadata.num_row_groups(), 1);

    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
    let mut stream = builder
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut readers = vec![];
    while let Some(reader) = stream.next_row_group().await.unwrap() {
        readers.push(reader);
    }

    let async_batches: Vec<_> = readers
        .into_iter()
        .flat_map(|r| r.map(|v| v.unwrap()).collect::<Vec<_>>())
        .collect();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_projection(mask)
        .with_batch_size(104)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    assert_eq!(async_batches, sync_batches);

    let requests = requests.lock().unwrap();
    let (offset_1, length_1) = metadata.row_group(0).column(1).byte_range();
    let (offset_2, length_2) = metadata.row_group(0).column(2).byte_range();

    assert_eq!(
        &requests[..],
        &[
            offset_1 as usize..(offset_1 + length_1) as usize,
            offset_2 as usize..(offset_2 + length_2) as usize
        ]
    );
}

#[tokio::test]
async fn test_async_reader_with_index() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data.clone());

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap();

    // The builder should have page and offset indexes loaded now
    let metadata_with_index = builder.metadata();
    assert_eq!(metadata_with_index.num_row_groups(), 1);

    // Check offset indexes are present for all columns of all row groups
    let page_index = metadata_with_index
        .page_index()
        .expect("page index should be present");
    assert!(page_index.is_complete());
    let num_rowgroups = metadata_with_index.num_row_groups();
    let num_columns = metadata_with_index
        .file_metadata()
        .schema_descr()
        .num_columns();
    for rgidx in 0..num_rowgroups {
        // some column indexes are not defined, but all offset indexes should be
        for colidx in 0..num_columns {
            assert!(page_index.offset_index(rgidx, colidx).is_some());
        }
    }

    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
    let stream = builder
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .build()
        .unwrap();

    let async_batches: Vec<_> = stream.try_collect().await.unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_projection(mask)
        .with_batch_size(1024)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    assert_eq!(async_batches, sync_batches);
}

#[tokio::test]
async fn test_async_reader_with_limit() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let metadata = Arc::new(metadata);

    assert_eq!(metadata.num_row_groups(), 1);

    let async_reader = TestReader::new(data.clone());

    let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();

    assert_eq!(builder.metadata().num_row_groups(), 1);

    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
    let stream = builder
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .with_limit(1)
        .build()
        .unwrap();

    let async_batches: Vec<_> = stream.try_collect().await.unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_projection(mask)
        .with_batch_size(1024)
        .with_limit(1)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    assert_eq!(async_batches, sync_batches);
}

#[tokio::test]
async fn test_async_reader_skip_pages() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data.clone());

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap();

    assert_eq!(builder.metadata().num_row_groups(), 1);

    let selection = RowSelection::from(vec![
        RowSelector::skip(21),   // Skip first page
        RowSelector::select(21), // Select page to boundary
        RowSelector::skip(41),   // Skip multiple pages
        RowSelector::select(41), // Select multiple pages
        RowSelector::skip(25),   // Skip page across boundary
        RowSelector::select(25), // Select across page boundary
        RowSelector::skip(7116), // Skip to final page boundary
        RowSelector::select(10), // Select final page
    ]);

    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![9]);

    let stream = builder
        .with_projection(mask.clone())
        .with_row_selection(selection.clone())
        .build()
        .expect("building stream");

    let async_batches: Vec<_> = stream.try_collect().await.unwrap();

    let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .with_projection(mask)
        .with_batch_size(1024)
        .with_row_selection(selection)
        .build()
        .unwrap()
        .collect::<ArrowResult<Vec<_>>>()
        .unwrap();

    assert_eq!(async_batches, sync_batches);
}

#[tokio::test]
async fn test_fuzz_async_reader_selection() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let mut rand = rng();

    for _ in 0..100 {
        let mut expected_rows = 0;
        let mut total_rows = 0;
        let mut skip = false;
        let mut selectors = vec![];

        while total_rows < 7300 {
            let row_count: usize = rand.random_range(1..100);

            let row_count = row_count.min(7300 - total_rows);

            selectors.push(RowSelector { row_count, skip });

            total_rows += row_count;
            if !skip {
                expected_rows += row_count;
            }

            skip = !skip;
        }

        let selection = RowSelection::from(selectors);

        let async_reader = TestReader::new(data.clone());

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        assert_eq!(builder.metadata().num_row_groups(), 1);

        let col_idx: usize = rand.random_range(0..13);
        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![col_idx]);

        let stream = builder
            .with_projection(mask.clone())
            .with_row_selection(selection.clone())
            .build()
            .expect("building stream");

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let actual_rows: usize = async_batches.into_iter().map(|b| b.num_rows()).sum();

        assert_eq!(actual_rows, expected_rows);
    }
}

#[tokio::test]
async fn test_async_reader_zero_row_selector() {
    //See https://github.com/apache/arrow-rs/issues/2669
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let mut rand = rng();

    let mut expected_rows = 0;
    let mut total_rows = 0;
    let mut skip = false;
    let mut selectors = vec![];

    selectors.push(RowSelector {
        row_count: 0,
        skip: false,
    });

    while total_rows < 7300 {
        let row_count: usize = rand.random_range(1..100);

        let row_count = row_count.min(7300 - total_rows);

        selectors.push(RowSelector { row_count, skip });

        total_rows += row_count;
        if !skip {
            expected_rows += row_count;
        }

        skip = !skip;
    }

    let selection = RowSelection::from(selectors);

    let async_reader = TestReader::new(data.clone());

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap();

    assert_eq!(builder.metadata().num_row_groups(), 1);

    let col_idx: usize = rand.random_range(0..13);
    let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![col_idx]);

    let stream = builder
        .with_projection(mask.clone())
        .with_row_selection(selection.clone())
        .build()
        .expect("building stream");

    let async_batches: Vec<_> = stream.try_collect().await.unwrap();

    let actual_rows: usize = async_batches.into_iter().map(|b| b.num_rows()).sum();

    assert_eq!(actual_rows, expected_rows);
}

#[tokio::test]
async fn test_limit_multiple_row_groups() {
    let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
    let b = StringArray::from_iter_values(["1", "2", "3", "4", "5", "6"]);
    let c = Int32Array::from_iter(0..6);
    let data = RecordBatch::try_from_iter([
        ("a", Arc::new(a) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
        ("c", Arc::new(c) as ArrayRef),
    ])
    .unwrap();

    let mut buf = Vec::with_capacity(1024);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), Some(props)).unwrap();
    writer.write(&data).unwrap();
    writer.close().unwrap();

    let data: Bytes = buf.into();
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();

    assert_eq!(metadata.num_row_groups(), 2);

    let test = TestReader::new(data);

    let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
        .await
        .unwrap()
        .with_batch_size(1024)
        .with_limit(4)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    // Expect one batch for each row group
    assert_eq!(batches.len(), 2);

    let batch = &batches[0];
    // First batch should contain all rows
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3);
    let col2 = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(col2.values(), &[0, 1, 2]);

    let batch = &batches[1];
    // Second batch should trigger the limit and only have one row
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);
    let col2 = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(col2.values(), &[3]);

    let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
        .await
        .unwrap()
        .with_offset(2)
        .with_limit(3)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    // Expect one batch for each row group
    assert_eq!(batches.len(), 2);

    let batch = &batches[0];
    // First batch should contain one row
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);
    let col2 = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(col2.values(), &[2]);

    let batch = &batches[1];
    // Second batch should contain two rows
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);
    let col2 = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(col2.values(), &[3, 4]);

    let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
        .await
        .unwrap()
        .with_offset(4)
        .with_limit(20)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    // Should skip first row group
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    // First batch should contain two rows
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);
    let col2 = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(col2.values(), &[4, 5]);
}

#[tokio::test]
async fn test_parquet_record_batch_stream_schema() {
    fn get_all_field_names(schema: &Schema) -> Vec<&String> {
        schema.flattened_fields().iter().map(|f| f.name()).collect()
    }

    // ParquetRecordBatchReaderBuilder::schema differs from
    // ParquetRecordBatchReader::schema and RecordBatch::schema in the returned
    // schema contents (in terms of custom metadata attached to schema, and fields
    // returned). Test to ensure this remains consistent behaviour.
    //
    // Ensure same for asynchronous versions of the above.

    // Prep data, for a schema with nested fields, with custom metadata
    let mut metadata = HashMap::with_capacity(1);
    metadata.insert("key".to_string(), "value".to_string());

    let nested_struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("d", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("e", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
        ),
    ]);
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![-1, 1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::UInt64, true)),
            Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "c",
                nested_struct_array.data_type().clone(),
                true,
            )),
            Arc::new(nested_struct_array) as ArrayRef,
        ),
    ]);

    let schema =
        Arc::new(Schema::new(struct_array.fields().clone()).with_metadata(metadata.clone()));
    let record_batch = RecordBatch::from(struct_array)
        .with_schema(schema.clone())
        .unwrap();

    // Write parquet with custom metadata in schema
    let mut file = tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    writer.write(&record_batch).unwrap();
    writer.close().unwrap();

    let all_fields = ["a", "b", "c", "d", "e"];
    // (leaf indices in mask, expected names in output schema all fields)
    let projections = [
        (vec![], vec![]),
        (vec![0], vec!["a"]),
        (vec![0, 1], vec!["a", "b"]),
        (vec![0, 1, 2], vec!["a", "b", "c", "d"]),
        (vec![0, 1, 2, 3], vec!["a", "b", "c", "d", "e"]),
    ];

    // Ensure we're consistent for each of these projections
    for (indices, expected_projected_names) in projections {
        let assert_schemas = |builder: SchemaRef, reader: SchemaRef, batch: SchemaRef| {
            // Builder schema should preserve all fields and metadata
            assert_eq!(get_all_field_names(&builder), all_fields);
            assert_eq!(builder.metadata, metadata);
            // Reader & batch schema should show only projected fields, and no metadata
            assert_eq!(get_all_field_names(&reader), expected_projected_names);
            assert_eq!(reader.metadata, HashMap::default());
            assert_eq!(get_all_field_names(&batch), expected_projected_names);
            assert_eq!(batch.metadata, HashMap::default());
        };

        let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
        let sync_builder_schema = builder.schema().clone();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), indices.clone());
        let mut reader = builder.with_projection(mask).build().unwrap();
        let sync_reader_schema = reader.schema();
        let batch = reader.next().unwrap().unwrap();
        let sync_batch_schema = batch.schema();
        assert_schemas(sync_builder_schema, sync_reader_schema, sync_batch_schema);

        // asynchronous should be same
        let file = tokio::fs::File::from(file.try_clone().unwrap());
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let async_builder_schema = builder.schema().clone();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), indices);
        let mut reader = builder.with_projection(mask).build().unwrap();
        let async_reader_schema = reader.schema().clone();
        let batch = reader.next().await.unwrap().unwrap();
        let async_batch_schema = batch.schema();
        assert_schemas(
            async_builder_schema,
            async_reader_schema,
            async_batch_schema,
        );
    }
}

#[tokio::test]
async fn test_nested_skip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::UInt64, false),
        Field::new_list("col_2", Field::new_list_field(DataType::Utf8, true), true),
    ]));

    // Default writer properties
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(256)
        .set_write_batch_size(256)
        .set_max_row_group_row_count(Some(1024));

    // Write data
    let mut file = tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), Some(props.build())).unwrap();

    let mut builder = ListBuilder::new(StringBuilder::new());
    for id in 0..1024 {
        match id % 3 {
            0 => builder.append_value([Some("val_1".to_string()), Some(format!("id_{id}"))]),
            1 => builder.append_value([Some(format!("id_{id}"))]),
            _ => builder.append_null(),
        }
    }
    let refs = vec![
        Arc::new(UInt64Array::from_iter_values(0..1024)) as ArrayRef,
        Arc::new(builder.finish()) as ArrayRef,
    ];

    let batch = RecordBatch::try_new(schema.clone(), refs).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let selections = [
        RowSelection::from(vec![
            RowSelector::skip(313),
            RowSelector::select(1),
            RowSelector::skip(709),
            RowSelector::select(1),
        ]),
        RowSelection::from(vec![
            RowSelector::skip(255),
            RowSelector::select(1),
            RowSelector::skip(767),
            RowSelector::select(1),
        ]),
        RowSelection::from(vec![
            RowSelector::select(255),
            RowSelector::skip(1),
            RowSelector::select(767),
            RowSelector::skip(1),
        ]),
        RowSelection::from(vec![
            RowSelector::skip(254),
            RowSelector::select(1),
            RowSelector::select(1),
            RowSelector::skip(767),
            RowSelector::select(1),
        ]),
    ];

    for selection in selections {
        let expected = selection.row_count();
        // Read data
        let mut reader = ParquetRecordBatchStreamBuilder::new_with_options(
            tokio::fs::File::from_std(file.try_clone().unwrap()),
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
        )
        .await
        .unwrap();

        reader = reader.with_row_selection(selection);

        let mut stream = reader.build().unwrap();

        let mut total_rows = 0;
        while let Some(rb) = stream.next().await {
            let rb = rb.unwrap();
            total_rows += rb.num_rows();
        }
        assert_eq!(total_rows, expected);
    }
}

#[tokio::test]
async fn non_empty_offset_index_doesnt_panic_in_read_row_group() {
    use tokio::fs::File;
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages.parquet");
    let mut file = File::open(&path).await.unwrap();
    let file_size = file.metadata().await.unwrap().len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .load_and_finish(&mut file, file_size)
        .await
        .unwrap();

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
        .build()
        .unwrap();

    let result = reader.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 8);
}

#[tokio::test]
async fn empty_offset_index_doesnt_panic_in_column_chunks() {
    use tempfile::TempDir;
    use tokio::fs::File;
    fn write_metadata_to_local_file(metadata: ParquetMetaData, file: impl AsRef<std::path::Path>) {
        use parquet::file::metadata::ParquetMetaDataWriter;
        use std::fs::File;
        let file = File::create(file).unwrap();
        ParquetMetaDataWriter::new(file, &metadata)
            .finish()
            .unwrap()
    }

    fn read_metadata_from_local_file(file: impl AsRef<std::path::Path>) -> ParquetMetaData {
        use std::fs::File;
        let file = File::open(file).unwrap();
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap()
    }

    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let mut file = File::open(&path).await.unwrap();
    let file_size = file.metadata().await.unwrap().len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .load_and_finish(&mut file, file_size)
        .await
        .unwrap();

    let tempdir = TempDir::new().unwrap();
    let metadata_path = tempdir.path().join("thrift_metadata.dat");
    write_metadata_to_local_file(metadata, &metadata_path);
    let metadata = read_metadata_from_local_file(&metadata_path);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
        .build()
        .unwrap();

    // Panics here
    let result = reader.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_cached_array_reader_sparse_offset_error() {
    use futures::TryStreamExt;

    use arrow_array::{BooleanArray, RecordBatch};
    use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector};

    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let async_reader = TestReader::new(data);

    // Enable page index so the fetch logic loads only required pages
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap();

    // Skip the first 22 rows (entire first Parquet page) and then select the
    // next 3 rows (22, 23, 24). This means the fetch step will not include
    // the first page starting at file offset 0.
    let selection = RowSelection::from(vec![RowSelector::skip(22), RowSelector::select(3)]);

    // Trivial predicate on column 0 that always returns `true`. Using the
    // same column in both predicate and projection activates the caching
    // layer (Producer/Consumer pattern).
    let parquet_schema = builder.parquet_schema();
    let proj = ProjectionMask::leaves(parquet_schema, vec![0]);
    let always_true = ArrowPredicateFn::new(proj.clone(), |batch: RecordBatch| {
        Ok(BooleanArray::from(vec![true; batch.num_rows()]))
    });
    let filter = RowFilter::new(vec![Box::new(always_true)]);

    // Build the stream with batch size 8 so the cache reads whole batches
    // that straddle the requested row range (rows 0-7, 8-15, 16-23, …).
    let stream = builder
        .with_batch_size(8)
        .with_projection(proj)
        .with_row_selection(selection)
        .with_row_filter(filter)
        .build()
        .unwrap();

    // Collecting the stream should fail with the sparse column chunk offset
    // error we want to reproduce.
    let _result: Vec<_> = stream.try_collect().await.unwrap();
}

#[tokio::test]
async fn test_predicate_cache_disabled() {
    let k = Int32Array::from_iter_values(0..10);
    let data = RecordBatch::try_from_iter([("k", Arc::new(k) as ArrayRef)]).unwrap();

    let mut buf = Vec::new();
    // both the page row limit and batch size are set to 1 to create one page per row
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_max_row_group_row_count(Some(10))
        .set_write_page_header_statistics(true)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), Some(props)).unwrap();
    writer.write(&data).unwrap();
    writer.close().unwrap();

    let data = Bytes::from(buf);
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&data)
        .unwrap();
    let parquet_schema = metadata.file_metadata().schema_descr_ptr();

    // the filter is not clone-able, so we use a lambda to simplify
    let build_filter = || {
        let scalar = Int32Array::from_iter_values([5]);
        let predicate = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![0]),
            move |batch| eq(batch.column(0), &Scalar::new(&scalar)),
        );
        RowFilter::new(vec![Box::new(predicate)])
    };

    // select only one of the pages
    let selection = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(1)]);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();

    // using the predicate cache (default)
    let reader_with_cache = TestReader::new(data.clone());
    let requests_with_cache = reader_with_cache.requests();
    let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
        reader_with_cache,
        reader_metadata.clone(),
    )
    .with_batch_size(1000)
    .with_row_selection(selection.clone())
    .with_row_filter(build_filter())
    .build()
    .unwrap();
    let batches_with_cache: Vec<_> = stream.try_collect().await.unwrap();

    // disabling the predicate cache
    let reader_without_cache = TestReader::new(data);
    let requests_without_cache = reader_without_cache.requests();
    let stream =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader_without_cache, reader_metadata)
            .with_batch_size(1000)
            .with_row_selection(selection)
            .with_row_filter(build_filter())
            .with_max_predicate_cache_size(0) // disabling it by setting the limit to 0
            .build()
            .unwrap();
    let batches_without_cache: Vec<_> = stream.try_collect().await.unwrap();

    assert_eq!(batches_with_cache, batches_without_cache);

    let requests_with_cache = requests_with_cache.lock().unwrap();
    let requests_without_cache = requests_without_cache.lock().unwrap();

    // less requests will be made without the predicate cache
    assert_eq!(requests_with_cache.len(), 11);
    assert_eq!(requests_without_cache.len(), 2);

    // less bytes will be retrieved without the predicate cache
    assert_eq!(
        requests_with_cache.iter().map(|r| r.len()).sum::<usize>(),
        433
    );
    assert_eq!(
        requests_without_cache
            .iter()
            .map(|r| r.len())
            .sum::<usize>(),
        92
    );
}

#[tokio::test]
async fn test_nested_lists() -> Result<()> {
    // Test case for https://github.com/apache/arrow-rs/issues/8657
    let list_inner_field = Arc::new(Field::new("item", DataType::Float32, true));
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector", DataType::List(list_inner_field.clone()), true),
    ]));

    let mut list_builder =
        ListBuilder::new(Float32Builder::new()).with_field(list_inner_field.clone());
    list_builder.values().append_slice(&[10.0, 10.0, 10.0]);
    list_builder.append(true);
    list_builder.values().append_slice(&[20.0, 20.0, 20.0]);
    list_builder.append(true);
    list_builder.values().append_slice(&[30.0, 30.0, 30.0]);
    list_builder.append(true);
    list_builder.values().append_slice(&[40.0, 40.0, 40.0]);
    list_builder.append(true);
    let list_array = list_builder.finish();

    let data = vec![RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(list_array),
        ],
    )?];

    let mut buffer = Vec::new();
    let mut writer = AsyncArrowWriter::try_new(&mut buffer, table_schema, None)?;

    for batch in data {
        writer.write(&batch).await?;
    }

    writer.close().await?;

    let reader = TestReader::new(Bytes::from(buffer));
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

    let predicate = ArrowPredicateFn::new(ProjectionMask::all(), |batch| {
        Ok(BooleanArray::from(vec![true; batch.num_rows()]))
    });

    let projection_mask = ProjectionMask::all();

    let mut stream = builder
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_projection(projection_mask)
        .build()?;

    while let Some(batch) = stream.next().await {
        let _ = batch.unwrap(); // ensure there is no panic
    }

    Ok(())
}

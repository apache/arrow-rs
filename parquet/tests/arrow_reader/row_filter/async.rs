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

use crate::io::TestReader;
use std::sync::{Arc, Mutex};

use arrow::{
    array::AsArray,
    compute::{concat_batches, kernels::cmp::eq, or},
    datatypes::{Int32Type, TimestampNanosecondType},
};
use arrow_array::{
    ArrayRef, BooleanArray, Int8Array, Int32Array, Int64Array, RecordBatch, Scalar, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelectionPolicy,
            RowSelector,
        },
    },
    file::{
        metadata::{PageIndexPolicy, ParquetMetaDataReader},
        properties::WriterProperties,
    },
};

fn make_two_column_i64_file(values: &[i64], rows_per_page: usize) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let keys = Int64Array::from(values.to_vec());
    let values = Int64Array::from(values.to_vec());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_write_batch_size(rows_per_page)
        .set_data_page_row_count_limit(rows_per_page)
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
}

#[tokio::test]
async fn test_row_filter_full_page_skip_is_handled_async() {
    let first_value: i64 = 1111;
    let last_value: i64 = 9999;
    let num_rows: usize = 12;

    // build data with row selection average length 4
    // The result would be (1111 XXXX) ... (4 page in the middle)... (XXXX 9999)
    // The Row Selection would be [1111, (skip 10), 9999]
    let mut int_values: Vec<i64> = (0..num_rows as i64).collect();
    int_values[0] = first_value;
    int_values[num_rows - 1] = last_value;
    let data = make_two_column_i64_file(&int_values, 2);

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data.clone()),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();
    let schema = builder.parquet_schema().clone();
    let filter_mask = ProjectionMask::leaves(&schema, [0]);

    for policy in [
        RowSelectionPolicy::Auto { threshold: 32 },
        RowSelectionPolicy::Mask,
    ] {
        for max_predicate_cache_size in [None, Some(0)] {
            for with_initial_selection in [false, true] {
                let predicate_batch_sizes = Arc::new(Mutex::new(Vec::new()));
                let observed_batch_sizes = Arc::clone(&predicate_batch_sizes);
                let predicate =
                    ArrowPredicateFn::new(filter_mask.clone(), move |batch: RecordBatch| {
                        observed_batch_sizes.lock().unwrap().push(batch.num_rows());
                        let column = batch.column(0);
                        let match_first = eq(column, &Int64Array::new_scalar(first_value))?;
                        let match_second = eq(column, &Int64Array::new_scalar(last_value))?;
                        or(&match_first, &match_second)
                    });

                let stream = ParquetRecordBatchStreamBuilder::new_with_options(
                    TestReader::new(data.clone()),
                    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
                )
                .await
                .unwrap()
                .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
                .with_batch_size(12)
                .with_row_selection_policy(policy);
                let stream = if with_initial_selection {
                    stream.with_row_selection(RowSelection::from(vec![
                        RowSelector::select(1),
                        RowSelector::skip(10),
                        RowSelector::select(1),
                    ]))
                } else {
                    stream
                };
                let stream = match max_predicate_cache_size {
                    Some(size) => stream.with_max_predicate_cache_size(size),
                    None => stream,
                }
                .build()
                .unwrap();

                let schema = stream.schema().clone();
                let batches: Vec<_> = stream.try_collect().await.unwrap();
                let batch_sizes = batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .collect::<Vec<_>>();
                assert_eq!(
                    batch_sizes,
                    vec![2],
                    "policy={policy:?}, cache={max_predicate_cache_size:?}, initial={with_initial_selection}"
                );
                assert_eq!(
                    *predicate_batch_sizes.lock().unwrap(),
                    if with_initial_selection {
                        vec![2]
                    } else {
                        vec![12]
                    },
                    "policy={policy:?}, cache={max_predicate_cache_size:?}, initial={with_initial_selection}"
                );

                let result = concat_batches(&schema, &batches).unwrap();
                assert_eq!(result.num_rows(), 2);
                assert_eq!(
                    result
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(vec![first_value, last_value])
                );
            }
        }
    }
}

#[tokio::test]
async fn test_mask_coalesces_loaded_ranges_to_batch_size() {
    let values = (0..12).collect::<Vec<i64>>();
    let data = make_two_column_i64_file(&values, 2);
    let stream = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap()
    .with_row_selection(RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(4),
        RowSelector::select(1),
        RowSelector::skip(5),
        RowSelector::select(1),
    ]))
    .with_batch_size(2)
    .with_row_selection_policy(RowSelectionPolicy::Mask)
    .build()
    .unwrap();

    let schema = stream.schema().clone();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );

    let batch = concat_batches(&schema, &batches).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 5, 11]
    );
}

#[tokio::test]
async fn test_mask_nested_projection_with_different_page_boundaries() {
    use arrow_array::{
        Array, UInt32Array,
        builder::{Int32Builder, ListBuilder, StringBuilder, StructBuilder},
    };
    use arrow_schema::Fields;
    use parquet::file::properties::WriterVersion;

    let num_rows = 14usize;
    let long_prefix = "x".repeat(500);
    let struct_fields = Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]);
    let mut list_builder = ListBuilder::new(StructBuilder::from_fields(struct_fields, 0));
    for row in 0..num_rows {
        let values = list_builder.values();
        for item in 0..if row % 2 == 0 { 2 } else { 3 } {
            values
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(row as i32 * 10 + item);
            values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(format!("{long_prefix}_{row}_{item}"));
            values.append(true);
        }
        list_builder.append(true);
    }
    let values = list_builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        values.data_type().clone(),
        false,
    )]));
    let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();

    // The fixed-width leaf fits in one page, while the long string leaf flushes
    // frequently and therefore has different page boundaries.
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_write_batch_size(2)
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(512)
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(Bytes::from(buffer)),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();
    let page_first_rows = builder.metadata().offset_index().unwrap()[0]
        .iter()
        .map(|column| {
            column
                .page_locations()
                .iter()
                .map(|page| page.first_row_index)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(page_first_rows.len(), 2);
    assert_eq!(page_first_rows[0], vec![0]);
    assert_ne!(page_first_rows[0], page_first_rows[1]);

    let variable_width_pages = &page_first_rows[1];
    assert_eq!(variable_width_pages.first(), Some(&0));
    let selected_pages = [0, 6, 13]
        .map(|row| variable_width_pages.partition_point(|first_row| *first_row <= row) - 1);
    assert!(
        selected_pages
            .windows(2)
            .all(|pages| pages[1] > pages[0] + 1),
        "selected rows must leave unloaded pages between them: {page_first_rows:?}"
    );

    let stream = builder
        .with_row_selection(RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(5),
            RowSelector::select(1),
            RowSelector::skip(6),
            RowSelector::select(1),
        ]))
        .with_batch_size(2)
        .with_row_selection_policy(RowSelectionPolicy::Mask)
        .build()
        .unwrap();

    let output_schema = stream.schema().clone();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );

    let output = concat_batches(&output_schema, &batches).unwrap();
    let expected = arrow::compute::take(
        input.column(0).as_ref(),
        &UInt32Array::from(vec![0, 6, 13]),
        None,
    )
    .unwrap();
    assert_eq!(output.column(0).to_data(), expected.to_data());
}

#[tokio::test]
async fn test_row_filter() {
    let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
    let b = StringArray::from_iter_values(["1", "2", "3", "4", "5", "6"]);
    let data = RecordBatch::try_from_iter([
        ("a", Arc::new(a) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
    ])
    .unwrap();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), None).unwrap();
    writer.write(&data).unwrap();
    writer.close().unwrap();

    let data: Bytes = buf.into();
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let parquet_schema = metadata.file_metadata().schema_descr_ptr();

    let test = TestReader::new(data);
    let requests = test.requests();

    let a_scalar = StringArray::from_iter_values(["b"]);
    let a_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![0]),
        move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
    );

    let filter = RowFilter::new(vec![Box::new(a_filter)]);

    let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 1]);
    let stream = ParquetRecordBatchStreamBuilder::new(test)
        .await
        .unwrap()
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .with_row_filter(filter)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 2);

    // Filter should have kept only rows with "b" in column 0
    assert_eq!(
        batch.column(0).as_ref(),
        &StringArray::from_iter_values(["b", "b", "b"])
    );
    assert_eq!(
        batch.column(1).as_ref(),
        &StringArray::from_iter_values(["2", "3", "4"])
    );

    // Should only have made 2 requests:
    // * First request fetches data for evaluating the predicate
    // * Second request fetches data for evaluating the projection
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn test_two_row_filters() {
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
    let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), None).unwrap();
    writer.write(&data).unwrap();
    writer.close().unwrap();

    let data: Bytes = buf.into();
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let parquet_schema = metadata.file_metadata().schema_descr_ptr();

    let test = TestReader::new(data);
    let requests = test.requests();

    let a_scalar = StringArray::from_iter_values(["b"]);
    let a_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![0]),
        move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
    );

    let b_scalar = StringArray::from_iter_values(["4"]);
    let b_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![1]),
        move |batch| eq(batch.column(0), &Scalar::new(&b_scalar)),
    );

    let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

    let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);
    let stream = ParquetRecordBatchStreamBuilder::new(test)
        .await
        .unwrap()
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .with_row_filter(filter)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);

    let col = batch.column(0);
    let val = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(val, "b");

    let col = batch.column(1);
    let val = col.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
    assert_eq!(val, 3);

    // Should only have made 3 requests
    // * First request fetches data for evaluating the first predicate
    // * Second request fetches data for evaluating the second predicate
    // * Third request fetches data for evaluating the projection
    assert_eq!(requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn test_row_filter_with_index() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
    let data = Bytes::from(std::fs::read(path).unwrap());

    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let parquet_schema = metadata.file_metadata().schema_descr_ptr();

    assert_eq!(metadata.num_row_groups(), 1);

    let async_reader = TestReader::new(data.clone());

    let a_filter =
        ArrowPredicateFn::new(ProjectionMask::leaves(&parquet_schema, vec![1]), |batch| {
            Ok(batch.column(0).as_boolean().clone())
        });

    let b_scalar = Int8Array::from(vec![2]);
    let b_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![2]),
        move |batch| eq(batch.column(0), &Scalar::new(&b_scalar)),
    );

    let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

    let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let stream = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
        .await
        .unwrap()
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .with_row_filter(filter)
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 730);
}

#[tokio::test]
async fn test_row_filter_nested() {
    let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
    let b = StructArray::from(vec![
        (
            Arc::new(Field::new("aa", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["a", "b", "b", "b", "c", "c"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("bb", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5", "6"])) as ArrayRef,
        ),
    ]);
    let c = Int32Array::from_iter(0..6);
    let data = RecordBatch::try_from_iter([
        ("a", Arc::new(a) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
        ("c", Arc::new(c) as ArrayRef),
    ])
    .unwrap();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), None).unwrap();
    writer.write(&data).unwrap();
    writer.close().unwrap();

    let data: Bytes = buf.into();
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&data)
        .unwrap();
    let parquet_schema = metadata.file_metadata().schema_descr_ptr();

    let test = TestReader::new(data);
    let requests = test.requests();

    let a_scalar = StringArray::from_iter_values(["b"]);
    let a_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![0]),
        move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
    );

    let b_scalar = StringArray::from_iter_values(["4"]);
    let b_filter = ArrowPredicateFn::new(
        ProjectionMask::leaves(&parquet_schema, vec![2]),
        move |batch| {
            // Filter on the second element of the struct.
            let struct_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            eq(struct_array.column(0), &Scalar::new(&b_scalar))
        },
    );

    let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

    let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 3]);
    let stream = ParquetRecordBatchStreamBuilder::new(test)
        .await
        .unwrap()
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .with_row_filter(filter)
        .build()
        .unwrap();

    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);

    let col = batch.column(0);
    let val = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(val, "b");

    let col = batch.column(1);
    let val = col.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
    assert_eq!(val, 3);

    // Should only have made 3 requests
    // * First request fetches data for evaluating the first predicate
    // * Second request fetches data for evaluating the second predicate
    // * Third request fetches data for evaluating the projection
    assert_eq!(requests.lock().unwrap().len(), 3);
}

/// Regression test for adaptive predicate pushdown attempting to read skipped pages.
/// Related issue: https://github.com/apache/arrow-rs/issues/9239
#[tokio::test]
async fn test_predicate_pushdown_with_skipped_pages() {
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::TimeUnit;

    // Time range constants
    const TIME_IN_RANGE_START: i64 = 1_704_092_400_000_000_000;
    const TIME_IN_RANGE_END: i64 = 1_704_110_400_000_000_000;
    const TIME_BEFORE_RANGE: i64 = 1_704_078_000_000_000_000;

    // Create test data: 2 row groups, 300 rows each
    // "tag" column: 'a', 'b', 'c' (100 rows each, sorted)
    // "time" column: alternating in-range/out-of-range timestamps
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(300))
        .set_data_page_row_count_limit(33)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

    // Write 2 row groups
    for _ in 0..2 {
        for (tag_idx, tag) in ["a", "b", "c"].iter().enumerate() {
            let times: Vec<i64> = (0..100)
                .map(|j| {
                    let row_idx = tag_idx * 100 + j;
                    if row_idx % 2 == 0 {
                        TIME_IN_RANGE_START + (j as i64 * 1_000_000)
                    } else {
                        TIME_BEFORE_RANGE + (j as i64 * 1_000_000)
                    }
                })
                .collect();
            let tags: Vec<&str> = (0..100).map(|_| *tag).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(times)) as ArrayRef,
                    Arc::new(StringArray::from(tags)) as ArrayRef,
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let buffer = Bytes::from(buffer);
    // Read back with various page index policies, should get the same answer with all
    for policy in [
        PageIndexPolicy::Skip,
        PageIndexPolicy::Optional,
        PageIndexPolicy::Required,
    ] {
        println!("Testing with page index policy: {:?}", policy);
        let reader = TestReader::new(buffer.clone());
        let options = ArrowReaderOptions::default().with_page_index_policy(policy);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
            .await
            .unwrap();

        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
        let num_row_groups = builder.metadata().num_row_groups();

        // Initial selection: skip middle 100 rows (tag='b') per row group
        let mut selectors = Vec::new();
        for _ in 0..num_row_groups {
            selectors.push(RowSelector::select(100));
            selectors.push(RowSelector::skip(100));
            selectors.push(RowSelector::select(100));
        }
        let selection = RowSelection::from(selectors);

        // Predicate 1: time >= START
        let time_gte_predicate =
            ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), |batch| {
                let col = batch.column(0).as_primitive::<TimestampNanosecondType>();
                Ok(BooleanArray::from_iter(
                    col.iter().map(|t| t.map(|v| v >= TIME_IN_RANGE_START)),
                ))
            });

        // Predicate 2: time < END
        let time_lt_predicate =
            ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), |batch| {
                let col = batch.column(0).as_primitive::<TimestampNanosecondType>();
                Ok(BooleanArray::from_iter(
                    col.iter().map(|t| t.map(|v| v < TIME_IN_RANGE_END)),
                ))
            });

        let row_filter = RowFilter::new(vec![
            Box::new(time_gte_predicate),
            Box::new(time_lt_predicate),
        ]);

        // Output projection: Only tag column (time not in output)
        let projection = ProjectionMask::roots(&schema_descr, [1]);

        let stream = builder
            .with_row_filter(row_filter)
            .with_row_selection(selection)
            .with_projection(projection)
            .build()
            .unwrap();

        // Stream should complete without error and the same results
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_columns(), 1);
        let expected = StringArray::from_iter_values(
            std::iter::repeat_n("a", 50)
                .chain(std::iter::repeat_n("c", 50))
                .chain(std::iter::repeat_n("a", 50))
                .chain(std::iter::repeat_n("c", 50)),
        );
        assert_eq!(batch.column(0).as_string(), &expected);
    }
}

/// Regression test: Auto resolution and loaded row ranges must be refreshed for
/// each predicate's current selection and projection.
///
/// Scenario:
/// - Dense initial RowSelection (alternating select/skip) covers all pages → Auto resolves to Mask
/// - Predicate 1 evaluates on column A, narrows selection to skip middle pages
/// - Predicate 2's column B is fetched sparsely with the narrowed selection (missing middle pages)
/// - Predicate 2 remains safe with Mask because its loaded row ranges are recomputed for column B
#[tokio::test]
async fn test_multi_predicate_auto_mask_with_sparse_pages() {
    // 300 rows, 1 row group, 100 rows per page (3 pages)
    let num_rows = 300usize;
    let rows_per_page = 100;

    let schema = Arc::new(Schema::new(vec![
        Field::new("filter_col", DataType::Int32, false),
        Field::new("value_col", DataType::Int32, false),
    ]));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(num_rows))
        .set_data_page_row_count_limit(rows_per_page)
        .set_write_batch_size(rows_per_page)
        .set_dictionary_enabled(false)
        .build();

    // filter_col: 0 for first and last 100 rows, 1 for middle 100 rows
    // value_col: just row index
    let filter_values: Vec<i32> = (0..num_rows as i32)
        .map(|i| if (100..200).contains(&i) { 1 } else { 0 })
        .collect();
    let value_values: Vec<i32> = (0..num_rows as i32).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(filter_values)) as ArrayRef,
            Arc::new(Int32Array::from(value_values)) as ArrayRef,
        ],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let buffer = Bytes::from(buffer);

    let reader = TestReader::new(buffer);
    let options = ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap();

    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    // Dense initial selection: Select(1), Skip(1) repeated → triggers Mask strategy
    // Covers all pages since every page has selected rows
    let selectors: Vec<RowSelector> = (0..num_rows / 2)
        .flat_map(|_| vec![RowSelector::select(1), RowSelector::skip(1)])
        .collect();
    let selection = RowSelection::from(selectors);

    // Predicate 1 on filter_col: keeps only rows where filter_col == 0
    // (first 100 and last 100 rows). After this, middle page is excluded.
    let pred1 = ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), |batch| {
        let col = batch.column(0).as_primitive::<Int32Type>();
        Ok(BooleanArray::from_iter(
            col.iter().map(|v| v.map(|val| val == 0)),
        ))
    });

    // Predicate 2 on value_col: keeps rows where value_col < 250
    // This column is fetched AFTER predicate 1 narrows the selection.
    // Its sparse data will be missing the middle page.
    let predicate_batch_sizes = Arc::new(Mutex::new(Vec::new()));
    let observed_batch_sizes = Arc::clone(&predicate_batch_sizes);
    let pred2 = ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [1]), move |batch| {
        observed_batch_sizes.lock().unwrap().push(batch.num_rows());
        let col = batch.column(0).as_primitive::<Int32Type>();
        Ok(BooleanArray::from_iter(
            col.iter().map(|v| v.map(|val| val < 250)),
        ))
    });

    let row_filter = RowFilter::new(vec![Box::new(pred1), Box::new(pred2)]);
    let projection = ProjectionMask::roots(&schema_descr, [0, 1]);

    let stream = builder
        .with_row_filter(row_filter)
        .with_row_selection(selection)
        .with_projection(projection)
        .with_max_predicate_cache_size(0)
        .build()
        .unwrap();

    // Without loaded-range-aware Mask reads, this panics with an invalid sparse-page offset.
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(*predicate_batch_sizes.lock().unwrap(), vec![100]);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![75]
    );

    let batch = concat_batches(&batches[0].schema(), &batches).unwrap();

    // Verify results: rows where filter_col==0 AND value_col<250 AND original alternating selection
    // That's even-indexed rows in [0,100) with value<250 → rows 0,2,4,...,98 (50 rows)
    // Plus even-indexed rows in [200,250) with value<250 → rows 200,202,...,248 (25 rows)
    assert_eq!(batch.num_rows(), 75);
}

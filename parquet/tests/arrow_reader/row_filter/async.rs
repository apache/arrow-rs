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
use std::sync::Arc;

use arrow::{
    array::AsArray,
    compute::{concat_batches, kernels::cmp::eq, or},
    datatypes::TimestampNanosecondType,
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
    schema::types::ColumnPath,
};

#[tokio::test]
async fn test_row_filter_full_page_skip_is_handled_async() {
    let first_value: i64 = 1111;
    let last_value: i64 = 9999;
    let num_rows: usize = 12;

    // build data with row selection average length 4
    // The result would be (1111 XXXX) ... (4 page in the middle)... (XXXX 9999)
    // The Row Selection would be [1111, (skip 10), 9999]
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
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

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data.clone()),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();
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

    let predicate = make_predicate(filter_mask.clone());

    // The batch size is set to 12 to read all rows in one go after filtering
    // If the Reader chooses mask to handle filter, it might cause panic because the mid 4 pages may not be decoded.
    let stream = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data.clone()),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap()
    .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
    .with_batch_size(12)
    .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 32 })
    .build()
    .unwrap();

    let schema = stream.schema().clone();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let result = concat_batches(&schema, &batches).unwrap();
    assert_eq!(result.num_rows(), 2);
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

/// Regression test for adaptive selection switching to mask materialization while
/// evaluating a predicate that references multiple columns with different page boundaries.
#[tokio::test]
async fn test_complex_predicate_pushdown_with_skipped_pages() {
    const NUM_ROWS: usize = 240;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("int_flag", DataType::Int8, false),
        Field::new("text_flag", DataType::Utf8, false),
    ]));

    let huge_true = "T".repeat(128);
    let huge_false = "F".repeat(128);

    let ids = Int32Array::from_iter_values(0..NUM_ROWS as i32);
    let int_flag = Int8Array::from_iter_values((0..NUM_ROWS).map(|i| (i % 2 == 0) as i8));
    let text_flag = StringArray::from_iter_values((0..NUM_ROWS).map(|i| {
        if i % 3 == 0 {
            huge_true.as_str()
        } else {
            huge_false.as_str()
        }
    }));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(int_flag), Arc::new(text_flag)],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_write_batch_size(1)
        .set_data_page_size_limit(512)
        .set_data_page_row_count_limit(128)
        .set_column_data_page_size_limit(ColumnPath::from("text_flag"), 256)
        .set_column_dictionary_enabled(ColumnPath::from("text_flag"), false)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();

    let schema_descr = builder.parquet_schema().clone();

    // Start with a sparse RLE row selection, then apply a predicate that produces
    // a dense, fragmented mask.
    let selection = RowSelection::from(vec![
        RowSelector::select(60),
        RowSelector::skip(120),
        RowSelector::select(60),
    ]);

    let huge_true_scalar = StringArray::from_iter_values([huge_true.as_str()]);
    let predicate =
        ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [1, 2]), move |batch| {
            let int_true = eq(batch.column(0), &Int8Array::new_scalar(1))?;
            let text_true = eq(batch.column(1), &Scalar::new(&huge_true_scalar))?;
            or(&int_true, &text_true)
        });

    let stream = builder
        .with_row_selection(selection)
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_projection(ProjectionMask::roots(&schema_descr, [0]))
        .with_batch_size(NUM_ROWS)
        .with_row_selection_policy(RowSelectionPolicy::Mask)
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();

    let expected_ids =
        Int32Array::from_iter_values((0..60).chain(180..240).filter(|i| i % 2 == 0 || i % 3 == 0));

    assert_eq!(result.num_columns(), 1);
    assert_eq!(
        result
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>(),
        &expected_ids
    );
}

/// Regression test for mask materialization when a manual selection skips full pages
/// and projected columns have different page boundaries.
#[tokio::test]
async fn test_mask_selection_projection_with_skipped_pages() {
    const NUM_ROWS: usize = 240;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("int_flag", DataType::Int8, false),
        Field::new("text_flag", DataType::Utf8, false),
    ]));

    let huge_true = "T".repeat(128);
    let huge_false = "F".repeat(128);

    let ids = Int32Array::from_iter_values(0..NUM_ROWS as i32);
    let int_flag = Int8Array::from_iter_values((0..NUM_ROWS).map(|i| (i % 2 == 0) as i8));
    let text_flag = StringArray::from_iter_values((0..NUM_ROWS).map(|i| {
        if i % 3 == 0 {
            huge_true.as_str()
        } else {
            huge_false.as_str()
        }
    }));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(int_flag), Arc::new(text_flag)],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_write_batch_size(1)
        .set_data_page_size_limit(512)
        .set_data_page_row_count_limit(128)
        .set_column_data_page_size_limit(ColumnPath::from("text_flag"), 256)
        .set_column_dictionary_enabled(ColumnPath::from("text_flag"), false)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();

    let schema_descr = builder.parquet_schema().clone();
    let selection = RowSelection::from(vec![
        RowSelector::select(60),
        RowSelector::skip(120),
        RowSelector::select(60),
    ]);

    let stream = builder
        .with_row_selection(selection)
        .with_row_selection_policy(RowSelectionPolicy::Mask)
        .with_projection(ProjectionMask::roots(&schema_descr, [0, 2]))
        .with_batch_size(NUM_ROWS)
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();

    let expected_ids = Int32Array::from_iter_values((0..60).chain(180..240));

    assert_eq!(result.num_columns(), 2);
    assert_eq!(
        result
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>(),
        &expected_ids
    );
    assert_eq!(result.num_rows(), expected_ids.len());
}

/// Regression test for mask materialization during filtering when predicates span
/// columns with different page boundaries.
#[tokio::test]
async fn test_mask_selection_multi_col_predicate_with_skipped_pages() {
    const NUM_ROWS: usize = 240;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("int_flag", DataType::Int8, false),
        Field::new("text_flag", DataType::Utf8, false),
    ]));

    let huge_true = "T".repeat(128);
    let huge_false = "F".repeat(128);

    let ids = Int32Array::from_iter_values(0..NUM_ROWS as i32);
    let int_flag = Int8Array::from_iter_values((0..NUM_ROWS).map(|i| (i % 2 == 0) as i8));
    let text_flag = StringArray::from_iter_values((0..NUM_ROWS).map(|i| {
        if i % 3 == 0 {
            huge_true.as_str()
        } else {
            huge_false.as_str()
        }
    }));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(int_flag), Arc::new(text_flag)],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_write_batch_size(1)
        .set_data_page_size_limit(512)
        .set_data_page_row_count_limit(128)
        .set_column_data_page_size_limit(ColumnPath::from("text_flag"), 256)
        .set_column_dictionary_enabled(ColumnPath::from("text_flag"), false)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let data = Bytes::from(buffer);

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        TestReader::new(data),
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();

    let schema_descr = builder.parquet_schema().clone();
    let selection = RowSelection::from(vec![
        RowSelector::select(60),
        RowSelector::skip(120),
        RowSelector::select(60),
    ]);

    let huge_true_scalar = StringArray::from_iter_values([huge_true.as_str()]);
    let predicate =
        ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [1, 2]), move |batch| {
            let int_true = eq(batch.column(0), &Int8Array::new_scalar(1))?;
            let text_true = eq(batch.column(1), &Scalar::new(&huge_true_scalar))?;
            or(&int_true, &text_true)
        });

    let stream = builder
        .with_row_selection(selection)
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_projection(ProjectionMask::roots(&schema_descr, [0]))
        .with_row_selection_policy(RowSelectionPolicy::Mask)
        .with_batch_size(NUM_ROWS)
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();

    let expected_ids =
        Int32Array::from_iter_values((0..60).chain(180..240).filter(|i| i % 2 == 0 || i % 3 == 0));

    assert_eq!(result.num_columns(), 1);
    assert_eq!(
        result
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>(),
        &expected_ids
    );
}

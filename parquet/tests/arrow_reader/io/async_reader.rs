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

//! Tests for the async reader ([`ParquetRecordBatchStreamBuilder`])

use crate::io::{
    LogEntry, OperationLog, TestParquetFile, filter_a_175_b_625, filter_b_575_625, filter_b_false,
    test_file, test_options,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::Result;
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::Arc;

#[tokio::test]
async fn test_read_entire_file() {
    // read entire file without any filtering or projection
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "  Row Group 0, column 'c': MultiPage(dictionary_page: true, data_pages: [0, 1])  (7346 bytes, 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        "  Row Group 1, column 'c': MultiPage(dictionary_page: true, data_pages: [0, 1])  (7456 bytes, 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_read_single_group() {
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options())
        .await
        // read only second row group
        .with_row_groups(vec![1]);

    // Expect to see only IO for Row Group 1. Should see no IO for Row Group 0.
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 1, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "  Row Group 1, column 'c': MultiPage(dictionary_page: true, data_pages: [0, 1])  (7456 bytes, 1 requests) [data]",
        ]
    "#);
}

#[tokio::test]
async fn test_read_single_column() {
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder.with_projection(ProjectionMask::columns(&schema_descr, ["b"]));
    // Expect to see only IO for column "b". Should see no IO for columns "a" or "c".
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        ]
    "#);
}

#[tokio::test]
async fn test_read_row_selection() {
    // There are 400 total rows spread across 4 data pages (100 rows each)
    // select rows 175..225 (i.e. DataPage(1) of row group 0 and DataPage(0) of row group 1)
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(175),
            RowSelector::select(50),
        ]));

    // Expect to see only data IO for one page for each column for each row group
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "  Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Read Multi:",
        "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "  Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_read_limit() {
    // There are 400 total rows spread across 4 data pages (100 rows each)
    // a limit of 125 rows should only fetch the first two data pages (DataPage(0) and DataPage(1)) from row group 0
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
        .with_limit(125);

    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
    [
        "Get Provided Metadata",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Read Multi:",
        "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
    ]
    "#);
}

#[tokio::test]
async fn test_read_single_row_filter() {
    // Values from column "b" range 400..799
    // filter  "b" > 575 and < than 625
    // (last data page in Row Group 0 and first DataPage in Row Group 1)
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_575_625(&schema_descr));

    // Expect to see I/O for column b in both row groups to evaluate filter,
    // then a single pages for the "a" column in each row group
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
            "  Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
            "  Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        ]
    "#);
}

#[tokio::test]
async fn test_read_single_row_filter_no_page_index() {
    // Values from column "b" range 400..799
    // Apply a filter  "b" > 575 and <less> than 625
    // (last data page in Row Group 0 and first DataPage in Row Group 1)
    let test_file = test_file();
    let options = test_options().with_page_index(false);
    let builder = async_builder(&test_file, options).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_575_625(&schema_descr));

    // Since we don't have the page index, expect to see:
    // 1. I/O for all pages of column b to evaluate the filter
    // 2. IO for all pages of column a as the reader doesn't know where the page
    //    boundaries are so needs to scan them.
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        ]
    "#);
}

#[tokio::test]
async fn test_read_multiple_row_filter() {
    // Values in column "a" range 0..399
    // Values in column "b" range 400..799
    // First filter: "a" > 175  (last data page in Row Group 0)
    // Second filter: "b" < 625 (last data page in Row Group 0 and first DataPage in RowGroup 1)
    // Read column "c"
    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["c"]))
        .with_row_filter(filter_a_175_b_625(&schema_descr));

    // Expect that we will see
    // 1. IO for all pages of column A (to evaluate the first filter)
    // 2. IO for pages of column b that passed the first filter (to evaluate the second filter)
    // 3. IO after reader is built only for column c for the rows that passed both filters
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 0, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
            "  Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
            "Read Multi:",
            "  Row Group 0, column 'c': DictionaryPage   (7107 bytes, 1 requests) [data]",
            "  Row Group 0, column 'c': DataPage(1)      (126 bytes , 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'a': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
            "  Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
            "  Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'c': DictionaryPage   (7217 bytes, 1 requests) [data]",
            "  Row Group 1, column 'c': DataPage(0)      (113 bytes , 1 requests) [data]",
        ]
    "#);
}

#[tokio::test]
async fn test_read_single_row_filter_all() {
    // Apply a filter that filters out all rows

    let test_file = test_file();
    let builder = async_builder(&test_file, test_options()).await;
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_false(&schema_descr));

    // Expect to see reads for column "b" to evaluate the filter, but no reads
    // for column "a" as no rows pass the filter
    insta::assert_debug_snapshot!(run(
        &test_file,
        builder).await, @r#"
        [
            "Get Provided Metadata",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Read Multi:",
            "  Row Group 0, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
            "Read Multi:",
            "  Row Group 1, column 'b': MultiPage(dictionary_page: true, data_pages: [0, 1])  (1856 bytes, 1 requests) [data]",
        ]
    "#);
}

/// Return a [`ParquetRecordBatchStreamBuilder`] for reading this file
async fn async_builder(
    test_file: &TestParquetFile,
    options: ArrowReaderOptions,
) -> ParquetRecordBatchStreamBuilder<RecordingAsyncFileReader> {
    let parquet_meta_data = if options.page_index() {
        Arc::clone(test_file.parquet_metadata())
    } else {
        // strip out the page index from the metadata
        let metadata = test_file
            .parquet_metadata()
            .as_ref()
            .clone()
            .into_builder()
            .set_column_index(None)
            .set_offset_index(None)
            .build();
        Arc::new(metadata)
    };

    let reader = RecordingAsyncFileReader {
        bytes: test_file.bytes().clone(),
        ops: Arc::clone(test_file.ops()),
        parquet_meta_data,
    };

    ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap()
}

/// Build the reader from the specified builder and read all batches from it,
/// and return the operations log.
async fn run(
    test_file: &TestParquetFile,
    builder: ParquetRecordBatchStreamBuilder<RecordingAsyncFileReader>,
) -> Vec<String> {
    let ops = test_file.ops();
    ops.add_entry(LogEntry::event("Builder Configured"));
    let mut stream = builder.build().unwrap();
    ops.add_entry(LogEntry::event("Reader Built"));
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(_) => {}
            Err(e) => panic!("Error reading batch: {e}"),
        }
    }
    ops.snapshot()
}

struct RecordingAsyncFileReader {
    bytes: Bytes,
    ops: Arc<OperationLog>,
    parquet_meta_data: Arc<ParquetMetaData>,
}

impl AsyncFileReader for RecordingAsyncFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let ops = Arc::clone(&self.ops);
        let data = self
            .bytes
            .slice(range.start as usize..range.end as usize)
            .clone();

        // translate to usize from u64
        let logged_range = Range {
            start: range.start as usize,
            end: range.end as usize,
        };
        async move {
            ops.add_entry_for_range(&logged_range);
            Ok(data)
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        let ops = Arc::clone(&self.ops);
        let datas = ranges
            .iter()
            .map(|range| {
                self.bytes
                    .slice(range.start as usize..range.end as usize)
                    .clone()
            })
            .collect::<Vec<_>>();
        // translate to usize from u64
        let logged_ranges = ranges
            .into_iter()
            .map(|r| Range {
                start: r.start as usize,
                end: r.end as usize,
            })
            .collect::<Vec<_>>();

        async move {
            ops.add_entry_for_ranges(&logged_ranges);
            Ok(datas)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        let ops = Arc::clone(&self.ops);
        let parquet_meta_data = Arc::clone(&self.parquet_meta_data);
        async move {
            ops.add_entry(LogEntry::GetProvidedMetadata);
            Ok(parquet_meta_data)
        }
        .boxed()
    }
}

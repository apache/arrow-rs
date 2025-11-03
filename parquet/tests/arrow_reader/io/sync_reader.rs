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

//! Tests for the sync reader - [`ParquetRecordBatchReaderBuilder`]

use crate::io::{
    LogEntry, OperationLog, TestParquetFile, filter_a_175_b_625, filter_b_575_625, filter_b_false,
    test_file, test_options,
};

use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::file::reader::{ChunkReader, Length};
use std::io::Read;
use std::sync::Arc;

#[test]
fn test_read_entire_file() {
    // read entire file without any filtering or projection
    let test_file = test_file();
    // Expect to see IO for all data pages for each row group and column
    let builder = sync_builder(&test_file, test_options());
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 0, column 'c': DictionaryPage   (7107 bytes, 1 requests) [data]",
        "Row Group 0, column 'c': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'c': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'c': DictionaryPage   (7217 bytes, 1 requests) [data]",
        "Row Group 1, column 'c': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'c': DataPage(1)      (126 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_single_group() {
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options()).with_row_groups(vec![1]); // read only second row group

    // Expect to see only IO for Row Group 1. Should see no IO for Row Group 0.
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'c': DictionaryPage   (7217 bytes, 1 requests) [data]",
        "Row Group 1, column 'c': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'c': DataPage(1)      (126 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_single_column() {
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder.with_projection(ProjectionMask::columns(&schema_descr, ["b"]));
    // Expect to see only IO for column "b". Should see no IO for columns "a" or "c".
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_single_column_no_page_index() {
    let test_file = test_file();
    let options = test_options().with_page_index(false);
    let builder = sync_builder(&test_file, options);
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder.with_projection(ProjectionMask::columns(&schema_descr, ["b"]));
    // Expect to see only IO for column "b", should see no IO for columns "a" or "c".
    //
    // Note that we need to read all data page headers to find the pages for column b
    // so there are many more small reads than in the test_read_single_column test above
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 requests) [header]",
        "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 requests) [header]",
        "Row Group 0, column 'b': DataPage(0)      (93 bytes  , 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 requests) [header]",
        "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 requests) [header]",
        "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 requests) [header]",
        "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 requests) [header]",
        "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_row_selection() {
    // There are 400 total rows spread across 4 data pages (100 rows each)
    // select rows 175..225 (i.e. DataPage(1) of row group 0 and DataPage(0) of row group 1)
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder
        .with_projection(
            // read both "a" and "b"
            ProjectionMask::columns(&schema_descr, ["a", "b"]),
        )
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(175),
            RowSelector::select(50),
        ]));

    // Expect to see only data IO for one page for each column for each row group
    // Note the data page headers for all pages need to be read to find the correct pages
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_limit() {
    // There are 400 total rows spread across 4 data pages (100 rows each)
    // a limit of 125 rows should only fetch the first two data pages (DataPage(0) and DataPage(1)) from row group 0
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
        .with_limit(125);

    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Event: Reader Built",
        "Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_single_row_filter() {
    // Values from column "b" range 400..799
    // filter  "b" > 575 and < 625
    // (last data page in Row Group 0 and first DataPage in Row Group 1)
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(
            // read both "a" and "b"
            ProjectionMask::columns(&schema_descr, ["a", "b"]),
        )
        // "b" > 575 and "b" < 625
        .with_row_filter(filter_b_575_625(&schema_descr));

    // Expect to see I/O for column b in both row groups and then reading just a
    // single pages for a in each row group
    //
    // Note there is significant IO that happens during the construction of the
    // reader (between "Builder Configured" and "Reader Built")
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Event: Reader Built",
        "Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_multiple_row_filter() {
    // Values in column "a" range 0..399
    // Values in column "b" range 400..799
    // First filter: "a" > 175  (last data page in Row Group 0)
    // Second filter: "b" < 625 (last data page in Row Group 0 and first DataPage in RowGroup 1)
    // Read column "c"
    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(
            ProjectionMask::columns(&schema_descr, ["c"]), // read "c"
        )
        // a > 175 and b < 625
        .with_row_filter(filter_a_175_b_625(&schema_descr));

    // Expect that we will see
    // 1. IO for all pages of column A
    // 2. IO for pages of column b that passed 1.
    // 3. IO after reader is built only for column c
    //
    // Note there is significant IO that happens during the construction of the
    // reader (between "Builder Configured" and "Reader Built")
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Row Group 0, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'a': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Event: Reader Built",
        "Row Group 0, column 'c': DictionaryPage   (7107 bytes, 1 requests) [data]",
        "Row Group 0, column 'c': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'c': DictionaryPage   (7217 bytes, 1 requests) [data]",
        "Row Group 1, column 'c': DataPage(0)      (113 bytes , 1 requests) [data]",
    ]
    "#);
}

#[test]
fn test_read_single_row_filter_all() {
    // Apply a filter that entirely filters out rows based on a predicate from one column
    // should not read any data pages for any other column

    let test_file = test_file();
    let builder = sync_builder(&test_file, test_options());
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(filter_b_false(&schema_descr));

    // Expect to see the Footer and Metadata, then I/O for column b
    // in both row groups but then nothing for column "a"
    // since the row filter entirely filters out all rows.
    //
    // Note that all IO that happens during the construction of the reader
    // (between "Builder Configured" and "Reader Built")
    insta::assert_debug_snapshot!(run(&test_file, builder),
        @r#"
    [
        "Footer: 8 bytes",
        "Metadata: 1162",
        "UNKNOWN: 22230..22877 (maybe Page Index)",
        "Event: Builder Configured",
        "Row Group 0, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 0, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DictionaryPage   (1617 bytes, 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(0)      (113 bytes , 1 requests) [data]",
        "Row Group 1, column 'b': DataPage(1)      (126 bytes , 1 requests) [data]",
        "Event: Reader Built",
    ]
    "#);
}

/// Return a [`ParquetRecordBatchReaderBuilder`] for reading this file
fn sync_builder(
    test_file: &TestParquetFile,
    options: ArrowReaderOptions,
) -> ParquetRecordBatchReaderBuilder<RecordingChunkReader> {
    let reader = RecordingChunkReader {
        inner: test_file.bytes().clone(),
        ops: Arc::clone(test_file.ops()),
    };
    ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)
        .expect("ParquetRecordBatchReaderBuilder")
}

/// build the reader, and read all batches from it, returning the recorded IO operations
fn run(
    test_file: &TestParquetFile,
    builder: ParquetRecordBatchReaderBuilder<RecordingChunkReader>,
) -> Vec<String> {
    let ops = test_file.ops();
    ops.add_entry(LogEntry::event("Builder Configured"));
    let reader = builder.build().unwrap();
    ops.add_entry(LogEntry::event("Reader Built"));
    for batch in reader {
        match batch {
            Ok(_) => {}
            Err(e) => panic!("Error reading batch: {e}"),
        }
    }
    ops.snapshot()
}

/// Records IO operations on an in-memory chunk reader
struct RecordingChunkReader {
    inner: Bytes,
    ops: Arc<OperationLog>,
}

impl Length for RecordingChunkReader {
    fn len(&self) -> u64 {
        self.inner.len() as u64
    }
}

impl ChunkReader for RecordingChunkReader {
    type T = RecordingStdIoReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let reader = RecordingStdIoReader {
            start: start as usize,
            inner: self.inner.clone(),
            ops: Arc::clone(&self.ops),
        };
        Ok(reader)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = start as usize;
        let range = start..start + length;
        self.ops.add_entry_for_range(&range);
        Ok(self.inner.slice(start..start + length))
    }
}

/// Wrapper around a `Bytes` object that implements `Read`
struct RecordingStdIoReader {
    /// current offset in the inner `Bytes` that this reader is reading from
    start: usize,
    inner: Bytes,
    ops: Arc<OperationLog>,
}

impl Read for RecordingStdIoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remain = self.inner.len() - self.start;
        let start = self.start;
        let read_length = buf.len().min(remain);
        let read_range = start..start + read_length;

        self.ops.add_entry_for_range(&read_range);

        buf.copy_from_slice(self.inner.slice(read_range).as_ref());
        // Update the inner position
        self.start += read_length;
        Ok(read_length)
    }
}

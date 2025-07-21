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

//! Tests for IO read patterns in the Parquet Reader
//!
//! These tests
//! 1. Create a temporary Parquet file with a known row group structure
//! 2. Reads data from that file using the Arrow Parquet Reader, recording the IO operations
//! 3. Asserts the expected IO patterns based on the read operations

use arrow::compute::and;
use arrow::compute::kernels::cmp::{gt, lt};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringViewArray};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection,
    RowSelector,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::data_type::AsBytes;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetOffsetIndex};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::FOOTER_SIZE;
use parquet::format::PageLocation;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::Read;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[test]
fn test_read_entire_file() {
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    // Expect to see IO for all data pages for each row group and column
    let builder = test_file.sync_builder(options);
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Row Group 0, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 0, column 'c': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'c': DictionaryPage   (7090 bytes, 1 reads ) [data]",
            "Row Group 0, column 'c': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'c': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'c': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'c': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'c': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'c': DictionaryPage   (7200 bytes, 1 reads ) [data]",
            "Row Group 1, column 'c': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'c': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'c': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'c': DataPage(1)      (106 bytes , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_single_group() {
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options).with_row_groups(vec![1]); // read only second row group

    // Expect to see only IO for Row Group 1.
    // Should see no IO for Row Group 0.
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Row Group 1, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'c': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'c': DictionaryPage   (7200 bytes, 1 reads ) [data]",
            "Row Group 1, column 'c': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'c': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'c': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'c': DataPage(1)      (106 bytes , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_single_column() {
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options);
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
    let builder = builder.with_projection(ProjectionMask::columns(&schema_descr, ["b"]));
    // Expect to see only IO for column "b"
    // Should see no IO for columns "a" or "c".
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_row_selection() {
    // There are 400 total rows spread across 4 data pages (100 rows each)
    // select rows 175..225 (i.e. DataPage(1) of row group 0 and DataPage(0) of row group 1)
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options);
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

    // Expect to see only data IO for one page from each
    // column from each row group
    // TODO why is why are the data page headers read for row group 0?
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Event: Reader Built",
            "Row Group 0, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_single_row_filter() {
    // Values from column "b" range 400..799
    // Apply a filter  "b" > 575 and less than 625
    // (last data page in Row Group 0 and first DataPage in Row Group 1)
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options);
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    // "b" > 575 and "b" < 625
    let row_filter = ArrowPredicateFn::new(
        ProjectionMask::columns(&schema_descr, ["b"]),
        |batch: RecordBatch| {
            let scalar_575 = Int64Array::new_scalar(575);
            let scalar_625 = Int64Array::new_scalar(625);
            let column = batch.column(0).as_primitive::<Int64Type>();
            and(&gt(column, &scalar_575)?, &lt(column, &scalar_625)?)
        },
    );

    let builder = builder
        .with_projection(
            // read both "a" and "b"
            ProjectionMask::columns(&schema_descr, ["a", "b"]),
        )
        .with_row_filter(RowFilter::new(vec![Box::new(row_filter)]));

    // Expect to see I/O for column b in both row groups and then reading just a
    // single pages for a in each row group
    //
    // Note there is significant IO that happens during the construction of the
    // reader.
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Event: Reader Built",
            "Row Group 0, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_multiple_row_filter() {
    // Values in column "a" range 0..399
    // Values in column "b" range 400..799
    // First filter: "a" > 175  (last data page in Row Group 0)
    // Second filter: "b" < 625 (last data page in Row Group 0 and first DataPage in RowGroup 1)
    // Read column "c"
    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options);
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    // a > 175
    let row_filter_a = ArrowPredicateFn::new(
        ProjectionMask::columns(&schema_descr, ["a"]),
        |batch: RecordBatch| {
            let scalar_175 = Int64Array::new_scalar(175);
            let column = batch.column(0).as_primitive::<Int64Type>();
            gt(column, &scalar_175)
        },
    );

    // b < 625
    let row_filter_b = ArrowPredicateFn::new(
        ProjectionMask::columns(&schema_descr, ["b"]),
        |batch: RecordBatch| {
            let scalar_625 = Int64Array::new_scalar(625);
            let column = batch.column(0).as_primitive::<Int64Type>();
            lt(column, &scalar_625)
        },
    );

    let builder = builder
        .with_projection(
            ProjectionMask::columns(&schema_descr, ["c"]), // read "c"
        )
        .with_row_filter(RowFilter::new(vec![
            Box::new(row_filter_a),
            Box::new(row_filter_b),
        ]));

    // Expect that we will see
    // 1. IO for all pages of column A
    // 2. IO for pages of column b that passed 1.
    // 3. IO after reader is built only for column c
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Row Group 0, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'a': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'a': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'a': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'a': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Event: Reader Built",
            "Row Group 0, column 'c': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'c': DictionaryPage   (7090 bytes, 1 reads ) [data]",
            "Row Group 0, column 'c': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'c': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'c': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'c': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'c': DictionaryPage   (7200 bytes, 1 reads ) [data]",
            "Row Group 1, column 'c': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'c': DataPage(0)      (93 bytes  , 1 reads ) [data]",
        ],
    );
}

#[test]
fn test_read_single_row_filter_all() {
    // Apply a filter that entirely filters out rows based on a predicate from one column
    // should not read any data pages for any other column

    let test_file = test_file();
    let options = ArrowReaderOptions::default();
    let builder = test_file.sync_builder(options);
    let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

    // predicate selects  "b"
    let row_filter = ArrowPredicateFn::new(
        ProjectionMask::columns(&schema_descr, ["b"]),
        |batch: RecordBatch| {
            let result =
                BooleanArray::from_iter(std::iter::repeat_n(Some(false), batch.num_rows()));
            Ok(result)
        },
    );

    let builder = builder
        .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
        .with_row_filter(RowFilter::new(vec![Box::new(row_filter)]));

    // Expect to see the Footer and Metadata, then I/O for column b
    // in both row groups but then nothing for column "a"
    // since the row filter entirely filters out all rows.
    //
    // Note there is significant IO that happens during the construction of the
    // reader.
    test_file.run_test(
        builder,
        vec![
            "Footer: 8 bytes",
            "Metadata: 1162",
            "Event: Builder Configured",
            "Row Group 0, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 0, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 0, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 0, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Row Group 1, column 'b': DictionaryPage   (17 bytes  , 17 reads) [header]",
            "Row Group 1, column 'b': DictionaryPage   (1600 bytes, 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(0)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(0)      (93 bytes  , 1 reads ) [data]",
            "Row Group 1, column 'b': DataPage(1)      (20 bytes  , 20 reads) [header]",
            "Row Group 1, column 'b': DataPage(1)      (106 bytes , 1 reads ) [data]",
            "Event: Reader Built",
        ],
    );
}

// test with multiple row filters

// ------------------------------------
// Supporting Test Infrastructure
// ------------------------------------

/// Create a new `TestParquetFile` with:
/// 3 columns: "a", "b", "c"
///
/// 2 row groups, each with 200 rows
/// each data page has 100 rows
///
/// Values of column "a" are 0..399
/// Values of column "b" are 400..799
/// Values of column "c" are alternating strings of length 12 and longer
fn test_file() -> TestParquetFile {
    // Input batch has 400 rows, with 3 columns: "a", "b", "c"
    // Note c is a different types (so the data page sizes will be different)
    let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
    let b: ArrayRef = Arc::new(Int64Array::from_iter_values(400..800));
    let c: ArrayRef = Arc::new(StringViewArray::from_iter_values((0..400).map(|i| {
        if i % 2 == 0 {
            format!("string_{i}")
        } else {
            format!("A string larger than 12 bytes and thus not inlined {i}")
        }
    })));

    let input_batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

    let mut output = Vec::new();

    let writer_options = WriterProperties::builder()
        .set_max_row_group_size(200)
        .set_data_page_row_count_limit(100)
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut output, input_batch.schema(), Some(writer_options)).unwrap();

    // since the limits are only enforced on batch boundaries, write the input
    // batch in chunks of 50
    let mut row_remain = input_batch.num_rows();
    while row_remain > 0 {
        let chunk_size = row_remain.min(50);
        let chunk = input_batch.slice(input_batch.num_rows() - row_remain, chunk_size);
        writer.write(&chunk).unwrap();
        row_remain -= chunk_size;
    }
    writer.close().unwrap();
    TestParquetFile::new(Bytes::from(output))
}

/// A test parquet file and its layout.
struct TestParquetFile {
    bytes: Bytes,
    /// The operation log for IO operations performed on this file
    ops: Arc<OperationLog>,
}

impl TestParquetFile {
    /// Return a [`ParquetRecordBatchReaderBuilder`] for reading this file
    fn sync_builder(
        &self,
        options: ArrowReaderOptions,
    ) -> ParquetRecordBatchReaderBuilder<RecordingChunkReader> {
        let reader = RecordingChunkReader {
            inner: self.bytes.clone(),
            ops: Arc::clone(&self.ops),
        };
        ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)
            .expect("ParquetRecordBatchReaderBuilder")
    }

    /// Build the reader from the specified builder and read all batches from it,
    /// and assert that the operations log contains the expected entries.
    fn run_test(
        &self,
        builder: ParquetRecordBatchReaderBuilder<RecordingChunkReader>,
        expected: Vec<&str>,
    ) {
        self.ops.add_entry(LogEntry::event("Builder Configured"));
        let reader = builder.build().unwrap();
        self.ops.add_entry(LogEntry::event("Reader Built"));
        for batch in reader {
            match batch {
                Ok(_) => {}
                Err(e) => panic!("Error reading batch: {e}"),
            }
        }
        self.ops.assert(expected)
    }

    /// Create a new `TestParquetFile` with the specified temporary directory and path
    /// and determines the row group layout.
    fn new(bytes: Bytes) -> Self {
        // Read the parquet file to determine its layout
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes.clone(),
            ArrowReaderOptions::default().with_page_index(true),
        )
        .unwrap();

        let parquet_metadata = builder.metadata();

        let offset_index = parquet_metadata
            .offset_index()
            .expect("Parquet metadata should have a page index");

        let row_groups = TestRowGroups::new(parquet_metadata, offset_index);

        // figure out the footer location in the file
        let footer_location = bytes.len() - FOOTER_SIZE..bytes.len();
        let footer = bytes.slice(footer_location.clone());
        let footer: &[u8; FOOTER_SIZE] = footer
            .as_bytes()
            .try_into() // convert to a fixed size array
            .unwrap();

        // figure out the metadata location
        let footer = ParquetMetaDataReader::decode_footer_tail(footer).unwrap();
        let metadata_len = footer.metadata_length();
        let metadata_location = footer_location.start - metadata_len..footer_location.start;

        let ops = Arc::new(OperationLog::new(
            footer_location,
            metadata_location,
            row_groups,
        ));

        TestParquetFile { bytes, ops }
    }
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
        self.ops.log_access(&range);
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

        self.ops.log_access(&read_range);

        buf.copy_from_slice(self.inner.slice(read_range).as_ref());
        // Update the inner position
        self.start += read_length;
        Ok(read_length)
    }
}

/// Information about a column chunk
#[derive(Debug)]
struct TestColumnChunk {
    /// The name of the column
    name: String,

    /// The location of the entire column chunk in the file including data pages
    /// and data pages.
    location: Range<usize>,

    /// The offset of the start of of the dictionary page if any
    dictionary_page_location: Option<i64>,

    /// The location of the data pages in the file
    page_locations: Vec<PageLocation>,
}

/// Information about the pages in a single row group
#[derive(Debug)]
struct TestRowGroup {
    /// Maps column_name -> Information about the column chunk
    columns: BTreeMap<String, TestColumnChunk>,
}

/// Information about all the row groups in a Parquet file, extracted from its metadata
#[derive(Debug)]
struct TestRowGroups {
    /// List of row groups, each containing information about its columns and page locations
    row_groups: Vec<TestRowGroup>,
}

impl TestRowGroups {
    fn new(parquet_metadata: &ParquetMetaData, offset_index: &ParquetOffsetIndex) -> Self {
        let row_groups = parquet_metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(rg_index, rg_meta)| {
                let columns = rg_meta
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col_idx, col_meta)| {
                        let column_name = col_meta.column_descr().name().to_string();
                        let page_locations =
                            offset_index[rg_index][col_idx].page_locations().to_vec();
                        let dictionary_page_location = col_meta.dictionary_page_offset();

                        // We can find the byte range of the entire column chunk
                        let (start_offset, length) = col_meta.byte_range();
                        let start_offset = start_offset as usize;
                        let end_offset = start_offset + length as usize;

                        TestColumnChunk {
                            name: column_name.clone(),
                            location: start_offset..end_offset,
                            dictionary_page_location,
                            page_locations,
                        }
                    })
                    .map(|test_column_chunk| {
                        // make key=value pairs to insert into the BTreeMap
                        (test_column_chunk.name.clone(), test_column_chunk)
                    })
                    .collect::<BTreeMap<_, _>>();
                TestRowGroup { columns }
            })
            .collect();

        Self { row_groups }
    }

    fn iter(&self) -> impl Iterator<Item = &TestRowGroup> {
        self.row_groups.iter()
    }
}

#[derive(Debug)]
struct OperationLog {
    /// The operations performed on the file
    ops: Mutex<Vec<LogEntry>>,

    /// Footer location in the parquet file
    footer_location: Range<usize>,

    /// Metadata location in the parquet file
    metadata_location: Range<usize>,

    /// Information about the row group layout in the parquet file, used to
    /// translate read operations into human understandable IO operations
    /// Path to the parquet file
    row_groups: TestRowGroups,
}

/// Store structured entries in the log to make it easier to combine multiple entries
#[derive(Debug)]
enum LogEntry {
    /// Read the footer (last 8 bytes) of the parquet file
    ReadFooter(Range<usize>),
    /// Read the metadata of the parquet file
    ReadMetadata(Range<usize>),
    /// Read data from a Column Chunk
    ReadColumnChunk {
        row_group_index: usize,
        column_name: String,
        range: Range<usize>,
        read_type: ReadType,
        num_reads: usize,
    },
    /// Not known where the read came from
    Unknown(Range<usize>),
    /// A user defined event
    Event(String),
}

impl LogEntry {
    fn event(event: impl Into<String>) -> Self {
        LogEntry::Event(event.into())
    }
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntry::ReadFooter(range) => write!(f, "Footer: {} bytes", range.len()),
            LogEntry::ReadMetadata(range) => write!(f, "Metadata: {}", range.len()),
            LogEntry::ReadColumnChunk {
                row_group_index,
                column_name,
                range,
                read_type,
                num_reads,
            } => {
                // If the average read size is less than 10 bytes, assume it is the thrift
                // decoder reading the page headers and add an annotation
                let annotation = if (range.len() / num_reads) < 10 {
                    " [header]"
                } else {
                    " [data]"
                };

                // align the read type to 20 characters for better readability, not sure why
                // this does not work inline with write! macro below
                write!(
                    f,
                    "Row Group {row_group_index}, column '{column_name}': {:15}  ({:10}, {:8}){annotation}",
                    // convert to strings so alignment works
                    format!("{read_type:?}"),
                    format!("{} bytes", range.len()),
                    format!("{num_reads} reads"),
                )
            }
            LogEntry::Unknown(range) => write!(f, "UNKNOWN: {range:?}"),
            LogEntry::Event(event) => write!(f, "Event: {event}"),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadType {
    DataPage(usize),
    DictionaryPage,
    Unknown,
}

impl OperationLog {
    fn new(
        footer_location: Range<usize>,
        metadata_location: Range<usize>,
        row_groups: TestRowGroups,
    ) -> Self {
        OperationLog {
            ops: Mutex::new(Vec::new()),
            metadata_location,
            footer_location,
            row_groups,
        }
    }

    /// Add an operation to the log
    fn add_entry(&self, entry: LogEntry) {
        let mut ops = self.ops.lock().unwrap();
        ops.push(entry);
    }

    // Combine entries in the log that are similar to reduce noise in the log.
    fn coalesce_entries(&self) {
        let mut ops = self.ops.lock().unwrap();

        // Coalesce entries with the same read type
        let prev_ops = std::mem::take(&mut *ops);
        for entry in prev_ops {
            let Some(last) = ops.last_mut() else {
                ops.push(entry);
                continue;
            };

            let LogEntry::ReadColumnChunk {
                row_group_index: last_rg_index,
                column_name: last_column_name,
                range: last_range,
                read_type: last_read_type,
                num_reads: last_num_reads,
            } = last
            else {
                // If the last entry is not a ReadColumnChunk, just push it
                ops.push(entry);
                continue;
            };

            // If the entry is not a ReadColumnChunk, just push it
            let LogEntry::ReadColumnChunk {
                row_group_index,
                column_name,
                range,
                read_type,
                num_reads,
            } = &entry
            else {
                ops.push(entry);
                continue;
            };

            // Combine the entries if they are the same and this read is less than 10b.
            //
            // This heuristic is used to combine small reads (typically 1-2
            // byte) made by the thrift decoder when reading the data/dictionary
            // page headers.
            if *row_group_index != *last_rg_index
                || column_name != last_column_name
                || read_type != last_read_type
                || (range.start > last_range.end)
                || (range.end < last_range.start)
                || range.len() > 10
            {
                ops.push(entry);
                continue;
            }
            // combine
            *last_range = last_range.start.min(range.start)..last_range.end.max(range.end);
            *last_num_reads += num_reads;
        }
    }

    /// Assert that the operations in the log match the expected operations
    /// with an error message that can be copy/pasted to update a test on failure.
    fn assert(&self, expected: Vec<&str>) {
        self.coalesce_entries();
        let ops = self.ops.lock().unwrap();

        let ops = ops.iter().map(|op| op.to_string()).collect::<Vec<_>>();
        let actual = ops.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "Operation log mismatch\n\nactual: {actual:#?}\nexpected: {expected:#?}"
        );
    }

    /// Adds entries to the operation log for each interesting object that is
    /// accessed by the specified range
    ///
    /// This function checks the ranges in order against possible locations
    /// and adds the appropriate operation to the log for the first match found.
    fn log_access(&self, range: &Range<usize>) {
        let start = range.start as i64;
        let end = range.end as i64;

        // figure out what logical part of the file this range corresponds to
        if self.metadata_location.contains(&range.start)
            || self.metadata_location.contains(&(range.end - 1))
        {
            self.add_entry(LogEntry::ReadMetadata(range.clone()));
            return;
        }

        if self.footer_location.contains(&range.start)
            || self.footer_location.contains(&(range.end - 1))
        {
            self.add_entry(LogEntry::ReadFooter(range.clone()));
            return;
        }

        // Search for the location in each column chunk.
        //
        // The actual parquet reader must in general decode the page headers
        // and determine the byte ranges of the pages. However, for this test
        // we assume the following layout:
        //
        // ```text
        // (Dictionary Page)
        // (Data Page)
        // ...
        // (Data Page)
        // ```
        //
        // We also assume that `self.page_locations` holds the location of all
        // data pages, so any read operation that overlaps with a data page
        // location is considered a read of that page, and any other read must
        // be a dictionary page read.
        for (row_group_index, row_group) in self.row_groups.iter().enumerate() {
            for (column_name, test_column_chunk) in &row_group.columns {
                // Check if the range overlaps with any data page locations
                let page_locations = test_column_chunk.page_locations.iter();
                for (page_index, page_location) in page_locations.enumerate() {
                    let page_offset = page_location.offset;
                    let page_end = page_offset + page_location.compressed_page_size as i64;
                    if start >= page_offset && end <= page_end {
                        self.add_entry(LogEntry::ReadColumnChunk {
                            row_group_index,
                            column_name: column_name.clone(),
                            range: range.clone(),
                            read_type: ReadType::DataPage(page_index),
                            num_reads: 1,
                        });
                        return;
                    }
                }

                // Check if the range overlaps with the dictionary page location
                if let Some(dict_page_offset) = test_column_chunk.dictionary_page_location {
                    let dict_page_end = dict_page_offset + test_column_chunk.location.len() as i64;
                    if start >= dict_page_offset && end < dict_page_end {
                        self.add_entry(LogEntry::ReadColumnChunk {
                            row_group_index,
                            column_name: column_name.clone(),
                            range: range.clone(),
                            read_type: ReadType::DictionaryPage,
                            num_reads: 1,
                        });
                        return;
                    }
                }

                // If we can't find a page, but the range overlaps with the
                // column chunk location, use the column chunk location
                let column_byte_range = &test_column_chunk.location;
                if column_byte_range.contains(&range.start)
                    && column_byte_range.contains(&(range.end - 1))
                {
                    self.add_entry(LogEntry::ReadColumnChunk {
                        row_group_index,
                        column_name: column_name.clone(),
                        range: range.clone(),
                        read_type: ReadType::Unknown,
                        num_reads: 1,
                    });
                    return;
                }
            }
        }

        // If we reach here, the range does not match any known logical part of the file
        self.add_entry(LogEntry::Unknown(range.clone()));
    }
}

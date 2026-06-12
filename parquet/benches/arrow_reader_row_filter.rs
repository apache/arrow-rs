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

//! Benchmark for evaluating row filters and projections on a Parquet file.
//!
//! # Background:
//!
//! As described in [Efficient Filter Pushdown in Parquet], evaluating
//! pushdown filters is a two-step process:
//!
//! 1. Build a filter mask by decoding and evaluating filter functions on
//!    the filter column(s).
//!
//! 2. Decode the rows that match the filter mask from the projected columns.
//!
//! The performance depends on factors such as the number of rows selected,
//! the clustering of results (which affects the efficiency of the filter mask),
//! and whether the same column is used for both filtering and projection.
//!
//! This benchmark helps measure the performance of these operations.
//!
//! [Efficient Filter Pushdown in Parquet]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
//!
//! The benchmark creates an in-memory Parquet file with 100K rows and ten columns.
//! The first four columns are:
//!   - int64: random integers (range: 0..100) generated with a fixed seed.
//!   - float64: random floating-point values (range: 0.0..100.0) generated with a fixed seed.
//!   - utf8View: random strings with some empty values and occasional constant "const" values.
//!   - ts: sequential timestamps in milliseconds.
//!
//! The following six columns (for filtering) are generated to mimic different
//! filter selectivity and clustering patterns:
//!   - pt: for Point Lookup – exactly one row is set to "unique_point", all others are random strings.
//!   - sel: for Selective Unclustered – exactly 1% of rows (those with i % 100 == 0) are "selected".
//!   - mod_clustered: for Moderately Selective Clustered – in each 10K-row block, the first 10 rows are "mod_clustered".
//!   - mod_unclustered: for Moderately Selective Unclustered – exactly 10% of rows (those with i % 10 == 1) are "mod_unclustered".
//!   - unsel_unclustered: for Unselective Unclustered – exactly 99% of rows (those with i % 100 != 0) are "unsel_unclustered".
//!   - unsel_clustered: for Unselective Clustered – in each 10K-row block, rows with an offset >= 1000 are "unsel_clustered".
//!

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StructArray, TimestampMillisecondArray,
};
use arrow::compute::kernels::cmp::{eq, gt, lt, neq};
use arrow::compute::{and, or};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::StringViewArray;
use arrow_array::builder::{ArrayBuilder, StringViewBuilder};
use bytes::Bytes;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
    RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::ops::Range;
use std::sync::Arc;

const COLUMN_NAMES: [&str; 4] = ["int64", "float64", "utf8View", "ts"];
const UTF8_VIEW_MISSING_VALUE: &str = "__arrow_rs_missing__";

/// Generates a random string. Has a 50% chance to generate a short string (3–11 characters)
/// or a long string (13–20 characters).
fn random_string(rng: &mut StdRng) -> String {
    let charset = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let is_long = rng.random_bool(0.5);
    let len = if is_long {
        rng.random_range(13..21)
    } else {
        rng.random_range(3..12)
    };
    (0..len)
        .map(|_| charset[rng.random_range(0..charset.len())] as char)
        .collect()
}

/// Creates an int64 array of a given size with random integers in [0, 100).
/// Then, it overwrites a single random index with 9999 to serve as the unique value for point lookup.
fn create_int64_array(size: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(42);
    let mut values: Vec<i64> = (0..size).map(|_| rng.random_range(0..100)).collect();
    let unique_index = rng.random_range(0..size);
    values[unique_index] = 9999; // Unique value for point lookup
    Arc::new(Int64Array::from(values)) as ArrayRef
}

/// Creates a float64 array of a given size with random floats in [0.0, 100.0).
fn create_float64_array(size: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(43);
    let values: Vec<f64> = (0..size).map(|_| rng.random_range(0.0..100.0)).collect();
    Arc::new(Float64Array::from(values)) as ArrayRef
}

/// Creates a utf8View array of a given size with random strings.
///
/// This is modeled after the "SearchPhrase" column in the ClickBench benchmark.
///
/// See <https://github.com/apache/arrow-rs/issues/7460> for calculations.
///
/// The important ClickBench data properties are:
/// * Selectivity is: 13172392 / 99997497 = 0.132
/// * Number of RowSelections = 14054784
/// * Average run length of each RowSelection: 99997497 / 14054784 = 7.114
///
/// The properties of this array are:
/// * Selectivity is: 15144 / 100000 = 0.15144
/// * Number of RowSelections = 12904
/// * Average run length of each RowSelection: 100000 / 12904 = 7.75
fn create_utf8_view_array(size: usize) -> ArrayRef {
    const AVG_RUN_LENGTH: usize = 4; // average number of empty/non-empty strings in a row
    const EMPTY_DENSITY: u32 = 85; // percent chance that each run is an empty string

    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(44);
    while builder.len() < size {
        let mut run_length = rng.random_range(1..AVG_RUN_LENGTH);
        if builder.len() + run_length > size {
            // cap to size rows
            run_length = size - builder.len();
        }

        let choice = rng.random_range(0..100);
        if choice < EMPTY_DENSITY {
            for _ in 0..run_length {
                builder.append_value("");
            }
        } else {
            for _ in 0..run_length {
                builder.append_value(random_string(&mut rng));
            }
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Creates a ts (timestamp) array of a given size. Each value is computed as i % 10_000,
/// which simulates repeating blocks (each block of 10,000) to model clustered patterns.
fn create_ts_array(size: usize) -> ArrayRef {
    let values: Vec<i64> = (0..size).map(|i| (i % 10_000) as i64).collect();
    Arc::new(TimestampMillisecondArray::from(values)) as ArrayRef
}

/// Creates a RecordBatch with 100K rows and 4 columns: int64, float64, utf8View, and ts.
fn create_record_batch(size: usize) -> RecordBatch {
    let fields = vec![
        Field::new("int64", DataType::Int64, false),
        Field::new("float64", DataType::Float64, false),
        Field::new("utf8View", DataType::Utf8View, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ];
    let schema = Arc::new(Schema::new(fields));

    let int64_array = create_int64_array(size);
    let float64_array = create_float64_array(size);
    let utf8_array = create_utf8_view_array(size);
    let ts_array = create_ts_array(size);

    let arrays: Vec<ArrayRef> = vec![int64_array, float64_array, utf8_array, ts_array];
    RecordBatch::try_new(schema, arrays).unwrap()
}

/// Total number of rows.
const TOTAL_ROWS: usize = 500_000;

/// Maximum rows per row group.
const ROW_GROUP_SIZE: usize = 100_000;

/// Writes the RecordBatch to an in memory buffer, returning the buffer
fn write_parquet_file() -> Vec<u8> {
    write_parquet_file_with_rows(TOTAL_ROWS, ROW_GROUP_SIZE)
}

/// Writes a RecordBatch with a configurable shape to an in memory buffer,
/// returning the buffer.
fn write_parquet_file_with_rows(total_rows: usize, row_group_size: usize) -> Vec<u8> {
    let batch = create_record_batch(total_rows);
    write_record_batch_to_parquet(&batch, row_group_size)
}

fn write_record_batch_to_parquet(batch: &RecordBatch, row_group_size: usize) -> Vec<u8> {
    let schema = batch.schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(row_group_size))
        .build();
    let mut buffer = vec![];
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    buffer
}

fn create_nested_record_batch(size: usize) -> RecordBatch {
    let tag = Arc::new(StringViewArray::from_iter_values(
        (0..size).map(|idx| format!("tag_{}", idx % 7)),
    )) as ArrayRef;
    let payload = StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Int64Array::from_iter_values(
                (0..size).map(|idx| idx as i64 + 1_000),
            )) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8View, false)),
            Arc::new(StringViewArray::from_iter_values(
                (0..size).map(|idx| format!("payload_{idx}")),
            )) as ArrayRef,
        ),
    ]);
    let payload = Arc::new(payload) as ArrayRef;
    let value = Arc::new(Int64Array::from_iter_values(
        (0..size).map(|idx| idx as i64 + 10_000),
    )) as ArrayRef;

    RecordBatch::try_from_iter(vec![("tag", tag), ("payload", payload), ("value", value)]).unwrap()
}

fn write_nested_parquet_file_with_rows(total_rows: usize, row_group_size: usize) -> Vec<u8> {
    let batch = create_nested_record_batch(total_rows);
    write_record_batch_to_parquet(&batch, row_group_size)
}

/// ProjectionCase defines the projection mode for the benchmark:
/// either projecting all columns or excluding the column that is used for filtering.
#[derive(Clone, Copy)]
enum ProjectionCase {
    AllColumns,
    ExcludeFilterColumn,
    FilterColumnsOnly,
    CountOnly,
    FixedColumns,
    Float64AndTs,
    Float64Only,
    Int64AndFloat64,
    Int64AndUtf8,
    TsAndUtf8,
    Utf8Only,
}

impl std::fmt::Display for ProjectionCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionCase::AllColumns => write!(f, "all_columns"),
            ProjectionCase::ExcludeFilterColumn => write!(f, "exclude_filter_column"),
            ProjectionCase::FilterColumnsOnly => write!(f, "filter_columns_only"),
            ProjectionCase::CountOnly => write!(f, "count_only"),
            ProjectionCase::FixedColumns => write!(f, "fixed_columns"),
            ProjectionCase::Float64AndTs => write!(f, "float64_and_ts"),
            ProjectionCase::Float64Only => write!(f, "float64_only"),
            ProjectionCase::Int64AndFloat64 => write!(f, "int64_and_float64"),
            ProjectionCase::Int64AndUtf8 => write!(f, "int64_and_utf8"),
            ProjectionCase::TsAndUtf8 => write!(f, "ts_and_utf8"),
            ProjectionCase::Utf8Only => write!(f, "utf8_only"),
        }
    }
}

#[derive(Clone, Copy)]
enum SyncStrategy {
    FullPostFilter,
    PushdownAuto,
    PushdownSelectors,
    PushdownMask,
}

impl std::fmt::Display for SyncStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncStrategy::FullPostFilter => write!(f, "full_post_filter"),
            SyncStrategy::PushdownAuto => write!(f, "pushdown_auto"),
            SyncStrategy::PushdownSelectors => write!(f, "pushdown_selectors"),
            SyncStrategy::PushdownMask => write!(f, "pushdown_mask"),
        }
    }
}

#[derive(Clone, Copy)]
enum AsyncStrategy {
    FullPostFilter,
    PushdownAuto,
    PushdownSelectors,
    PushdownMask,
}

impl std::fmt::Display for AsyncStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncStrategy::FullPostFilter => write!(f, "full_post_filter"),
            AsyncStrategy::PushdownAuto => write!(f, "pushdown_auto"),
            AsyncStrategy::PushdownSelectors => write!(f, "pushdown_selectors"),
            AsyncStrategy::PushdownMask => write!(f, "pushdown_mask"),
        }
    }
}

/// FilterType encapsulates the different filter comparisons.
/// The variants correspond to the different filter patterns.
#[derive(Clone, Copy, Debug)]
enum FilterType {
    /// "Point Lookup": selects a single row
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │               │    │      ...      │
    /// │               │    │               │
    /// │               │    │               │
    /// │     ...       │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// │               │    │      ...      │
    /// │               │    │               │
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (1 RowSelection of 1 row)
    PointLookup,
    /// selective (1%) unclustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │      ...      │    │               │
    /// │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// │               │    │      ...      │
    /// │               │    │               │
    /// │               │    │               │
    /// │      ...      │    │               │
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (1000 RowSelection of 10 rows each)
    SelectiveUnclustered,
    /// moderately selective (10%) clustered filter
    /// ```text
    ///  ┌───────────────┐    ┌───────────────┐
    ///  │               │    │               │
    ///  │               │    │               │
    ///  │               │    │     ...       │
    ///  │               │    │               │
    ///  │     ...       │    │               │
    ///  │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    ///  │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    ///  │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    ///  │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    ///  └───────────────┘    └───────────────┘
    /// ```
    /// (10 RowSelections of 10,000 rows each)
    ModeratelySelectiveClustered,
    /// moderately selective (10%) clustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │      ...      │    │               │
    /// │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// │               │    │               │
    /// │               │    │      ...      │
    /// │      ...      │    │               │
    /// │               │    │               │
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (10 RowSelections of 10,000 rows each)
    ModeratelySelectiveUnclustered,
    /// unselective (99%) unclustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (99,000 RowSelections of 10 rows each)
    UnselectiveUnclustered,
    /// unselective (90%) clustered filter
    /// ```text
    /// ┌───────────────┐    ┌───────────────┐
    /// │               │    │               │
    /// │               │    │               │
    /// │               │    │     ...       │
    /// │               │    │               │
    /// │     ...       │    │               │
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// └───────────────┘    └───────────────┘
    /// ```
    /// (99 RowSelection of 10,000 rows each)
    UnselectiveClustered,
    /// [`Self::SelectivelUnclusered`] `AND`
    /// [`Self::ModeratelySelectiveClustered`]
    Composite,
    /// `utf8View <> ''` modeling [ClickBench] [Q21-Q27]
    ///
    /// [ClickBench]: https://github.com/ClickHouse/ClickBench
    /// [Q21-Q27]: https://github.com/apache/datafusion/blob/b7177234e65cbbb2dcc04c252f6acd80bb026362/benchmarks/queries/clickbench/queries.sql#L22-L28
    Utf8ViewNonEmpty,
    /// Sparse variable-width predicate shaped like TPC-DS Q83 dynamic
    /// `i_item_id` filters, where the predicate column is also projected.
    Utf8ViewMissing,
    /// Scalar-only part of ClickBench Q37:
    ///
    /// ```sql
    /// WHERE CounterID = 62
    ///   AND EventDate BETWEEN ...
    ///   AND DontCountHits = 0
    ///   AND IsRefresh = 0
    ///   AND Title <> ''
    /// ```
    ///
    /// DataFusion `Auto` does not push down the `Title <> ''` string predicate,
    /// but it can push down the scalar prefix to defer decoding `Title`.
    /// This synthetic predicate keeps that reader-level shape: cheap scalar
    /// filter columns protect an expensive `Utf8View` output column.
    ClickBenchQ37ScalarPrefix,
    /// Shape of ClickBench extended Q6 under DataFusion row-filter pushdown:
    /// an early cheap fixed-width predicate can prune almost all rows before a
    /// later unprojected variable-width predicate is decoded.
    ClickBenchQ6MixedPredicates,
    /// Same scalar + variable-width predicate columns as [`Self::ClickBenchQ6MixedPredicates`],
    /// but with the variable-width predicate evaluated first. This anchors the
    /// static post-filter gate against predicate-order drift.
    ClickBenchQ6VarWidthFirst,
    /// Shape of ClickBench Q41-like fixed-width filters: sparse fragmented
    /// scalar predicates with a cheap fixed-width output projection.
    ClickBenchQ41SparseFixedOutput,
    /// Shape of ClickBench Q40: multiple cheap scalar predicates, very small
    /// output, and one projected predicate column used later by grouping.
    ClickBenchQ40ScalarGroupBy,
    /// Shape of TPC-DS Q41: a complex OR predicate over dictionary/string-like
    /// and scalar columns where predicate evaluation dominates reader time.
    TpcdsQ41ComplexOr,
    /// Shape of TPC-DS Q20 catalog_sales after dynamic filters: multiple
    /// fixed-width predicates where predicate columns are also projected.
    TpcdsQ20ProjectedDynamicFilters,
    /// Shape of TPC-DS Q21 after dynamic-filter pruning: sparse fragmented
    /// fixed-width predicates where the final projection still includes the
    /// predicate columns. This protects against choosing selectors for columns
    /// that were already decoded/cached by predicate evaluation.
    TpcdsQ21ProjectedFixedOutput,
    /// Shape of TPC-DS Q2 fact scans: the dynamic filter applies to the date
    /// key, the same date key is projected, and an additional fixed-width sales
    /// value can still be deferred by predicate pushdown.
    TpcdsQ2ProjectedPredicate5Pct,
    TpcdsQ2ProjectedPredicate8Pct,
    TpcdsQ2ProjectedPredicate10Pct,
    TpcdsQ2ProjectedPredicate20Pct,
    TpcdsQ2ProjectedPredicate30Pct,
    TpcdsQ2ProjectedPredicate40Pct,
    TpcdsQ2ProjectedPredicate50Pct,
    /// Scalar range predicate shaped like TPC-DS Q9 `ss_quantity BETWEEN ...`
    /// subqueries. The selected rows are random and moderately selective, and
    /// benchmark projections cover both count-only and numeric aggregate cases.
    TpcdsQ9QuantityRange,
    /// Exact shape for the projected-predicate moderate-selectivity gate:
    /// a clustered 20% timestamp predicate where the predicate column is
    /// projected and the deferred output is variable-width.
    ProjectedTs8PctClustered,
    ProjectedTs20PctClustered,
    /// Very sparse projected fixed-width scan shaped like TPC-DS fact-table
    /// filters where the predicate column is also needed in the output projection.
    TpcdsSparseProjectedFactScan,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FilterType::PointLookup => "int64 == 9999",
            FilterType::SelectiveUnclustered => "float64 > 99.0",
            FilterType::ModeratelySelectiveClustered => "ts >= 9000",
            FilterType::ModeratelySelectiveUnclustered => "int64 > 90",
            FilterType::UnselectiveUnclustered => "float64 <= 99.0",
            FilterType::UnselectiveClustered => "ts < 9000",
            FilterType::Composite => "float64 > 99.0 AND ts >= 9000",
            FilterType::Utf8ViewNonEmpty => "utf8View <> ''",
            FilterType::Utf8ViewMissing => "utf8View == '<missing>'",
            FilterType::ClickBenchQ37ScalarPrefix => "int64 == 62 AND ts < 9000",
            FilterType::ClickBenchQ6MixedPredicates => "int64 == 9999 AND utf8View <> ''",
            FilterType::ClickBenchQ6VarWidthFirst => "utf8View <> '' AND int64 == 9999",
            FilterType::ClickBenchQ41SparseFixedOutput => "int64 < 8 AND ts < 9000",
            FilterType::ClickBenchQ40ScalarGroupBy => {
                "int64 == 62 AND float64 > 10.0 AND ts < 9000"
            }
            FilterType::TpcdsQ41ComplexOr => {
                "(utf8View <> '' AND int64 < 8) OR (ts < 100 AND float64 > 95.0)"
            }
            FilterType::TpcdsQ20ProjectedDynamicFilters => {
                "int64 < 12 AND ts < 9000 projected dynamic filters"
            }
            FilterType::TpcdsQ21ProjectedFixedOutput => {
                "int64 < 8 AND ts < 9000 projected predicates"
            }
            FilterType::TpcdsQ2ProjectedPredicate10Pct => {
                "int64 < 10 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate5Pct => {
                "int64 < 5 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate8Pct => {
                "int64 < 8 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate20Pct => {
                "int64 < 20 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate30Pct => {
                "int64 < 30 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate40Pct => {
                "int64 < 40 projected predicate with fixed output"
            }
            FilterType::TpcdsQ2ProjectedPredicate50Pct => {
                "int64 < 50 projected predicate with fixed output"
            }
            FilterType::TpcdsQ9QuantityRange => "int64 > 0 AND int64 < 21",
            FilterType::ProjectedTs20PctClustered => {
                "ts < 2000 projected predicate with utf8 output"
            }
            FilterType::ProjectedTs8PctClustered => "ts < 800 projected predicate with utf8 output",
            FilterType::TpcdsSparseProjectedFactScan => "ts % 1000 == 0",
        };
        write!(f, "{s}")
    }
}

impl FilterType {
    /// Applies the specified filter on the given RecordBatch and returns a BooleanArray mask.
    fn filter_batch(&self, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
        match self {
            // Point Lookup on int64 column
            FilterType::PointLookup => {
                let array = batch.column(batch.schema().index_of("int64")?);
                let scalar = Int64Array::new_scalar(9999);
                eq(array, &scalar)
            }
            // Selective Unclustered on float64 column: float64 > 99.0
            FilterType::SelectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("float64")?);
                let scalar = Float64Array::new_scalar(99.0);
                gt(array, &scalar)
            }
            // Moderately Selective Clustered on ts column: ts >= 9000 (implemented as > 8999)
            FilterType::ModeratelySelectiveClustered => {
                let array = batch.column(batch.schema().index_of("ts")?);
                gt(array, &TimestampMillisecondArray::new_scalar(8999))
            }
            // Moderately Selective Unclustered on int64 column: int64 > 90
            FilterType::ModeratelySelectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("int64")?);
                let scalar = Int64Array::new_scalar(90);
                gt(array, &scalar)
            }
            // Unselective Unclustered on float64 column: NOT (float64 > 99.0)
            FilterType::UnselectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("float64")?);
                gt(array, &Float64Array::new_scalar(99.0))
            }
            // Unselective Clustered on ts column: ts < 9000
            FilterType::UnselectiveClustered => {
                let array = batch.column(batch.schema().index_of("ts")?);
                lt(array, &TimestampMillisecondArray::new_scalar(9000))
            }
            // Composite filter: logical AND of (float64 > 99.0) and (ts >= 9000)
            FilterType::Composite => {
                let mask1 = FilterType::SelectiveUnclustered.filter_batch(batch)?;
                let mask2 = FilterType::ModeratelySelectiveClustered.filter_batch(batch)?;
                and(&mask1, &mask2)
            }
            // Utf8ViewNonEmpty: selects rows where the utf8View column is not an empty string.
            FilterType::Utf8ViewNonEmpty => {
                let array = batch.column(batch.schema().index_of("utf8View")?);
                let scalar = StringViewArray::new_scalar("");
                neq(array, &scalar)
            }
            FilterType::Utf8ViewMissing => {
                let array = batch.column(batch.schema().index_of("utf8View")?);
                let scalar = StringViewArray::new_scalar(UTF8_VIEW_MISSING_VALUE);
                eq(array, &scalar)
            }
            // ClickBenchQ37ScalarPrefix: a cheap fragmented scalar predicate
            // evaluated before decoding a variable-width output column.
            FilterType::ClickBenchQ37ScalarPrefix => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let counter_match = eq(int64, &Int64Array::new_scalar(62))?;
                let date_like_range = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&counter_match, &date_like_range)
            }
            FilterType::ClickBenchQ6MixedPredicates | FilterType::ClickBenchQ6VarWidthFirst => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let utf8 = batch.column(batch.schema().index_of("utf8View")?);
                let cheap_prefix = eq(int64, &Int64Array::new_scalar(9999))?;
                let string_suffix = neq(utf8, &StringViewArray::new_scalar(""))?;
                and(&cheap_prefix, &string_suffix)
            }
            FilterType::ClickBenchQ41SparseFixedOutput
            | FilterType::TpcdsQ21ProjectedFixedOutput => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let counter_like = lt(int64, &Int64Array::new_scalar(8))?;
                let date_like = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&counter_like, &date_like)
            }
            FilterType::ClickBenchQ40ScalarGroupBy => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let float64 = batch.column(batch.schema().index_of("float64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let counter_match = eq(int64, &Int64Array::new_scalar(62))?;
                let width_match = gt(float64, &Float64Array::new_scalar(10.0))?;
                let date_like = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&and(&counter_match, &width_match)?, &date_like)
            }
            FilterType::TpcdsQ41ComplexOr => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let float64 = batch.column(batch.schema().index_of("float64")?);
                let utf8 = batch.column(batch.schema().index_of("utf8View")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let string_branch = and(
                    &neq(utf8, &StringViewArray::new_scalar(""))?,
                    &lt(int64, &Int64Array::new_scalar(8))?,
                )?;
                let scalar_branch = and(
                    &lt(ts, &TimestampMillisecondArray::new_scalar(100))?,
                    &gt(float64, &Float64Array::new_scalar(95.0))?,
                )?;
                or(&string_branch, &scalar_branch)
            }
            FilterType::TpcdsQ20ProjectedDynamicFilters => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let ts = batch.column(batch.schema().index_of("ts")?);
                let item_like = lt(int64, &Int64Array::new_scalar(12))?;
                let date_like = lt(ts, &TimestampMillisecondArray::new_scalar(9000))?;
                and(&item_like, &date_like)
            }
            FilterType::TpcdsQ2ProjectedPredicate5Pct
            | FilterType::TpcdsQ2ProjectedPredicate8Pct
            | FilterType::TpcdsQ2ProjectedPredicate10Pct
            | FilterType::TpcdsQ2ProjectedPredicate20Pct
            | FilterType::TpcdsQ2ProjectedPredicate30Pct
            | FilterType::TpcdsQ2ProjectedPredicate40Pct
            | FilterType::TpcdsQ2ProjectedPredicate50Pct => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let threshold = match self {
                    FilterType::TpcdsQ2ProjectedPredicate5Pct => 5,
                    FilterType::TpcdsQ2ProjectedPredicate8Pct => 8,
                    FilterType::TpcdsQ2ProjectedPredicate10Pct => 10,
                    FilterType::TpcdsQ2ProjectedPredicate20Pct => 20,
                    FilterType::TpcdsQ2ProjectedPredicate30Pct => 30,
                    FilterType::TpcdsQ2ProjectedPredicate40Pct => 40,
                    FilterType::TpcdsQ2ProjectedPredicate50Pct => 50,
                    _ => unreachable!(),
                };
                lt(int64, &Int64Array::new_scalar(threshold))
            }
            FilterType::TpcdsQ9QuantityRange => {
                let int64 = batch.column(batch.schema().index_of("int64")?);
                let lower = gt(int64, &Int64Array::new_scalar(0))?;
                let upper = lt(int64, &Int64Array::new_scalar(21))?;
                and(&lower, &upper)
            }
            FilterType::ProjectedTs8PctClustered => {
                let ts = batch.column(batch.schema().index_of("ts")?);
                lt(ts, &TimestampMillisecondArray::new_scalar(800))
            }
            FilterType::ProjectedTs20PctClustered => {
                let ts = batch.column(batch.schema().index_of("ts")?);
                lt(ts, &TimestampMillisecondArray::new_scalar(2000))
            }
            FilterType::TpcdsSparseProjectedFactScan => {
                let ts = batch
                    .column(batch.schema().index_of("ts")?)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                Ok(BooleanArray::from(
                    ts.values()
                        .iter()
                        .map(|value| value % 1000 == 0)
                        .collect::<Vec<_>>(),
                ))
            }
        }
    }

    /// Return the indexes in the batch's schema that are used for filtering.
    fn filter_projection(&self) -> &'static [usize] {
        match self {
            FilterType::PointLookup => &[0],
            FilterType::SelectiveUnclustered => &[1],
            FilterType::ModeratelySelectiveClustered => &[3],
            FilterType::ModeratelySelectiveUnclustered => &[0],
            FilterType::UnselectiveUnclustered => &[1],
            FilterType::UnselectiveClustered => &[3],
            FilterType::Composite => &[1, 3], // Use float64 column and ts column as representative for composite
            FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewMissing => &[2],
            FilterType::ClickBenchQ37ScalarPrefix => &[0, 3],
            FilterType::ClickBenchQ6MixedPredicates | FilterType::ClickBenchQ6VarWidthFirst => {
                &[0, 2]
            }
            FilterType::ClickBenchQ40ScalarGroupBy => &[0, 1, 3],
            FilterType::ClickBenchQ41SparseFixedOutput
            | FilterType::TpcdsQ20ProjectedDynamicFilters
            | FilterType::TpcdsQ21ProjectedFixedOutput => &[0, 3],
            FilterType::TpcdsQ41ComplexOr => &[0, 1, 2, 3],
            FilterType::TpcdsQ2ProjectedPredicate5Pct
            | FilterType::TpcdsQ2ProjectedPredicate8Pct
            | FilterType::TpcdsQ2ProjectedPredicate10Pct
            | FilterType::TpcdsQ2ProjectedPredicate20Pct
            | FilterType::TpcdsQ2ProjectedPredicate30Pct
            | FilterType::TpcdsQ2ProjectedPredicate40Pct
            | FilterType::TpcdsQ2ProjectedPredicate50Pct => &[0],
            FilterType::TpcdsQ9QuantityRange => &[0],
            FilterType::ProjectedTs8PctClustered | FilterType::ProjectedTs20PctClustered => &[3],
            FilterType::TpcdsSparseProjectedFactScan => &[3],
        }
    }
}

/// Benchmark filters and projections by reading the Parquet file.
/// This benchmark iterates over all individual filter types and two projection cases.
/// It measures the time to read and filter the Parquet file according to each scenario.
fn benchmark_filters_and_projections(c: &mut Criterion) {
    // make the parquet file in memory that can be shared
    let parquet_file = Bytes::from(write_parquet_file());
    let filter_types = vec![
        FilterType::PointLookup,
        FilterType::SelectiveUnclustered,
        FilterType::ModeratelySelectiveClustered,
        FilterType::ModeratelySelectiveUnclustered,
        FilterType::UnselectiveUnclustered,
        FilterType::UnselectiveClustered,
        FilterType::Utf8ViewNonEmpty,
        FilterType::Composite,
    ];
    let projection_cases = vec![
        ProjectionCase::AllColumns,
        ProjectionCase::ExcludeFilterColumn,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_row_filter");

    for filter_type in filter_types {
        for proj_case in &projection_cases {
            let filter_col = filter_type.filter_projection().to_vec();
            let output_projection = output_projection_for(filter_type, proj_case);

            let reader = InMemoryReader::try_new(&parquet_file).unwrap();
            let metadata = Arc::clone(reader.metadata());

            let schema_descr = metadata.file_metadata().schema_descr();
            let projection_mask = ProjectionMask::roots(schema_descr, output_projection.clone());
            let pred_mask = ProjectionMask::roots(schema_descr, filter_col.clone());

            let benchmark_name = format!("{filter_type}/{proj_case}",);

            // run the benchmark for the async reader
            let bench_id = BenchmarkId::new(benchmark_name.clone(), "async");
            let rt_captured = rt.handle().clone();
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let reader = reader.clone();
                    let pred_mask = pred_mask.clone();
                    let projection_mask = projection_mask.clone();
                    // row filters are not clone, so must make it each iter
                    let filter = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                        Ok(filter_type.filter_batch(&batch).unwrap())
                    });
                    let row_filter = RowFilter::new(vec![Box::new(filter)]);

                    rt_captured.block_on(async {
                        benchmark_async_reader(reader, projection_mask, row_filter).await;
                    })
                });
            });

            // run the benchmark for the sync reader
            let bench_id = BenchmarkId::new(benchmark_name, "sync");
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let reader = reader.clone();
                    let pred_mask = pred_mask.clone();
                    let projection_mask = projection_mask.clone();
                    // row filters are not clone, so must make it each iter
                    let filter = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                        Ok(filter_type.filter_batch(&batch).unwrap())
                    });
                    let row_filter = RowFilter::new(vec![Box::new(filter)]);

                    benchmark_sync_reader(reader, projection_mask, row_filter)
                });
            });
        }
    }
}

/// Compare full scan plus post-filtering against row-level pushdown strategies.
///
/// This group is intentionally sync-only and smaller than
/// [`benchmark_filters_and_projections`]. It tracks the cases most likely to
/// inform a future default `Auto` policy: selective random filters, clustered
/// filters, ClickBench-like string filters, and the forced selector strategy
/// that originally motivated apache/arrow-rs#8565.
fn benchmark_sync_strategy_matrix(c: &mut Criterion) {
    let parquet_file = Bytes::from(write_parquet_file());
    let filter_types = [
        FilterType::SelectiveUnclustered,
        FilterType::ModeratelySelectiveClustered,
        FilterType::ModeratelySelectiveUnclustered,
        FilterType::Utf8ViewNonEmpty,
    ];
    let strategies = [
        SyncStrategy::FullPostFilter,
        SyncStrategy::PushdownAuto,
        SyncStrategy::PushdownSelectors,
        SyncStrategy::PushdownMask,
    ];

    let mut group = c.benchmark_group("arrow_reader_row_filter_strategy_matrix");

    for filter_type in filter_types {
        for projection_case in [
            ProjectionCase::AllColumns,
            ProjectionCase::ExcludeFilterColumn,
        ] {
            let reader = InMemoryReader::try_new(&parquet_file).unwrap();
            let metadata = Arc::clone(reader.metadata());
            let schema_descr = metadata.file_metadata().schema_descr();
            let output_projection = output_projection_for(filter_type, &projection_case);
            let read_projection = full_post_filter_read_projection(filter_type, &output_projection);
            let output_column_names = projection_names(&output_projection);
            let projection_mask = ProjectionMask::roots(schema_descr, output_projection);
            let read_projection_mask = ProjectionMask::roots(schema_descr, read_projection);
            let pred_mask = ProjectionMask::roots(
                schema_descr,
                filter_type.filter_projection().iter().copied(),
            );

            for strategy in strategies {
                let bench_id = BenchmarkId::new(
                    format!("{filter_type}/{projection_case}"),
                    strategy.to_string(),
                );

                group.bench_function(bench_id, |b| {
                    b.iter(|| {
                        let reader = reader.clone();
                        let pred_mask = pred_mask.clone();
                        let projection_mask = projection_mask.clone();
                        let read_projection_mask = read_projection_mask.clone();
                        let output_column_names = output_column_names.clone();

                        match strategy {
                            SyncStrategy::FullPostFilter => benchmark_sync_reader_post_filter(
                                reader,
                                read_projection_mask,
                                output_column_names,
                                filter_type,
                            ),
                            SyncStrategy::PushdownAuto => {
                                let row_filter = row_filter_for(filter_type, pred_mask);
                                benchmark_sync_reader_with_policy(
                                    reader,
                                    projection_mask,
                                    row_filter,
                                    RowSelectionPolicy::default(),
                                )
                            }
                            SyncStrategy::PushdownSelectors => {
                                let row_filter = row_filter_for(filter_type, pred_mask);
                                benchmark_sync_reader_with_policy(
                                    reader,
                                    projection_mask,
                                    row_filter,
                                    RowSelectionPolicy::Selectors,
                                )
                            }
                            SyncStrategy::PushdownMask => {
                                let row_filter = row_filter_for(filter_type, pred_mask);
                                benchmark_sync_reader_with_policy(
                                    reader,
                                    projection_mask,
                                    row_filter,
                                    RowSelectionPolicy::Mask,
                                )
                            }
                        }
                    });
                });
            }
        }
    }
}

/// Compare async full scan plus post-filtering against async row-level pushdown
/// strategies. This is the matrix that exercises the current reader `Auto`
/// policy through the async stream backed by the push decoder row-group pipeline.
fn benchmark_async_strategy_matrix(c: &mut Criterion) {
    let parquet_file = Bytes::from(write_parquet_file());
    let filter_types = [
        FilterType::SelectiveUnclustered,
        FilterType::ModeratelySelectiveClustered,
        FilterType::ModeratelySelectiveUnclustered,
        FilterType::Utf8ViewNonEmpty,
    ];
    let strategies = [
        AsyncStrategy::FullPostFilter,
        AsyncStrategy::PushdownAuto,
        AsyncStrategy::PushdownSelectors,
        AsyncStrategy::PushdownMask,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_row_filter_async_strategy_matrix");

    for filter_type in filter_types {
        for projection_case in [
            ProjectionCase::AllColumns,
            ProjectionCase::ExcludeFilterColumn,
        ] {
            let reader = InMemoryReader::try_new(&parquet_file).unwrap();
            let metadata = Arc::clone(reader.metadata());
            let schema_descr = metadata.file_metadata().schema_descr();
            let output_projection = output_projection_for(filter_type, &projection_case);
            let read_projection = full_post_filter_read_projection(filter_type, &output_projection);
            let output_column_names = projection_names(&output_projection);
            let projection_mask = ProjectionMask::roots(schema_descr, output_projection);
            let read_projection_mask = ProjectionMask::roots(schema_descr, read_projection);
            let pred_mask = ProjectionMask::roots(
                schema_descr,
                filter_type.filter_projection().iter().copied(),
            );

            for strategy in strategies {
                let bench_id = BenchmarkId::new(
                    format!("{filter_type}/{projection_case}"),
                    strategy.to_string(),
                );
                let rt_captured = rt.handle().clone();

                group.bench_function(bench_id, |b| {
                    b.iter(|| {
                        let reader = reader.clone();
                        let pred_mask = pred_mask.clone();
                        let projection_mask = projection_mask.clone();
                        let read_projection_mask = read_projection_mask.clone();
                        let output_column_names = output_column_names.clone();

                        rt_captured.block_on(async {
                            match strategy {
                                AsyncStrategy::FullPostFilter => {
                                    benchmark_async_reader_post_filter(
                                        reader,
                                        read_projection_mask,
                                        output_column_names,
                                        filter_type,
                                    )
                                    .await
                                }
                                AsyncStrategy::PushdownAuto => {
                                    let row_filter = row_filter_for(filter_type, pred_mask);
                                    benchmark_async_reader_with_policy(
                                        reader,
                                        projection_mask,
                                        row_filter,
                                        RowSelectionPolicy::default(),
                                    )
                                    .await
                                }
                                AsyncStrategy::PushdownSelectors => {
                                    let row_filter = row_filter_for(filter_type, pred_mask);
                                    benchmark_async_reader_with_policy(
                                        reader,
                                        projection_mask,
                                        row_filter,
                                        RowSelectionPolicy::Selectors,
                                    )
                                    .await
                                }
                                AsyncStrategy::PushdownMask => {
                                    let row_filter = row_filter_for(filter_type, pred_mask);
                                    benchmark_async_reader_with_policy(
                                        reader,
                                        projection_mask,
                                        row_filter,
                                        RowSelectionPolicy::Mask,
                                    )
                                    .await
                                }
                            }
                        })
                    });
                });
            }
        }
    }
}

/// A small async-only matrix that isolates the cases most relevant to the
/// row-filter Auto policy. This is intentionally narrower than
/// [`benchmark_async_strategy_matrix`]: it keeps the benchmark output focused
/// on cases where later PRs may teach `Auto` to switch execution modes or
/// explicitly keep predicate pushdown.
///
/// The `profile_*` cases are derived from DataFusion ClickBench and TPC-DS
/// comparisons. They keep the reader-level shapes worth tracking while
/// excluding query regressions that did not construct a Parquet `RowFilter`.
fn benchmark_async_auto_policy_focus(c: &mut Criterion) {
    const SMALL_TOTAL_ROWS: usize = 20_000;
    const SMALL_ROW_GROUP_SIZE: usize = 5_000;

    let parquet_file = Bytes::from(write_parquet_file());
    let small_parquet_file = Bytes::from(write_parquet_file_with_rows(
        SMALL_TOTAL_ROWS,
        SMALL_ROW_GROUP_SIZE,
    ));
    let cases = [
        AsyncFocusCase::new(
            "utf8_non_empty",
            parquet_file.clone(),
            FilterType::Utf8ViewNonEmpty,
            ProjectionCase::ExcludeFilterColumn,
        ),
        AsyncFocusCase::new(
            "utf8_non_empty",
            parquet_file.clone(),
            FilterType::Utf8ViewNonEmpty,
            ProjectionCase::AllColumns,
        ),
        AsyncFocusCase::new(
            "high_selectivity_float64",
            parquet_file.clone(),
            FilterType::UnselectiveUnclustered,
            ProjectionCase::ExcludeFilterColumn,
        ),
        AsyncFocusCase::new(
            "high_selectivity_ts_clustered",
            parquet_file.clone(),
            FilterType::UnselectiveClustered,
            ProjectionCase::ExcludeFilterColumn,
        ),
        AsyncFocusCase::new(
            "fragmented_int64_10pct",
            parquet_file.clone(),
            FilterType::ModeratelySelectiveUnclustered,
            ProjectionCase::ExcludeFilterColumn,
        ),
        AsyncFocusCase::new(
            "selective_float64_1pct",
            parquet_file.clone(),
            FilterType::SelectiveUnclustered,
            ProjectionCase::ExcludeFilterColumn,
        ),
        AsyncFocusCase::new(
            "profile_q37_scalar_utf8",
            parquet_file.clone(),
            FilterType::ClickBenchQ37ScalarPrefix,
            ProjectionCase::Utf8Only,
        ),
        // Historical Q6 focus case: cheap fixed-width predicate before the
        // unprojected variable-width predicate.
        AsyncFocusCase::new(
            "profile_q6_mixed_predicates",
            parquet_file.clone(),
            FilterType::ClickBenchQ6MixedPredicates,
            ProjectionCase::Float64Only,
        ),
        AsyncFocusCase::new(
            "profile_varwidth_then_fixed_prefix",
            parquet_file.clone(),
            FilterType::ClickBenchQ6VarWidthFirst,
            ProjectionCase::Float64Only,
        ),
        AsyncFocusCase::new(
            "profile_q40_scalar_group_by",
            parquet_file.clone(),
            FilterType::ClickBenchQ40ScalarGroupBy,
            ProjectionCase::Float64AndTs,
        ),
        AsyncFocusCase::new(
            "profile_q41_sparse_fixed_output",
            parquet_file.clone(),
            FilterType::ClickBenchQ41SparseFixedOutput,
            ProjectionCase::Float64Only,
        ),
        AsyncFocusCase::new(
            "profile_tpcds_q41_complex_or",
            parquet_file.clone(),
            FilterType::TpcdsQ41ComplexOr,
            ProjectionCase::Float64Only,
        ),
        AsyncFocusCase::new(
            "profile_tpcds_q20_projected_dynamic_filters",
            parquet_file.clone(),
            FilterType::TpcdsQ20ProjectedDynamicFilters,
            ProjectionCase::FixedColumns,
        ),
        AsyncFocusCase::new(
            "profile_q21_projected_predicate_fixed_output",
            parquet_file.clone(),
            FilterType::TpcdsQ21ProjectedFixedOutput,
            ProjectionCase::FixedColumns,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_5pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate5Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_8pct_filter_only",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate8Pct,
            ProjectionCase::FilterColumnsOnly,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_8pct_fixed_output",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate8Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_8pct_varwidth_output",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate8Pct,
            ProjectionCase::Int64AndUtf8,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_10pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate10Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_20pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate20Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_20pct_varwidth_output",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate20Pct,
            ProjectionCase::Int64AndUtf8,
        ),
        AsyncFocusCase::new(
            "profile_projected_ts_8pct_fixed_output",
            parquet_file.clone(),
            FilterType::ProjectedTs8PctClustered,
            ProjectionCase::Float64AndTs,
        ),
        AsyncFocusCase::new(
            "profile_projected_ts_8pct_varwidth_output",
            parquet_file.clone(),
            FilterType::ProjectedTs8PctClustered,
            ProjectionCase::TsAndUtf8,
        ),
        AsyncFocusCase::new(
            "profile_projected_ts_20pct_fixed_output",
            parquet_file.clone(),
            FilterType::ProjectedTs20PctClustered,
            ProjectionCase::Float64AndTs,
        ),
        AsyncFocusCase::new(
            "profile_projected_ts_20pct_varwidth_output",
            parquet_file.clone(),
            FilterType::ProjectedTs20PctClustered,
            ProjectionCase::TsAndUtf8,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_30pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate30Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_40pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate40Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q2_projected_predicate_50pct",
            parquet_file.clone(),
            FilterType::TpcdsQ2ProjectedPredicate50Pct,
            ProjectionCase::Int64AndFloat64,
        ),
        AsyncFocusCase::new(
            "profile_q1_count_only",
            parquet_file.clone(),
            FilterType::ClickBenchQ41SparseFixedOutput,
            ProjectionCase::CountOnly,
        ),
        AsyncFocusCase::new(
            "profile_q19_no_defer",
            parquet_file.clone(),
            FilterType::PointLookup,
            ProjectionCase::FilterColumnsOnly,
        ),
        AsyncFocusCase::new(
            "profile_sparse_fixed_deferred_output",
            parquet_file.clone(),
            FilterType::PointLookup,
            ProjectionCase::Float64Only,
        ),
        AsyncFocusCase::new(
            "profile_tpcds_sparse_projected_fact_scan",
            parquet_file.clone(),
            FilterType::TpcdsSparseProjectedFactScan,
            ProjectionCase::FixedColumns,
        ),
        AsyncFocusCase::new(
            "profile_q83_sparse_utf8_projected",
            parquet_file.clone(),
            FilterType::Utf8ViewMissing,
            ProjectionCase::AllColumns,
        ),
        AsyncFocusCase::new(
            "profile_small_scalar_no_defer",
            small_parquet_file.clone(),
            FilterType::ModeratelySelectiveUnclustered,
            ProjectionCase::FilterColumnsOnly,
        ),
        AsyncFocusCase::new(
            "profile_small_q37_scalar_utf8",
            small_parquet_file,
            FilterType::ClickBenchQ37ScalarPrefix,
            ProjectionCase::Utf8Only,
        ),
        AsyncFocusCase::new(
            "profile_q9_quantity_count",
            parquet_file.clone(),
            FilterType::TpcdsQ9QuantityRange,
            ProjectionCase::FilterColumnsOnly,
        ),
        AsyncFocusCase::new(
            "profile_q9_quantity_avg",
            parquet_file,
            FilterType::TpcdsQ9QuantityRange,
            ProjectionCase::Float64Only,
        ),
    ];
    let strategies = [
        AsyncStrategy::FullPostFilter,
        AsyncStrategy::PushdownAuto,
        AsyncStrategy::PushdownMask,
        AsyncStrategy::PushdownSelectors,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_row_filter_async_auto_policy_focus");

    for case in cases {
        benchmark_async_focus_case(&mut group, &rt, case, &strategies);
    }
}

/// Isolate projected scans that do not construct a [`RowFilter`].
///
/// This tracks the reader-level shape seen in TPC-DS Q83 return-table scans:
/// a narrow primitive projection where row-level pushdown metrics are zero.
/// It deliberately lives outside the adaptive-materialization matrix because there is no
/// filter strategy to choose.
fn benchmark_projection_scan_focus(c: &mut Criterion) {
    let parquet_file = Bytes::from(write_parquet_file());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_projection_scan_focus");

    let case_name = "profile_q83_return_scan_primitives";
    let projection = vec![0, 1, 3];
    let reader = InMemoryReader::try_new(&parquet_file).unwrap();
    let metadata = Arc::clone(reader.metadata());
    let schema_descr = metadata.file_metadata().schema_descr();
    let projection_mask = ProjectionMask::roots(schema_descr, projection);

    let bench_id = BenchmarkId::new(case_name, "async");
    let rt_captured = rt.handle().clone();
    group.bench_function(bench_id, |b| {
        b.iter(|| {
            let reader = reader.clone();
            let projection_mask = projection_mask.clone();
            rt_captured.block_on(benchmark_async_reader_projected(reader, projection_mask));
        });
    });

    let bench_id = BenchmarkId::new(case_name, "sync");
    group.bench_function(bench_id, |b| {
        b.iter(|| {
            let reader = reader.clone();
            let projection_mask = projection_mask.clone();
            benchmark_sync_reader_projected(reader, projection_mask);
        });
    });
}

struct AsyncFocusCase {
    case_name: &'static str,
    parquet_file: Bytes,
    filter_type: FilterType,
    projection_case: ProjectionCase,
}

impl AsyncFocusCase {
    fn new(
        case_name: &'static str,
        parquet_file: Bytes,
        filter_type: FilterType,
        projection_case: ProjectionCase,
    ) -> Self {
        Self {
            case_name,
            parquet_file,
            filter_type,
            projection_case,
        }
    }
}

fn benchmark_async_focus_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &tokio::runtime::Runtime,
    case: AsyncFocusCase,
    strategies: &[AsyncStrategy],
) {
    let AsyncFocusCase {
        case_name,
        parquet_file,
        filter_type,
        projection_case,
    } = case;

    let reader = InMemoryReader::try_new(&parquet_file).unwrap();
    let metadata = Arc::clone(reader.metadata());
    let schema_descr = metadata.file_metadata().schema_descr();
    let output_projection = output_projection_for(filter_type, &projection_case);
    let read_projection = full_post_filter_read_projection(filter_type, &output_projection);
    let output_column_names = projection_names(&output_projection);
    let projection_mask = ProjectionMask::roots(schema_descr, output_projection);
    let read_projection_mask = ProjectionMask::roots(schema_descr, read_projection);
    let pred_mask = ProjectionMask::roots(
        schema_descr,
        filter_type.filter_projection().iter().copied(),
    );
    let q6_int64_pred_mask = ProjectionMask::roots(schema_descr, [0]);
    let q6_utf8_pred_mask = ProjectionMask::roots(schema_descr, [2]);
    let q41_int64_pred_mask = ProjectionMask::roots(schema_descr, [0]);
    let q41_ts_pred_mask = ProjectionMask::roots(schema_descr, [3]);
    let q40_float64_pred_mask = ProjectionMask::roots(schema_descr, [1]);

    for strategy in strategies.iter().copied() {
        let bench_id = BenchmarkId::new(
            format!("{case_name}/{projection_case}"),
            strategy.to_string(),
        );
        let rt_captured = rt.handle().clone();

        group.bench_function(bench_id, |b| {
            b.iter(|| {
                let reader = reader.clone();
                let pred_mask = pred_mask.clone();
                let q6_int64_pred_mask = q6_int64_pred_mask.clone();
                let q6_utf8_pred_mask = q6_utf8_pred_mask.clone();
                let q41_int64_pred_mask = q41_int64_pred_mask.clone();
                let q41_ts_pred_mask = q41_ts_pred_mask.clone();
                let q40_float64_pred_mask = q40_float64_pred_mask.clone();
                let projection_mask = projection_mask.clone();
                let read_projection_mask = read_projection_mask.clone();
                let output_column_names = output_column_names.clone();

                rt_captured.block_on(async {
                    match strategy {
                        AsyncStrategy::FullPostFilter => {
                            benchmark_async_reader_post_filter(
                                reader,
                                read_projection_mask,
                                output_column_names,
                                filter_type,
                            )
                            .await
                        }
                        AsyncStrategy::PushdownAuto => {
                            let row_filter = row_filter_for_focus_case(
                                filter_type,
                                pred_mask,
                                q6_int64_pred_mask,
                                q6_utf8_pred_mask,
                                q41_int64_pred_mask,
                                q41_ts_pred_mask,
                                q40_float64_pred_mask,
                            );
                            benchmark_async_reader_with_policy(
                                reader,
                                projection_mask,
                                row_filter,
                                RowSelectionPolicy::default(),
                            )
                            .await
                        }
                        AsyncStrategy::PushdownSelectors => {
                            let row_filter = row_filter_for_focus_case(
                                filter_type,
                                pred_mask,
                                q6_int64_pred_mask,
                                q6_utf8_pred_mask,
                                q41_int64_pred_mask,
                                q41_ts_pred_mask,
                                q40_float64_pred_mask,
                            );
                            benchmark_async_reader_with_policy(
                                reader,
                                projection_mask,
                                row_filter,
                                RowSelectionPolicy::Selectors,
                            )
                            .await
                        }
                        AsyncStrategy::PushdownMask => {
                            let row_filter = row_filter_for_focus_case(
                                filter_type,
                                pred_mask,
                                q6_int64_pred_mask,
                                q6_utf8_pred_mask,
                                q41_int64_pred_mask,
                                q41_ts_pred_mask,
                                q40_float64_pred_mask,
                            );
                            benchmark_async_reader_with_policy(
                                reader,
                                projection_mask,
                                row_filter,
                                RowSelectionPolicy::Mask,
                            )
                            .await
                        }
                    }
                })
            });
        });
    }
}

fn output_projection_for(filter_type: FilterType, projection_case: &ProjectionCase) -> Vec<usize> {
    let filter_columns = filter_type.filter_projection();
    match projection_case {
        ProjectionCase::AllColumns | ProjectionCase::ExcludeFilterColumn => COLUMN_NAMES
            .iter()
            .enumerate()
            .map(|(idx, _)| idx)
            .filter(move |idx| {
                matches!(projection_case, ProjectionCase::AllColumns)
                    || !filter_columns.contains(idx)
            })
            .collect(),
        ProjectionCase::FilterColumnsOnly => filter_columns.to_vec(),
        ProjectionCase::CountOnly => vec![],
        ProjectionCase::FixedColumns => vec![0, 1, 3],
        ProjectionCase::Float64AndTs => vec![1, 3],
        ProjectionCase::Float64Only => vec![1],
        ProjectionCase::Int64AndFloat64 => vec![0, 1],
        ProjectionCase::Int64AndUtf8 => vec![0, 2],
        ProjectionCase::TsAndUtf8 => vec![2, 3],
        ProjectionCase::Utf8Only => vec![2],
    }
}

fn full_post_filter_read_projection(
    filter_type: FilterType,
    output_projection: &[usize],
) -> Vec<usize> {
    let mut read_projection = output_projection.to_vec();
    for filter_idx in filter_type.filter_projection() {
        if !read_projection.contains(filter_idx) {
            read_projection.push(*filter_idx);
        }
    }
    read_projection.sort_unstable();
    read_projection
}

fn projection_names(projection: &[usize]) -> Vec<&'static str> {
    projection.iter().map(|idx| COLUMN_NAMES[*idx]).collect()
}

fn row_filter_for(filter_type: FilterType, pred_mask: ProjectionMask) -> RowFilter {
    let filter = ArrowPredicateFn::new(pred_mask, move |batch| filter_type.filter_batch(&batch));
    RowFilter::new(vec![Box::new(filter)])
}

fn row_filter_for_focus_case(
    filter_type: FilterType,
    pred_mask: ProjectionMask,
    q6_int64_pred_mask: ProjectionMask,
    q6_utf8_pred_mask: ProjectionMask,
    q41_int64_pred_mask: ProjectionMask,
    q41_ts_pred_mask: ProjectionMask,
    q40_float64_pred_mask: ProjectionMask,
) -> RowFilter {
    match filter_type {
        FilterType::ClickBenchQ6MixedPredicates | FilterType::ClickBenchQ6VarWidthFirst => {
            let int64_filter =
                ArrowPredicateFn::new(q6_int64_pred_mask, move |batch: RecordBatch| {
                    let int64 = batch.column(batch.schema().index_of("int64")?);
                    eq(int64, &Int64Array::new_scalar(9999))
                });
            let utf8_filter =
                ArrowPredicateFn::new(q6_utf8_pred_mask, move |batch: RecordBatch| {
                    let utf8 = batch.column(batch.schema().index_of("utf8View")?);
                    neq(utf8, &StringViewArray::new_scalar(""))
                });

            match filter_type {
                FilterType::ClickBenchQ6MixedPredicates => {
                    RowFilter::new(vec![Box::new(int64_filter), Box::new(utf8_filter)])
                }
                FilterType::ClickBenchQ6VarWidthFirst => {
                    RowFilter::new(vec![Box::new(utf8_filter), Box::new(int64_filter)])
                }
                _ => unreachable!(),
            }
        }
        FilterType::ClickBenchQ40ScalarGroupBy => {
            let int64_filter =
                ArrowPredicateFn::new(q41_int64_pred_mask, move |batch: RecordBatch| {
                    let int64 = batch.column(batch.schema().index_of("int64")?);
                    eq(int64, &Int64Array::new_scalar(62))
                });
            let float64_filter =
                ArrowPredicateFn::new(q40_float64_pred_mask, move |batch: RecordBatch| {
                    let float64 = batch.column(batch.schema().index_of("float64")?);
                    gt(float64, &Float64Array::new_scalar(10.0))
                });
            let ts_filter = ArrowPredicateFn::new(q41_ts_pred_mask, move |batch: RecordBatch| {
                let ts = batch.column(batch.schema().index_of("ts")?);
                lt(ts, &TimestampMillisecondArray::new_scalar(9000))
            });

            RowFilter::new(vec![
                Box::new(int64_filter),
                Box::new(float64_filter),
                Box::new(ts_filter),
            ])
        }
        FilterType::ClickBenchQ41SparseFixedOutput
        | FilterType::TpcdsQ20ProjectedDynamicFilters
        | FilterType::TpcdsQ21ProjectedFixedOutput => {
            let int64_filter =
                ArrowPredicateFn::new(q41_int64_pred_mask, move |batch: RecordBatch| {
                    let int64 = batch.column(batch.schema().index_of("int64")?);
                    let scalar = match filter_type {
                        FilterType::TpcdsQ20ProjectedDynamicFilters => 12,
                        _ => 8,
                    };
                    lt(int64, &Int64Array::new_scalar(scalar))
                });
            let ts_filter = ArrowPredicateFn::new(q41_ts_pred_mask, move |batch: RecordBatch| {
                let ts = batch.column(batch.schema().index_of("ts")?);
                lt(ts, &TimestampMillisecondArray::new_scalar(9000))
            });

            RowFilter::new(vec![Box::new(int64_filter), Box::new(ts_filter)])
        }
        _ => row_filter_for(filter_type, pred_mask),
    }
}

#[derive(Clone, Copy)]
enum NestedFilterType {
    AlwaysTrueTag,
    TagNotZero,
}

impl std::fmt::Display for NestedFilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlwaysTrueTag => write!(f, "always_true_tag"),
            Self::TagNotZero => write!(f, "tag_not_zero"),
        }
    }
}

impl NestedFilterType {
    fn filter_batch(self, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
        match self {
            Self::AlwaysTrueTag => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
            Self::TagNotZero => {
                let tag = batch.column(batch.schema().index_of("tag")?);
                let scalar = StringViewArray::new_scalar("tag_0");
                neq(tag, &scalar)
            }
        }
    }
}

fn nested_row_filter_for(filter_type: NestedFilterType, pred_mask: ProjectionMask) -> RowFilter {
    let filter = ArrowPredicateFn::new(pred_mask, move |batch| filter_type.filter_batch(&batch));
    RowFilter::new(vec![Box::new(filter)])
}

/// Use async API
async fn benchmark_async_reader(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .build()
        .unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap(); // consume the batches, no buffering
    }
}

async fn benchmark_async_reader_with_policy(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
    row_selection_policy: RowSelectionPolicy,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .with_row_selection_policy(row_selection_policy)
        .build()
        .unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap(); // consume the batches, no buffering
    }
}

async fn benchmark_async_reader_post_filter(
    reader: InMemoryReader,
    read_projection: ProjectionMask,
    output_column_names: Vec<&'static str>,
    filter_type: FilterType,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(read_projection)
        .build()
        .unwrap();

    while let Some(b) = stream.next().await {
        let batch = b.unwrap();
        let filter = filter_type.filter_batch(&batch).unwrap();
        let filtered = arrow_select::filter::filter_record_batch(&batch, &filter).unwrap();
        let output_projection = output_column_names
            .iter()
            .map(|name| filtered.schema().index_of(name).unwrap())
            .collect::<Vec<_>>();
        let output = filtered.project(&output_projection).unwrap();
        std::hint::black_box(output.num_rows());
    }
}

async fn benchmark_async_reader_post_filter_nested(
    reader: InMemoryReader,
    read_projection: ProjectionMask,
    output_column_names: &[&str],
    filter_type: NestedFilterType,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(read_projection)
        .build()
        .unwrap();

    while let Some(b) = stream.next().await {
        let batch = b.unwrap();
        let filter = filter_type.filter_batch(&batch).unwrap();
        let filtered = arrow_select::filter::filter_record_batch(&batch, &filter).unwrap();
        let output_projection = output_column_names
            .iter()
            .map(|name| filtered.schema().index_of(name).unwrap())
            .collect::<Vec<_>>();
        let output = filtered.project(&output_projection).unwrap();
        std::hint::black_box(output.num_rows());
    }
}

async fn benchmark_async_reader_projected(reader: InMemoryReader, projection_mask: ProjectionMask) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .build()
        .unwrap();
    while let Some(b) = stream.next().await {
        let batch = b.unwrap();
        std::hint::black_box(batch.num_rows());
    }
}

/// Like [`benchmark_async_reader`] but also threads `with_limit(limit)` into
/// the stream builder. Used by the `LIMIT` benchmark below.
async fn benchmark_async_reader_with_limit(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
    limit: usize,
) {
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .with_limit(limit)
        .build()
        .unwrap();
    while let Some(b) = stream.next().await {
        b.unwrap(); // consume the batches, no buffering
    }
}

/// Use sync API
fn benchmark_sync_reader(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
) {
    let stream = ParquetRecordBatchReaderBuilder::try_new(reader.into_inner())
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .build()
        .unwrap();
    for b in stream {
        b.unwrap(); // consume the batches, no buffering
    }
}

fn benchmark_sync_reader_with_policy(
    reader: InMemoryReader,
    projection_mask: ProjectionMask,
    row_filter: RowFilter,
    row_selection_policy: RowSelectionPolicy,
) {
    let stream = ParquetRecordBatchReaderBuilder::try_new(reader.into_inner())
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .with_row_filter(row_filter)
        .with_row_selection_policy(row_selection_policy)
        .build()
        .unwrap();
    for b in stream {
        b.unwrap(); // consume the batches, no buffering
    }
}

fn benchmark_sync_reader_post_filter(
    reader: InMemoryReader,
    read_projection: ProjectionMask,
    output_column_names: Vec<&'static str>,
    filter_type: FilterType,
) {
    let stream = ParquetRecordBatchReaderBuilder::try_new(reader.into_inner())
        .unwrap()
        .with_batch_size(8192)
        .with_projection(read_projection)
        .build()
        .unwrap();

    for b in stream {
        let batch = b.unwrap();
        let filter = filter_type.filter_batch(&batch).unwrap();
        let filtered = arrow_select::filter::filter_record_batch(&batch, &filter).unwrap();
        let output_projection = output_column_names
            .iter()
            .map(|name| filtered.schema().index_of(name).unwrap())
            .collect::<Vec<_>>();
        let output = filtered.project(&output_projection).unwrap();
        std::hint::black_box(output.num_rows());
    }
}

fn benchmark_sync_reader_projected(reader: InMemoryReader, projection_mask: ProjectionMask) {
    let stream = ParquetRecordBatchReaderBuilder::try_new(reader.into_inner())
        .unwrap()
        .with_batch_size(8192)
        .with_projection(projection_mask)
        .build()
        .unwrap();

    for b in stream {
        let batch = b.unwrap();
        std::hint::black_box(batch.num_rows());
    }
}

/// Adapter to read asynchronously from in memory bytes and always loads the
/// metadata with page indexes.
#[derive(Debug, Clone)]
struct InMemoryReader {
    inner: Bytes,
    metadata: Arc<ParquetMetaData>,
}

impl InMemoryReader {
    fn try_new(inner: &Bytes) -> parquet::errors::Result<Self> {
        let mut metadata_reader =
            ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);
        metadata_reader.try_parse(inner)?;
        let metadata = metadata_reader.finish().map(Arc::new)?;

        Ok(Self {
            // clone of bytes is cheap -- increments a refcount
            inner: inner.clone(),
            metadata,
        })
    }

    fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    fn into_inner(self) -> Bytes {
        self.inner
    }
}

impl AsyncFileReader for InMemoryReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let data = self.inner.slice(range.start as usize..range.end as usize);
        async move { Ok(data) }.boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}

/// Benchmark filters with `LIMIT` short-circuit (`with_limit(N)`)
///
/// `PointLookup` is excluded because the filter has only 1 match in the
/// whole file; `LIMIT 10` is not binding.
fn benchmark_filters_with_limit(c: &mut Criterion) {
    const LIMIT: usize = 10;

    let parquet_file = Bytes::from(write_parquet_file());
    let filter_types = vec![
        FilterType::SelectiveUnclustered,
        FilterType::ModeratelySelectiveClustered,
        FilterType::ModeratelySelectiveUnclustered,
        FilterType::UnselectiveUnclustered,
        FilterType::UnselectiveClustered,
        FilterType::Utf8ViewNonEmpty,
        FilterType::Composite,
    ];
    let projection_cases = vec![
        ProjectionCase::AllColumns,
        ProjectionCase::ExcludeFilterColumn,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_row_filter_limit");

    for filter_type in filter_types {
        for proj_case in &projection_cases {
            let filter_col = filter_type.filter_projection().to_vec();
            let output_projection = output_projection_for(filter_type, proj_case);

            let reader = InMemoryReader::try_new(&parquet_file).unwrap();
            let metadata = Arc::clone(reader.metadata());
            let schema_descr = metadata.file_metadata().schema_descr();
            let projection_mask = ProjectionMask::roots(schema_descr, output_projection);
            let pred_mask = ProjectionMask::roots(schema_descr, filter_col);

            let benchmark_name = format!("{filter_type}/{proj_case}/limit{LIMIT}");

            // async variant
            let bench_id = BenchmarkId::new(benchmark_name.clone(), "async");
            let rt_handle = rt.handle().clone();
            let pred_mask_async = pred_mask.clone();
            let projection_mask_async = projection_mask.clone();
            let reader_async = reader.clone();
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let reader = reader_async.clone();
                    let pred_mask = pred_mask_async.clone();
                    let projection_mask = projection_mask_async.clone();
                    // RowFilter and ArrowPredicateFn are not Clone — fresh each iter.
                    let predicate = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                        Ok(filter_type.filter_batch(&batch).unwrap())
                    });
                    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
                    rt_handle.block_on(benchmark_async_reader_with_limit(
                        reader,
                        projection_mask,
                        row_filter,
                        LIMIT,
                    ));
                });
            });
        }
    }
}

/// Focused nested-output case for comparing manual post-filtering against
/// row-filter pushdown policies.
///
/// The predicate column is an unprojected variable-width scalar column, and the
/// output is a whole nested `Struct` root. This isolates the reader case enabled
/// by root-aware post-filter projection without requiring recursive nested-child
/// projection.
fn benchmark_async_nested_post_filter_focus(c: &mut Criterion) {
    let parquet_file = Bytes::from(write_nested_parquet_file_with_rows(
        TOTAL_ROWS,
        ROW_GROUP_SIZE,
    ));
    let strategies = [
        AsyncStrategy::FullPostFilter,
        AsyncStrategy::PushdownAuto,
        AsyncStrategy::PushdownMask,
        AsyncStrategy::PushdownSelectors,
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("arrow_reader_row_filter_async_nested_post_filter_focus");
    let reader = InMemoryReader::try_new(&parquet_file).unwrap();
    let metadata = Arc::clone(reader.metadata());
    let schema_descr = metadata.file_metadata().schema_descr();
    let output_projection = ProjectionMask::columns(schema_descr, ["payload"]);
    let read_projection = ProjectionMask::columns(schema_descr, ["tag", "payload"]);
    let pred_mask = ProjectionMask::columns(schema_descr, ["tag"]);
    let filter_cases = [
        NestedFilterType::AlwaysTrueTag,
        NestedFilterType::TagNotZero,
    ];

    for filter_case in filter_cases {
        for strategy in strategies {
            let bench_id = BenchmarkId::new(
                format!("whole_struct_output/{filter_case}"),
                strategy.to_string(),
            );
            let rt_captured = rt.handle().clone();
            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    let reader = reader.clone();
                    let pred_mask = pred_mask.clone();
                    let output_projection = output_projection.clone();
                    let read_projection = read_projection.clone();
                    rt_captured.block_on(async {
                        match strategy {
                            AsyncStrategy::FullPostFilter => {
                                benchmark_async_reader_post_filter_nested(
                                    reader,
                                    read_projection,
                                    &["payload"],
                                    filter_case,
                                )
                                .await
                            }
                            AsyncStrategy::PushdownAuto => {
                                benchmark_async_reader_with_policy(
                                    reader,
                                    output_projection,
                                    nested_row_filter_for(filter_case, pred_mask),
                                    RowSelectionPolicy::default(),
                                )
                                .await
                            }
                            AsyncStrategy::PushdownSelectors => {
                                benchmark_async_reader_with_policy(
                                    reader,
                                    output_projection,
                                    nested_row_filter_for(filter_case, pred_mask),
                                    RowSelectionPolicy::Selectors,
                                )
                                .await
                            }
                            AsyncStrategy::PushdownMask => {
                                benchmark_async_reader_with_policy(
                                    reader,
                                    output_projection,
                                    nested_row_filter_for(filter_case, pred_mask),
                                    RowSelectionPolicy::Mask,
                                )
                                .await
                            }
                        }
                    })
                });
            });
        }
    }
}

criterion_group!(
    benches,
    benchmark_filters_and_projections,
    benchmark_sync_strategy_matrix,
    benchmark_async_strategy_matrix,
    benchmark_async_auto_policy_focus,
    benchmark_projection_scan_focus,
    benchmark_filters_with_limit,
    benchmark_async_nested_post_filter_focus,
);
criterion_main!(benches);

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

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, TimestampMillisecondArray};
use arrow::compute::and;
use arrow::compute::kernels::cmp::{eq, gt, lt, neq};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::StringViewBuilder;
use arrow_array::StringViewArray;
use arrow_cast::pretty::pretty_format_batches;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;

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
/// Now, this column is used in one filter case.
fn create_utf8_view_array(size: usize, null_density: f32) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(44);
    for _ in 0..size {
        let choice = rng.random_range(0..100);
        if choice < (null_density * 100.0) as u32 {
            builder.append_value("");
        } else if choice < 25 {
            builder.append_value("const");
        } else {
            builder.append_value(random_string(&mut rng));
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
    let utf8_array = create_utf8_view_array(size, 0.2);
    let ts_array = create_ts_array(size);

    let arrays: Vec<ArrayRef> = vec![int64_array, float64_array, utf8_array, ts_array];
    RecordBatch::try_new(schema, arrays).unwrap()
}

/// Writes the RecordBatch to a temporary Parquet file and returns the file handle.
fn write_parquet_file() -> NamedTempFile {
    let batch = create_record_batch(100_000);
    println!("Batch created with {} rows", 100_000);
    println!(
        "First 100 rows:\n{}",
        pretty_format_batches(&[batch.clone().slice(0, 100)]).unwrap()
    );
    let schema = batch.schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    {
        let file_reopen = file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file_reopen, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    file
}

/// ProjectionCase defines the projection mode for the benchmark:
/// either projecting all columns or excluding the column that is used for filtering.
#[derive(Clone)]
enum ProjectionCase {
    AllColumns,
    ExcludeFilterColumn,
}

impl std::fmt::Display for ProjectionCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionCase::AllColumns => write!(f, "all_columns"),
            ProjectionCase::ExcludeFilterColumn => write!(f, "exclude_filter_column"),
        }
    }
}

/// FilterType encapsulates the different filter comparisons.
/// The variants correspond to the different filter patterns.
#[derive(Clone)]
enum FilterType {
    /// Here is the 6 filter types:
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
    ///
    /// "Point Lookup": selects a single row
    /// (1 RowSelection of 1 row)
    ///
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
    /// selective (1%) unclustered filter
    /// (1000 RowSelection of 10 rows each)
    ///
    ///
    /// ┌───────────────┐    ┌───────────────┐                 ┌───────────────┐    ┌───────────────┐
    /// │      ...      │    │               │                 │               │    │               │
    /// │               │    │               │                 │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │     ...       │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │                 │               │    │               │
    /// │               │    │               │                 │     ...       │    │               │
    /// │               │    │      ...      │                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │      ...      │    │               │                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │               │                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// └───────────────┘    └───────────────┘                 └───────────────┘    └───────────────┘
    /// moderately selective (10%) unclustered filter           moderately selective (10%) clustered filter
    /// (10000 RowSelection of 10 rows each)                    (10 RowSelections of 10,000 rows each)
    /// ┌───────────────┐    ┌───────────────┐                 ┌───────────────┐    ┌───────────────┐
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │     ...       │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │     ...       │    │               │
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │               │                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 │               │    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│
    /// │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│    │▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│                 └───────────────┘    └───────────────┘
    /// └───────────────┘    └───────────────┘
    /// unselective (99%) unclustered filter                   unselective (90%) clustered filter
    /// (99,000 RowSelections of 10 rows each)                 (99 RowSelection of 10,000 rows each)
    PointLookup,
    SelectiveUnclustered,
    ModeratelySelectiveClustered,
    ModeratelySelectiveUnclustered,
    UnselectiveUnclustered,
    UnselectiveClustered,
    /// The following are Composite and Utf8ViewNonEmpty filters, which is the additional to above 6 filters.
    Composite,
    Utf8ViewNonEmpty,
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
        };
        write!(f, "{}", s)
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
        }
    }
}

/// Benchmark filters and projections by reading the Parquet file.
/// This benchmark iterates over all individual filter types and two projection cases.
/// It measures the time to read and filter the Parquet file according to each scenario.
fn benchmark_filters_and_projections(c: &mut Criterion) {
    let parquet_file = write_parquet_file();
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
    let mut group = c.benchmark_group("arrow_reader_row_filter");

    for filter_type in filter_types.clone() {
        for proj_case in &projection_cases {
            // All indices corresponding to the 10 columns.
            let all_indices = vec![0, 1, 2, 3];
            // Determine the filter column index based on the filter type.
            let filter_col = match filter_type {
                FilterType::PointLookup => vec![0],
                FilterType::SelectiveUnclustered => vec![1],
                FilterType::ModeratelySelectiveClustered => vec![3],
                FilterType::ModeratelySelectiveUnclustered => vec![0],
                FilterType::UnselectiveUnclustered => vec![1],
                FilterType::UnselectiveClustered => vec![3],
                FilterType::Composite => vec![1, 3], // Use float64 column and ts column as representative for composite
                FilterType::Utf8ViewNonEmpty => vec![2],
            };

            // For the projection, either select all columns or exclude the filter column(s).
            let output_projection: Vec<usize> = match proj_case {
                ProjectionCase::AllColumns => all_indices.clone(),
                ProjectionCase::ExcludeFilterColumn => all_indices
                    .into_iter()
                    .filter(|i| !filter_col.contains(i))
                    .collect(),
            };

            let bench_id =
                BenchmarkId::new(format!("filter: {} proj: {}", filter_type, proj_case), "");
            group.bench_function(bench_id, |b| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.iter(|| {
                    let filter_type_inner = filter_type.clone();
                    rt.block_on(async {
                        let file = File::open(parquet_file.path()).await.unwrap();
                        let options = ArrowReaderOptions::new().with_page_index(true);
                        let builder =
                            ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                                .await
                                .unwrap()
                                .with_batch_size(8192);
                        let file_metadata = builder.metadata().file_metadata().clone();
                        let mask = ProjectionMask::roots(
                            file_metadata.schema_descr(),
                            output_projection.clone(),
                        );
                        let pred_mask =
                            ProjectionMask::roots(file_metadata.schema_descr(), filter_col.clone());
                        let filter = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                            Ok(filter_type_inner.filter_batch(&batch).unwrap())
                        });
                        let stream = builder
                            .with_projection(mask)
                            .with_row_filter(RowFilter::new(vec![Box::new(filter)]))
                            .build()
                            .unwrap();
                        stream.try_collect::<Vec<_>>().await.unwrap();
                    })
                });
            });
        }
    }
}

criterion_group!(benches, benchmark_filters_and_projections,);
criterion_main!(benches);

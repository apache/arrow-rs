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
//! As a side note, an additional composite benchmark is provided which demonstrates
//! the performance when applying two filters simultaneously (i.e. chaining row selectors).

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, TimestampMillisecondArray};
use arrow::compute::kernels::cmp::{eq, gt, neq};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::StringViewBuilder;
use arrow_array::StringViewArray;
use arrow_cast::pretty::pretty_format_batches;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::properties::WriterProperties;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;

/// Generates a random string (either short: 3–11 bytes or long: 13–20 bytes) with 50% probability.
/// This is used to fill non-selected rows in the filter columns.
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

/// Create a random array for a given field, generating data with fixed seed reproducibility.
/// - For Int64, random integers in [0, 100).
/// - For Float64, random floats in [0.0, 100.0).
/// - For Utf8View, a mix of empty strings, the constant "const", and random strings.
/// - For Timestamp, sequential timestamps in milliseconds.
fn create_random_array(
    field: &Field,
    size: usize,
    null_density: f32,
    _true_density: f32,
) -> arrow::error::Result<ArrayRef> {
    match field.data_type() {
        DataType::Int64 => {
            let mut rng = StdRng::seed_from_u64(42);
            let values: Vec<i64> = (0..size).map(|_| rng.random_range(0..100)).collect();
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let mut rng = StdRng::seed_from_u64(43);
            let values: Vec<f64> = (0..size).map(|_| rng.random_range(0.0..100.0)).collect();
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Utf8View => {
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
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values: Vec<i64> = (0..size as i64).collect();
            Ok(Arc::new(TimestampMillisecondArray::from(values)) as ArrayRef)
        }
        _ => unimplemented!("Field type not supported in create_random_array"),
    }
}

/// Create the "pt" column: one random index is set to "unique_point", the remaining rows are filled with random strings.
fn create_filter_array_pt(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(100);
    let unique_index = rng.random_range(0..size);
    for i in 0..size {
        if i == unique_index {
            builder.append_value("unique_point");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create the "sel" column: exactly 1% of rows (those with index % 100 == 0) are set to "selected",
/// while the other 99% of rows are filled with random strings.
fn create_filter_array_sel(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(101);
    for i in 0..size {
        if i % 100 == 0 {
            builder.append_value("selected");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create the "mod_clustered" column: in each 10,000-row block, the first 10 rows are set to "mod_clustered"
/// (simulating a clustered filter with 10 rows per block), and the rest are filled with random strings.
fn create_filter_array_mod_clustered(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let block_size = 10_000;
    let mut rng = StdRng::seed_from_u64(102);
    for i in 0..size {
        if (i % block_size) < 10 {
            builder.append_value("mod_clustered");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create the "mod_unclustered" column: exactly 10% of rows (those with index % 10 == 1)
/// are set to "mod_unclustered", while the remaining rows receive random strings.
fn create_filter_array_mod_unclustered(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(103);
    for i in 0..size {
        if i % 10 == 1 {
            builder.append_value("mod_unclustered");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create the "unsel_unclustered" column: exactly 99% of rows (those with index % 100 != 0)
/// are set to "unsel_unclustered", and the remaining 1% get random strings.
fn create_filter_array_unsel_unclustered(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(104);
    for i in 0..size {
        if i % 100 != 0 {
            builder.append_value("unsel_unclustered");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create the "unsel_clustered" column: in each 10,000-row block, rows with an offset >= 1000
/// are set to "unsel_clustered" (representing a clustered filter selecting 90% of the rows),
/// while rows with offset < 1000 are filled with random strings.
fn create_filter_array_unsel_clustered(size: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(size);
    let block_size = 10_000;
    let mut rng = StdRng::seed_from_u64(105);
    for i in 0..size {
        if (i % block_size) >= 1000 {
            builder.append_value("unsel_clustered");
        } else {
            builder.append_value(random_string(&mut rng));
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

/// Create an extended RecordBatch with 100K rows and ten columns.
/// The schema includes the original four columns and the six additional filter columns,
/// whose names have been updated to use "clustered" and "unclustered" as appropriate.
fn create_extended_batch(size: usize) -> RecordBatch {
    let fields = vec![
        Field::new("int64", DataType::Int64, false),
        Field::new("float64", DataType::Float64, false),
        Field::new("utf8View", DataType::Utf8View, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("pt", DataType::Utf8View, true),
        Field::new("sel", DataType::Utf8View, true),
        Field::new("mod_clustered", DataType::Utf8View, true),
        Field::new("mod_unclustered", DataType::Utf8View, true),
        Field::new("unsel_unclustered", DataType::Utf8View, true),
        Field::new("unsel_clustered", DataType::Utf8View, true),
    ];
    let schema = Arc::new(Schema::new(fields));

    let int64_array =
        create_random_array(&Field::new("int64", DataType::Int64, false), size, 0.0, 0.0).unwrap();
    let float64_array = create_random_array(
        &Field::new("float64", DataType::Float64, false),
        size,
        0.0,
        0.0,
    )
    .unwrap();
    let utf8_array = create_random_array(
        &Field::new("utf8View", DataType::Utf8View, true),
        size,
        0.2,
        0.5,
    )
    .unwrap();
    let ts_array = create_random_array(
        &Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        size,
        0.0,
        0.0,
    )
    .unwrap();

    let pt_array = create_filter_array_pt(size);
    let sel_array = create_filter_array_sel(size);
    let mod_clustered_array = create_filter_array_mod_clustered(size);
    let mod_unclustered_array = create_filter_array_mod_unclustered(size);
    let unsel_unclustered_array = create_filter_array_unsel_unclustered(size);
    let unsel_clustered_array = create_filter_array_unsel_clustered(size);

    let arrays: Vec<ArrayRef> = vec![
        int64_array,
        float64_array,
        utf8_array,
        ts_array,
        pt_array,
        sel_array,
        mod_clustered_array,
        mod_unclustered_array,
        unsel_unclustered_array,
        unsel_clustered_array,
    ];
    RecordBatch::try_new(schema, arrays).unwrap()
}

/// Create a RecordBatch with 100K rows and print a summary (first 100 rows) to the console.
fn make_record_batch() -> RecordBatch {
    let num_rows = 100_000;
    let batch = create_extended_batch(num_rows);
    println!("Batch created with {} rows", num_rows);
    println!(
        "First 100 rows:\n{}",
        pretty_format_batches(&[batch.clone().slice(0, 100)]).unwrap()
    );
    batch
}

/// Write the RecordBatch to a temporary Parquet file and return the file handle.
fn write_parquet_file() -> NamedTempFile {
    let batch = make_record_batch();
    let schema = batch.schema();
    let props = WriterProperties::builder().build();
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
    Utf8ViewNonEmpty,
    Utf8ViewConst,
    Int64EqZero,
    TimestampGt,
    PointLookup,
    SelectiveUnclustered,
    ModeratelySelectiveClustered,
    ModeratelySelectiveUnclustered,
    UnselectiveUnclustered,
    UnselectiveClustered,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use FilterType::*;
        let s = match self {
            Utf8ViewNonEmpty => "utf8View <> ''",
            Utf8ViewConst => "utf8View = 'const'",
            Int64EqZero => "int64 = 0",
            TimestampGt => "ts > 50_000",
            PointLookup => "Point Lookup",
            SelectiveUnclustered => "1% Unclustered Filter",
            ModeratelySelectiveClustered => "10% Clustered Filter",
            ModeratelySelectiveUnclustered => "10% Unclustered Filter",
            UnselectiveUnclustered => "99% Unclustered Filter",
            UnselectiveClustered => "90% Clustered Filter",
        };
        write!(f, "{}", s)
    }
}

impl FilterType {
    /// Applies the specified filter on the given record batch, returning a BooleanArray mask.
    /// Each filter uses its dedicated column and checks equality against a fixed string.
    fn filter_batch(&self, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
        use FilterType::*;
        match self {
            Utf8ViewNonEmpty => {
                let array = batch.column(batch.schema().index_of("utf8View").unwrap());
                let scalar = StringViewArray::new_scalar("");
                neq(array, &scalar)
            }
            Utf8ViewConst => {
                let array = batch.column(batch.schema().index_of("utf8View").unwrap());
                let scalar = StringViewArray::new_scalar("const");
                eq(array, &scalar)
            }
            Int64EqZero => {
                let array = batch.column(batch.schema().index_of("int64").unwrap());
                eq(array, &Int64Array::new_scalar(0))
            }
            TimestampGt => {
                let array = batch.column(batch.schema().index_of("ts").unwrap());
                gt(array, &TimestampMillisecondArray::new_scalar(50_000))
            }
            PointLookup => {
                let array = batch.column(batch.schema().index_of("pt").unwrap());
                let scalar = StringViewArray::new_scalar("unique_point");
                eq(array, &scalar)
            }
            SelectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("sel").unwrap());
                let scalar = StringViewArray::new_scalar("selected");
                eq(array, &scalar)
            }
            ModeratelySelectiveClustered => {
                let array = batch.column(batch.schema().index_of("mod_clustered").unwrap());
                let scalar = StringViewArray::new_scalar("mod_clustered");
                eq(array, &scalar)
            }
            ModeratelySelectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("mod_unclustered").unwrap());
                let scalar = StringViewArray::new_scalar("mod_unclustered");
                eq(array, &scalar)
            }
            UnselectiveUnclustered => {
                let array = batch.column(batch.schema().index_of("unsel_unclustered").unwrap());
                let scalar = StringViewArray::new_scalar("unsel_unclustered");
                eq(array, &scalar)
            }
            UnselectiveClustered => {
                let array = batch.column(batch.schema().index_of("unsel_clustered").unwrap());
                let scalar = StringViewArray::new_scalar("unsel_clustered");
                eq(array, &scalar)
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
        FilterType::Utf8ViewNonEmpty,
        FilterType::Utf8ViewConst,
        FilterType::Int64EqZero,
        FilterType::TimestampGt,
        FilterType::PointLookup,
        FilterType::SelectiveUnclustered,
        FilterType::ModeratelySelectiveClustered,
        FilterType::ModeratelySelectiveUnclustered,
        FilterType::UnselectiveUnclustered,
        FilterType::UnselectiveClustered,
    ];
    let projection_cases = vec![
        ProjectionCase::AllColumns,
        ProjectionCase::ExcludeFilterColumn,
    ];
    let mut group = c.benchmark_group("arrow_reader_row_filter");

    for filter_type in filter_types.clone() {
        for proj_case in &projection_cases {
            // All indices corresponding to the 10 columns.
            let all_indices = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            // Determine the filter column index based on the filter type.
            let filter_col = match filter_type {
                FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewConst => 2,
                FilterType::Int64EqZero => 0,
                FilterType::TimestampGt => 3,
                FilterType::PointLookup => 4,
                FilterType::SelectiveUnclustered => 5,
                FilterType::ModeratelySelectiveClustered => 6,
                FilterType::ModeratelySelectiveUnclustered => 7,
                FilterType::UnselectiveUnclustered => 8,
                FilterType::UnselectiveClustered => 9,
            };
            // For the projection, either select all columns or exclude the filter column.
            let output_projection: Vec<usize> = match proj_case {
                ProjectionCase::AllColumns => all_indices.clone(),
                ProjectionCase::ExcludeFilterColumn => all_indices
                    .into_iter()
                    .filter(|i| *i != filter_col)
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
                            ProjectionMask::roots(file_metadata.schema_descr(), vec![filter_col]);
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

/// Benchmark composite filters by applying two filters simultaneously.
/// This benchmark creates a composite row filter that ANDs two predicates:
/// one on the "sel" column (exactly 1% selected) and one on the "mod_clustered" column
/// (first 10 rows in each 10K block), then measures the performance of the combined filtering.
fn benchmark_composite_filters(c: &mut Criterion) {
    let parquet_file = write_parquet_file();
    let mut group = c.benchmark_group("composite_filter");

    // For composite filtering, we choose:
    //   - Filter1: SelectiveUnclustered on column "sel" (index 5)
    //   - Filter2: ModeratelySelectiveClustered on column "mod_clustered" (index 6)
    // These filters are applied sequentially (logical AND).
    let filter1_col = 5;
    let filter2_col = 6;
    let bench_id = BenchmarkId::new("Composite Filter: sel AND mod_clustered", "");
    group.bench_function(bench_id, |b| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        b.iter(|| {
            rt.block_on(async {
                let file = File::open(parquet_file.path()).await.unwrap();
                let options = ArrowReaderOptions::new().with_page_index(true);
                let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                    .await
                    .unwrap()
                    .with_batch_size(8192);
                let file_metadata = builder.metadata().file_metadata().clone();
                // For projection, we select all columns.
                let all_indices = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
                let mask = ProjectionMask::roots(file_metadata.schema_descr(), all_indices.clone());
                let pred_mask1 =
                    ProjectionMask::roots(file_metadata.schema_descr(), vec![filter1_col]);
                let pred_mask2 =
                    ProjectionMask::roots(file_metadata.schema_descr(), vec![filter2_col]);

                // Create first filter: applies the "sel" filter.
                let filter1 = ArrowPredicateFn::new(pred_mask1, move |batch: RecordBatch| {
                    let scalar = StringViewArray::new_scalar("selected");
                    eq(
                        batch.column(batch.schema().index_of("sel").unwrap()),
                        &scalar,
                    )
                });
                // Create second filter: applies the "mod_clustered" filter.
                let filter2 = ArrowPredicateFn::new(pred_mask2, move |batch: RecordBatch| {
                    let scalar = StringViewArray::new_scalar("mod_clustered");
                    eq(
                        batch.column(batch.schema().index_of("mod_clustered").unwrap()),
                        &scalar,
                    )
                });
                let composite_filter = RowFilter::new(vec![Box::new(filter1), Box::new(filter2)]);

                let stream = builder
                    .with_projection(mask)
                    .with_row_filter(composite_filter)
                    .build()
                    .unwrap();
                stream.try_collect::<Vec<_>>().await.unwrap();
            })
        });
    });
}

criterion_group!(
    benches,
    benchmark_filters_and_projections,
    benchmark_composite_filters
);
criterion_main!(benches);

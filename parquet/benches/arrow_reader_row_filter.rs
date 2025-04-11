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
//! pushdown filters is a two step process:
//!
//! 1. Build a filter mask by decoding and evaluating filter functions on
//!    the filter column(s).
//!
//! 2. Decode the rows that match the filter mask from the projected columns.
//!
//! The performance of this process depending on several factors, including:
//!
//! 1. How many rows are selected as well and how well clustered the results
//!    are, where the representation of the filter mask is important.
//! 2. If the same column is used for both filtering and projection, as the
//!    columns that appear in both filtering and projection are decoded twice.
//!
//! This benchmark helps measure the performance of these operations.
//!
//! [Efficient Filter Pushdown in Parquet]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
//!
//! # To run:
//! To run the benchmark, use `cargo bench --bench bench_filter_projection`.
//!
//! This benchmark creates a Parquet file in memory with 100K rows and four columns:
//!  - int64: random integers generated using a fixed seed (range: 0..100)
//!  - float64: random floating-point values generated using a fixed seed (range: 0.0..100.0)
//!  - utf8View: random strings (with some empty values and the constant "const").
//!    Randomly produces short strings (3-12 bytes) and long strings (13-20 bytes).
//!  - ts: sequential timestamps in milliseconds
//!
//! Filters tested:
//!  - utf8View <> '' (non-selective)
//!  - utf8View = 'const' (selective)
//!  - int64 = 0 (selective)
//!  - ts > 50_000 (non-selective)
//!
//! Projections tested:
//!  - All columns.
//!  - All columns except the one used for filtering.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, TimestampMillisecondArray};
use arrow::compute::kernels::cmp::{eq, gt, neq};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow_array::builder::StringViewBuilder;
use arrow_array::StringViewArray;
use arrow_cast::pretty::pretty_format_batches;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::properties::WriterProperties;
use tokio::fs::File;

/// Create a random array for a given field.
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
                    // Use empty string to represent a null value.
                    builder.append_value("");
                } else if choice < 25 {
                    builder.append_value("const");
                } else {
                    let is_long = rng.random_bool(0.5);
                    let len = if is_long {
                        rng.random_range(13..21)
                    } else {
                        rng.random_range(3..12)
                    };
                    let charset = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                    let s: String = (0..len)
                        .map(|_| {
                            let idx = rng.random_range(0..charset.len());
                            charset[idx] as char
                        })
                        .collect();
                    builder.append_value(&s);
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

/// Create a random RecordBatch from the given schema.
pub fn create_random_batch(
    schema: SchemaRef,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> arrow::error::Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| create_random_array(field, size, null_density, true_density))
        .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_match_field_names(false),
    )
}

/// Create a RecordBatch with 100K rows and four columns.
fn make_record_batch() -> RecordBatch {
    let num_rows = 100_000;
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
    let batch = create_random_batch(schema, num_rows, 0.2, 0.5).unwrap();

    println!("Batch created with {} rows", num_rows);
    println!(
        "First 100 rows:\n{}",
        pretty_format_batches(&[batch.clone().slice(0, 100)]).unwrap()
    );
    batch
}

/// Write the RecordBatch to a temporary Parquet file.
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

/// FilterType encapsulates the different filter comparisons.
#[derive(Clone)]
enum FilterType {
    Utf8ViewNonEmpty,
    Utf8ViewConst,
    Int64EqZero,
    TimestampGt,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterType::Utf8ViewNonEmpty => write!(f, "utf8View <> ''"),
            FilterType::Utf8ViewConst => write!(f, "utf8View = 'const'"),
            FilterType::Int64EqZero => write!(f, "int64 = 0"),
            FilterType::TimestampGt => write!(f, "ts > 50_000"),
        }
    }
}

impl FilterType {
    /// Filters the given batch according to self using Arrow compute kernels.
    /// Returns a BooleanArray where true indicates that the row satisfies the condition.
    fn filter_batch(&self, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
        match self {
            FilterType::Utf8ViewNonEmpty => {
                let array = batch.column(batch.schema().index_of("utf8View").unwrap());
                let string_view_scalar = StringViewArray::new_scalar("");
                let not_equals_empty = neq(array, &string_view_scalar)?;
                Ok(not_equals_empty)
            }
            FilterType::Utf8ViewConst => {
                let array = batch.column(batch.schema().index_of("utf8View").unwrap());
                let string_view_scalar = StringViewArray::new_scalar("const");
                let eq_const = eq(array, &string_view_scalar)?;
                Ok(eq_const)
            }
            FilterType::Int64EqZero => {
                let array = batch.column(batch.schema().index_of("int64").unwrap());
                let eq_zero = eq(array, &Int64Array::new_scalar(0))?;
                Ok(eq_zero)
            }
            FilterType::TimestampGt => {
                let array = batch.column(batch.schema().index_of("ts").unwrap());
                let gt_thresh = gt(array, &TimestampMillisecondArray::new_scalar(50_000))?;
                Ok(gt_thresh)
            }
        }
    }
}

/// ProjectionCase defines the projection mode.
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

fn benchmark_filters_and_projections(c: &mut Criterion) {
    let parquet_file = write_parquet_file();

    let filter_types: Vec<FilterType> = vec![
        FilterType::Utf8ViewNonEmpty,
        FilterType::Utf8ViewConst,
        FilterType::Int64EqZero,
        FilterType::TimestampGt,
    ];

    let projection_cases = vec![
        ProjectionCase::AllColumns,
        ProjectionCase::ExcludeFilterColumn,
    ];

    let mut group = c.benchmark_group("arrow_reader_row_filter");

    for filter_type in filter_types.iter().cloned() {
        for proj_case in &projection_cases {
            // All column indices: [0: int64, 1: float64, 2: utf8View, 3: ts]
            let all_indices = vec![0, 1, 2, 3];
            let filter_col = match filter_type {
                FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewConst => 2,
                FilterType::Int64EqZero => 0,
                FilterType::TimestampGt => 3,
            };
            let output_projection: Vec<usize> = match proj_case {
                ProjectionCase::AllColumns => all_indices.clone(),
                ProjectionCase::ExcludeFilterColumn => all_indices
                    .into_iter()
                    .filter(|i| *i != filter_col)
                    .collect(),
            };
            // For predicate pushdown, include the filter column.
            let predicate_projection: Vec<usize> = match filter_type {
                FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewConst => vec![2],
                FilterType::Int64EqZero => vec![0],
                FilterType::TimestampGt => vec![3],
            };

            let bench_id =
                BenchmarkId::new(format!("filter: {} proj: {}", filter_type, proj_case), "");

            group.bench_function(bench_id, |b| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.iter(|| {
                    // Clone filter_type inside the closure to avoid moving it
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
                        let pred_mask = ProjectionMask::roots(
                            file_metadata.schema_descr(),
                            predicate_projection.clone(),
                        );

                        let filter = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                            // Clone filter_type within the closure
                            let filter_type_inner = filter_type_inner.clone();
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

criterion_group!(benches, benchmark_filters_and_projections);
criterion_main!(benches);

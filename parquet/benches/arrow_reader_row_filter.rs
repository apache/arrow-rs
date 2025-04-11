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
//! This benchmark creates a Parquet file in memory with 100K rows and four columns:
//!  - int64: random integers generated using a fixed seed (range: 0..100)
//!  - float64: random floating-point values generated using a fixed seed (range: 0.0..100.0)
//!  - utf8View: random strings (with some empty values and the constant "const").
//!             Randomly produces short strings (3-12 bytes) and long strings (13-20 bytes).
//!  - ts: sequential timestamps in milliseconds
//!
//! Filters tested:
//!  - utf8View <> '' (no selective) %80
//!  - utf8View = 'const' (selective) %5
//!  - int64 = 0 (selective)
//!  - ts > 50_000 (no selective) %50
//!
//! Projections tested:
//!  - All columns.
//!  - All columns except the one used for filtering.
//!
//! To run the benchmark, use `cargo bench --bench bench_filter_projection`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use tempfile::NamedTempFile;

use arrow::array::{ArrayRef, Float64Array, Int64Array, TimestampMillisecondArray};
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

// Use Arrow compute kernels for filtering.
// Returns a BooleanArray where true indicates the row satisfies the condition.
fn filter_utf8_view_nonempty(
    batch: &RecordBatch,
) -> arrow::error::Result<arrow::array::BooleanArray> {
    let array = batch.column(batch.schema().index_of("utf8View").unwrap());
    let string_view_scalar = StringViewArray::new_scalar("");
    // Compare with empty string
    let not_equals_empty = neq(array, &string_view_scalar)?;
    Ok(not_equals_empty)
}

fn filter_utf8_view_const(batch: &RecordBatch) -> arrow::error::Result<arrow::array::BooleanArray> {
    let array = batch.column(batch.schema().index_of("utf8View").unwrap());
    let string_view_scalar = StringViewArray::new_scalar("const");
    let eq_const = eq(array, &string_view_scalar)?;
    Ok(eq_const)
}
fn filter_int64_eq_zero(batch: &RecordBatch) -> arrow::error::Result<arrow::array::BooleanArray> {
    let array = batch.column(batch.schema().index_of("int64").unwrap());
    let eq_zero = eq(array, &Int64Array::new_scalar(0))?;
    Ok(eq_zero)
}

fn filter_timestamp_gt(batch: &RecordBatch) -> arrow::error::Result<arrow::array::BooleanArray> {
    let array = batch.column(batch.schema().index_of("ts").unwrap());
    // For Timestamp arrays, use ScalarValue::TimestampMillisecond.
    let gt_thresh = gt(array, &TimestampMillisecondArray::new_scalar(50_000))?;
    Ok(gt_thresh)
}

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

fn benchmark_filters_and_projections(c: &mut Criterion) {
    let parquet_file = write_parquet_file();

    type FilterFn = fn(&RecordBatch) -> arrow::error::Result<arrow::array::BooleanArray>;
    let filter_funcs: Vec<(FilterType, FilterFn)> = vec![
        (FilterType::Utf8ViewNonEmpty, filter_utf8_view_nonempty),
        (FilterType::Utf8ViewConst, filter_utf8_view_const),
        (FilterType::Int64EqZero, filter_int64_eq_zero),
        (FilterType::TimestampGt, filter_timestamp_gt),
    ];

    let mut group = c.benchmark_group("arrow_reader_row_filter");

    for (filter_type, filter_fn) in filter_funcs.into_iter() {
        for proj_case in ["all_columns", "exclude_filter_column"].iter() {
            let all_indices = vec![0, 1, 2, 3];

            let output_projection: Vec<usize> = if *proj_case == "all_columns" {
                all_indices.clone()
            } else {
                all_indices
                    .into_iter()
                    .filter(|i| match filter_type {
                        FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewConst => *i != 2,
                        FilterType::Int64EqZero => *i != 0,
                        FilterType::TimestampGt => *i != 3,
                    })
                    .collect()
            };

            let predicate_projection: Vec<usize> = match filter_type {
                FilterType::Utf8ViewNonEmpty | FilterType::Utf8ViewConst => vec![2],
                FilterType::Int64EqZero => vec![0],
                FilterType::TimestampGt => vec![3],
            };

            let bench_id = BenchmarkId::new(
                format!("filter_case: {} project_case: {}", filter_type, proj_case),
                "",
            );

            group.bench_function(bench_id, |b| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.iter(|| {
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

                        let f = filter_fn;
                        let filter = ArrowPredicateFn::new(pred_mask, move |batch: RecordBatch| {
                            Ok(f(&batch).unwrap())
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

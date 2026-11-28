// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A [`RowFilter`] `value > threshold` on an ALP-encoded DOUBLE column, read
//! through the sync reader and the push decoder.
//!
//! `decode` uses a predicate that only evaluates batches, so the reader decodes
//! the column and calls it. `pushdown` uses the same predicate declaring its
//! comparison, so the reader evaluates it from the encoded pages.
//!
//! Case names are `layout/threshold/output/path/mode`:
//! - `clustered` values drift slowly, so most ALP vectors lie entirely on one
//!   side of the threshold. `shuffled` holds the same values in random order,
//!   so almost every vector straddles it.
//! - `p99` keeps 1% of rows, `p50` keeps half.
//! - `id` outputs only the `id` column. `id_value` also outputs `value`, which
//!   the push decoder can otherwise serve from its predicate cache, so only
//!   `id_value` runs through both paths.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, Scalar};
use arrow::compute::kernels::cmp::gt;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ColumnComparison, ComparisonOp,
    ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use rand::{RngExt, SeedableRng, rngs::StdRng};

const ROWS: usize = 1_000_000;
const ROW_GROUP_ROWS: usize = 100_000;

/// Two-decimal readings that drift slowly, like a sensor.
fn clustered_values() -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut reading = 2000i64;
    (0..ROWS)
        .map(|_| {
            reading += rng.random_range(-3..=3);
            reading as f64 / 100.0
        })
        .collect()
}

fn shuffled(values: &[f64]) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(7);
    let mut values = values.to_vec();
    for i in (1..values.len()).rev() {
        values.swap(i, rng.random_range(0..=i));
    }
    values
}

fn write_file(values: &[f64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap();
    let value = ColumnPath::from("value");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_column_dictionary_enabled(value.clone(), false)
        .set_column_encoding(value, Encoding::ALP)
        .build();
    let mut out = vec![];
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(out)
}

/// The value with `percent` of `values` below it.
fn percentile(values: &[f64], percent: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[(sorted.len() - 1) * percent / 100]
}

fn row_filter(metadata: &ArrowReaderMetadata, threshold: f64, pushdown: bool) -> RowFilter {
    let schema = metadata.metadata().file_metadata().schema_descr();
    let scalar = Scalar::new(Arc::new(Float64Array::from(vec![threshold])) as ArrayRef);
    let predicate = ArrowPredicateFn::new(ProjectionMask::columns(schema, ["value"]), {
        let scalar = scalar.clone();
        move |batch: RecordBatch| gt(batch.column(0), &scalar)
    });
    let predicate = if pushdown {
        predicate.with_comparison(ColumnComparison::new(ComparisonOp::Gt, scalar))
    } else {
        predicate
    };
    RowFilter::new(vec![Box::new(predicate)])
}

fn read_sync(
    data: &Bytes,
    metadata: &ArrowReaderMetadata,
    projection: &ProjectionMask,
    filter: RowFilter,
) -> usize {
    ParquetRecordBatchReaderBuilder::new_with_metadata(data.clone(), metadata.clone())
        .with_projection(projection.clone())
        .with_row_filter(filter)
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum()
}

fn read_push(
    data: &Bytes,
    metadata: &ArrowReaderMetadata,
    projection: &ProjectionMask,
    filter: RowFilter,
) -> usize {
    let mut decoder = ParquetPushDecoderBuilder::new_with_metadata(metadata.clone())
        .with_projection(projection.clone())
        .with_row_filter(filter)
        .build()
        .unwrap();
    let mut rows = 0;
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let buffers = ranges
                    .iter()
                    .map(|r| data.slice(r.start as usize..r.end as usize))
                    .collect();
                decoder.push_ranges(ranges, buffers).unwrap();
            }
            DecodeResult::Data(batch) => rows += batch.num_rows(),
            DecodeResult::Finished => return rows,
        }
    }
}

fn bench_alp_filter(c: &mut Criterion) {
    let clustered = clustered_values();
    let layouts = [
        ("clustered", clustered.clone()),
        ("shuffled", shuffled(&clustered)),
    ];

    let mut group = c.benchmark_group("arrow_reader_alp_filter");
    for (layout, values) in &layouts {
        let data = write_file(values);
        let metadata = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new()).unwrap();
        let schema = metadata.metadata().file_metadata().schema_descr();
        for percent in [99, 50] {
            let threshold = percentile(values, percent);
            for (output, columns) in [("id", &["id"][..]), ("id_value", &["id", "value"][..])] {
                let projection = ProjectionMask::columns(schema, columns.iter().copied());
                for (path, read) in [
                    ("sync", read_sync as fn(&_, &_, &_, _) -> usize),
                    ("push", read_push),
                ] {
                    // The push decoder differs from the sync reader through its
                    // predicate cache, which only matters when `value` is output.
                    if path == "push" && output == "id" {
                        continue;
                    }
                    let expected = read(
                        &data,
                        &metadata,
                        &projection,
                        row_filter(&metadata, threshold, false),
                    );
                    for (mode, pushdown) in [("decode", false), ("pushdown", true)] {
                        assert_eq!(
                            read(
                                &data,
                                &metadata,
                                &projection,
                                row_filter(&metadata, threshold, pushdown)
                            ),
                            expected
                        );
                        group.bench_function(
                            format!("{layout}/p{percent}/{output}/{path}/{mode}"),
                            |b| {
                                b.iter(|| {
                                    read(
                                        &data,
                                        &metadata,
                                        &projection,
                                        row_filter(&metadata, threshold, pushdown),
                                    )
                                })
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_alp_filter);
criterion_main!(benches);

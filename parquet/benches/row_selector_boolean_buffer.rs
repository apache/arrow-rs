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

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint;
use std::sync::Arc;

const TOTAL_ROWS: usize = 3_000_000;
const SELECTIVITY_CASES: &[Selectivity] = &[
    Selectivity::new("select01", 1, 100),
    Selectivity::new("select10", 1, 10),
    Selectivity::new("select33", 1, 3),
    Selectivity::new("select80", 4, 5),
];

/// Generates a deterministic random row mask with the specified selectivity.
fn generate_random_row_selection(total_rows: usize, selectivity: Selectivity) -> BooleanBuffer {
    let mut rng = StdRng::seed_from_u64(0x5E1EC7_u64 ^ selectivity.seed());
    let bools: Vec<bool> = (0..total_rows)
        .map(|_| rng.random_bool(selectivity.ratio()))
        .collect();
    BooleanBuffer::from(bools)
}

fn generate_fragmented_selection(total_rows: usize, selectivity: Selectivity) -> BooleanBuffer {
    let mut builder = BooleanBufferBuilder::new(total_rows);
    for row in 0..total_rows {
        builder.append(row % selectivity.denominator < selectivity.numerator);
    }
    builder.finish()
}

fn generate_clustered_selection(total_rows: usize, selectivity: Selectivity) -> BooleanBuffer {
    const RUN: usize = 8 * 1024;
    const JITTER: usize = RUN / 2;
    let mut rng = StdRng::seed_from_u64(0xC1057E_u64 ^ selectivity.seed());
    let mut builder = BooleanBufferBuilder::new(total_rows);
    let mut rows_remaining = total_rows;
    let mut run_idx = 0usize;

    while rows_remaining != 0 {
        let run_len = rng
            .random_range((RUN - JITTER)..=(RUN + JITTER))
            .min(rows_remaining);
        let selected = run_idx % selectivity.denominator < selectivity.numerator;
        builder.append_n(run_len, selected);
        rows_remaining -= run_len;
        run_idx += 1;
    }
    builder.finish()
}

fn boolean_array(mask: &BooleanBuffer) -> BooleanArray {
    BooleanArray::new(mask.clone(), None)
}

fn criterion_benchmark(c: &mut Criterion) {
    let patterns = build_patterns();

    let mut construction = c.benchmark_group("row_selector_boolean_buffer/construction");
    for case in &patterns {
        construction.bench_with_input(
            BenchmarkId::new("from_filters", case.label()),
            &case.mask,
            |b, mask| {
                b.iter(|| {
                    let array = boolean_array(mask);
                    let selection = RowSelection::from_filters(&[array]);
                    hint::black_box(selection);
                })
            },
        );

        construction.bench_with_input(
            BenchmarkId::new("from_boolean_buffer", case.label()),
            &case.mask,
            |b, mask| {
                b.iter(|| {
                    let selection = RowSelection::from_boolean_buffer(mask.clone());
                    hint::black_box(selection);
                })
            },
        );
    }
    construction.finish();

    let parquet_data = write_parquet_file(TOTAL_ROWS);
    let mut reader = c.benchmark_group("row_selector_boolean_buffer/reader");
    for case in &patterns {
        reader.bench_with_input(
            BenchmarkId::new("from_filters", case.label()),
            &case.mask,
            |b, mask| {
                b.iter(|| {
                    let selection = RowSelection::from_filters(&[boolean_array(mask)]);
                    let rows = read_rows(&parquet_data, selection);
                    hint::black_box(rows);
                })
            },
        );

        reader.bench_with_input(
            BenchmarkId::new("from_boolean_buffer", case.label()),
            &case.mask,
            |b, mask| {
                b.iter(|| {
                    let selection = RowSelection::from_boolean_buffer(mask.clone());
                    let rows = read_rows(&parquet_data, selection);
                    hint::black_box(rows);
                })
            },
        );
    }
    reader.finish();
}

fn build_patterns() -> Vec<BenchCase> {
    let mut patterns = Vec::new();
    for selectivity in SELECTIVITY_CASES {
        patterns.push(BenchCase::new(
            "fragmented",
            *selectivity,
            generate_fragmented_selection(TOTAL_ROWS, *selectivity),
        ));
        patterns.push(BenchCase::new(
            "clustered",
            *selectivity,
            generate_clustered_selection(TOTAL_ROWS, *selectivity),
        ));
        patterns.push(BenchCase::new(
            "random",
            *selectivity,
            generate_random_row_selection(TOTAL_ROWS, *selectivity),
        ));
    }
    patterns
}

fn write_parquet_file(total_rows: usize) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(
        (0..total_rows).map(|row| row as i32),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();

    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    Bytes::from(writer.into_inner().unwrap())
}

fn read_rows(parquet_data: &Bytes, selection: RowSelection) -> usize {
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data.clone())
        .unwrap()
        .with_row_selection(selection)
        .build()
        .unwrap();

    reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>()
}

struct BenchCase {
    pattern: &'static str,
    selectivity: Selectivity,
    mask: BooleanBuffer,
}

impl BenchCase {
    fn new(pattern: &'static str, selectivity: Selectivity, mask: BooleanBuffer) -> Self {
        Self {
            pattern,
            selectivity,
            mask,
        }
    }

    fn label(&self) -> String {
        format!("{}/{}", self.pattern, self.selectivity.label)
    }
}

#[derive(Clone, Copy)]
struct Selectivity {
    label: &'static str,
    numerator: usize,
    denominator: usize,
}

impl Selectivity {
    const fn new(label: &'static str, numerator: usize, denominator: usize) -> Self {
        Self {
            label,
            numerator,
            denominator,
        }
    }

    fn ratio(self) -> f64 {
        self.numerator as f64 / self.denominator as f64
    }

    fn seed(self) -> u64 {
        ((self.numerator as u64) << 32) ^ self.denominator as u64
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

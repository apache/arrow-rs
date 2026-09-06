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

//! Benchmark for chains of [`RowFilter`] predicates that share one projection.
//!
//! Consecutive same-projection predicates decode, or replay from the predicate
//! cache, the same column once per predicate, unless the reader fuses them.
//!
//! Case names are `type/layout/cache/predicates/profile`: the filter column
//! type; `fragmented` (one key per row) or `clustered` (each key repeated for
//! 128 rows); whether the filter column is also projected and served by the
//! predicate cache; the chain length, where `1` is a control; and the survivor
//! rates, where `all<N>` keeps N% per predicate, `early1` / `late1` keep 1% in
//! the first or last predicate and 99% elsewhere, and `run<N>` keeps half of
//! the runs of `N` rows to probe the selection representation threshold of 32.

use std::fmt::{Display, Formatter};
use std::hint::black_box;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    criterion_main,
};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter,
};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const NUM_ROWS: usize = 262_144;
const BATCH_SIZE: usize = 8192;
const CLUSTER_SIZE: i64 = 128;
/// Leaf index of the payload column projected by `uncached` cases.
const PAYLOAD_COLUMN: usize = 4;
const LAYOUTS: [&str; 4] = [
    "int64/fragmented",
    "int64/clustered",
    "string/fragmented",
    "string/clustered",
];
const PROFILES: [Profile; 4] = [
    Profile::Uniform(99),
    Profile::Uniform(50),
    Profile::EarlySelective,
    Profile::LateSelective,
];

#[derive(Clone, Copy, PartialEq)]
enum Profile {
    Uniform(u64),
    EarlySelective,
    LateSelective,
    RunLength(usize),
}

impl Profile {
    fn value(self, key: u64, index: usize) -> u64 {
        match self {
            Self::RunLength(length) => (((key / length as u64) >> index) & 1) * 99,
            _ => bucket(key, index),
        }
    }
}

impl Display for Profile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uniform(percent) => write!(f, "all{percent}"),
            Self::EarlySelective => write!(f, "early1"),
            Self::LateSelective => write!(f, "late1"),
            Self::RunLength(length) => write!(f, "run{length}"),
        }
    }
}

/// Pseudo-random bucket in `0..100`, independent per (key, predicate).
fn bucket(key: u64, predicate: usize) -> u64 {
    let mut value = key.wrapping_add((predicate as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    (value ^ (value >> 31)) % 100
}

fn string_value(key: i64, profile: Profile) -> String {
    let mut value = String::with_capacity(64);
    for predicate in 0..32 {
        let digit = profile.value(key as u64, predicate) as u8;
        value.push((b'0' + digit / 10) as char);
        value.push((b'0' + digit % 10) as char);
    }
    value
}

/// An in-memory Parquet file with its metadata parsed once.
struct Dataset {
    data: Bytes,
    metadata: ArrowReaderMetadata,
}

impl Dataset {
    fn generate(runtime: &tokio::runtime::Runtime, profile: Profile) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_fragmented", DataType::Int64, false),
            Field::new("int_clustered", DataType::Int64, false),
            Field::new("string_fragmented", DataType::Utf8, false),
            Field::new("string_clustered", DataType::Utf8, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(Some(NUM_ROWS / 2))
            .build();
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, schema.clone(), Some(properties)).unwrap();
        for start in (0..NUM_ROWS).step_by(BATCH_SIZE) {
            let keys: Vec<i64> = (start..start + BATCH_SIZE).map(|row| row as i64).collect();
            let clustered: Vec<i64> = keys.iter().map(|key| key / CLUSTER_SIZE).collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(clustered.clone())),
                Arc::new(StringArray::from_iter_values(
                    keys.iter().map(|key| string_value(*key, profile)),
                )),
                Arc::new(StringArray::from_iter_values(
                    clustered.iter().map(|key| string_value(*key, profile)),
                )),
                Arc::new(Int64Array::from(keys)),
            ];
            writer
                .write(&RecordBatch::try_new(schema.clone(), columns).unwrap())
                .unwrap();
        }
        writer.close().unwrap();

        let data = Bytes::from(buffer);
        let metadata = runtime
            .block_on(ArrowReaderMetadata::load_async(
                &mut Cursor::new(data.clone()),
                ArrowReaderOptions::new(),
            ))
            .unwrap();
        Self { data, metadata }
    }

    fn builder(&self) -> ParquetRecordBatchStreamBuilder<Cursor<Bytes>> {
        ParquetRecordBatchStreamBuilder::new_with_metadata(
            Cursor::new(self.data.clone()),
            self.metadata.clone(),
        )
    }
}

struct Case {
    column: usize,
    project_filter: bool,
    predicates: usize,
    profile: Profile,
}

impl Case {
    fn is_string(&self) -> bool {
        self.column >= 2
    }

    fn is_clustered(&self) -> bool {
        self.column % 2 == 1
    }

    fn threshold(&self, index: usize) -> u64 {
        match self.profile {
            Profile::Uniform(percent) => percent,
            Profile::RunLength(_) => 50,
            Profile::EarlySelective => {
                if index == 0 {
                    1
                } else {
                    99
                }
            }
            Profile::LateSelective => {
                if index + 1 == self.predicates {
                    1
                } else {
                    99
                }
            }
        }
    }

    fn expected_rows(&self) -> usize {
        let run_length = if self.is_clustered() {
            CLUSTER_SIZE as usize
        } else {
            1
        };
        (0..NUM_ROWS / run_length)
            .filter(|key| {
                (0..self.predicates)
                    .all(|index| self.profile.value(*key as u64, index) < self.threshold(index))
            })
            .count()
            * run_length
    }

    fn filter(&self, projection: ProjectionMask, validate: bool) -> RowFilter {
        let predicates = (0..self.predicates)
            .map(|index| {
                let threshold = self.threshold(index);
                let is_string = self.is_string();
                let profile = self.profile;
                Box::new(ArrowPredicateFn::new(projection.clone(), move |batch| {
                    let filter = if is_string {
                        let array = batch.column(0).as_string::<i32>();
                        BooleanBuffer::collect_bool(array.len(), |row| {
                            let bytes = array.value(row).as_bytes();
                            let value = (bytes[index * 2] - b'0') as u64 * 10
                                + (bytes[index * 2 + 1] - b'0') as u64;
                            value < threshold
                        })
                    } else {
                        let values = batch.column(0).as_primitive::<Int64Type>().values();
                        BooleanBuffer::collect_bool(values.len(), |row| {
                            profile.value(values[row] as u64, index) < threshold
                        })
                    };
                    if let (true, Profile::RunLength(length)) = (validate, profile) {
                        assert_run_lengths(&filter, length);
                    }
                    Ok(BooleanArray::new(filter, None))
                })) as Box<dyn ArrowPredicate>
            })
            .collect();
        RowFilter::new(predicates)
    }
}

/// Interior runs must have the target length; batch-edge runs may be partial.
fn assert_run_lengths(mask: &BooleanBuffer, length: usize) {
    let mut iter = mask.iter();
    let Some(mut value) = iter.next() else {
        return;
    };
    let mut run_length = 1;
    let mut first_run = true;
    for next in iter {
        if next != value {
            if first_run {
                assert!(run_length <= length);
                first_run = false;
            } else {
                assert_eq!(run_length, length);
            }
            value = next;
            run_length = 0;
        }
        run_length += 1;
    }
    assert!(run_length <= length);
}

async fn scan(
    dataset: &Dataset,
    case: &Case,
    metrics: ArrowReaderMetrics,
    validate: bool,
) -> usize {
    let builder = dataset.builder();
    let filter = case.filter(
        ProjectionMask::leaves(builder.parquet_schema(), [case.column]),
        validate,
    );
    let output = if case.project_filter {
        case.column
    } else {
        PAYLOAD_COLUMN
    };
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [output]);
    let mut reader = builder
        .with_batch_size(BATCH_SIZE)
        .with_metrics(metrics)
        .with_projection(projection)
        .with_row_filter(filter)
        .build()
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.try_next().await.unwrap() {
        rows += batch.num_rows();
        black_box(batch);
    }
    rows
}

/// Check the row count and predicate cache use once, outside measurement.
fn validate_case(runtime: &tokio::runtime::Runtime, dataset: &Dataset, case: &Case, id: &str) {
    let metrics = ArrowReaderMetrics::enabled();
    let rows = runtime.block_on(scan(dataset, case, metrics.clone(), true));
    assert_eq!(rows, case.expected_rows(), "{id}: unexpected row count");
    let from_cache = metrics.records_read_from_cache().unwrap();
    if case.project_filter {
        assert!(from_cache > 0, "{id}: predicate cache unused");
    } else {
        assert_eq!(from_cache, 0, "{id}: predicate cache used");
    }
}

fn register_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &tokio::runtime::Runtime,
    dataset: &Dataset,
    function: String,
    parameter: String,
    case: Case,
) {
    validate_case(runtime, dataset, &case, &format!("{function}/{parameter}"));
    let id = BenchmarkId::new(function, parameter);
    group.bench_with_input(id, &case, |b, case| {
        b.iter(|| {
            black_box(runtime.block_on(scan(dataset, case, ArrowReaderMetrics::disabled(), false)))
        });
    });
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>) {
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.sampling_mode(SamplingMode::Flat);
}

fn benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Only the run-length profiles change the string columns.
    let dataset = Dataset::generate(&runtime, Profile::Uniform(99));
    let mut group = c.benchmark_group("same_projection_filter");
    configure(&mut group);
    for (column, name) in LAYOUTS.iter().enumerate() {
        for project_filter in [false, true] {
            let cache = if project_filter { "cached" } else { "uncached" };
            for predicates in [1, 2, 4] {
                for profile in PROFILES {
                    if predicates == 1 && profile != Profile::Uniform(99) {
                        continue;
                    }
                    let case = Case {
                        column,
                        project_filter,
                        predicates,
                        profile,
                    };
                    register_case(
                        &mut group,
                        &runtime,
                        &dataset,
                        format!("{name}/{cache}"),
                        format!("{predicates}/{profile}"),
                        case,
                    );
                }
            }
        }
    }
    group.finish();

    let mut group = c.benchmark_group("same_projection_filter/selection_boundary");
    configure(&mut group);
    for length in [16, 32, 64] {
        let profile = Profile::RunLength(length);
        let dataset = Dataset::generate(&runtime, profile);
        let case = Case {
            column: 0,
            project_filter: false,
            predicates: 4,
            profile,
        };
        register_case(
            &mut group,
            &runtime,
            &dataset,
            "int64/uncached".to_string(),
            format!("4/{profile}"),
            case,
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

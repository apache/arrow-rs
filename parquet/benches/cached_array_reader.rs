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

use criterion::{Criterion, criterion_group, criterion_main};
use parquet::arrow::array_reader::{ArrayReader, CacheRole, CachedArrayReader, RowGroupCache};
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::errors::Result;

use arrow_array::ArrayRef;
use arrow_array::builder::StringBuilder;
use arrow_schema::DataType as ArrowType;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::any::Any;
use std::hint::black_box;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const TOTAL_ROWS: usize = 4_194_304;
const BATCH_SIZE: usize = 1_024;
const ESTIMATED_AVG_BYTES_PER_ROW_NUM: usize = 67;
const ESTIMATED_AVG_BYTES_PER_ROW_DEN: usize = 8;

#[derive(Clone, Copy)]
enum SelectionOp {
    Read(usize),
    Skip(usize),
}

struct MockArrayRefReader {
    data: ArrayRef,
    position: usize,
    records_to_consume: usize,
    data_type: ArrowType,
}

impl MockArrayRefReader {
    fn new(data: ArrayRef) -> Self {
        Self {
            data_type: data.data_type().clone(),
            data,
            position: 0,
            records_to_consume: 0,
        }
    }
}

impl ArrayReader for MockArrayRefReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let remaining = self.data.len() - self.position;
        let to_read = std::cmp::min(batch_size, remaining);
        self.records_to_consume += to_read;
        Ok(to_read)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let start = self.position;
        let end = start + self.records_to_consume;
        self.position = end;
        self.records_to_consume = 0;
        Ok(self.data.slice(start, end - start))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let remaining = self.data.len() - self.position;
        let to_skip = std::cmp::min(num_records, remaining);
        self.position += to_skip;
        Ok(to_skip)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

struct BenchCase {
    data: ArrayRef,
    selection_ops: Vec<SelectionOp>,
    selected_rows: usize,
}

impl BenchCase {
    fn new() -> Self {
        let data = make_string_array(TOTAL_ROWS);
        let (selection_ops, selected_rows) = make_selection_ops(TOTAL_ROWS);

        Self {
            data,
            selection_ops,
            selected_rows,
        }
    }

    fn prepare_reader(&self) -> CachedArrayReader {
        let metrics = ArrowReaderMetrics::disabled();
        let cache = Arc::new(RwLock::new(RowGroupCache::new(BATCH_SIZE, usize::MAX)));
        let mut reader = CachedArrayReader::new(
            Box::new(MockArrayRefReader::new(self.data.clone())),
            cache,
            0,
            CacheRole::Consumer,
            metrics,
        );

        for op in &self.selection_ops {
            match op {
                SelectionOp::Read(count) => {
                    assert_eq!(reader.read_records(*count).unwrap(), *count);
                }
                SelectionOp::Skip(count) => {
                    assert_eq!(reader.skip_records(*count).unwrap(), *count);
                }
            }
        }

        reader
    }
}

fn make_string_array(total_rows: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(44);
    let value_capacity =
        total_rows * ESTIMATED_AVG_BYTES_PER_ROW_NUM / ESTIMATED_AVG_BYTES_PER_ROW_DEN;
    let mut builder = StringBuilder::with_capacity(total_rows, value_capacity);

    for _ in 0..total_rows {
        let value = if rng.random_bool(0.5) {
            let len = if rng.random_bool(0.5) {
                rng.random_range(13..21)
            } else {
                rng.random_range(3..12)
            };

            (0..len)
                .map(|_| (b'a' + rng.random_range(0..26)) as char)
                .collect()
        } else {
            "const".to_string()
        };
        builder.append_value(value);
    }

    Arc::new(builder.finish())
}

fn make_selection_ops(total_rows: usize) -> (Vec<SelectionOp>, usize) {
    let mut rng = StdRng::seed_from_u64(9060);
    let mut remaining = total_rows;
    let mut selected_rows = 0;
    let mut ops = Vec::new();

    while remaining > 0 {
        // Match the issue more closely: small selected runs and much longer gaps.
        let read = std::cmp::min(rng.random_range(4..11), remaining);
        ops.push(SelectionOp::Read(read));
        selected_rows += read;
        remaining -= read;

        if remaining == 0 {
            break;
        }

        let skip = std::cmp::min(rng.random_range(35..66), remaining);
        ops.push(SelectionOp::Skip(skip));
        remaining -= skip;
    }

    (ops, selected_rows)
}

fn cached_array_reader_benchmark(c: &mut Criterion) {
    let case = BenchCase::new();

    let array = case.prepare_reader().consume_batch().unwrap();
    assert_eq!(array.len(), case.selected_rows);

    let mut group = c.benchmark_group("cached_array_reader");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(4));
    group.bench_function("utf8_sparse_cross_batch_4m_rows/consume_batch", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut reader = case.prepare_reader();
                let start = Instant::now();
                let array = reader.consume_batch().unwrap();
                black_box(&array);
                total += start.elapsed();
            }
            total
        })
    });
    group.finish();
}

criterion_group!(benches, cached_array_reader_benchmark);
criterion_main!(benches);

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

use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use std::fmt::Write;
use std::hint::black_box;
use std::sync::Arc;

// Projection benchmark constants
const WIDE_PROJECTION_ROWS: usize = 1 << 14; // 16K rows
const WIDE_PROJECTION_TOTAL_FIELDS: usize = 100; // 100 fields total, select only 3

fn bench_decode_schema(
    c: &mut Criterion,
    name: &str,
    data: &[u8],
    schema: Arc<Schema>,
    rows: usize,
) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function(BenchmarkId::from_parameter(rows), |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(rows)
                .build_decoder()
                .unwrap();

            let mut offset = 0;
            while offset < data.len() {
                let read = decoder.decode(black_box(&data[offset..])).unwrap();
                if read == 0 {
                    break;
                }
                offset += read;
            }

            let batch = decoder.flush().unwrap();
            black_box(batch);
        })
    });
    group.finish();
}

fn build_wide_projection_json(rows: usize, total_fields: usize) -> Vec<u8> {
    // Estimate: each field ~15 bytes ("fXX":VVVVVVV,), total ~15*100 + overhead
    let per_row_size = total_fields * 15 + 10;
    let mut data = String::with_capacity(rows * per_row_size);

    for _row in 0..rows {
        data.push('{');
        for i in 0..total_fields {
            if i > 0 {
                data.push(',');
            }
            // Use fixed-width values for stable benchmarks: 7 digits
            let _ = write!(data, "\"f{}\":{:07}", i, i);
        }
        data.push('}');
        data.push('\n');
    }
    data.into_bytes()
}

fn criterion_benchmark(c: &mut Criterion) {
    // Wide projection workload: tests overhead of parsing unused fields
    let wide_projection_data =
        build_wide_projection_json(WIDE_PROJECTION_ROWS, WIDE_PROJECTION_TOTAL_FIELDS);

    // Full schema: all 100 fields
    let mut full_fields = Vec::new();
    for i in 0..WIDE_PROJECTION_TOTAL_FIELDS {
        full_fields.push(Field::new(format!("f{}", i), DataType::Int64, false));
    }
    let full_schema = Arc::new(Schema::new(full_fields));
    bench_decode_schema(
        c,
        "decode_wide_projection_full_json",
        &wide_projection_data,
        full_schema,
        WIDE_PROJECTION_ROWS,
    );

    // Projected schema: only 3 fields (f0, f10, f50) out of 100
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("f0", DataType::Int64, false),
        Field::new("f10", DataType::Int64, false),
        Field::new("f50", DataType::Int64, false),
    ]));
    bench_decode_schema(
        c,
        "decode_wide_projection_narrow_json",
        &wide_projection_data,
        projected_schema,
        WIDE_PROJECTION_ROWS,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

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

use arrow_array::RecordBatch;
use arrow_flight::{FlightClient, FlightData, encode::FlightDataEncoderBuilder};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::TryStreamExt;
use tonic::transport::Channel;

mod common;
use common::{TYPES, build_batch, start_server};

const ROWS: [usize; 2] = [8 * 1024, 64 * 1024];
const COLS: [usize; 4] = [1, 4, 8, 16];

fn bench_encode(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut g = c.benchmark_group("encode");

    for &(name, build) in TYPES {
        for &rows in &ROWS {
            for &cols in &COLS {
                let batch = build_batch(name, rows, cols, build);
                let id = BenchmarkId::new(name, format!("{rows}x{cols}"));
                g.throughput(Throughput::Bytes(batch.get_array_memory_size() as u64));
                g.bench_with_input(id, &batch, |b, batch| {
                    b.to_async(&rt).iter(|| async {
                        let _: Vec<FlightData> = FlightDataEncoderBuilder::new()
                            .build(futures::stream::iter([Ok(batch.clone())]))
                            .try_collect()
                            .await
                            .unwrap();
                    });
                });
            }
        }
    }
}

async fn roundtrip(channel: Channel, batch: RecordBatch) {
    let mut client = FlightClient::new(channel);
    let frames = FlightDataEncoderBuilder::new().build(futures::stream::iter([Ok(batch)]));
    let _: Vec<RecordBatch> = client
        .do_exchange(frames)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
}

fn bench_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (channel, _) = rt.block_on(start_server());
    let mut g = c.benchmark_group("roundtrip");

    for &(name, build) in TYPES {
        for &rows in &ROWS {
            for &cols in &COLS {
                let batch = build_batch(name, rows, cols, build);
                let id = BenchmarkId::new(name, format!("{rows}x{cols}"));
                g.throughput(Throughput::Bytes(batch.get_array_memory_size() as u64));
                g.bench_with_input(id, &batch, |b, batch| {
                    b.to_async(&rt)
                        .iter(|| roundtrip(channel.clone(), batch.clone()));
                });
            }
        }
    }
}

criterion_group!(benches, bench_encode, bench_roundtrip);
criterion_main!(benches);

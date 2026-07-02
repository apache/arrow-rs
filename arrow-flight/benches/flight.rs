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
use arrow_flight::{
    FlightClient, FlightData,
    decode::FlightRecordBatchStream,
    encode::{DictionaryHandling, FlightDataEncoderBuilder},
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::TryStreamExt;
use tonic::transport::Channel;

mod common;
use common::{DICT_TYPES, TYPES, build_batch, start_server};

const ROWS: [usize; 2] = [8 * 1024, 64 * 1024];
const COLS: [usize; 2] = [4, 8];
const BATCHES: usize = 4;

fn bench_encode(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
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

fn bench_decode(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut g = c.benchmark_group("decode");

    for &(name, build) in TYPES {
        for &rows in &ROWS {
            for &cols in &COLS {
                let batches: Vec<RecordBatch> = (0..BATCHES)
                    .map(|_| build_batch(name, rows, cols, build))
                    .collect();
                let total_bytes: u64 = batches
                    .iter()
                    .map(|b| b.get_array_memory_size() as u64)
                    .sum();
                let frames: Vec<FlightData> = rt
                    .block_on(
                        FlightDataEncoderBuilder::new()
                            .build(futures::stream::iter(batches.into_iter().map(Ok)))
                            .try_collect(),
                    )
                    .unwrap();
                let id = BenchmarkId::new(name, format!("{rows}x{cols}"));
                g.throughput(Throughput::Bytes(total_bytes));
                g.bench_function(id, |b| {
                    b.to_async(&rt).iter_batched(
                        || frames.clone(),
                        |frames| async move {
                            let _: Vec<RecordBatch> =
                                FlightRecordBatchStream::new_from_flight_data(
                                    futures::stream::iter(frames.into_iter().map(Ok)),
                                )
                                .try_collect()
                                .await
                                .unwrap();
                        },
                        criterion::BatchSize::SmallInput,
                    );
                });
            }
        }
    }
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

fn bench_decode_dictionary_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut g = c.benchmark_group("decode_stream");

    for &(name, build) in TYPES.iter().chain(DICT_TYPES) {
        for &rows in &ROWS {
            for &cols in &COLS {
                let batches: Vec<RecordBatch> = (0..BATCHES)
                    .map(|_| build_batch(name, rows, cols, build))
                    .collect();
                let total_bytes: u64 = batches
                    .iter()
                    .map(|b| b.get_array_memory_size() as u64)
                    .sum();
                let frames: Vec<FlightData> = rt
                    .block_on(
                        FlightDataEncoderBuilder::new()
                            .with_dictionary_handling(DictionaryHandling::Resend)
                            .build(futures::stream::iter(batches.into_iter().map(Ok)))
                            .try_collect(),
                    )
                    .unwrap();
                let id = BenchmarkId::new(name, format!("{rows}x{cols}x{BATCHES}"));
                g.throughput(Throughput::Bytes(total_bytes));
                g.bench_function(id, |b| {
                    b.to_async(&rt).iter_batched(
                        || frames.clone(),
                        |frames| async move {
                            let _: Vec<RecordBatch> =
                                FlightRecordBatchStream::new_from_flight_data(
                                    futures::stream::iter(frames.into_iter().map(Ok)),
                                )
                                .try_collect()
                                .await
                                .unwrap();
                        },
                        criterion::BatchSize::SmallInput,
                    );
                });
            }
        }
    }
}

fn bench_do_put_dictionary(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (channel, _) = rt.block_on(start_server());
    let mut g = c.benchmark_group("do_put_dictionary");

    for &(name, build) in DICT_TYPES {
        for &rows in &ROWS {
            for &cols in &COLS {
                let batch = build_batch(name, rows, cols, build);
                g.throughput(Throughput::Bytes(batch.get_array_memory_size() as u64));

                for (label, handling) in [
                    ("hydrate", DictionaryHandling::Hydrate),
                    ("resend", DictionaryHandling::Resend),
                ] {
                    let frames: Vec<FlightData> = rt
                        .block_on(
                            FlightDataEncoderBuilder::new()
                                .with_dictionary_handling(handling)
                                .build(futures::stream::iter([Ok(batch.clone())]))
                                .try_collect(),
                        )
                        .unwrap();
                    let id = BenchmarkId::new(format!("{name}/{label}"), format!("{rows}x{cols}"));
                    g.bench_function(id, |b| {
                        b.to_async(&rt).iter_batched(
                            || (FlightClient::new(channel.clone()), frames.clone()),
                            |(mut client, frames)| async move {
                                client
                                    .do_put(futures::stream::iter(frames.into_iter().map(Ok)))
                                    .await
                                    .unwrap()
                                    .try_collect::<Vec<_>>()
                                    .await
                                    .unwrap();
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    });
                }
            }
        }
    }
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_decode_dictionary_stream,
    bench_roundtrip,
    bench_do_put_dictionary
);
criterion_main!(benches);

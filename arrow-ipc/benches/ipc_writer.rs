// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::builder::{
    Date32Builder, Decimal128Builder, Int32Builder, StringBuilder, StringDictionaryBuilder,
};
use arrow_array::types::UInt32Type;
use arrow_array::{FixedSizeBinaryArray, Int32Array, ListViewArray, RecordBatch};
use arrow_buffer::ScalarBuffer;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{
    DictionaryHandling, FileWriter, IpcWriteOptions, StreamEncoder, StreamWriter,
};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_ipc_stream_writer");

    group.bench_function("StreamWriter/write_10", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    for (name, slice) in [("unsliced", false), ("sliced", true)] {
        group.bench_function(format!("StreamWriter/list_view/{name}"), |b| {
            let batch = create_list_view_batch(1_000_000);
            let batch = if slice {
                batch.slice(1000, batch.num_rows() / 2)
            } else {
                batch
            };
            let mut buffer = Vec::with_capacity(16 * 1024 * 1024);
            b.iter(move || {
                buffer.clear();
                let mut writer =
                    StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            })
        });
    }

    group.bench_function("StreamWriter/write_10/zstd", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                    .unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    group.bench_function("StreamWriter/write_10/lz4", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                    .unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    for (rows, threads) in [(8192, 1), (65536, 1), (8192, 16), (65536, 16)] {
        group.bench_function(
            format!("StreamEncoder/encode_10/lz4/rows={rows}/threads={threads}"),
            |b| {
                let batch = create_batch(rows, true);
                b.iter(|| {
                    std::thread::scope(|s| {
                        for _ in 0..threads {
                            s.spawn(|| {
                                let options = IpcWriteOptions::default()
                                    .try_with_compression(Some(CompressionType::LZ4_FRAME))
                                    .unwrap();
                                let mut encoder = StreamEncoder::try_new_with_options(
                                    batch.schema().as_ref(),
                                    options,
                                )
                                .unwrap();
                                for _ in 0..10 {
                                    black_box(encoder.encode(&batch).unwrap());
                                }
                            });
                        }
                    })
                })
            },
        );
    }

    group.bench_function("StreamEncoder/encode_10", |b| {
        let batch = create_batch(8192, true);
        b.iter(move || {
            let mut encoder = StreamEncoder::try_new(batch.schema().as_ref()).unwrap();
            for _ in 0..10 {
                black_box(encoder.encode(&batch).unwrap());
            }
            black_box(encoder.finish().unwrap());
        })
    });

    group.bench_function("StreamEncoder/encode_10/zstd", |b| {
        let batch = create_batch(8192, true);
        b.iter(move || {
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut encoder =
                StreamEncoder::try_new_with_options(batch.schema().as_ref(), options).unwrap();
            for _ in 0..10 {
                black_box(encoder.encode(&batch).unwrap());
            }
            black_box(encoder.finish().unwrap());
        })
    });

    group.bench_function("FileWriter/write_10", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let mut writer = FileWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
            for _ in 0..10 {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    group.bench_function("StreamWriter/write_10/dict", |b| {
        let batches = create_unique_dict_batches(10, 8192);
        let schema = batches[0].schema();
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            let mut writer = StreamWriter::try_new(&mut buffer, schema.as_ref()).unwrap();
            for batch in &batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        })
    });

    group.bench_function("StreamEncoder/encode_10/dict", |b| {
        let batches = create_unique_dict_batches(10, 8192);
        let schema = batches[0].schema();
        b.iter(move || {
            let mut encoder = StreamEncoder::try_new(schema.as_ref()).unwrap();
            for batch in &batches {
                black_box(encoder.encode(batch).unwrap());
            }
            black_box(encoder.finish().unwrap());
        })
    });

    group.bench_function("StreamWriter/write_10/dict/delta", |b| {
        let batches = create_delta_dict_batches(10, 8192);
        let schema = batches[0].schema();
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);

        b.iter(move || {
            buffer.clear();

            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, schema.as_ref(), options.clone())
                    .unwrap();

            for batch in &batches {
                writer.write(batch).unwrap();
            }

            writer.finish().unwrap();
        })
    });

    group.bench_function("StreamEncoder/encode_10/dict/delta", |b| {
        let batches = create_delta_dict_batches(10, 8192);
        let schema = batches[0].schema();
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);

        b.iter(move || {
            let mut encoder =
                StreamEncoder::try_new_with_options(schema.as_ref(), options.clone()).unwrap();
            for batch in &batches {
                black_box(encoder.encode(batch).unwrap());
            }
            black_box(encoder.finish().unwrap());
        })
    });

    // The file writer rejects dictionary replacement, so only the delta case is
    // exercised here (growing dictionaries that are prefixes of one another).
    group.bench_function("FileWriter/write_10/dict/delta", |b| {
        let batches = create_delta_dict_batches(10, 8192);
        let schema = batches[0].schema();
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);

        b.iter(move || {
            buffer.clear();

            let mut writer =
                FileWriter::try_new_with_options(&mut buffer, schema.as_ref(), options.clone())
                    .unwrap();

            for batch in &batches {
                writer.write(batch).unwrap();
            }

            writer.finish().unwrap();
        })
    });
}

/// Build `n` record batches with a single dictionary column whose dictionary
/// grows across batches. A single builder is reused with `finish_preserve_values`
/// so each batch's dictionary has the previous batch's as a prefix which allows
/// us to emit deltas.
fn create_delta_dict_batches(n: usize, num_rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d0",
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        false,
    )]));
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();

    let mut batches = Vec::with_capacity(n);
    for i in 0..n {
        // 3/4 of the rows reuse values shared by every batch, the other 1/4
        // introduce values unique to this batch which extends the dictionary.
        for r in 0..num_rows {
            if r < num_rows / 4 {
                builder.append_value(format!("batch {i} value {r}"));
            } else {
                builder.append_value(format!("shared {r}"));
            }
        }

        // Preserve the values builder so the dictionary accumulates across batches.
        let dict = builder.finish_preserve_values();
        batches.push(RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap());
    }

    batches
}

/// Build `n` record batches each with a completely distinct dictionary for each batch.
fn create_unique_dict_batches(n: usize, num_rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d0",
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        false,
    )]));

    let mut batches = Vec::with_capacity(n);
    for i in 0..n {
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for r in 0..num_rows {
            builder.append_value(format!("batch {i} value {}", r % (num_rows / 2)));
        }
        let dict = builder.finish();
        batches.push(RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap());
    }

    batches
}

fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
        Field::new("c4", DataType::FixedSizeBinary(16), false),
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();
    let fixed_value = [0xa5; 16];
    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }
    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();
    let e =
        FixedSizeBinaryArray::try_from_iter((0..num_rows).map(|_| fixed_value.as_slice())).unwrap();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(a),
            Arc::new(b),
            Arc::new(c),
            Arc::new(d),
            Arc::new(e),
        ],
    )
    .unwrap()
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);

/// Creates a batch with one `ListView<Int32>` column of `rows` views of size 1.
fn create_list_view_batch(rows: i32) -> RecordBatch {
    let field = Arc::new(Field::new_list_field(DataType::Int32, false));
    let list = ListViewArray::new(
        field,
        ScalarBuffer::from_iter(0..rows),
        ScalarBuffer::from(vec![1; rows as usize]),
        Arc::new(Int32Array::from_iter_values(0..rows)),
        None,
    );
    RecordBatch::try_from_iter([("a", Arc::new(list) as _)]).unwrap()
}

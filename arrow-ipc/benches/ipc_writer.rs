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

use arrow_array::builder::{
    Date32Builder, Decimal128Builder, Int32Builder, StringBuilder, StringDictionaryBuilder,
};
use arrow_array::types::UInt32Type;
use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch};
use arrow_ipc::CompressionType;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{
    DictionaryHandling, DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteContext,
    IpcWriteOptions, StreamEncoder, StreamWriter,
};
use arrow_ipc::{BodyCompressionMethod, MessageHeader, root_as_message};
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
        let options = lz4_options();
        let stream = write_stream(&batch, options.clone());
        validate_stream(&stream, &batch);
        assert_lz4_compressed_buffers(&batch, false);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            write_stream_into(&mut buffer, &batch, options.clone());
            black_box(buffer.len());
        })
    });

    group.bench_function("StreamWriter/write_10/fixed_size_binary_256", |b| {
        let batch = create_fixed_size_binary_batch(1, 256);
        let options = IpcWriteOptions::default();
        let stream = write_stream(&batch, options.clone());
        validate_stream(&stream, &batch);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            write_stream_into(&mut buffer, &batch, options.clone());
            black_box(buffer.len());
        })
    });

    group.bench_function("StreamWriter/write_10/fixed_size_binary_256/lz4", |b| {
        let batch = create_fixed_size_binary_batch(1, 256);
        let options = lz4_options();
        let stream = write_stream(&batch, options.clone());
        validate_stream(&stream, &batch);
        assert_lz4_compressed_buffers(&batch, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            write_stream_into(&mut buffer, &batch, options.clone());
            black_box(buffer.len());
        })
    });

    group.bench_function("StreamWriter/write_10/fixed_size_binary_16x16", |b| {
        let batch = create_fixed_size_binary_batch(16, 16);
        let options = IpcWriteOptions::default();
        let stream = write_stream(&batch, options.clone());
        validate_stream(&stream, &batch);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            write_stream_into(&mut buffer, &batch, options.clone());
            black_box(buffer.len());
        })
    });

    group.bench_function("StreamWriter/write_10/fixed_size_binary_16x16/lz4", |b| {
        let batch = create_fixed_size_binary_batch(16, 16);
        let options = lz4_options();
        let stream = write_stream(&batch, options.clone());
        validate_stream(&stream, &batch);
        assert_lz4_compressed_buffers(&batch, true);
        let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
        b.iter(move || {
            buffer.clear();
            write_stream_into(&mut buffer, &batch, options.clone());
            black_box(buffer.len());
        })
    });

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
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();
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
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    )
    .unwrap()
}

fn lz4_options() -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap()
}

fn write_stream(batch: &RecordBatch, options: IpcWriteOptions) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    write_stream_into(&mut buffer, batch, options);
    buffer
}

fn write_stream_into(buffer: &mut Vec<u8>, batch: &RecordBatch, options: IpcWriteOptions) {
    let mut writer =
        StreamWriter::try_new_with_options(buffer, batch.schema().as_ref(), options).unwrap();
    for _ in 0..10 {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

fn validate_stream(buffer: &[u8], expected: &RecordBatch) {
    let mut reader = StreamReader::try_new(buffer, None).unwrap();
    for _ in 0..10 {
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(&actual, expected);
    }
    assert!(reader.next().is_none());
}

fn assert_lz4_compressed_buffers(batch: &RecordBatch, require_all_positive: bool) {
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut write_context = IpcWriteContext::default();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut dictionary_tracker,
            &lz4_options(),
            &mut write_context,
        )
        .unwrap();
    let message = root_as_message(&encoded.ipc_message).unwrap();
    assert_eq!(message.header_type(), MessageHeader::RecordBatch);
    let record_batch = message.header_as_record_batch().unwrap();
    let compression = record_batch.compression().unwrap();
    assert_eq!(compression.codec(), CompressionType::LZ4_FRAME);
    assert_eq!(compression.method(), BodyCompressionMethod::BUFFER);

    let buffers = record_batch.buffers().unwrap();
    let mut compressed_buffers = 0;
    for buffer in buffers {
        let length = usize::try_from(buffer.length()).unwrap();
        if length == 0 {
            continue;
        }
        let offset = usize::try_from(buffer.offset()).unwrap();
        let prefix_end = offset.checked_add(8).unwrap();
        assert!(prefix_end <= encoded.arrow_data.len());
        let prefix = i64::from_le_bytes(encoded.arrow_data[offset..prefix_end].try_into().unwrap());
        assert!(prefix == -1 || prefix > 0);
        if prefix > 0 {
            compressed_buffers += 1;
        }
        if require_all_positive {
            assert!(prefix > 0);
        }
    }
    assert!(compressed_buffers > 0);
}

fn create_fixed_size_binary_batch(num_columns: usize, value_size: usize) -> RecordBatch {
    const NUM_ROWS: usize = 1024;
    let value = vec![0xa5; value_size];
    let value_size = i32::try_from(value_size).unwrap();
    let fields = (0..num_columns)
        .map(|column| {
            Field::new(
                format!("c{column}"),
                DataType::FixedSizeBinary(value_size),
                false,
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let columns = (0..num_columns)
        .map(|_| {
            Arc::new(
                FixedSizeBinaryArray::try_from_iter((0..NUM_ROWS).map(|_| value.as_slice()))
                    .unwrap(),
            ) as ArrayRef
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
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

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

use arrow_array::builder::{Date32Builder, Decimal128Builder, Int32Builder};
use arrow_array::{FixedSizeBinaryArray, Float64Array, RecordBatch, builder::StringBuilder};
use arrow_buffer::Buffer;
use arrow_ipc::convert::try_fb_to_schema;
use arrow_ipc::reader::{
    BufferStreamReader, FileDecoder, FileReader, StreamDecoder, StreamReader, read_footer_length,
};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow_ipc::{Block, CompressionType, root_as_footer};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::io::{Cursor, Write};
use std::sync::Arc;
use tempfile::tempdir;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_ipc_reader");

    group.bench_function("StreamReader/read_10", |b| {
        let buffer = ipc_stream(IpcWriteOptions::default());
        b.iter(move || {
            let projection = None;
            let mut reader = StreamReader::try_new(buffer.as_slice(), projection).unwrap();
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    group.bench_function("StreamReader/no_validation/read_10", |b| {
        let buffer = ipc_stream(IpcWriteOptions::default());
        b.iter(move || {
            let projection = None;
            let mut reader = StreamReader::try_new(buffer.as_slice(), projection).unwrap();
            unsafe {
                // safety: we created a valid IPC file
                reader = reader.with_skip_validation(true);
            }
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    group.bench_function("StreamReader/read_10/zstd", |b| {
        let buffer = ipc_stream(
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap(),
        );
        b.iter(move || {
            let projection = None;
            let mut reader = StreamReader::try_new(buffer.as_slice(), projection).unwrap();
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    group.bench_function("StreamReader/read_10/lz4", |b| {
        let buffer = ipc_stream(
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .unwrap(),
        );
        b.iter(move || {
            let projection = None;
            let mut reader = StreamReader::try_new(buffer.as_slice(), projection).unwrap();
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    group.bench_function("StreamReader/no_validation/read_10/zstd", |b| {
        let buffer = ipc_stream(
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap(),
        );
        b.iter(move || {
            let projection = None;
            let mut reader = StreamReader::try_new(buffer.as_slice(), projection).unwrap();
            unsafe {
                // safety: we created a valid IPC file
                reader = reader.with_skip_validation(true);
            }
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    // --- Create IPC File ---
    group.bench_function("FileReader/read_10", |b| {
        let buffer = ipc_file();
        b.iter(move || {
            let projection = None;
            let cursor = Cursor::new(buffer.as_slice());
            let mut reader = FileReader::try_new(cursor, projection).unwrap();
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    group.bench_function("FileReader/no_validation/read_10", |b| {
        let buffer = ipc_file();
        b.iter(move || {
            let projection = None;
            let cursor = Cursor::new(buffer.as_slice());
            let mut reader = FileReader::try_new(cursor, projection).unwrap();
            unsafe {
                // safety: we created a valid IPC file
                reader = reader.with_skip_validation(true);
            }
            for _ in 0..10 {
                reader.next().unwrap().unwrap();
            }
            assert!(reader.next().is_none());
        })
    });

    // write to an actual file
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&ipc_file()).unwrap();
    drop(file);

    group.bench_function("FileReader/read_10/mmap", |b| {
        let path = &path;
        b.iter(move || {
            let ipc_file = std::fs::File::open(path).expect("failed to open file");
            let mmap = unsafe { memmap2::Mmap::map(&ipc_file).expect("failed to mmap file") };

            // Convert the mmap region to an Arrow `Buffer` to back the arrow arrays.
            let bytes = bytes::Bytes::from_owner(mmap);
            let buffer = Buffer::from(bytes);
            let decoder = IPCBufferDecoder::new(buffer);
            assert_eq!(decoder.num_batches(), 10);

            for i in 0..decoder.num_batches() {
                decoder.get_batch(i);
            }
        })
    });

    group.bench_function("FileReader/no_validation/read_10/mmap", |b| {
        let path = &path;
        b.iter(move || {
            let ipc_file = std::fs::File::open(path).expect("failed to open file");
            let mmap = unsafe { memmap2::Mmap::map(&ipc_file).expect("failed to mmap file") };

            // Convert the mmap region to an Arrow `Buffer` to back the arrow arrays.
            let bytes = bytes::Bytes::from_owner(mmap);
            let buffer = Buffer::from(bytes);
            let decoder = IPCBufferDecoder::new(buffer);
            let decoder = unsafe { decoder.with_skip_validation(true) };
            assert_eq!(decoder.num_batches(), 10);

            for i in 0..decoder.num_batches() {
                decoder.get_batch(i);
            }
        })
    });
}

/// Compare readers when a complete, uncompressed IPC stream is already in memory.
/// Serialization and input allocation are excluded; construction, validation,
/// decoding through EOS, and dropping the decoded batches are included.
fn in_memory_stream_benchmark(c: &mut Criterion) {
    for (num_rows, num_batches) in [(1024, 1), (1_000_000, 1), (10_000_000, 1), (1_000_000, 10)] {
        let batch = RecordBatch::try_from_iter(vec![(
            "values",
            Arc::new(Float64Array::from_iter_values(
                (0..num_rows).map(|i| i as f64),
            )) as _,
        )])
        .unwrap();
        bench_in_memory_stream(c, "float64", &batch, num_batches);
    }

    // Include variable-length data and nulls, whose validation still reads data
    // even when the column buffers can be shared with the input.
    bench_in_memory_stream(c, "mixed", &create_batch(8192, true), 10);
}

fn bench_in_memory_stream(c: &mut Criterion, name: &str, batch: &RecordBatch, num_batches: usize) {
    let mut serialized = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut serialized, batch.schema().as_ref()).unwrap();
        for _ in 0..num_batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }

    // All readers use the same aligned allocation. Misaligned input can require
    // copies in StreamDecoder. Neither this copy nor the Bytes wrapper is timed.
    let buffer = Buffer::from(serialized.as_slice());
    let bytes = bytes::Bytes::from(buffer.clone());
    drop(serialized);

    // Check results and buffer sharing outside the timed loop for both new APIs.
    for mut reader in [
        BufferStreamReader::try_new(buffer.clone()).unwrap(),
        BufferStreamReader::try_new_from_bytes(bytes.clone()).unwrap(),
    ] {
        assert_eq!(reader.schema(), &batch.schema());
        for _ in 0..num_batches {
            let decoded = reader.next().unwrap().unwrap();
            assert_eq!(&decoded, batch);
            for column in decoded.columns() {
                for data in column.to_data().buffers() {
                    let start = buffer.as_ptr() as usize;
                    let ptr = data.as_ptr() as usize;
                    assert!(ptr >= start && ptr + data.len() <= start + buffer.len());
                }
            }
        }
        assert!(reader.next().is_none());
    }

    let mut group = c.benchmark_group("arrow_ipc_reader/in_memory");
    let input = format!("{name}/{}x{num_batches}", batch.num_rows());
    // This is logical IPC throughput: zero-copy readers do not scan every byte.
    group.throughput(Throughput::Bytes(buffer.len() as u64));

    group.bench_function(BenchmarkId::new("StreamReader", &input), |b| {
        b.iter(|| {
            let mut reader =
                StreamReader::try_new(Cursor::new(black_box(bytes.as_ref())), None).unwrap();
            for _ in 0..num_batches {
                black_box(reader.next().unwrap().unwrap());
            }
            assert!(reader.next().is_none());
        });
    });

    group.bench_function(BenchmarkId::new("StreamDecoder", &input), |b| {
        b.iter(|| {
            let mut remaining = black_box(buffer.clone());
            let mut decoder = StreamDecoder::new();
            for _ in 0..num_batches {
                black_box(decoder.decode(&mut remaining).unwrap().unwrap());
            }
            assert!(decoder.decode(&mut remaining).unwrap().is_none());
            decoder.finish().unwrap();
            assert!(remaining.is_empty());
        });
    });

    group.bench_function(BenchmarkId::new("BufferStreamReader", &input), |b| {
        b.iter(|| {
            let mut reader = BufferStreamReader::try_new(black_box(buffer.clone())).unwrap();
            for _ in 0..num_batches {
                black_box(reader.next().unwrap().unwrap());
            }
            assert!(reader.next().is_none());
        });
    });

    group.bench_function(
        BenchmarkId::new("BufferStreamReader_from_bytes", &input),
        |b| {
            b.iter(|| {
                let mut reader =
                    BufferStreamReader::try_new_from_bytes(black_box(bytes.clone())).unwrap();
                for _ in 0..num_batches {
                    black_box(reader.next().unwrap().unwrap());
                }
                assert!(reader.next().is_none());
            });
        },
    );
    group.finish();
}

/// Return an IPC stream with 10 record batches
fn ipc_stream(options: IpcWriteOptions) -> Vec<u8> {
    let batch = create_batch(8192, true);
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options).unwrap();
    for _ in 0..10 {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    buffer
}

/// Return an IPC file with 10 record batches
fn ipc_file() -> Vec<u8> {
    let batch = create_batch(8192, true);
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    let mut writer = FileWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
    for _ in 0..10 {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    buffer
}

// copied from the zero_copy_ipc example.
// should we move this to an actual API?
/// Wrapper around the example in the `FileDecoder` which handles the
/// low level interaction with the Arrow IPC format.
struct IPCBufferDecoder {
    /// Memory (or memory mapped) Buffer with the data
    buffer: Buffer,
    /// Decoder that reads Arrays that refers to the underlying buffers
    decoder: FileDecoder,
    /// Location of the batches within the buffer
    batches: Vec<Block>,
}

impl IPCBufferDecoder {
    fn new(buffer: Buffer) -> Self {
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

        let schema = try_fb_to_schema(footer.schema().unwrap()).unwrap();

        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());

        // Read dictionaries
        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);
            decoder.read_dictionary(block, &data).unwrap();
        }

        // convert to Vec from the flatbuffers Vector to avoid having a direct dependency on flatbuffers
        let batches = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        Self {
            buffer,
            decoder,
            batches,
        }
    }

    unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        self.decoder = unsafe { self.decoder.with_skip_validation(skip_validation) };
        self
    }

    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    fn get_batch(&self, i: usize) -> RecordBatch {
        let block = &self.batches[i];
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = self
            .buffer
            .slice_with_length(block.offset() as _, block_len);
        self.decoder
            .read_record_batch(block, &data)
            .unwrap()
            .unwrap()
    }
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
    targets = criterion_benchmark, in_memory_stream_benchmark
}
criterion_main!(benches);

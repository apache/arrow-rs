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
use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, builder::StringBuilder};
use arrow_buffer::Buffer;
use arrow_ipc::convert::try_fb_to_schema;
use arrow_ipc::reader::{FileDecoder, FileReader, StreamReader, read_footer_length};
use arrow_ipc::writer::{
    DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
};
use arrow_ipc::{Block, CompressionType, MessageHeader, root_as_footer, root_as_message};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
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
        let mixed_batch = create_batch(8192, true);
        let mixed_lz4 = ipc_stream_with_batch(&mixed_batch, lz4_options());
        validate_stream(&mixed_lz4, &mixed_batch);
        assert_lz4_compressed_buffers(&mixed_batch, false);
        b.iter(|| read_stream(mixed_lz4.as_slice()))
    });

    group.bench_function("StreamReader/read_10/fixed_size_binary_256", |b| {
        let wide_uncompressed = fixed_size_binary_stream(1, 256, IpcWriteOptions::default(), false);
        b.iter(|| read_stream(wide_uncompressed.as_slice()))
    });
    group.bench_function("StreamReader/read_10/fixed_size_binary_256/lz4", |b| {
        let wide_lz4 = fixed_size_binary_stream(1, 256, lz4_options(), true);
        b.iter(|| read_stream(wide_lz4.as_slice()))
    });

    group.bench_function("StreamReader/read_10/fixed_size_binary_16x16", |b| {
        let narrow_uncompressed =
            fixed_size_binary_stream(16, 16, IpcWriteOptions::default(), false);
        b.iter(|| read_stream(narrow_uncompressed.as_slice()))
    });
    group.bench_function("StreamReader/read_10/fixed_size_binary_16x16/lz4", |b| {
        let narrow_lz4 = fixed_size_binary_stream(16, 16, lz4_options(), true);
        b.iter(|| read_stream(narrow_lz4.as_slice()))
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

/// Return an IPC stream with 10 record batches
fn ipc_stream(options: IpcWriteOptions) -> Vec<u8> {
    let batch = create_batch(8192, true);
    ipc_stream_with_batch(&batch, options)
}

fn ipc_stream_with_batch(batch: &RecordBatch, options: IpcWriteOptions) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options).unwrap();
    for _ in 0..10 {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    buffer
}

fn lz4_options() -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap()
}

fn read_stream(buffer: &[u8]) {
    let projection = None;
    let mut reader = StreamReader::try_new(buffer, projection).unwrap();
    for _ in 0..10 {
        std::hint::black_box(reader.next().unwrap().unwrap());
    }
    assert!(reader.next().is_none());
}

fn validate_stream(buffer: &[u8], expected: &RecordBatch) {
    let projection = None;
    let mut reader = StreamReader::try_new(buffer, projection).unwrap();
    for _ in 0..10 {
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(&actual, expected);
    }
    assert!(reader.next().is_none());
}

fn assert_lz4_compressed_buffers(batch: &RecordBatch, require_all_positive: bool) {
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut write_context = IpcWriteContext::default();
    let options = lz4_options();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(batch, &mut dictionary_tracker, &options, &mut write_context)
        .unwrap();
    let message = root_as_message(&encoded.ipc_message).unwrap();
    assert_eq!(message.header_type(), MessageHeader::RecordBatch);
    let record_batch = message.header_as_record_batch().unwrap();
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

fn fixed_size_binary_stream(
    num_columns: usize,
    value_size: usize,
    options: IpcWriteOptions,
    require_all_positive: bool,
) -> Vec<u8> {
    let batch = create_fixed_size_binary_batch(num_columns, value_size);
    let stream = ipc_stream_with_batch(&batch, options);
    validate_stream(&stream, &batch);
    if require_all_positive {
        assert_lz4_compressed_buffers(&batch, true);
    }
    stream
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

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);

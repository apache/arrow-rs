// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::builder::{
    Date32Builder, Decimal128Builder, Int32Builder, ListBuilder, StringDictionaryBuilder,
};
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, builder::StringBuilder};
use arrow_buffer::Buffer;
use arrow_ipc::convert::try_fb_to_schema;
use arrow_ipc::reader::{FileDecoder, FileReader, StreamDecoder, StreamReader, read_footer_length};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow_ipc::{Block, CompressionType, root_as_footer};
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

    group.bench_function("StreamDecoder/read_10", |b| {
        let buffer = Buffer::from_vec(ipc_stream(IpcWriteOptions::default()));
        b.iter(move || {
            let mut decoder = StreamDecoder::new();
            let mut buffer = buffer.clone();
            for _ in 0..10 {
                decoder.decode(&mut buffer).unwrap().unwrap();
            }
            assert!(decoder.decode(&mut buffer).unwrap().is_none());
            decoder.finish().unwrap();
        })
    });

    group.bench_function("StreamDecoder/no_validation/read_10", |b| {
        let buffer = Buffer::from_vec(ipc_stream(IpcWriteOptions::default()));
        b.iter(move || {
            // safety: we created a valid IPC stream
            let mut decoder = unsafe { StreamDecoder::new().with_skip_validation(true) };
            let mut buffer = buffer.clone();
            for _ in 0..10 {
                decoder.decode(&mut buffer).unwrap().unwrap();
            }
            assert!(decoder.decode(&mut buffer).unwrap().is_none());
            decoder.finish().unwrap();
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

    for (rows, threads) in [(256, 1), (8192, 1), (256, 16), (8192, 16)] {
        group.bench_function(
            format!("StreamReader/read_10/lz4/rows={rows}/threads={threads}"),
            |b| {
                let buffer = ipc_stream_with_rows(
                    rows,
                    IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .unwrap(),
                );
                b.iter(|| {
                    std::thread::scope(|s| {
                        for _ in 0..threads {
                            s.spawn(|| {
                                let mut reader =
                                    StreamReader::try_new(buffer.as_slice(), None).unwrap();
                                for _ in 0..10 {
                                    reader.next().unwrap().unwrap();
                                }
                                assert!(reader.next().is_none());
                            });
                        }
                    })
                })
            },
        );
    }

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

    group.bench_function("StreamReader/no_validation/read_10/lz4", |b| {
        let buffer = ipc_stream(
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
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

    // A message holds one compressed buffer per field node plus its validity and offsets, so the
    // per-buffer cost of decompression is what a wide relation pays most of, and the four-column
    // shape above barely shows it. 200 columns of 4096 rows keeps every buffer small; 20 columns
    // of 131072 rows makes each values buffer half a megabyte.
    //
    // For lz4 the declared maximum block size is set explicitly rather than left to the writer's
    // default, because it is what a frame costs a streaming decoder to open, whatever the buffer
    // behind it holds: 4 MiB is what Arrow C++ and Comet declare.
    for (cols, rows) in [(200, 4096), (20, 131072)] {
        let batch = create_wide_batch(cols, rows);
        let shape = format!("wide_{cols}x{rows}");

        for block_size in [64 * 1024, 4 * 1024 * 1024] {
            let name = format!(
                "StreamReader/read_10/{shape}/lz4_{}kib_blocks",
                block_size / 1024
            );
            group.bench_function(&name, |b| {
                let buffer = ipc_stream_of(
                    &batch,
                    IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .unwrap()
                        .try_with_lz4_block_size(Some(block_size))
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
        }

        let name = format!("StreamReader/read_10/{shape}/zstd");
        group.bench_function(&name, |b| {
            let buffer = ipc_stream_of(
                &batch,
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
    }

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

    let num_columns = 1000;
    for num_distinct in [4, 4096] {
        for (layout, list) in [("flat", false), ("list", true)] {
            let buffer = dictionary_ipc_stream(num_columns, num_distinct, list);
            let name = format!(
                "StreamReader/dictionaries/{num_columns}_columns/{num_distinct}_distinct/{layout}"
            );
            group.bench_function(name, |b| {
                b.iter(|| {
                    let mut reader = StreamReader::try_new(buffer.as_slice(), None).unwrap();
                    reader.next().unwrap().unwrap();
                    assert!(reader.next().is_none());
                })
            });
        }
    }
}

/// Return an IPC stream with one record batch of `num_columns` string dictionary columns. Each
/// column holds the same 4096 values with `num_distinct` distinct values, either directly or, with
/// `list`, grouped into lists of 4.
fn dictionary_ipc_stream(num_columns: usize, num_distinct: usize, list: bool) -> Vec<u8> {
    let values: Vec<String> = (0..4096)
        .map(|i| format!("value_{}", i % num_distinct))
        .collect();
    let column: ArrayRef = if list {
        let mut builder = ListBuilder::new(StringDictionaryBuilder::<Int32Type>::new());
        for chunk in values.chunks(4) {
            builder.append_value(chunk.iter().map(Some));
        }
        Arc::new(builder.finish())
    } else {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.extend(values.iter().map(Some));
        Arc::new(builder.finish())
    };
    let batch = RecordBatch::try_from_iter(
        (0..num_columns).map(|i| (format!("c{i}"), Arc::clone(&column))),
    )
    .unwrap();
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    buffer
}

/// Return an IPC stream with 10 record batches
fn ipc_stream(options: IpcWriteOptions) -> Vec<u8> {
    ipc_stream_with_rows(8192, options)
}

/// Return an IPC stream with 10 record batches of `rows` rows each
fn ipc_stream_with_rows(rows: usize, options: IpcWriteOptions) -> Vec<u8> {
    let batch = create_batch(rows, true);
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options).unwrap();
    for _ in 0..10 {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    buffer
}

/// Return an IPC stream with 10 copies of `batch`
fn ipc_stream_of(batch: &RecordBatch, options: IpcWriteOptions) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(2 * 1024 * 1024);
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options).unwrap();
    for _ in 0..10 {
        writer.write(batch).unwrap();
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

/// A batch of `num_cols` nullable Int32 columns of `num_rows` rows, so that a message carries
/// many compressed buffers rather than a few large ones.
fn create_wide_batch(num_cols: usize, num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        (0..num_cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ));
    let columns = (0..num_cols)
        .map(|col| {
            let mut b = Int32Builder::with_capacity(num_rows);
            for row in 0..num_rows {
                if row % 10 == 0 {
                    b.append_null();
                } else {
                    b.append_value((row * (col + 1)) as i32);
                }
            }
            Arc::new(b.finish()) as _
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

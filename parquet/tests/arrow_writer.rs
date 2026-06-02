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

//! Tests for [`ArrowWriter`]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs::File;
use std::io::{Read as _, Seek, SeekFrom, Write as _};
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowWriterOptions, PageKey, PageStore, PageStoreFactory};
use parquet::basic::Encoding;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

#[test]
#[should_panic(
    expected = "DeltaBitPackDecoder only supports Int32Type, UInt32Type, Int64Type, and UInt64Type"
)]
fn test_delta_bit_pack_type() {
    let props = WriterProperties::builder()
        .set_column_encoding("col".into(), Encoding::DELTA_BINARY_PACKED)
        .build();

    let record_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            false,
        )])),
        vec![Arc::new(Float64Array::from_iter_values(vec![1., 2.]))],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props)).unwrap();
    let _ = writer.write(&record_batch);
}

// ---------------------------------------------------------------------------
// Heap-memory regression test for the writer's page buffering.
//
// This proves the headline invariant of the pluggable [`PageStore`]: while a
// row group is being written, the heap used to buffer completed pages grows
// with the row group size for the default in-memory store, but stays bounded
// (≈ a few pages per leaf column) once a spilling backend is plugged in.
//
// Peak heap is measured with a thread-local tracking allocator (the same
// pattern used by `parquet/benches/arrow_reader_peak_memory.rs`), so the test
// needs no external profiling dependency. Tracking is thread-local, so the
// measured peak reflects only allocations made on the measuring thread; the
// default `ArrowWriter` is single-threaded, so the writer's buffering all lands
// there. Each measurement resets the peak to the current live baseline and
// reports the delta, so the threads of unrelated tests in this binary do not
// perturb it.
//
// [`PageStore`]: parquet::arrow::arrow_writer::PageStore
// ---------------------------------------------------------------------------

thread_local! {
    static LIVE_BYTES: Cell<usize> = const { Cell::new(0) };
    static PEAK_BYTES: Cell<usize> = const { Cell::new(0) };
}

struct TrackingAllocator {
    inner: System,
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator { inner: System };

fn add_live_bytes(size: usize) {
    LIVE_BYTES.with(|live| {
        let new = live.get().saturating_add(size);
        live.set(new);
        PEAK_BYTES.with(|peak| {
            if new > peak.get() {
                peak.set(new);
            }
        });
    });
}

fn subtract_live_bytes(size: usize) {
    LIVE_BYTES.with(|live| {
        live.set(live.get().saturating_sub(size));
    });
}

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            add_live_bytes(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        subtract_live_bytes(layout.size());
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            if new_size > old_size {
                add_live_bytes(new_size - old_size);
            } else {
                subtract_live_bytes(old_size - new_size);
            }
        }
        new_ptr
    }
}

/// Run `f` and return the peak *additional* live heap (bytes) observed on this
/// thread during it — the delta from the live heap when `f` began.
fn peak_heap_bytes(f: impl FnOnce()) -> usize {
    let start = LIVE_BYTES.with(Cell::get);
    // Reset the peak to the window's baseline so prior allocations don't count.
    PEAK_BYTES.with(|peak| peak.set(start));
    f();
    PEAK_BYTES.with(Cell::get).saturating_sub(start)
}

/// Width of each value in the one "fat" column, in bytes.
const FAT_VALUE_LEN: usize = 4096;
/// Rows per input batch fed to the writer. Kept small so each batch is dropped
/// promptly — only the writer's *buffering* should accumulate, not the input.
const ROWS_PER_BATCH: usize = 64;
/// Number of batches, all funnelled into a single large row group.
const NUM_BATCHES: usize = 64;
/// Total bytes of fat-column payload written (≈ 16 MiB).
const TOTAL_FAT_BYTES: usize = FAT_VALUE_LEN * ROWS_PER_BATCH * NUM_BATCHES;

/// A wide schema: one fat, high-cardinality binary column (the spill target)
/// plus several tiny integer columns.
fn skewed_schema() -> SchemaRef {
    let mut fields = vec![Field::new("fat", DataType::Binary, false)];
    for i in 0..8 {
        fields.push(Field::new(format!("small_{i}"), DataType::Int32, false));
    }
    Arc::new(Schema::new(fields))
}

/// Build one batch of `ROWS_PER_BATCH` rows. The fat column holds unique,
/// high-entropy values (so they neither dictionary-encode nor compress away),
/// derived deterministically from `batch_index`.
fn make_batch(schema: &SchemaRef, batch_index: usize) -> RecordBatch {
    let mut fat: Vec<u8> = vec![0u8; FAT_VALUE_LEN * ROWS_PER_BATCH];
    // A cheap xorshift fill keyed by the batch index → distinct, incompressible.
    let mut state = (batch_index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for byte in fat.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state >> 24) as u8;
    }
    let offsets: Vec<i32> = (0..=ROWS_PER_BATCH)
        .map(|i| (i * FAT_VALUE_LEN) as i32)
        .collect();
    let fat_array = BinaryArray::try_new(
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        arrow::buffer::Buffer::from_vec(fat),
        None,
    )
    .unwrap();

    let mut columns: Vec<ArrayRef> = vec![Arc::new(fat_array)];
    for c in 0..8 {
        let vals: Vec<i32> = (0..ROWS_PER_BATCH)
            .map(|r| (batch_index * ROWS_PER_BATCH + r + c) as i32)
            .collect();
        columns.push(Arc::new(Int32Array::from(vals)));
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

/// Writer properties forcing the whole dataset into a single, uncompressed row
/// group (so the page buffer is the only thing that grows).
fn single_row_group_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        // One row group for everything: never auto-flush on row count.
        .set_max_row_group_row_count(Some(ROWS_PER_BATCH * NUM_BATCHES * 2))
        .build()
}

/// Write the full skewed dataset with the given writer options, feeding small
/// batches (each dropped immediately) into one row group.
///
/// The output is sent to [`io::sink`] so the produced file bytes never live on
/// the heap — the measured peak then reflects only the writer's internal page
/// *buffering*, which is exactly what a [`PageStore`] governs.
fn write_skewed_dataset(options: ArrowWriterOptions) {
    let schema = skewed_schema();
    let mut writer =
        ArrowWriter::try_new_with_options(std::io::sink(), schema.clone(), options).unwrap();
    for b in 0..NUM_BATCHES {
        let batch = make_batch(&schema, b);
        writer.write(&batch).unwrap();
        // `batch` dropped here — only the writer's internal buffering persists.
    }
    writer.close().unwrap();
}

/// A spilling [`PageStore`]: one temp file per column chunk. `put` appends the
/// blob and records its `(offset, len)`; `take` seeks and reads it back. The
/// file is unlinked on creation (via [`tempfile::tempfile`]) so it is cleaned up
/// when the store is dropped. This is the canonical "spill completed pages off
/// the heap" backend the design targets.
struct TempFilePageStore {
    file: File,
    end: u64,
    locs: Vec<(u64, usize)>,
}

impl TempFilePageStore {
    fn new() -> Result<Self> {
        Ok(Self {
            file: tempfile::tempfile()?,
            end: 0,
            locs: Vec::new(),
        })
    }
}

impl PageStore for TempFilePageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        // Always append at the logical end (a prior `take` may have moved the
        // OS file cursor).
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&value)?;
        let key = PageKey::new(self.locs.len() as u64);
        self.locs.push((self.end, value.len()));
        self.end += value.len() as u64;
        Ok(key)
    }

    fn take(&mut self, key: PageKey) -> Result<Bytes> {
        let (offset, len) = self.locs[key.get() as usize];
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

#[derive(Debug, Default)]
struct TempFilePageStoreFactory;

impl PageStoreFactory for TempFilePageStoreFactory {
    fn create(&self, _column_index: usize) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(TempFilePageStore::new()?))
    }
}

/// Rows per batch / batches for the dictionary-column scenario (~4.2M rows).
const DICT_ROWS_PER_BATCH: usize = 8192;
const DICT_NUM_BATCHES: usize = 512;

/// Write a single, low-cardinality (16 distinct values), high-row-count column
/// as one row group. Such a column stays dictionary-encoded, so its completed
/// data pages would historically pile up in `GenericColumnWriter` until close —
/// the second accumulation point that plain page-buffer spilling does not reach.
fn write_dict_dataset(options: ArrowWriterOptions) {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(DICT_ROWS_PER_BATCH * DICT_NUM_BATCHES * 2))
        .build();
    let options = options.with_properties(props);
    let mut writer =
        ArrowWriter::try_new_with_options(std::io::sink(), schema.clone(), options).unwrap();
    for b in 0..DICT_NUM_BATCHES {
        let vals: Vec<i32> = (0..DICT_ROWS_PER_BATCH)
            .map(|r| ((b + r) % 16) as i32)
            .collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

/// All measurements run in one function so they execute sequentially on a single
/// thread — the tracking allocator is thread-local, so running them as separate
/// parallel tests would each see only their own thread's allocations (which is
/// fine), but keeping them together also keeps the in-memory/spill comparison on
/// one consistent baseline.
#[test]
fn page_store_bounds_write_memory() {
    let props = single_row_group_props();

    // Baseline: the default in-memory store buffers the whole row group, so peak
    // heap is at least the size of the buffered column data.
    let in_memory_peak = peak_heap_bytes(|| {
        let opts = ArrowWriterOptions::new().with_properties(props.clone());
        write_skewed_dataset(opts);
    });

    // Spilling: the temp-file store keeps completed pages off the heap, so peak
    // heap stays bounded by the in-flight encoder/dictionary buffers plus a page
    // or two in flight — independent of the row group size.
    let spill_peak = peak_heap_bytes(|| {
        let opts = ArrowWriterOptions::new()
            .with_properties(props.clone())
            .with_page_store_factory(Arc::new(TempFilePageStoreFactory));
        write_skewed_dataset(opts);
    });

    eprintln!(
        "peak heap — in-memory: {:.1} MiB, temp-file spill: {:.1} MiB (total fat payload {:.1} MiB)",
        in_memory_peak as f64 / (1024.0 * 1024.0),
        spill_peak as f64 / (1024.0 * 1024.0),
        TOTAL_FAT_BYTES as f64 / (1024.0 * 1024.0),
    );

    // The in-memory store must hold most of the ~16 MiB of buffered data.
    let in_memory_floor = TOTAL_FAT_BYTES * 3 / 4;
    assert!(
        in_memory_peak >= in_memory_floor,
        "expected in-memory peak >= {in_memory_floor} bytes, got {in_memory_peak}"
    );

    // The spilling store must stay near the per-column bound — roughly
    // (data_page_size + dict_page_size) per leaf column, ~2 MiB × 9 columns —
    // and far below the in-memory baseline. We assert a generous 8 MiB ceiling
    // (well under the ~16 MiB row group) to stay robust across platforms.
    const SPILL_CEILING: usize = 8 * 1024 * 1024;
    assert!(
        spill_peak < SPILL_CEILING,
        "expected spilling peak < {SPILL_CEILING} bytes (bounded by page/dict size × columns), \
         got {spill_peak}"
    );
    assert!(
        spill_peak * 2 < in_memory_peak,
        "expected spilling peak ({spill_peak}) to be far below the in-memory baseline \
         ({in_memory_peak})"
    );

    // Dictionary-encoded column: completed data pages reach the page writer (and
    // thus the store) as they are produced, so spilling bounds them too.
    let dict_in_memory = peak_heap_bytes(|| write_dict_dataset(ArrowWriterOptions::new()));
    let dict_spill = peak_heap_bytes(|| {
        write_dict_dataset(
            ArrowWriterOptions::new().with_page_store_factory(Arc::new(TempFilePageStoreFactory)),
        )
    });
    eprintln!(
        "dict column ({} rows) peak heap — in-memory: {:.2} MiB, temp-file spill: {:.2} MiB",
        DICT_ROWS_PER_BATCH * DICT_NUM_BATCHES,
        dict_in_memory as f64 / (1024.0 * 1024.0),
        dict_spill as f64 / (1024.0 * 1024.0),
    );
    assert!(
        dict_spill * 2 < dict_in_memory,
        "expected dict-column spilling peak ({dict_spill}) to be far below the in-memory \
         baseline ({dict_in_memory}) — dictionary data pages should spill, not accumulate"
    );
}

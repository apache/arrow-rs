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

//! Heap-memory regression tests for the Parquet writer's page buffering,
//! measured with [`dhat`].
//!
//! These prove the headline invariant of the pluggable [`PageStore`]: while a
//! row group is being written, the heap used to buffer completed pages grows
//! with the row group size for the default in-memory store, but stays bounded
//! (≈ a few pages per leaf column) once a spilling backend is plugged in.
//!
//! The whole test binary uses dhat's allocator, so every test here observes
//! precise peak-heap statistics. dhat's profiler is process-global and only one
//! may be live at a time, so all measurements run in a single, serialized test.
//!
//! [`PageStore`]: parquet::arrow::arrow_writer::PageStore

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::properties::WriterProperties;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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

/// Run `f` under a fresh dhat profiler and return the peak live heap (bytes)
/// observed during it.
fn peak_heap_bytes(f: impl FnOnce()) -> usize {
    let profiler = dhat::Profiler::builder().testing().build();
    f();
    let stats = dhat::HeapStats::get();
    drop(profiler);
    stats.max_bytes
}

#[test]
fn in_memory_store_buffers_whole_row_group() {
    // Baseline: with the default in-memory page store, peak heap while writing a
    // single large row group is at least the size of the buffered column data —
    // memory grows with the row group, unbounded. A spilling backend (added in a
    // later commit) is measured against this.
    let props = single_row_group_props();
    let peak = peak_heap_bytes(|| {
        let opts = ArrowWriterOptions::new().with_properties(props.clone());
        write_skewed_dataset(opts);
    });

    eprintln!(
        "in-memory peak heap: {peak} bytes ({:.1} MiB); total fat payload {TOTAL_FAT_BYTES} bytes",
        peak as f64 / (1024.0 * 1024.0)
    );

    // The fat column alone is ~16 MiB and must be fully resident in the page
    // buffer at flush. Allow generous headroom below the total to stay robust.
    let floor = TOTAL_FAT_BYTES * 3 / 4;
    assert!(
        peak >= floor,
        "expected in-memory peak heap >= {floor} bytes (3/4 of buffered data), got {peak}"
    );
}

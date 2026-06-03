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

//! Demonstrates the pluggable [`PageStore`] API by implementing a **spilling**
//! page store that keeps completed Parquet pages in temp files instead of on the
//! heap.
//!
//! # Background
//!
//! Parquet requires every column chunk to be contiguous in the file, but Arrow
//! record batches arrive with all columns interleaved. So while a row group is
//! being written, the writer must buffer every column's completed, compressed
//! pages until the row group is flushed. Peak write memory therefore grows with
//! the row group size — painful for wide schemas with large, skewed columns
//! (e.g. a few `id` columns next to a pile of fat string columns).
//!
//! A [`PageStore`] lets that page buffer live somewhere other than the heap. This
//! example plugs in a [`TempFilePageStore`] (one temp file per column chunk) and
//! compares peak writer memory against the default in-memory buffering.
//!
//! # Concurrency
//!
//! To keep every core busy this example splits the work across two thread pools,
//! sized to the machine: `N/2` **generator** threads building record batches and
//! `N/2` **encoder** threads encoding them (`N` = available cores). It uses the
//! low-level [`ArrowColumnWriter`] API (rather than the single-threaded
//! [`ArrowWriter`]) so encoding can be parallelized:
//!
//! - Each generator claims the next batch index from a shared counter, builds
//!   that batch deterministically, and sends it to the main thread.
//! - The main thread re-orders batches by index (so the output is deterministic
//!   regardless of how the generators interleave) and broadcasts each one to all
//!   encoders. Batches are cheap to share — the columns are reference-counted.
//! - The columns are distributed across the encoder threads, each owning a
//!   disjoint subset of [`ArrowColumnWriter`]s backed by their own [`PageStore`].
//!   Each encoder picks out its columns from every batch via [`compute_leaves`].
//! - The finished [`ArrowColumnChunk`]s are collected, sorted into schema order,
//!   and spliced into the row group — streaming back out of the store one page
//!   at a time — on the main thread.
//!
//! (For clarity this hand-rolls the pools with threads and channels; production
//! code would use rayon or tokio.)
//!
//! # Running
//!
//! ```sh
//! # Default in-memory page buffering (baseline): peak writer memory grows with
//! # the row group.
//! cargo run --release --features cli --example spill_page_store
//!
//! # Spill completed pages to temp files: peak writer memory stays bounded by
//! # the in-flight encoder buffers, independent of the row group size.
//! cargo run --release --features cli --example spill_page_store -- --spill
//!
//! # Make the schema wider / the skew worse:
//! cargo run --release --features cli --example spill_page_store -- --spill --large-string-columns 40
//! ```
//!
//! [`ArrowWriter`]: parquet::arrow::ArrowWriter
//! [`ArrowColumnWriter`]: parquet::arrow::arrow_writer::ArrowColumnWriter
//! [`ArrowColumnChunk`]: parquet::arrow::arrow_writer::ArrowColumnChunk
//! [`compute_leaves`]: parquet::arrow::arrow_writer::compute_leaves
//! [`PageStore`]: parquet::arrow::arrow_writer::PageStore

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use tempfile::NamedTempFile;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, PageKey, PageStore,
    PageStoreFactory, compute_leaves,
};
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

/// Write a skewed, wide Parquet file and compare peak writer memory with and
/// without a spilling `PageStore`.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of large (~8 KiB average) string columns to write — the fat
    /// columns that make the in-memory page buffer blow up.
    #[arg(long, default_value_t = 10)]
    large_string_columns: usize,

    /// Number of small (~20 byte average) string columns to write.
    #[arg(long, default_value_t = 5)]
    small_string_columns: usize,

    /// Number of integer (`Int64`) columns to write.
    #[arg(long, default_value_t = 3)]
    int_columns: usize,

    /// Total number of rows, all funnelled into a single row group.
    #[arg(long, default_value_t = 2048)]
    rows: usize,

    /// Rows per input batch fed to the writer. Each batch is dropped right after
    /// it is written, so only the writer's internal buffering accumulates.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Use the spilling [`TempFilePageStore`] instead of the default in-memory
    /// page buffering.
    #[arg(long)]
    spill: bool,

    /// Optional path to write the Parquet file to. Defaults to `io::sink()` so
    /// the produced file bytes never live on the heap and the reported memory
    /// reflects only the writer's page buffering.
    #[arg(long)]
    output: Option<std::path::PathBuf>,
}

/// Average length, in bytes, of values in a "large" string column.
const LARGE_AVG_LEN: usize = 8 * 1024;
/// Average length, in bytes, of values in a "small" string column.
const SMALL_AVG_LEN: usize = 20;

// ---------------------------------------------------------------------------
// The spilling page store.
//
// A `PageStore` is intentionally "dumb": it maps an opaque, store-allocated
// `PageKey` to a blob of bytes and knows nothing about pages, dictionaries, or
// ordering. The caller (`ArrowWriter`) keeps the handles and decides what they
// mean. That is all a backend has to implement to move the page buffer off the
// heap.
// ---------------------------------------------------------------------------

/// Shared spill bookkeeping, aggregated across every per-column
/// [`TempFilePageStore`]. Each store holds an `Arc` to the same instance, so the
/// totals and per-file records survive the stores being dropped at row group
/// flush.
#[derive(Debug, Default)]
struct SpillStats {
    /// Total bytes handed to `put` and written to a temp file.
    bytes_written: AtomicU64,
    /// Total bytes read back out of a temp file by `take` (at row group flush).
    bytes_read: AtomicU64,
    /// One record per temp file, pushed when each store is dropped.
    files: Mutex<Vec<FileRecord>>,
}

/// What a single spill temp file ended up holding.
#[derive(Debug)]
struct FileRecord {
    /// Leaf column index the store was created for.
    column_index: usize,
    /// Filesystem path of the temp file (valid while the store was alive).
    path: PathBuf,
    /// Total bytes written into the file.
    bytes: u64,
}

/// A spilling [`PageStore`]: one temp file per column chunk.
///
/// `put` appends the blob to the file and records its `(offset, len)`; `take`
/// seeks and reads it back. A [`NamedTempFile`] is used (rather than an
/// anonymous one) so the file has a reportable path; the OS reclaims it when the
/// store is dropped at row group flush.
struct TempFilePageStore {
    file: NamedTempFile,
    /// Leaf column index this store backs (used only for reporting).
    column_index: usize,
    /// Logical end of the file — where the next `put` appends.
    end: u64,
    /// `(offset, len)` for each stored blob, indexed by the `PageKey` we minted.
    locs: Vec<(u64, usize)>,
    /// Shared, cross-store spill bookkeeping (see [`SpillStats`]).
    stats: Arc<SpillStats>,
}

impl TempFilePageStore {
    fn new(stats: Arc<SpillStats>, column_index: usize) -> Result<Self> {
        Ok(Self {
            file: NamedTempFile::new()?,
            column_index,
            end: 0,
            locs: Vec::new(),
            stats,
        })
    }
}

impl PageStore for TempFilePageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        // Always append at the logical end (a prior `take` may have moved the
        // OS file cursor).
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&value)?;
        self.stats
            .bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
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
        self.stats
            .bytes_read
            .fetch_add(len as u64, Ordering::Relaxed);
        Ok(Bytes::from(buf))
    }

    // `memory_size` keeps its default of 0: once a blob is handed to `put` it
    // lives in the temp file, not on the heap. That zero is what makes
    // `ArrowWriter::memory_size()` drop to just the in-flight encoder buffers.
}

impl Drop for TempFilePageStore {
    fn drop(&mut self) {
        // Record this file's path and final byte count before the NamedTempFile
        // is unlinked, so `main` can list it after the writer is closed.
        self.stats.files.lock().unwrap().push(FileRecord {
            column_index: self.column_index,
            path: self.file.path().to_path_buf(),
            bytes: self.end,
        });
    }
}

/// Creates a fresh [`TempFilePageStore`] for every column chunk the writer opens,
/// handing each one an `Arc` to the shared [`SpillStats`].
#[derive(Debug)]
struct TempFilePageStoreFactory {
    stats: Arc<SpillStats>,
}

impl PageStoreFactory for TempFilePageStoreFactory {
    fn create(&self, column_index: usize) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(TempFilePageStore::new(
            self.stats.clone(),
            column_index,
        )?))
    }
}

// ---------------------------------------------------------------------------
// Schema + data generation.
// ---------------------------------------------------------------------------

/// Build the wide, skewed schema: a few integer columns, then a bunch of small
/// string columns, then the fat large string columns.
fn build_schema(args: &Args) -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..args.int_columns {
        fields.push(Field::new(format!("int_{i}"), DataType::Int64, false));
    }
    for i in 0..args.small_string_columns {
        fields.push(Field::new(format!("small_str_{i}"), DataType::Utf8, false));
    }
    for i in 0..args.large_string_columns {
        fields.push(Field::new(format!("large_str_{i}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// The per-batch column counts a generator needs (a small `Copy` view of `Args`
/// so it can be handed to each generator thread).
#[derive(Clone, Copy)]
struct BatchSpec {
    int_columns: usize,
    small_string_columns: usize,
    large_string_columns: usize,
}

/// Fill `buf` with a deterministic value of exactly `len` bytes derived from the
/// counter `n`.
///
/// The 20-digit zero-padded counter makes every value distinct, so the fat
/// columns stay plain-encoded (high cardinality) rather than dictionary-encoding
/// away; the remainder is padded with a fixed `a`–`z` cycle. No RNG — the content
/// is a pure function of `n`, so a run is fully reproducible.
fn fill_value(buf: &mut String, n: u64, len: usize) {
    use std::fmt::Write;
    buf.clear();
    let _ = write!(buf, "{n:020}");
    while buf.len() < len {
        buf.push((b'a' + (buf.len() % 26) as u8) as char);
    }
    buf.truncate(len); // all bytes are ASCII, so this is a clean char boundary
}

/// Build a string column of `rows` values, each exactly `len` bytes, keyed by the
/// global row index (`row_offset + r`) and a per-column `salt` so values are
/// distinct within the column.
fn make_string_array(rows: usize, row_offset: u64, salt: u64, len: usize) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows, rows * len);
    let mut value = String::new();
    for r in 0..rows {
        let n = (row_offset + r as u64).wrapping_mul(101).wrapping_add(salt);
        fill_value(&mut value, n, len);
        builder.append_value(&value);
    }
    Arc::new(builder.finish())
}

/// Build the record batch covering rows `[row_offset, row_offset + rows)`.
///
/// Fully deterministic in `row_offset`: every column's values are a pure function
/// of the global row index, so the batch a generator produces depends only on its
/// claimed index — not on thread timing.
fn make_batch(schema: &SchemaRef, spec: BatchSpec, row_offset: u64, rows: usize) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut salt = 0u64; // distinguishes columns so they don't all hold equal values
    for _ in 0..spec.int_columns {
        let s = salt;
        salt += 1;
        let vals: Vec<i64> = (0..rows)
            .map(|r| (row_offset + r as u64 + s) as i64)
            .collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    for _ in 0..spec.small_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, SMALL_AVG_LEN));
        salt += 1;
    }
    for _ in 0..spec.large_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, LARGE_AVG_LEN));
        salt += 1;
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

// ---------------------------------------------------------------------------
// Memory reporting.
// ---------------------------------------------------------------------------

/// Current process resident set size (RSS), in bytes.
fn rss_bytes(system: &mut System) -> u64 {
    let Ok(pid) = sysinfo::get_current_pid() else {
        return 0;
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::everything(),
    );
    system.process(pid).map(|p| p.memory()).unwrap_or(0)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<()> {
    let start = Instant::now();
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset, so the page buffer (the
    // thing a PageStore governs) is the only thing that grows. Uncompressed keeps
    // the reported numbers easy to reason about — the buffer holds the raw page
    // bytes.
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_row_count(Some(args.rows * 2))
            .build(),
    );

    let spill_stats = Arc::new(SpillStats::default());

    // Total logical payload across the large columns — the part that dominates.
    let large_payload = args.large_string_columns * LARGE_AVG_LEN * args.rows;
    println!(
        "Writing {} rows × {} columns ({} int, {} small-string ~{}B, {} large-string ~{}B)",
        args.rows,
        args.int_columns + args.small_string_columns + args.large_string_columns,
        args.int_columns,
        args.small_string_columns,
        SMALL_AVG_LEN,
        args.large_string_columns,
        LARGE_AVG_LEN,
    );
    println!(
        "Page buffering: {}  (large-column payload ≈ {:.1} MiB)",
        if args.spill {
            "TempFilePageStore (spilling to temp files)"
        } else {
            "InMemoryPageStore (default, on the heap)"
        },
        mib(large_payload),
    );
    // Split the cores: half generate batches, half encode them.
    let num_cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let num_generators = (num_cores / 2).max(1);
    let num_encoders = (num_cores / 2).max(1);
    let num_batches = args.rows.div_ceil(args.batch_size);
    println!(
        "Cores: {num_cores}  ({num_generators} generator threads, {num_encoders} encoder threads, \
         {num_batches} batches of ≤{} rows)",
        args.batch_size,
    );

    let mut system = System::new_with_specifics(RefreshKind::everything());
    let rss_start = rss_bytes(&mut system);

    // Build the lower-level file writer so columns can be encoded in parallel.
    // `io::sink()` discards the produced file bytes so they never inflate the
    // heap — the measured peak then reflects only the writer's page buffering.
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(&schema)?;
    let writer_sink: Box<dyn Write + Send> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(std::io::sink()),
    };
    let mut file_writer =
        SerializedFileWriter::new(writer_sink, parquet_schema.root_schema_ptr(), props.clone())?;

    // One `ArrowColumnWriter` per leaf column, each backed by its own PageStore.
    // With `--spill`, that store is a TempFilePageStore, so each worker thread's
    // completed pages land in a temp file instead of the heap.
    let mut factory = ArrowRowGroupWriterFactory::new(&file_writer, schema.clone());
    if args.spill {
        factory = factory.with_page_store_factory(Arc::new(TempFilePageStoreFactory {
            stats: spill_stats.clone(),
        }));
    }
    let col_writers = factory.create_column_writers(0)?;

    // Distribute the columns across the encoder threads round-robin, so the fat
    // columns (contiguous at the end of the schema) spread evenly. Each encoder
    // owns a disjoint set of `(column index, ArrowColumnWriter)` pairs.
    let mut encoder_cols: Vec<Vec<(usize, ArrowColumnWriter)>> =
        (0..num_encoders).map(|_| Vec::new()).collect();
    for (idx, writer) in col_writers.into_iter().enumerate() {
        encoder_cols[idx % num_encoders].push((idx, writer));
    }

    // Spawn the encoder pool. Each encoder receives whole batches over a small
    // bounded channel (back-pressure), encodes only its own columns from each,
    // tracks the peak bytes each writer held resident, and returns the finished
    // chunks plus those peaks.
    let mut encoder_txs = Vec::with_capacity(num_encoders);
    let mut encoder_handles = Vec::with_capacity(num_encoders);
    for mut cols in encoder_cols {
        let (tx, rx) = sync_channel::<Arc<RecordBatch>>(1);
        let schema = schema.clone();
        let handle = thread::spawn(move || -> Result<Vec<(usize, ArrowColumnChunk, usize)>> {
            let mut peaks = vec![0usize; cols.len()];
            for batch in rx {
                for (slot, (idx, writer)) in cols.iter_mut().enumerate() {
                    let field = &schema.fields()[*idx];
                    for leaf in compute_leaves(field, batch.column(*idx))? {
                        writer.write(&leaf)?;
                    }
                    // `memory_size()` is the bytes this column's writer holds
                    // resident — pages in its PageStore plus in-flight encoder
                    // buffers. With the in-memory store it climbs toward the whole
                    // column chunk; with spilling it stays flat.
                    peaks[slot] = peaks[slot].max(writer.memory_size());
                }
            }
            cols.into_iter()
                .zip(peaks)
                .map(|((idx, writer), peak)| {
                    let peak = peak.max(writer.memory_size());
                    Ok((idx, writer.close()?, peak))
                })
                .collect()
        });
        encoder_txs.push(tx);
        encoder_handles.push(handle);
    }

    // Spawn the generator pool. Each generator claims the next batch index from a
    // shared counter, builds that batch deterministically, and sends `(index,
    // batch)` to the main thread. The bounded channel applies back-pressure so
    // generators don't race arbitrarily far ahead of the encoders.
    // Keep the result channel shallow: with wide/huge schemas each batch can be
    // hundreds of MiB, and they pipeline, so the in-flight input — not the
    // (spilled) page buffer — dominates process RSS. A depth of 1 bounds it to
    // roughly the working set the generators and encoders are actively touching.
    let next_batch = Arc::new(AtomicUsize::new(0));
    let (result_tx, result_rx) = sync_channel::<(usize, Arc<RecordBatch>)>(1);
    let mut gen_handles = Vec::with_capacity(num_generators);
    let spec = BatchSpec {
        int_columns: args.int_columns,
        small_string_columns: args.small_string_columns,
        large_string_columns: args.large_string_columns,
    };
    for _ in 0..num_generators {
        let schema = schema.clone();
        let next_batch = next_batch.clone();
        let tx = result_tx.clone();
        let (rows, batch_size) = (args.rows, args.batch_size);
        let handle = thread::spawn(move || {
            loop {
                let i = next_batch.fetch_add(1, Ordering::Relaxed);
                if i >= num_batches {
                    break;
                }
                let row_offset = (i * batch_size) as u64;
                let rows_in = batch_size.min(rows - i * batch_size);
                let batch = make_batch(&schema, spec, row_offset, rows_in);
                if tx.send((i, Arc::new(batch))).is_err() {
                    break; // main hung up (shouldn't happen on the happy path)
                }
            }
        });
        gen_handles.push(handle);
    }
    drop(result_tx); // only the generators hold senders now

    // Main: pull generated batches, re-order them by index, and broadcast each in
    // index order to every encoder. Re-ordering keeps the written row order — and
    // therefore the output file — deterministic regardless of generator timing.
    let mut peak_rss = rss_start;
    let mut pending: HashMap<usize, Arc<RecordBatch>> = HashMap::new();
    let mut next_emit = 0usize;
    while next_emit < num_batches {
        if let Some(batch) = pending.remove(&next_emit) {
            for tx in &encoder_txs {
                tx.send(batch.clone()).unwrap(); // cheap: clones an Arc
            }
            next_emit += 1;
            peak_rss = peak_rss.max(rss_bytes(&mut system));
        } else {
            let (i, batch) = result_rx.recv().expect("generators ended early");
            pending.insert(i, batch);
        }
    }
    drop(encoder_txs); // signal end-of-input to the encoders
    for handle in gen_handles {
        handle.join().expect("generator thread panicked");
    }

    // Collect the encoded chunks, sort them back into schema order, and splice
    // each into the row group (streaming pages back out of the store). Columns
    // must be appended in schema order.
    let mut chunks: Vec<(usize, ArrowColumnChunk, usize)> = Vec::new();
    for handle in encoder_handles {
        chunks.extend(handle.join().expect("encoder thread panicked")?);
        peak_rss = peak_rss.max(rss_bytes(&mut system));
    }
    chunks.sort_by_key(|(idx, _, _)| *idx);

    let mut row_group_writer = file_writer.next_row_group()?;
    let mut peak_writer_memory = 0usize;
    for (_idx, chunk, col_peak) in chunks {
        peak_writer_memory += col_peak;
        chunk.append_to_row_group(&mut row_group_writer)?;
        peak_rss = peak_rss.max(rss_bytes(&mut system));
    }
    row_group_writer.close()?;
    file_writer.close()?;
    peak_rss = peak_rss.max(rss_bytes(&mut system));
    let elapsed = start.elapsed();
    let written = args.rows;

    println!();
    println!("Done. Wrote {written} rows.");
    println!(
        "Peak writer memory (Σ per-column): {:>8.1} MiB   <- bytes the column writers held on the heap",
        mib(peak_writer_memory),
    );
    println!(
        "Peak process RSS delta          : {:>8.1} MiB",
        (peak_rss.saturating_sub(rss_start)) as f64 / (1024.0 * 1024.0),
    );
    println!(
        "Total elapsed time              : {:>8.3} s",
        elapsed.as_secs_f64(),
    );
    println!();
    if args.spill {
        let mut files = spill_stats.files.lock().unwrap();
        files.sort_by_key(|f| f.column_index);
        let written_bytes = spill_stats.bytes_written.load(Ordering::Relaxed);
        let read_bytes = spill_stats.bytes_read.load(Ordering::Relaxed);
        println!(
            "Spill temp files created        : {:>8}   <- one per column chunk",
            files.len(),
        );
        println!(
            "Total bytes spilled (written)   : {:>8.1} MiB",
            mib(written_bytes as usize),
        );
        println!(
            "Total bytes read back (take)    : {:>8.1} MiB",
            mib(read_bytes as usize),
        );
        println!();
        println!("Per spill file (column index, type, path, bytes stored):");
        for f in files.iter() {
            // `schema` is flat, so the leaf column index is also the field index.
            let field = schema.field(f.column_index);
            println!(
                "  col {:>3}  {:<8}  {}  {} bytes ({:.1} MiB)",
                f.column_index,
                format!("{}", field.data_type()),
                f.path.display(),
                f.bytes,
                mib(f.bytes as usize),
            );
        }
        println!();
        println!(
            "With spilling, peak writer memory is bounded by the in-flight encoder \n\
             buffers (a page or two per column), not the {:.1} MiB row group.",
            mib(large_payload),
        );
    } else {
        println!(
            "Re-run with --spill to keep those pages off the heap and watch peak \n\
             writer memory drop well below the {:.1} MiB row group payload.",
            mib(large_payload),
        );
    }

    Ok(())
}
